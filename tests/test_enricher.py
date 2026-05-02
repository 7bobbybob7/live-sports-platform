"""Unit + integration tests for the enricher service."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from schemas.event_ids import derive_pitch_id
from schemas.mlb import EventType, MLBPitchEnrichedEvent, MLBPitchEvent, Sport
from services.enricher.handler import EnrichmentError, MessageHandler
from services.enricher.state import GameContext, GameStateStore


def _make_pitch(
    game_pk: str = "777001", at_bat: int = 1, pitch_no: int = 1, outs: int | None = 1
) -> MLBPitchEvent:
    event = MLBPitchEvent()
    spine = event.spine
    spine.event_id = derive_pitch_id(game_pk, at_bat, pitch_no)
    spine.event_type = EventType.EVENT_TYPE_PITCH
    spine.sport = Sport.SPORT_MLB
    spine.game_pk = game_pk
    ts = Timestamp()
    ts.FromDatetime(datetime(2026, 4, 30, 19, 0, tzinfo=UTC))
    spine.event_time.CopyFrom(ts)
    spine.source_time.CopyFrom(ts)
    spine.ingest_time.CopyFrom(ts)
    event.at_bat_index = at_bat
    event.pitch_number = pitch_no
    event.inning = 3
    if outs is not None:
        event.outs = outs
    return event


# ---------- handler unit tests ----------


@pytest.mark.asyncio
async def test_handle_emits_enriched_event_with_zeroed_state_first_time() -> None:
    store = AsyncMock()
    store.get.return_value = GameContext()
    handler = MessageHandler(store)

    pitch = _make_pitch(game_pk="42")
    key, payload = await handler.handle(pitch.SerializeToString())

    assert key == b"42"
    enriched = MLBPitchEnrichedEvent.FromString(payload)
    assert enriched.spine.game_pk == "42"
    assert enriched.spine.event_id == pitch.spine.event_id
    assert enriched.pitch.spine.event_id == pitch.spine.event_id
    assert enriched.home_score == 0
    assert enriched.away_score == 0
    assert enriched.runner_on_first is False
    assert enriched.runner_on_second is False
    assert enriched.runner_on_third is False


@pytest.mark.asyncio
async def test_handle_writes_updated_inning_outs_to_state_store() -> None:
    store = AsyncMock()
    store.get.return_value = GameContext(inning=2, outs=0)
    handler = MessageHandler(store)

    pitch = _make_pitch(game_pk="42", outs=2)
    pitch.inning = 5
    await handler.handle(pitch.SerializeToString())

    store.set.assert_awaited_once()
    args, _ = store.set.call_args
    assert args[0] == "42"
    new_ctx: GameContext = args[1]
    assert new_ctx.inning == 5
    assert new_ctx.outs == 2


@pytest.mark.asyncio
async def test_handle_skips_state_write_when_unchanged() -> None:
    existing = GameContext(inning=3, outs=1)
    store = AsyncMock()
    store.get.return_value = existing
    handler = MessageHandler(store)

    await handler.handle(_make_pitch(outs=1).SerializeToString())
    store.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_carries_over_score_and_baserunner_state() -> None:
    store = AsyncMock()
    store.get.return_value = GameContext(
        home_score=3,
        away_score=1,
        runner_on_first=True,
        runner_on_third=True,
    )
    handler = MessageHandler(store)

    _, payload = await handler.handle(_make_pitch().SerializeToString())
    enriched = MLBPitchEnrichedEvent.FromString(payload)
    assert enriched.home_score == 3
    assert enriched.away_score == 1
    assert enriched.runner_on_first is True
    assert enriched.runner_on_second is False
    assert enriched.runner_on_third is True


@pytest.mark.asyncio
async def test_handle_raises_on_decode_error() -> None:
    handler = MessageHandler(AsyncMock())
    with pytest.raises(EnrichmentError, match="proto decode failed"):
        await handler.handle(b"not a valid proto")


@pytest.mark.asyncio
async def test_handle_raises_on_missing_game_pk() -> None:
    store = AsyncMock()
    handler = MessageHandler(store)
    pitch = _make_pitch()
    pitch.spine.game_pk = ""
    with pytest.raises(EnrichmentError, match="game_pk is empty"):
        await handler.handle(pitch.SerializeToString())


# ---------- integration ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enricher_end_to_end_through_real_kafka_and_redis() -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from redis.asyncio import Redis
    from testcontainers.kafka import KafkaContainer
    from testcontainers.redis import RedisContainer

    from services.enricher.consumer import Enricher

    raw_topic = "mlb.events.raw"
    enriched_topic = "mlb.events.enriched"

    with KafkaContainer() as kafka, RedisContainer() as redis_container:
        bootstrap = kafka.get_bootstrap_server()
        redis_url = (
            f"redis://{redis_container.get_container_host_ip()}:"
            f"{redis_container.get_exposed_port(6379)}/0"
        )

        redis = Redis.from_url(redis_url, decode_responses=True)
        store = GameStateStore(redis)

        source_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await source_producer.start()

        consumer = AIOKafkaConsumer(
            raw_topic,
            bootstrap_servers=bootstrap,
            group_id="enricher-it",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap, enable_idempotence=True, acks="all"
        )

        enricher = Enricher(
            consumer=consumer,
            producer=producer,
            handler=MessageHandler(store),
            source_topic=raw_topic,
            sink_topic=enriched_topic,
        )

        downstream = AIOKafkaConsumer(
            enriched_topic,
            bootstrap_servers=bootstrap,
            group_id="enriched-it-reader",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        await enricher.start()
        await downstream.start()
        try:
            pitch = _make_pitch(game_pk="500001")
            await source_producer.send_and_wait(
                raw_topic, pitch.SerializeToString(), key=b"500001"
            )

            stop_event = asyncio.Event()
            run_task = asyncio.create_task(enricher.run(stop_event))

            received: MLBPitchEnrichedEvent | None = None
            async for msg in downstream:
                received = MLBPitchEnrichedEvent.FromString(msg.value)
                break

            stop_event.set()
            await asyncio.wait_for(run_task, timeout=5)

            assert received is not None
            assert received.spine.game_pk == "500001"
            assert received.pitch.spine.event_id == pitch.spine.event_id

            ctx = await store.get("500001")
            assert ctx.inning == pitch.inning
            assert ctx.outs == pitch.outs
        finally:
            await source_producer.stop()
            await downstream.stop()
            await enricher.stop()
            await redis.aclose()
