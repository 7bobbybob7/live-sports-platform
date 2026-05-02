"""Unit tests for the ingestor's Kafka publisher."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from schemas.mlb import EventType, MLBPitchEvent, Sport
from services.mlb_ingestor.publisher import KafkaPublisher


def _make_pitch(game_pk: str = "777001") -> MLBPitchEvent:
    event = MLBPitchEvent()
    spine = event.spine
    spine.event_id = f"mlb:pitch:{game_pk}:1:1"
    spine.event_type = EventType.EVENT_TYPE_PITCH
    spine.sport = Sport.SPORT_MLB
    spine.game_pk = game_pk
    ts = Timestamp()
    ts.FromDatetime(datetime(2026, 4, 30, 19, 0, tzinfo=UTC))
    spine.event_time.CopyFrom(ts)
    spine.source_time.CopyFrom(ts)
    spine.ingest_time.CopyFrom(ts)
    event.at_bat_index = 1
    event.pitch_number = 1
    return event


@pytest.mark.asyncio
async def test_publish_pitch_uses_game_pk_as_key_and_proto_bytes() -> None:
    producer = AsyncMock()
    pub = KafkaPublisher(producer, topic="mlb.events.raw")

    pitch = _make_pitch(game_pk="777042")
    await pub.publish_pitch(pitch)

    producer.send_and_wait.assert_awaited_once()
    args, kwargs = producer.send_and_wait.call_args
    topic_arg, payload_arg = args
    assert topic_arg == "mlb.events.raw"
    assert kwargs["key"] == b"777042"
    # Round-trip the payload to confirm it's the serialized proto.
    decoded = MLBPitchEvent.FromString(payload_arg)
    assert decoded.spine.event_id == pitch.spine.event_id


@pytest.mark.asyncio
async def test_publish_pitch_propagates_producer_failure() -> None:
    producer = AsyncMock()
    producer.send_and_wait.side_effect = RuntimeError("broker down")
    pub = KafkaPublisher(producer, topic="mlb.events.raw")

    with pytest.raises(RuntimeError, match="broker down"):
        await pub.publish_pitch(_make_pitch())


@pytest.mark.asyncio
async def test_close_stops_producer() -> None:
    producer = AsyncMock()
    pub = KafkaPublisher(producer, topic="mlb.events.raw")
    await pub.close()
    producer.stop.assert_awaited_once()


# ---------- integration ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_pitch_round_trips_through_real_broker() -> None:
    from aiokafka import AIOKafkaConsumer
    from testcontainers.kafka import KafkaContainer

    topic = "mlb.events.raw"

    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()

        publisher = await KafkaPublisher.connect(
            bootstrap_servers=bootstrap,
            client_id="test-ingestor",
            topic=topic,
        )
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id="test-publisher-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            pitch_a = _make_pitch(game_pk="100001")
            pitch_b = _make_pitch(game_pk="100002")
            await publisher.publish_pitch(pitch_a)
            await publisher.publish_pitch(pitch_b)

            received: dict[bytes, MLBPitchEvent] = {}
            async for msg in consumer:
                received[msg.key] = MLBPitchEvent.FromString(msg.value)
                if len(received) == 2:
                    break

            assert received[b"100001"].spine.event_id == pitch_a.spine.event_id
            assert received[b"100002"].spine.event_id == pitch_b.spine.event_id
        finally:
            await publisher.close()
            await consumer.stop()
