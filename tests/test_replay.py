"""Tests for replay-service: PG -> Kafka in (event_time, event_id) order."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from schemas.event_ids import derive_pitch_id
from schemas.mlb import EventType, MLBPitchEvent, Sport
from schemas.proto_utils import payload_dict_to_proto, proto_to_payload_dict
from services.replay.service import ReplayService


def _make_pitch(
    game_pk: str = "999001",
    at_bat: int = 1,
    pitch_no: int = 1,
    event_time: datetime | None = None,
) -> MLBPitchEvent:
    event = MLBPitchEvent()
    spine = event.spine
    spine.event_id = derive_pitch_id(game_pk, at_bat, pitch_no)
    spine.event_type = EventType.EVENT_TYPE_PITCH
    spine.sport = Sport.SPORT_MLB
    spine.game_pk = game_pk
    ts = Timestamp()
    ts.FromDatetime(event_time or datetime(2026, 4, 30, 19, 0, tzinfo=UTC))
    spine.event_time.CopyFrom(ts)
    spine.source_time.CopyFrom(ts)
    spine.ingest_time.CopyFrom(ts)
    event.at_bat_index = at_bat
    event.pitch_number = pitch_no
    event.inning = 5
    event.outs = 1
    event.batter_name = "Test Batter"
    return event


# ---------- inverse helper round-trip ----------


def test_payload_dict_round_trips_through_inverse_helper() -> None:
    original = _make_pitch()
    payload = proto_to_payload_dict(original)

    restored = MLBPitchEvent()
    payload_dict_to_proto(restored, payload)

    assert restored.spine.event_id == original.spine.event_id
    assert restored.spine.event_type == original.spine.event_type
    assert restored.spine.sport == original.spine.sport
    assert restored.spine.game_pk == original.spine.game_pk
    assert (
        restored.spine.event_time.ToDatetime() == original.spine.event_time.ToDatetime()
    )
    assert restored.at_bat_index == original.at_bat_index
    assert restored.pitch_number == original.pitch_number
    assert restored.inning == original.inning
    assert restored.outs == original.outs
    assert restored.batter_name == original.batter_name


# ---------- service unit tests ----------


class _FakeReader:
    def __init__(self, pitches: list[MLBPitchEvent]):
        self._pitches = pitches

    async def stream_pitches(
        self, game_pk: str, **_: Any
    ):  # AsyncIterator-shaped
        for pitch in self._pitches:
            yield pitch


@pytest.mark.asyncio
async def test_service_publishes_in_order_and_paces_with_speed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = datetime(2026, 4, 30, 19, 0, tzinfo=UTC)
    pitches = [
        _make_pitch(at_bat=1, pitch_no=1, event_time=base),
        _make_pitch(at_bat=1, pitch_no=2, event_time=base + timedelta(seconds=20)),
        _make_pitch(at_bat=1, pitch_no=3, event_time=base + timedelta(seconds=50)),
    ]
    reader = _FakeReader(pitches)
    publisher = AsyncMock()

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    service = ReplayService(reader, publisher, speed=10.0)  # type: ignore[arg-type]
    count = await service.replay_game("999001")

    assert count == 3
    assert publisher.publish_pitch.await_count == 3
    # Gaps were 20s and 30s; at 10x they become 2.0s and 3.0s.
    assert sleeps == [2.0, 3.0]
    # First call gets the first pitch.
    first_call_pitch = publisher.publish_pitch.await_args_list[0].args[0]
    assert first_call_pitch.spine.event_id == pitches[0].spine.event_id


@pytest.mark.asyncio
async def test_service_with_empty_stream_publishes_nothing() -> None:
    reader = _FakeReader([])
    publisher = AsyncMock()
    service = ReplayService(reader, publisher, speed=1.0)  # type: ignore[arg-type]
    assert await service.replay_game("none") == 0
    publisher.publish_pitch.assert_not_awaited()


def test_service_rejects_zero_or_negative_speed() -> None:
    publisher = AsyncMock()
    reader = _FakeReader([])
    with pytest.raises(ValueError, match="speed must be > 0"):
        ReplayService(reader, publisher, speed=0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="speed must be > 0"):
        ReplayService(reader, publisher, speed=-1)  # type: ignore[arg-type]


# ---------- integration ----------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_replay_round_trips_pg_events_through_kafka() -> None:
    import json as _json
    import pathlib

    from aiokafka import AIOKafkaConsumer
    from testcontainers.kafka import KafkaContainer
    from testcontainers.postgres import PostgresContainer

    from services.replay.publisher import ReplayPublisher
    from services.replay.reader import EventReader

    topic = "mlb.events.raw"
    base = datetime(2026, 4, 30, 19, 0, tzinfo=UTC)
    pitches = [
        _make_pitch(at_bat=1, pitch_no=1, event_time=base),
        _make_pitch(at_bat=1, pitch_no=2, event_time=base + timedelta(milliseconds=10)),
    ]

    with PostgresContainer("postgres:16-alpine") as pg, KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        db_url = pg.get_connection_url(driver=None)

        reader = await EventReader.connect(db_url)
        migration = pathlib.Path("migrations/001_initial.sql").read_text()
        async with reader._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(migration)

            for pitch in pitches:
                payload = proto_to_payload_dict(pitch)
                spine = pitch.spine
                await conn.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, sport, game_pk,
                        event_time, source_time, ingest_time, payload
                    ) VALUES ($1, 'pitch', 'mlb', $2, $3, $4, $5, $6::jsonb)
                    """,
                    spine.event_id,
                    spine.game_pk,
                    spine.event_time.ToDatetime(tzinfo=UTC),
                    spine.source_time.ToDatetime(tzinfo=UTC),
                    spine.ingest_time.ToDatetime(tzinfo=UTC),
                    _json.dumps(payload),
                )

        publisher = await ReplayPublisher.connect(
            bootstrap_servers=bootstrap,
            client_id="replay-it",
            topic=topic,
        )
        downstream = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id="replay-it-reader",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await downstream.start()
        try:
            service = ReplayService(reader, publisher, speed=100.0)
            count = await service.replay_game("999001")
            assert count == 2

            received: list[MLBPitchEvent] = []
            async for msg in downstream:
                received.append(MLBPitchEvent.FromString(msg.value))
                if len(received) == 2:
                    break

            ids_in = [p.spine.event_id for p in pitches]
            ids_out = [p.spine.event_id for p in received]
            assert ids_out == ids_in  # delivered in (event_time, event_id) order
        finally:
            await publisher.close()
            await downstream.stop()
            await reader.close()
