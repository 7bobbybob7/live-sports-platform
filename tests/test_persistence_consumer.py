"""Tests for the persistence consumer.

Tiered:

- Unit tests (default): exercise the handler, retry loop, and error
  classification with an AsyncMock store. Millisecond-scale, no I/O.
- Integration test (marked `integration`): spins up real Kafka + Postgres
  via testcontainers, produces a proto message, and asserts it lands in
  the DB. Skipped by default; run with `pytest -m integration`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from schemas.mlb import EventType, MLBPitchEvent, Sport
from services.persistence_consumer.errors import (
    PoisonPillError,
    RetryBudgetExhaustedError,
    TransientError,
    classify_exception,
)
from services.persistence_consumer.handler import (
    MessageHandler,
    MessageOutcome,
    _validate_spine,
)


def _build_valid_pitch() -> MLBPitchEvent:
    pitch = MLBPitchEvent()
    pitch.spine.event_id = "mlb:745612:pitch:0:1"
    pitch.spine.event_type = EventType.EVENT_TYPE_PITCH
    pitch.spine.sport = Sport.SPORT_MLB
    pitch.spine.game_pk = "745612"
    now = datetime.now(UTC)
    pitch.spine.event_time.FromDatetime(now)
    pitch.spine.source_time.FromDatetime(now)
    pitch.spine.ingest_time.FromDatetime(now)
    pitch.at_bat_index = 0
    pitch.pitch_number = 1
    return pitch


# ---------- classify_exception ----------


class TestClassifyException:
    def test_value_error_is_poison_pill(self) -> None:
        assert classify_exception(ValueError("x")) is PoisonPillError

    def test_unknown_is_transient(self) -> None:
        assert classify_exception(RuntimeError("blip")) is TransientError

    def test_explicit_poison_pill_passthrough(self) -> None:
        assert classify_exception(PoisonPillError("x")) is PoisonPillError

    def test_explicit_transient_passthrough(self) -> None:
        assert classify_exception(TransientError("x")) is TransientError

    def test_decode_error_is_poison_pill(self) -> None:
        from google.protobuf.message import DecodeError

        assert classify_exception(DecodeError("bad bytes")) is PoisonPillError


# ---------- _validate_spine ----------


class TestValidateSpine:
    def test_valid_pitch_passes(self) -> None:
        _validate_spine(_build_valid_pitch())

    def test_missing_event_id_raises(self) -> None:
        pitch = _build_valid_pitch()
        pitch.spine.event_id = ""
        with pytest.raises(ValueError, match="event_id"):
            _validate_spine(pitch)

    def test_missing_game_pk_raises(self) -> None:
        pitch = _build_valid_pitch()
        pitch.spine.game_pk = ""
        with pytest.raises(ValueError, match="game_pk"):
            _validate_spine(pitch)

    def test_unspecified_sport_raises(self) -> None:
        pitch = _build_valid_pitch()
        pitch.spine.sport = Sport.SPORT_UNSPECIFIED
        with pytest.raises(ValueError, match="sport"):
            _validate_spine(pitch)


# ---------- MessageHandler ----------


class TestMessageHandler:
    @pytest.fixture
    def store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def handler(self, store: AsyncMock) -> MessageHandler:
        # Zero-delay retry schedule so tests stay fast.
        return MessageHandler(store, retry_delays=(0.0, 0.0, 0.0))

    async def test_valid_pitch_writes_new(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        store.insert_pitch.return_value = True
        outcome = await handler.handle(_build_valid_pitch().SerializeToString())
        assert outcome == MessageOutcome.WRITTEN
        assert store.insert_pitch.await_count == 1

    async def test_duplicate_pitch_is_silent(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        store.insert_pitch.return_value = False
        outcome = await handler.handle(_build_valid_pitch().SerializeToString())
        assert outcome == MessageOutcome.DUPLICATE

    async def test_invalid_proto_raises_poison_pill(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        with pytest.raises(PoisonPillError, match="proto decode"):
            await handler.handle(b"\xff\xff\xff\xff not proto")
        store.insert_pitch.assert_not_awaited()

    async def test_missing_spine_field_raises_poison_pill(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        pitch = _build_valid_pitch()
        pitch.spine.event_id = ""
        with pytest.raises(PoisonPillError, match="spine validation"):
            await handler.handle(pitch.SerializeToString())
        store.insert_pitch.assert_not_awaited()

    async def test_transient_retries_then_succeeds(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        # Fail twice, then succeed.
        store.insert_pitch.side_effect = [
            RuntimeError("db blip 1"),
            RuntimeError("db blip 2"),
            True,
        ]
        outcome = await handler.handle(_build_valid_pitch().SerializeToString())
        assert outcome == MessageOutcome.WRITTEN
        assert store.insert_pitch.await_count == 3

    async def test_transient_exhausts_budget(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        store.insert_pitch.side_effect = RuntimeError("forever")
        with pytest.raises(RetryBudgetExhaustedError):
            await handler.handle(_build_valid_pitch().SerializeToString())
        # 1 initial + 3 retries (len(retry_delays) == 3)
        assert store.insert_pitch.await_count == 4

    async def test_write_path_value_error_is_poison_pill(
        self, handler: MessageHandler, store: AsyncMock
    ) -> None:
        """A ValueError from the store shouldn't retry — it's caller-side bad data."""
        store.insert_pitch.side_effect = ValueError("bad column")
        with pytest.raises(PoisonPillError, match="write-path poison pill"):
            await handler.handle(_build_valid_pitch().SerializeToString())
        assert store.insert_pitch.await_count == 1


# ---------- Integration ----------


@pytest.mark.integration
class TestIntegration:
    """Full-loop test: real Kafka + Postgres via testcontainers.

    Skipped by default. Run with: `pytest -m integration`.
    """

    async def test_end_to_end_write(self, tmp_path: Any) -> None:
        import pathlib

        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        from testcontainers.kafka import KafkaContainer
        from testcontainers.postgres import PostgresContainer

        from services.persistence_consumer.consumer import PersistenceConsumer
        from services.persistence_consumer.dlq import DLQPublisher
        from services.persistence_consumer.handler import MessageHandler
        from services.persistence_consumer.storage import EventStore

        topic = "mlb.events.raw"
        dlq_topic = "mlb.events.raw.dlq"

        with PostgresContainer("postgres:16-alpine") as pg, KafkaContainer() as kafka:
            bootstrap = kafka.get_bootstrap_server()
            db_url = pg.get_connection_url(driver=None)

            store = await EventStore.connect(db_url)
            migration = pathlib.Path("migrations/001_initial.sql").read_text()
            async with store._pool.acquire() as conn:  # type: ignore[attr-defined]
                await conn.execute(migration)

            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id="persistence-consumer-test",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            dlq_producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap,
                enable_idempotence=True,
                acks="all",
            )
            producer = AIOKafkaProducer(bootstrap_servers=bootstrap)

            handler = MessageHandler(store)
            dlq = DLQPublisher(dlq_producer, dlq_topic)
            svc = PersistenceConsumer(
                consumer=consumer,
                producer=dlq_producer,
                handler=handler,
                dlq=dlq,
                source_topic=topic,
            )

            try:
                await svc.start()
                await producer.start()

                pitch = _build_valid_pitch()
                await producer.send_and_wait(
                    topic,
                    value=pitch.SerializeToString(),
                    key=pitch.spine.game_pk.encode(),
                )

                stop_event = asyncio.Event()
                run_task = asyncio.create_task(svc.run(stop_event))

                async def _wait_for_row() -> None:
                    for _ in range(60):
                        async with store._pool.acquire() as conn:  # type: ignore[attr-defined]
                            row = await conn.fetchrow(
                                "SELECT event_id FROM events WHERE event_id = $1",
                                pitch.spine.event_id,
                            )
                        if row is not None:
                            return
                        await asyncio.sleep(0.5)
                    raise AssertionError("row never landed")

                await asyncio.wait_for(_wait_for_row(), timeout=30.0)

                stop_event.set()
                await run_task
            finally:
                await producer.stop()
                await svc.stop()
                await store.close()
