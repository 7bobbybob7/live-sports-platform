"""Per-message processing: deserialize, validate, write.

Kept separate from the consumer loop so the retry policy can be unit-tested
against mocks without a real Kafka broker.
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Protocol

from google.protobuf.message import DecodeError

from schemas.mlb import MLBPitchEvent
from services.persistence_consumer.errors import (
    PoisonPillError,
    RetryBudgetExhaustedError,
    TransientError,
    classify_exception,
)

# 5 attempts, delays sum to 18.5s — stays inside the 30s budget from the spec.
_RETRY_DELAYS_SECONDS: tuple[float, ...] = (0.5, 1.0, 2.0, 5.0, 10.0)


class MessageOutcome(Enum):
    WRITTEN = "written"
    DUPLICATE = "duplicate"


class _Store(Protocol):
    async def insert_pitch(self, event: MLBPitchEvent) -> bool: ...


def _validate_spine(event: MLBPitchEvent) -> None:
    """Re-establish required-field semantics proto3 lost in translation."""
    spine = event.spine
    if not spine.event_id:
        raise ValueError("spine.event_id is empty")
    if spine.event_type == 0:
        raise ValueError("spine.event_type is unspecified")
    if spine.sport == 0:
        raise ValueError("spine.sport is unspecified")
    if not spine.game_pk:
        raise ValueError("spine.game_pk is empty")
    for ts_field in ("event_time", "source_time", "ingest_time"):
        if not spine.HasField(ts_field):
            raise ValueError(f"spine.{ts_field} is missing")


class MessageHandler:
    """Single-message handler with retry. Stateless across calls."""

    def __init__(self, store: _Store, retry_delays: tuple[float, ...] = _RETRY_DELAYS_SECONDS):
        self._store = store
        self._retry_delays = retry_delays

    async def handle(self, payload: bytes) -> MessageOutcome:
        """Process one message. Raises PoisonPillError or RetryBudgetExhaustedError.

        Caller supplies raw bytes; we deserialize here so decode errors are
        classified as poison pills, not transient.
        """
        try:
            event = MLBPitchEvent.FromString(payload)
        except DecodeError as exc:
            raise PoisonPillError(f"proto decode failed: {exc}") from exc

        try:
            _validate_spine(event)
        except ValueError as exc:
            raise PoisonPillError(f"spine validation failed: {exc}") from exc

        return await self._write_with_retry(event)

    async def _write_with_retry(self, event: MLBPitchEvent) -> MessageOutcome:
        last_exc: Exception | None = None
        attempts_remaining = len(self._retry_delays) + 1  # initial attempt + retries
        for attempt_idx in range(attempts_remaining):
            try:
                was_new = await self._store.insert_pitch(event)
                return MessageOutcome.WRITTEN if was_new else MessageOutcome.DUPLICATE
            except Exception as exc:  # noqa: BLE001
                cls = classify_exception(exc)
                if cls is PoisonPillError:
                    raise PoisonPillError(f"write-path poison pill: {exc!r}") from exc
                if cls is TransientError:
                    last_exc = exc
                    if attempt_idx < len(self._retry_delays):
                        await asyncio.sleep(self._retry_delays[attempt_idx])
                        continue
                    break
                # classify_exception should only ever return one of the two above
                raise  # pragma: no cover
        assert last_exc is not None
        raise RetryBudgetExhaustedError(last_exc)
