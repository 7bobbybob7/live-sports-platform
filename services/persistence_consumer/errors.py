"""Error taxonomy for the persistence consumer.

Per the retry policy in docs/DESIGN.md:

- Transient:  in-process exponential backoff, capped attempts + wall-clock.
              If still failing, pause the partition and alert. Never DLQ.
- PoisonPill: unparseable or invalid data that cannot succeed under retry.
              Immediate DLQ, commit offset, move on.
- Duplicate:  not an error. `ON CONFLICT DO NOTHING` eats it silently at the DB.

Wrapping failures in these classes is how `handler.py` communicates outcome to
`consumer.py`. Plain exceptions from below (asyncpg, aiokafka, etc.) are
translated to these classes by `classify_exception` at the boundary.
"""

from __future__ import annotations


class PersistenceConsumerError(Exception):
    """Base class for errors the consumer handles specially."""


class TransientError(PersistenceConsumerError):
    """A retriable failure — DB blip, timeout, etc."""


class PoisonPillError(PersistenceConsumerError):
    """A non-retriable failure — bad bytes, schema violation."""


class RetryBudgetExhaustedError(PersistenceConsumerError):
    """Transient failure that persisted past the retry budget.

    The consumer pauses the partition on this and leaves the message uncommitted,
    so it will be re-delivered when the partition is resumed.
    """

    def __init__(self, last_error: Exception):
        super().__init__(f"retry budget exhausted: {last_error!r}")
        self.last_error = last_error


def classify_exception(exc: BaseException) -> type[PersistenceConsumerError]:
    """Map a raw exception to a handled error class.

    Unknown failures default to TransientError — being conservative avoids
    shipping something malformed to the DLQ the first time a new upstream
    library surfaces a novel exception class.
    """
    if isinstance(exc, PoisonPillError):
        return PoisonPillError
    if isinstance(exc, TransientError):
        return TransientError

    # Poison-pill shapes: protobuf decode, our spine validation.
    # Import lazily to avoid a hard dep on protobuf at module import time.
    try:
        from google.protobuf.message import DecodeError

        if isinstance(exc, DecodeError):
            return PoisonPillError
    except ImportError:  # pragma: no cover
        pass

    if isinstance(exc, ValueError):
        # Our `_validate_spine` raises ValueError on missing required fields.
        return PoisonPillError

    return TransientError
