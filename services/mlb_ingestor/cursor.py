"""Per-game ingestor cursor, persisted to Redis.

The cursor tracks the last `(at_bat_index, pitch_number)` we successfully
published for a given game. On startup, the ingestor reads the cursor and
resumes from there — no duplicates, no gaps.

Cursor loss is recoverable: consumer-side dedup by `event_id` catches any
re-published duplicates.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from redis.asyncio import Redis

from services.common.logging import get_logger

_logger = get_logger(__name__)

_CURSOR_KEY_PREFIX = "ingestor:cursor:mlb:"


@dataclass(frozen=True)
class GameCursor:
    last_at_bat_index: int
    last_pitch_number: int
    updated_at: datetime

    def is_after(self, at_bat_index: int, pitch_number: int) -> bool:
        """Return True if (at_bat_index, pitch_number) is at or before this cursor."""
        if at_bat_index < self.last_at_bat_index:
            return True
        if at_bat_index == self.last_at_bat_index:
            return pitch_number <= self.last_pitch_number
        return False


class CursorStore:
    def __init__(self, redis: Redis):
        self._redis = redis

    @staticmethod
    def _key(game_pk: int | str) -> str:
        return f"{_CURSOR_KEY_PREFIX}{game_pk}"

    async def get(self, game_pk: int | str) -> GameCursor | None:
        raw = await self._redis.get(self._key(game_pk))
        if raw is None:
            return None
        data = json.loads(raw)
        return GameCursor(
            last_at_bat_index=int(data["last_at_bat_index"]),
            last_pitch_number=int(data["last_pitch_number"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )

    async def set(
        self, game_pk: int | str, at_bat_index: int, pitch_number: int
    ) -> None:
        cursor = {
            "last_at_bat_index": at_bat_index,
            "last_pitch_number": pitch_number,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        await self._redis.set(self._key(game_pk), json.dumps(cursor))

    async def delete(self, game_pk: int | str) -> None:
        await self._redis.delete(self._key(game_pk))
