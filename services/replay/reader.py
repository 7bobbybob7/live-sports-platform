"""Stream historical events from Postgres ordered by (event_time, event_id)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import asyncpg

from schemas.mlb import MLBPitchEvent
from schemas.proto_utils import payload_dict_to_proto


class EventReader:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> EventReader:
        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=2)
        assert pool is not None
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def stream_pitches(
        self,
        game_pk: str,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> AsyncIterator[MLBPitchEvent]:
        """Yield pitch events for a game in (event_time, event_id) order."""
        query = """
        SELECT event_time, payload
        FROM events
        WHERE game_pk = $1
          AND event_type = 'pitch'
          AND ($2::timestamptz IS NULL OR event_time >= $2)
          AND ($3::timestamptz IS NULL OR event_time <= $3)
        ORDER BY event_time, event_id
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                async for row in conn.cursor(query, game_pk, since, until):
                    yield _row_to_pitch(row["payload"])


def _row_to_pitch(payload_raw: Any) -> MLBPitchEvent:
    payload = (
        json.loads(payload_raw) if isinstance(payload_raw, str) else dict(payload_raw)
    )
    pitch = MLBPitchEvent()
    payload_dict_to_proto(pitch, payload)
    return pitch
