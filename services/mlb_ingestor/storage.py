"""Postgres writer for game metadata.

Pitch events flow through Kafka -> persistence-consumer; this module retains
only the games-table upsert. Dedup of events happens downstream at the DB
via `events.event_id` PK.
"""

from __future__ import annotations

import json
from datetime import UTC
from typing import Any

import asyncpg

from schemas.mlb import MLBGameState, Sport
from schemas.proto_utils import enum_int_to_name
from services.common.logging import get_logger

_logger = get_logger(__name__)


class EventStore:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> EventStore:
        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
        assert pool is not None
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def upsert_game(self, game: MLBGameState, raw: dict[str, Any]) -> None:
        query = """
        INSERT INTO games (
            game_pk, sport, status, home_team, away_team, start_time, updated_at, payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, now(), $7::jsonb)
        ON CONFLICT (game_pk) DO UPDATE SET
            status     = EXCLUDED.status,
            home_team  = EXCLUDED.home_team,
            away_team  = EXCLUDED.away_team,
            start_time = EXCLUDED.start_time,
            updated_at = now(),
            payload    = EXCLUDED.payload
        """
        spine = game.spine
        async with self._pool.acquire() as conn:
            await conn.execute(
                query,
                spine.game_pk,
                enum_int_to_name(Sport.DESCRIPTOR, spine.sport),
                game.status or None,
                game.home_team or None,
                game.away_team or None,
                spine.event_time.ToDatetime(tzinfo=UTC),
                json.dumps(raw),
            )
