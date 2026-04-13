"""Thin Postgres repository for the query API.

Queries are raw SQL by design — we control every index this hits and don't
want an ORM hiding access patterns from us. Every query here corresponds to
an index defined in migrations/001_initial.sql.
"""

from __future__ import annotations

from typing import Any

import asyncpg


class QueryRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> QueryRepository:
        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
        assert pool is not None
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def healthcheck(self) -> bool:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
        return result == 1

    async def latest_pitch_per_active_game(self) -> list[dict[str, Any]]:
        """One row per active MLB game, with that game's most recent pitch."""
        query = """
        WITH active_games AS (
            SELECT game_pk, home_team, away_team, status, updated_at
            FROM games
            WHERE sport = 'mlb'
              AND status NOT IN ('Final', 'Game Over', 'Completed Early', 'Postponed', 'Cancelled')
        ),
        latest AS (
            SELECT DISTINCT ON (e.game_pk)
                e.game_pk,
                e.event_id,
                e.event_time,
                e.source_time,
                e.ingest_time,
                e.payload
            FROM events e
            JOIN active_games g ON g.game_pk = e.game_pk
            WHERE e.sport = 'mlb' AND e.event_type = 'pitch'
            ORDER BY e.game_pk, e.event_time DESC, e.event_id DESC
        )
        SELECT
            g.game_pk,
            g.home_team,
            g.away_team,
            g.status,
            l.event_id,
            l.event_time,
            l.source_time,
            l.ingest_time,
            l.payload
        FROM active_games g
        LEFT JOIN latest l ON l.game_pk = g.game_pk
        ORDER BY g.updated_at DESC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query)
        return [dict(row) for row in rows]

    async def pitches_for_game(
        self, game_pk: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        query = """
        SELECT event_id, event_time, source_time, ingest_time, payload
        FROM events
        WHERE game_pk = $1 AND event_type = 'pitch' AND sport = 'mlb'
        ORDER BY event_time DESC, event_id DESC
        LIMIT $2
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, game_pk, limit)
        return [dict(row) for row in rows]
