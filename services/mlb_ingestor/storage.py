"""Postgres writer for the MLB ingestor.

Phase 1 writes events directly to Postgres. Phase 3 step 6 splits this:
the ingestor produces to Kafka and the persistence-consumer writes here.

Dedup happens at the DB via `events.event_id` PK — ON CONFLICT DO NOTHING.
"""

from __future__ import annotations

import json
from datetime import UTC
from typing import Any

import asyncpg

from schemas.mlb import EventType, MLBGameState, MLBPitchEvent, Sport
from schemas.proto_utils import enum_int_to_name, proto_to_payload_dict
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

    async def insert_pitch(self, event: MLBPitchEvent) -> bool:
        """Insert a pitch event. Returns True if it was new, False if dedup'd."""
        query = """
        INSERT INTO events (
            event_id, event_type, sport, game_pk,
            event_time, source_time, ingest_time, payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """
        payload = proto_to_payload_dict(event)
        spine = event.spine
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                spine.event_id,
                enum_int_to_name(EventType.DESCRIPTOR, spine.event_type),
                enum_int_to_name(Sport.DESCRIPTOR, spine.sport),
                spine.game_pk,
                spine.event_time.ToDatetime(tzinfo=UTC),
                spine.source_time.ToDatetime(tzinfo=UTC),
                spine.ingest_time.ToDatetime(tzinfo=UTC),
                json.dumps(payload),
            )
        return row is not None

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
