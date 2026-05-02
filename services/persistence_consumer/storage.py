"""Postgres writer for the persistence consumer.

Writes enriched proto events to the `events` table. Uses `ON CONFLICT
(event_id) DO NOTHING` so re-deliveries from Kafka (e.g. after an offset
rewind) don't produce duplicate rows — this is the idempotency story.

Duplicated temporarily from services/mlb_ingestor/storage.py. Step 6 of
Phase 3 removes the ingestor copy once the ingestor writes to Kafka only.
"""

from __future__ import annotations

import json
from datetime import UTC

import asyncpg

from schemas.mlb import EventType, MLBPitchEvent, Sport
from schemas.proto_utils import enum_int_to_name, proto_to_payload_dict


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
        """Insert a pitch event. Returns True if new, False if dedup'd."""
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
