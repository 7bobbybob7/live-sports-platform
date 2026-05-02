"""Per-game rolling context kept in Redis.

PRD: enricher maintains rolling per-game context and writes hot state to
Redis so query-api can read current-state without a DB hop.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from redis.asyncio import Redis


@dataclass(frozen=True)
class GameContext:
    home_score: int = 0
    away_score: int = 0
    runner_on_first: bool = False
    runner_on_second: bool = False
    runner_on_third: bool = False
    inning: int = 0
    outs: int = 0


_KEY_PREFIX = "enricher:state:"


class GameStateStore:
    def __init__(self, redis: Redis):
        self._redis = redis

    @staticmethod
    def _key(game_pk: str) -> str:
        return f"{_KEY_PREFIX}{game_pk}"

    async def get(self, game_pk: str) -> GameContext:
        raw = await self._redis.get(self._key(game_pk))
        if raw is None:
            return GameContext()
        return GameContext(**json.loads(raw))

    async def set(self, game_pk: str, ctx: GameContext) -> None:
        await self._redis.set(self._key(game_pk), json.dumps(asdict(ctx)))
