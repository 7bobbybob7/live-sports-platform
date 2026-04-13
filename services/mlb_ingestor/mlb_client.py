"""MLB Stats API client.

The MLB Stats API is public, undocumented, and rate-limited only by "be
reasonable." We use httpx.AsyncClient with connection pooling and a short
timeout. Errors are logged and re-raised; the caller decides retry/backoff
policy.

Endpoints we care about in Phase 1:
    GET /schedule?sportId=1&date=YYYY-MM-DD
        → list of games for a date; used to find live games
    GET /game/{gamePk}/feed/live
        → full game state including every play and every pitch event
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from services.common.logging import get_logger

_logger = get_logger(__name__)

_LIVE_STATUS_CODES = {"I", "IR", "IH", "IO", "IP"}  # MLB "in progress" family


class MLBClient:
    def __init__(self, base_url: str, timeout_seconds: float = 10.0):
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"User-Agent": "live-sports-platform/0.1 (+github.com/7bobbybob7)"},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_schedule(self, date: datetime | None = None) -> list[dict[str, Any]]:
        """Return the list of game dicts scheduled for `date` (UTC today by default)."""
        d = (date or datetime.now(UTC)).strftime("%Y-%m-%d")
        url = f"{self._base}/schedule"
        params = {"sportId": 1, "date": d}
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        body = resp.json()
        games: list[dict[str, Any]] = []
        for date_block in body.get("dates", []):
            games.extend(date_block.get("games", []))
        return games

    async def fetch_live_feed(self, game_pk: int | str) -> dict[str, Any]:
        """Return the full live feed for a game."""
        url = f"{self._base.replace('/v1', '/v1.1')}/game/{game_pk}/feed/live"
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def is_live(game: dict[str, Any]) -> bool:
        status = game.get("status", {})
        code = status.get("statusCode") or ""
        abstract = status.get("abstractGameState") or ""
        return code in _LIVE_STATUS_CODES or abstract == "Live"
