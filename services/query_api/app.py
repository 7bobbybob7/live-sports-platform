"""FastAPI query service.

Phase 1 endpoints:
    GET  /healthz           Liveness
    GET  /readyz            Readiness (checks DB)
    GET  /games/latest      One row per live game + that game's most recent pitch
    GET  /games/{pk}/pitches Recent pitches for a given game

`/metrics` is served on a separate port by prometheus_client, not by the
FastAPI app — matches how Prometheus scrapes microservices.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import ORJSONResponse

from services.common.config import Config
from services.common.logging import configure_logging, get_logger
from services.common.metrics import start_metrics_server
from services.common.sentry import init_sentry
from services.query_api.db import QueryRepository

_logger = get_logger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    config = Config.load()
    configure_logging(config.log_level)
    init_sentry(config.sentry_dsn, config.sentry_environment, "query-api")
    start_metrics_server(config.metrics_port_query_api)

    repo = await QueryRepository.connect(config.database_url)
    app.state.repo = repo
    app.state.config = config
    _logger.info("query_api_ready", port=config.query_api_port)

    try:
        yield
    finally:
        await repo.close()
        _logger.info("query_api_shutdown")


app = FastAPI(
    title="Live Sports Platform Query API",
    version="0.1.0",
    default_response_class=ORJSONResponse,
    lifespan=_lifespan,
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    repo: QueryRepository = app.state.repo
    try:
        ok = await repo.healthcheck()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"db_error: {exc}") from exc
    if not ok:
        raise HTTPException(status_code=503, detail="db_unhealthy")
    return {"status": "ready"}


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """asyncpg returns datetimes and JSONB strings — coerce to JSON-ready shapes."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k == "payload" and isinstance(v, str):
            out[k] = json.loads(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


@app.get("/games/latest")
async def games_latest() -> dict[str, Any]:
    repo: QueryRepository = app.state.repo
    rows = await repo.latest_pitch_per_active_game()
    return {"games": [_normalize_row(r) for r in rows], "count": len(rows)}


@app.get("/games/{game_pk}/pitches")
async def pitches_for_game(
    game_pk: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    repo: QueryRepository = app.state.repo
    rows = await repo.pitches_for_game(game_pk, limit)
    return {
        "game_pk": game_pk,
        "count": len(rows),
        "pitches": [_normalize_row(r) for r in rows],
    }
