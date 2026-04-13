# Live Sports Platform

Distributed streaming platform for live MLB and near-live NFL events.

Full design: [sports-platform-prd.md](sports-platform-prd.md)

## Phase 1 — Local foundation

What's running in Phase 1:

- **mlb-ingestor** — polls MLB Stats API per active game, writes pitches to Postgres, persists cursor to Redis
- **query-api** — FastAPI service exposing recent pitches via REST
- **postgres** — durable event log
- **redis** — hot cache + ingestor cursor

No Kafka yet — that's Phase 2. No enrichment yet — that's Phase 3.

## Quick start

Requires Python 3.12+, Docker, and [uv](https://github.com/astral-sh/uv).

```bash
# 1. Copy env and fill in defaults
cp .env.example .env.local

# 2. Start Postgres + Redis
docker compose up -d

# 3. Install Python deps
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# 4. Apply migrations
psql $DATABASE_URL -f migrations/001_initial.sql

# 5. Run the ingestor (in one terminal)
python -m services.mlb_ingestor

# 6. Run the query API (in another terminal)
python -m services.query_api

# 7. Check it's working (requires a live MLB game in progress)
curl http://localhost:8080/games/latest
```

## Tests

```bash
pytest
```

## Repo layout

```
schemas/          Pydantic models + event ID derivation (single source of truth)
services/         One directory per runnable service
  common/         Shared logging, metrics, Sentry setup
  mlb_ingestor/   MLB Stats API ingestor
  query_api/      FastAPI REST API
migrations/       SQL migrations
tests/            Unit and integration tests
docker-compose.yml  Local Postgres + Redis
```

## Observability (Phase 1)

- Every service exposes `/metrics` in Prometheus format.
- Structured JSON logs with correlation IDs via `structlog`.
- Optional Sentry error tracking (set `SENTRY_DSN` in `.env.local`).

Prometheus + Grafana dashboards arrive in Phase 2. OpenTelemetry tracing in Phase 3.
