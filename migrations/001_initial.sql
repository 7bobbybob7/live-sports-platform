-- Phase 1 schema. See sports-platform-prd.md for the data model design.
--
-- Design notes:
--   * event_id is PRIMARY KEY → hard dedup at the write boundary.
--   * (game_pk, event_time, event_id) composite index → replay in event-time
--     order with deterministic tiebreak. Build this from day one; retrofitting
--     onto a 50M-row table is painful.
--   * Avoid over-indexing — bulk backfill performance matters in Phase 3.

BEGIN;

CREATE TABLE IF NOT EXISTS games (
    game_pk      TEXT PRIMARY KEY,
    sport        TEXT NOT NULL,
    status       TEXT,
    home_team    TEXT,
    away_team    TEXT,
    start_time   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload      JSONB
);

CREATE INDEX IF NOT EXISTS games_sport_status_idx
    ON games (sport, status);

CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    sport        TEXT NOT NULL,
    game_pk      TEXT NOT NULL,

    event_time   TIMESTAMPTZ NOT NULL,
    source_time  TIMESTAMPTZ NOT NULL,
    ingest_time  TIMESTAMPTZ NOT NULL DEFAULT now(),

    payload      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS events_game_time_idx
    ON events (game_pk, event_time, event_id);

CREATE INDEX IF NOT EXISTS events_event_time_idx
    ON events (event_time);

CREATE INDEX IF NOT EXISTS events_type_idx
    ON events (event_type);

COMMIT;
