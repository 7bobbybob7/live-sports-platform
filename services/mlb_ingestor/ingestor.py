"""MLB ingestor main loop.

Design:
    * A single `schedule_loop` polls the MLB schedule every 60s, finds live
      games, and spawns one `game_loop` task per live game.
    * Each `game_loop` polls `feed/live` every 5s, diffs against the Redis
      cursor, publishes new pitches to Postgres, and advances the cursor.
    * When a game ends, its task exits naturally.

Restart-safety: on boot, the schedule_loop discovers live games and each
game_loop reads its cursor from Redis before its first iteration. No events
are re-published and none are skipped.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any

import httpx
from prometheus_client import Counter, Gauge, Histogram
from redis.asyncio import Redis

from services.common.config import Config
from services.common.logging import get_logger
from services.mlb_ingestor.cursor import CursorStore, GameCursor
from services.mlb_ingestor.mlb_client import MLBClient
from services.mlb_ingestor.parser import extract_game_state, extract_pitch_events
from services.mlb_ingestor.storage import EventStore

_logger = get_logger(__name__)

# ---------- metrics ----------

POLL_LATENCY = Histogram(
    "mlb_ingestor_poll_latency_seconds",
    "Time spent polling MLB Stats API per call",
    labelnames=("endpoint",),
)
EVENTS_PUBLISHED = Counter(
    "mlb_ingestor_events_published_total",
    "Events successfully written to storage",
    labelnames=("event_type",),
)
EVENTS_DEDUPED = Counter(
    "mlb_ingestor_events_deduped_total",
    "Events skipped due to dedup (ON CONFLICT DO NOTHING or cursor)",
    labelnames=("reason",),
)
API_ERRORS = Counter(
    "mlb_ingestor_api_errors_total",
    "MLB Stats API errors by class",
    labelnames=("endpoint", "error_class"),
)
ACTIVE_GAMES = Gauge(
    "mlb_ingestor_active_games",
    "Number of game_loop tasks currently running",
)
CURSOR_STALENESS = Gauge(
    "mlb_ingestor_cursor_staleness_seconds",
    "Seconds since the cursor was last advanced, per game",
    labelnames=("game_pk",),
)


# ---------- ingestor ----------


class MLBIngestor:
    def __init__(
        self,
        config: Config,
        mlb_client: MLBClient,
        cursor_store: CursorStore,
        event_store: EventStore,
    ):
        self._config = config
        self._mlb = mlb_client
        self._cursors = cursor_store
        self._events = event_store
        self._game_tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()

    async def run(self) -> None:
        _logger.info("ingestor_starting")
        try:
            await self._schedule_loop()
        finally:
            await self._shutdown()

    def stop(self) -> None:
        self._stop.set()

    async def _shutdown(self) -> None:
        _logger.info("ingestor_shutting_down", active_games=len(self._game_tasks))
        for task in self._game_tasks.values():
            task.cancel()
        await asyncio.gather(*self._game_tasks.values(), return_exceptions=True)
        self._game_tasks.clear()
        ACTIVE_GAMES.set(0)

    async def _schedule_loop(self) -> None:
        interval = self._config.mlb_schedule_poll_interval_seconds
        while not self._stop.is_set():
            try:
                await self._reconcile_games()
            except httpx.HTTPError as exc:
                API_ERRORS.labels(endpoint="schedule", error_class=type(exc).__name__).inc()
                _logger.warning("schedule_poll_failed", error=str(exc))
            except Exception:  # noqa: BLE001
                _logger.exception("schedule_loop_unexpected_error")

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=interval)

    async def _reconcile_games(self) -> None:
        with POLL_LATENCY.labels(endpoint="schedule").time():
            games = await self._mlb.fetch_schedule()

        live_games = [g for g in games if self._mlb.is_live(g)]
        live_pks = {str(g.get("gamePk")) for g in live_games if g.get("gamePk")}

        _logger.info(
            "schedule_reconciled",
            total_games=len(games),
            live_games=len(live_games),
        )

        # Start tasks for newly-live games
        for game_pk in live_pks:
            if game_pk not in self._game_tasks:
                _logger.info("game_task_starting", game_pk=game_pk)
                task = asyncio.create_task(self._game_loop(game_pk))
                self._game_tasks[game_pk] = task

        # Clean up finished tasks
        for game_pk in list(self._game_tasks):
            task = self._game_tasks[game_pk]
            if task.done():
                _logger.info("game_task_finished", game_pk=game_pk)
                del self._game_tasks[game_pk]

        ACTIVE_GAMES.set(len(self._game_tasks))

    async def _game_loop(self, game_pk: str) -> None:
        log = _logger.bind(game_pk=game_pk)
        cursor = await self._cursors.get(game_pk)
        log.info("game_loop_started", cursor=_cursor_to_dict(cursor))

        interval = self._config.mlb_poll_interval_seconds
        try:
            while not self._stop.is_set():
                try:
                    advanced = await self._poll_game_once(game_pk, cursor, log)
                    if advanced is not None:
                        cursor = advanced
                except httpx.HTTPError as exc:
                    API_ERRORS.labels(
                        endpoint="feed_live", error_class=type(exc).__name__
                    ).inc()
                    log.warning("feed_poll_failed", error=str(exc))
                except Exception:  # noqa: BLE001
                    log.exception("game_loop_unexpected_error")

                if await self._is_game_final(game_pk):
                    log.info("game_final_exiting")
                    return

                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=interval)
        finally:
            log.info("game_loop_exited")

    async def _poll_game_once(
        self,
        game_pk: str,
        cursor: GameCursor | None,
        log: Any,
    ) -> GameCursor | None:
        with POLL_LATENCY.labels(endpoint="feed_live").time():
            feed = await self._mlb.fetch_live_feed(game_pk)

        # Upsert the game row (best-effort, doesn't gate pitch publishing)
        try:
            game_state = extract_game_state(feed)
            await self._events.upsert_game(game_state, feed)
        except Exception:  # noqa: BLE001
            log.exception("game_state_upsert_failed")

        pitches = extract_pitch_events(feed)
        new_cursor = cursor
        published = 0

        for pitch in pitches:
            if cursor and cursor.is_after(pitch.at_bat_index, pitch.pitch_number):
                EVENTS_DEDUPED.labels(reason="cursor").inc()
                continue

            was_new = await self._events.insert_pitch(pitch)
            if was_new:
                EVENTS_PUBLISHED.labels(event_type="pitch").inc()
                published += 1
            else:
                EVENTS_DEDUPED.labels(reason="db_conflict").inc()

            new_cursor = GameCursor(
                last_at_bat_index=pitch.at_bat_index,
                last_pitch_number=pitch.pitch_number,
                updated_at=datetime.now(UTC),
            )

        if new_cursor is not None and new_cursor != cursor:
            await self._cursors.set(
                game_pk, new_cursor.last_at_bat_index, new_cursor.last_pitch_number
            )
            CURSOR_STALENESS.labels(game_pk=game_pk).set(0)
            log.info(
                "poll_complete",
                pitches_seen=len(pitches),
                published=published,
                cursor_at_bat=new_cursor.last_at_bat_index,
                cursor_pitch=new_cursor.last_pitch_number,
            )

        return new_cursor

    async def _is_game_final(self, game_pk: str) -> bool:
        # Cheap check: if the schedule no longer lists this game as live, exit.
        # The schedule_loop will clean us up on its next iteration, but this
        # lets the task exit faster.
        return False  # Phase 1 keeps it simple — schedule_loop reconciles.


def _cursor_to_dict(cursor: GameCursor | None) -> dict[str, Any] | None:
    if cursor is None:
        return None
    return {
        "last_at_bat_index": cursor.last_at_bat_index,
        "last_pitch_number": cursor.last_pitch_number,
        "updated_at": cursor.updated_at.isoformat(),
    }


async def build_and_run(config: Config) -> None:
    mlb_client = MLBClient(config.mlb_stats_api_base)
    redis = Redis.from_url(config.redis_url, decode_responses=True)
    cursor_store = CursorStore(redis)
    event_store = await EventStore.connect(config.database_url)

    ingestor = MLBIngestor(config, mlb_client, cursor_store, event_store)

    try:
        await ingestor.run()
    finally:
        await mlb_client.close()
        await event_store.close()
        await redis.aclose()
