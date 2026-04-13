"""Parse MLB Stats API live-feed payloads into typed events.

Keeping this isolated from the ingestor loop makes it easy to unit-test
against fixture payloads without a network or database.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from schemas.event_ids import derive_pitch_id
from schemas.mlb import MLBGameState, MLBPitchEvent


def _parse_mlb_time(value: str | None) -> datetime | None:
    if not value:
        return None
    # MLB timestamps are ISO 8601, sometimes with 'Z'
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def extract_game_state(feed: dict[str, Any], now: datetime | None = None) -> MLBGameState:
    """Pull high-level game state from a live feed payload."""
    now = now or datetime.now(UTC)
    game_data = feed.get("gameData", {})
    live_data = feed.get("liveData", {})

    game_pk = str(feed.get("gamePk") or game_data.get("game", {}).get("pk") or "")
    status = game_data.get("status", {}).get("detailedState")
    teams = game_data.get("teams", {})
    home_team = teams.get("home", {}).get("name")
    away_team = teams.get("away", {}).get("name")

    linescore = live_data.get("linescore", {})
    home_score = linescore.get("teams", {}).get("home", {}).get("runs")
    away_score = linescore.get("teams", {}).get("away", {}).get("runs")
    inning = linescore.get("currentInning")
    inning_half = (linescore.get("inningHalf") or "").lower() or None

    start_time = _parse_mlb_time(
        game_data.get("datetime", {}).get("dateTime")
    ) or now

    metadata_ts = _parse_mlb_time(feed.get("metaData", {}).get("timeStamp"))
    source_time = metadata_ts or now

    return MLBGameState(
        event_id=f"mlb:{game_pk}:game:state:{metadata_ts.isoformat() if metadata_ts else now.isoformat()}",
        game_pk=game_pk,
        event_time=start_time,
        source_time=source_time,
        ingest_time=now,
        status=status,
        home_team=home_team,
        away_team=away_team,
        home_score=home_score,
        away_score=away_score,
        inning=inning,
        inning_half=inning_half,
        raw_payload=None,  # keep the event small; full feed stored on games row
    )


def extract_pitch_events(
    feed: dict[str, Any], now: datetime | None = None
) -> list[MLBPitchEvent]:
    """Walk a live feed and return every pitch as a typed event.

    The caller is responsible for filtering with a cursor so already-published
    pitches aren't re-emitted. This function is intentionally stateless.
    """
    now = now or datetime.now(UTC)
    game_pk = str(feed.get("gamePk") or feed.get("gameData", {}).get("game", {}).get("pk") or "")
    metadata_ts = _parse_mlb_time(feed.get("metaData", {}).get("timeStamp")) or now

    events: list[MLBPitchEvent] = []

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", []) or []
    for play in plays:
        about = play.get("about", {})
        at_bat_index = about.get("atBatIndex")
        if at_bat_index is None:
            continue

        inning = about.get("inning")
        inning_half = (about.get("halfInning") or "").lower() or None

        matchup = play.get("matchup", {})
        batter = matchup.get("batter", {}) or {}
        pitcher = matchup.get("pitcher", {}) or {}

        for play_event in play.get("playEvents", []) or []:
            if not play_event.get("isPitch"):
                continue

            pitch_number = play_event.get("pitchNumber")
            if pitch_number is None:
                # Fall back to the event's monotonic index within the at-bat
                pitch_number = play_event.get("index")
                if pitch_number is None:
                    continue

            details = play_event.get("details", {}) or {}
            pitch_data = play_event.get("pitchData", {}) or {}
            count = play_event.get("count", {}) or {}
            pitch_type_obj = details.get("type", {}) or {}

            event_time = _parse_mlb_time(play_event.get("startTime")) or metadata_ts

            events.append(
                MLBPitchEvent(
                    event_id=derive_pitch_id(game_pk, at_bat_index, pitch_number),
                    game_pk=game_pk,
                    event_time=event_time,
                    source_time=metadata_ts,
                    ingest_time=now,
                    at_bat_index=at_bat_index,
                    pitch_number=pitch_number,
                    inning=inning,
                    inning_half=inning_half,
                    batter_id=batter.get("id"),
                    batter_name=batter.get("fullName"),
                    pitcher_id=pitcher.get("id"),
                    pitcher_name=pitcher.get("fullName"),
                    pitch_type=pitch_type_obj.get("code"),
                    pitch_type_description=pitch_type_obj.get("description"),
                    start_speed_mph=pitch_data.get("startSpeed"),
                    end_speed_mph=pitch_data.get("endSpeed"),
                    spin_rate_rpm=(pitch_data.get("breaks", {}) or {}).get("spinRate"),
                    balls=count.get("balls"),
                    strikes=count.get("strikes"),
                    outs=count.get("outs"),
                    call_code=(details.get("call", {}) or {}).get("code"),
                    call_description=(details.get("call", {}) or {}).get("description"),
                    is_in_play=details.get("isInPlay"),
                    is_strike=details.get("isStrike"),
                    is_ball=details.get("isBall"),
                    raw_payload=play_event,
                )
            )
    return events
