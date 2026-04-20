"""Parse MLB Stats API live-feed payloads into typed proto events.

Keeping this isolated from the ingestor loop makes it easy to unit-test
against fixture payloads without a network or database.

Proto3 has no required fields — missing scalars silently default to
zero/empty. `_validate_spine` explicitly re-establishes the required-
field semantics Pydantic was giving us for free so malformed feed data
raises loudly instead of propagating as zero-valued events.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from schemas.event_ids import derive_pitch_id
from schemas.mlb import EventType, MLBGameState, MLBPitchEvent, Sport


def _parse_mlb_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _validate_spine(msg: MLBPitchEvent | MLBGameState) -> None:
    spine = msg.spine
    if not spine.event_id:
        raise ValueError("spine.event_id is empty")
    if spine.event_type == 0:
        raise ValueError("spine.event_type is unspecified")
    if spine.sport == 0:
        raise ValueError("spine.sport is unspecified")
    if not spine.game_pk:
        raise ValueError("spine.game_pk is empty")
    for ts_field in ("event_time", "source_time", "ingest_time"):
        if not spine.HasField(ts_field):
            raise ValueError(f"spine.{ts_field} is missing")


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

    start_time = _parse_mlb_time(game_data.get("datetime", {}).get("dateTime")) or now
    metadata_ts = _parse_mlb_time(feed.get("metaData", {}).get("timeStamp"))
    source_time = metadata_ts or now
    event_id = (
        f"mlb:{game_pk}:game:state:"
        f"{metadata_ts.isoformat() if metadata_ts else now.isoformat()}"
    )

    state = MLBGameState()
    state.spine.event_id = event_id
    state.spine.event_type = EventType.EVENT_TYPE_GAME_STATE
    state.spine.sport = Sport.SPORT_MLB
    state.spine.game_pk = game_pk
    state.spine.event_time.FromDatetime(start_time)
    state.spine.source_time.FromDatetime(source_time)
    state.spine.ingest_time.FromDatetime(now)

    if status is not None:
        state.status = status
    if home_team is not None:
        state.home_team = home_team
    if away_team is not None:
        state.away_team = away_team
    if home_score is not None:
        state.home_score = home_score
    if away_score is not None:
        state.away_score = away_score
    if inning is not None:
        state.inning = inning
    if inning_half is not None:
        state.inning_half = inning_half

    _validate_spine(state)
    return state


def extract_pitch_events(
    feed: dict[str, Any], now: datetime | None = None
) -> list[MLBPitchEvent]:
    """Walk a live feed and return every pitch as a typed proto event.

    The caller filters with a cursor so already-published pitches aren't
    re-emitted. This function is intentionally stateless.
    """
    now = now or datetime.now(UTC)
    game_pk = str(
        feed.get("gamePk") or feed.get("gameData", {}).get("game", {}).get("pk") or ""
    )
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
                pitch_number = play_event.get("index")
                if pitch_number is None:
                    continue

            details = play_event.get("details", {}) or {}
            pitch_data = play_event.get("pitchData", {}) or {}
            count = play_event.get("count", {}) or {}
            pitch_type_obj = details.get("type", {}) or {}
            call_obj = details.get("call", {}) or {}

            event_time = _parse_mlb_time(play_event.get("startTime")) or metadata_ts

            pitch = MLBPitchEvent()
            pitch.spine.event_id = derive_pitch_id(game_pk, at_bat_index, pitch_number)
            pitch.spine.event_type = EventType.EVENT_TYPE_PITCH
            pitch.spine.sport = Sport.SPORT_MLB
            pitch.spine.game_pk = game_pk
            pitch.spine.event_time.FromDatetime(event_time)
            pitch.spine.source_time.FromDatetime(metadata_ts)
            pitch.spine.ingest_time.FromDatetime(now)

            pitch.at_bat_index = at_bat_index
            pitch.pitch_number = pitch_number

            if inning is not None:
                pitch.inning = inning
            if inning_half is not None:
                pitch.inning_half = inning_half
            if batter.get("id") is not None:
                pitch.batter_id = batter["id"]
            if batter.get("fullName") is not None:
                pitch.batter_name = batter["fullName"]
            if pitcher.get("id") is not None:
                pitch.pitcher_id = pitcher["id"]
            if pitcher.get("fullName") is not None:
                pitch.pitcher_name = pitcher["fullName"]
            if pitch_type_obj.get("code") is not None:
                pitch.pitch_type = pitch_type_obj["code"]
            if pitch_type_obj.get("description") is not None:
                pitch.pitch_type_description = pitch_type_obj["description"]
            if pitch_data.get("startSpeed") is not None:
                pitch.start_speed_mph = pitch_data["startSpeed"]
            if pitch_data.get("endSpeed") is not None:
                pitch.end_speed_mph = pitch_data["endSpeed"]
            spin_rate = (pitch_data.get("breaks", {}) or {}).get("spinRate")
            if spin_rate is not None:
                pitch.spin_rate_rpm = spin_rate
            if count.get("balls") is not None:
                pitch.balls = count["balls"]
            if count.get("strikes") is not None:
                pitch.strikes = count["strikes"]
            if count.get("outs") is not None:
                pitch.outs = count["outs"]
            if call_obj.get("code") is not None:
                pitch.call_code = call_obj["code"]
            if call_obj.get("description") is not None:
                pitch.call_description = call_obj["description"]
            if details.get("isInPlay") is not None:
                pitch.is_in_play = details["isInPlay"]
            if details.get("isStrike") is not None:
                pitch.is_strike = details["isStrike"]
            if details.get("isBall") is not None:
                pitch.is_ball = details["isBall"]

            pitch.raw_payload_json = json.dumps(play_event)

            _validate_spine(pitch)
            events.append(pitch)
    return events
