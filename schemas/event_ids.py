"""Deterministic event ID derivation.

The full rule table lives in schemas/event_ids.md. Every function here takes
only the minimal fields needed to derive an ID — no full payloads, no hidden
state — so the rules are easy to test and easy to reason about.

Namespace prefix (`mlb:`, `nfl:`) keeps sports isolated so they can coexist
on shared infrastructure. gamePk / game_id always appears second so it
doubles as a Kafka partition key when we pass the ID straight to the producer.
"""

from __future__ import annotations

import hashlib
import json


def _payload_hash(payload: dict) -> str:
    """Stable hash of a dict — sorted keys, compact separators."""
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


# ---------- MLB ----------


def derive_pitch_id(game_pk: int | str, at_bat_index: int, pitch_number: int) -> str:
    return f"mlb:{game_pk}:pitch:{at_bat_index}:{pitch_number}"


def derive_atbat_start_id(game_pk: int | str, at_bat_index: int) -> str:
    return f"mlb:{game_pk}:ab_start:{at_bat_index}"


def derive_atbat_end_id(game_pk: int | str, at_bat_index: int) -> str:
    return f"mlb:{game_pk}:ab_end:{at_bat_index}"


def derive_pickoff_id(game_pk: int | str, at_bat_index: int, pickoff_index: int) -> str:
    return f"mlb:{game_pk}:pickoff:{at_bat_index}:{pickoff_index}"


def derive_mound_visit_id(game_pk: int | str, at_bat_index: int, visit_index: int) -> str:
    return f"mlb:{game_pk}:mound_visit:{at_bat_index}:{visit_index}"


def derive_pitching_change_id(
    game_pk: int | str, at_bat_index: int, new_pitcher_id: int
) -> str:
    return f"mlb:{game_pk}:pitching_change:{at_bat_index}:{new_pitcher_id}"


def derive_substitution_id(
    game_pk: int | str,
    at_bat_index: int,
    in_player_id: int,
    out_player_id: int,
) -> str:
    return (
        f"mlb:{game_pk}:substitution:{at_bat_index}:{in_player_id}:{out_player_id}"
    )


def derive_defensive_shift_id(
    game_pk: int | str, at_bat_index: int, pitch_number: int
) -> str:
    return f"mlb:{game_pk}:defensive_shift:{at_bat_index}:{pitch_number}"


def derive_inning_state_id(
    game_pk: int | str, inning_num: int, half: str, state: str
) -> str:
    return f"mlb:{game_pk}:inning:{inning_num}:{half}:{state}"


def derive_game_state_id(
    game_pk: int | str, state_change_type: str, payload: dict
) -> str:
    """Fallback for game-level state changes without a stable natural ordinal."""
    return f"mlb:{game_pk}:game:{state_change_type}:{_payload_hash(payload)}"


# ---------- NFL (spine only — expanded in Phase 2) ----------


def derive_nfl_play_id(game_id: str, play_id: int) -> str:
    return f"nfl:{game_id}:play:{play_id}"
