from typing import Any

from pydantic import ConfigDict, Field

from schemas.base import BaseEvent, EventType, Sport


class MLBPitchEvent(BaseEvent):
    """A single pitch from an MLB game.

    Only `event_id`, `event_type`, `sport`, `game_pk`, and the three timestamps
    are required. Every other field is Optional with default None so the schema
    can evolve additively without breaking older consumers.
    See schemas/EVOLUTION.md.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_type: EventType = EventType.PITCH
    sport: Sport = Sport.MLB

    at_bat_index: int
    pitch_number: int

    inning: int | None = None
    inning_half: str | None = Field(default=None, description="'top' or 'bottom'")

    batter_id: int | None = None
    batter_name: str | None = None
    pitcher_id: int | None = None
    pitcher_name: str | None = None

    pitch_type: str | None = Field(default=None, description="MLB pitch type code, e.g. 'FF'")
    pitch_type_description: str | None = None
    start_speed_mph: float | None = None
    end_speed_mph: float | None = None
    spin_rate_rpm: float | None = None

    balls: int | None = None
    strikes: int | None = None
    outs: int | None = None

    call_code: str | None = Field(default=None, description="MLB call code, e.g. 'C', 'S', 'B', 'X'")
    call_description: str | None = None
    is_in_play: bool | None = None
    is_strike: bool | None = None
    is_ball: bool | None = None

    raw_payload: dict[str, Any] | None = Field(
        default=None,
        description="Original MLB Stats API playEvent dict — preserved for debugging",
    )


class MLBGameState(BaseEvent):
    """Snapshot of a game's high-level state (score, inning, status)."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_type: EventType = EventType.GAME_STATE
    sport: Sport = Sport.MLB

    status: str | None = Field(default=None, description="e.g. 'Scheduled', 'In Progress', 'Final'")
    home_team: str | None = None
    away_team: str | None = None
    home_score: int | None = None
    away_score: int | None = None
    inning: int | None = None
    inning_half: str | None = None

    raw_payload: dict[str, Any] | None = None
