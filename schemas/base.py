from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Sport(StrEnum):
    MLB = "mlb"
    NFL = "nfl"


class EventType(StrEnum):
    PITCH = "pitch"
    ATBAT_START = "atbat_start"
    ATBAT_END = "atbat_end"
    PICKOFF = "pickoff"
    MOUND_VISIT = "mound_visit"
    PITCHING_CHANGE = "pitching_change"
    SUBSTITUTION = "substitution"
    DEFENSIVE_SHIFT = "defensive_shift"
    INNING_STATE = "inning_state"
    GAME_STATE = "game_state"
    NFL_PLAY = "nfl_play"


class BaseEvent(BaseModel):
    """Every event carries this spine. See schemas/EVOLUTION.md for field rules."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: str = Field(..., description="Deterministic ID; see schemas/event_ids.md")
    event_type: EventType
    sport: Sport
    game_pk: str = Field(..., description="Canonical game identifier for the source")

    event_time: datetime = Field(..., description="When the thing actually happened")
    source_time: datetime = Field(..., description="When the source API published/updated the row")
    ingest_time: datetime = Field(..., description="When the ingestor first saw the event")
