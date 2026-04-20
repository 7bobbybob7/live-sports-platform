"""Tests for the MLB live-feed parser.

We build a minimal fixture that mirrors the shape of the real MLB Stats API
response (just the fields the parser reads) so we don't need network access
or a stored multi-megabyte JSON blob. The shape is documented in
services/mlb_ingestor/parser.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from schemas.mlb import EventType, Sport
from services.mlb_ingestor.parser import extract_game_state, extract_pitch_events


def _make_feed() -> dict[str, Any]:
    return {
        "gamePk": 745612,
        "metaData": {"timeStamp": "2024-07-15T23:05:12.000Z"},
        "gameData": {
            "game": {"pk": 745612},
            "status": {"detailedState": "In Progress"},
            "teams": {
                "home": {"name": "San Diego Padres"},
                "away": {"name": "Los Angeles Dodgers"},
            },
            "datetime": {"dateTime": "2024-07-15T22:40:00Z"},
        },
        "liveData": {
            "linescore": {
                "currentInning": 4,
                "inningHalf": "Top",
                "teams": {
                    "home": {"runs": 2},
                    "away": {"runs": 1},
                },
            },
            "plays": {
                "allPlays": [
                    {
                        "about": {
                            "atBatIndex": 0,
                            "inning": 1,
                            "halfInning": "top",
                        },
                        "matchup": {
                            "batter": {"id": 605141, "fullName": "Mookie Betts"},
                            "pitcher": {"id": 543037, "fullName": "Yu Darvish"},
                        },
                        "playEvents": [
                            {
                                "isPitch": True,
                                "pitchNumber": 1,
                                "index": 0,
                                "startTime": "2024-07-15T22:45:10.000Z",
                                "details": {
                                    "type": {"code": "FF", "description": "Four-Seam Fastball"},
                                    "call": {"code": "C", "description": "Called Strike"},
                                    "isInPlay": False,
                                    "isStrike": True,
                                    "isBall": False,
                                },
                                "count": {"balls": 0, "strikes": 1, "outs": 0},
                                "pitchData": {
                                    "startSpeed": 94.2,
                                    "endSpeed": 86.5,
                                    "breaks": {"spinRate": 2280},
                                },
                            },
                            {
                                "isPitch": False,  # non-pitch event, should be skipped
                                "index": 1,
                            },
                            {
                                "isPitch": True,
                                "pitchNumber": 2,
                                "index": 2,
                                "startTime": "2024-07-15T22:45:25.000Z",
                                "details": {
                                    "type": {"code": "SL", "description": "Slider"},
                                    "call": {"code": "B", "description": "Ball"},
                                    "isStrike": False,
                                    "isBall": True,
                                },
                                "count": {"balls": 1, "strikes": 1, "outs": 0},
                                "pitchData": {"startSpeed": 82.1},
                            },
                        ],
                    },
                    {
                        "about": {
                            "atBatIndex": 1,
                            "inning": 1,
                            "halfInning": "top",
                        },
                        "matchup": {
                            "batter": {"id": 665742, "fullName": "Shohei Ohtani"},
                            "pitcher": {"id": 543037, "fullName": "Yu Darvish"},
                        },
                        "playEvents": [
                            {
                                "isPitch": True,
                                "pitchNumber": 1,
                                "index": 0,
                                "startTime": "2024-07-15T22:47:30.000Z",
                                "details": {
                                    "type": {"code": "FF"},
                                    "call": {"code": "X"},
                                    "isInPlay": True,
                                },
                                "count": {"balls": 0, "strikes": 0, "outs": 1},
                                "pitchData": {"startSpeed": 95.0},
                            }
                        ],
                    },
                ]
            },
        },
    }


class TestExtractPitchEvents:
    def test_parses_all_pitches(self) -> None:
        events = extract_pitch_events(_make_feed())
        assert len(events) == 3

    def test_skips_non_pitch_events(self) -> None:
        events = extract_pitch_events(_make_feed())
        assert all(e.spine.event_type == EventType.EVENT_TYPE_PITCH for e in events)

    def test_sport_is_mlb(self) -> None:
        events = extract_pitch_events(_make_feed())
        assert all(e.spine.sport == Sport.SPORT_MLB for e in events)

    def test_event_ids_are_deterministic(self) -> None:
        a = extract_pitch_events(_make_feed())
        b = extract_pitch_events(_make_feed())
        assert [e.spine.event_id for e in a] == [e.spine.event_id for e in b]

    def test_event_ids_are_unique(self) -> None:
        events = extract_pitch_events(_make_feed())
        ids = {e.spine.event_id for e in events}
        assert len(ids) == len(events)

    def test_event_ids_follow_derivation_rule(self) -> None:
        events = extract_pitch_events(_make_feed())
        assert events[0].spine.event_id == "mlb:745612:pitch:0:1"
        assert events[1].spine.event_id == "mlb:745612:pitch:0:2"
        assert events[2].spine.event_id == "mlb:745612:pitch:1:1"

    def test_three_timestamps_populated(self) -> None:
        events = extract_pitch_events(_make_feed())
        for e in events:
            assert e.spine.HasField("event_time")
            assert e.spine.HasField("source_time")
            assert e.spine.HasField("ingest_time")

    def test_source_time_comes_from_metadata(self) -> None:
        events = extract_pitch_events(_make_feed())
        expected = datetime(2024, 7, 15, 23, 5, 12, tzinfo=UTC)
        for e in events:
            assert e.spine.source_time.ToDatetime(tzinfo=UTC) == expected

    def test_pitch_details_populated(self) -> None:
        events = extract_pitch_events(_make_feed())
        first = events[0]
        assert first.pitch_type == "FF"
        assert first.start_speed_mph == 94.2
        assert first.spin_rate_rpm == 2280
        assert first.call_code == "C"
        assert first.is_strike is True
        assert first.batter_name == "Mookie Betts"
        assert first.pitcher_name == "Yu Darvish"
        assert first.at_bat_index == 0
        assert first.pitch_number == 1

    def test_raw_payload_json_populated(self) -> None:
        import json

        events = extract_pitch_events(_make_feed())
        first_raw = json.loads(events[0].raw_payload_json)
        assert first_raw["pitchNumber"] == 1
        assert first_raw["details"]["type"]["code"] == "FF"

    def test_empty_feed_returns_empty_list(self) -> None:
        empty_feed: dict[str, Any] = {
            "gamePk": 1,
            "metaData": {},
            "liveData": {"plays": {"allPlays": []}},
        }
        assert extract_pitch_events(empty_feed) == []


class TestExtractGameState:
    def test_extracts_top_level_state(self) -> None:
        state = extract_game_state(_make_feed())
        assert state.spine.game_pk == "745612"
        assert state.status == "In Progress"
        assert state.home_team == "San Diego Padres"
        assert state.away_team == "Los Angeles Dodgers"
        assert state.home_score == 2
        assert state.away_score == 1
        assert state.inning == 4
        assert state.inning_half == "top"

    def test_spine_populated(self) -> None:
        state = extract_game_state(_make_feed())
        assert state.spine.event_type == EventType.EVENT_TYPE_GAME_STATE
        assert state.spine.sport == Sport.SPORT_MLB
        assert state.spine.HasField("event_time")
        assert state.spine.HasField("source_time")
        assert state.spine.HasField("ingest_time")
