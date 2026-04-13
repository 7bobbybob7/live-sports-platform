"""Unit tests for event ID derivation rules.

These test the invariants documented in schemas/event_ids.md. If any of these
fail, the dedup guarantee is broken and we shouldn't ship.
"""

from __future__ import annotations

import pytest

from schemas.event_ids import (
    derive_atbat_end_id,
    derive_atbat_start_id,
    derive_defensive_shift_id,
    derive_game_state_id,
    derive_inning_state_id,
    derive_mound_visit_id,
    derive_nfl_play_id,
    derive_pickoff_id,
    derive_pitch_id,
    derive_pitching_change_id,
    derive_substitution_id,
)


class TestPitchId:
    def test_format(self):
        assert derive_pitch_id(745612, 42, 3) == "mlb:745612:pitch:42:3"

    def test_accepts_str_game_pk(self):
        assert derive_pitch_id("745612", 0, 0) == "mlb:745612:pitch:0:0"

    def test_deterministic(self):
        assert derive_pitch_id(745612, 5, 2) == derive_pitch_id(745612, 5, 2)

    def test_unique_per_pitch_within_at_bat(self):
        a = derive_pitch_id(745612, 5, 1)
        b = derive_pitch_id(745612, 5, 2)
        assert a != b

    def test_unique_across_at_bats(self):
        a = derive_pitch_id(745612, 5, 1)
        b = derive_pitch_id(745612, 6, 1)
        assert a != b

    def test_unique_across_games(self):
        a = derive_pitch_id(745612, 5, 1)
        b = derive_pitch_id(745613, 5, 1)
        assert a != b


class TestAtBatIds:
    def test_start_and_end_differ(self):
        assert derive_atbat_start_id(100, 7) != derive_atbat_end_id(100, 7)

    def test_format(self):
        assert derive_atbat_start_id(100, 7) == "mlb:100:ab_start:7"
        assert derive_atbat_end_id(100, 7) == "mlb:100:ab_end:7"


class TestPickoffId:
    def test_format(self):
        assert derive_pickoff_id(100, 7, 0) == "mlb:100:pickoff:7:0"

    def test_multiple_pickoffs_in_at_bat(self):
        ids = {derive_pickoff_id(100, 7, i) for i in range(5)}
        assert len(ids) == 5


class TestMoundVisitId:
    def test_format(self):
        assert derive_mound_visit_id(100, 7, 0) == "mlb:100:mound_visit:7:0"

    def test_different_visits_unique(self):
        assert derive_mound_visit_id(100, 7, 0) != derive_mound_visit_id(100, 7, 1)


class TestPitchingChangeId:
    def test_format(self):
        assert (
            derive_pitching_change_id(100, 7, 543037)
            == "mlb:100:pitching_change:7:543037"
        )


class TestSubstitutionId:
    def test_format(self):
        assert (
            derive_substitution_id(100, 7, 111, 222)
            == "mlb:100:substitution:7:111:222"
        )

    def test_swap_is_different_event(self):
        a = derive_substitution_id(100, 7, 111, 222)
        b = derive_substitution_id(100, 7, 222, 111)
        assert a != b


class TestDefensiveShiftId:
    def test_format(self):
        assert (
            derive_defensive_shift_id(100, 7, 2)
            == "mlb:100:defensive_shift:7:2"
        )


class TestInningStateId:
    def test_format(self):
        assert (
            derive_inning_state_id(100, 5, "top", "start")
            == "mlb:100:inning:5:top:start"
        )

    def test_half_distinguishes(self):
        a = derive_inning_state_id(100, 5, "top", "start")
        b = derive_inning_state_id(100, 5, "bottom", "start")
        assert a != b


class TestGameStateId:
    def test_stable_for_same_payload(self):
        payload = {"status": "Delayed", "reason": "Rain"}
        a = derive_game_state_id(100, "status", payload)
        b = derive_game_state_id(100, "status", payload)
        assert a == b

    def test_changes_when_payload_changes(self):
        a = derive_game_state_id(100, "status", {"status": "Delayed"})
        b = derive_game_state_id(100, "status", {"status": "Resumed"})
        assert a != b

    def test_key_order_does_not_affect_id(self):
        # Hash must be over sorted keys so dict insertion order doesn't matter.
        a = derive_game_state_id(100, "status", {"a": 1, "b": 2})
        b = derive_game_state_id(100, "status", {"b": 2, "a": 1})
        assert a == b

    def test_format_prefix(self):
        out = derive_game_state_id(100, "status", {"x": 1})
        assert out.startswith("mlb:100:game:status:")


class TestNFLPlayId:
    def test_format(self):
        assert derive_nfl_play_id("2024_01_KC_BAL", 42) == "nfl:2024_01_KC_BAL:play:42"


class TestNamespaceIsolation:
    """The namespace prefix prevents collisions between sports."""

    def test_mlb_and_nfl_never_collide(self):
        mlb = derive_pitch_id("12345", 1, 1)
        nfl = derive_nfl_play_id("12345", 1)
        assert not mlb.startswith("nfl:")
        assert not nfl.startswith("mlb:")


@pytest.mark.parametrize(
    "derivation,args",
    [
        (derive_pitch_id, (100, 1, 1)),
        (derive_atbat_start_id, (100, 1)),
        (derive_atbat_end_id, (100, 1)),
        (derive_pickoff_id, (100, 1, 0)),
        (derive_mound_visit_id, (100, 1, 0)),
        (derive_pitching_change_id, (100, 1, 543037)),
        (derive_substitution_id, (100, 1, 111, 222)),
        (derive_defensive_shift_id, (100, 1, 1)),
        (derive_inning_state_id, (100, 1, "top", "start")),
    ],
)
def test_determinism(derivation, args):
    """Every derivation function must be pure and deterministic."""
    assert derivation(*args) == derivation(*args)
