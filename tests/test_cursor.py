"""Tests for the GameCursor dedup logic.

The cursor's job is to correctly answer "have I already published this pitch?"
for every `(at_bat_index, pitch_number)` pair. Getting this wrong means
duplicates or gaps after a restart.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from services.mlb_ingestor.cursor import GameCursor


@pytest.fixture
def cursor() -> GameCursor:
    return GameCursor(
        last_at_bat_index=5,
        last_pitch_number=3,
        updated_at=datetime.now(UTC),
    )


class TestIsAfter:
    def test_earlier_at_bat_is_before_cursor(self, cursor: GameCursor):
        assert cursor.is_after(4, 99) is True

    def test_same_at_bat_earlier_pitch_is_before(self, cursor: GameCursor):
        assert cursor.is_after(5, 2) is True

    def test_exact_cursor_position_is_before_or_equal(self, cursor: GameCursor):
        # The pitch at the cursor was already published — must be filtered.
        assert cursor.is_after(5, 3) is True

    def test_same_at_bat_later_pitch_is_after(self, cursor: GameCursor):
        assert cursor.is_after(5, 4) is False

    def test_later_at_bat_is_after(self, cursor: GameCursor):
        assert cursor.is_after(6, 0) is False

    def test_much_later_at_bat_is_after(self, cursor: GameCursor):
        assert cursor.is_after(42, 0) is False
