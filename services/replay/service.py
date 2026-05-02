"""Replay loop: read PG events in order, sleep based on speed, publish."""

from __future__ import annotations

import asyncio

from services.common.logging import get_logger
from services.replay.publisher import ReplayPublisher
from services.replay.reader import EventReader

_logger = get_logger(__name__)


class ReplayService:
    def __init__(
        self,
        reader: EventReader,
        publisher: ReplayPublisher,
        speed: float,
    ):
        if speed <= 0:
            raise ValueError(f"speed must be > 0, got {speed}")
        self._reader = reader
        self._publisher = publisher
        self._speed = speed

    async def replay_game(self, game_pk: str) -> int:
        """Replay every pitch for `game_pk`. Returns the count published."""
        count = 0
        prev_event_time = None
        async for pitch in self._reader.stream_pitches(game_pk):
            event_time = pitch.spine.event_time.ToDatetime()
            if prev_event_time is not None:
                gap_seconds = (event_time - prev_event_time).total_seconds()
                wait = max(gap_seconds / self._speed, 0.0)
                if wait > 0:
                    await asyncio.sleep(wait)
            await self._publisher.publish_pitch(pitch)
            prev_event_time = event_time
            count += 1
        _logger.info("replay_complete", game_pk=game_pk, pitches=count, speed=self._speed)
        return count
