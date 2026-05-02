"""Per-message enricher: raw pitch -> enriched pitch + state update."""

from __future__ import annotations

from google.protobuf.message import DecodeError

from schemas.event_ids import derive_pitch_id
from schemas.mlb import EventType, MLBPitchEnrichedEvent, MLBPitchEvent, Sport
from services.enricher.state import GameContext, GameStateStore


class EnrichmentError(Exception):
    pass


class MessageHandler:
    def __init__(self, store: GameStateStore):
        self._store = store

    async def handle(self, payload: bytes) -> tuple[bytes, bytes]:
        """Return (kafka_key, serialized enriched event) for the input pitch."""
        try:
            pitch = MLBPitchEvent.FromString(payload)
        except DecodeError as exc:
            raise EnrichmentError(f"proto decode failed: {exc}") from exc

        spine = pitch.spine
        if not spine.game_pk:
            raise EnrichmentError("pitch.spine.game_pk is empty")

        ctx = await self._store.get(spine.game_pk)
        new_ctx = _project(ctx, pitch)
        if new_ctx != ctx:
            await self._store.set(spine.game_pk, new_ctx)

        enriched = _build_enriched(pitch, new_ctx)
        return spine.game_pk.encode("utf-8"), enriched.SerializeToString()


def _project(ctx: GameContext, pitch: MLBPitchEvent) -> GameContext:
    """Update rolling context from a pitch event.

    Pitch events carry inning + outs; score and baserunner transitions
    require richer source events that the ingestor doesn't yet emit, so
    those fields stay at their last-known values.
    """
    inning = pitch.inning if pitch.HasField("inning") else ctx.inning
    outs = pitch.outs if pitch.HasField("outs") else ctx.outs
    return GameContext(
        home_score=ctx.home_score,
        away_score=ctx.away_score,
        runner_on_first=ctx.runner_on_first,
        runner_on_second=ctx.runner_on_second,
        runner_on_third=ctx.runner_on_third,
        inning=inning,
        outs=outs,
    )


def _build_enriched(
    pitch: MLBPitchEvent, ctx: GameContext
) -> MLBPitchEnrichedEvent:
    enriched = MLBPitchEnrichedEvent()
    enriched.spine.event_id = _derive_enriched_id(pitch)
    enriched.spine.event_type = EventType.EVENT_TYPE_PITCH
    enriched.spine.sport = Sport.SPORT_MLB
    enriched.spine.game_pk = pitch.spine.game_pk
    enriched.spine.event_time.CopyFrom(pitch.spine.event_time)
    enriched.spine.source_time.CopyFrom(pitch.spine.source_time)
    enriched.spine.ingest_time.CopyFrom(pitch.spine.ingest_time)
    enriched.pitch.CopyFrom(pitch)
    enriched.home_score = ctx.home_score
    enriched.away_score = ctx.away_score
    enriched.runner_on_first = ctx.runner_on_first
    enriched.runner_on_second = ctx.runner_on_second
    enriched.runner_on_third = ctx.runner_on_third
    return enriched


def _derive_enriched_id(pitch: MLBPitchEvent) -> str:
    """Stable enriched-event id derived from the pitch's identifying tuple.

    Reuses the same derivation rule as the raw pitch so dedup at the
    persistence-consumer layer continues to work.
    """
    return derive_pitch_id(pitch.spine.game_pk, pitch.at_bat_index, pitch.pitch_number)
