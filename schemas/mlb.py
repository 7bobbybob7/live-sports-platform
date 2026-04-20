"""Aliasing layer over the canonical protobuf schemas.

Call sites import from here, not from `schemas.proto.mlb.v1.events_pb2`
directly. This makes the v1 -> v2 migration a single-file change: when v2
lands, flip the imports here and every consumer keeps working.
"""

from schemas.proto.mlb.v1 import events_pb2

MLBPitchEvent = events_pb2.MLBPitch
MLBGameState = events_pb2.MLBGameState
Spine = events_pb2.Spine
Sport = events_pb2.Sport
EventType = events_pb2.EventType

__all__ = [
    "EventType",
    "MLBGameState",
    "MLBPitchEvent",
    "Sport",
    "Spine",
]
