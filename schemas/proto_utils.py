"""Convert protobuf event messages to JSON-ready dicts for JSONB storage.

Two concerns these helpers handle:

1. Proto enum values are int-based (e.g. `SPORT_MLB = 1`). Our storage
   wire format uses lowercase short names (`"sport": "mlb"`) so the query
   API stays API-compatible with Phase 1/2. The stripping rule derives
   the prefix from the enum's type name via descriptor reflection so
   adding a new enum value is a one-place change.

2. Proto timestamps serialize as ISO-8601 UTC strings. Nested `Spine`
   fields are flattened to the top level of the payload dict. The
   optional `raw_payload_json` string is parsed back to a dict under
   `raw_payload` for readability downstream.
"""

from __future__ import annotations

import json
import re
from datetime import UTC
from typing import Any

from google.protobuf.descriptor import EnumDescriptor, FieldDescriptor
from google.protobuf.message import Message


def _camel_to_upper_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).upper()


def enum_int_to_name(enum_descriptor: EnumDescriptor, value: int) -> str:
    """Normalize a proto enum int value to its lowercase short name.

    ``SPORT_MLB`` -> ``"mlb"``; ``EVENT_TYPE_PITCH`` -> ``"pitch"``.
    The prefix is derived from the enum's type name so new values added
    to the schema get the right treatment automatically.
    """
    full_name = enum_descriptor.values_by_number[value].name
    prefix = _camel_to_upper_snake(enum_descriptor.name) + "_"
    return full_name.removeprefix(prefix).lower()


def _is_unset_optional(msg: Message, field_name: str) -> bool:
    try:
        return not msg.HasField(field_name)
    except ValueError:
        # Scalar field without presence tracking — always considered set.
        return False


def _field_to_value(field: Any, value: Any) -> Any:
    # `field` is a FieldDescriptor; typed as Any because the proto runtime mixes
    # the pure-Python and C++ descriptor types in a union the stubs can't narrow.
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        message_type = field.message_type
        assert message_type is not None  # proto invariant for message fields
        if message_type.full_name == "google.protobuf.Timestamp":
            return value.ToDatetime(tzinfo=UTC).isoformat()
        return _message_to_dict(value)
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum_type = field.enum_type
        assert enum_type is not None  # proto invariant for enum fields
        return enum_int_to_name(enum_type, value)
    return value


def _message_to_dict(msg: Message) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in msg.DESCRIPTOR.fields:
        if _is_unset_optional(msg, field.name):
            continue
        out[field.name] = _field_to_value(field, getattr(msg, field.name))
    return out


def proto_to_payload_dict(msg: Message) -> dict[str, Any]:
    """Convert an event proto message to a JSONB-ready payload dict.

    Spine fields are flattened to the top level so the payload shape
    matches Phase 1/2 (e.g. `payload.event_id`, not `payload.spine.event_id`).
    """
    out: dict[str, Any] = {}
    for field in msg.DESCRIPTOR.fields:
        if _is_unset_optional(msg, field.name):
            continue
        value = getattr(msg, field.name)
        if field.name == "spine":
            spine_type = field.message_type
            assert spine_type is not None
            for sub_field in spine_type.fields:
                if _is_unset_optional(value, sub_field.name):
                    continue
                out[sub_field.name] = _field_to_value(sub_field, getattr(value, sub_field.name))
        elif field.name == "raw_payload_json":
            out["raw_payload"] = json.loads(value) if value else None
        else:
            out[field.name] = _field_to_value(field, value)
    return out


def _enum_name_to_int(enum_descriptor: EnumDescriptor, name: str) -> int:
    """Inverse of `enum_int_to_name`."""
    prefix = _camel_to_upper_snake(enum_descriptor.name) + "_"
    full_name = prefix + name.upper()
    return enum_descriptor.values_by_name[full_name].number


def _value_to_field(field: Any, raw: Any) -> Any:
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum_type = field.enum_type
        assert enum_type is not None
        return _enum_name_to_int(enum_type, raw)
    return raw


def payload_dict_to_proto(msg: Message, payload: dict[str, Any]) -> None:
    """Inverse of `proto_to_payload_dict`. Mutates `msg` in place.

    Used by replay-service to reconstruct a proto from the flattened jsonb
    rows the persistence layer stores.
    """
    from google.protobuf.timestamp_pb2 import Timestamp

    spine_field = msg.DESCRIPTOR.fields_by_name.get("spine")
    spine_type = spine_field.message_type if spine_field is not None else None
    spine_field_names = (
        {f.name for f in spine_type.fields} if spine_type is not None else set()
    )

    spine = getattr(msg, "spine", None) if spine_type is not None else None

    for key, value in payload.items():
        if value is None:
            continue
        if spine is not None and key in spine_field_names:
            spine_sub = spine_type.fields_by_name[key]  # type: ignore[union-attr]
            if spine_sub.type == FieldDescriptor.TYPE_MESSAGE:
                ts = Timestamp()
                ts.FromJsonString(value)
                getattr(spine, key).CopyFrom(ts)
            else:
                setattr(spine, key, _value_to_field(spine_sub, value))
            continue
        if key == "raw_payload":
            if "raw_payload_json" in msg.DESCRIPTOR.fields_by_name:
                setattr(msg, "raw_payload_json", json.dumps(value))
            continue
        msg_field = msg.DESCRIPTOR.fields_by_name.get(key)
        if msg_field is None:
            continue
        setattr(msg, key, _value_to_field(msg_field, value))
