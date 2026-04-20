# Proto schema evolution

Protobuf is the canonical schema layer for this project's Kafka messages and internal gRPC contracts. Every evolution rule on this page exists to keep old consumers running when a new producer ships — that's the consumer contract in the PRD.

## Canonical location

- **Source of truth:** `.proto` files under `schemas/proto/<domain>/v1/`
- **Generated Python:** `_pb2.py` + `_pb2.pyi` committed alongside each `.proto`
- **Typed aliases:** [`schemas/mlb.py`](../mlb.py) re-exports proto classes with friendly names so call sites never import from `events_pb2` directly. When a v2 arrives, flipping imports in that one file migrates every consumer.

## Hard rules (break these and old consumers break)

1. **Field numbers are permanent.** Once assigned, never reused for any other field. Removing a field requires `reserved N;` on its number.
2. **No type changes.** `int32 → int64`, `string → bytes`, or any other scalar swap on an existing field is forbidden. If the type needs to change, add a new field with a new number and deprecate the old.
3. **No field renames.** Cosmetic rename is semantically remove + add. A wrong name lives; `v2` is the only fix.
4. **New fields go at the end** of the message, using the next available field number.
5. **All new scalar fields are `optional`** (proto3 explicit presence). Lets consumers distinguish "field not present" from "zero-valued field" via `HasField()` — the proto3 default-zero trap bit us enough in the parser already, no need to invite it into new fields.
6. **Enum values are permanent.** Existing values never renumber; removed values get `reserved N;` on their number.
7. **Zero means UNSPECIFIED.** Every enum's `0` value is an explicit `UNSPECIFIED` marker. Producers never set it; consumers may check for it as "missing."

## Conventions

- **Package segment `v1`** (e.g. `mlb.v1`). Breaking changes create `mlb.v2` — we don't break `v1` in place.
- **Enum value prefix.** Values are prefixed with the enum's `UPPER_SNAKE` type name: `Sport → SPORT_MLB`, `EventType → EVENT_TYPE_PITCH`. [`schemas/proto_utils.py`](../proto_utils.py) strips this prefix for the wire serialization so storage payloads read `"sport": "mlb"`, preserving the Phase 1/2 API shape.
- **Spine composition, not inheritance.** Every event message carries a `Spine` sub-message at field 1 with the seven required fields (`event_id`, `event_type`, `sport`, `game_pk`, `event_time`, `source_time`, `ingest_time`). Proto has no inheritance; composition makes the required contract explicit across all event types.
- **`raw_payload_json` as a `string` escape hatch.** Carries the original upstream JSON for debugging and lineage. Kept as a string rather than `google.protobuf.Struct` — simpler to serialize, consumers parse on demand.
- **Timestamps use `google.protobuf.Timestamp`**, UTC only.

## Making a change

### Adding a field (additive, OK)

1. Append the field at the end of the message with the next available field number and the `optional` modifier.
2. Run `make proto`. Commit the `.proto` source and regenerated `_pb2.py` / `_pb2.pyi` in the same commit.
3. Deploy producers first, consumers second. Old consumers silently ignore the new field.

### Removing a field (semi-breaking, do carefully)

1. Mark it with a `// DEPRECATED:` comment.
2. Wait one full release cycle for producers to stop writing it.
3. Delete the field. Add `reserved N;` and `reserved "field_name";` to lock in the retirement.
4. Regenerate, commit.

### Adding a new message (additive, OK)

1. Append the message definition at the bottom of the file.
2. Regenerate, commit.
3. If consumers need a friendly name, add an alias in [`schemas/mlb.py`](../mlb.py).

### Breaking changes (new package version)

Any change that can't follow the rules above — changing a field's semantic meaning, changing its type, removing and replacing it — is a v2 change.

1. Create `schemas/proto/<domain>/v2/events.proto`.
2. Run v1 and v2 topics or packages in parallel.
3. Migrate consumers off v1. Once the v1 topic has drained, retire v1 and remove its generated code.

## Enforcement

- **[`.github/workflows/proto-check.yml`](../../.github/workflows/proto-check.yml)** runs `make proto` on every PR and fails if the regenerated code diverges from what's checked in. Catches the "I edited the `.proto` but forgot to regenerate" class of bug before main.
- Code review is the enforcement mechanism for the hard rules above (number reuse, type changes, renames). `.proto` diffs are small and should be read closely.
