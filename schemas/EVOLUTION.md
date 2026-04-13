# Schema evolution policy

This file defines how schemas in `schemas/` are allowed to change over time.
It exists so the consumer contract in the PRD ("purely additive, zero platform
code changes to plug in new consumers") is **enforceable**, not aspirational.

The key failure mode we're avoiding: a schema change ships, an old consumer
deserializes a new event, and crashes. Every rule below is designed to prevent
that.

## Pydantic rules (Phase 1 onward)

1. **Every field is `Optional[T]` with default `None`**, except the spine:
   `event_id`, `event_type`, `sport`, `game_pk`, `event_time`, `source_time`,
   `ingest_time`. The spine is required because every downstream system
   depends on it — removing any spine field is by definition a breaking
   change.
2. **`extra = "ignore"` on every model.** Unknown fields from a newer producer
   never crash an older consumer — they're silently dropped.
3. **Field order is stable.** New fields go at the end of the model, never in
   the middle. Field order maps 1:1 to protobuf field numbers in Phase 2.
4. **No renames, ever.** Renaming a field is semantically a remove + add, which
   is a breaking change. If a name is wrong, live with it or ship a `v2`.
5. **No type changes, ever.** Widening `int` → `int | None` is fine because of
   rule 1; changing `int` → `str` is a breaking change.

## Protobuf rules (Phase 2 onward)

1. **Field numbers are permanent.** Once a field number is assigned, it is
   never reused for any other field, ever. If you remove a field, mark its
   number as `reserved`.
2. **Field order in `.proto` matches Pydantic declaration order.** The
   top-of-file comment in each `.proto` documents this invariant.
3. **Every field is `optional`** (proto3 optional) to match the Pydantic
   convention.
4. **Additive changes only, in place.** New fields at the end, new field
   numbers.

## Making changes

### Adding a field (OK, additive)

1. Add the field to the Pydantic model at the end, as `Optional[T] = None`.
2. Add the field to the matching `.proto` at the end, with a new field number.
3. Update the top-of-file `.proto` comment to document the new field number.
4. Deploy producers first, then consumers. Old consumers will `ignore` the
   field until they're updated.

### Removing a field (semi-breaking, do carefully)

1. Mark the field deprecated in a comment in both Pydantic and proto.
2. Keep the field in the schema for **one full release cycle** so older
   producers can still populate it without errors.
3. In the next release, remove the field from Pydantic. Mark the proto field
   number as `reserved` — never reuse it.

### Semantic-breaking changes (new topic)

If the meaning of an existing field changes — e.g., `balls` starts counting
foul balls where it didn't before — this is **not** an in-place change.

1. Define the new schema as `MLBPitchEventV2` and write a new proto message.
2. Publish to a new topic: `mlb.pitches.v2`.
3. Old and new topics run in parallel until all consumers migrate.
4. Once all consumers are on v2, retire v1.

## Enforcement

- CI will run a schema compat check (Phase 7) that fails if Pydantic models
  add required fields or reorder existing fields.
- Code review rejects any PR that changes a field type or name.
- When in doubt, ship a new topic.
