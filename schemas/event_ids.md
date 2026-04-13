# Event ID derivation rules

Event IDs must be **deterministic** and **stable** across re-ingests. Dedup at
the Kafka consumer and the Postgres PK depends on this property — if you
re-ingest the same event twice and get two different IDs, the platform is
broken.

Every rule here is implemented in `schemas/event_ids.py` and unit-tested
against real MLB Stats API samples in `tests/test_event_ids.py`.

## Principles

1. **Namespace first.** Every ID starts with `mlb:` / `nfl:` so sports can
   coexist on shared infrastructure.
2. **gamePk / game_id always second.** Doubles as the Kafka partition key.
3. **Use the source's own ordinals** (`atBatIndex`, `pitchNumber`, `playId`)
   wherever stable. Do not invent counters — they won't survive restarts.
4. **Payload hash only as a last resort**, for events where the source does
   not provide a stable ordinal.
5. **No timestamps in IDs.** Timestamps can shift on re-ingest when the
   source updates the row.

## MLB

| Event type       | ID format                                                                          |
| ---------------- | ---------------------------------------------------------------------------------- |
| pitch            | `mlb:{gamePk}:pitch:{atBatIndex}:{pitchNumber}`                                    |
| atbat_start      | `mlb:{gamePk}:ab_start:{atBatIndex}`                                               |
| atbat_end        | `mlb:{gamePk}:ab_end:{atBatIndex}`                                                 |
| pickoff          | `mlb:{gamePk}:pickoff:{atBatIndex}:{pickoffIndex}`                                 |
| mound_visit      | `mlb:{gamePk}:mound_visit:{atBatIndex}:{visitIndex}`                               |
| pitching_change  | `mlb:{gamePk}:pitching_change:{atBatIndex}:{newPitcherId}`                         |
| substitution     | `mlb:{gamePk}:substitution:{atBatIndex}:{inPlayerId}:{outPlayerId}`                |
| defensive_shift  | `mlb:{gamePk}:defensive_shift:{atBatIndex}:{pitchNumber}`                          |
| inning_state     | `mlb:{gamePk}:inning:{inningNum}:{half}:{state}`                                   |
| game_state       | `mlb:{gamePk}:game:{stateChangeType}:{sha256_16(payload)}`                         |

### Notes

- `atBatIndex` comes from MLB Stats API `play.about.atBatIndex` — stable per game.
- `pitchNumber` comes from the pitch's `index` within `playEvents[]` — stable
  unless MLB corrects the play, in which case dedup wins.
- `pickoffIndex` / `visitIndex` are our own 0-based counts within an at-bat;
  derived from scanning `playEvents` in order. This is stable because
  `playEvents` itself is ordered by `index`.
- `half` is `'top'` or `'bottom'`. `state` is one of `'start'`, `'end'`,
  `'middle'`.
- `stateChangeType` for `game_state` is one of `'status'`, `'lineup'`, `'weather'`
  — we hash the payload because these events don't carry a stable ordinal.

## NFL

| Event type | ID format                     |
| ---------- | ----------------------------- |
| play       | `nfl:{game_id}:play:{play_id}` |

NFL coverage is near-live batch via nflverse — see the PRD. Additional event
subtypes will be added as the NFL ingestor expands in Phase 2.

## Dedup contract

- Postgres `events.event_id` is the primary key → hard dedup at the write.
- Kafka consumers idempotently upsert by `event_id` → soft dedup on replay and
  re-ingestion.
- If two different payloads ever produce the same ID, that's a bug in this
  file. Fix the rule, add a regression test.
