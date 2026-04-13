# Live Sports Data Platform — PRD

## One-line pitch
A distributed streaming platform that ingests live MLB and near-live NFL events, enriches them with game state, and serves them to downstream consumers (starting with a live dashboard, later a betting model) through Kafka and gRPC.


## Non-goals
- Not a betting execution system. No real bets, no broker integration.
- Not a sharp-book-beating low-latency system. Latency is bounded by public API update cadence; the platform is not trying to win on speed.
- Not a generic data platform for arbitrary sports. MLB and NFL only, with room to add NBA later if off-season coverage becomes a problem.
- Not an ML platform. Models consume from the platform; they don't run inside it.

## Success criteria
- Live MLB game ingested end-to-end: pitch occurs → event available to consumers with **end-to-end p99 under 20 seconds** (dominated by MLB Stats API delay, not platform latency).
- **Platform-internal p99 latency** (`ingest_time` → enriched event served via gRPC/WebSocket) under 500ms, measured directly from OpenTelemetry spans.
- **Upstream latency** (`event_time` → `source_time`, the MLB API contribution) reported separately so platform vs upstream contributions are visible in Grafana.
- Replay service can replay any historical game at configurable speed (1x, 10x, 100x), indistinguishable to consumers from live.
- Betting model can subscribe as a Kafka consumer with zero platform code changes.
- Every service survives a pod restart without data loss or consumer-visible failures (ingestor cursor persisted to Redis, consumer offsets to Kafka).
- Deployed to a real cloud Kubernetes cluster (DigitalOcean Kubernetes), not just minikube.

---

## Architecture

### Ingestion layer (Python)
- One ingestor per data source: `mlb-ingestor` (live), `nfl-ingestor` (near-live batch).
- **MLB**: polls MLB Stats API per active game using one asyncio task per game (~5–10s cadence per game during play). Genuinely live, bounded only by MLB API delay.
- **NFL**: polls `nfl_data_py` / underlying nflverse sources on a slower cadence. Honest near-live batch ingest, not low-latency streaming. Documented as such in the README and surfaced in metrics.
- Diffs against last-seen state, publishes only new events to Kafka.
- **Cursor persistence**: after each successful publish, writes `(gamePk, atBatIndex, pitchNumber)` cursor to Redis. On startup, reads cursor and resumes from there. Cursor loss is recoverable — re-ingest a few minutes of events; consumer-side dedup by `event_id` catches duplicates.
- Handles rate limits, exponential backoff, and API failures gracefully.
- Emits Prometheus metrics from day one: poll latency, events produced, API error rate, cursor staleness.
- Structured JSON logs with correlation IDs from day one.

### Streaming layer (Kafka)
- Topics organized by sport and stage: `mlb.pitches.raw`, `mlb.pitches.enriched`, `mlb.games`, `nfl.plays.raw`, `nfl.plays.enriched`.
- **Dead-letter topics**: `mlb.pitches.dlq`, `nfl.plays.dlq` for malformed or unprocessable events. DLQ messages carry the original payload plus error metadata (consumer ID, error class, stack trace fingerprint).
- Partitioned by game ID for in-order processing per game.
- 7-day retention; older history served via Postgres-backed replay.

### Processing layer (Python, Kafka consumer groups)
- `enricher` service maintains per-game rolling state (score, inning, outs, baserunners, pitcher pitch count, batter context).
- Reads `*.raw` topics, produces `*.enriched` topics with full context attached per event.
- Writes durable state to PostgreSQL, hot state to Redis.
- **Enriched events must be self-contained** — a consumer should never need to query Postgres for context mid-game.

### Storage layer
- **PostgreSQL**: durable event log, game metadata, historical queries, source of truth for replay.
  - Composite index `(game_pk, event_time, event_id)` from day one — supports replay and per-game time-range queries.
  - Unique index on `event_id` enforces dedup at the DB layer (belt and suspenders with Kafka-side dedup).
  - Index on `(event_time)` for cross-game time-range queries.
  - Avoid over-indexing — bulk backfill performance matters.
- **Redis**: hot cache for current game state (sub-ms lookups) and ingestor cursor persistence.
- **ClickHouse** (stretch, Phase 8): analytical queries over full event history.

### Serving layer
- **gRPC service** (`query-api`) for internal consumers — defined via protobuf schemas in a shared `schemas/` folder.
- **REST gateway** (grpc-gateway) fronting the gRPC service for web clients.
- **WebSocket service** (`live-gateway`) that subscribes to Kafka and pushes live updates to browsers.

### Replay service
- Reads historical events from PostgreSQL ordered by `(event_time, event_id)`, republishes them to Kafka at configurable speed (1x, 10x, 100x).
- Deterministic tiebreak on `event_id` handles simultaneous events cleanly.
- Consumers can't distinguish replay from live.
- Enables year-round development and backtesting against historical games.
- **Build this in Phase 3, not later** — it's the foundation of all testing and of future CLV backtesting.

### Frontend (Next.js + TypeScript)
- Live game dashboard with pitch-by-pitch WebSocket updates.
- Historical game browser backed by gRPC-gateway REST.
- Platform health page: ingestion lag, event throughput, consumer lag, latency breakdown.
- Deployed to Vercel.

### Infrastructure
- All services containerized (multi-stage Docker builds, distroless runtime images).
- **Kubernetes**: DigitalOcean Kubernetes (DOKS). Realistic all-in cost: ~$24/mo cluster (2 nodes) + ~$15/mo managed Postgres + ~$15/mo managed Redis + free GitHub Container Registry = **~$55/mo**. Pods scaled to zero when not actively testing.
- **GitHub Student Developer Pack credits applied**: $200 DigitalOcean credit (applied before cluster creation — new-account-only) covers ~3.5 months at full burn, longer with pods scaled to zero between sessions. Namecheap free `.me` domain + SSL used for the public dashboard in Phase 6. Sentry free dev plan added in Phase 1 alongside structured logging for low-effort exception tracking.
- Helm charts per service, versioned.
- Terraform for cluster provisioning and managed Postgres/Redis. Credentials pulled from DO API at apply time, written to k8s Secrets.
- Horizontal pod autoscaling: CPU via built-in HPA, Kafka consumer lag via **KEDA** (the built-in HPA can't scale on queue depth).
- Liveness and readiness probes on every service.

### Observability
Rolled out incrementally across phases, not deferred to a single phase:
- **Phase 1**: every service exposes `/metrics` (Prometheus format). Structured JSON logs with correlation IDs.
- **Phase 2**: Prometheus + Grafana added to local Docker Compose. Basic dashboards: ingestion rate, error rate, consumer lag, cursor staleness.
- **Phase 3**: OpenTelemetry tracing instrumented end-to-end. Local Jaeger via Compose. Trace context propagated across Kafka via message headers (use OTel Kafka instrumentation libraries, not hand-rolled).
- **Phase 4**: deploy to cluster with instrumentation already working. In-cluster Prometheus scrapes services that already expose `/metrics`.
- **Phase 5**: observability polish — production alerting, dashboard design, SLO definitions, runbooks.

Latency reporting always splits **upstream**, **platform**, and **end-to-end** so the "MLB API delay vs platform latency" attribution is visible.

### Secrets management
- **Local dev**: `.env.local` (gitignored). `.env.example` checked in with placeholder keys.
- **Cluster**: Kubernetes Secrets, provisioned via Terraform from DigitalOcean's secret references. Sealed-secrets if anything must be checked into the repo.
- **Managed Postgres/Redis credentials**: pulled from DO API at Terraform apply time, written into k8s Secret objects, mounted into pods as env vars.
- Nothing secret in the repo, ever.

### CI/CD
- GitHub Actions: on PR, run unit tests, integration tests, linting, type checks.
- On merge to main: build images, push to GitHub Container Registry, deploy to staging via Helm.
- Manual promotion to production with a single workflow dispatch.

---

## Consumer contract (the key design constraint)

The platform is designed so that **any future consumer — including the betting model — is purely additive**. No platform code changes required to plug in new consumers.

### Integration point
- Consumers subscribe to Kafka topics directly. No polling, no request/response for live event delivery.
- gRPC/REST is for historical queries and current-state lookups, not for live event subscription.

### Event payload contract
- All `*.enriched` events are self-contained: a consumer receives one message and has every field needed to react.
- No follow-up queries required for game state, score, pitcher context, or batter context at time of event.

### Base event schema
Every event, regardless of sport or type, carries the same spine:

```python
class BaseEvent(BaseModel):
    event_id: str            # see schemas/event_ids.md for derivation rules
    event_time: datetime     # when the thing happened (per source)
    source_time: datetime    # when the source API published/last-updated the row
    ingest_time: datetime    # when the ingestor first saw it
    # ... sport-specific fields, all Optional
    class Config:
        extra = "ignore"     # forward-compat: ignore unknown fields
```

These three timestamps unlock the latency breakdown:
- **upstream_latency** = `source_time - event_time`
- **platform_latency** = `served_time - ingest_time`
- **end_to_end_latency** = `consumer_receive_time - event_time`

### Event ID derivation
Stable, deterministic event IDs are required for dedup. The full per-event-type rules live in `schemas/event_ids.md`. Example for MLB:

```
pitch:        mlb:{gamePk}:pitch:{atBatIndex}:{pitchNumber}
atbat_start:  mlb:{gamePk}:ab_start:{atBatIndex}
atbat_end:    mlb:{gamePk}:ab_end:{atBatIndex}
pickoff:      mlb:{gamePk}:pickoff:{atBatIndex}:{pickoffIndex}
mound_visit:  mlb:{gamePk}:mound_visit:{atBatIndex}:{visitIndex}
pitch_change: mlb:{gamePk}:pitching_change:{atBatIndex}:{newPitcherId}
sub:          mlb:{gamePk}:substitution:{atBatIndex}:{inPlayerId}:{outPlayerId}
shift:        mlb:{gamePk}:defensive_shift:{atBatIndex}:{pitchNumber}
inning_state: mlb:{gamePk}:inning:{inningNum}:{half}:{state}
game_state:   mlb:{gamePk}:game:{stateChangeType}:{hash(payload)}
```

Encoded principles:
- Namespace prefix (`mlb:`) so NFL/NBA can coexist in shared infrastructure.
- `gamePk` always present; doubles as the Kafka partition key.
- Secondary ordinals (`atBatIndex`, `pickoffIndex`) come from MLB's own numbering, which is stable per game.
- Fallback to payload hash for events where MLB doesn't provide a stable ordinal.
- NFL follows the same pattern with `nfl:{game_id}:{play_id}` as the spine.

Derivation function lives in `schemas/event_ids.py` with unit tests against real MLB Stats API samples.

### Schema governance
- Event schemas defined as **Pydantic models** in `schemas/` from Phase 1 — single source of truth for Python services.
- **Phase 2**: protobuf definitions added alongside, hand-translated from Pydantic. Field numbers map to Pydantic field declaration order, documented in a comment at the top of each `.proto` file. Both representations coexist; gRPC services use proto, internal Python services may use either.
- See `schemas/EVOLUTION.md` for the full evolution policy. Summary:
  - Every field `Optional[T]` with default `None`, except the spine (`event_id`, `event_time`, `source_time`, `ingest_time`).
  - `extra = "ignore"` on Pydantic models so unknown fields from future versions don't crash older consumers.
  - Additive changes in place: new fields always at the end, always optional.
  - Removed fields: marked deprecated, field number never reused, kept in schema for one full release cycle.
  - Semantic-breaking changes: new topic with `v2` suffix (`mlb.pitches.v2`).

### State boundary
- **Platform owns:** raw events, enriched events, current game state, historical event log.
- **Consumers own:** their own derived features, predictions, external data (odds, lines), analysis outputs.
- Consumers never write to platform storage. Platform never knows which consumers exist.

---

## Build phases (dependency-ordered)

### Phase 1 — Local foundation
**Depends on:** nothing.
- MLB ingestor in Python; one asyncio task per active game; cursor persisted to Redis.
- Local Postgres (Docker Compose) with schema and indexes from day one.
- **Local Redis** (Docker Compose) for ingestor cursor and (later) game state cache.
- Pydantic schemas in `schemas/` — `BaseEvent`, MLB pitch, MLB game state. Single source of truth.
- `schemas/event_ids.py` — deterministic ID derivation per event type, unit-tested against real MLB Stats API samples.
- `schemas/EVOLUTION.md` — written down before any field is added to a model.
- Basic FastAPI REST endpoint to query recent pitches.
- `/metrics` Prometheus endpoint on every service.
- Structured JSON logging with correlation IDs.
- Unit tests, integration tests against a real local Postgres.

**Milestone:** `curl localhost:8080/games/latest` returns live pitch data from an active MLB game; restarting the ingestor container resumes from the Redis cursor without duplicates or gaps.

### Phase 2 — Kafka, split services, NFL, local observability
**Depends on:** Phase 1.
- Introduce Kafka (Redpanda locally) as the event bus.
- Split ingestor (produces to `mlb.pitches.raw`) from persistence consumer (reads from Kafka, writes to Postgres).
- Add NFL ingestor (near-live batch via `nfl_data_py`).
- Add DLQ topics (`mlb.pitches.dlq`, `nfl.plays.dlq`) and DLQ-write paths in consumers.
- Wire Redis as the hot game-state cache (already running from Phase 1).
- Add protobuf schemas alongside Pydantic — hand-translated, field numbers stable and documented.
- Add Prometheus + Grafana to local Docker Compose. First operational dashboards: ingestion rate, error rate, consumer lag, cursor staleness.

**Milestone:** events visible on Kafka topics; multiple services coordinate through the message bus; Grafana dashboard shows live ingestion rate; restart any service without data loss.

### Phase 3 — Enrichment, gRPC, replay, tracing
**Depends on:** Phase 2.
- Build `enricher` service: reads `*.raw`, produces `*.enriched` with full game state.
- Build gRPC `query-api` service with proto-defined interface.
- Add grpc-gateway REST layer in front.
- **Build replay service:** reads historical events from Postgres ordered by `(event_time, event_id)`, republishes to Kafka at configurable speed.
- Instrument every service with OpenTelemetry. Trace context propagated across Kafka via message headers (OTel Kafka instrumentation libraries, not hand-rolled).
- Local Jaeger via Compose; traces visible end-to-end across the full request path.

**Milestone:** replay an entire 2024 World Series game through Kafka; consumers can't tell it's not live; Jaeger shows end-to-end traces; platform-internal p99 latency directly measurable from span data.

### Phase 4 — Kubernetes deployment to DOKS
**Depends on:** Phase 3.
- Multi-stage Docker builds, distroless runtime images.
- Helm charts for every service.
- Terraform: provision DOKS cluster, managed Postgres, managed Redis, container registry credentials.
- Deploy full stack to DO; debug networking; fix what breaks.
- Health checks, resource limits, KEDA-based HPA on Kafka consumer lag.
- In-cluster Prometheus scrapes services (already exposing `/metrics` from Phase 1). Tracing already wired from Phase 3.

**Milestone:** `kubectl get pods` shows the full stack running on DOKS, ingesting a live MLB game, with metrics and traces flowing.

### Phase 5 — Observability polish
**Depends on:** Phase 4.
- Production-grade Grafana dashboards: latency breakdown (upstream / platform / end-to-end), consumer lag by group, error rates by service, cursor staleness, throughput by topic.
- Slack alerting: sustained error rate > 1%, consumer lag > 30s, ingestor cursor staleness > 60s.
- SLO definitions tied directly to success criteria.
- Runbooks for common failures (Kafka partition rebalance, Postgres connection exhaustion, MLB API outage, NFL data source breakage).

**Milestone:** watch a live game through a Grafana dashboard; latency breakdown shows clean separation of upstream vs platform contributions; alerts fire correctly on injected failures.

### Phase 6 — Frontend + live gateway
**Depends on:** Phase 5.
- `live-gateway` WebSocket service subscribing to Kafka and pushing to browsers.
- Next.js + TypeScript frontend: live game view, historical browser, platform health page.
- Deploy to Vercel, wire to production backend.

**Milestone:** public URL where anyone can watch live pitch-by-pitch data, powered by the full pipeline.

### Phase 7 — CI/CD + documentation
**Depends on:** Phase 6.
- GitHub Actions: test, lint, build, deploy on merge.
- Separate staging and production namespaces with promotion workflow.
- README with architecture diagram, setup instructions, runbook for common failures.
- Benchmark suite measuring p99 latency (split: upstream / platform / end-to-end), throughput under load.
- Concrete numbers captured for resume bullets.

**Milestone:** merging a PR auto-deploys to staging; real benchmarks documented in the repo.

### Phase 8 (optional stretch) — ClickHouse + advanced analytics
**Depends on:** Phase 7.
- Add ClickHouse for analytical queries over full event history.
- Build analytical endpoints (e.g., "every 3-2 count pitch in the 9th inning this season").
- Only if time permits and it unlocks a consumer use case.

---

## Follow-on (separate project): betting model integration

Not part of this PRD. Lives in its own repo. Once the platform ships through Phase 7, the betting model integration looks like:

1. Betting model adds a Kafka consumer subscribing to `mlb.pitches.enriched`.
2. On each event, model updates in-memory state and recomputes win probability.
3. Model logs predictions to its own Postgres tables with timestamps.
4. Separate polling service pulls lines from the-odds-api ($30/month tier) to its own tables.
5. CLV analysis job joins predictions against line movements to measure whether model probabilities predict market settlement.

Platform requires zero changes to support this. The entire betting extension sits on top of existing Kafka topics.

---

## Stack summary

| Layer | Choice | Why |
|---|---|---|
| Backend language | Python | Existing sports code integrates immediately; ships faster; no language learning curve |
| Message bus | Kafka (Redpanda locally) | Industry standard; partition semantics fit per-game ordering |
| Durable storage | PostgreSQL | Already in ecosystem via Supabase; reliable, familiar |
| Hot cache + cursor | Redis | Sub-ms lookups for current game state; durable enough for ingestor cursor |
| Internal RPC | gRPC + protobuf | Schema-first, strong contracts, easy to evolve |
| External API | grpc-gateway REST | Single source of truth with gRPC, auto-generated |
| Live push | WebSocket via dedicated gateway service | Clean separation between internal Kafka and external clients |
| Frontend | Next.js + TypeScript | Production-grade React, SSR, deploys cleanly to Vercel |
| Orchestration | DigitalOcean Kubernetes (DOKS) | Real cloud deployment, no clock on free credits, ~$55/mo all-in |
| Autoscaling | KEDA + built-in HPA | Built-in HPA can't scale on Kafka consumer lag; KEDA can |
| Deployment | Helm + Terraform | Standard IaC patterns |
| Metrics | Prometheus + Grafana | De facto standard |
| Tracing | OpenTelemetry + Jaeger | Vendor-neutral, future-proof |
| CI/CD | GitHub Actions | Native to the repo |
| Container registry | GitHub Container Registry | Free, integrates with Actions |

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Scope creep kills the project | Strict phase gating; each phase ends with a working milestone before starting the next |
| MLB/NFL off-season removes live data | Replay service (Phase 3) means year-round development is unaffected; add NBA ingestor if needed |
| `nfl_data_py` underlying sources break or go stale | NFL framed as near-live batch from day one (no overclaiming); ESPN endpoints documented as fallback path |
| Cloud cluster costs spiral | Pods scaled to zero when not actively testing; managed Postgres/Redis on smallest tiers |
| Python concurrency limits under load | If throughput becomes a real problem (unlikely at this scale), rewrite only the hottest service in Go as a targeted optimization |
| Breaking schema changes break consumers | Versioned topics (v1, v2) for breaking changes; additive-only in place; `schemas/EVOLUTION.md` enforces the rules |
| Trying to build betting model in parallel | Explicitly out of scope until Phase 7 complete; enforce separation of repos |
| Ingestor restart causes duplicates or gaps | Cursor persisted to Redis after every successful publish; consumer-side dedup by `event_id` catches any duplicates |

---

## Explicitly out of scope

- Betting execution, bet placement, real money.
- Sharp-book-beating latency (bounded by public APIs).
- Multi-region deployment, service mesh, GitOps (Istio/ArgoCD).
- ML models running inside the platform.
- NBA coverage (unless off-season forces it).
- Public multi-tenant access. Platform is single-operator.
