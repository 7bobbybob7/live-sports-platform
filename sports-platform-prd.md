# Real-Time Sports Data Platform — Final PRD

## One-line pitch
A distributed event-driven platform that ingests live MLB events, enriches them with game state, stores them durably, replays historical games, and serves downstream consumers through typed internal services and real-time external interfaces.

## Goal
Build a finished, measurable, deployed distributed-systems project that maximizes signal for big-tech SWE internship recruiting.

## Version 1 scope
V1 is intentionally constrained to **MLB only**.

The project should demonstrate:
- real-time ingestion
- event streaming
- schema evolution
- stateful processing
- durable + hot-state storage
- replay
- cloud deployment
- observability
- benchmarked performance
- documented failure behavior

## Non-goals
- NFL or multi-sport support in v1
- betting execution or broker integration
- public multi-tenant productization
- ClickHouse or warehouse analytics in v1
- Terraform
- Vercel
- service mesh / GitOps extras
- multi-region deployment
- full chaos-engineering platform

## Final architecture

### Core technologies
- **Language:** Python
- **Message bus:** Kafka
- **Schema format:** Protobuf
- **Durable storage:** PostgreSQL
- **Hot state / cursor store:** Redis
- **Internal RPC:** gRPC
- **External query interface:** REST
- **External live interface:** WebSocket
- **Containerization:** Docker
- **Deployment:** Kubernetes
- **Metrics / dashboards:** Prometheus + Grafana
- **Tracing:** OpenTelemetry on the hot path
- **CI:** GitHub Actions

### High-level service graph

MLB Stats API
→ `mlb-ingestor`
→ Kafka topic: `mlb.events.raw`
→ `enricher`
→ Kafka topic: `mlb.events.enriched`
→ `persistence-consumer` → PostgreSQL

Redis is used by:
- `mlb-ingestor` for cursor persistence
- `enricher` for hot current-game state

Downstream services:
- `query-api` for internal gRPC and external REST access
- `live-gateway` for WebSocket fanout
- `replay-service` to read historical events from PostgreSQL and republish into Kafka

### Why this architecture
- Kafka gives strong distributed-systems signal and clean decoupling.
- Protobuf provides a real schema-evolution story and justifies internal gRPC cheaply.
- PostgreSQL is the durable source of truth and replay source.
- Redis is narrowly scoped to cursor recovery and hot state.
- REST + WebSocket gives a simple, recruiter-readable external story.
- Internal gRPC provides typed service-to-service boundaries without bloating the external surface area.
- Kubernetes gives real deployment signal without unnecessary infra sprawl.

## Services

### 1. `mlb-ingestor`
Responsibilities:
- Poll MLB Stats API asynchronously for active games
- Diff last-seen state to emit only new events
- Derive deterministic `event_id`
- Stamp `event_time`, `source_time`, and `ingest_time`
- Persist per-game cursor to Redis after successful publish
- Publish raw protobuf events to Kafka

Key guarantees:
- restart-safe cursor recovery
- deterministic event identity
- no intentional duplicate emission on clean recovery

### 2. `enricher`
Responsibilities:
- Consume `mlb.events.raw`
- Maintain rolling per-game context
- Enrich each event with inning / outs / score / baserunners / pitcher-batter context
- Publish self-contained protobuf events to `mlb.events.enriched`
- Write hot state to Redis

Key guarantee:
- downstream consumers should not need extra DB lookups to react to a live event

### 3. `persistence-consumer`
Responsibilities:
- Consume enriched events
- Persist durable event history and metadata to PostgreSQL
- Enforce uniqueness at the DB layer through `event_id`

Reason to keep separate:
- cleaner fault boundaries
- easier write-path reasoning
- clearer interview story about service separation

### 4. `query-api`
Responsibilities:
- Serve historical and recent event queries from PostgreSQL
- Serve current-state reads from Redis when appropriate
- Expose:
  - internal gRPC for service-to-service querying
  - external REST for easy demoability and browser access

Representative external endpoints:
- `GET /games/latest`
- `GET /games/{gamePk}/events`
- `GET /games/{gamePk}/state`

### 5. `live-gateway`
Responsibilities:
- Subscribe to `mlb.events.enriched`
- Push live updates to browsers or clients over WebSocket

Reason to keep separate:
- isolates live fanout concerns from query logic
- keeps the architecture legible

### 6. `replay-service`
Responsibilities:
- Read historical events from PostgreSQL ordered by `(event_time, event_id)`
- Republish them into Kafka at configurable speeds such as 1x / 10x / 100x
- Preserve the same event contract as live flow

Why it matters:
- makes the system testable year-round
- enables deterministic demos and benchmarks
- gives strong distributed-systems and reliability signal

## Data model and contracts

### Canonical schema
Protobuf is the canonical schema layer for:
- Kafka messages
- internal gRPC contracts

### Event requirements
Every event must include:
- `event_id`
- `event_time`
- `source_time`
- `ingest_time`

All non-spine fields should be additive and forward-compatible where possible.

### Event identity
`event_id` must be:
- deterministic
- stable across reprocessing
- unique per logical event
- usable by consumers for deduplication

### Ordering model
Kafka partitions should use `game_pk` so processing is ordered per game.

## Storage

### PostgreSQL
Used for:
- durable event log
- replay source of truth
- historical queries
- game metadata

Required indexes:
- unique index on `event_id`
- composite index on `(game_pk, event_time, event_id)`
- index on `event_time`

### Redis
Used only for:
- ingestor cursor persistence
- hot current-game state

Redis should not become a general-purpose dumping ground.

## Observability

### Required
- Prometheus metrics on every service
- Grafana dashboards
- structured JSON logs with correlation IDs

### Required but scoped
- OpenTelemetry tracing only on the hot path:
  - `mlb-ingestor`
  - Kafka publish / consume boundary
  - `enricher`
  - `persistence-consumer` and/or `query-api`

Deliverable:
- one clean end-to-end trace showing a single event moving through the system
- one screenshot and one interview-ready explanation

## Performance reporting

Latency claims must be precisely defined.

### Standard definitions
- **Upstream latency** = `source_time - event_time`
- **Platform pipeline latency** = `enriched_publish_time - ingest_time`
- **Platform serving latency** = `served_time - ingest_time`
- **End-to-end latency** = `client_receive_time - event_time`

### Headline metric
Use **platform serving latency** as the main README / benchmark / interview metric.

Every benchmark report must include:
- p50 / p95 / p99
- throughput
- workload definition
- event source (live or replay)
- event mix
- hardware / cluster size
- number of runs

## Failure injection

This is a real deliverable, not a vague stretch goal.

### Goal
Document how the system behaves under targeted failure scenarios tied directly to your guarantees.

### Required scenarios
- kill `mlb-ingestor` mid-game
- kill `enricher` mid-game
- restart or flush Redis
- make PostgreSQL temporarily unavailable
- restart Kafka consumers

### Artifacts
- `chaos/` or `failure_injection/` scripts
- short writeup documenting:
  - failure injected
  - expected behavior
  - observed behavior
  - recovery time
  - duplicate / gap outcome
  - follow-up fixes if needed

This is intentionally targeted failure testing, not a huge chaos platform.

## Documentation deliverables

### 1. `README.md`
Must include:
- project overview
- architecture diagram
- local setup
- deployment overview
- demo path
- screenshots / links
- benchmark summary

### 2. `docs/DESIGN.md`
Must explain the **why**, not just the what.

Include:
- requirements and non-goals
- major architectural decisions
- why Kafka over simpler alternatives
- why protobuf over JSON
- why PostgreSQL + Redis
- why `persistence-consumer` is separate from `enricher`
- ordering and dedup model
- replay design
- tradeoffs and alternatives considered
- what you would change in v2

### 3. `docs/BENCHMARKS.md`
Must explain:
- latency definitions
- load generation methodology
- replay-based load testing
- API load testing harness
- cluster or hardware configuration
- benchmark tables
- limitations of results

## Deployment

### Keep
- Docker for local development
- Kubernetes for production deployment
- managed PostgreSQL
- managed Redis
- GitHub Actions for CI

### Optional
- Helm for deployment packaging if it stays lightweight

### Excluded
- Terraform
- Vercel
- staging / prod complexity beyond what is needed to deploy reliably

## Final phased build plan

### Phase 1 — Foundation
- schemas
- deterministic event IDs
- local Postgres + Redis
- migrations
- query API scaffold
- MLB ingestor scaffold
- unit tests
- structured logs
- Prometheus metrics

### Phase 2 — End-to-end live path
- prove live MLB ingestion into Postgres
- verify query API returns live data
- validate cursor recovery against real infra
- capture first live demo

### Phase 3 — Kafka split + canonical protobuf
- move event flow onto Kafka
- define protobuf schemas as source of truth
- add `persistence-consumer`
- preserve per-game ordering
- add DLQ handling

### Phase 4 — Enrichment + replay
- build `enricher`
- emit self-contained enriched events
- build `replay-service`
- validate replay is consumer-compatible with live flow

### Phase 5 — Deployment
- containerize all services
- deploy to Kubernetes
- wire managed Postgres and Redis
- add health checks and readiness probes
- add basic CI through GitHub Actions

### Phase 6 — Observability + benchmarks + failure injection
- Grafana dashboards
- hot-path OpenTelemetry tracing
- benchmark harness and `docs/BENCHMARKS.md`
- targeted failure-injection scripts and results writeup

### Phase 7 — Documentation + demo polish
- complete `README.md`
- complete `docs/DESIGN.md`
- add architecture diagram
- add screenshots / demo assets
- capture final numbers for resume bullets

## Final success criteria
The final project is successful if it can honestly support all of the following claims:
- live MLB ingestion works end-to-end
- event processing is ordered per game
- recovery from restarts does not create consumer-visible gaps
- replay works without downstream code changes
- the system is deployed to Kubernetes
- throughput and latency are benchmarked credibly
- failure behavior is documented
- architectural tradeoffs are documented clearly
