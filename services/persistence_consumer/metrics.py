"""Prometheus metrics for the persistence consumer.

Shape chosen so a single Grafana panel can answer each of:

- Are we writing? `messages_total{outcome="written"}` rate
- Are we drowning? `lag{partition=*}` gauge (computed from highwater - committed)
- Are we blackholing? `dlq_total{reason=*}` rate
- Are we stuck? `paused_partitions` gauge + `partition_pauses_total` counter
- Are we slow? `process_duration_seconds` histogram p95/p99
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

messages_total = Counter(
    "persistence_consumer_messages_total",
    "Messages processed by outcome.",
    labelnames=("outcome",),  # written | duplicate | dlq | retry_exhausted
)

dlq_total = Counter(
    "persistence_consumer_dlq_total",
    "Messages pushed to the DLQ, by reason.",
    labelnames=("reason",),  # poison_pill | retry_exhausted
)

partition_pauses_total = Counter(
    "persistence_consumer_partition_pauses_total",
    "Partitions paused after a failure, by reason.",
    labelnames=("topic", "partition", "reason"),
)

paused_partitions = Gauge(
    "persistence_consumer_paused_partitions",
    "Number of partitions currently paused due to retry-budget exhaustion.",
)

consumer_lag = Gauge(
    "persistence_consumer_lag",
    "Consumer lag in messages, per (topic, partition). Computed periodically.",
    labelnames=("topic", "partition"),
)

process_duration = Histogram(
    "persistence_consumer_process_duration_seconds",
    "End-to-end per-message processing time, including retries.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
