"""Environment-driven config, loaded once at startup.

Keep this dumb: read env vars, coerce types, fail fast on missing required
values. No business logic, no defaults that hide misconfiguration in prod.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Self

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv(".env")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required env var {name} is not set")
    return value


def _optional(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class Config:
    database_url: str
    redis_url: str

    mlb_stats_api_base: str
    mlb_poll_interval_seconds: int
    mlb_schedule_poll_interval_seconds: int

    query_api_host: str
    query_api_port: int

    kafka_bootstrap_servers: str
    kafka_client_id: str
    kafka_group_persistence: str
    kafka_topic_mlb_raw: str
    kafka_topic_mlb_raw_dlq: str

    log_level: str
    metrics_port_ingestor: int
    metrics_port_query_api: int
    metrics_port_persistence_consumer: int

    sentry_dsn: str
    sentry_environment: str

    @classmethod
    def load(cls) -> Self:
        return cls(
            database_url=_require("DATABASE_URL"),
            redis_url=_optional("REDIS_URL", "redis://localhost:6379/0"),
            mlb_stats_api_base=_optional(
                "MLB_STATS_API_BASE", "https://statsapi.mlb.com/api/v1"
            ),
            mlb_poll_interval_seconds=_int("MLB_POLL_INTERVAL_SECONDS", 5),
            mlb_schedule_poll_interval_seconds=_int(
                "MLB_SCHEDULE_POLL_INTERVAL_SECONDS", 60
            ),
            query_api_host=_optional("QUERY_API_HOST", "0.0.0.0"),
            query_api_port=_int("QUERY_API_PORT", 8080),
            kafka_bootstrap_servers=_optional(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            kafka_client_id=_optional("KAFKA_CLIENT_ID", "live-sports-platform"),
            kafka_group_persistence=_optional(
                "KAFKA_GROUP_PERSISTENCE", "persistence-consumer"
            ),
            kafka_topic_mlb_raw=_optional("KAFKA_TOPIC_MLB_RAW", "mlb.events.raw"),
            kafka_topic_mlb_raw_dlq=_optional(
                "KAFKA_TOPIC_MLB_RAW_DLQ", "mlb.events.raw.dlq"
            ),
            log_level=_optional("LOG_LEVEL", "INFO"),
            metrics_port_ingestor=_int("METRICS_PORT_INGESTOR", 9100),
            metrics_port_query_api=_int("METRICS_PORT_QUERY_API", 9101),
            metrics_port_persistence_consumer=_int(
                "METRICS_PORT_PERSISTENCE_CONSUMER", 9102
            ),
            sentry_dsn=_optional("SENTRY_DSN", ""),
            sentry_environment=_optional("SENTRY_ENVIRONMENT", "local"),
        )
