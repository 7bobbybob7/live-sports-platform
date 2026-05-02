"""Kafka publisher for raw MLB events.

Per PRD: ingestor publishes protobuf events to `mlb.events.raw`, keyed by
`game_pk` so per-game ordering is preserved on a single partition. Cursor
advance in `_poll_game_once` is gated on a successful publish ack.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer
from prometheus_client import Counter

from schemas.mlb import MLBPitchEvent
from services.common.logging import get_logger

_logger = get_logger(__name__)

EVENTS_PUBLISHED_KAFKA = Counter(
    "mlb_ingestor_kafka_published_total",
    "Events successfully acknowledged by the Kafka broker",
    labelnames=("event_type",),
)
PUBLISH_FAILURES = Counter(
    "mlb_ingestor_kafka_publish_failures_total",
    "Publish attempts that raised before ack",
    labelnames=("event_type", "error_class"),
)


class KafkaPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic

    @classmethod
    async def connect(
        cls, bootstrap_servers: str, client_id: str, topic: str
    ) -> KafkaPublisher:
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            enable_idempotence=True,
            acks="all",
        )
        await producer.start()
        return cls(producer, topic)

    async def close(self) -> None:
        await self._producer.stop()

    async def publish_pitch(self, event: MLBPitchEvent) -> None:
        """Publish a pitch event. Awaits broker ack; raises on failure."""
        key = event.spine.game_pk.encode("utf-8")
        payload = event.SerializeToString()
        try:
            await self._producer.send_and_wait(self._topic, payload, key=key)
        except Exception as exc:
            PUBLISH_FAILURES.labels(
                event_type="pitch", error_class=type(exc).__name__
            ).inc()
            raise
        EVENTS_PUBLISHED_KAFKA.labels(event_type="pitch").inc()
