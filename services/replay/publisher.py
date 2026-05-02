"""Kafka publisher for replayed events.

Publishes proto bytes keyed by `game_pk` so per-game ordering is preserved
on a single partition — same contract as the live ingestor.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer

from schemas.mlb import MLBPitchEvent


class ReplayPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic

    @classmethod
    async def connect(
        cls, bootstrap_servers: str, client_id: str, topic: str
    ) -> ReplayPublisher:
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
        await self._producer.send_and_wait(
            self._topic,
            event.SerializeToString(),
            key=event.spine.game_pk.encode("utf-8"),
        )
