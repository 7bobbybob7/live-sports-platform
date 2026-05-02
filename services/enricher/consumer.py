"""aiokafka consumer loop for the enricher.

Consumes raw pitch events, hands each to the enricher, publishes the
resulting enriched event, and commits the source offset only after the
enriched publish is acked.
"""

from __future__ import annotations

import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import ConsumerRecord

from services.common.logging import get_logger
from services.enricher.handler import EnrichmentError, MessageHandler

_logger = get_logger(__name__)


class Enricher:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        handler: MessageHandler,
        source_topic: str,
        sink_topic: str,
    ):
        self._consumer = consumer
        self._producer = producer
        self._handler = handler
        self._source_topic = source_topic
        self._sink_topic = sink_topic

    async def start(self) -> None:
        await self._consumer.start()
        await self._producer.start()
        _logger.info(
            "enricher_started", source=self._source_topic, sink=self._sink_topic
        )

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()
        _logger.info("enricher_stopped")

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            records = await self._consumer.getmany(timeout_ms=1000, max_records=10)
            for tp, msgs in records.items():
                for msg in msgs:
                    await self._process_one(tp, msg)

    async def _process_one(self, tp: TopicPartition, msg: ConsumerRecord) -> None:
        try:
            key, payload = await self._handler.handle(msg.value or b"")
        except EnrichmentError as exc:
            _logger.warning(
                "enrichment_failed",
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
                error=repr(exc),
            )
            await self._commit(tp, msg.offset)
            return

        await self._producer.send_and_wait(self._sink_topic, payload, key=key)
        await self._commit(tp, msg.offset)

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        await self._consumer.commit({tp: offset + 1})
