"""aiokafka consumer loop.

Threads together:
- Manual offset commits (auto-commit is off): commit only after a successful
  DB write so a crash mid-process re-delivers the message, and the DB's
  ON CONFLICT DO NOTHING makes the re-delivery harmless.
- Partition pause on retry-budget exhaustion: the message stays uncommitted
  so when the partition resumes (manual intervention in Phase 3; automatic
  backoff-resume is a Phase 6 concern), it's re-delivered.
- DLQ on poison pill: envelope published, original offset committed.
- Periodic lag reporting: background task computes `highwater - committed`
  per partition and exports as a Gauge.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import ConsumerRecord

from services.common.logging import get_logger
from services.persistence_consumer import metrics
from services.persistence_consumer.dlq import DLQPublisher
from services.persistence_consumer.errors import (
    PoisonPillError,
    RetryBudgetExhaustedError,
)
from services.persistence_consumer.handler import MessageHandler

_logger = get_logger(__name__)

_LAG_REPORT_INTERVAL_SECONDS = 10.0


class PersistenceConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        handler: MessageHandler,
        dlq: DLQPublisher,
        source_topic: str,
    ):
        self._consumer = consumer
        self._producer = producer
        self._handler = handler
        self._dlq = dlq
        self._source_topic = source_topic
        self._paused: set[TopicPartition] = set()
        self._lag_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._consumer.start()
        await self._producer.start()
        self._lag_task = asyncio.create_task(self._report_lag_periodically())
        _logger.info("persistence_consumer_started", topic=self._source_topic)

    async def stop(self) -> None:
        if self._lag_task is not None:
            self._lag_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._lag_task
        await self._consumer.stop()
        await self._producer.stop()
        _logger.info("persistence_consumer_stopped")

    async def run(self, stop_event: asyncio.Event) -> None:
        """Consume until `stop_event` is set. Commits after every successful write."""
        while not stop_event.is_set():
            records = await self._consumer.getmany(timeout_ms=1000, max_records=10)
            for tp, msgs in records.items():
                for msg in msgs:
                    await self._process_one(tp, msg)

    async def _process_one(self, tp: TopicPartition, msg: ConsumerRecord) -> None:
        with metrics.process_duration.time():
            try:
                outcome = await self._handler.handle(msg.value or b"")
            except PoisonPillError as exc:
                _logger.warning(
                    "poison_pill",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    error=repr(exc),
                )
                await self._dlq.publish(
                    msg,
                    error_class="poison_pill",
                    error_message=str(exc),
                    attempts=1,
                )
                metrics.dlq_total.labels(reason="poison_pill").inc()
                metrics.messages_total.labels(outcome="dlq").inc()
                await self._commit(tp, msg.offset)
                return
            except RetryBudgetExhaustedError as exc:
                _logger.error(
                    "partition_paused",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    reason="retry_budget_exhausted",
                    last_error=repr(exc.last_error),
                )
                metrics.partition_pauses_total.labels(
                    topic=msg.topic,
                    partition=str(msg.partition),
                    reason="retry_budget_exhausted",
                ).inc()
                metrics.messages_total.labels(outcome="retry_exhausted").inc()
                self._pause(tp)
                return

            metrics.messages_total.labels(outcome=outcome.value).inc()
            await self._commit(tp, msg.offset)

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        # Commit the next offset to read (Kafka convention).
        await self._consumer.commit({tp: offset + 1})

    def _pause(self, tp: TopicPartition) -> None:
        if tp in self._paused:
            return
        self._consumer.pause(tp)
        self._paused.add(tp)
        metrics.paused_partitions.set(len(self._paused))

    async def _report_lag_periodically(self) -> None:
        while True:
            await asyncio.sleep(_LAG_REPORT_INTERVAL_SECONDS)
            try:
                await self._refresh_lag(self._consumer.assignment())
            except Exception as exc:  # noqa: BLE001
                _logger.warning("lag_report_failed", error=repr(exc))

    async def _refresh_lag(self, partitions: Iterable[TopicPartition]) -> None:
        for tp in partitions:
            highwater = self._consumer.highwater(tp)
            position = await self._consumer.position(tp)
            if highwater is None or position is None:
                continue
            lag = max(highwater - position, 0)
            metrics.consumer_lag.labels(topic=tp.topic, partition=str(tp.partition)).set(lag)
