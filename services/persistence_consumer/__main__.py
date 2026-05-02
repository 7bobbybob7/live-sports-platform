"""Entry point: `python -m services.persistence_consumer`."""

from __future__ import annotations

import asyncio
import signal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from services.common.config import Config
from services.common.logging import configure_logging, get_logger
from services.common.metrics import start_metrics_server
from services.common.sentry import init_sentry
from services.persistence_consumer.consumer import PersistenceConsumer
from services.persistence_consumer.dlq import DLQPublisher
from services.persistence_consumer.handler import MessageHandler
from services.persistence_consumer.storage import EventStore


async def _main() -> None:
    config = Config.load()
    configure_logging(config.log_level)
    init_sentry(config.sentry_dsn, config.sentry_environment, "persistence-consumer")
    start_metrics_server(config.metrics_port_persistence_consumer)

    log = get_logger(__name__)
    log.info("persistence_consumer_booting")

    store = await EventStore.connect(config.database_url)

    consumer = AIOKafkaConsumer(
        config.kafka_topic_mlb_raw,
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=config.kafka_client_id,
        group_id=config.kafka_group_persistence,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=f"{config.kafka_client_id}-dlq",
        enable_idempotence=True,
        acks="all",
    )

    handler = MessageHandler(store)
    dlq = DLQPublisher(producer, config.kafka_topic_mlb_raw_dlq)
    consumer_service = PersistenceConsumer(
        consumer=consumer,
        producer=producer,
        handler=handler,
        dlq=dlq,
        source_topic=config.kafka_topic_mlb_raw,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("shutdown_signal_received", signal=sig)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    try:
        await consumer_service.start()
        await consumer_service.run(stop_event)
    finally:
        await consumer_service.stop()
        await store.close()
        log.info("persistence_consumer_shutdown")


if __name__ == "__main__":
    asyncio.run(_main())
