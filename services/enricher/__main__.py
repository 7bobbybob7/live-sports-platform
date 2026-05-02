"""Entry point: `python -m services.enricher`."""

from __future__ import annotations

import asyncio
import signal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from redis.asyncio import Redis

from services.common.config import Config
from services.common.logging import configure_logging, get_logger
from services.common.metrics import start_metrics_server
from services.common.sentry import init_sentry
from services.enricher.consumer import Enricher
from services.enricher.handler import MessageHandler
from services.enricher.state import GameStateStore


async def _main() -> None:
    config = Config.load()
    configure_logging(config.log_level)
    init_sentry(config.sentry_dsn, config.sentry_environment, "enricher")
    start_metrics_server(config.metrics_port_enricher)

    log = get_logger(__name__)
    log.info("enricher_booting")

    redis = Redis.from_url(config.redis_url, decode_responses=True)
    store = GameStateStore(redis)

    consumer = AIOKafkaConsumer(
        config.kafka_topic_mlb_raw,
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=config.kafka_client_id,
        group_id=config.kafka_group_enricher,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=f"{config.kafka_client_id}-enricher",
        enable_idempotence=True,
        acks="all",
    )

    handler = MessageHandler(store)
    service = Enricher(
        consumer=consumer,
        producer=producer,
        handler=handler,
        source_topic=config.kafka_topic_mlb_raw,
        sink_topic=config.kafka_topic_mlb_enriched,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("shutdown_signal_received", signal=sig)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    try:
        await service.start()
        await service.run(stop_event)
    finally:
        await service.stop()
        await redis.aclose()
        log.info("enricher_shutdown")


if __name__ == "__main__":
    asyncio.run(_main())
