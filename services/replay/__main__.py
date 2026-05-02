"""Entry point: `python -m services.replay --game-pk <pk> --speed <n>`."""

from __future__ import annotations

import argparse
import asyncio

from services.common.config import Config
from services.common.logging import configure_logging, get_logger
from services.common.sentry import init_sentry
from services.replay.publisher import ReplayPublisher
from services.replay.reader import EventReader
from services.replay.service import ReplayService


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay PG events into Kafka")
    parser.add_argument("--game-pk", required=True)
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Speed multiplier (1, 10, 100). Default 1x.",
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    config = Config.load()
    configure_logging(config.log_level)
    init_sentry(config.sentry_dsn, config.sentry_environment, "replay")

    log = get_logger(__name__)
    log.info("replay_starting", game_pk=args.game_pk, speed=args.speed)

    reader = await EventReader.connect(config.database_url)
    publisher = await ReplayPublisher.connect(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=f"{config.kafka_client_id}-replay",
        topic=config.kafka_topic_mlb_raw,
    )
    service = ReplayService(reader, publisher, speed=args.speed)
    try:
        await service.replay_game(args.game_pk)
    finally:
        await publisher.close()
        await reader.close()


if __name__ == "__main__":
    asyncio.run(_main())
