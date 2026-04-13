"""Entry point: `python -m services.mlb_ingestor`."""

from __future__ import annotations

import asyncio
import signal

from services.common.config import Config
from services.common.logging import configure_logging, get_logger
from services.common.metrics import start_metrics_server
from services.common.sentry import init_sentry
from services.mlb_ingestor.ingestor import build_and_run


async def _main() -> None:
    config = Config.load()
    configure_logging(config.log_level)
    init_sentry(config.sentry_dsn, config.sentry_environment, "mlb-ingestor")
    start_metrics_server(config.metrics_port_ingestor)

    log = get_logger(__name__)
    log.info("mlb_ingestor_booting")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("shutdown_signal_received", signal=sig)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    run_task = asyncio.create_task(build_and_run(config))
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    for task in done:
        exc = task.exception()
        if exc:
            raise exc


if __name__ == "__main__":
    asyncio.run(_main())
