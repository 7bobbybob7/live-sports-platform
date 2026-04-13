"""Entry point: `python -m services.query_api`."""

from __future__ import annotations

import uvicorn

from services.common.config import Config


def main() -> None:
    config = Config.load()
    uvicorn.run(
        "services.query_api.app:app",
        host=config.query_api_host,
        port=config.query_api_port,
        log_config=None,  # structlog handles logs
    )


if __name__ == "__main__":
    main()
