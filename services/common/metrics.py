"""Prometheus metrics server.

Every service exposes /metrics on its own port. In Phase 2 we add Grafana to
local compose and point it at these endpoints. In Phase 4 Prometheus in-cluster
scrapes them automatically via pod annotations.
"""

from __future__ import annotations

from prometheus_client import start_http_server

from services.common.logging import get_logger

_logger = get_logger(__name__)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    _logger.info("metrics_server_started", port=port)
