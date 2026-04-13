"""Optional Sentry init.

Sentry is a no-op unless SENTRY_DSN is set. This keeps local dev zero-config
while still catching exceptions in staging/prod.
"""

from __future__ import annotations

import sentry_sdk

from services.common.logging import get_logger

_logger = get_logger(__name__)


def init_sentry(dsn: str, environment: str, service_name: str) -> None:
    if not dsn:
        _logger.info("sentry_disabled", reason="no_dsn")
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        traces_sample_rate=0.0,
        send_default_pii=False,
    )
    sentry_sdk.set_tag("service", service_name)
    _logger.info("sentry_initialized", environment=environment, service=service_name)
