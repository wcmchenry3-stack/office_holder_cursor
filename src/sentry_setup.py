# -*- coding: utf-8 -*-
"""Sentry SDK initialization. Import and call init_sentry() once at startup."""

import logging
import os

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration


def init_sentry() -> None:
    """Initialize Sentry SDK. No-ops gracefully when SENTRY_DSN is unset (local dev)."""
    dsn = os.environ.get("SENTRY_DSN")
    if not dsn:
        return

    environment = os.environ.get("APP_ENVIRONMENT", "dev")
    traces_rate = float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1"))

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        integrations=[
            FastApiIntegration(transaction_style="endpoint"),
            LoggingIntegration(
                level=logging.WARNING,
                event_level=logging.ERROR,
            ),
        ],
        traces_sample_rate=traces_rate,
        send_default_pii=False,
    )
