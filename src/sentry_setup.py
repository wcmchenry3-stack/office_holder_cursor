# -*- coding: utf-8 -*-
"""Sentry SDK initialization. Import and call init_sentry() once at startup."""

import logging
import os

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration


def _before_send(event: dict, hint: dict) -> dict | None:
    """Drop intentional budget-exhaustion events from Sentry.

    Defense-in-depth: scheduled job guards and balance probes already handle
    these exceptions before they can reach sentry_sdk.capture_exception(), but
    this filter ensures no stray RateLimitError / RESOURCE_EXHAUSTED events
    produce noise if one escapes through an unguarded path.
    """
    exc_info = hint.get("exc_info")
    if exc_info and exc_info[0] is not None:
        type_name = exc_info[0].__name__
        # openai.RateLimitError and anthropic.RateLimitError share the same __name__
        if type_name == "RateLimitError":
            return None
        # Gemini signals quota exhaustion via ClientError with RESOURCE_EXHAUSTED in the message
        if exc_info[1] is not None and "RESOURCE_EXHAUSTED" in str(exc_info[1]):
            return None
    return event


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
        before_send=_before_send,
    )
