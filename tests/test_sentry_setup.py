# -*- coding: utf-8 -*-
"""Unit tests for src/sentry_setup.py — init_sentry."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.sentry_setup import init_sentry


def test_init_sentry_no_ops_without_dsn(monkeypatch):
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    with patch("sentry_sdk.init") as mock_init:
        init_sentry()
    mock_init.assert_not_called()


def test_init_sentry_calls_sdk_with_dsn(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "https://fake@sentry.io/123")
    monkeypatch.setenv("APP_ENVIRONMENT", "staging")
    with patch("sentry_sdk.init") as mock_init:
        init_sentry()
    mock_init.assert_called_once()
    kwargs = mock_init.call_args.kwargs
    assert kwargs["dsn"] == "https://fake@sentry.io/123"
    assert kwargs["environment"] == "staging"
    assert kwargs["send_default_pii"] is False


def test_init_sentry_default_environment_is_dev(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "https://fake@sentry.io/123")
    monkeypatch.delenv("APP_ENVIRONMENT", raising=False)
    with patch("sentry_sdk.init") as mock_init:
        init_sentry()
    kwargs = mock_init.call_args.kwargs
    assert kwargs["environment"] == "dev"


def test_init_sentry_traces_sample_rate_configurable(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "https://fake@sentry.io/123")
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.5")
    with patch("sentry_sdk.init") as mock_init:
        init_sentry()
    kwargs = mock_init.call_args.kwargs
    assert kwargs["traces_sample_rate"] == pytest.approx(0.5)
