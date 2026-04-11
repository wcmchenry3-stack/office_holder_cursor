# -*- coding: utf-8 -*-
"""Unit tests for src/scheduled_tasks.py.

Tests cover pure-function helpers and mocked scheduled job entry points.
All external I/O (DB, subprocess, SMTP) is mocked — no live calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import src.scheduled_tasks as st


# ---------------------------------------------------------------------------
# is_runners_enabled
# ---------------------------------------------------------------------------


class TestIsRunnersEnabled:
    def test_enabled_by_default(self, monkeypatch):
        monkeypatch.delenv("RUNNERS_ENABLED", raising=False)
        assert st.is_runners_enabled() is True

    def test_disabled_by_zero(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "0")
        assert st.is_runners_enabled() is False

    def test_disabled_by_false(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "false")
        assert st.is_runners_enabled() is False

    def test_disabled_by_no(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "no")
        assert st.is_runners_enabled() is False

    def test_disabled_by_off(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "off")
        assert st.is_runners_enabled() is False

    def test_enabled_by_one(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "1")
        assert st.is_runners_enabled() is True

    def test_enabled_by_true(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "true")
        assert st.is_runners_enabled() is True

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("RUNNERS_ENABLED", "FALSE")
        assert st.is_runners_enabled() is False


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_seconds_only(self):
        assert st._format_duration(45) == "45s"

    def test_minutes_and_seconds(self):
        assert st._format_duration(125) == "2m 5s"

    def test_exact_minute(self):
        assert st._format_duration(60) == "1m 0s"

    def test_zero(self):
        assert st._format_duration(0) == "0s"

    def test_large_value(self):
        assert st._format_duration(3661) == "61m 1s"


# ---------------------------------------------------------------------------
# _format_errors
# ---------------------------------------------------------------------------


class TestFormatErrors:
    def test_empty_returns_none_string(self):
        assert st._format_errors([]) == "None"

    def test_single_error_with_url(self):
        result = st._format_errors([{"url": "https://example.com", "error": "timeout"}])
        assert "https://example.com" in result
        assert "timeout" in result

    def test_uses_wiki_url_fallback(self):
        result = st._format_errors([{"wiki_url": "/wiki/Test", "error": "404"}])
        assert "/wiki/Test" in result

    def test_multiple_errors(self):
        errors = [
            {"url": "http://a.com", "error": "err1"},
            {"url": "http://b.com", "error": "err2"},
        ]
        result = st._format_errors(errors)
        assert "http://a.com" in result
        assert "http://b.com" in result

    def test_missing_url_uses_unknown(self):
        result = st._format_errors([{"error": "some error"}])
        assert "unknown" in result


# ---------------------------------------------------------------------------
# _has_active_scheduled_run
# ---------------------------------------------------------------------------


class TestHasActiveScheduledRun:
    def test_returns_false_when_no_active(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.scheduled_job_runs.count_active_scheduled_runs", lambda: 0
        )
        assert st._has_active_scheduled_run("daily_delta") is False

    def test_returns_true_when_active(self, monkeypatch):
        monkeypatch.setattr(
            "src.db.scheduled_job_runs.count_active_scheduled_runs", lambda: 1
        )
        assert st._has_active_scheduled_run("daily_delta") is True

    def test_returns_false_on_db_error(self, monkeypatch):
        def _raise():
            raise RuntimeError("db unreachable")

        monkeypatch.setattr(
            "src.db.scheduled_job_runs.count_active_scheduled_runs", _raise
        )
        # Should not raise — DB error is caught and returns False (non-fatal)
        assert st._has_active_scheduled_run("daily_delta") is False


# ---------------------------------------------------------------------------
# _send_summary_email / _send_job_summary_email — no-op without password
# ---------------------------------------------------------------------------


class TestEmailNoOp:
    def test_send_summary_email_skips_without_password(self, monkeypatch):
        monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)
        run_start = datetime.now(timezone.utc)
        # Should not raise and should not try to connect to SMTP
        with patch("smtplib.SMTP_SSL") as mock_smtp:
            st._send_summary_email({"terms_parsed": 5}, 30.0, run_start)
        mock_smtp.assert_not_called()

    def test_send_job_summary_email_skips_without_password(self, monkeypatch):
        monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)
        run_start = datetime.now(timezone.utc)
        with patch("smtplib.SMTP_SSL") as mock_smtp:
            st._send_job_summary_email("Gemini Research", {"count": 3}, 60.0, run_start)
        mock_smtp.assert_not_called()

    def test_send_expiry_email_skips_without_password(self, monkeypatch):
        monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)
        with patch("smtplib.SMTP_SSL") as mock_smtp:
            st._send_expiry_email({"id": "abc", "type": "delta", "status": "error"})
        mock_smtp.assert_not_called()

    def test_send_model_deprecated_email_skips_without_password(self, monkeypatch):
        monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)
        with patch("smtplib.SMTP_SSL") as mock_smtp:
            st._send_model_deprecated_email("gemini-pro", "Not found")
        mock_smtp.assert_not_called()


# ---------------------------------------------------------------------------
# run_daily_maintenance
# ---------------------------------------------------------------------------


class TestRunDailyMaintenance:
    def test_calls_expire_stale_jobs(self, monkeypatch):
        calls = {"expire_stale": 0, "expire_scheduled": 0}

        def _fake_expire_stale():
            calls["expire_stale"] += 1

        monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", _fake_expire_stale)
        monkeypatch.setattr(
            "src.db.scheduled_job_runs.expire_stale_scheduled_job_runs",
            lambda: 0,
        )

        st.run_daily_maintenance()
        assert calls["expire_stale"] == 1

    def test_logs_expired_scheduled_runs(self, monkeypatch, caplog):
        import logging

        monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", lambda: None)
        monkeypatch.setattr(
            "src.db.scheduled_job_runs.expire_stale_scheduled_job_runs",
            lambda: 3,
        )

        with caplog.at_level(logging.WARNING, logger="src.scheduled_tasks"):
            st.run_daily_maintenance()

        assert any("3" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# run_daily_delta — guard checks
# ---------------------------------------------------------------------------


class TestRunDailyDeltaGuards:
    def _patch_all(self, monkeypatch, runners_enabled=True, paused=False, active_jobs=0, active_scheduled=0):
        monkeypatch.setattr("src.scheduled_tasks.is_runners_enabled", lambda: runners_enabled)
        monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda job_id: paused)
        monkeypatch.setattr("src.db.scraper_jobs.count_active_jobs", lambda: active_jobs)
        monkeypatch.setattr(
            "src.db.scheduled_job_runs.count_active_scheduled_runs", lambda: active_scheduled
        )
        monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", lambda: None)
        monkeypatch.setattr("src.scraper.runner._cleanup_disk_cache", lambda **_: 0)
        monkeypatch.setattr("src.db.scraper_jobs.delete_jobs_older_than", lambda hours: 0)

    def test_skips_when_runners_disabled(self, monkeypatch):
        self._patch_all(monkeypatch, runners_enabled=False)
        created = []
        monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: created.append(a))
        st.run_daily_delta()
        assert created == []

    def test_skips_when_paused(self, monkeypatch):
        self._patch_all(monkeypatch, paused=True)
        created = []
        monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: created.append(a))
        st.run_daily_delta()
        assert created == []

    def test_skips_when_active_jobs(self, monkeypatch):
        self._patch_all(monkeypatch, active_jobs=1)
        created = []
        monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: created.append(a))
        st.run_daily_delta()
        assert created == []


# ---------------------------------------------------------------------------
# SCHEDULED_JOBS registry
# ---------------------------------------------------------------------------


class TestScheduledJobsRegistry:
    def test_registry_has_five_jobs(self):
        assert len(st.SCHEDULED_JOBS) == 5

    def test_daily_maintenance_not_pauseable(self):
        maint = next(j for j in st.SCHEDULED_JOBS if j["job_id"] == "daily_maintenance")
        assert maint["pauseable"] is False

    def test_all_other_jobs_pauseable(self):
        for job in st.SCHEDULED_JOBS:
            if job["job_id"] != "daily_maintenance":
                assert job["pauseable"] is True, f"{job['job_id']} should be pauseable"

    def test_all_jobs_have_required_keys(self):
        required = {"job_id", "label", "cron", "pauseable", "description"}
        for job in st.SCHEDULED_JOBS:
            assert required.issubset(job.keys()), f"Missing keys in {job}"
