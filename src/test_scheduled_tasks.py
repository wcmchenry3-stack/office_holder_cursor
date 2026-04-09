"""Tests for scheduled_tasks.py — daily delta run and email summary.

All SMTP calls are replaced with a FakeSMTP spy. No network access.

Run: pytest src/test_scheduled_tasks.py -v
"""

from __future__ import annotations

import smtplib
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# SMTP spy
# ---------------------------------------------------------------------------


class _FakeSMTP:
    """Context-manager SMTP stub that records sendmail calls."""

    instances: list["_FakeSMTP"] = []

    def __init__(self, *args, **kwargs):
        self.sent: list[tuple] = []
        _FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def login(self, user, password):
        pass

    def sendmail(self, from_addr, to_addrs, msg):
        self.sent.append((from_addr, to_addrs, msg))


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_send_summary_email_skips_when_no_password(monkeypatch):
    """_send_summary_email does nothing when EMAIL_APP_PASSWORD is not set."""
    monkeypatch.delenv("EMAIL_APP_PASSWORD", raising=False)

    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP_SSL", _FakeSMTP)

    from src.scheduled_tasks import _send_summary_email

    _send_summary_email({}, 10.0, _now())

    assert _FakeSMTP.instances == [], "SMTP_SSL should not be instantiated when no password"


def test_send_summary_email_sends_when_password_set(monkeypatch):
    """_send_summary_email sends one email containing summary fields."""
    monkeypatch.setenv("EMAIL_APP_PASSWORD", "fake-password")
    monkeypatch.setenv("EMAIL_FROM", "from@test.com")
    monkeypatch.setenv("EMAIL_TO", "to@test.com")

    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP_SSL", _FakeSMTP)

    from src.scheduled_tasks import _send_summary_email

    result = {
        "office_count": 10,
        "offices_unchanged": 3,
        "terms_parsed": 42,
        "bio_success_count": 5,
        "bio_error_count": 1,
    }
    _send_summary_email(result, 90.0, _now())

    assert len(_FakeSMTP.instances) == 1
    smtp = _FakeSMTP.instances[0]
    assert len(smtp.sent) == 1
    _, _, raw_msg = smtp.sent[0]

    # Body may be base64-encoded; parse with email stdlib to get plain text.
    import email as _email

    parsed = _email.message_from_string(raw_msg)
    body = parsed.get_payload(decode=True)
    if body is not None:
        body_text = body.decode("utf-8")
    else:
        body_text = parsed.get_payload()
    assert "Terms parsed" in body_text
    assert "42" in body_text


def test_run_daily_delta_sends_crash_email_on_exception(monkeypatch):
    """run_daily_delta catches scraper exceptions and emails the crash output."""
    monkeypatch.setenv("EMAIL_APP_PASSWORD", "fake-password")
    monkeypatch.setenv("EMAIL_FROM", "from@test.com")
    monkeypatch.setenv("EMAIL_TO", "to@test.com")

    _FakeSMTP.instances.clear()
    monkeypatch.setattr(smtplib, "SMTP_SSL", _FakeSMTP)

    def _explode(**kwargs):
        raise RuntimeError("scraper exploded")

    monkeypatch.setattr("src.scheduled_tasks._run_daily_delta_in_subprocess", _explode)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: 1)
    monkeypatch.setattr("src.db.scheduled_job_runs.finish_run", lambda *a, **kw: None)

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()  # must not raise

    assert len(_FakeSMTP.instances) == 1
    smtp = _FakeSMTP.instances[0]
    assert len(smtp.sent) == 1
    _, _, raw_msg = smtp.sent[0]
    assert "FAILED" in raw_msg


def test_is_runners_enabled_parses_false_values(monkeypatch):
    from src.scheduled_tasks import is_runners_enabled

    for raw in ("0", "false", "False", "NO", "off"):
        monkeypatch.setenv("RUNNERS_ENABLED", raw)
        assert is_runners_enabled() is False


def test_run_daily_delta_skips_when_runners_disabled(monkeypatch):
    monkeypatch.setenv("RUNNERS_ENABLED", "0")

    called = {"subprocess": False}

    def _should_not_run(**kwargs):
        called["subprocess"] = True
        return {}

    monkeypatch.setattr("src.scheduled_tasks._run_daily_delta_in_subprocess", _should_not_run)

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()
    assert not called["subprocess"]

    assert called["subprocess"] is False


# ---------------------------------------------------------------------------
# Active-job skip logic
# ---------------------------------------------------------------------------


def test_run_daily_delta_skips_when_active_job_running(monkeypatch):
    """run_daily_delta returns early without calling the subprocess when a job is active."""
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr("src.db.scraper_jobs.count_active_jobs", lambda: 1)
    monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", lambda: None)

    called = {"subprocess": False}

    def _should_not_run(**kwargs):
        called["subprocess"] = True
        return {}

    monkeypatch.setattr("src.scheduled_tasks._run_daily_delta_in_subprocess", _should_not_run)

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()

    assert not called["subprocess"]


def test_run_daily_delta_calls_expire_before_active_check(monkeypatch):
    """_expire_stale_jobs_with_email is called before the active-job count check."""
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)

    call_order: list[str] = []

    monkeypatch.setattr(
        "src.scheduled_tasks._expire_stale_jobs_with_email",
        lambda: call_order.append("expire"),
    )
    # Return 1 active job (triggers skip) AND record the call order
    monkeypatch.setattr(
        "src.db.scraper_jobs.count_active_jobs",
        lambda: call_order.append("count") or 1,
    )

    from src.scheduled_tasks import run_daily_delta

    run_daily_delta()

    assert "expire" in call_order
    assert "count" in call_order
    assert call_order.index("expire") < call_order.index("count")


# ---------------------------------------------------------------------------
# run_daily_maintenance
# ---------------------------------------------------------------------------


def test_run_daily_maintenance_always_calls_expiry(monkeypatch):
    """run_daily_maintenance calls _expire_stale_jobs_with_email even when RUNNERS_ENABLED=0."""
    monkeypatch.setenv("RUNNERS_ENABLED", "0")

    called = {"expire": False}
    monkeypatch.setattr(
        "src.scheduled_tasks._expire_stale_jobs_with_email",
        lambda: called.__setitem__("expire", True),
    )
    monkeypatch.setattr("src.db.scheduled_job_runs.expire_stale_scheduled_job_runs", lambda: 0)

    from src.scheduled_tasks import run_daily_maintenance

    run_daily_maintenance()

    assert called["expire"]


def test_run_daily_maintenance_ignores_job_pause_state(monkeypatch):
    """run_daily_maintenance never checks is_job_paused — it always runs."""
    pause_checked = {"checked": False}

    def _should_not_check(*a, **kw):
        pause_checked["checked"] = True
        return True  # paused — but maintenance should still run

    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", _should_not_check)
    monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", lambda: None)
    monkeypatch.setattr("src.db.scheduled_job_runs.expire_stale_scheduled_job_runs", lambda: 0)

    from src.scheduled_tasks import run_daily_maintenance

    run_daily_maintenance()

    assert not pause_checked["checked"]


def test_run_daily_maintenance_calls_expire_stale_scheduled_job_runs(monkeypatch):
    """run_daily_maintenance must call expire_stale_scheduled_job_runs() (#375)."""
    monkeypatch.setattr("src.scheduled_tasks._expire_stale_jobs_with_email", lambda: None)

    called = {"count": 0}

    def fake_expire():
        called["count"] += 1
        return 0

    monkeypatch.setattr("src.db.scheduled_job_runs.expire_stale_scheduled_job_runs", fake_expire)

    from src.scheduled_tasks import run_daily_maintenance

    run_daily_maintenance()

    assert (
        called["count"] == 1
    ), "expire_stale_scheduled_job_runs must be called once by maintenance"


# ---------------------------------------------------------------------------
# run_daily_page_quality
# ---------------------------------------------------------------------------


def _patch_page_quality_deps(monkeypatch, inspect_side_effect):
    """Patch all DB/external deps for run_daily_page_quality tests."""
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: 1)
    monkeypatch.setattr("src.db.scheduled_job_runs.finish_run", lambda *a, **kw: None)
    monkeypatch.setattr("src.services.page_quality_inspector.inspect_one_page", inspect_side_effect)


def test_run_daily_page_quality_records_run_in_db(monkeypatch):
    """run_daily_page_quality calls create_run and finish_run so the run is visible in the UI."""
    create_calls = []
    finish_calls = []

    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr(
        "src.db.scheduled_job_runs.create_run",
        lambda job_id, **kw: create_calls.append(job_id) or 1,
    )
    monkeypatch.setattr(
        "src.db.scheduled_job_runs.finish_run",
        lambda run_id, **kw: finish_calls.append(kw),
    )
    monkeypatch.setattr(
        "src.services.page_quality_inspector.inspect_one_page",
        lambda: {
            "result": "ok",
            "source_page_id": 1,
            "check_id": 1,
            "html_char_count": 100,
            "office_terms_count": 5,
        },
    )

    from src.scheduled_tasks import run_daily_page_quality

    run_daily_page_quality()

    assert create_calls == ["daily_page_quality"]
    assert len(finish_calls) == 1
    assert finish_calls[0]["status"] == "complete"


def test_run_daily_page_quality_skips_no_data_pages_and_retries(monkeypatch):
    """When inspect_one_page returns no_data, run_daily_page_quality tries the next page."""
    call_count = {"n": 0}

    def _inspect():
        call_count["n"] += 1
        if call_count["n"] < 3:
            return {
                "result": "no_data",
                "source_page_id": call_count["n"],
                "check_id": call_count["n"],
            }
        return {
            "result": "ok",
            "source_page_id": 99,
            "check_id": 99,
            "html_char_count": 200,
            "office_terms_count": 3,
        }

    finish_calls = []
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: 1)
    monkeypatch.setattr(
        "src.db.scheduled_job_runs.finish_run",
        lambda run_id, **kw: finish_calls.append(kw),
    )
    monkeypatch.setattr("src.services.page_quality_inspector.inspect_one_page", _inspect)

    from src.scheduled_tasks import run_daily_page_quality

    run_daily_page_quality()

    assert call_count["n"] == 3
    assert finish_calls[0]["status"] == "complete"
    assert finish_calls[0]["result"]["result"] == "ok"
    assert finish_calls[0]["result"]["attempts"] == 3


def test_run_daily_page_quality_records_skipped_no_data_after_max_attempts(monkeypatch):
    """If all 10 pages have no data, result is skipped_no_data — no crash, no wasted tokens."""
    from src.scheduled_tasks import _PAGE_QUALITY_MAX_ATTEMPTS

    finish_calls = []
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: 1)
    monkeypatch.setattr(
        "src.db.scheduled_job_runs.finish_run",
        lambda run_id, **kw: finish_calls.append(kw),
    )
    call_count = {"n": 0}

    def _always_no_data():
        call_count["n"] += 1
        return {"result": "no_data", "source_page_id": call_count["n"], "check_id": call_count["n"]}

    monkeypatch.setattr("src.services.page_quality_inspector.inspect_one_page", _always_no_data)

    from src.scheduled_tasks import run_daily_page_quality

    run_daily_page_quality()

    assert call_count["n"] == _PAGE_QUALITY_MAX_ATTEMPTS
    assert finish_calls[0]["result"]["result"] == "skipped_no_data"
    assert finish_calls[0]["result"]["attempts"] == _PAGE_QUALITY_MAX_ATTEMPTS


def test_run_daily_page_quality_stops_on_none_result(monkeypatch):
    """If inspect_one_page returns None (error/no pages), stop immediately."""
    call_count = {"n": 0}

    def _inspect():
        call_count["n"] += 1
        return None

    finish_calls = []
    monkeypatch.setenv("RUNNERS_ENABLED", "1")
    monkeypatch.setattr("src.db.scheduler_settings.is_job_paused", lambda *a, **kw: False)
    monkeypatch.setattr("src.db.scheduled_job_runs.create_run", lambda *a, **kw: 1)
    monkeypatch.setattr(
        "src.db.scheduled_job_runs.finish_run",
        lambda run_id, **kw: finish_calls.append(kw),
    )
    monkeypatch.setattr("src.services.page_quality_inspector.inspect_one_page", _inspect)

    from src.scheduled_tasks import run_daily_page_quality

    run_daily_page_quality()

    assert call_count["n"] == 1
    assert finish_calls[0]["result"]["result"] == "no_pages"


# ---------------------------------------------------------------------------
# Sentry subprocess instrumentation — verify real payload content
# ---------------------------------------------------------------------------


import subprocess
import json as _json
from unittest.mock import patch, MagicMock


def _make_completed_process(stdout: str, returncode: int = 0) -> "subprocess.CompletedProcess":
    cp = MagicMock(spec=subprocess.CompletedProcess)
    cp.returncode = returncode
    cp.stdout = stdout
    cp.stderr = ""
    return cp


def test_daily_delta_subprocess_payload_contains_sentry_instrumentation():
    """_run_daily_delta_in_subprocess payload must init Sentry, set context, and capture exceptions."""
    captured_payload: list[str] = []

    fake_result = _json.dumps({"office_count": 1, "terms_parsed": 0})

    def _fake_run(cmd, **kwargs):
        # cmd is [sys.executable, "-c", payload]
        captured_payload.append(cmd[2])
        return _make_completed_process(stdout=fake_result)

    with patch("src.scheduled_tasks.subprocess.run", side_effect=_fake_run):
        from src.scheduled_tasks import _run_daily_delta_in_subprocess

        _run_daily_delta_in_subprocess(today_batch=3)

    assert len(captured_payload) == 1
    payload = captured_payload[0]
    assert "sentry_sdk.init(" in payload
    assert 'set_tag("subprocess_job", "daily_delta")' in payload
    assert '"bio_batch": 3' in payload
    assert "sentry_sdk.capture_exception(_exc)" in payload
    assert "sentry_sdk.flush(timeout=5)" in payload


def test_run_mode_subprocess_payload_contains_sentry_instrumentation():
    """_run_mode_in_subprocess payload must init Sentry, set context, and capture exceptions."""
    for mode in ("delta_insufficient_vitals", "gemini_vitals_research"):
        captured_payload: list[str] = []

        fake_result = _json.dumps({"office_count": 0})

        def _fake_run(cmd, **kwargs):
            captured_payload.append(cmd[2])
            return _make_completed_process(stdout=fake_result)

        with patch("src.scheduled_tasks.subprocess.run", side_effect=_fake_run):
            from src.scheduled_tasks import _run_mode_in_subprocess

            _run_mode_in_subprocess(run_mode=mode, today_batch=15)

        assert len(captured_payload) == 1, f"expected one subprocess call for mode {mode}"
        payload = captured_payload[0]
        assert "sentry_sdk.init(" in payload, f"missing sentry_sdk.init for mode {mode}"
        assert f'set_tag("subprocess_job", "{mode}")' in payload
        assert f'"run_mode": "{mode}"' in payload
        assert '"bio_batch": 15' in payload
        assert "sentry_sdk.capture_exception(_exc)" in payload
        assert "sentry_sdk.flush(timeout=5)" in payload
