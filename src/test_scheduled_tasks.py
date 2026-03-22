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

    monkeypatch.setattr("src.scraper.runner.run_with_db", _explode)

    from src.scheduled_tasks import run_daily_delta
    run_daily_delta()  # must not raise

    assert len(_FakeSMTP.instances) == 1
    smtp = _FakeSMTP.instances[0]
    assert len(smtp.sent) == 1
    _, _, raw_msg = smtp.sent[0]
    assert "FAILED" in raw_msg
