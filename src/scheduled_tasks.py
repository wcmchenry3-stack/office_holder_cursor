# -*- coding: utf-8 -*-
"""Scheduled background tasks for the Office Holder app.

The daily delta + live-person run is triggered by APScheduler at 06:00 UTC.
On completion, a summary email is sent to EMAIL_TO via Gmail SMTP.

Required env var (for email):
    EMAIL_APP_PASSWORD  — Gmail App Password (myaccount.google.com/apppasswords)

Optional env vars:
    EMAIL_FROM          — sender address (default: wcmchenry3@gmail.com)
    EMAIL_TO            — recipient address (default: wcmchenry3@gmail.com)
"""

from __future__ import annotations

import os
import smtplib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText

_DEFAULT_EMAIL = "wcmchenry3@gmail.com"


def run_daily_delta() -> None:
    """Entry point called by APScheduler at 06:00 UTC each day."""
    from src.scraper.runner import run_with_db

    run_start = datetime.now(timezone.utc)
    print(f"[scheduler] Daily delta run starting at {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    try:
        result = run_with_db(
            run_mode="delta",
            run_bio=True,
            run_office_bio=True,
        )
    except Exception:
        tb = traceback.format_exc()
        print(f"[scheduler] Daily run crashed:\n{tb}")
        _send_summary_email(None, 0.0, run_start, error=tb)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"[scheduler] Daily run complete in {duration_s:.0f}s — sending summary email")
    _send_summary_email(result, duration_s, run_start)


def _format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _format_errors(errors: list[dict]) -> str:
    if not errors:
        return "None"
    lines = []
    for e in errors:
        url = e.get("url") or e.get("wiki_url") or "unknown"
        msg = e.get("error") or e.get("message") or "unknown error"
        lines.append(f"  {url}\n    {msg}")
    return "\n".join(lines)


def _send_summary_email(
    result: dict | None,
    duration_s: float,
    run_start: datetime,
    error: str | None = None,
) -> None:
    """Format and send the daily run summary email via Gmail SMTP."""
    app_password = os.environ.get("EMAIL_APP_PASSWORD", "")
    if not app_password:
        print("[scheduler] EMAIL_APP_PASSWORD not set — skipping summary email")
        return

    email_from = os.environ.get("EMAIL_FROM", _DEFAULT_EMAIL)
    email_to = os.environ.get("EMAIL_TO", _DEFAULT_EMAIL)
    date_str = run_start.strftime("%Y-%m-%d")
    started_str = run_start.strftime("%H:%M:%S UTC")

    if error or result is None:
        status = "✗ FAILED"
        body = f"""\
Run date : {date_str}
Started  : {started_str}
Status   : FAILED

CRASH OUTPUT
------------
{error or 'Unknown error — result was None'}
"""
    else:
        office_count = result.get("office_count", 0)
        unchanged = result.get("offices_unchanged", 0)
        processed = office_count - unchanged
        terms = result.get("terms_parsed", 0)
        bio_ok = result.get("bio_success_count", 0)
        bio_fail = result.get("bio_error_count", 0)
        living_fail = result.get("living_error_count", 0)
        bio_errors = result.get("bio_errors") or []
        living_errors = result.get("living_errors") or []
        office_errors = result.get("office_errors") or []
        cancelled = result.get("cancelled", False)
        status = "✗ CANCELLED" if cancelled else "✓ Complete"

        all_errors = bio_errors + living_errors + [
            {"url": e, "error": "office-level error"} for e in office_errors if isinstance(e, str)
        ]

        body = f"""\
Run date  : {date_str}
Started   : {started_str}
Duration  : {_format_duration(duration_s)}
Status    : {status}

SUMMARY
-------
Offices total     : {office_count}
Offices processed : {processed}
Offices unchanged : {unchanged}
Terms parsed      : {terms}
Bio updates OK    : {bio_ok}
Bio errors        : {bio_fail + living_fail}

ERRORS
------
{_format_errors(all_errors)}
"""

    subject = f"Office Holder Daily Run — {date_str} — {status}"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_from, app_password)
            smtp.sendmail(email_from, [email_to], msg.as_string())
        print(f"[scheduler] Summary email sent to {email_to}")
    except Exception as exc:
        print(f"[scheduler] Failed to send summary email: {exc}")
