# -*- coding: utf-8 -*-
"""Scheduled background tasks for the Office Holder app.

The daily delta + live-person run is triggered by APScheduler at 06:00 UTC.
On completion, a summary email is sent to EMAIL_TO via Gmail SMTP.

Required env var (for email):
    EMAIL_APP_PASSWORD  — Gmail App Password (myaccount.google.com/apppasswords)

Optional env vars:
    EMAIL_FROM          — sender address (default: wcmchenry3@gmail.com)
    EMAIL_TO            — recipient address (default: wcmchenry3@gmail.com)
    DAILY_DELTA_ENABLED — set to 0/false/no/off to pause daily job (default: enabled)
"""

from __future__ import annotations

import os
import json
import smtplib
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

_DEFAULT_EMAIL = "wcmchenry3@gmail.com"


def is_daily_delta_enabled() -> bool:
    """Return True unless DAILY_DELTA_ENABLED is set to a false-like value."""
    raw = os.environ.get("DAILY_DELTA_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _run_daily_delta_in_subprocess(today_batch: int) -> dict:
    """Run scraper in a child process so memory is fully released when job ends."""
    payload = f"""
import json
from src.scraper.runner import run_with_db

result = run_with_db(
    run_mode="delta",
    run_bio=True,
    run_office_bio=True,
    bio_batch={today_batch},
)
print(json.dumps(result))
"""
    completed = subprocess.run(
        [sys.executable, "-c", payload],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr or stdout or "subprocess exited with non-zero status"
        raise RuntimeError(details)
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise RuntimeError("subprocess returned no output")
    last_line = stdout.splitlines()[-1]
    try:
        parsed = json.loads(last_line)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid subprocess JSON output: {last_line[:300]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("subprocess result was not a dict")
    return parsed


def run_daily_delta() -> None:
    """Entry point called by APScheduler at 06:00 UTC each day."""
    if not is_daily_delta_enabled():
        print("[scheduler] Daily delta run skipped because DAILY_DELTA_ENABLED is disabled")
        return

    try:
        from src.db.scraper_jobs import count_active_jobs

        active = count_active_jobs()
        if active > 0:
            print(
                f"[scheduler] Daily delta run skipped: {active} job(s) already running or queued."
            )
            return
    except Exception as e:
        print(f"[scheduler] Could not check active jobs (non-fatal): {e}")

    from src.scraper.runner import _cleanup_disk_cache

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.weekday()  # 0=Mon … 6=Sun
    print(
        f"[scheduler] Daily delta run starting at {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC (bio_batch={today_batch})"
    )

    try:
        cache_deleted = _cleanup_disk_cache(max_age_days=30)
    except Exception as e:
        cache_deleted = 0
        print(f"[scheduler] Cache cleanup error (non-fatal): {e}")

    try:
        from src.db.scraper_jobs import delete_jobs_older_than

        jobs_deleted = delete_jobs_older_than(hours=48)
        if jobs_deleted:
            print(f"[scheduler] Deleted {jobs_deleted} stale scraper_jobs records.")
    except Exception as e:
        print(f"[scheduler] scraper_jobs cleanup error (non-fatal): {e}")

    try:
        result = _run_daily_delta_in_subprocess(today_batch=today_batch)
        result["cache_deleted"] = cache_deleted
    except Exception:
        tb = traceback.format_exc()
        print(f"[scheduler] Daily run crashed:\n{tb}")
        _send_summary_email(None, 0.0, run_start, error=tb, cache_deleted=cache_deleted)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"[scheduler] Daily run complete in {duration_s:.0f}s — sending summary email")
    _send_summary_email(result, duration_s, run_start)


def _run_mode_in_subprocess(run_mode: str, today_batch: int) -> dict:
    """Run a specific scraper mode in a child process so memory is fully released."""
    payload = f"""
import json
from src.scraper.runner import run_with_db

result = run_with_db(
    run_mode="{run_mode}",
    bio_batch={today_batch},
)
print(json.dumps(result))
"""
    completed = subprocess.run(
        [sys.executable, "-c", payload],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr or stdout or "subprocess exited with non-zero status"
        raise RuntimeError(details)
    stdout = (completed.stdout or "").strip()
    if not stdout:
        raise RuntimeError("subprocess returned no output")
    last_line = stdout.splitlines()[-1]
    try:
        parsed = json.loads(last_line)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid subprocess JSON output: {last_line[:300]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("subprocess result was not a dict")
    return parsed


def run_daily_insufficient_vitals() -> None:
    """Entry point called by APScheduler at 07:00 UTC each day."""
    try:
        from src.db.scraper_jobs import count_active_jobs

        if count_active_jobs() > 0:
            print("[scheduler] Insufficient vitals run skipped: jobs already active.")
            return
    except Exception as e:
        print(f"[scheduler] Could not check active jobs (non-fatal): {e}")

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.day % 30
    print(
        f"[scheduler] Insufficient vitals run starting at {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC (batch={today_batch})"
    )

    try:
        result = _run_mode_in_subprocess("delta_insufficient_vitals", today_batch)
    except Exception:
        tb = traceback.format_exc()
        print(f"[scheduler] Insufficient vitals run crashed:\n{tb}")
        _send_job_summary_email("Insufficient Vitals", None, 0.0, run_start, error=tb)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"[scheduler] Insufficient vitals run complete in {duration_s:.0f}s")
    _send_job_summary_email("Insufficient Vitals", result, duration_s, run_start)


def run_daily_gemini_research() -> None:
    """Entry point called by APScheduler at 08:00 UTC each day."""
    try:
        from src.db.scraper_jobs import count_active_jobs

        if count_active_jobs() > 0:
            print("[scheduler] Gemini research run skipped: jobs already active.")
            return
    except Exception as e:
        print(f"[scheduler] Could not check active jobs (non-fatal): {e}")

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.day % 30
    print(
        f"[scheduler] Gemini research run starting at {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC (batch={today_batch})"
    )

    try:
        result = _run_mode_in_subprocess("gemini_vitals_research", today_batch)
    except Exception:
        tb = traceback.format_exc()
        print(f"[scheduler] Gemini research run crashed:\n{tb}")
        _send_job_summary_email("Gemini Research", None, 0.0, run_start, error=tb)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"[scheduler] Gemini research run complete in {duration_s:.0f}s")
    _send_job_summary_email("Gemini Research", result, duration_s, run_start)


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


def _send_job_summary_email(
    job_name: str,
    result: dict | None,
    duration_s: float,
    run_start: datetime,
    error: str | None = None,
) -> None:
    """Generic job summary email for scheduled tasks (vitals recheck, Gemini research, etc.)."""
    app_password = os.environ.get("EMAIL_APP_PASSWORD", "")
    if not app_password:
        print(f"[scheduler] EMAIL_APP_PASSWORD not set — skipping {job_name} email")
        return

    email_from = os.environ.get("EMAIL_FROM", _DEFAULT_EMAIL)
    email_to = os.environ.get("EMAIL_TO", _DEFAULT_EMAIL)
    date_str = run_start.strftime("%Y-%m-%d")
    started_str = run_start.strftime("%H:%M:%S UTC")

    if error or result is None:
        status = "FAILED"
        body = (
            f"Job       : {job_name}\n"
            f"Run date  : {date_str}\n"
            f"Started   : {started_str}\n"
            f"Status    : FAILED\n\n"
            f"Error:\n{error or 'Unknown error'}\n"
        )
    else:
        status = "Complete"
        lines = [
            f"Job       : {job_name}",
            f"Run date  : {date_str}",
            f"Started   : {started_str}",
            f"Duration  : {_format_duration(duration_s)}",
            "Status    : Complete",
            "",
            "RESULTS",
            "-------",
        ]
        # Include relevant result keys
        for key, val in sorted(result.items()):
            if key in ("preview_rows", "dry_run", "test_run"):
                continue
            lines.append(f"  {key}: {val}")

        all_errors = result.get("bio_errors") or []
        all_errors += result.get("living_errors") or []
        lines.append("")
        lines.append("ERRORS")
        lines.append("------")
        lines.append(_format_errors(all_errors))
        body = "\n".join(lines)

    subject = f"Office Holder {job_name} — {date_str} — {status}"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_from, app_password)
            smtp.sendmail(email_from, [email_to], msg.as_string())
        print(f"[scheduler] {job_name} email sent to {email_to}")
    except Exception as exc:
        print(f"[scheduler] Failed to send {job_name} email: {exc}")


def _send_summary_email(
    result: dict | None,
    duration_s: float,
    run_start: datetime,
    error: str | None = None,
    cache_deleted: int = 0,
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

See attached log file for the full crash output.
"""
        crash_log = (error or "Unknown error — result was None").encode("utf-8")
    else:
        office_count = result.get("office_count", 0)
        unchanged = result.get("offices_unchanged", 0)
        processed = office_count - unchanged
        terms = result.get("terms_parsed", 0)
        bio_ok = result.get("bio_success_count", 0)
        bio_fail = result.get("bio_error_count", 0)
        living_ok = result.get("living_success_count", 0)
        living_fail = result.get("living_error_count", 0)
        bio_errors = result.get("bio_errors") or []
        living_errors = result.get("living_errors") or []
        office_errors = result.get("office_errors") or []
        cancelled = result.get("cancelled", False)
        bio_batch_val = result.get("cache_deleted", cache_deleted)
        status = "✗ CANCELLED" if cancelled else "✓ Complete"

        all_errors = (
            bio_errors
            + living_errors
            + [
                {"url": e, "error": "office-level error"}
                for e in office_errors
                if isinstance(e, str)
            ]
        )

        body = f"""\
Run date  : {date_str}
Started   : {started_str}
Duration  : {_format_duration(duration_s)}
Status    : {status}

SUMMARY
-------
Offices total        : {office_count}
Offices processed    : {processed}
Offices unchanged    : {unchanged}
Terms parsed         : {terms}
Bio updates OK       : {bio_ok}
Bio errors           : {bio_fail}
Living bio refreshed : {living_ok}
Living bio errors    : {living_fail}
Cache files deleted  : {bio_batch_val}

ERRORS
------
{_format_errors(all_errors)}
"""

    subject = f"Office Holder Daily Run — {date_str} — {status}"

    if error or result is None:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = email_to
        msg.attach(MIMEText(body, "plain", "utf-8"))
        attachment = MIMEBase("text", "plain", charset="utf-8")
        attachment.set_payload(crash_log)
        encoders.encode_base64(attachment)
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=f"crash_{date_str}.log",
        )
        msg.attach(attachment)
    else:
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
