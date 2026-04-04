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

import logging
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

import sentry_sdk

logger = logging.getLogger(__name__)

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


def _expire_stale_jobs_with_email() -> None:
    """Expire stale jobs and send an email notification for each expired job."""
    try:
        from src.db.scraper_jobs import expire_stale_jobs

        expired = expire_stale_jobs()
        for job in expired:
            logger.info("Expired stale job: %s", job)
            _send_expiry_email(job)
    except Exception as e:
        logger.warning("Stale job expiry check failed (non-fatal): %s", e)


def _send_model_deprecated_email(model_name: str, error_msg: str) -> None:
    """Send an urgent email when the Gemini model is deprecated/not found."""
    app_password = os.environ.get("EMAIL_APP_PASSWORD", "")
    if not app_password:
        logger.warning("EMAIL_APP_PASSWORD not set — skipping model deprecated email")
        return

    email_from = os.environ.get("EMAIL_FROM", _DEFAULT_EMAIL)
    email_to = os.environ.get("EMAIL_TO", _DEFAULT_EMAIL)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = (
        f"ACTION REQUIRED: The Gemini model used for vitals research is no longer available.\n\n"
        f"  Model    : {model_name}\n"
        f"  Error    : {error_msg}\n"
        f"  Detected : {date_str}\n\n"
        f"Gemini research (both nightly and manual) is disabled until the model is updated.\n"
        f"Update the model in src/services/gemini_vitals_researcher.py and redeploy.\n\n"
        f"Available models: https://ai.google.dev/gemini-api/docs/models/gemini\n"
    )

    subject = f"Office Holder — URGENT: Gemini model '{model_name}' deprecated ({date_str})"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_from, app_password)
            smtp.sendmail(email_from, [email_to], msg.as_string())
        logger.info("Model deprecated email sent to %s", email_to)
    except Exception as exc:
        logger.warning("Failed to send model deprecated email: %s", exc)


def _send_expiry_email(job: dict) -> None:
    """Send an email notification when a job is expired."""
    app_password = os.environ.get("EMAIL_APP_PASSWORD", "")
    if not app_password:
        return

    email_from = os.environ.get("EMAIL_FROM", _DEFAULT_EMAIL)
    email_to = os.environ.get("EMAIL_TO", _DEFAULT_EMAIL)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = (
        f"A scraper job was automatically expired.\n\n"
        f"  Job ID   : {job.get('id', 'unknown')}\n"
        f"  Type     : {job.get('type', 'unknown')}\n"
        f"  Status   : {job.get('status', 'unknown')} → error\n"
        f"  Reason   : {job.get('reason', 'unknown')}\n"
        f"  Expired  : {date_str}\n"
    )

    subject = f"Office Holder — Job Expired — {job.get('type', 'unknown')} ({date_str})"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_from, app_password)
            smtp.sendmail(email_from, [email_to], msg.as_string())
        logger.info("Expiry email sent for job %s", job.get("id"))
    except Exception as exc:
        logger.warning("Failed to send expiry email: %s", exc)


def run_daily_delta() -> None:
    """Entry point called by APScheduler at 06:00 UTC each day."""
    sentry_sdk.set_tag("scheduled_task", "daily_delta")
    if not is_daily_delta_enabled():
        logger.info("Daily delta run skipped because DAILY_DELTA_ENABLED is disabled")
        return

    _expire_stale_jobs_with_email()

    try:
        from src.db.scraper_jobs import count_active_jobs

        active = count_active_jobs()
        if active > 0:
            logger.warning("Daily delta run skipped: %d job(s) already running or queued.", active)
            sentry_sdk.capture_message(
                f"Daily delta skipped: {active} active job(s)",
                level="warning",
            )
            return
    except Exception as e:
        logger.warning("Could not check active jobs (non-fatal): %s", e)

    from src.scraper.runner import _cleanup_disk_cache

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.weekday()  # 0=Mon … 6=Sun
    logger.info(
        "Daily delta run starting at %s UTC (bio_batch=%d)",
        run_start.strftime("%Y-%m-%d %H:%M:%S"),
        today_batch,
    )

    try:
        cache_deleted = _cleanup_disk_cache(max_age_days=30)
    except Exception as e:
        cache_deleted = 0
        logger.warning("Cache cleanup error (non-fatal): %s", e)

    try:
        from src.db.scraper_jobs import delete_jobs_older_than

        jobs_deleted = delete_jobs_older_than(hours=48)
        if jobs_deleted:
            logger.info("Deleted %d stale scraper_jobs records.", jobs_deleted)
    except Exception as e:
        logger.warning("scraper_jobs cleanup error (non-fatal): %s", e)

    try:
        result = _run_daily_delta_in_subprocess(today_batch=today_batch)
        result["cache_deleted"] = cache_deleted
    except Exception:
        sentry_sdk.capture_exception()
        tb = traceback.format_exc()
        logger.error("Daily run crashed:\n%s", tb)
        _send_summary_email(None, 0.0, run_start, error=tb, cache_deleted=cache_deleted)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    logger.info("Daily run complete in %.0fs — sending summary email", duration_s)
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
    sentry_sdk.set_tag("scheduled_task", "insufficient_vitals")
    _expire_stale_jobs_with_email()

    try:
        from src.db.scraper_jobs import count_active_jobs

        active = count_active_jobs()
        if active > 0:
            logger.warning("Insufficient vitals run skipped: %d job(s) already active.", active)
            sentry_sdk.capture_message(
                f"Insufficient vitals skipped: {active} active job(s)",
                level="warning",
            )
            return
    except Exception as e:
        logger.warning("Could not check active jobs (non-fatal): %s", e)

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.day % 30
    logger.info(
        "Insufficient vitals run starting at %s UTC (batch=%d)",
        run_start.strftime("%Y-%m-%d %H:%M:%S"),
        today_batch,
    )

    try:
        result = _run_mode_in_subprocess("delta_insufficient_vitals", today_batch)
    except Exception:
        sentry_sdk.capture_exception()
        tb = traceback.format_exc()
        logger.error("Insufficient vitals run crashed:\n%s", tb)
        _send_job_summary_email("Insufficient Vitals", None, 0.0, run_start, error=tb)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    logger.info("Insufficient vitals run complete in %.0fs", duration_s)
    _send_job_summary_email("Insufficient Vitals", result, duration_s, run_start)


def run_daily_gemini_research() -> None:
    """Entry point called by APScheduler at 08:00 UTC each day."""
    sentry_sdk.set_tag("scheduled_task", "gemini_research")
    _expire_stale_jobs_with_email()

    try:
        from src.db.scraper_jobs import count_active_jobs

        active = count_active_jobs()
        if active > 0:
            logger.warning("Gemini research run skipped: %d job(s) already active.", active)
            sentry_sdk.capture_message(
                f"Gemini research skipped: {active} active job(s)",
                level="warning",
            )
            return
    except Exception as e:
        logger.warning("Could not check active jobs (non-fatal): %s", e)

    run_start = datetime.now(timezone.utc)
    today_batch = run_start.day % 30
    logger.info(
        "Gemini research run starting at %s UTC (batch=%d)",
        run_start.strftime("%Y-%m-%d %H:%M:%S"),
        today_batch,
    )

    try:
        result = _run_mode_in_subprocess("gemini_vitals_research", today_batch)
    except Exception:
        sentry_sdk.capture_exception()
        tb = traceback.format_exc()
        logger.error("Gemini research run crashed:\n%s", tb)
        # Detect model deprecation from subprocess traceback
        if "GeminiModelDeprecatedError" in tb:
            _send_model_deprecated_email("gemini-3.1-pro-preview", tb)
        _send_job_summary_email("Gemini Research", None, 0.0, run_start, error=tb)
        return

    duration_s = (datetime.now(timezone.utc) - run_start).total_seconds()
    logger.info("Gemini research run complete in %.0fs", duration_s)
    _send_job_summary_email("Gemini Research", result, duration_s, run_start)


def run_daily_page_quality() -> None:
    """Entry point called by APScheduler at 09:00 UTC each day."""
    sentry_sdk.set_tag("scheduled_task", "page_quality")
    run_start = datetime.now(timezone.utc)
    logger.info(
        "Page quality inspection starting at %s UTC",
        run_start.strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        from src.services.page_quality_inspector import inspect_one_page

        result = inspect_one_page()
        if result is None:
            logger.info("Page quality inspection: no pages to inspect or error occurred")
        else:
            logger.info(
                "Page quality inspection complete: result=%s source_page_id=%s",
                result.get("result"),
                result.get("source_page_id"),
            )
    except Exception:
        sentry_sdk.capture_exception()
        logger.exception("Page quality inspection crashed")


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
        logger.info("EMAIL_APP_PASSWORD not set — skipping %s email", job_name)
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
        logger.info("%s email sent to %s", job_name, email_to)
    except Exception as exc:
        logger.warning("Failed to send %s email: %s", job_name, exc)


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
        logger.info("EMAIL_APP_PASSWORD not set — skipping summary email")
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
        logger.info("Summary email sent to %s", email_to)
    except Exception as exc:
        logger.warning("Failed to send summary email: %s", exc)
