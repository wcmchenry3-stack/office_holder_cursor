# -*- coding: utf-8 -*-
"""
CRUD helpers for the scraper_jobs table.

Each job represents one run_scraper or preview run.  The table is the durable
backing store; the routers keep an in-memory dict for live progress updates.

Status lifecycle: running → complete | cancelled | error
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.db.connection import get_connection


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_job(job_id: str, job_type: str, conn=None) -> None:
    """Insert a new job record with status='running'."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        now = _now_iso()
        conn.execute(
            "INSERT INTO scraper_jobs (id, type, status, created_at, updated_at)"
            " VALUES (%s, %s, %s, %s, %s)",
            (job_id, job_type, "running", now, now),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def update_job(job_id: str, status: str, result: dict | None = None, conn=None) -> None:
    """Update job status and optionally store the final result payload."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        result_json = json.dumps(result) if result is not None else None
        conn.execute(
            "UPDATE scraper_jobs SET status = %s, updated_at = %s, result_json = %s WHERE id = %s",
            (status, _now_iso(), result_json, job_id),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_job(job_id: str, conn=None) -> dict | None:
    """Return the job record as a dict, or None if not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, type, status, created_at, updated_at, result_json"
            " FROM scraper_jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        record = dict(zip(("id", "type", "status", "created_at", "updated_at", "result_json"), row))
        if record["result_json"]:
            try:
                record["result"] = json.loads(record["result_json"])
            except (ValueError, TypeError):
                record["result"] = None
        del record["result_json"]
        return record
    finally:
        if own_conn:
            conn.close()


def list_recent_jobs(limit: int = 20, conn=None) -> list[dict]:
    """Return the most recent *limit* jobs, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, type, status, created_at, updated_at"
            " FROM scraper_jobs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
        return [
            dict(zip(("id", "type", "status", "created_at", "updated_at"), row)) for row in rows
        ]
    finally:
        if own_conn:
            conn.close()


def delete_jobs_older_than(hours: int = 48, conn=None) -> int:
    """Delete completed/cancelled/error jobs older than *hours* hours.  Returns deleted count."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cutoff = datetime.now(timezone.utc)
        from datetime import timedelta

        cutoff = cutoff - timedelta(hours=hours)
        cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor = conn.execute(
            "DELETE FROM scraper_jobs WHERE status != %s AND created_at < %s",
            ("running", cutoff_iso),
        )
        deleted = cursor.rowcount if hasattr(cursor, "rowcount") else 0
        if own_conn:
            conn.commit()
        return deleted
    finally:
        if own_conn:
            conn.close()


def enqueue_job(job_id: str, job_type: str, job_params_json: str, conn=None) -> None:
    """Insert a job with status='queued'. Called when a job arrives but one is already running."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        now = _now_iso()
        conn.execute(
            "INSERT INTO scraper_jobs"
            " (id, type, status, queued_at, job_params_json, created_at, updated_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (job_id, job_type, "queued", now, job_params_json, now, now),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def pop_next_queued_job(conn=None) -> dict | None:
    """Claim the oldest queued job atomically: set status='running', return its record.

    Returns None if no queued jobs exist.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, type, job_params_json FROM scraper_jobs"
            " WHERE status = %s ORDER BY queued_at ASC LIMIT 1",
            ("queued",),
        ).fetchone()
        if row is None:
            return None
        job_id = row[0]
        conn.execute(
            "UPDATE scraper_jobs SET status = %s, updated_at = NOW() WHERE id = %s",
            ("running", job_id),
        )
        if own_conn:
            conn.commit()
        return {"id": row[0], "type": row[1], "job_params_json": row[2]}
    finally:
        if own_conn:
            conn.close()


def count_queued_jobs(conn=None) -> int:
    """Return the number of jobs currently in the queue (status='queued')."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM scraper_jobs WHERE status = %s",
            ("queued",),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        if own_conn:
            conn.close()


def expire_stale_jobs(cancel_callback=None, conn=None) -> list[dict]:
    """Mark stale running/queued jobs as 'error' and return details of expired jobs.

    Expiry rules:
    - Queued jobs older than 12 hours → expired
    - Running jobs older than 8 hours → expired (except type='full')
    - Running 'full' jobs older than 24 hours → expired

    Args:
        cancel_callback: Optional callable(job_id) invoked for each expired job so the
            caller can signal in-memory threads to stop (e.g. set cancelled=True).
        conn: Optional shared DB connection; if None, a new connection is opened and
            committed by this function.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        rows = conn.execute(
            "SELECT id, type, status, created_at FROM scraper_jobs WHERE status IN (%s, %s)",
            ("running", "queued"),
        ).fetchall()

        expired = []
        for row in rows:
            job_id, job_type, status, created_at_raw = row[0], row[1], row[2], row[3]
            # PostgreSQL TIMESTAMPTZ returns datetime; SQLite TEXT returns str.
            if isinstance(created_at_raw, datetime):
                created_at = created_at_raw
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            else:
                try:
                    created_at = datetime.strptime(created_at_raw, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc
                    )
                except (ValueError, TypeError):
                    continue
            age = now - created_at
            reason = None
            if status == "queued" and age > timedelta(hours=12):
                reason = f"Queued job expired after {age}"
            elif status == "running" and job_type == "full" and age > timedelta(hours=24):
                reason = f"Full run expired after {age}"
            elif status == "running" and job_type != "full" and age > timedelta(hours=8):
                reason = f"Running job expired after {age}"

            if reason:
                conn.execute(
                    "UPDATE scraper_jobs SET status = %s, updated_at = %s WHERE id = %s",
                    ("error", now.strftime("%Y-%m-%dT%H:%M:%SZ"), job_id),
                )
                expired.append({"id": job_id, "type": job_type, "status": status, "reason": reason})
                if cancel_callback is not None:
                    try:
                        cancel_callback(job_id)
                    except Exception:
                        pass

        if own_conn:
            conn.commit()
        return expired
    finally:
        if own_conn:
            conn.close()


def count_active_jobs(conn=None) -> int:
    """Return the number of jobs with status 'running' or 'queued'."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM scraper_jobs WHERE status IN (%s, %s)",
            ("running", "queued"),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        if own_conn:
            conn.close()
