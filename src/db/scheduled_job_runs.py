# -*- coding: utf-8 -*-
"""CRUD helpers for the scheduled_job_runs table.

Each row records one APScheduler job execution (daily_delta, insufficient_vitals,
gemini_research).  Status lifecycle: running → complete | error.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.db.connection import get_connection


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_run(job_name: str, conn=None) -> int:
    """Insert a new run record with status='running'. Returns the new row id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        now = _now_iso()
        cursor = conn.execute(
            "INSERT INTO scheduled_job_runs (job_name, started_at, status)" " VALUES (%s, %s, %s)",
            (job_name, now, "running"),
        )
        # PostgreSQL: use RETURNING; SQLite: use lastrowid
        row_id: int | None = None
        if hasattr(cursor, "fetchone"):
            fetched = cursor.fetchone()
            if fetched:
                row_id = int(fetched[0])
        if row_id is None and hasattr(cursor, "lastrowid") and cursor.lastrowid:
            row_id = int(cursor.lastrowid)
        if own_conn:
            conn.commit()
        return row_id or 0
    finally:
        if own_conn:
            conn.close()


def finish_run(
    run_id: int,
    status: str,
    result: dict | None = None,
    error: str | None = None,
    conn=None,
) -> None:
    """Update a run record with finished_at, duration_s, status, result, and error."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        now = datetime.now(timezone.utc)
        now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        result_json = json.dumps(result) if result is not None else None

        # Compute duration in Python so the query works with both SQLite and PostgreSQL.
        duration_s: float | None = None
        row = conn.execute(
            "SELECT started_at FROM scheduled_job_runs WHERE id = %s", (run_id,)
        ).fetchone()
        if row:
            raw = row[0]
            # raw may be a string (SQLite) or a datetime object (PostgreSQL)
            if isinstance(raw, datetime):
                started = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
            else:
                try:
                    started = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc
                    )
                except (ValueError, TypeError):
                    started = None
            if started is not None:
                duration_s = (now - started).total_seconds()

        conn.execute(
            "UPDATE scheduled_job_runs"
            " SET finished_at = %s, status = %s, result_json = %s, error = %s,"
            "     duration_s = %s"
            " WHERE id = %s",
            (now_iso, status, result_json, error, duration_s, run_id),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_last_run_for_job(job_name: str, conn=None) -> dict | None:
    """Return the most recent run record for a given job_name, or None."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, job_name, started_at, finished_at, status, duration_s, error"
            " FROM scheduled_job_runs"
            " WHERE job_name = %s"
            " ORDER BY started_at DESC, id DESC LIMIT 1",
            (job_name,),
        ).fetchone()
        if row is None:
            return None
        cols = ("id", "job_name", "started_at", "finished_at", "status", "duration_s", "error")
        return dict(zip(cols, row))
    finally:
        if own_conn:
            conn.close()


def list_recent_runs(days: int = 90, conn=None) -> list[dict]:
    """Return runs from the last *days* days, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = conn.execute(
            "SELECT id, job_name, started_at, finished_at, status, duration_s, result_json, error"
            " FROM scheduled_job_runs"
            " WHERE started_at >= %s"
            " ORDER BY started_at DESC, id DESC",
            (cutoff,),
        ).fetchall()
        cols = (
            "id",
            "job_name",
            "started_at",
            "finished_at",
            "status",
            "duration_s",
            "result_json",
            "error",
        )
        result = []
        for row in rows:
            record = dict(zip(cols, row))
            if record["result_json"]:
                try:
                    record["result"] = json.loads(record["result_json"])
                except (ValueError, TypeError):
                    record["result"] = None
            else:
                record["result"] = None
            del record["result_json"]
            result.append(record)
        return result
    finally:
        if own_conn:
            conn.close()
