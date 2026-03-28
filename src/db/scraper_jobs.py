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
            dict(zip(("id", "type", "status", "created_at", "updated_at"), row))
            for row in rows
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
