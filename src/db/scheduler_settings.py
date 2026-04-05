# -*- coding: utf-8 -*-
"""CRUD helpers for the scheduler_settings table.

Each row stores the pause state for one APScheduler job ID.
Rows are seeded on startup; the UI updates them via the /api/scheduler-settings endpoints.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.db.connection import get_connection

# Job IDs that are registered in APScheduler and can be paused via the UI.
# The maintenance job is intentionally excluded — it must always run.
PAUSEABLE_JOB_IDS = [
    "daily_delta",
    "daily_insufficient_vitals",
    "daily_gemini_research",
    "daily_page_quality",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def seed_scheduler_settings(conn=None) -> None:
    """Upsert a row for each known pauseable job ID (idempotent, called from init_db)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        for job_id in PAUSEABLE_JOB_IDS:
            conn.execute(
                "INSERT INTO scheduler_settings (job_id, paused, updated_at)"
                " VALUES (%s, %s, %s)"
                " ON CONFLICT (job_id) DO NOTHING",
                (job_id, False, _now_iso()),
            )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def is_job_paused(job_id: str, conn=None) -> bool:
    """Return True if the given job is currently paused."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT paused FROM scheduler_settings WHERE job_id = %s",
            (job_id,),
        ).fetchone()
        if row is None:
            return False
        val = row[0]
        # PostgreSQL returns bool; SQLite returns 0/1
        return bool(val)
    finally:
        if own_conn:
            conn.close()


def set_job_paused(job_id: str, paused: bool, conn=None) -> None:
    """Set the paused state for the given job ID."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE scheduler_settings SET paused = %s, updated_at = %s WHERE job_id = %s",
            (paused, _now_iso(), job_id),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def list_all_settings(conn=None) -> list[dict]:
    """Return all scheduler_settings rows as dicts."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT job_id, paused, updated_at FROM scheduler_settings ORDER BY job_id",
        ).fetchall()
        return [
            {
                "job_id": row[0],
                "paused": bool(row[1]),
                "updated_at": row[2],
            }
            for row in rows
        ]
    finally:
        if own_conn:
            conn.close()
