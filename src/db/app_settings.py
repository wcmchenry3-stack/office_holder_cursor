# -*- coding: utf-8 -*-
"""CRUD helpers for the app_settings table.

Stores operational constants that were previously hardcoded:
  - Job expiry thresholds (used by expire_stale_jobs)
  - Queue depth limit (used by run_scraper router)
  - APScheduler cron times (read at startup before scheduler.add_job)

get_setting() is fault-tolerant: returns the hardcoded default on DB error so
the app never crashes due to a missing setting.

Cron time changes take effect on the next application restart.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.db.connection import get_connection

APP_SETTINGS_DEFAULTS: list[dict] = [
    {
        "key": "expiry_hours_queued",
        "value": "12",
        "value_type": "int",
        "description": "Hours before a queued job is marked as expired",
    },
    {
        "key": "expiry_hours_running_full",
        "value": "24",
        "value_type": "int",
        "description": "Hours before a running full-type job is marked as expired",
    },
    {
        "key": "expiry_hours_running_other",
        "value": "8",
        "value_type": "int",
        "description": "Hours before any other running job is marked as expired",
    },
    {
        "key": "max_queued_jobs",
        "value": "1",
        "value_type": "int",
        "description": "Maximum number of jobs that can queue behind an active job",
    },
    {
        "key": "cron_daily_maintenance_hour",
        "value": "5",
        "value_type": "int",
        "description": "UTC hour for daily maintenance job (takes effect on restart)",
    },
    {
        "key": "cron_daily_maintenance_minute",
        "value": "30",
        "value_type": "int",
        "description": "UTC minute for daily maintenance job (takes effect on restart)",
    },
    {
        "key": "cron_daily_delta_hour",
        "value": "6",
        "value_type": "int",
        "description": "UTC hour for daily delta scrape job (takes effect on restart)",
    },
    {
        "key": "cron_daily_delta_minute",
        "value": "0",
        "value_type": "int",
        "description": "UTC minute for daily delta scrape job (takes effect on restart)",
    },
    {
        "key": "cron_daily_insufficient_vitals_hour",
        "value": "7",
        "value_type": "int",
        "description": "UTC hour for insufficient vitals job (takes effect on restart)",
    },
    {
        "key": "cron_daily_insufficient_vitals_minute",
        "value": "0",
        "value_type": "int",
        "description": "UTC minute for insufficient vitals job (takes effect on restart)",
    },
    {
        "key": "cron_daily_gemini_research_hour",
        "value": "8",
        "value_type": "int",
        "description": "UTC hour for Gemini research job (takes effect on restart)",
    },
    {
        "key": "cron_daily_gemini_research_minute",
        "value": "0",
        "value_type": "int",
        "description": "UTC minute for Gemini research job (takes effect on restart)",
    },
    {
        "key": "cron_daily_page_quality_hour",
        "value": "9",
        "value_type": "int",
        "description": "UTC hour for page quality job (takes effect on restart)",
    },
    {
        "key": "cron_daily_page_quality_minute",
        "value": "0",
        "value_type": "int",
        "description": "UTC minute for page quality job (takes effect on restart)",
    },
]

# Fast lookup: key → default value (cast to int since all current settings are int)
_DEFAULTS_BY_KEY: dict[str, str] = {row["key"]: row["value"] for row in APP_SETTINGS_DEFAULTS}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def seed_app_settings(conn=None) -> None:
    """Upsert default rows — idempotent, never overwrites existing values."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        for row in APP_SETTINGS_DEFAULTS:
            conn.execute(
                "INSERT INTO app_settings (key, value, value_type, description, updated_at)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (key) DO NOTHING",
                (row["key"], row["value"], row["value_type"], row["description"], _now_iso()),
            )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_setting(key: str, default: int | str | float) -> int | str | float:
    """Read a setting from DB; return *default* on miss or any DB error.

    Always casts the stored TEXT value to the same type as *default*.
    """
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT value FROM app_settings WHERE key = %s",
                (key,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return default
        raw = row[0]
        if isinstance(default, int):
            return int(raw)
        if isinstance(default, float):
            return float(raw)
        return raw
    except Exception:
        return default


def set_setting(key: str, value: str, conn=None) -> None:
    """Update a setting's value and updated_at."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE app_settings SET value = %s, updated_at = %s WHERE key = %s",
            (value, _now_iso(), key),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def list_all_settings(conn=None) -> list[dict]:
    """Return all app_settings rows as a list of dicts."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT key, value, value_type, description, updated_at"
            " FROM app_settings ORDER BY key",
        ).fetchall()
        return [
            {
                "key": row[0],
                "value": row[1],
                "value_type": row[2],
                "description": row[3],
                "updated_at": row[4],
            }
            for row in rows
        ]
    finally:
        if own_conn:
            conn.close()
