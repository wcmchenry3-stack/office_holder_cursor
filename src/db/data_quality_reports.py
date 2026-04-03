# -*- coding: utf-8 -*-
"""CRUD helpers for the data_quality_reports table.

Each record represents one data quality check fingerprint. Used by the
data quality pipeline to deduplicate checks and track GitHub issue creation.
"""

from __future__ import annotations

import hashlib

from src.db.connection import get_connection


def make_fingerprint(record_type: str, record_id: int, check_type: str) -> str:
    """Generate a deterministic fingerprint for a data quality check.

    Format: dq-{sha256(record_type|record_id|check_type)[:16]}
    """
    raw = f"{record_type}|{record_id}|{check_type}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"dq-{digest}"


def find_by_fingerprint(fingerprint: str, conn=None) -> dict | None:
    """Return the record for this fingerprint, or None if not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, fingerprint, record_type, record_id, check_type, flagged_by,"
            " concern_details, github_issue_url, github_issue_number, created_at"
            " FROM data_quality_reports WHERE fingerprint = %s",
            (fingerprint,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        keys = [
            "id",
            "fingerprint",
            "record_type",
            "record_id",
            "check_type",
            "flagged_by",
            "concern_details",
            "github_issue_url",
            "github_issue_number",
            "created_at",
        ]
        return dict(zip(keys, row))
    finally:
        if own_conn:
            conn.close()


def insert_report(
    fingerprint: str,
    record_type: str,
    record_id: int,
    check_type: str,
    flagged_by: str,
    concern_details: str | None = None,
    github_issue_url: str | None = None,
    github_issue_number: int | None = None,
    conn=None,
) -> int:
    """Insert a new data quality report. Returns the new row id.

    Silently ignores duplicates (UNIQUE on fingerprint).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO data_quality_reports"
            " (fingerprint, record_type, record_id, check_type, flagged_by,"
            "  concern_details, github_issue_url, github_issue_number)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (fingerprint) DO NOTHING"
            " RETURNING id",
            (
                fingerprint,
                record_type,
                record_id,
                check_type,
                flagged_by,
                concern_details,
                github_issue_url,
                github_issue_number,
            ),
        )
        row = cur.fetchone()
        if own_conn:
            conn.commit()
        return row[0] if row else 0
    finally:
        if own_conn:
            conn.close()


def list_recent_reports(limit: int = 100, conn=None) -> list[dict]:
    """Return up to `limit` most recent data quality reports, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, fingerprint, record_type, record_id, check_type, flagged_by,"
            " concern_details, github_issue_url, github_issue_number, created_at"
            " FROM data_quality_reports"
            " ORDER BY created_at DESC"
            " LIMIT %s",
            (limit,),
        )
        keys = [
            "id",
            "fingerprint",
            "record_type",
            "record_id",
            "check_type",
            "flagged_by",
            "concern_details",
            "github_issue_url",
            "github_issue_number",
            "created_at",
        ]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def count_by_check_type(conn=None) -> dict[str, int]:
    """Return a dict of {check_type: count} across all reports."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT check_type, COUNT(*) FROM data_quality_reports GROUP BY check_type"
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()
