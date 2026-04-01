# -*- coding: utf-8 -*-
"""CRUD helpers for the parse_error_reports table.

Each record represents one parser failure fingerprint that has been reported
as a GitHub issue. Used for two-level deduplication in ParseErrorReporter:
a DB lookup is faster than a GitHub API call, so we check here first.
"""

from __future__ import annotations

from src.db.connection import get_connection


def find_by_fingerprint(fingerprint: str, conn=None) -> dict | None:
    """Return the record for this fingerprint, or None if not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, fingerprint, function_name, error_type, wiki_url, office_name,"
            " github_issue_url, github_issue_number, created_at"
            " FROM parse_error_reports WHERE fingerprint = %s",
            (fingerprint,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        keys = [
            "id",
            "fingerprint",
            "function_name",
            "error_type",
            "wiki_url",
            "office_name",
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
    function_name: str,
    error_type: str,
    wiki_url: str | None,
    office_name: str | None,
    github_issue_url: str | None,
    github_issue_number: int | None,
    conn=None,
) -> None:
    """Insert a new parse error report. Silently ignores duplicates (UNIQUE on fingerprint)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO parse_error_reports"
            " (fingerprint, function_name, error_type, wiki_url, office_name,"
            "  github_issue_url, github_issue_number)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (fingerprint) DO NOTHING",
            (
                fingerprint,
                function_name,
                error_type,
                wiki_url,
                office_name,
                github_issue_url,
                github_issue_number,
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def list_recent_reports(limit: int = 50, conn=None) -> list[dict]:
    """Return up to `limit` most recent parse error reports, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, fingerprint, function_name, error_type, wiki_url, office_name,"
            " github_issue_url, github_issue_number, created_at"
            " FROM parse_error_reports"
            " ORDER BY created_at DESC"
            " LIMIT %s",
            (limit,),
        )
        keys = [
            "id",
            "fingerprint",
            "function_name",
            "error_type",
            "wiki_url",
            "office_name",
            "github_issue_url",
            "github_issue_number",
            "created_at",
        ]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
