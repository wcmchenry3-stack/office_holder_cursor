# -*- coding: utf-8 -*-
"""Query helpers for the AI decisions dashboard (Issue #221).

Builds a unified view of all AI-driven actions across:
  - data_quality_reports     (per-record sequential QC pipeline)
  - parse_error_reports      (parser failure reports)
  - page_quality_checks      (daily page inspection, Issue #218)
  - suspect_record_flags     (pre-insertion gate, Issue #217)

Common columns returned:
  decision_type, subject, action_taken, gh_issue_url, created_at
"""

from __future__ import annotations

import json

from src.db.connection import get_connection

_PAGE_SIZE = 100


def _safe_json(raw) -> list:
    """Parse JSON list or return empty list on failure."""
    if raw is None:
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def list_ai_decisions(
    decision_type: str | None = None,
    result: str | None = None,
    offset: int = 0,
    limit: int = _PAGE_SIZE,
    conn=None,
) -> list[dict]:
    """Return up to `limit` AI decisions newest-first, with optional filters.

    Filters:
        decision_type: 'data_quality' | 'parse_error' | 'page_quality' | 'suspect_flag'
        result:        free-text match on action_taken column
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = _fetch_union(
            conn, decision_type=decision_type, result=result, offset=offset, limit=limit
        )
        return rows
    finally:
        if own_conn:
            conn.close()


def count_ai_decisions(
    decision_type: str | None = None,
    result: str | None = None,
    conn=None,
) -> int:
    """Return total count matching the given filters."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        return _fetch_count(conn, decision_type=decision_type, result=result)
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Internal query builders
# ---------------------------------------------------------------------------

_UNION_SQL = """
SELECT
    'data_quality'  AS decision_type,
    CAST(record_id AS TEXT) AS subject,
    check_type      AS action_taken,
    github_issue_url AS gh_issue_url,
    created_at,
    NULL            AS ai_votes
FROM data_quality_reports

UNION ALL

SELECT
    'parse_error'   AS decision_type,
    COALESCE(wiki_url, office_name, 'unknown') AS subject,
    error_type      AS action_taken,
    github_issue_url AS gh_issue_url,
    created_at,
    NULL            AS ai_votes
FROM parse_error_reports

UNION ALL

SELECT
    'page_quality'  AS decision_type,
    COALESCE(sp.url, CAST(pqc.source_page_id AS TEXT)) AS subject,
    pqc.result      AS action_taken,
    pqc.gh_issue_url,
    pqc.created_at,
    pqc.ai_votes
FROM page_quality_checks pqc
LEFT JOIN source_pages sp ON sp.id = pqc.source_page_id

UNION ALL

SELECT
    'suspect_flag'  AS decision_type,
    COALESCE(full_name, wiki_url, 'unknown') AS subject,
    result          AS action_taken,
    gh_issue_url,
    created_at,
    NULL            AS ai_votes
FROM suspect_record_flags
"""


def _build_where(decision_type: str | None, result: str | None) -> tuple[str, list]:
    """Build WHERE clause and params for filtering the UNION."""
    conditions = []
    params: list = []
    if decision_type:
        conditions.append("decision_type = %s")
        params.append(decision_type)
    if result:
        conditions.append("action_taken = %s")
        params.append(result)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


def _fetch_union(conn, decision_type, result, offset, limit) -> list[dict]:
    where, params = _build_where(decision_type, result)
    sql = f"""
        SELECT decision_type, subject, action_taken, gh_issue_url, created_at, ai_votes
        FROM ({_UNION_SQL}) AS combined
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params += [limit, offset]
    cur = conn.execute(sql, params)
    keys = ["decision_type", "subject", "action_taken", "gh_issue_url", "created_at", "ai_votes"]
    rows = []
    for row in cur.fetchall():
        d = dict(zip(keys, row))
        d["ai_votes"] = _safe_json(d["ai_votes"])
        rows.append(d)
    return rows


def _fetch_count(conn, decision_type, result) -> int:
    where, params = _build_where(decision_type, result)
    sql = f"""
        SELECT COUNT(*)
        FROM ({_UNION_SQL}) AS combined
        {where}
    """
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else 0
