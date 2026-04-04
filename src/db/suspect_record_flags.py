# -*- coding: utf-8 -*-
"""CRUD helpers for the suspect_record_flags table.

Each row records one pre-insertion flag event from the suspect record gate
(Issue #217). Used for audit visibility and the AI decision dashboard.
"""

from __future__ import annotations

import json

from src.db.connection import get_connection


def insert_flag(
    office_id: int,
    full_name: str | None,
    wiki_url: str | None,
    flag_reasons: list[str],
    ai_votes: list[dict] | None,
    result: str,
    individual_id: int | None = None,
    gh_issue_url: str | None = None,
    conn=None,
) -> int:
    """Insert a suspect record flag entry. Returns the new row id.

    Args:
        office_id: The office being scraped when the flag was raised.
        full_name: _name_from_table value from the parsed row.
        wiki_url: wiki_url value from the parsed row.
        flag_reasons: List of pattern match descriptions that triggered the flag.
        ai_votes: List of AIVote dicts (serialised to JSON). None if vote not run.
        result: One of 'allowed', 'skipped', 'gh_issue'.
        individual_id: Set after insertion if result is 'allowed'.
        gh_issue_url: GitHub issue URL if one was created.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO suspect_record_flags"
            " (individual_id, office_id, full_name, wiki_url,"
            "  flag_reasons, ai_votes, result, gh_issue_url)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (
                individual_id,
                office_id,
                full_name,
                wiki_url,
                json.dumps(flag_reasons),
                json.dumps(ai_votes) if ai_votes is not None else None,
                result,
                gh_issue_url,
            ),
        )
        row = cur.fetchone()
        if own_conn:
            conn.commit()
        return row[0] if row else 0
    finally:
        if own_conn:
            conn.close()


def update_individual_id(flag_id: int, individual_id: int, conn=None) -> None:
    """Backfill individual_id after an 'allowed' record is inserted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE suspect_record_flags SET individual_id = %s WHERE id = %s",
            (individual_id, flag_id),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def list_recent(limit: int = 100, conn=None) -> list[dict]:
    """Return up to `limit` most recent flag events, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, individual_id, office_id, full_name, wiki_url,"
            " flag_reasons, ai_votes, result, gh_issue_url, created_at"
            " FROM suspect_record_flags"
            " ORDER BY id DESC"
            " LIMIT %s",
            (limit,),
        )
        keys = [
            "id",
            "individual_id",
            "office_id",
            "full_name",
            "wiki_url",
            "flag_reasons",
            "ai_votes",
            "result",
            "gh_issue_url",
            "created_at",
        ]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
