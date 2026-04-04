# -*- coding: utf-8 -*-
"""CRUD helpers for the nolink_supersede_log table.

Each row records one lifecycle event: a "No link:…" placeholder individual
was retired when a real-URL individual was found for the same person in the
same office. Used for audit visibility and the AI decision dashboard.
"""

from __future__ import annotations

from src.db.connection import get_connection


def insert_log(
    old_individual_id: int,
    new_individual_id: int,
    office_id: int,
    old_wiki_url: str,
    new_wiki_url: str,
    office_terms_reassigned: int,
    conn=None,
) -> int:
    """Insert a supersede log entry. Returns the new row id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO nolink_supersede_log"
            " (old_individual_id, new_individual_id, office_id,"
            "  old_wiki_url, new_wiki_url, office_terms_reassigned)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (
                old_individual_id,
                new_individual_id,
                office_id,
                old_wiki_url,
                new_wiki_url,
                office_terms_reassigned,
            ),
        )
        row = cur.fetchone()
        if own_conn:
            conn.commit()
        return row[0] if row else 0
    finally:
        if own_conn:
            conn.close()


def list_recent(limit: int = 100, conn=None) -> list[dict]:
    """Return up to `limit` most recent supersede events, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, old_individual_id, new_individual_id, office_id,"
            " old_wiki_url, new_wiki_url, office_terms_reassigned, created_at"
            " FROM nolink_supersede_log"
            " ORDER BY id DESC"
            " LIMIT %s",
            (limit,),
        )
        keys = [
            "id",
            "old_individual_id",
            "new_individual_id",
            "office_id",
            "old_wiki_url",
            "new_wiki_url",
            "office_terms_reassigned",
            "created_at",
        ]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
