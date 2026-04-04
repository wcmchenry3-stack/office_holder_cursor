# -*- coding: utf-8 -*-
"""CRUD helpers for the page_quality_checks table.

Each row records one daily page quality inspection run (Issue #218).
"""

from __future__ import annotations

import json

from src.db.connection import get_connection


def insert_check(
    source_page_id: int,
    html_char_count: int | None,
    office_terms_count: int | None,
    ai_votes: list[dict] | None,
    result: str,
    gh_issue_url: str | None = None,
    conn=None,
) -> int:
    """Insert a page quality check record. Returns the new row id.

    Args:
        source_page_id: The source_pages.id that was inspected.
        html_char_count: Length of fetched HTML passed to AI (chars).
        office_terms_count: Number of office_terms rows in our DB for this page.
        ai_votes: List of AIVote dicts serialised to JSON. None if vote not run.
        result: One of 'ok', 'reparse_ok', 'gh_issue', 'manual_review'.
        gh_issue_url: GitHub issue URL if one was created.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO page_quality_checks"
            " (source_page_id, html_char_count, office_terms_count, ai_votes, result, gh_issue_url)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " RETURNING id",
            (
                source_page_id,
                html_char_count,
                office_terms_count,
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


def mark_page_checked(source_page_id: int, conn=None) -> None:
    """Update source_pages.last_quality_checked_at = NOW() for this page."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE source_pages SET last_quality_checked_at = NOW() WHERE id = %s",
            (source_page_id,),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def pick_next_page(conn=None) -> dict | None:
    """Select the next source_page to inspect using LRU-with-unchecked-first.

    Picks unchecked pages first (NULLS FIRST), then the longest-since-checked.
    Returns a dict with 'id' and 'url', or None if no enabled pages exist.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, url FROM source_pages"
            " WHERE enabled = 1"
            " ORDER BY last_quality_checked_at ASC NULLS FIRST, RANDOM()"
            " LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {"id": row[0], "url": row[1]}
    finally:
        if own_conn:
            conn.close()


def list_recent(limit: int = 100, conn=None) -> list[dict]:
    """Return up to `limit` most recent page quality checks, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, source_page_id, checked_at, html_char_count,"
            " office_terms_count, ai_votes, result, gh_issue_url, created_at"
            " FROM page_quality_checks"
            " ORDER BY id DESC"
            " LIMIT %s",
            (limit,),
        )
        keys = [
            "id",
            "source_page_id",
            "checked_at",
            "html_char_count",
            "office_terms_count",
            "ai_votes",
            "result",
            "gh_issue_url",
            "created_at",
        ]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
