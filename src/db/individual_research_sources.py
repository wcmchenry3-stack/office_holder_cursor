# -*- coding: utf-8 -*-
"""CRUD operations for individual_research_sources and wiki_draft_proposals tables."""

from __future__ import annotations

from typing import Any

from .connection import get_connection

# ---------------------------------------------------------------------------
# individual_research_sources
# ---------------------------------------------------------------------------


def insert_research_source(
    individual_id: int,
    source_url: str,
    source_type: str | None = None,
    found_data_json: str | None = None,
    conn=None,
) -> int:
    """Insert a research source and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO individual_research_sources"
            " (individual_id, source_url, source_type, found_data_json)"
            " VALUES (%s, %s, %s, %s) RETURNING id",
            (individual_id, source_url, source_type, found_data_json),
        )
        if own_conn:
            conn.commit()
        return cur.fetchone()["id"]
    finally:
        if own_conn:
            conn.close()


def list_sources_for_individual(individual_id: int, conn=None) -> list[dict]:
    """Return all research sources for an individual, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, individual_id, source_url, source_type, found_data_json, created_at"
            " FROM individual_research_sources"
            " WHERE individual_id = %s"
            " ORDER BY created_at DESC",
            (individual_id,),
        )
        keys = ["id", "individual_id", "source_url", "source_type", "found_data_json", "created_at"]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# wiki_draft_proposals
# ---------------------------------------------------------------------------


def insert_wiki_draft_proposal(
    individual_id: int,
    proposal_text: str,
    status: str = "pending",
    conn=None,
) -> int:
    """Insert a wiki draft proposal and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO wiki_draft_proposals"
            " (individual_id, proposal_text, status)"
            " VALUES (%s, %s, %s) RETURNING id",
            (individual_id, proposal_text, status),
        )
        if own_conn:
            conn.commit()
        return cur.fetchone()["id"]
    finally:
        if own_conn:
            conn.close()


def get_wiki_draft_proposal(proposal_id: int, conn=None) -> dict | None:
    """Return a single wiki draft proposal by id, or None."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT wp.id, wp.individual_id, wp.proposal_text, wp.status, wp.created_at,"
            " i.full_name, i.wiki_url"
            " FROM wiki_draft_proposals wp"
            " JOIN individuals i ON i.id = wp.individual_id"
            " WHERE wp.id = %s",
            (proposal_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        keys = [
            "id",
            "individual_id",
            "proposal_text",
            "status",
            "created_at",
            "full_name",
            "wiki_url",
        ]
        return dict(zip(keys, row))
    finally:
        if own_conn:
            conn.close()


def list_wiki_draft_proposals(status: str | None = None, conn=None) -> list[dict]:
    """Return wiki draft proposals, optionally filtered by status, newest first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if status:
            cur = conn.execute(
                "SELECT wp.id, wp.individual_id, wp.status, wp.created_at,"
                " i.full_name, i.wiki_url"
                " FROM wiki_draft_proposals wp"
                " JOIN individuals i ON i.id = wp.individual_id"
                " WHERE wp.status = %s"
                " ORDER BY wp.created_at DESC",
                (status,),
            )
        else:
            cur = conn.execute(
                "SELECT wp.id, wp.individual_id, wp.status, wp.created_at,"
                " i.full_name, i.wiki_url"
                " FROM wiki_draft_proposals wp"
                " JOIN individuals i ON i.id = wp.individual_id"
                " ORDER BY wp.created_at DESC",
            )
        keys = ["id", "individual_id", "status", "created_at", "full_name", "wiki_url"]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def update_wiki_draft_proposal_status(proposal_id: int, status: str, conn=None) -> None:
    """Update the status of a wiki draft proposal."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE wiki_draft_proposals SET status = %s WHERE id = %s",
            (status, proposal_id),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()
