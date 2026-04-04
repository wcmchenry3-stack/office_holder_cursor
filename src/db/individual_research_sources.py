# -*- coding: utf-8 -*-
"""CRUD operations for individual_research_sources and wiki_draft_proposals tables.

DB-only module — no HTTP requests. All Wikipedia/Wikimedia requests go through
wiki_fetch.py which sets the User-Agent header per Wikimedia etiquette policy.
Rate-limit, retry, and backoff logic lives in wiki_fetch.py and wikipedia_submit.py.
"""

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
    origin: str = "manual",
    conn=None,
) -> int:
    """Insert a research source and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO individual_research_sources"
            " (individual_id, source_url, source_type, found_data_json, origin)"
            " VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (individual_id, source_url, source_type, found_data_json, origin),
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
            "SELECT id, individual_id, source_url, source_type, found_data_json, origin, created_at"
            " FROM individual_research_sources"
            " WHERE individual_id = %s"
            " ORDER BY created_at DESC",
            (individual_id,),
        )
        keys = [
            "id",
            "individual_id",
            "source_url",
            "source_type",
            "found_data_json",
            "origin",
            "created_at",
        ]
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
    origin: str = "manual",
    conn=None,
) -> int:
    """Insert a wiki draft proposal and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO wiki_draft_proposals"
            " (individual_id, proposal_text, status, origin)"
            " VALUES (%s, %s, %s, %s) RETURNING id",
            (individual_id, proposal_text, status, origin),
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
            "SELECT wp.id, wp.individual_id, wp.proposal_text, wp.status, wp.origin, wp.created_at,"
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
            "origin",
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
                "SELECT wp.id, wp.individual_id, wp.status, wp.origin, wp.created_at,"
                " i.full_name, i.wiki_url"
                " FROM wiki_draft_proposals wp"
                " JOIN individuals i ON i.id = wp.individual_id"
                " WHERE wp.status = %s"
                " ORDER BY wp.created_at DESC",
                (status,),
            )
        else:
            cur = conn.execute(
                "SELECT wp.id, wp.individual_id, wp.status, wp.origin, wp.created_at,"
                " i.full_name, i.wiki_url"
                " FROM wiki_draft_proposals wp"
                " JOIN individuals i ON i.id = wp.individual_id"
                " ORDER BY wp.created_at DESC",
            )
        keys = ["id", "individual_id", "status", "origin", "created_at", "full_name", "wiki_url"]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Notability threshold (deterministic — no AI)
# ---------------------------------------------------------------------------

# Domains that are Wikipedia mirrors (not independent sources)
_WIKIPEDIA_MIRROR_DOMAINS = frozenset(
    {
        "wikipedia.org",
        "en.m.wikipedia.org",
        "wikimedia.org",
        "wikidata.org",
        "wikiwand.com",
        "dbpedia.org",
        "wiki2.org",
    }
)

_GOV_ACADEMIC_SOURCE_TYPES = frozenset({"government", "academic"})


def _is_wikipedia_mirror(url: str) -> bool:
    """Return True if *url* belongs to a known Wikipedia mirror domain."""
    from urllib.parse import urlparse

    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    host = host.lower()
    for domain in _WIKIPEDIA_MIRROR_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return True
    return False


def check_notability_threshold(
    sources: list[dict],
    term_dates: str | None = None,
) -> bool:
    """Deterministic notability gate — all three criteria must be true.

    1. ≥ 2 independent source URLs (not Wikipedia mirrors)
    2. ≥ 1 source from a government or academic domain
    3. Verifiable term dates exist (non-empty string)

    *sources* should be a list of dicts with at least ``url`` and ``source_type``
    keys (matching individual_research_sources rows or SourceRecord dicts).
    """
    if not term_dates or not term_dates.strip():
        return False

    independent = []
    has_gov_academic = False
    for src in sources:
        url = src.get("url") or src.get("source_url") or ""
        if not url or _is_wikipedia_mirror(url):
            continue
        independent.append(url)
        source_type = (src.get("source_type") or "").lower()
        if source_type in _GOV_ACADEMIC_SOURCE_TYPES:
            has_gov_academic = True

    return len(independent) >= 2 and has_gov_academic


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
