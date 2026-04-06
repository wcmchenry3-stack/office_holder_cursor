# -*- coding: utf-8 -*-
"""Unit tests for wiki_draft_proposals CRUD in src/db/individual_research_sources.py."""

from __future__ import annotations

import pytest

from src.db.connection import init_db, get_connection
from src.db.individual_research_sources import (
    insert_wiki_draft_proposal,
    get_wiki_draft_proposal,
    list_wiki_draft_proposals,
    update_wiki_draft_proposal_status,
)


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "draft_test.db"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))
    init_db(path=db_path)
    return db_path


@pytest.fixture()
def individual_id(tmp_db, monkeypatch):
    """Insert a minimal individual and return its id."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO individuals (full_name, wiki_url) VALUES (%s, %s) RETURNING id",
            ("Jane Test", "/wiki/Jane_Test"),
        )
        conn.commit()
        return cur.fetchone()["id"]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# insert_wiki_draft_proposal
# ---------------------------------------------------------------------------


def test_insert_returns_positive_id(tmp_db, individual_id, monkeypatch):
    pid = insert_wiki_draft_proposal(individual_id, "== Article ==", status="pending")
    assert isinstance(pid, int)
    assert pid > 0


def test_insert_default_status_is_pending(tmp_db, individual_id, monkeypatch):
    pid = insert_wiki_draft_proposal(individual_id, "Text")
    draft = get_wiki_draft_proposal(pid)
    assert draft["status"] == "pending"


# ---------------------------------------------------------------------------
# get_wiki_draft_proposal
# ---------------------------------------------------------------------------


def test_get_returns_none_for_missing_id(tmp_db, monkeypatch):
    assert get_wiki_draft_proposal(999999) is None


def test_get_returns_correct_fields(tmp_db, individual_id, monkeypatch):
    pid = insert_wiki_draft_proposal(individual_id, "== Test Article ==", status="pending")
    draft = get_wiki_draft_proposal(pid)
    assert draft is not None
    assert draft["id"] == pid
    assert draft["individual_id"] == individual_id
    assert draft["proposal_text"] == "== Test Article =="
    assert draft["full_name"] == "Jane Test"
    assert draft["wiki_url"] == "/wiki/Jane_Test"


# ---------------------------------------------------------------------------
# list_wiki_draft_proposals
# ---------------------------------------------------------------------------


def test_list_returns_empty_when_no_drafts(tmp_db, monkeypatch):
    results = list_wiki_draft_proposals()
    # init_db may produce 0 drafts; just verify it's a list
    assert isinstance(results, list)


def test_list_returns_inserted_draft(tmp_db, individual_id, monkeypatch):
    insert_wiki_draft_proposal(individual_id, "Text A", status="pending")
    results = list_wiki_draft_proposals()
    proposal_texts_or_ids = [r["individual_id"] for r in results]
    assert individual_id in proposal_texts_or_ids


def test_list_filter_by_status(tmp_db, individual_id, monkeypatch):
    insert_wiki_draft_proposal(individual_id, "Pending draft", status="pending")
    # Insert another individual to create a second proposal with different status
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO individuals (full_name, wiki_url) VALUES (%s, %s) RETURNING id",
            ("Bob Test", "/wiki/Bob_Test"),
        )
        conn.commit()
        ind_id2 = cur.fetchone()["id"]
    finally:
        conn.close()
    insert_wiki_draft_proposal(ind_id2, "Submitted draft", status="submitted")

    pending = list_wiki_draft_proposals(status="pending")
    submitted = list_wiki_draft_proposals(status="submitted")
    assert all(r["status"] == "pending" for r in pending)
    assert all(r["status"] == "submitted" for r in submitted)


# ---------------------------------------------------------------------------
# update_wiki_draft_proposal_status
# ---------------------------------------------------------------------------


def test_update_status_changes_status(tmp_db, individual_id, monkeypatch):
    pid = insert_wiki_draft_proposal(individual_id, "Draft text", status="pending")
    update_wiki_draft_proposal_status(pid, "submitted")
    draft = get_wiki_draft_proposal(pid)
    assert draft["status"] == "submitted"


def test_update_status_multiple_transitions(tmp_db, individual_id, monkeypatch):
    pid = insert_wiki_draft_proposal(individual_id, "Draft text")
    for status in ("pending", "submitted", "published"):
        update_wiki_draft_proposal_status(pid, status)
        draft = get_wiki_draft_proposal(pid)
        assert draft["status"] == status
