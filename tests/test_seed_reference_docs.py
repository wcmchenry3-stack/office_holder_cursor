# -*- coding: utf-8 -*-
"""Unit tests for src/db/seed_reference_docs.py — seed_wikipedia_mos."""

from __future__ import annotations

import pytest

from src.db.connection import init_db, get_connection
from src.db.seed_reference_docs import seed_wikipedia_mos
from src.db.reference_documents import get_reference_document


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "seed_ref_test.db"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))
    init_db(path=db_path)
    return db_path


def test_seed_wikipedia_mos_creates_row(tmp_db, monkeypatch):
    doc = get_reference_document("wikipedia_mos")
    assert doc is not None
    assert doc["doc_key"] == "wikipedia_mos"
    assert len(doc["content"]) > 100  # substantive content


def test_seed_wikipedia_mos_is_idempotent(tmp_db, monkeypatch):
    # init_db already seeded; calling again should not raise or duplicate
    seed_wikipedia_mos()
    doc = get_reference_document("wikipedia_mos")
    assert doc is not None
    # Verify there's only one row with this key (no duplicates)
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM reference_documents WHERE doc_key = %s", ("wikipedia_mos",)
        )
        count = cur.fetchone()[0]
    finally:
        conn.close()
    assert count == 1


def test_seed_wikipedia_mos_content_contains_infobox(tmp_db, monkeypatch):
    doc = get_reference_document("wikipedia_mos")
    assert "Infobox" in doc["content"] or "infobox" in doc["content"]
