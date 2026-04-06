# -*- coding: utf-8 -*-
"""Unit tests for src/db/reference_documents.py."""

from __future__ import annotations

import pytest

from src.db.connection import init_db
from src.db.reference_documents import get_reference_document, upsert_reference_document


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "ref_docs_test.db"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))
    init_db(path=db_path)
    return db_path


# ---------------------------------------------------------------------------
# get_reference_document
# ---------------------------------------------------------------------------


def test_get_reference_document_returns_none_for_missing_key(tmp_db, monkeypatch):
    result = get_reference_document("nonexistent_key_xyz")
    assert result is None


def test_get_reference_document_returns_dict_after_upsert(tmp_db, monkeypatch):
    upsert_reference_document("test_key", "Test content here")
    doc = get_reference_document("test_key")
    assert doc is not None
    assert doc["doc_key"] == "test_key"
    assert doc["content"] == "Test content here"


# ---------------------------------------------------------------------------
# upsert_reference_document
# ---------------------------------------------------------------------------


def test_upsert_inserts_new_row_and_returns_id(tmp_db, monkeypatch):
    row_id = upsert_reference_document("new_doc", "Initial content")
    assert isinstance(row_id, int)
    assert row_id > 0


def test_upsert_updates_content_on_second_call(tmp_db, monkeypatch):
    upsert_reference_document("update_doc", "Version 1")
    upsert_reference_document("update_doc", "Version 2")
    doc = get_reference_document("update_doc")
    assert doc["content"] == "Version 2"


def test_upsert_idempotent_returns_same_id(tmp_db, monkeypatch):
    id1 = upsert_reference_document("same_doc", "Content")
    id2 = upsert_reference_document("same_doc", "Updated Content")
    assert id1 == id2


def test_upsert_multiple_keys_independent(tmp_db, monkeypatch):
    upsert_reference_document("doc_a", "Content A")
    upsert_reference_document("doc_b", "Content B")
    assert get_reference_document("doc_a")["content"] == "Content A"
    assert get_reference_document("doc_b")["content"] == "Content B"
