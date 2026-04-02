# -*- coding: utf-8 -*-
"""CRUD operations for the reference_documents table."""

from __future__ import annotations

from .connection import get_connection


def get_reference_document(doc_key: str, conn=None) -> dict | None:
    """Return the reference document for *doc_key*, or None if not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, doc_key, content, fetched_at, created_at, updated_at"
            " FROM reference_documents WHERE doc_key = %s",
            (doc_key,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        keys = ["id", "doc_key", "content", "fetched_at", "created_at", "updated_at"]
        return dict(zip(keys, row))
    finally:
        if own_conn:
            conn.close()


def upsert_reference_document(doc_key: str, content: str, conn=None) -> int:
    """Insert or update a reference document. Returns the id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id FROM reference_documents WHERE doc_key = %s",
            (doc_key,),
        )
        row = cur.fetchone()
        if row:
            conn.execute(
                "UPDATE reference_documents"
                " SET content = %s, fetched_at = NOW(), updated_at = NOW()"
                " WHERE id = %s",
                (content, row["id"]),
            )
            if own_conn:
                conn.commit()
            return row["id"]
        cur = conn.execute(
            "INSERT INTO reference_documents (doc_key, content, fetched_at)"
            " VALUES (%s, %s, NOW()) RETURNING id",
            (doc_key, content),
        )
        if own_conn:
            conn.commit()
        return cur.fetchone()["id"]
    finally:
        if own_conn:
            conn.close()
