"""Parties CRUD and list for scraper."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


def list_parties(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all parties as list of dicts (with country_name from JOIN)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT p.id, p.country_id, c.name AS country_name, p.party_name, p.party_link, p.created_at
               FROM parties p
               LEFT JOIN countries c ON c.id = p.country_id
               ORDER BY c.name, p.party_name"""
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_party_list_for_scraper(
    conn: sqlite3.Connection | None = None,
) -> dict[str, list[dict[str, str]]]:
    """Return party list in scraper format: { country_name: [ {name, link}, ... ] }."""
    rows = list_parties(conn)
    out: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        c = r.get("country_name") or ""
        if c not in out:
            out[c] = []
        out[c].append({"name": r.get("party_name") or "", "link": r.get("party_link") or ""})
    return out


def get_party(party_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one party by id (with country_name from JOIN)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT p.*, c.name AS country_name FROM parties p LEFT JOIN countries c ON c.id = p.country_id WHERE p.id = ?",
            (party_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def create_party(data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Insert party and return new id. Uses country_id (FK)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        cur = conn.execute(
            "INSERT INTO parties (country_id, party_name, party_link) VALUES (?, ?, ?)",
            (country_id, data.get("party_name") or "", data.get("party_link") or ""),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def update_party(
    party_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None
) -> bool:
    """Update party by id. Uses country_id (FK)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        cur = conn.execute(
            "UPDATE parties SET country_id=?, party_name=?, party_link=? WHERE id=?",
            (country_id, data.get("party_name") or "", data.get("party_link") or "", party_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def resolve_party_id_by_country(
    country_id: int,
    party_name_or_link: str | None,
    conn: sqlite3.Connection | None = None,
) -> int | None:
    """Resolve scraped party text to party id by country. Returns None if no match."""
    if not party_name_or_link or not str(party_name_or_link).strip():
        return None
    if not country_id:
        return None
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT id FROM parties WHERE country_id = ? AND (party_name = ? OR party_link = ?) LIMIT 1""",
            (country_id, party_name_or_link.strip(), party_name_or_link.strip()),
        )
        r = cur.fetchone()
        return r["id"] if r else None
    finally:
        if own_conn:
            conn.close()


def resolve_party_id(
    office_id: int,
    party_name_or_link: str | None,
    conn: sqlite3.Connection | None = None,
) -> int | None:
    """Resolve scraped party text to party id using office's country (office_details_id -> source_pages in hierarchy). Returns None if no match."""
    if not party_name_or_link or not str(party_name_or_link).strip():
        return None
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT sp.country_id FROM office_details od JOIN source_pages sp ON sp.id = od.source_page_id WHERE od.id = ? LIMIT 1",
            (office_id,),
        ).fetchone()
        if not row:
            row = conn.execute(
                "SELECT o.country_id FROM offices o WHERE o.id = ? LIMIT 1",
                (office_id,),
            ).fetchone()
        if not row:
            return None
        country_id = row["country_id"]
        return resolve_party_id_by_country(country_id, party_name_or_link, conn=conn)
    finally:
        if own_conn:
            conn.close()


def delete_party(party_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete party by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM parties WHERE id = ?", (party_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()
