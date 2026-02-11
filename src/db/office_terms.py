"""Office terms (scraped results) write/read and delta logic."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


def insert_office_term(
    office_id: int,
    individual_id: int | None,
    wiki_url: str,
    party_id: int | None = None,
    district: str | None = None,
    term_start: str | None = None,
    term_end: str | None = None,
    term_start_year: int | None = None,
    term_end_year: int | None = None,
    term_start_imprecise: bool = False,
    term_end_imprecise: bool = False,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Insert one office term. Returns id. Party is referenced by party_id (FK) only. For years-only terms use term_start_year/term_end_year and leave term_start/term_end None."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        ts_imp = 1 if term_start_imprecise else 0
        te_imp = 1 if term_end_imprecise else 0
        cur = conn.execute(
            """INSERT OR REPLACE INTO office_terms
               (office_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, term_start_imprecise, term_end_imprecise, wiki_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (office_id, individual_id, party_id, district or None, term_start, term_end, term_start_year, term_end_year, ts_imp, te_imp, wiki_url),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def count_terms_for_office(office_id: int, conn: sqlite3.Connection | None = None) -> int:
    """Return the number of office_terms for an office."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM office_terms WHERE office_id = ?", (office_id,))
        return cur.fetchone()[0]
    finally:
        if own_conn:
            conn.close()


def get_terms_counts_by_office(conn: sqlite3.Connection | None = None) -> dict[int, int]:
    """Return dict mapping office_id to count of office_terms."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT office_id, COUNT(*) FROM office_terms GROUP BY office_id"
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_existing_terms_for_office(office_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return existing office_terms for an office (for delta compare)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT id, office_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, wiki_url
               FROM office_terms WHERE office_id = ?""",
            (office_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def delete_office_terms_for_office(office_id: int, conn: sqlite3.Connection | None = None) -> int:
    """Delete all office_terms for an office. Returns count deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM office_terms WHERE office_id = ?", (office_id,))
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def purge_all_office_terms(conn: sqlite3.Connection | None = None) -> int:
    """Delete all office_terms. Returns count deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM office_terms")
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def purge_all_individuals(conn: sqlite3.Connection | None = None) -> int:
    """Delete all individuals. Returns count deleted. Call after purge_all_office_terms if doing full reset."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM individuals")
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def list_office_terms(
    limit: int = 200,
    offset: int = 0,
    office_id: int | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """List office terms with optional office filter and pagination."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if office_id is not None:
            cur = conn.execute(
                """SELECT ot.id, ot.office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          o.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   JOIN offices o ON o.id = ot.office_id
                   LEFT JOIN countries c ON c.id = o.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   WHERE ot.office_id = ?
                   ORDER BY COALESCE(ot.term_start, ot.term_start_year) DESC LIMIT ? OFFSET ?""",
                (office_id, limit, offset),
            )
        else:
            cur = conn.execute(
                """SELECT ot.id, ot.office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          o.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   JOIN offices o ON o.id = ot.office_id
                   LEFT JOIN countries c ON c.id = o.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   ORDER BY ot.scraped_at DESC LIMIT ? OFFSET ?""",
                (limit, offset),
            )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
