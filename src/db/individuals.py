"""Individuals (bio) CRUD and helpers for scraper."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


def list_individuals(
    limit: int = 500,
    offset: int = 0,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return individuals with optional pagination."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT id, wiki_url, page_path, full_name, birth_date, death_date,
                      birth_date_imprecise, death_date_imprecise,
                      birth_place, death_place, is_dead_link, created_at, updated_at
               FROM individuals ORDER BY full_name LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_individual_by_wiki_url(wiki_url: str, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one individual by wiki_url."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM individuals WHERE wiki_url = ?", (wiki_url,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def get_individual(individual_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one individual by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM individuals WHERE id = ?", (individual_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def upsert_individual(data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Insert or update individual by wiki_url. Returns id. Accepts birth_date_imprecise, death_date_imprecise (0/1)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        wiki_url = (data.get("wiki_url") or "").strip()
        if not wiki_url:
            raise ValueError("wiki_url required")
        bd_imprecise = 1 if data.get("birth_date_imprecise") else 0
        dd_imprecise = 1 if data.get("death_date_imprecise") else 0
        cur = conn.execute("SELECT id FROM individuals WHERE wiki_url = ?", (wiki_url,))
        row = cur.fetchone()
        is_dead_link = 1 if data.get("is_dead_link") else 0
        if row:
            conn.execute(
                """UPDATE individuals SET
                    page_path=?, full_name=?, birth_date=?, death_date=?,
                    birth_date_imprecise=?, death_date_imprecise=?,
                    birth_place=?, death_place=?, is_dead_link=?, updated_at=datetime('now')
                WHERE id=?""",
                (
                    data.get("page_path"),
                    data.get("full_name"),
                    data.get("birth_date"),
                    data.get("death_date"),
                    bd_imprecise,
                    dd_imprecise,
                    data.get("birth_place"),
                    data.get("death_place"),
                    is_dead_link,
                    row["id"],
                ),
            )
            conn.commit()
            return row["id"]
        cur = conn.execute(
            """INSERT INTO individuals (wiki_url, page_path, full_name, birth_date, death_date, birth_date_imprecise, death_date_imprecise, birth_place, death_place, is_dead_link)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                wiki_url,
                data.get("page_path"),
                data.get("full_name"),
                data.get("birth_date"),
                data.get("death_date"),
                bd_imprecise,
                dd_imprecise,
                data.get("birth_place"),
                data.get("death_place"),
                is_dead_link,
            ),
        )
        conn.commit()
        return cur.lastrowid
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


def get_all_individual_wiki_urls(conn: sqlite3.Connection | None = None) -> set[str]:
    """Return set of all wiki_urls in the individuals table."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT wiki_url FROM individuals")
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_living_individual_wiki_urls(conn: sqlite3.Connection | None = None) -> set[str]:
    """Return set of wiki_urls for individuals with no death_date (likely living)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT wiki_url FROM individuals WHERE death_date IS NULL OR death_date = '' OR death_date LIKE 'Invalid%'"
        )
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_dead_link_wiki_urls(conn: sqlite3.Connection | None = None) -> set[str]:
    """Return set of wiki_urls for individuals marked as dead link (no bio page)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT wiki_url FROM individuals WHERE is_dead_link = 1")
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def list_individuals_for_office_category(
    office_category_id: int,
    living_only: bool = False,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return individuals connected to office terms in a given office category."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        where = ["od.office_category_id = ?", "i.id IS NOT NULL"]
        params: list[Any] = [office_category_id]
        if living_only:
            where.append("i.death_date IS NULL")
        cur = conn.execute(
            f"""
            SELECT
                s.name AS state_name,
                od.name AS senate_class,
                i.page_path,
                ot.term_start,
                ot.term_end,
                i.birth_date,
                i.death_date,
                i.id,
                i.wiki_url,
                i.full_name
            FROM office_details od
            LEFT JOIN office_terms ot ON ot.office_details_id = od.id
            LEFT JOIN individuals i ON ot.individual_id = i.id
            LEFT JOIN source_pages sp ON od.source_page_id = sp.id
            LEFT JOIN states s ON sp.state_id = s.id
            WHERE {' AND '.join(where)}
            ORDER BY i.id, ot.term_start
            """,
            tuple(params),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
