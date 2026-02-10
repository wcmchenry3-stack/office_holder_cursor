"""Office config CRUD and list for scraper."""

import sqlite3
from pathlib import Path
from typing import Any

from .connection import get_connection, get_db_path
from .utils import _row_to_dict


def list_offices(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all office configs as list of dicts (with country_name, state_name, level_name, branch_name from FKs)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT o.id, o.country_id, o.state_id, o.level_id, o.branch_id,
                      c.name AS country_name, s.name AS state_name, l.name AS level_name, b.name AS branch_name,
                      o.department, o.name, o.enabled, o.notes, o.url,
                      o.table_no, o.table_rows, o.link_column, o.party_column,
                      o.term_start_column, o.term_end_column, o.district_column,
                      o.dynamic_parse, o.read_right_to_left, o.find_date_in_infobox,
                      o.parse_rowspan, o.rep_link, o.party_link, o.alt_link,
                      o.use_full_page_for_table, o.created_at
               FROM offices o
               LEFT JOIN countries c ON c.id = o.country_id
               LEFT JOIN states s ON s.id = o.state_id
               LEFT JOIN levels l ON l.id = o.level_id
               LEFT JOIN branches b ON b.id = o.branch_id
               ORDER BY c.name, o.name"""
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_office(office_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one office by id (with country_name, state_name, level_name, branch_name from JOINs)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT o.*, c.name AS country_name, s.name AS state_name, l.name AS level_name, b.name AS branch_name
               FROM offices o
               LEFT JOIN countries c ON c.id = o.country_id
               LEFT JOIN states s ON s.id = o.state_id
               LEFT JOIN levels l ON l.id = o.level_id
               LEFT JOIN branches b ON b.id = o.branch_id
               WHERE o.id = ?""",
            (office_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def create_office(data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Insert office and return new id. Uses country_id, state_id, level_id, branch_id (FKs)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        cur = conn.execute(
            """INSERT INTO offices (
                country_id, state_id, level_id, branch_id, department, name, enabled, notes,
                url, table_no, table_rows, link_column, party_column,
                term_start_column, term_end_column, district_column,
                dynamic_parse, read_right_to_left, find_date_in_infobox,
                parse_rowspan, rep_link, party_link, alt_link, use_full_page_for_table
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                country_id,
                int(data.get("state_id") or 0) or None,
                int(data.get("level_id") or 0) or None,
                int(data.get("branch_id") or 0) or None,
                data.get("department") or "",
                data.get("name") or "",
                1 if data.get("enabled") in (True, 1, "TRUE", "true", "1") else 1,  # default on
                data.get("notes") or "",
                data.get("url") or "",
                int(data.get("table_no", 1)),
                int(data.get("table_rows", 4)),
                int(data.get("link_column", 1)),
                int(data.get("party_column", 0)),
                int(data.get("term_start_column", 4)),
                int(data.get("term_end_column", 5)),
                int(data.get("district_column", 0)),
                1 if data.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                data.get("alt_link") or None,
                1 if data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def update_office(office_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> bool:
    """Update office by id. Uses country_id, state_id, level_id, branch_id (FKs)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        if "enabled" in data:
            enabled_val = 1 if data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        else:
            row = conn.execute("SELECT enabled FROM offices WHERE id = ?", (office_id,)).fetchone()
            enabled_val = row["enabled"] if row and "enabled" in row.keys() else 1
        cur = conn.execute(
            """UPDATE offices SET
                country_id=?, state_id=?, level_id=?, branch_id=?, department=?, name=?, enabled=?, notes=?,
                url=?, table_no=?, table_rows=?, link_column=?, party_column=?,
                term_start_column=?, term_end_column=?, district_column=?,
                dynamic_parse=?, read_right_to_left=?, find_date_in_infobox=?,
                parse_rowspan=?, rep_link=?, party_link=?, alt_link=?, use_full_page_for_table=?
            WHERE id=?""",
            (
                country_id,
                int(data.get("state_id") or 0) or None,
                int(data.get("level_id") or 0) or None,
                int(data.get("branch_id") or 0) or None,
                data.get("department") or "",
                data.get("name") or "",
                enabled_val,
                data.get("notes") or "",
                data.get("url") or "",
                int(data.get("table_no", 1)),
                int(data.get("table_rows", 4)),
                int(data.get("link_column", 1)),
                int(data.get("party_column", 0)),
                int(data.get("term_start_column", 4)),
                int(data.get("term_end_column", 5)),
                int(data.get("district_column", 0)),
                1 if data.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                data.get("alt_link") or None,
                1 if data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                office_id,
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def set_office_enabled(office_id: int, enabled: bool, conn: sqlite3.Connection | None = None) -> bool:
    """Set enabled flag for one office (True = on, False = off). Returns True if a row was updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE offices SET enabled = ? WHERE id = ?", (1 if enabled else 0, office_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def set_all_offices_enabled(enabled: bool, conn: sqlite3.Connection | None = None) -> int:
    """Set enabled flag for all offices. Returns number of rows updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE offices SET enabled = ?", (1 if enabled else 0,))
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def delete_office(office_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete office by id. Returns True if a row was deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM offices WHERE id = ?", (office_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def _col_1based_to_0based(val: Any) -> int:
    """CSV/DB: 1-based column index; 0 means 'no column'. Scraper: 0-based; use -1 for none."""
    v = int(val or 0)
    return (v - 1) if v > 0 else -1


def office_row_to_table_config(row: dict[str, Any]) -> dict[str, Any]:
    """Convert DB office row to scraper table_config format (0-based columns, booleans)."""
    return {
        "table_no": int(row["table_no"]),
        "table_rows": int(row["table_rows"]),
        "link_column": _col_1based_to_0based(row.get("link_column")),
        "party_column": _col_1based_to_0based(row.get("party_column")),
        "term_start_column": _col_1based_to_0based(row.get("term_start_column")),
        "term_end_column": _col_1based_to_0based(row.get("term_end_column")),
        "district_column": _col_1based_to_0based(row.get("district_column")),
        "run_dynamic_parse": bool(row.get("dynamic_parse")),
        "find_date_in_infobox": bool(row.get("find_date_in_infobox")),
        "read_columns_right_to_left": bool(row.get("read_right_to_left")),
        "parse_rowspan": bool(row.get("parse_rowspan")),
        "rep_link": bool(row.get("rep_link")),
        "party_link": bool(row.get("party_link")),
        "alt_link": row["alt_link"] if row.get("alt_link") else None,
    }


def office_row_to_office_details(row: dict[str, Any]) -> dict[str, Any]:
    """Convert DB office row to scraper office_details format (uses joined country_name, state_name, etc.)."""
    return {
        "office_country": row.get("country_name") or "",
        "office_level": row.get("level_name") or "",
        "office_branch": row.get("branch_name") or "",
        "office_department": row.get("department") or "",
        "office_name": row.get("name") or "",
        "office_state": row.get("state_name") or "",
        "office_notes": row.get("notes") or "",
    }
