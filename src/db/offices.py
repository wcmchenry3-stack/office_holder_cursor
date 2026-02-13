"""Office config CRUD and list for scraper."""

import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .connection import get_connection, get_db_path
from .utils import _row_to_dict


def _bool(data: dict, key: str) -> bool:
    """Return True if data[key] is truthy (1, true, '1', 'true', etc.)."""
    v = data.get(key)
    return v is not None and str(v).strip().lower() in ("true", "1", "yes")


def validate_office_table_config(
    data: dict[str, Any],
    *,
    term_dates_merged: bool = False,
    party_ignore: bool = False,
    district_ignore: bool = False,
    district_at_large: bool = False,
) -> None:
    """
    Validate table_no, table_rows, and column settings. Raises ValueError with a clear message on failure.
    Form/DB use 1-based column numbers; 0 means 'no column' for party/district.
    When term_dates_merged is True, term_start and term_end may be equal.
    When party_ignore is True, party_column is not required to be distinct.
    When district_ignore or district_at_large is True, district_column is not required to be distinct.
    """
    try:
        table_no = int(data.get("table_no", 1))
        table_rows = int(data.get("table_rows", 4))
    except (TypeError, ValueError):
        raise ValueError("table_no and table_rows must be integers") from None
    if table_no < 1 or table_rows < 1:
        raise ValueError("table_no and table_rows must be at least 1")

    try:
        link_column = int(data.get("link_column", 1))
        party_column = int(data.get("party_column", 0))
        term_start_column = int(data.get("term_start_column", 4))
        term_end_column = int(data.get("term_end_column", 5))
        district_column = int(data.get("district_column", 0))
    except (TypeError, ValueError):
        raise ValueError("link, party, term start, term end, and district columns must be integers") from None

    dynamic_parse = data.get("dynamic_parse") in (True, 1, "1", "true", "TRUE")
    if link_column < 1 and not dynamic_parse:
        raise ValueError("link column must be at least 1")
    if term_start_column < 1 or term_end_column < 1:
        raise ValueError("term start and term end columns must be at least 1")

    # Build set of column numbers that must be pairwise distinct (only positive values count).
    # When term_dates_merged, only one "term" column counts for distinctness.
    if term_dates_merged:
        used = [link_column, term_start_column]
    else:
        used = [link_column, term_start_column, term_end_column]
    if not party_ignore:
        used.append(party_column)
    if not district_ignore and not district_at_large:
        used.append(district_column)
    # Require all positive values to be distinct
    positive = [c for c in used if c > 0]
    if len(positive) != len(set(positive)):
        raise ValueError(
            "link, party, term start, term end, and district columns must all be different "
            "(when term dates merged, term start and end may be the same)"
        )


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
                      o.parse_rowspan, o.consolidate_rowspan_terms, o.rep_link, o.party_link, o.alt_link_include_main,
                      o.use_full_page_for_table, o.years_only,
                      o.term_dates_merged, o.party_ignore, o.district_ignore, o.district_at_large,
                      o.created_at
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
        term_dates_merged = _bool(data, "term_dates_merged")
        party_ignore = _bool(data, "party_ignore")
        district_ignore = _bool(data, "district_ignore")
        district_at_large = _bool(data, "district_at_large")
        # When merged, force term_end = term_start
        row_data = dict(data)
        if term_dates_merged:
            row_data["term_end_column"] = row_data.get("term_start_column", 4)
        validate_office_table_config(
            row_data,
            term_dates_merged=term_dates_merged,
            party_ignore=party_ignore,
            district_ignore=district_ignore,
            district_at_large=district_at_large,
        )
        cur = conn.execute(
            """INSERT INTO offices (
                country_id, state_id, level_id, branch_id, department, name, enabled, notes,
                url, table_no, table_rows, link_column, party_column,
                term_start_column, term_end_column, district_column,
                dynamic_parse, read_right_to_left, find_date_in_infobox,
                parse_rowspan, consolidate_rowspan_terms, rep_link, party_link, alt_link_include_main, use_full_page_for_table, years_only,
                term_dates_merged, party_ignore, district_ignore, district_at_large
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                country_id,
                int(row_data.get("state_id") or 0) or None,
                int(row_data.get("level_id") or 0) or None,
                int(row_data.get("branch_id") or 0) or None,
                row_data.get("department") or "",
                row_data.get("name") or "",
                1 if row_data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0,
                row_data.get("notes") or "",
                row_data.get("url") or "",
                int(row_data.get("table_no", 1)),
                int(row_data.get("table_rows", 4)),
                int(row_data.get("link_column", 1)),
                int(row_data.get("party_column", 0)),
                int(row_data.get("term_start_column", 4)),
                int(row_data.get("term_end_column", 5)),
                int(row_data.get("district_column", 0)),
                1 if row_data.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                1 if term_dates_merged else 0,
                1 if party_ignore else 0,
                1 if district_ignore else 0,
                1 if district_at_large else 0,
            ),
        )
        conn.commit()
        new_id = cur.lastrowid
        set_alt_links_for_office(new_id, row_data.get("alt_links") or [], conn=conn)
        return new_id
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
        term_dates_merged = _bool(data, "term_dates_merged")
        party_ignore = _bool(data, "party_ignore")
        district_ignore = _bool(data, "district_ignore")
        district_at_large = _bool(data, "district_at_large")
        row_data = dict(data)
        if term_dates_merged:
            row_data["term_end_column"] = row_data.get("term_start_column", 4)
        validate_office_table_config(
            row_data,
            term_dates_merged=term_dates_merged,
            party_ignore=party_ignore,
            district_ignore=district_ignore,
            district_at_large=district_at_large,
        )
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
                parse_rowspan=?, consolidate_rowspan_terms=?, rep_link=?, party_link=?, alt_link_include_main=?, use_full_page_for_table=?, years_only=?,
                term_dates_merged=?, party_ignore=?, district_ignore=?, district_at_large=?
            WHERE id=?""",
            (
                country_id,
                int(row_data.get("state_id") or 0) or None,
                int(row_data.get("level_id") or 0) or None,
                int(row_data.get("branch_id") or 0) or None,
                row_data.get("department") or "",
                row_data.get("name") or "",
                enabled_val,
                row_data.get("notes") or "",
                row_data.get("url") or "",
                int(row_data.get("table_no", 1)),
                int(row_data.get("table_rows", 4)),
                int(row_data.get("link_column", 1)),
                int(row_data.get("party_column", 0)),
                int(row_data.get("term_start_column", 4)),
                int(row_data.get("term_end_column", 5)),
                int(row_data.get("district_column", 0)),
                1 if row_data.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                1 if term_dates_merged else 0,
                1 if party_ignore else 0,
                1 if district_ignore else 0,
                1 if district_at_large else 0,
                office_id,
            ),
        )
        conn.commit()
        set_alt_links_for_office(office_id, row_data.get("alt_links") or [], conn=conn)
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
    """Delete office by id (and its alt_links). Returns True if a row was deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute("DELETE FROM alt_links WHERE office_id = ?", (office_id,))
        cur = conn.execute("DELETE FROM offices WHERE id = ?", (office_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def list_alt_links(office_id: int, conn: sqlite3.Connection | None = None) -> list[str]:
    """Return list of link_path strings for the office (from alt_links table)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT link_path FROM alt_links WHERE office_id = ? ORDER BY id",
            (office_id,),
        )
        return [row["link_path"] for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def set_alt_links_for_office(office_id: int, paths: list[str], conn: sqlite3.Connection | None = None) -> None:
    """Replace all alt links for the office with the given list of paths (normalized)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute("DELETE FROM alt_links WHERE office_id = ?", (office_id,))
        for raw in paths:
            path = _normalize_alt_link_path(raw)
            if path:
                conn.execute(
                    "INSERT OR IGNORE INTO alt_links (office_id, link_path) VALUES (?, ?)",
                    (office_id, path),
                )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def _normalize_alt_link_path(raw: Any) -> str:
    """Normalize to a path (e.g. /wiki/Foo)."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s or s.lower() in ("none", ""):
        return ""
    if s.startswith("http"):
        return urlparse(s).path or ""
    if not s.startswith("/"):
        return "/wiki/" + s.lstrip("/")
    return s if s.startswith("/wiki/") else "/wiki/" + s.lstrip("/")


def _col_1based_to_0based(val: Any) -> int:
    """CSV/DB: 1-based column index; 0 means 'no column'. Scraper: 0-based; use -1 for none."""
    v = int(val or 0)
    return (v - 1) if v > 0 else -1


def office_row_to_table_config(row: dict[str, Any], alt_links: list[str] | None = None) -> dict[str, Any]:
    """Convert DB office row to scraper table_config format (0-based columns, booleans). alt_links from list_alt_links(office_id)."""
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
        "years_only": bool(row.get("years_only")),
        "read_columns_right_to_left": bool(row.get("read_right_to_left")),
        "parse_rowspan": bool(row.get("parse_rowspan")),
        "consolidate_rowspan_terms": bool(row.get("consolidate_rowspan_terms")),
        "rep_link": bool(row.get("rep_link")),
        "party_link": bool(row.get("party_link")),
        "alt_links": list(alt_links) if alt_links is not None else [],
        "alt_link_include_main": bool(row.get("alt_link_include_main")),
        "term_dates_merged": bool(row.get("term_dates_merged")),
        "party_ignore": bool(row.get("party_ignore")),
        "district_ignore": bool(row.get("district_ignore")),
        "district_at_large": bool(row.get("district_at_large")),
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
