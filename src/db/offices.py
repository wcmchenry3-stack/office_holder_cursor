"""Office config CRUD and list for scraper. Supports hierarchy: source_pages -> office_details -> office_table_config."""

import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .connection import get_connection, get_db_path
from .utils import _row_to_dict


def _use_hierarchy(conn: sqlite3.Connection) -> bool:
    """True if we have hierarchy data (source_pages has rows)."""
    try:
        n = conn.execute("SELECT COUNT(*) FROM source_pages").fetchone()[0]
        return n > 0
    except sqlite3.OperationalError:
        return False


def _flatten_hierarchy_row(
    p: dict, od: dict, tc: dict, country_name: str, state_name: str, level_name: str, branch_name: str, alt_links: list[str]
) -> dict[str, Any]:
    """Build a single flattened office row (same shape as legacy offices row) from hierarchy + ref names + alt_links."""
    def _int(v, default: int) -> int:
        if v is None:
            return default
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    return {
        "id": od["id"],
        "url": (p.get("url") or "").strip(),
        "name": (od.get("name") or "").strip(),
        "department": (od.get("department") or "").strip(),
        "notes": (od.get("notes") or "").strip(),
        "country_id": p.get("country_id"),
        "state_id": p.get("state_id"),
        "level_id": p.get("level_id"),
        "branch_id": p.get("branch_id"),
        "country_name": country_name or "",
        "state_name": state_name or "",
        "level_name": level_name or "",
        "branch_name": branch_name or "",
        "enabled": bool(tc.get("enabled") if tc.get("enabled") is not None else 1) and bool(od.get("enabled") if od.get("enabled") is not None else 1) and bool(p.get("enabled") if p.get("enabled") is not None else 1),
        "table_no": _int(tc.get("table_no"), 1),
        "table_rows": _int(tc.get("table_rows"), 4),
        "link_column": _int(tc.get("link_column"), 1),
        "party_column": _int(tc.get("party_column"), 0),
        "term_start_column": _int(tc.get("term_start_column"), 4),
        "term_end_column": _int(tc.get("term_end_column"), 5),
        "district_column": _int(tc.get("district_column"), 0),
        "dynamic_parse": bool(tc.get("dynamic_parse") if tc.get("dynamic_parse") is not None else 0),
        "read_right_to_left": bool(tc.get("read_right_to_left") if tc.get("read_right_to_left") is not None else 0),
        "find_date_in_infobox": bool(tc.get("find_date_in_infobox") if tc.get("find_date_in_infobox") is not None else 0),
        "parse_rowspan": bool(tc.get("parse_rowspan") if tc.get("parse_rowspan") is not None else 0),
        "consolidate_rowspan_terms": bool(tc.get("consolidate_rowspan_terms") if tc.get("consolidate_rowspan_terms") is not None else 0),
        "rep_link": bool(tc.get("rep_link") if tc.get("rep_link") is not None else 0),
        "party_link": bool(tc.get("party_link") if tc.get("party_link") is not None else 0),
        "alt_link_include_main": bool(od.get("alt_link_include_main") if od.get("alt_link_include_main") is not None else 0),
        "use_full_page_for_table": bool(tc.get("use_full_page_for_table") if tc.get("use_full_page_for_table") is not None else 0),
        "years_only": bool(tc.get("years_only") if tc.get("years_only") is not None else 0),
        "term_dates_merged": bool(tc.get("term_dates_merged") if tc.get("term_dates_merged") is not None else 0),
        "party_ignore": bool(tc.get("party_ignore") if tc.get("party_ignore") is not None else 0),
        "district_ignore": bool(tc.get("district_ignore") if tc.get("district_ignore") is not None else 0),
        "district_at_large": bool(tc.get("district_at_large") if tc.get("district_at_large") is not None else 0),
        "created_at": tc.get("created_at") or od.get("created_at"),
        "alt_links": list(alt_links) if alt_links else [],
    }


def _ref_names(conn: sqlite3.Connection, country_id: int | None, state_id: int | None, level_id: int | None, branch_id: int | None) -> tuple[str, str, str, str]:
    """Return (country_name, state_name, level_name, branch_name)."""
    c = s = lv = b = ""
    if country_id:
        r = conn.execute("SELECT name FROM countries WHERE id = ?", (country_id,)).fetchone()
        c = r["name"] if r else ""
    if state_id:
        r = conn.execute("SELECT name FROM states WHERE id = ?", (state_id,)).fetchone()
        s = r["name"] if r else ""
    if level_id:
        r = conn.execute("SELECT name FROM levels WHERE id = ?", (level_id,)).fetchone()
        lv = r["name"] if r else ""
    if branch_id:
        r = conn.execute("SELECT name FROM branches WHERE id = ?", (branch_id,)).fetchone()
        b = r["name"] if r else ""
    return (c, s, lv, b)


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


def get_runnable_unit_ids_for_office(office_id: int, conn: sqlite3.Connection | None = None) -> list[int]:
    """Return list of runnable unit ids (office_table_config_id in hierarchy) for this office (office_details_id). For legacy, returns [office_id]."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute(
                """SELECT tc.id FROM office_table_config tc
                   JOIN office_details od ON od.id = tc.office_details_id AND od.enabled = 1
                   JOIN source_pages p ON p.id = od.source_page_id AND p.enabled = 1
                   WHERE tc.office_details_id = ? AND tc.enabled = 1""",
                (office_id,),
            )
            return [row[0] for row in cur.fetchall()]
        return [office_id]
    finally:
        if own_conn:
            conn.close()


def list_runnable_units(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return runnable units from hierarchy: one per enabled office_table_config (page + office + table all enabled).
    Each unit is a flattened office_row with id=office_table_config_id, office_details_id, country_id, alt_links."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return []
        cur = conn.execute(
            """SELECT p.id AS page_id, p.country_id, p.state_id, p.level_id, p.branch_id, p.url, p.notes AS page_notes, p.enabled AS page_enabled,
                      od.id AS office_details_id, od.name, od.department, od.notes, od.alt_link_include_main, od.enabled AS od_enabled,
                      tc.id AS office_table_config_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                      tc.term_start_column, tc.term_end_column, tc.district_column, tc.dynamic_parse, tc.read_right_to_left,
                      tc.find_date_in_infobox, tc.parse_rowspan, tc.rep_link, tc.party_link, tc.enabled AS tc_enabled,
                      tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large,
                      tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.created_at
               FROM source_pages p
               JOIN office_details od ON od.source_page_id = p.id AND od.enabled = 1
               JOIN office_table_config tc ON tc.office_details_id = od.id AND tc.enabled = 1
               WHERE p.enabled = 1
               ORDER BY p.id, od.id, tc.id"""
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            rd = _row_to_dict(r)
            od_id = rd["office_details_id"]
            alt_links = [
                row["link_path"]
                for row in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
            ]
            c, s, lv, b = _ref_names(
                conn,
                rd.get("country_id"),
                rd.get("state_id"),
                rd.get("level_id"),
                rd.get("branch_id"),
            )
            p = {"url": rd.get("url"), "country_id": rd.get("country_id"), "state_id": rd.get("state_id"), "level_id": rd.get("level_id"), "branch_id": rd.get("branch_id"), "notes": rd.get("page_notes"), "enabled": rd.get("page_enabled")}
            od = {"id": od_id, "name": rd.get("name"), "department": rd.get("department"), "notes": rd.get("notes"), "alt_link_include_main": rd.get("alt_link_include_main"), "enabled": rd.get("od_enabled")}
            tc = {"table_no": rd.get("table_no"), "table_rows": rd.get("table_rows"), "link_column": rd.get("link_column"), "party_column": rd.get("party_column"), "term_start_column": rd.get("term_start_column"), "term_end_column": rd.get("term_end_column"), "district_column": rd.get("district_column"), "dynamic_parse": rd.get("dynamic_parse"), "read_right_to_left": rd.get("read_right_to_left"), "find_date_in_infobox": rd.get("find_date_in_infobox"), "parse_rowspan": rd.get("parse_rowspan"), "rep_link": rd.get("rep_link"), "party_link": rd.get("party_link"), "enabled": rd.get("tc_enabled"), "use_full_page_for_table": rd.get("use_full_page_for_table"), "years_only": rd.get("years_only"), "term_dates_merged": rd.get("term_dates_merged"), "party_ignore": rd.get("party_ignore"), "district_ignore": rd.get("district_ignore"), "district_at_large": rd.get("district_at_large"), "consolidate_rowspan_terms": rd.get("consolidate_rowspan_terms"), "notes": rd.get("tc_notes"), "created_at": rd.get("created_at")}
            flat = _flatten_hierarchy_row(p, od, tc, c, s, lv, b, alt_links)
            flat["id"] = rd["office_table_config_id"]
            flat["office_details_id"] = od_id
            flat["office_table_config_id"] = rd["office_table_config_id"]
            flat["country_id"] = rd.get("country_id")
            out.append(flat)
        return out
    finally:
        if own_conn:
            conn.close()


def list_offices(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all office configs as list of dicts (with country_name, state_name, level_name, branch_name from FKs).
    Uses hierarchy (office_details) when available; else legacy offices table."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute(
                """SELECT p.id AS page_id, p.country_id, p.state_id, p.level_id, p.branch_id, p.url, p.notes AS page_notes, p.enabled AS page_enabled,
                          od.id AS office_details_id, od.name, od.department, od.notes, od.alt_link_include_main, od.enabled AS od_enabled,
                          tc.id AS tc_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                          tc.term_start_column, tc.term_end_column, tc.district_column, tc.dynamic_parse, tc.read_right_to_left,
                          tc.find_date_in_infobox, tc.parse_rowspan, tc.rep_link, tc.party_link, tc.enabled AS tc_enabled,
                          tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large,
                          tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.created_at
                   FROM office_details od
                   JOIN source_pages p ON p.id = od.source_page_id
                   LEFT JOIN office_table_config tc ON tc.office_details_id = od.id
                   ORDER BY p.id, od.id"""
            )
            rows = cur.fetchall()
            out = []
            for r in rows:
                rd = _row_to_dict(r)
                od_id = rd["office_details_id"]
                alt_links = [
                    row["link_path"]
                    for row in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
                ]
                c, s, lv, b = _ref_names(conn, rd.get("country_id"), rd.get("state_id"), rd.get("level_id"), rd.get("branch_id"))
                p = {"url": rd.get("url"), "country_id": rd.get("country_id"), "state_id": rd.get("state_id"), "level_id": rd.get("level_id"), "branch_id": rd.get("branch_id"), "notes": rd.get("page_notes"), "enabled": rd.get("page_enabled")}
                od = {"id": od_id, "name": rd.get("name"), "department": rd.get("department"), "notes": rd.get("notes"), "alt_link_include_main": rd.get("alt_link_include_main"), "enabled": rd.get("od_enabled")}
                tc = {"table_no": rd.get("table_no"), "table_rows": rd.get("table_rows"), "link_column": rd.get("link_column"), "party_column": rd.get("party_column"), "term_start_column": rd.get("term_start_column"), "term_end_column": rd.get("term_end_column"), "district_column": rd.get("district_column"), "dynamic_parse": rd.get("dynamic_parse"), "read_right_to_left": rd.get("read_right_to_left"), "find_date_in_infobox": rd.get("find_date_in_infobox"), "parse_rowspan": rd.get("parse_rowspan"), "rep_link": rd.get("rep_link"), "party_link": rd.get("party_link"), "enabled": rd.get("tc_enabled"), "use_full_page_for_table": rd.get("use_full_page_for_table"), "years_only": rd.get("years_only"), "term_dates_merged": rd.get("term_dates_merged"), "party_ignore": rd.get("party_ignore"), "district_ignore": rd.get("district_ignore"), "district_at_large": rd.get("district_at_large"), "consolidate_rowspan_terms": rd.get("consolidate_rowspan_terms"), "notes": rd.get("tc_notes"), "created_at": rd.get("created_at")}
                flat = _flatten_hierarchy_row(p, od, tc, c, s, lv, b, alt_links)
                flat["id"] = od_id
                out.append(flat)
            return out
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
    """Return one office by id. With hierarchy, office_id is office_details_id; else legacy offices id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute(
                """SELECT p.id AS page_id, p.country_id, p.state_id, p.level_id, p.branch_id, p.url, p.notes AS page_notes, p.enabled AS page_enabled,
                          od.id AS office_details_id, od.name, od.department, od.notes, od.alt_link_include_main, od.enabled AS od_enabled,
                          tc.id AS tc_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                          tc.term_start_column, tc.term_end_column, tc.district_column, tc.dynamic_parse, tc.read_right_to_left,
                          tc.find_date_in_infobox, tc.parse_rowspan, tc.rep_link, tc.party_link, tc.enabled AS tc_enabled,
                          tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large,
                          tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.created_at
                   FROM office_details od
                   JOIN source_pages p ON p.id = od.source_page_id
                   LEFT JOIN office_table_config tc ON tc.office_details_id = od.id
                   WHERE od.id = ?""",
                (office_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            rd = _row_to_dict(row)
            od_id = rd["office_details_id"]
            alt_links = [
                r["link_path"]
                for r in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
            ]
            c, s, lv, b = _ref_names(conn, rd.get("country_id"), rd.get("state_id"), rd.get("level_id"), rd.get("branch_id"))
            p = {"url": rd.get("url"), "country_id": rd.get("country_id"), "state_id": rd.get("state_id"), "level_id": rd.get("level_id"), "branch_id": rd.get("branch_id"), "notes": rd.get("page_notes"), "enabled": rd.get("page_enabled")}
            od = {"id": od_id, "name": rd.get("name"), "department": rd.get("department"), "notes": rd.get("notes"), "alt_link_include_main": rd.get("alt_link_include_main"), "enabled": rd.get("od_enabled")}
            tc = {"table_no": rd.get("table_no"), "table_rows": rd.get("table_rows"), "link_column": rd.get("link_column"), "party_column": rd.get("party_column"), "term_start_column": rd.get("term_start_column"), "term_end_column": rd.get("term_end_column"), "district_column": rd.get("district_column"), "dynamic_parse": rd.get("dynamic_parse"), "read_right_to_left": rd.get("read_right_to_left"), "find_date_in_infobox": rd.get("find_date_in_infobox"), "parse_rowspan": rd.get("parse_rowspan"), "rep_link": rd.get("rep_link"), "party_link": rd.get("party_link"), "enabled": rd.get("tc_enabled"), "use_full_page_for_table": rd.get("use_full_page_for_table"), "years_only": rd.get("years_only"), "term_dates_merged": rd.get("term_dates_merged"), "party_ignore": rd.get("party_ignore"), "district_ignore": rd.get("district_ignore"), "district_at_large": rd.get("district_at_large"), "consolidate_rowspan_terms": rd.get("consolidate_rowspan_terms"), "notes": rd.get("tc_notes"), "created_at": rd.get("created_at")}
            flat = _flatten_hierarchy_row(p, od, tc, c, s, lv, b, alt_links)
            flat["id"] = od_id
            return flat
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
    """Insert office and return new id (office_details_id in hierarchy). Creates source_page + office_details + office_table_config."""
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
        enabled = 1 if row_data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        conn.execute(
            """INSERT INTO source_pages (country_id, state_id, level_id, branch_id, url, notes, enabled, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                country_id,
                int(row_data.get("state_id") or 0) or None,
                int(row_data.get("level_id") or 0) or None,
                int(row_data.get("branch_id") or 0) or None,
                (row_data.get("url") or "").strip(),
                row_data.get("notes") or "",
                enabled,
            ),
        )
        page_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO office_details (source_page_id, name, variant_name, department, notes, alt_link_include_main, enabled, created_at, updated_at)
               VALUES (?, ?, '', ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                page_id,
                (row_data.get("name") or "").strip(),
                row_data.get("department") or "",
                row_data.get("notes") or "",
                1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                enabled,
            ),
        )
        od_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO office_table_config (office_details_id, table_no, table_rows, link_column, party_column,
                  term_start_column, term_end_column, district_column, dynamic_parse, read_right_to_left, find_date_in_infobox,
                  parse_rowspan, rep_link, party_link, enabled, use_full_page_for_table, years_only,
                  term_dates_merged, party_ignore, district_ignore, district_at_large, consolidate_rowspan_terms, notes, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                od_id,
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
                1 if row_data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                enabled,
                1 if row_data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                1 if row_data.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                1 if term_dates_merged else 0,
                1 if party_ignore else 0,
                1 if district_ignore else 0,
                1 if district_at_large else 0,
                1 if row_data.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                row_data.get("notes") or "",
            ),
        )
        conn.commit()
        set_alt_links_for_office(od_id, row_data.get("alt_links") or [], conn=conn)
        return od_id
    finally:
        if own_conn:
            conn.close()


def update_office(office_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> bool:
    """Update office by id (office_details_id in hierarchy). Updates source_page, office_details, office_table_config."""
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
        enabled_val = 1 if data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        if _use_hierarchy(conn):
            row = conn.execute("SELECT source_page_id FROM office_details WHERE id = ?", (office_id,)).fetchone()
            if not row:
                return False
            page_id = row["source_page_id"]
            conn.execute(
                """UPDATE source_pages SET country_id=?, state_id=?, level_id=?, branch_id=?, url=?, notes=?, enabled=?, updated_at=datetime('now') WHERE id=?""",
                (
                    country_id,
                    int(row_data.get("state_id") or 0) or None,
                    int(row_data.get("level_id") or 0) or None,
                    int(row_data.get("branch_id") or 0) or None,
                    (row_data.get("url") or "").strip(),
                    row_data.get("notes") or "",
                    enabled_val,
                    page_id,
                ),
            )
            conn.execute(
                """UPDATE office_details SET name=?, department=?, notes=?, alt_link_include_main=?, enabled=?, updated_at=datetime('now') WHERE id=?""",
                (
                    (row_data.get("name") or "").strip(),
                    row_data.get("department") or "",
                    row_data.get("notes") or "",
                    1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                    enabled_val,
                    office_id,
                ),
            )
            conn.execute(
                """UPDATE office_table_config SET table_no=?, table_rows=?, link_column=?, party_column=?,
                      term_start_column=?, term_end_column=?, district_column=?, dynamic_parse=?, read_right_to_left=?,
                      find_date_in_infobox=?, parse_rowspan=?, rep_link=?, party_link=?, enabled=?, use_full_page_for_table=?,
                      years_only=?, term_dates_merged=?, party_ignore=?, district_ignore=?, district_at_large=?,
                      consolidate_rowspan_terms=?, notes=?, updated_at=datetime('now') WHERE office_details_id=?""",
                (
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
                    1 if row_data.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                    1 if row_data.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                    enabled_val,
                    1 if row_data.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                    1 if row_data.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                    1 if term_dates_merged else 0,
                    1 if party_ignore else 0,
                    1 if district_ignore else 0,
                    1 if district_at_large else 0,
                    1 if row_data.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                    row_data.get("notes") or "",
                    office_id,
                ),
            )
            conn.commit()
            set_alt_links_for_office(office_id, row_data.get("alt_links") or [], conn=conn)
            return True
        if "enabled" not in data:
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
    """Set enabled flag for one office (office_details_id in hierarchy). Returns True if a row was updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute("UPDATE office_details SET enabled = ? WHERE id = ?", (1 if enabled else 0, office_id))
            conn.commit()
            return cur.rowcount > 0
        cur = conn.execute("UPDATE offices SET enabled = ? WHERE id = ?", (1 if enabled else 0, office_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def set_all_offices_enabled(enabled: bool, conn: sqlite3.Connection | None = None) -> int:
    """Set enabled flag for all offices (office_details in hierarchy). Returns number of rows updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute("UPDATE office_details SET enabled = ?", (1 if enabled else 0,))
        else:
            cur = conn.execute("UPDATE offices SET enabled = ?", (1 if enabled else 0,))
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def delete_office(office_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete office by id (office_details_id in hierarchy: table_configs, alt_links, terms, office_details, source_page)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            row = conn.execute("SELECT source_page_id FROM office_details WHERE id = ?", (office_id,)).fetchone()
            if not row:
                return False
            page_id = row["source_page_id"]
            conn.execute("DELETE FROM office_table_config WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM office_terms WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM office_details WHERE id = ?", (office_id,))
            conn.execute("DELETE FROM source_pages WHERE id = ?", (page_id,))
            conn.commit()
            return True
        conn.execute("DELETE FROM alt_links WHERE office_id = ?", (office_id,))
        cur = conn.execute("DELETE FROM offices WHERE id = ?", (office_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def list_alt_links(office_id: int, conn: sqlite3.Connection | None = None) -> list[str]:
    """Return list of link_path strings for the office (office_details_id in hierarchy)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            cur = conn.execute(
                "SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id",
                (office_id,),
            )
            return [row["link_path"] for row in cur.fetchall()]
        cur = conn.execute(
            "SELECT link_path FROM alt_links WHERE office_id = ? ORDER BY id",
            (office_id,),
        )
        return [row["link_path"] for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def set_alt_links_for_office(office_id: int, paths: list[str], conn: sqlite3.Connection | None = None) -> None:
    """Replace all alt links for the office (office_details_id in hierarchy)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (office_id,))
            for raw in paths:
                path = _normalize_alt_link_path(raw)
                if path:
                    conn.execute(
                        "INSERT OR IGNORE INTO alt_links (office_details_id, link_path) VALUES (?, ?)",
                        (office_id, path),
                    )
        else:
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
