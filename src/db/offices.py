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


def use_hierarchy(conn: sqlite3.Connection | None = None) -> bool:
    """True if hierarchy (source_pages) is in use. Public wrapper for route logic."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        return _use_hierarchy(conn)
    finally:
        if own_conn:
            conn.close()


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


def _tc_row_to_config(rd: dict[str, Any]) -> dict[str, Any]:
    """Build a table config dict from a row dict (tc_id, table_no, table_rows, ...). Used for get_office / list grouping."""
    return {
        "id": rd.get("tc_id"),
        "table_no": rd.get("table_no"),
        "table_rows": rd.get("table_rows"),
        "link_column": rd.get("link_column"),
        "party_column": rd.get("party_column"),
        "term_start_column": rd.get("term_start_column"),
        "term_end_column": rd.get("term_end_column"),
        "district_column": rd.get("district_column"),
        "dynamic_parse": rd.get("dynamic_parse"),
        "read_right_to_left": rd.get("read_right_to_left"),
        "find_date_in_infobox": rd.get("find_date_in_infobox"),
        "parse_rowspan": rd.get("parse_rowspan"),
        "rep_link": rd.get("rep_link"),
        "party_link": rd.get("party_link"),
        "enabled": rd.get("tc_enabled"),
        "use_full_page_for_table": rd.get("use_full_page_for_table"),
        "years_only": rd.get("years_only"),
        "term_dates_merged": rd.get("term_dates_merged"),
        "party_ignore": rd.get("party_ignore"),
        "district_ignore": rd.get("district_ignore"),
        "district_at_large": rd.get("district_at_large"),
        "consolidate_rowspan_terms": rd.get("consolidate_rowspan_terms"),
        "notes": rd.get("tc_notes"),
        "name": rd.get("tc_name") or "",
        "created_at": rd.get("created_at"),
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

    # Build list of (value, field_name) for columns that must be pairwise distinct (only positive values count).
    # When term_dates_merged, only one "term" column counts for distinctness.
    entries: list[tuple[int, str]] = []
    entries.append((link_column, "Link column"))
    entries.append((term_start_column, "Term start column"))
    if not term_dates_merged:
        entries.append((term_end_column, "Term end column"))
    if not party_ignore:
        entries.append((party_column, "Party column"))
    if not district_ignore and not district_at_large:
        entries.append((district_column, "District column"))
    positive_entries = [(v, n) for v, n in entries if v > 0]
    values = [v for v, _ in positive_entries]
    if len(values) != len(set(values)):
        if not term_dates_merged and term_start_column == term_end_column and term_start_column > 0:
            raise ValueError(
                "Term start column and term end column must be different, or check 'Term dates merged'."
            )
        # Find which value is duplicated and which field names use it
        seen: dict[int, list[str]] = {}
        for v, n in positive_entries:
            seen.setdefault(v, []).append(n)
        dup_val = next(v for v, names in seen.items() if len(names) > 1)
        names_using = seen[dup_val]
        raise ValueError(
            f"Duplicate column number: {dup_val}. "
            f"{' and '.join(names_using)} both use {dup_val}. Each must use a different column number."
        )


def _table_nos_on_page(
    conn: sqlite3.Connection, source_page_id: int, *, exclude_office_details_id: int | None = None
) -> set[int]:
    """Return set of table_no values from all office_table_config rows on this page (optionally excluding one office)."""
    if exclude_office_details_id is not None:
        cur = conn.execute(
            """SELECT tc.table_no FROM office_table_config tc
               JOIN office_details od ON od.id = tc.office_details_id WHERE od.source_page_id = ? AND od.id != ?""",
            (source_page_id, exclude_office_details_id),
        )
    else:
        cur = conn.execute(
            """SELECT tc.table_no FROM office_table_config tc
               JOIN office_details od ON od.id = tc.office_details_id WHERE od.source_page_id = ?""",
            (source_page_id,),
        )
    return {int(row[0]) for row in cur.fetchall() if row[0] is not None}


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
                          tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.name AS tc_name, tc.created_at
                   FROM office_details od
                   JOIN source_pages p ON p.id = od.source_page_id
                   LEFT JOIN office_table_config tc ON tc.office_details_id = od.id
                   ORDER BY p.id, od.id, tc.table_no, tc.id"""
            )
            rows = cur.fetchall()
            by_od: dict[int, list[dict]] = {}
            for r in rows:
                rd = _row_to_dict(r)
                od_id = rd["office_details_id"]
                if od_id not in by_od:
                    by_od[od_id] = []
                by_od[od_id].append(rd)
            out = []
            for od_id, group in by_od.items():
                rd0 = group[0]
                alt_links = [
                    row["link_path"]
                    for row in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
                ]
                c, s, lv, b = _ref_names(conn, rd0.get("country_id"), rd0.get("state_id"), rd0.get("level_id"), rd0.get("branch_id"))
                p = {"url": rd0.get("url"), "country_id": rd0.get("country_id"), "state_id": rd0.get("state_id"), "level_id": rd0.get("level_id"), "branch_id": rd0.get("branch_id"), "notes": rd0.get("page_notes"), "enabled": rd0.get("page_enabled")}
                od = {"id": od_id, "name": rd0.get("name"), "department": rd0.get("department"), "notes": rd0.get("notes"), "alt_link_include_main": rd0.get("alt_link_include_main"), "enabled": rd0.get("od_enabled")}
                table_configs = []
                for rd in group:
                    if rd.get("tc_id") is not None:
                        table_configs.append(_tc_row_to_config(rd))
                table_configs.sort(key=lambda x: (x.get("table_no") or 0, x.get("id") or 0))
                first_tc = table_configs[0] if table_configs else {}
                tc_flat = {"table_no": first_tc.get("table_no"), "table_rows": first_tc.get("table_rows"), "link_column": first_tc.get("link_column"), "party_column": first_tc.get("party_column"), "term_start_column": first_tc.get("term_start_column"), "term_end_column": first_tc.get("term_end_column"), "district_column": first_tc.get("district_column"), "dynamic_parse": first_tc.get("dynamic_parse"), "read_right_to_left": first_tc.get("read_right_to_left"), "find_date_in_infobox": first_tc.get("find_date_in_infobox"), "parse_rowspan": first_tc.get("parse_rowspan"), "rep_link": first_tc.get("rep_link"), "party_link": first_tc.get("party_link"), "enabled": first_tc.get("enabled"), "use_full_page_for_table": first_tc.get("use_full_page_for_table"), "years_only": first_tc.get("years_only"), "term_dates_merged": first_tc.get("term_dates_merged"), "party_ignore": first_tc.get("party_ignore"), "district_ignore": first_tc.get("district_ignore"), "district_at_large": first_tc.get("district_at_large"), "consolidate_rowspan_terms": first_tc.get("consolidate_rowspan_terms"), "notes": first_tc.get("notes"), "created_at": first_tc.get("created_at")}
                flat = _flatten_hierarchy_row(p, od, tc_flat, c, s, lv, b, alt_links)
                flat["id"] = od_id
                flat["source_page_id"] = rd0.get("page_id")
                flat["table_configs"] = table_configs
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


def list_pages(
    country_id: int | None = None,
    state_id: int | None = None,
    level_id: int | None = None,
    branch_id: int | None = None,
    enabled: int | None = None,
    limit: int | None = None,
    office_count_filter: str = "all",
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return source pages with optional filters and counts (office_count, table_count, first_office_id).
    Used when hierarchy is in use; returns [] otherwise.
    office_count_filter: "all", "gt0" (has offices), or "eq0" (no offices)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return []
        where_parts: list[str] = ["1=1"]
        params: list[Any] = []
        if country_id is not None and country_id != 0:
            where_parts.append("p.country_id = ?")
            params.append(country_id)
        if state_id is not None and state_id != 0:
            where_parts.append("p.state_id = ?")
            params.append(state_id)
        if level_id is not None and level_id != 0:
            where_parts.append("p.level_id = ?")
            params.append(level_id)
        if branch_id is not None and branch_id != 0:
            where_parts.append("p.branch_id = ?")
            params.append(branch_id)
        if enabled is not None and enabled in (0, 1):
            where_parts.append("p.enabled = ?")
            params.append(enabled)
        if office_count_filter == "gt0":
            where_parts.append("(SELECT COUNT(*) FROM office_details od WHERE od.source_page_id = p.id) > 0")
        elif office_count_filter == "eq0":
            where_parts.append("(SELECT COUNT(*) FROM office_details od WHERE od.source_page_id = p.id) = 0")
        where_sql = " AND ".join(where_parts)
        limit_sql = ""
        if limit is not None and limit > 0:
            limit_sql = " LIMIT ?"
            params.append(limit)
        sql = f"""
            SELECT p.id, p.country_id, p.state_id, p.level_id, p.branch_id, p.url, p.enabled,
                   c.name AS country_name, s.name AS state_name, l.name AS level_name, b.name AS branch_name,
                   (SELECT COUNT(*) FROM office_details od WHERE od.source_page_id = p.id) AS office_count,
                   (SELECT COUNT(*) FROM office_details od
                    JOIN office_table_config tc ON tc.office_details_id = od.id
                    WHERE od.source_page_id = p.id) AS table_count,
                   (SELECT MIN(od.id) FROM office_details od WHERE od.source_page_id = p.id) AS first_office_id
            FROM source_pages p
            LEFT JOIN countries c ON c.id = p.country_id
            LEFT JOIN states s ON s.id = p.state_id
            LEFT JOIN levels l ON l.id = p.level_id
            LEFT JOIN branches b ON b.id = p.branch_id
            WHERE {where_sql}
            ORDER BY COALESCE(c.name, ''), COALESCE(l.name, ''), COALESCE(b.name, ''), p.url
            {limit_sql}
        """
        cur = conn.execute(sql, params)
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
                          od.id AS office_details_id, od.name, od.department, od.notes, od.alt_link_include_main, od.enabled AS od_enabled, od.office_category_id,
                          tc.id AS tc_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                          tc.term_start_column, tc.term_end_column, tc.district_column, tc.dynamic_parse, tc.read_right_to_left,
                          tc.find_date_in_infobox, tc.parse_rowspan, tc.rep_link, tc.party_link, tc.enabled AS tc_enabled,
                          tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large,
                          tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.name AS tc_name, tc.created_at
                   FROM office_details od
                   JOIN source_pages p ON p.id = od.source_page_id
                   LEFT JOIN office_table_config tc ON tc.office_details_id = od.id
                   WHERE od.id = ?""",
                (office_id,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            rd0 = _row_to_dict(rows[0])
            od_id = rd0["office_details_id"]
            alt_links = [
                r["link_path"]
                for r in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
            ]
            c, s, lv, b = _ref_names(conn, rd0.get("country_id"), rd0.get("state_id"), rd0.get("level_id"), rd0.get("branch_id"))
            p = {"url": rd0.get("url"), "country_id": rd0.get("country_id"), "state_id": rd0.get("state_id"), "level_id": rd0.get("level_id"), "branch_id": rd0.get("branch_id"), "notes": rd0.get("page_notes"), "enabled": rd0.get("page_enabled")}
            od = {"id": od_id, "name": rd0.get("name"), "department": rd0.get("department"), "notes": rd0.get("notes"), "alt_link_include_main": rd0.get("alt_link_include_main"), "enabled": rd0.get("od_enabled"), "office_category_id": rd0.get("office_category_id")}
            table_configs = []
            for r in rows:
                rd = _row_to_dict(r)
                if rd.get("tc_id") is not None:
                    table_configs.append(_tc_row_to_config(rd))
            table_configs.sort(key=lambda x: (x.get("table_no") or 0, x.get("id") or 0))
            first_tc = table_configs[0] if table_configs else {}
            tc_flat = {"table_no": first_tc.get("table_no"), "table_rows": first_tc.get("table_rows"), "link_column": first_tc.get("link_column"), "party_column": first_tc.get("party_column"), "term_start_column": first_tc.get("term_start_column"), "term_end_column": first_tc.get("term_end_column"), "district_column": first_tc.get("district_column"), "dynamic_parse": first_tc.get("dynamic_parse"), "read_right_to_left": first_tc.get("read_right_to_left"), "find_date_in_infobox": first_tc.get("find_date_in_infobox"), "parse_rowspan": first_tc.get("parse_rowspan"), "rep_link": first_tc.get("rep_link"), "party_link": first_tc.get("party_link"), "enabled": first_tc.get("enabled"), "use_full_page_for_table": first_tc.get("use_full_page_for_table"), "years_only": first_tc.get("years_only"), "term_dates_merged": first_tc.get("term_dates_merged"), "party_ignore": first_tc.get("party_ignore"), "district_ignore": first_tc.get("district_ignore"), "district_at_large": first_tc.get("district_at_large"), "consolidate_rowspan_terms": first_tc.get("consolidate_rowspan_terms"), "notes": first_tc.get("notes"), "created_at": first_tc.get("created_at")}
            flat = _flatten_hierarchy_row(p, od, tc_flat, c, s, lv, b, alt_links)
            flat["id"] = od_id
            flat["source_page_id"] = rd0.get("page_id")
            flat["table_configs"] = table_configs
            flat["office_category_id"] = rd0.get("office_category_id")
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


def get_page(source_page_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return source_pages row as dict (id, url, country_id, state_id, level_id, branch_id, notes, enabled, allow_reuse_tables) or None."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, url, country_id, state_id, level_id, branch_id, notes, enabled FROM source_pages WHERE id = ?",
            (source_page_id,),
        ).fetchone()
        if not row:
            return None
        d = _row_to_dict(row)
        try:
            r2 = conn.execute("SELECT allow_reuse_tables FROM source_pages WHERE id = ?", (source_page_id,)).fetchone()
            d["allow_reuse_tables"] = r2[0] if r2 is not None else 0
        except (sqlite3.OperationalError, IndexError):
            d["allow_reuse_tables"] = 0
        return d
    finally:
        if own_conn:
            conn.close()


def get_source_page_id_by_url(url: str, conn: sqlite3.Connection | None = None) -> int | None:
    """Return source_pages.id if a row exists with this URL (comparison: trimmed, case-insensitive). Else None."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        url_clean = (url or "").strip()
        if not url_clean:
            return None
        row = conn.execute(
            "SELECT id FROM source_pages WHERE LOWER(TRIM(url)) = LOWER(?)",
            (url_clean,),
        ).fetchone()
        return row[0] if row else None
    finally:
        if own_conn:
            conn.close()


def update_page(source_page_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> bool:
    """Update only source_pages row. Data: url, country_id, state_id, level_id, branch_id, notes, enabled, allow_reuse_tables."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        url = (data.get("url") or "").strip()
        if not url:
            raise ValueError("url required")
        enabled = 1 if data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        allow_reuse_tables = 1 if data.get("allow_reuse_tables") in (True, 1, "TRUE", "true", "1") else 0
        try:
            conn.execute(
                """UPDATE source_pages SET country_id=?, state_id=?, level_id=?, branch_id=?, url=?, notes=?, enabled=?, allow_reuse_tables=?, updated_at=datetime('now') WHERE id=?""",
                (
                    country_id,
                    int(data.get("state_id") or 0) or None,
                    int(data.get("level_id") or 0) or None,
                    int(data.get("branch_id") or 0) or None,
                    url,
                    data.get("notes") or "",
                    enabled,
                    allow_reuse_tables,
                    source_page_id,
                ),
            )
        except sqlite3.OperationalError:
            conn.execute(
                """UPDATE source_pages SET country_id=?, state_id=?, level_id=?, branch_id=?, url=?, notes=?, enabled=?, updated_at=datetime('now') WHERE id=?""",
                (
                    country_id,
                    int(data.get("state_id") or 0) or None,
                    int(data.get("level_id") or 0) or None,
                    int(data.get("branch_id") or 0) or None,
                    url,
                    data.get("notes") or "",
                    enabled,
                    source_page_id,
                ),
            )
        conn.commit()
        return True
    finally:
        if own_conn:
            conn.close()


def get_page_export(source_page_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return full hierarchy for export: page (all source_pages cols), offices (each with office_details, alt_links, tables).
    Uses SELECT * so any new columns are included. Returns None if not hierarchy or page not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return None
        row = conn.execute("SELECT * FROM source_pages WHERE id = ?", (source_page_id,)).fetchone()
        if not row:
            return None
        page_dict = _row_to_dict(row)
        offices_list: list[dict[str, Any]] = []
        for od_row in conn.execute(
            "SELECT * FROM office_details WHERE source_page_id = ? ORDER BY id", (source_page_id,)
        ).fetchall():
            od_dict = _row_to_dict(od_row)
            od_id = od_dict.get("id")
            alt_links_rows: list[dict[str, Any]] = []
            try:
                for al_row in conn.execute(
                    "SELECT * FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)
                ).fetchall():
                    alt_links_rows.append(_row_to_dict(al_row))
            except sqlite3.OperationalError:
                pass
            tables_rows: list[dict[str, Any]] = []
            for tc_row in conn.execute(
                "SELECT * FROM office_table_config WHERE office_details_id = ? ORDER BY table_no, id", (od_id,)
            ).fetchall():
                tables_rows.append(_row_to_dict(tc_row))
            offices_list.append({"office": od_dict, "alt_links": alt_links_rows, "tables": tables_rows})
        return {"page": page_dict, "offices": offices_list}
    finally:
        if own_conn:
            conn.close()


def get_full_export(conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Return full hierarchy for all pages: pages (each with page row, offices with alt_links and tables).
    Uses SELECT * so any new columns are included. Returns {\"pages\": []} when not hierarchy."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return {"pages": []}
        pages_list: list[dict[str, Any]] = []
        for page_row in conn.execute("SELECT * FROM source_pages ORDER BY id").fetchall():
            page_dict = _row_to_dict(page_row)
            source_page_id = page_dict.get("id")
            offices_list: list[dict[str, Any]] = []
            for od_row in conn.execute(
                "SELECT * FROM office_details WHERE source_page_id = ? ORDER BY id", (source_page_id,)
            ).fetchall():
                od_dict = _row_to_dict(od_row)
                od_id = od_dict.get("id")
                alt_links_rows: list[dict[str, Any]] = []
                try:
                    for al_row in conn.execute(
                        "SELECT * FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)
                    ).fetchall():
                        alt_links_rows.append(_row_to_dict(al_row))
                except sqlite3.OperationalError:
                    pass
                tables_rows: list[dict[str, Any]] = []
                for tc_row in conn.execute(
                    "SELECT * FROM office_table_config WHERE office_details_id = ? ORDER BY table_no, id", (od_id,)
                ).fetchall():
                    tables_rows.append(_row_to_dict(tc_row))
                offices_list.append({"office": od_dict, "alt_links": alt_links_rows, "tables": tables_rows})
            pages_list.append({"page": page_dict, "offices": offices_list})
        return {"pages": pages_list}
    finally:
        if own_conn:
            conn.close()


def list_offices_for_page(source_page_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all offices (flat) for a given source_page_id. Empty if not using hierarchy or page not found."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return []
        cur = conn.execute(
            """SELECT p.id AS page_id, p.country_id, p.state_id, p.level_id, p.branch_id, p.url, p.notes AS page_notes, p.enabled AS page_enabled,
                          od.id AS office_details_id, od.name, od.department, od.notes, od.alt_link_include_main, od.enabled AS od_enabled, od.office_category_id,
                          tc.id AS tc_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                          tc.term_start_column, tc.term_end_column, tc.district_column, tc.dynamic_parse, tc.read_right_to_left,
                          tc.find_date_in_infobox, tc.parse_rowspan, tc.rep_link, tc.party_link, tc.enabled AS tc_enabled,
                          tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large,
                          tc.consolidate_rowspan_terms, tc.notes AS tc_notes, tc.name AS tc_name, tc.created_at
                   FROM office_details od
                   JOIN source_pages p ON p.id = od.source_page_id
                   LEFT JOIN office_table_config tc ON tc.office_details_id = od.id
                   WHERE p.id = ?
                   ORDER BY od.id, tc.table_no, tc.id""",
            (source_page_id,),
        )
        rows = cur.fetchall()
        # Group by office_details_id (one office per group, multiple rows per office when multiple table configs)
        by_od: dict[int, list[dict]] = {}
        for r in rows:
            rd = _row_to_dict(r)
            od_id = rd["office_details_id"]
            if od_id not in by_od:
                by_od[od_id] = []
            by_od[od_id].append(rd)
        out = []
        for od_id, group in by_od.items():
            rd0 = group[0]
            alt_links = [
                row["link_path"]
                for row in conn.execute("SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id", (od_id,)).fetchall()
            ]
            c, s, lv, b = _ref_names(conn, rd0.get("country_id"), rd0.get("state_id"), rd0.get("level_id"), rd0.get("branch_id"))
            p = {"url": rd0.get("url"), "country_id": rd0.get("country_id"), "state_id": rd0.get("state_id"), "level_id": rd0.get("level_id"), "branch_id": rd0.get("branch_id"), "notes": rd0.get("page_notes"), "enabled": rd0.get("page_enabled")}
            od = {"id": od_id, "name": rd0.get("name"), "department": rd0.get("department"), "notes": rd0.get("notes"), "alt_link_include_main": rd0.get("alt_link_include_main"), "enabled": rd0.get("od_enabled"), "office_category_id": rd0.get("office_category_id")}
            table_configs = []
            for rd in group:
                if rd.get("tc_id") is not None:
                    table_configs.append(_tc_row_to_config(rd))
            table_configs.sort(key=lambda x: (x.get("table_no") or 0, x.get("id") or 0))
            first_tc = table_configs[0] if table_configs else {}
            tc_flat = {"table_no": first_tc.get("table_no"), "table_rows": first_tc.get("table_rows"), "link_column": first_tc.get("link_column"), "party_column": first_tc.get("party_column"), "term_start_column": first_tc.get("term_start_column"), "term_end_column": first_tc.get("term_end_column"), "district_column": first_tc.get("district_column"), "dynamic_parse": first_tc.get("dynamic_parse"), "read_right_to_left": first_tc.get("read_right_to_left"), "find_date_in_infobox": first_tc.get("find_date_in_infobox"), "parse_rowspan": first_tc.get("parse_rowspan"), "rep_link": first_tc.get("rep_link"), "party_link": first_tc.get("party_link"), "enabled": first_tc.get("enabled"), "use_full_page_for_table": first_tc.get("use_full_page_for_table"), "years_only": first_tc.get("years_only"), "term_dates_merged": first_tc.get("term_dates_merged"), "party_ignore": first_tc.get("party_ignore"), "district_ignore": first_tc.get("district_ignore"), "district_at_large": first_tc.get("district_at_large"), "consolidate_rowspan_terms": first_tc.get("consolidate_rowspan_terms"), "notes": first_tc.get("notes"), "created_at": first_tc.get("created_at")}
            flat = _flatten_hierarchy_row(p, od, tc_flat, c, s, lv, b, alt_links)
            flat["id"] = od_id
            flat["source_page_id"] = rd0.get("page_id")
            flat["table_configs"] = table_configs
            flat["office_category_id"] = rd0.get("office_category_id")
            out.append(flat)
        return out
    finally:
        if own_conn:
            conn.close()


def _insert_one_table_config(
    conn: sqlite3.Connection, od_id: int, tc: dict[str, Any], enabled: int
) -> None:
    """Insert one office_table_config row from tc dict."""
    t_merged = _bool(tc, "term_dates_merged")
    conn.execute(
        """INSERT INTO office_table_config (office_details_id, table_no, table_rows, link_column, party_column,
              term_start_column, term_end_column, district_column, dynamic_parse, read_right_to_left, find_date_in_infobox,
              parse_rowspan, rep_link, party_link, enabled, use_full_page_for_table, years_only,
              term_dates_merged, party_ignore, district_ignore, district_at_large, consolidate_rowspan_terms, notes, name, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
        (
            od_id,
            int(tc.get("table_no", 1)),
            int(tc.get("table_rows", 4)),
            int(tc.get("link_column", 1)),
            int(tc.get("party_column", 0)),
            int(tc.get("term_start_column", 4)),
            int(tc.get("term_end_column", 5)),
            int(tc.get("district_column", 0)),
            1 if tc.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("enabled") in (True, 1, "TRUE", "true", "1") else enabled,
            1 if tc.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
            1 if t_merged else 0,
            1 if tc.get("party_ignore") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("district_ignore") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("district_at_large") in (True, 1, "TRUE", "true", "1") else 0,
            1 if tc.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
            tc.get("notes") or "",
            tc.get("name") or "",
        ),
    )


def create_office_for_page(source_page_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Add a new office (and its table config(s)) to an existing page. Returns new office_details id.
    If data has table_configs, creates multiple configs; else one from data."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, country_id, state_id, level_id, branch_id, url, notes, enabled FROM source_pages WHERE id = ?",
            (source_page_id,),
        ).fetchone()
        if not row:
            raise ValueError("Page not found")
        row_data = dict(data)
        table_configs = data.get("table_configs")
        if table_configs is None or len(table_configs) == 0:
            term_dates_merged = _bool(data, "term_dates_merged")
            party_ignore = _bool(data, "party_ignore")
            district_ignore = _bool(data, "district_ignore")
            district_at_large = _bool(data, "district_at_large")
            if term_dates_merged:
                row_data["term_end_column"] = row_data.get("term_start_column", 4)
            validate_office_table_config(
                row_data,
                term_dates_merged=term_dates_merged,
                party_ignore=party_ignore,
                district_ignore=district_ignore,
                district_at_large=district_at_large,
            )
            table_configs = [row_data]
        else:
            for tc in table_configs:
                t_merged = _bool(tc, "term_dates_merged")
                tcd = dict(tc)
                if t_merged:
                    tcd["term_end_column"] = tc.get("term_start_column", 4)
                validate_office_table_config(
                    tcd,
                    term_dates_merged=t_merged,
                    party_ignore=_bool(tc, "party_ignore"),
                    district_ignore=_bool(tc, "district_ignore"),
                    district_at_large=_bool(tc, "district_at_large"),
                )
            table_nos = [int(tc.get("table_no") or 1) for tc in table_configs]
            if len(table_nos) != len(set(table_nos)):
                raise ValueError("Duplicate table_no within office")
            page_data = get_page(source_page_id, conn)
            if page_data and page_data.get("allow_reuse_tables"):
                other_nos = _table_nos_on_page(conn, source_page_id)
                if set(table_nos) & other_nos:
                    raise ValueError(
                        "Table numbers must be unique per page when 'Allow reuse of tables' is checked"
                    )
        enabled = 1 if row_data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        _ocid = row_data.get("office_category_id")
        if _ocid is not None and _ocid != "":
            try:
                _ocid = int(_ocid) if _ocid else None
            except (TypeError, ValueError):
                _ocid = None
        else:
            _ocid = None
        conn.execute(
            """INSERT INTO office_details (source_page_id, name, variant_name, department, notes, alt_link_include_main, enabled, office_category_id, created_at, updated_at)
               VALUES (?, ?, '', ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                source_page_id,
                (row_data.get("name") or "New office").strip(),
                row_data.get("department") or "",
                row_data.get("notes") or "",
                1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                enabled,
                _ocid,
            ),
        )
        od_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for tc in table_configs:
            _insert_one_table_config(conn, od_id, tc, enabled)
        conn.commit()
        set_alt_links_for_office(od_id, row_data.get("alt_links") or [], conn=conn)
        return od_id
    finally:
        if own_conn:
            conn.close()


def create_office(data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Insert office and return new id (office_details_id in hierarchy). Creates source_page + office_details + office_table_config(s).
    If data has table_configs, creates multiple configs; else one from data."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        country_id = int(data.get("country_id") or 0)
        if not country_id:
            raise ValueError("country_id required")
        row_data = dict(data)
        table_configs = data.get("table_configs")
        if table_configs is None or len(table_configs) == 0:
            term_dates_merged = _bool(data, "term_dates_merged")
            party_ignore = _bool(data, "party_ignore")
            district_ignore = _bool(data, "district_ignore")
            district_at_large = _bool(data, "district_at_large")
            if term_dates_merged:
                row_data["term_end_column"] = row_data.get("term_start_column", 4)
            validate_office_table_config(
                row_data,
                term_dates_merged=term_dates_merged,
                party_ignore=party_ignore,
                district_ignore=district_ignore,
                district_at_large=district_at_large,
            )
            table_configs = [row_data]
        else:
            for tc in table_configs:
                t_merged = _bool(tc, "term_dates_merged")
                tcd = dict(tc)
                if t_merged:
                    tcd["term_end_column"] = tc.get("term_start_column", 4)
                validate_office_table_config(
                    tcd,
                    term_dates_merged=t_merged,
                    party_ignore=_bool(tc, "party_ignore"),
                    district_ignore=_bool(tc, "district_ignore"),
                    district_at_large=_bool(tc, "district_at_large"),
                )
            table_nos = [int(tc.get("table_no") or 1) for tc in table_configs]
            if len(table_nos) != len(set(table_nos)):
                raise ValueError("Duplicate table_no within office")
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
        _ocid = data.get("office_category_id")
        if _ocid is not None and _ocid != "":
            try:
                _ocid = int(_ocid) if _ocid else None
            except (TypeError, ValueError):
                _ocid = None
        else:
            _ocid = None
        conn.execute(
            """INSERT INTO office_details (source_page_id, name, variant_name, department, notes, alt_link_include_main, enabled, office_category_id, created_at, updated_at)
               VALUES (?, ?, '', ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                page_id,
                (row_data.get("name") or "").strip(),
                row_data.get("department") or "",
                row_data.get("notes") or "",
                1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                enabled,
                _ocid,
            ),
        )
        od_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for tc in table_configs:
            _insert_one_table_config(conn, od_id, tc, enabled)
        conn.commit()
        set_alt_links_for_office(od_id, row_data.get("alt_links") or [], conn=conn)
        return od_id
    finally:
        if own_conn:
            conn.close()


def update_office(office_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None, *, office_only: bool = False) -> bool:
    """Update office by id (office_details_id in hierarchy). Updates source_page (unless office_only), office_details, office_table_config."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        row_data = dict(data)
        row_data.pop("office_only", None)
        term_dates_merged = _bool(data, "term_dates_merged")
        party_ignore = _bool(data, "party_ignore")
        district_ignore = _bool(data, "district_ignore")
        district_at_large = _bool(data, "district_at_large")
        if term_dates_merged:
            row_data["term_end_column"] = row_data.get("term_start_column", 4)
        enabled_val = 1 if data.get("enabled") in (True, 1, "TRUE", "true", "1") else 0
        if not office_only:
            country_id = int(data.get("country_id") or 0)
            if not country_id:
                raise ValueError("country_id required")
        if _use_hierarchy(conn):
            row = conn.execute("SELECT source_page_id FROM office_details WHERE id = ?", (office_id,)).fetchone()
            if not row:
                return False
            page_id = row["source_page_id"]
            if not office_only:
                conn.execute(
                    """UPDATE source_pages SET country_id=?, state_id=?, level_id=?, branch_id=?, url=?, notes=?, enabled=?, updated_at=datetime('now') WHERE id=?""",
                    (
                        int(row_data.get("country_id") or 0),
                        int(row_data.get("state_id") or 0) or None,
                        int(row_data.get("level_id") or 0) or None,
                        int(row_data.get("branch_id") or 0) or None,
                        (row_data.get("url") or "").strip(),
                        row_data.get("notes") or "",
                        enabled_val,
                        page_id,
                    ),
                )
            _ocid = row_data.get("office_category_id")
            if _ocid is not None and _ocid != "":
                try:
                    _ocid = int(_ocid) if _ocid else None
                except (TypeError, ValueError):
                    _ocid = None
            else:
                _ocid = None
            conn.execute(
                """UPDATE office_details SET name=?, department=?, notes=?, alt_link_include_main=?, enabled=?, office_category_id=?, updated_at=datetime('now') WHERE id=?""",
                (
                    (row_data.get("name") or "").strip(),
                    row_data.get("department") or "",
                    row_data.get("notes") or "",
                    1 if row_data.get("alt_link_include_main") in (True, 1, "TRUE", "true", "1") else 0,
                    enabled_val,
                    _ocid,
                    office_id,
                ),
            )
            table_configs = data.get("table_configs")
            if table_configs is None:
                # Backward compat: build single table config from flat fields
                one = {
                    "table_no": row_data.get("table_no", 1),
                    "table_rows": row_data.get("table_rows", 4),
                    "link_column": row_data.get("link_column", 1),
                    "party_column": row_data.get("party_column", 0),
                    "term_start_column": row_data.get("term_start_column", 4),
                    "term_end_column": row_data.get("term_end_column", 5),
                    "district_column": row_data.get("district_column", 0),
                    "dynamic_parse": row_data.get("dynamic_parse"),
                    "read_right_to_left": row_data.get("read_right_to_left"),
                    "find_date_in_infobox": row_data.get("find_date_in_infobox"),
                    "parse_rowspan": row_data.get("parse_rowspan"),
                    "rep_link": row_data.get("rep_link"),
                    "party_link": row_data.get("party_link"),
                    "enabled": data.get("enabled"),
                    "use_full_page_for_table": row_data.get("use_full_page_for_table"),
                    "years_only": row_data.get("years_only"),
                    "term_dates_merged": row_data.get("term_dates_merged"),
                    "party_ignore": row_data.get("party_ignore"),
                    "district_ignore": row_data.get("district_ignore"),
                    "district_at_large": row_data.get("district_at_large"),
                    "consolidate_rowspan_terms": row_data.get("consolidate_rowspan_terms"),
                    "notes": row_data.get("notes"),
                }
                existing_tc = conn.execute(
                    "SELECT id FROM office_table_config WHERE office_details_id = ?", (office_id,)
                ).fetchall()
                if len(existing_tc) == 1:
                    one["id"] = existing_tc[0][0]
                table_configs = [one]
            if table_configs is not None:
                if not table_configs:
                    raise ValueError("Office must have at least one table config")
                for tc in table_configs:
                    t_merged = _bool(tc, "term_dates_merged")
                    p_ignore = _bool(tc, "party_ignore")
                    d_ignore = _bool(tc, "district_ignore")
                    d_large = _bool(tc, "district_at_large")
                    tcd = dict(tc)
                    if t_merged:
                        tcd["term_end_column"] = tc.get("term_start_column", 4)
                    validate_office_table_config(
                        tcd,
                        term_dates_merged=t_merged,
                        party_ignore=p_ignore,
                        district_ignore=d_ignore,
                        district_at_large=d_large,
                    )
                table_nos = [int(tc.get("table_no") or 1) for tc in table_configs]
                if len(table_nos) != len(set(table_nos)):
                    raise ValueError("Duplicate table_no within office")
                page_data = get_page(page_id, conn)
                if page_data and page_data.get("allow_reuse_tables"):
                    other_nos = _table_nos_on_page(conn, page_id, exclude_office_details_id=office_id)
                    if set(table_nos) & other_nos:
                        raise ValueError(
                            "Table numbers must be unique per page when 'Allow reuse of tables' is checked"
                        )
                existing_ids = {
                    r[0]
                    for r in conn.execute(
                        "SELECT id FROM office_table_config WHERE office_details_id = ?", (office_id,)
                    ).fetchall()
                }
                kept_ids = []
                for tc in table_configs:
                    tc_id = tc.get("id")
                    if tc_id is not None and int(tc_id) in existing_ids:
                        tc_id = int(tc_id)
                        t_merged = _bool(tc, "term_dates_merged")
                        conn.execute(
                            """UPDATE office_table_config SET table_no=?, table_rows=?, link_column=?, party_column=?,
                                  term_start_column=?, term_end_column=?, district_column=?, dynamic_parse=?, read_right_to_left=?,
                                  find_date_in_infobox=?, parse_rowspan=?, rep_link=?, party_link=?, enabled=?, use_full_page_for_table=?,
                                  years_only=?, term_dates_merged=?, party_ignore=?, district_ignore=?, district_at_large=?,
                                  consolidate_rowspan_terms=?, notes=?, name=?, updated_at=datetime('now') WHERE id=?""",
                            (
                                int(tc.get("table_no", 1)),
                                int(tc.get("table_rows", 4)),
                                int(tc.get("link_column", 1)),
                                int(tc.get("party_column", 0)),
                                int(tc.get("term_start_column", 4)),
                                int(tc.get("term_end_column", 5)),
                                int(tc.get("district_column", 0)),
                                1 if tc.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("enabled") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if t_merged else 0,
                                1 if tc.get("party_ignore") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("district_ignore") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("district_at_large") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                                tc.get("notes") or "",
                                tc.get("name") or "",
                                tc_id,
                            ),
                        )
                        kept_ids.append(tc_id)
                    else:
                        t_merged = _bool(tc, "term_dates_merged")
                        conn.execute(
                            """INSERT INTO office_table_config (office_details_id, table_no, table_rows, link_column, party_column,
                                  term_start_column, term_end_column, district_column, dynamic_parse, read_right_to_left, find_date_in_infobox,
                                  parse_rowspan, rep_link, party_link, enabled, use_full_page_for_table, years_only,
                                  term_dates_merged, party_ignore, district_ignore, district_at_large, consolidate_rowspan_terms, notes, name, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
                            (
                                office_id,
                                int(tc.get("table_no", 1)),
                                int(tc.get("table_rows", 4)),
                                int(tc.get("link_column", 1)),
                                int(tc.get("party_column", 0)),
                                int(tc.get("term_start_column", 4)),
                                int(tc.get("term_end_column", 5)),
                                int(tc.get("district_column", 0)),
                                1 if tc.get("dynamic_parse") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("read_right_to_left") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("find_date_in_infobox") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("parse_rowspan") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("rep_link") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("party_link") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("enabled") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("use_full_page_for_table") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("years_only") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if t_merged else 0,
                                1 if tc.get("party_ignore") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("district_ignore") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("district_at_large") in (True, 1, "TRUE", "true", "1") else 0,
                                1 if tc.get("consolidate_rowspan_terms") in (True, 1, "TRUE", "true", "1") else 0,
                                tc.get("notes") or "",
                                tc.get("name") or "",
                            ),
                        )
                        kept_ids.append(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                if kept_ids:
                    placeholders = ",".join("?" * len(kept_ids))
                    conn.execute(
                        f"DELETE FROM office_table_config WHERE office_details_id = ? AND id NOT IN ({placeholders})",
                        (office_id, *kept_ids),
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


def delete_table(office_table_config_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete one office_table_config and its office_terms. Fails if it would leave the office with zero configs."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return False
        row = conn.execute(
            "SELECT office_details_id FROM office_table_config WHERE id = ?", (office_table_config_id,)
        ).fetchone()
        if not row:
            return False
        od_id = row[0]
        count = conn.execute(
            "SELECT COUNT(*) FROM office_table_config WHERE office_details_id = ?", (od_id,)
        ).fetchone()[0]
        if count <= 1:
            raise ValueError("Office must have at least one table config")
        conn.execute("DELETE FROM office_terms WHERE office_table_config_id = ?", (office_table_config_id,))
        conn.execute("DELETE FROM office_table_config WHERE id = ?", (office_table_config_id,))
        conn.commit()
        return True
    finally:
        if own_conn:
            conn.close()


def move_table(
    tc_id: int,
    to_office_details_id: int,
    delete_source_office_if_empty: bool = False,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Move a table config to another office on the same page. Returns to_office_details_id on success.
    If source office has only one table and delete_source_office_if_empty is False, raises ValueError
    with message 'OFFICE_WOULD_BE_EMPTY:Office Name' so the route can return 409."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            raise ValueError("Hierarchy required")
        row = conn.execute(
            "SELECT office_details_id FROM office_table_config WHERE id = ?", (tc_id,)
        ).fetchone()
        if not row:
            raise ValueError("Table config not found")
        source_od_id = row[0]
        if source_od_id == to_office_details_id:
            raise ValueError("Table is already in that office")
        src_page = conn.execute(
            "SELECT source_page_id FROM office_details WHERE id = ?", (source_od_id,)
        ).fetchone()
        tgt_page = conn.execute(
            "SELECT source_page_id FROM office_details WHERE id = ?", (to_office_details_id,)
        ).fetchone()
        if not src_page or not tgt_page or src_page[0] != tgt_page[0]:
            raise ValueError("Source and target office must be on the same page")
        count = conn.execute(
            "SELECT COUNT(*) FROM office_table_config WHERE office_details_id = ?", (source_od_id,)
        ).fetchone()[0]
        if count == 1 and not delete_source_office_if_empty:
            name_row = conn.execute(
                "SELECT name FROM office_details WHERE id = ?", (source_od_id,)
            ).fetchone()
            source_name = (name_row[0] or "Office").strip() if name_row else "Office"
            raise ValueError(f"OFFICE_WOULD_BE_EMPTY:{source_name}")
        try:
            conn.execute(
                "UPDATE office_table_config SET office_details_id = ?, updated_at = datetime('now') WHERE id = ?",
                (to_office_details_id, tc_id),
            )
        except sqlite3.IntegrityError:
            raise ValueError(
                "Target office already has a table with that table number. Change one of the table numbers first."
            )
        conn.commit()
        if count == 1 and delete_source_office_if_empty:
            delete_office(source_od_id, conn=conn)
        return to_office_details_id
    finally:
        if own_conn:
            conn.close()


def delete_page(source_page_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete page and all its offices (each office's table configs, alt_links, office_terms, then office_details, then source_pages row)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if not _use_hierarchy(conn):
            return False
        office_ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM office_details WHERE source_page_id = ?", (source_page_id,)
            ).fetchall()
        ]
        for od_id in office_ids:
            conn.execute("DELETE FROM office_table_config WHERE office_details_id = ?", (od_id,))
            conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (od_id,))
            conn.execute("DELETE FROM office_terms WHERE office_details_id = ?", (od_id,))
            conn.execute("DELETE FROM office_details WHERE id = ?", (od_id,))
        conn.execute("DELETE FROM source_pages WHERE id = ?", (source_page_id,))
        conn.commit()
        return True
    finally:
        if own_conn:
            conn.close()


def delete_office(office_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete office by id (office_details_id in hierarchy: table_configs, alt_links, terms, office_details). Does not delete source_pages."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _use_hierarchy(conn):
            row = conn.execute("SELECT id FROM office_details WHERE id = ?", (office_id,)).fetchone()
            if not row:
                return False
            conn.execute("DELETE FROM office_table_config WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM office_terms WHERE office_details_id = ?", (office_id,))
            conn.execute("DELETE FROM office_details WHERE id = ?", (office_id,))
            conn.commit()
            return True
        conn.execute("DELETE FROM alt_links WHERE office_id = ?", (office_id,))
        cur = conn.execute("DELETE FROM offices WHERE id = ?", (office_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def deduplicate_source_pages_by_url(conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Deduplicate source_pages by URL: for each duplicate URL, keep the row with smallest id,
    relink all office_details from other rows to that one, then disable the duplicate rows.
    Returns {"relinked": [(duplicate_id, kept_id, office_count), ...], "disabled": [id, ...], "errors": []}."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    relinked: list[tuple[int, int, int]] = []
    disabled: list[int] = []
    errors: list[str] = []
    try:
        if not _use_hierarchy(conn):
            return {"relinked": [], "disabled": [], "errors": ["Hierarchy (source_pages) not in use."]}
        rows = conn.execute("SELECT id, url FROM source_pages").fetchall()
        by_url: dict[str, list[tuple[int, str]]] = {}
        for r in rows:
            url = (r["url"] or "").strip()
            if not url:
                continue
            if url not in by_url:
                by_url[url] = []
            by_url[url].append((r["id"], url))
        for url, id_list in by_url.items():
            if len(id_list) <= 1:
                continue
            id_list.sort(key=lambda x: x[0])
            kept_id = id_list[0][0]
            duplicate_ids = [x[0] for x in id_list[1:]]
            for dup_id in duplicate_ids:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM office_details WHERE source_page_id = ?", (dup_id,)
                )
                count = cur.fetchone()[0]
                conn.execute(
                    "UPDATE office_details SET source_page_id = ?, updated_at = datetime('now') WHERE source_page_id = ?",
                    (kept_id, dup_id),
                )
                relinked.append((dup_id, kept_id, count))
                conn.execute(
                    "UPDATE source_pages SET enabled = 0, updated_at = datetime('now') WHERE id = ?",
                    (dup_id,),
                )
                disabled.append(dup_id)
        conn.commit()
    except Exception as e:
        if own_conn:
            conn.rollback()
        errors.append(str(e))
    finally:
        if own_conn:
            conn.close()
    return {"relinked": relinked, "disabled": list(disabled), "errors": errors}


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
