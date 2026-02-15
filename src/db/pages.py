"""
Page → Office → Table hierarchy: CRUD for pages, office_details, office_table_configs,
and config provider that returns runnable units for the new model.
"""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict
from . import offices as db_offices
from . import refs as db_refs


def list_pages(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all pages with joined country, state, level, branch names."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT p.id, p.url, p.country_id, p.state_id, p.level_id, p.branch_id,
                      p.enabled, p.table_reuse_across_offices, p.last_scraped_at, p.created_at, p.updated_at,
                      c.name AS country_name, s.name AS state_name, l.name AS level_name, b.name AS branch_name
               FROM pages p
               LEFT JOIN countries c ON c.id = p.country_id
               LEFT JOIN states s ON s.id = p.state_id
               LEFT JOIN levels l ON l.id = p.level_id
               LEFT JOIN branches b ON b.id = p.branch_id
               ORDER BY p.id"""
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_page(page_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one page by id with joined names."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT p.*, c.name AS country_name, s.name AS state_name, l.name AS level_name, b.name AS branch_name
               FROM pages p
               LEFT JOIN countries c ON c.id = p.country_id
               LEFT JOIN states s ON s.id = p.state_id
               LEFT JOIN levels l ON l.id = p.level_id
               LEFT JOIN branches b ON b.id = p.branch_id
               WHERE p.id = ?""",
            (page_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def create_page(
    url: str,
    country_id: int,
    state_id: int | None = None,
    level_id: int | None = None,
    branch_id: int | None = None,
    enabled: bool = True,
    table_reuse_across_offices: bool = False,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Insert a page and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO pages (url, country_id, state_id, level_id, branch_id, enabled, table_reuse_across_offices)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (url.strip(), country_id, state_id, level_id, branch_id, 1 if enabled else 0, 1 if table_reuse_across_offices else 0),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def update_page(
    page_id: int,
    *,
    url: str | None = None,
    country_id: int | None = None,
    state_id: int | None = None,
    level_id: int | None = None,
    branch_id: int | None = None,
    enabled: bool | None = None,
    table_reuse_across_offices: bool | None = None,
    conn: sqlite3.Connection | None = None,
) -> bool:
    """Update a page. Returns True if a row was updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        updates = []
        params = []
        if url is not None:
            updates.append("url = ?")
            params.append(url.strip())
        if country_id is not None:
            updates.append("country_id = ?")
            params.append(country_id)
        if state_id is not None:
            updates.append("state_id = ?")
            params.append(state_id)
        if level_id is not None:
            updates.append("level_id = ?")
            params.append(level_id)
        if branch_id is not None:
            updates.append("branch_id = ?")
            params.append(branch_id)
        if enabled is not None:
            updates.append("enabled = ?")
            params.append(1 if enabled else 0)
        if table_reuse_across_offices is not None:
            updates.append("table_reuse_across_offices = ?")
            params.append(1 if table_reuse_across_offices else 0)
        if not updates:
            return False
        updates.append("updated_at = datetime('now')")
        params.append(page_id)
        cur = conn.execute(f"UPDATE pages SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def delete_page(page_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete a page and its office_details and office_table_configs (cascade). Returns True if deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        # Get office_details ids for this page
        cur = conn.execute("SELECT id FROM office_details WHERE source_page_id = ?", (page_id,))
        od_ids = [row[0] for row in cur.fetchall()]
        for od_id in od_ids:
            conn.execute("DELETE FROM office_table_configs WHERE office_details_id = ?", (od_id,))
            conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (od_id,))
        conn.execute("DELETE FROM office_details WHERE source_page_id = ?", (page_id,))
        cur = conn.execute("DELETE FROM pages WHERE id = ?", (page_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def list_office_details_for_page(page_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return office_details for a page, ordered by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM office_details WHERE source_page_id = ? ORDER BY id",
            (page_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_office_detail(office_details_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one office_detail by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM office_details WHERE id = ?", (office_details_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def create_office_detail(
    source_page_id: int,
    name: str,
    enabled: bool = True,
    notes: str | None = None,
    alt_link_include_main: bool = False,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Insert an office_detail and return its id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO office_details (source_page_id, name, enabled, notes, alt_link_include_main)
               VALUES (?, ?, ?, ?, ?)""",
            (source_page_id, name.strip(), 1 if enabled else 0, notes or "", 1 if alt_link_include_main else 0),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def update_office_detail(
    office_details_id: int,
    *,
    name: str | None = None,
    enabled: bool | None = None,
    notes: str | None = None,
    alt_link_include_main: bool | None = None,
    conn: sqlite3.Connection | None = None,
) -> bool:
    """Update an office_detail. Returns True if a row was updated."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name.strip())
        if enabled is not None:
            updates.append("enabled = ?")
            params.append(1 if enabled else 0)
        if notes is not None:
            updates.append("notes = ?")
            params.append(notes)
        if alt_link_include_main is not None:
            updates.append("alt_link_include_main = ?")
            params.append(1 if alt_link_include_main else 0)
        if not updates:
            return False
        updates.append("updated_at = datetime('now')")
        params.append(office_details_id)
        cur = conn.execute(f"UPDATE office_details SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def delete_office_detail(office_details_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete an office_detail and its table configs and alt_links. Returns True if deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute("DELETE FROM office_table_configs WHERE office_details_id = ?", (office_details_id,))
        conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (office_details_id,))
        cur = conn.execute("DELETE FROM office_details WHERE id = ?", (office_details_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def list_table_configs_for_office(office_details_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return office_table_configs for an office_detail, ordered by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT * FROM office_table_configs WHERE office_details_id = ? ORDER BY id",
            (office_details_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_table_config(config_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return one office_table_config by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM office_table_configs WHERE id = ?", (config_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def create_table_config(office_details_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    """Insert an office_table_config; validate with validate_office_table_config. Returns new id."""
    db_offices.validate_office_table_config(
        data,
        term_dates_merged=_bool(data, "term_dates_merged"),
        party_ignore=_bool(data, "party_ignore"),
        district_ignore=_bool(data, "district_ignore"),
        district_at_large=_bool(data, "district_at_large"),
    )
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO office_table_configs (
                   office_details_id, table_no, table_rows, link_column, party_column,
                   term_start_column, term_end_column, district_column,
                   dynamic_parse, read_right_to_left, find_date_in_infobox, parse_rowspan, consolidate_rowspan_terms,
                   rep_link, party_link, use_full_page_for_table, years_only,
                   term_dates_merged, party_ignore, district_ignore, district_at_large, enabled
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                office_details_id,
                int(data.get("table_no", 1)),
                int(data.get("table_rows", 4)),
                int(data.get("link_column", 1)),
                int(data.get("party_column", 0)),
                int(data.get("term_start_column", 4)),
                int(data.get("term_end_column", 5)),
                int(data.get("district_column", 0)),
                1 if data.get("dynamic_parse") in (True, 1, "1", "true") else 0,
                1 if data.get("read_right_to_left") in (True, 1, "1", "true") else 0,
                1 if data.get("find_date_in_infobox") in (True, 1, "1", "true") else 0,
                1 if data.get("parse_rowspan") in (True, 1, "1", "true") else 0,
                1 if data.get("consolidate_rowspan_terms") in (True, 1, "1", "true") else 0,
                1 if data.get("rep_link") in (True, 1, "1", "true") else 0,
                1 if data.get("party_link") in (True, 1, "1", "true") else 0,
                1 if data.get("use_full_page_for_table") in (True, 1, "1", "true") else 0,
                1 if data.get("years_only") in (True, 1, "1", "true") else 0,
                1 if data.get("term_dates_merged") in (True, 1, "1", "true") else 0,
                1 if data.get("party_ignore") in (True, 1, "1", "true") else 0,
                1 if data.get("district_ignore") in (True, 1, "1", "true") else 0,
                1 if data.get("district_at_large") in (True, 1, "1", "true") else 0,
                1 if data.get("enabled", True) in (True, 1, "1", "true") else 0,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        if own_conn:
            conn.close()


def update_table_config(config_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> bool:
    """Update an office_table_config. Returns True if updated."""
    db_offices.validate_office_table_config(
        data,
        term_dates_merged=_bool(data, "term_dates_merged"),
        party_ignore=_bool(data, "party_ignore"),
        district_ignore=_bool(data, "district_ignore"),
        district_at_large=_bool(data, "district_at_large"),
    )
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """UPDATE office_table_configs SET
                   table_no = ?, table_rows = ?, link_column = ?, party_column = ?,
                   term_start_column = ?, term_end_column = ?, district_column = ?,
                   dynamic_parse = ?, read_right_to_left = ?, find_date_in_infobox = ?, parse_rowspan = ?, consolidate_rowspan_terms = ?,
                   rep_link = ?, party_link = ?, use_full_page_for_table = ?, years_only = ?,
                   term_dates_merged = ?, party_ignore = ?, district_ignore = ?, district_at_large = ?, enabled = ?,
                   updated_at = datetime('now')
               WHERE id = ?""",
            (
                int(data.get("table_no", 1)),
                int(data.get("table_rows", 4)),
                int(data.get("link_column", 1)),
                int(data.get("party_column", 0)),
                int(data.get("term_start_column", 4)),
                int(data.get("term_end_column", 5)),
                int(data.get("district_column", 0)),
                1 if data.get("dynamic_parse") in (True, 1, "1", "true") else 0,
                1 if data.get("read_right_to_left") in (True, 1, "1", "true") else 0,
                1 if data.get("find_date_in_infobox") in (True, 1, "1", "true") else 0,
                1 if data.get("parse_rowspan") in (True, 1, "1", "true") else 0,
                1 if data.get("consolidate_rowspan_terms") in (True, 1, "1", "true") else 0,
                1 if data.get("rep_link") in (True, 1, "1", "true") else 0,
                1 if data.get("party_link") in (True, 1, "1", "true") else 0,
                1 if data.get("use_full_page_for_table") in (True, 1, "1", "true") else 0,
                1 if data.get("years_only") in (True, 1, "1", "true") else 0,
                1 if data.get("term_dates_merged") in (True, 1, "1", "true") else 0,
                1 if data.get("party_ignore") in (True, 1, "1", "true") else 0,
                1 if data.get("district_ignore") in (True, 1, "1", "true") else 0,
                1 if data.get("district_at_large") in (True, 1, "1", "true") else 0,
                1 if data.get("enabled", True) in (True, 1, "1", "true") else 0,
                config_id,
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def delete_table_config(config_id: int, conn: sqlite3.Connection | None = None) -> bool:
    """Delete an office_table_config. Returns True if deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM office_table_configs WHERE id = ?", (config_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            conn.close()


def _bool(data: dict, key: str) -> bool:
    v = data.get(key)
    return v is not None and str(v).strip().lower() in ("true", "1", "yes")


def list_alt_links_for_office_details(office_details_id: int, conn: sqlite3.Connection | None = None) -> list[str]:
    """Return list of link_path strings for the office_detail (from alt_links.office_details_id)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT link_path FROM alt_links WHERE office_details_id = ? ORDER BY id",
            (office_details_id,),
        )
        return [row["link_path"] for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def set_alt_links_for_office_details(
    office_details_id: int, paths: list[str], conn: sqlite3.Connection | None = None
) -> None:
    """Replace all alt links for the office_detail with the given list (normalized)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute("DELETE FROM alt_links WHERE office_details_id = ?", (office_details_id,))
        for raw in paths:
            path = _normalize_alt_link_path(raw)
            if path:
                conn.execute(
                    "INSERT OR IGNORE INTO alt_links (office_id, office_details_id, link_path) VALUES (NULL, ?, ?)",
                    (office_details_id, path),
                )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def _normalize_alt_link_path(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s or s.lower() in ("none", ""):
        return ""
    if s.startswith("http"):
        from urllib.parse import urlparse
        return urlparse(s).path or ""
    if not s.startswith("/"):
        return "/wiki/" + s.lstrip("/")
    return s if s.startswith("/wiki/") else "/wiki/" + s.lstrip("/")


def get_office_row_for_table_config(
    office_table_config_id: int, conn: sqlite3.Connection | None = None
) -> dict[str, Any] | None:
    """
    Build the same office_row dict the runner uses for this table config (page + office + table).
    Returns None if table config not found. Used for preview/test-config/export by id without form draft.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT p.id AS page_id, p.url, p.country_id, p.state_id, p.level_id, p.branch_id,
                      od.id AS office_details_id, od.name AS office_name, od.notes AS office_notes, od.alt_link_include_main,
                      tc.table_no, tc.table_rows, tc.link_column, tc.party_column, tc.term_start_column, tc.term_end_column,
                      tc.district_column, tc.dynamic_parse, tc.read_right_to_left, tc.find_date_in_infobox,
                      tc.parse_rowspan, tc.consolidate_rowspan_terms, tc.rep_link, tc.party_link,
                      tc.use_full_page_for_table, tc.years_only, tc.term_dates_merged, tc.party_ignore,
                      tc.district_ignore, tc.district_at_large
               FROM office_table_configs tc
               JOIN office_details od ON od.id = tc.office_details_id
               JOIN pages p ON p.id = od.source_page_id
               WHERE tc.id = ?""",
            (office_table_config_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        r = _row_to_dict(row)
        od_id = r["office_details_id"]
        alt_links = list_alt_links_for_office_details(od_id, conn=conn)
        country_id = int(r.get("country_id") or 0)
        state_id = int(r.get("state_id") or 0) or None
        level_id = int(r.get("level_id") or 0) or None
        branch_id = int(r.get("branch_id") or 0) or None
        office_row = {
            "url": (r.get("url") or "").strip(),
            "name": (r.get("office_name") or "").strip(),
            "notes": (r.get("office_notes") or "").strip(),
            "department": "",
            "table_no": r.get("table_no", 1),
            "table_rows": r.get("table_rows", 4),
            "link_column": r.get("link_column", 1),
            "party_column": r.get("party_column", 0),
            "term_start_column": r.get("term_start_column", 4),
            "term_end_column": r.get("term_end_column", 5),
            "district_column": r.get("district_column", 0),
            "dynamic_parse": bool(r.get("dynamic_parse")),
            "read_right_to_left": bool(r.get("read_right_to_left")),
            "find_date_in_infobox": bool(r.get("find_date_in_infobox")),
            "parse_rowspan": bool(r.get("parse_rowspan")),
            "consolidate_rowspan_terms": bool(r.get("consolidate_rowspan_terms")),
            "rep_link": bool(r.get("rep_link")),
            "party_link": bool(r.get("party_link")),
            "alt_links": alt_links,
            "alt_link_include_main": bool(r.get("alt_link_include_main")),
            "use_full_page_for_table": bool(r.get("use_full_page_for_table")),
            "years_only": bool(r.get("years_only")),
            "term_dates_merged": bool(r.get("term_dates_merged")),
            "party_ignore": bool(r.get("party_ignore")),
            "district_ignore": bool(r.get("district_ignore")),
            "district_at_large": bool(r.get("district_at_large")),
            "country_name": db_refs.get_country_name(country_id, conn=conn),
            "state_name": db_refs.get_state_name(state_id, conn=conn),
            "level_name": db_refs.get_level_name(level_id, conn=conn),
            "branch_name": db_refs.get_branch_name(branch_id, conn=conn),
        }
        return office_row
    finally:
        if own_conn:
            conn.close()


def get_runnable_units_new(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """
    Return runnable units for the new hierarchy: each unit has url, alt_links, table_config (scraper format),
    and identity (source_page_id, office_details_id, office_table_config_id). Only enabled pages/offices/tables.
    Each unit is a dict that can be passed to the runner like a legacy office_row, plus hierarchy ids.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT p.id AS source_page_id, p.url, p.country_id,
                      od.id AS office_details_id, od.name AS office_name, od.alt_link_include_main,
                      tc.id AS office_table_config_id, tc.table_no, tc.table_rows, tc.link_column, tc.party_column,
                      tc.term_start_column, tc.term_end_column, tc.district_column,
                      tc.dynamic_parse, tc.read_right_to_left, tc.find_date_in_infobox, tc.parse_rowspan, tc.consolidate_rowspan_terms,
                      tc.rep_link, tc.party_link, tc.use_full_page_for_table, tc.years_only,
                      tc.term_dates_merged, tc.party_ignore, tc.district_ignore, tc.district_at_large
               FROM pages p
               JOIN office_details od ON od.source_page_id = p.id AND od.enabled = 1
               JOIN office_table_configs tc ON tc.office_details_id = od.id AND tc.enabled = 1
               WHERE p.enabled = 1
               ORDER BY p.id, od.id, tc.id"""
        )
        rows = cur.fetchall()
        units = []
        for row in rows:
            r = _row_to_dict(row)
            page_id = r["source_page_id"]
            od_id = r["office_details_id"]
            tc_id = r["office_table_config_id"]
            alt_links = list_alt_links_for_office_details(od_id, conn=conn)
            # Build a row that looks like an office row for office_row_to_table_config
            office_row = {
                "url": r["url"],
                "table_no": r["table_no"],
                "table_rows": r["table_rows"],
                "link_column": r["link_column"],
                "party_column": r["party_column"],
                "term_start_column": r["term_start_column"],
                "term_end_column": r["term_end_column"],
                "district_column": r["district_column"],
                "dynamic_parse": r["dynamic_parse"],
                "read_right_to_left": r["read_right_to_left"],
                "find_date_in_infobox": r["find_date_in_infobox"],
                "parse_rowspan": r["parse_rowspan"],
                "consolidate_rowspan_terms": r["consolidate_rowspan_terms"],
                "rep_link": r["rep_link"],
                "party_link": r["party_link"],
                "alt_link_include_main": r["alt_link_include_main"],
                "use_full_page_for_table": r["use_full_page_for_table"],
                "years_only": r["years_only"],
                "term_dates_merged": r["term_dates_merged"],
                "party_ignore": r["party_ignore"],
                "district_ignore": r["district_ignore"],
                "district_at_large": r["district_at_large"],
            }
            table_config = db_offices.office_row_to_table_config(office_row, alt_links=alt_links)
            units.append({
                "url": r["url"],
                "alt_links": alt_links,
                "table_config": table_config,
                "office_row": office_row,
                "source_page_id": page_id,
                "office_details_id": od_id,
                "office_table_config_id": tc_id,
                "office_id": None,
                "id": tc_id,
                "country_id": r.get("country_id"),
            })
        return units
    finally:
        if own_conn:
            conn.close()


def get_runnable_units_legacy(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """
    Return runnable units for legacy offices (same shape as get_runnable_units_new).
    Only offices where superseded_by_office_details_id IS NULL and enabled = 1.
    """
    offices = list_offices_legacy(conn=conn)
    units = []
    for row in offices:
        if not row.get("enabled", 1):
            continue
        office_id = row["id"]
        alt_links = list_alt_links_for_office_legacy(office_id, conn=conn)
        office_row = {**row}
        table_config = db_offices.office_row_to_table_config(office_row, alt_links=alt_links)
        units.append({
            "url": row["url"],
            "alt_links": alt_links,
            "table_config": table_config,
            "office_row": office_row,
            "source_page_id": None,
            "office_details_id": None,
            "office_table_config_id": None,
            "office_id": office_id,
            "id": office_id,
        })
    return units


def list_alt_links_for_office_legacy(office_id: int, conn: sqlite3.Connection | None = None) -> list[str]:
    """Return list of link_path for legacy office (office_id)."""
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


def list_offices_legacy(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return legacy offices where superseded_by_office_details_id IS NULL (for old/hybrid provider)."""
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
               WHERE o.superseded_by_office_details_id IS NULL
               ORDER BY c.name, o.name"""
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
