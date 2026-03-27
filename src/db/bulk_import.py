"""Bulk import office configs from CSV (e.g. OfficeTables export). Resolves country/state/level/branch to FKs."""

import csv
from pathlib import Path
from typing import Any

from .connection import get_connection
from .offices import create_office
from .parties import create_party


def _resolve_refs(
    conn, country_name: str, state_name: str, level_name: str, branch_name: str
) -> tuple[int | None, int | None, int | None, int | None]:
    """Return (country_id, state_id, level_id, branch_id). None if not found."""
    country_id = state_id = level_id = branch_id = None
    if country_name:
        cur = conn.execute(
            "SELECT id FROM countries WHERE name = %s LIMIT 1", (country_name.strip(),)
        )
        row = cur.fetchone()
        country_id = row["id"] if row else None
    if state_name and country_id:
        cur = conn.execute(
            "SELECT id FROM states WHERE country_id = %s AND name = %s LIMIT 1",
            (country_id, state_name.strip()),
        )
        row = cur.fetchone()
        state_id = row["id"] if row else None
    if level_name:
        cur = conn.execute("SELECT id FROM levels WHERE name = %s LIMIT 1", (level_name.strip(),))
        row = cur.fetchone()
        level_id = row["id"] if row else None
    if branch_name:
        cur = conn.execute("SELECT id FROM branches WHERE name = %s LIMIT 1", (branch_name.strip(),))
        row = cur.fetchone()
        branch_id = row["id"] if row else None
    return (country_id, state_id, level_id, branch_id)


def _bool_from_cell(val: Any) -> int:
    """Convert CSV TRUE/FALSE or bool to 0/1 for DB."""
    if val is None or val == "":
        return 0
    s = str(val).strip().upper()
    return 1 if s in ("TRUE", "1", "YES") else 0


def _int_from_cell(val: Any, default: int = 0) -> int:
    try:
        return int(val) if val is not None and str(val).strip() != "" else default
    except (ValueError, TypeError):
        return default


def bulk_import_offices_from_csv(
    csv_path: Path | str,
    conn: Any = None,
) -> tuple[int, int]:
    """Import office rows from CSV. Returns (imported_count, error_count).
    CSV must have headers: Country, Level, Branch, Department, Name, State, URL,
    Table No, Table Rows, Link Column, Party Column, Term Start Column, Term End Column,
    District, Dynamic Parse, Read columns right to left, Find Date, Parse Rowspan, Consolidate Rowspan Terms,
    Rep Link, Party Link, Notes, Alt Link.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    imported = 0
    errors = 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    country_name = (row.get("Country") or "").strip()
                    state_name = (row.get("State") or "").strip()
                    level_name = (row.get("Level") or "").strip()
                    branch_name = (row.get("Branch") or "").strip()
                    country_id, state_id, level_id, branch_id = _resolve_refs(
                        conn, country_name, state_name, level_name, branch_name
                    )
                    if not country_id:
                        errors += 1  # require valid country
                        continue
                    data = {
                        "country_id": country_id,
                        "state_id": state_id,
                        "level_id": level_id,
                        "branch_id": branch_id,
                        "department": (row.get("Department") or "").strip(),
                        "name": (row.get("Name") or "").strip(),
                        "notes": (row.get("Notes") or "").strip(),
                        "url": (row.get("URL") or "").strip(),
                        "table_no": _int_from_cell(row.get("Table No"), 1),
                        "table_rows": _int_from_cell(row.get("Table Rows"), 4),
                        "link_column": _int_from_cell(row.get("Link Column"), 1),
                        "party_column": _int_from_cell(row.get("Party Column"), 0),
                        "term_start_column": _int_from_cell(row.get("Term Start Column"), 4),
                        "term_end_column": _int_from_cell(row.get("Term End Column"), 5),
                        "district_column": _int_from_cell(row.get("District"), 0),
                        "dynamic_parse": _bool_from_cell(row.get("Dynamic Parse")),
                        "read_right_to_left": _bool_from_cell(
                            row.get("Read columns right to left")
                        ),
                        "find_date_in_infobox": _bool_from_cell(row.get("Find Date")),
                        "years_only": _bool_from_cell(row.get("Years Only")),
                        "parse_rowspan": _bool_from_cell(row.get("Parse Rowspan")),
                        "consolidate_rowspan_terms": _bool_from_cell(
                            row.get("Consolidate Rowspan Terms")
                        ),
                        "rep_link": _bool_from_cell(row.get("Rep Link")),
                        "party_link": _bool_from_cell(row.get("Party Link")),
                        "alt_links": [
                            x.strip() for x in (row.get("Alt Link") or "").split(",") if x.strip()
                        ],
                        "alt_link_include_main": _bool_from_cell(row.get("Alt Link Include Main")),
                    }
                    if not data["url"] or not data["name"]:
                        errors += 1
                        continue
                    create_office(data, conn=conn)
                    imported += 1
                except Exception:
                    errors += 1
        return (imported, errors)
    finally:
        if own_conn:
            conn.close()


def bulk_import_parties_from_csv(
    csv_path: Path | str,
    overwrite: bool,
    conn: Any = None,
) -> tuple[int, int]:
    """Import party rows from CSV. Returns (imported_count, error_count).
    CSV must have headers: Country, Party name (or Party Name), Party link (or Party Link).
    If overwrite is True, deletes all existing parties first; otherwise appends.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    imported = 0
    errors = 0
    try:
        if overwrite:
            conn.execute("DELETE FROM parties")
            conn.commit()
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    country_name = (row.get("Country") or "").strip()
                    country_id, _, _, _ = _resolve_refs(conn, country_name, "", "", "")
                    if not country_id:
                        errors += 1
                        continue
                    party_name = (row.get("Party name") or row.get("Party Name") or "").strip()
                    party_link = (row.get("Party link") or row.get("Party Link") or "").strip()
                    if not party_name or not party_link:
                        errors += 1
                        continue
                    create_party(
                        {
                            "country_id": country_id,
                            "party_name": party_name,
                            "party_link": party_link,
                        },
                        conn=conn,
                    )
                    imported += 1
                except Exception:
                    errors += 1
        return (imported, errors)
    finally:
        if own_conn:
            conn.close()
