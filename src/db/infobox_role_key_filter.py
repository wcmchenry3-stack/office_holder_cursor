"""Infobox role key filters, scoped by country/level/branch."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


def list_infobox_role_key_filters(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return all infobox role key filters."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name, role_key FROM infobox_role_key_filter ORDER BY name")
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_infobox_role_key_filter(filter_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return filter dict with scope ids. None if not found."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, name, role_key FROM infobox_role_key_filter WHERE id = ?", (filter_id,)
        ).fetchone()
        if not row:
            return None
        d = _row_to_dict(row)
        d["country_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT country_id FROM infobox_role_key_filter_countries WHERE filter_id = ?", (filter_id,)
            ).fetchall()
        ]
        d["level_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT level_id FROM infobox_role_key_filter_levels WHERE filter_id = ?", (filter_id,)
            ).fetchall()
        ]
        d["branch_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT branch_id FROM infobox_role_key_filter_branches WHERE filter_id = ?", (filter_id,)
            ).fetchall()
        ]
        return d
    finally:
        if own:
            conn.close()


def create_infobox_role_key_filter(
    name: str,
    role_key: str,
    country_ids: list[int],
    level_ids: list[int],
    branch_ids: list[int],
    conn: sqlite3.Connection | None = None,
) -> int:
    """Insert filter and scope rows. Empty scope list = all."""
    name = (name or "").strip()
    role_key = (role_key or "").strip()
    if not name:
        raise ValueError("Filter name is required")
    if not role_key:
        raise ValueError("Role key is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO infobox_role_key_filter (name, role_key) VALUES (?, ?)", (name, role_key))
        filter_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for cid in country_ids:
            if cid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_countries (filter_id, country_id) VALUES (?, ?)",
                    (filter_id, cid),
                )
        for lid in level_ids:
            if lid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_levels (filter_id, level_id) VALUES (?, ?)",
                    (filter_id, lid),
                )
        for bid in branch_ids:
            if bid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_branches (filter_id, branch_id) VALUES (?, ?)",
                    (filter_id, bid),
                )
        conn.commit()
        return filter_id
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A filter with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def update_infobox_role_key_filter(
    filter_id: int,
    name: str,
    role_key: str,
    country_ids: list[int],
    level_ids: list[int],
    branch_ids: list[int],
    conn: sqlite3.Connection | None = None,
) -> bool:
    """Update filter and replace scope rows."""
    name = (name or "").strip()
    role_key = (role_key or "").strip()
    if not name:
        raise ValueError("Filter name is required")
    if not role_key:
        raise ValueError("Role key is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE infobox_role_key_filter SET name = ?, role_key = ? WHERE id = ?",
            (name, role_key, filter_id),
        )
        if cur.rowcount == 0:
            return False
        conn.execute("DELETE FROM infobox_role_key_filter_countries WHERE filter_id = ?", (filter_id,))
        conn.execute("DELETE FROM infobox_role_key_filter_levels WHERE filter_id = ?", (filter_id,))
        conn.execute("DELETE FROM infobox_role_key_filter_branches WHERE filter_id = ?", (filter_id,))
        for cid in country_ids:
            if cid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_countries (filter_id, country_id) VALUES (?, ?)",
                    (filter_id, cid),
                )
        for lid in level_ids:
            if lid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_levels (filter_id, level_id) VALUES (?, ?)",
                    (filter_id, lid),
                )
        for bid in branch_ids:
            if bid:
                conn.execute(
                    "INSERT INTO infobox_role_key_filter_branches (filter_id, branch_id) VALUES (?, ?)",
                    (filter_id, bid),
                )
        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A filter with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def delete_infobox_role_key_filter(filter_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete filter and scope rows."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("DELETE FROM infobox_role_key_filter_countries WHERE filter_id = ?", (filter_id,))
        conn.execute("DELETE FROM infobox_role_key_filter_levels WHERE filter_id = ?", (filter_id,))
        conn.execute("DELETE FROM infobox_role_key_filter_branches WHERE filter_id = ?", (filter_id,))
        conn.execute("DELETE FROM infobox_role_key_filter WHERE id = ?", (filter_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def list_filters_for_context(
    country_id: int | None,
    level_id: int | None,
    branch_id: int | None,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return filters valid for this context. Empty scoped list means all."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        where = """
        (NOT EXISTS (SELECT 1 FROM infobox_role_key_filter_countries c WHERE c.filter_id = f.id)
         OR (? IS NOT NULL AND EXISTS (SELECT 1 FROM infobox_role_key_filter_countries c WHERE c.filter_id = f.id AND c.country_id = ?)))
        AND (NOT EXISTS (SELECT 1 FROM infobox_role_key_filter_levels l WHERE l.filter_id = f.id)
         OR (? IS NOT NULL AND EXISTS (SELECT 1 FROM infobox_role_key_filter_levels l WHERE l.filter_id = f.id AND l.level_id = ?)))
        AND (NOT EXISTS (SELECT 1 FROM infobox_role_key_filter_branches b WHERE b.filter_id = f.id)
         OR (? IS NOT NULL AND EXISTS (SELECT 1 FROM infobox_role_key_filter_branches b WHERE b.filter_id = f.id AND b.branch_id = ?)))
        """
        params: list[Any] = [country_id, country_id, level_id, level_id, branch_id, branch_id]
        cur = conn.execute(
            f"SELECT f.id, f.name, f.role_key FROM infobox_role_key_filter f WHERE {where} ORDER BY f.name",
            params,
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()
