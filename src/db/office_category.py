"""Office category: optional label per office, scoped by country/level/branch."""

from typing import Any

from .connection import get_connection, _DB_UNIQUE_ERRORS
from .utils import _row_to_dict


def list_office_categories(conn=None) -> list[dict[str, Any]]:
    """Return all categories as list of dicts with id, name."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name FROM office_category ORDER BY name")
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_office_category(
    category_id: int, conn=None
) -> dict[str, Any] | None:
    """Return category dict with id, name, country_ids, level_ids, branch_ids. None if not found."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, name FROM office_category WHERE id = %s", (category_id,)
        ).fetchone()
        if not row:
            return None
        d = _row_to_dict(row)
        d["country_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT country_id FROM office_category_countries WHERE category_id = %s",
                (category_id,),
            ).fetchall()
        ]
        d["level_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT level_id FROM office_category_levels WHERE category_id = %s", (category_id,)
            ).fetchall()
        ]
        d["branch_ids"] = [
            r[0]
            for r in conn.execute(
                "SELECT branch_id FROM office_category_branches WHERE category_id = %s",
                (category_id,),
            ).fetchall()
        ]
        return d
    finally:
        if own:
            conn.close()


def create_office_category(
    name: str,
    country_ids: list[int],
    level_ids: list[int],
    branch_ids: list[int],
    conn=None,
) -> int:
    """Insert category and junction rows. Empty list = all for that dimension. Returns new id."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Office category name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO office_category (name) VALUES (%s) RETURNING id", (name,)
        )
        category_id = cur.fetchone()["id"]
        for cid in country_ids:
            if cid:
                conn.execute(
                    "INSERT INTO office_category_countries (category_id, country_id) VALUES (%s, %s)",
                    (category_id, cid),
                )
        for lid in level_ids:
            if lid:
                conn.execute(
                    "INSERT INTO office_category_levels (category_id, level_id) VALUES (%s, %s)",
                    (category_id, lid),
                )
        for bid in branch_ids:
            if bid:
                conn.execute(
                    "INSERT INTO office_category_branches (category_id, branch_id) VALUES (%s, %s)",
                    (category_id, bid),
                )
        conn.commit()
        return category_id
    except _DB_UNIQUE_ERRORS as e:
        if "UNIQUE" in str(e) or "duplicate key" in str(e):
            raise ValueError("An office category with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def update_office_category(
    category_id: int,
    name: str,
    country_ids: list[int],
    level_ids: list[int],
    branch_ids: list[int],
    conn=None,
) -> bool:
    """Update category name and replace junction rows. Returns True if updated."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Office category name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE office_category SET name = %s WHERE id = %s", (name, category_id))
        if cur.rowcount == 0:
            return False
        conn.execute("DELETE FROM office_category_countries WHERE category_id = %s", (category_id,))
        conn.execute("DELETE FROM office_category_levels WHERE category_id = %s", (category_id,))
        conn.execute("DELETE FROM office_category_branches WHERE category_id = %s", (category_id,))
        for cid in country_ids:
            if cid:
                conn.execute(
                    "INSERT INTO office_category_countries (category_id, country_id) VALUES (%s, %s)",
                    (category_id, cid),
                )
        for lid in level_ids:
            if lid:
                conn.execute(
                    "INSERT INTO office_category_levels (category_id, level_id) VALUES (%s, %s)",
                    (category_id, lid),
                )
        for bid in branch_ids:
            if bid:
                conn.execute(
                    "INSERT INTO office_category_branches (category_id, branch_id) VALUES (%s, %s)",
                    (category_id, bid),
                )
        conn.commit()
        return True
    except _DB_UNIQUE_ERRORS as e:
        if "UNIQUE" in str(e) or "duplicate key" in str(e):
            raise ValueError("An office category with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def delete_office_category(category_id: int, conn=None) -> None:
    """Delete category. Raises ValueError if still in use by office_details."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM office_details WHERE office_category_id = %s", (category_id,)
        ).fetchone()[0]
        if n > 0:
            raise ValueError("Cannot delete: still in use by offices")
        conn.execute("DELETE FROM office_category_countries WHERE category_id = %s", (category_id,))
        conn.execute("DELETE FROM office_category_levels WHERE category_id = %s", (category_id,))
        conn.execute("DELETE FROM office_category_branches WHERE category_id = %s", (category_id,))
        conn.execute("DELETE FROM office_category WHERE id = %s", (category_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def list_categories_for_office(
    country_id: int | None,
    level_id: int | None,
    branch_id: int | None,
    conn=None,
) -> list[dict[str, Any]]:
    """Return categories valid for this context (page country/level/branch). NULL = only categories with no rows for that dimension."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        where = """
        (NOT EXISTS (SELECT 1 FROM office_category_countries c WHERE c.category_id = oc.id)
         OR (%s IS NOT NULL AND EXISTS (SELECT 1 FROM office_category_countries c WHERE c.category_id = oc.id AND c.country_id = %s)))
        AND (NOT EXISTS (SELECT 1 FROM office_category_levels l WHERE l.category_id = oc.id)
         OR (%s IS NOT NULL AND EXISTS (SELECT 1 FROM office_category_levels l WHERE l.category_id = oc.id AND l.level_id = %s)))
        AND (NOT EXISTS (SELECT 1 FROM office_category_branches b WHERE b.category_id = oc.id)
         OR (%s IS NOT NULL AND EXISTS (SELECT 1 FROM office_category_branches b WHERE b.category_id = oc.id AND b.branch_id = %s)))
        """
        params: list[Any] = [
            country_id,
            country_id,
            level_id,
            level_id,
            branch_id,
            branch_id,
        ]
        sql = f"SELECT oc.id, oc.name FROM office_category oc WHERE {where} ORDER BY oc.name"
        cur = conn.execute(sql, params)
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()
