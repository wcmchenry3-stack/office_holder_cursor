"""Reference data: countries, states, levels, branches."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


def _count_refs(conn: sqlite3.Connection, table: str, column: str, value: int) -> int:
    """Return number of rows in table where column = value."""
    cur = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = ?", (value,))
    return cur.fetchone()[0]


def list_countries(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name FROM countries ORDER BY name")
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def list_states(country_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name FROM states WHERE country_id = ? ORDER BY name", (country_id,))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def list_levels(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name FROM levels ORDER BY name")
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def list_branches(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT id, name FROM branches ORDER BY name")
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_country_name(country_id: int, conn: sqlite3.Connection | None = None) -> str:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT name FROM countries WHERE id = ?", (country_id,))
        row = cur.fetchone()
        return row["name"] if row else ""
    finally:
        if own:
            conn.close()


def create_country(name: str, conn: sqlite3.Connection | None = None) -> int:
    """Insert country, return id. Raises ValueError if name empty or duplicate."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Country name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO countries (name) VALUES (?)", (name,))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A country with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def update_country(country_id: int, name: str, conn: sqlite3.Connection | None = None) -> bool:
    """Update country name. Returns True if updated. Raises ValueError if name empty or duplicate."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Country name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE countries SET name = ? WHERE id = ?", (name, country_id))
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A country with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def delete_country(country_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete country. Raises ValueError if still in use by source_pages, offices, parties, or states."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        in_use = []
        try:
            if _count_refs(conn, "source_pages", "country_id", country_id) > 0:
                in_use.append("source pages")
            if _count_refs(conn, "states", "country_id", country_id) > 0:
                in_use.append("states")
            if _count_refs(conn, "parties", "country_id", country_id) > 0:
                in_use.append("parties")
        except sqlite3.OperationalError:
            pass
        try:
            if _count_refs(conn, "offices", "country_id", country_id) > 0:
                in_use.append("offices")
        except sqlite3.OperationalError:
            pass
        if in_use:
            raise ValueError("Cannot delete: still in use by " + ", ".join(in_use))
        conn.execute("DELETE FROM countries WHERE id = ?", (country_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def list_states_with_country(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return states with id, name, country_id, country_name, ordered by country name then state name."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT s.id, s.name, s.country_id, c.name AS country_name FROM states s "
            "JOIN countries c ON c.id = s.country_id ORDER BY c.name, s.name"
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_state(state_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return state row with id, name, country_id, or None."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT s.id, s.name, s.country_id FROM states s WHERE s.id = ?", (state_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own:
            conn.close()


def create_state(country_id: int, name: str, conn: sqlite3.Connection | None = None) -> int:
    """Insert state, return id. Raises ValueError if name empty or duplicate for country."""
    name = (name or "").strip()
    if not name:
        raise ValueError("State name is required")
    if not country_id:
        raise ValueError("Country is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO states (country_id, name) VALUES (?, ?)", (country_id, name))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e) or "FOREIGN" in str(e):
            raise ValueError("A state with this name already exists for this country") from e
        raise
    finally:
        if own:
            conn.close()


def update_state(state_id: int, country_id: int, name: str, conn: sqlite3.Connection | None = None) -> bool:
    """Update state. Returns True if updated."""
    name = (name or "").strip()
    if not name:
        raise ValueError("State name is required")
    if not country_id:
        raise ValueError("Country is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE states SET country_id = ?, name = ? WHERE id = ?", (country_id, name, state_id)
        )
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A state with this name already exists for this country") from e
        raise
    finally:
        if own:
            conn.close()


def delete_state(state_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete state. Raises ValueError if still in use by source_pages, offices, or cities."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        in_use = []
        try:
            if _count_refs(conn, "source_pages", "state_id", state_id) > 0:
                in_use.append("source pages")
            if _count_refs(conn, "offices", "state_id", state_id) > 0:
                in_use.append("offices")
            if _count_refs(conn, "cities", "state_id", state_id) > 0:
                in_use.append("cities")
        except sqlite3.OperationalError:
            pass
        if in_use:
            raise ValueError("Cannot delete: still in use by " + ", ".join(in_use))
        conn.execute("DELETE FROM states WHERE id = ?", (state_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def list_cities(state_id: int, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return cities for the given state (for page dropdown). state_id required."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT id, name FROM cities WHERE state_id = ? ORDER BY name", (state_id,)
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def list_cities_with_country_state(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return cities with id, name, state_id, country_name, state_name for refs list."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT c.id, c.name, c.state_id, co.name AS country_name, s.name AS state_name "
            "FROM cities c JOIN states s ON s.id = c.state_id JOIN countries co ON co.id = s.country_id "
            "ORDER BY co.name, s.name, c.name"
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_city(city_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return city row with id, name, state_id, or None."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute(
            "SELECT c.id, c.name, c.state_id FROM cities c WHERE c.id = ?", (city_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own:
            conn.close()


def create_city(state_id: int, name: str, conn: sqlite3.Connection | None = None) -> int:
    """Insert city, return id. state_id required. Raises ValueError if name empty or duplicate for state."""
    name = (name or "").strip()
    if not name:
        raise ValueError("City name is required")
    if not state_id:
        raise ValueError("State is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO cities (state_id, name) VALUES (?, ?)", (state_id, name))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A city with this name already exists for this state") from e
        raise
    finally:
        if own:
            conn.close()


def update_city(city_id: int, state_id: int, name: str, conn: sqlite3.Connection | None = None) -> bool:
    """Update city. Returns True if updated."""
    name = (name or "").strip()
    if not name:
        raise ValueError("City name is required")
    if not state_id:
        raise ValueError("State is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE cities SET state_id = ?, name = ? WHERE id = ?", (state_id, name, city_id)
        )
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A city with this name already exists for this state") from e
        raise
    finally:
        if own:
            conn.close()


def delete_city(city_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete city. Raises ValueError if still in use by source_pages."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        try:
            if _count_refs(conn, "source_pages", "city_id", city_id) > 0:
                raise ValueError("Cannot delete: city is linked to one or more pages")
        except sqlite3.OperationalError:
            pass
        conn.execute("DELETE FROM cities WHERE id = ?", (city_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def get_city_name(city_id: int | None, conn: sqlite3.Connection | None = None) -> str:
    """Return city name for display, or empty string if no city_id."""
    if not city_id:
        return ""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,))
        row = cur.fetchone()
        return row["name"] if row else ""
    finally:
        if own:
            conn.close()


def get_state_name(state_id: int | None, conn: sqlite3.Connection | None = None) -> str:
    if not state_id:
        return ""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT name FROM states WHERE id = ?", (state_id,))
        row = cur.fetchone()
        return row["name"] if row else ""
    finally:
        if own:
            conn.close()


def get_level_name(level_id: int | None, conn: sqlite3.Connection | None = None) -> str:
    if not level_id:
        return ""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT name FROM levels WHERE id = ?", (level_id,))
        row = cur.fetchone()
        return row["name"] if row else ""
    finally:
        if own:
            conn.close()


def get_branch_name(branch_id: int | None, conn: sqlite3.Connection | None = None) -> str:
    if not branch_id:
        return ""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT name FROM branches WHERE id = ?", (branch_id,))
        row = cur.fetchone()
        return row["name"] if row else ""
    finally:
        if own:
            conn.close()


def get_country(country_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return country row with id, name, or None."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute("SELECT id, name FROM countries WHERE id = ?", (country_id,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own:
            conn.close()


def get_level(level_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return level row with id, name, or None."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute("SELECT id, name FROM levels WHERE id = ?", (level_id,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own:
            conn.close()


def get_branch(branch_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    """Return branch row with id, name, or None."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        row = conn.execute("SELECT id, name FROM branches WHERE id = ?", (branch_id,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own:
            conn.close()


def create_level(name: str, conn: sqlite3.Connection | None = None) -> int:
    """Insert level, return id. Raises ValueError if name empty or duplicate."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Level name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO levels (name) VALUES (?)", (name,))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A level with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def update_level(level_id: int, name: str, conn: sqlite3.Connection | None = None) -> bool:
    """Update level name. Returns True if updated."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Level name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE levels SET name = ? WHERE id = ?", (name, level_id))
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A level with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def delete_level(level_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete level. Raises ValueError if still in use by source_pages or offices."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        in_use = []
        try:
            if _count_refs(conn, "source_pages", "level_id", level_id) > 0:
                in_use.append("source pages")
            if _count_refs(conn, "offices", "level_id", level_id) > 0:
                in_use.append("offices")
        except sqlite3.OperationalError:
            pass
        if in_use:
            raise ValueError("Cannot delete: still in use by " + ", ".join(in_use))
        conn.execute("DELETE FROM levels WHERE id = ?", (level_id,))
        conn.commit()
    finally:
        if own:
            conn.close()


def create_branch(name: str, conn: sqlite3.Connection | None = None) -> int:
    """Insert branch, return id. Raises ValueError if name empty or duplicate."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Branch name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        conn.execute("INSERT INTO branches (name) VALUES (?)", (name,))
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A branch with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def update_branch(branch_id: int, name: str, conn: sqlite3.Connection | None = None) -> bool:
    """Update branch name. Returns True if updated."""
    name = (name or "").strip()
    if not name:
        raise ValueError("Branch name is required")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.execute("UPDATE branches SET name = ? WHERE id = ?", (name, branch_id))
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e):
            raise ValueError("A branch with this name already exists") from e
        raise
    finally:
        if own:
            conn.close()


def delete_branch(branch_id: int, conn: sqlite3.Connection | None = None) -> None:
    """Delete branch. Raises ValueError if still in use by source_pages or offices."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        in_use = []
        try:
            if _count_refs(conn, "source_pages", "branch_id", branch_id) > 0:
                in_use.append("source pages")
            if _count_refs(conn, "offices", "branch_id", branch_id) > 0:
                in_use.append("offices")
        except sqlite3.OperationalError:
            pass
        if in_use:
            raise ValueError("Cannot delete: still in use by " + ", ".join(in_use))
        conn.execute("DELETE FROM branches WHERE id = ?", (branch_id,))
        conn.commit()
    finally:
        if own:
            conn.close()
