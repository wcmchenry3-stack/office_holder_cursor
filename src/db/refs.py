"""Reference data: countries, states, levels, branches."""

import sqlite3
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


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
