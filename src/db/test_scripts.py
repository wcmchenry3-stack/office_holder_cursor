"""CRUD for parser test scripts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .connection import get_connection
from .utils import _row_to_dict


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEST_SCRIPTS_DIR = PROJECT_ROOT / "test_scripts"


def ensure_test_scripts_dir() -> Path:
    TEST_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    return TEST_SCRIPTS_DIR


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS parser_test_scripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            test_type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            html_file TEXT NOT NULL,
            source_url TEXT,
            config_json TEXT NOT NULL,
            expected_json TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parser_test_scripts_enabled ON parser_test_scripts(enabled)")
    conn.commit()


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("config_json", "expected_json"):
        try:
            out[key] = json.loads(out[key]) if out.get(key) else None
        except Exception:
            out[key] = None
    out["enabled"] = bool(out.get("enabled"))
    return out


def list_tests(conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        cur = conn.execute("SELECT * FROM parser_test_scripts ORDER BY id DESC")
        return [_parse_row(_row_to_dict(r)) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def get_test(test_id: int, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        row = conn.execute("SELECT * FROM parser_test_scripts WHERE id = ?", (test_id,)).fetchone()
        return _parse_row(_row_to_dict(row)) if row else None
    finally:
        if own:
            conn.close()


def create_test(data: dict[str, Any], conn: sqlite3.Connection | None = None) -> int:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        cur = conn.execute(
            """
            INSERT INTO parser_test_scripts(name, test_type, enabled, html_file, source_url, config_json, expected_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (data.get("name") or "").strip(),
                (data.get("test_type") or "table_config").strip(),
                1 if data.get("enabled", True) else 0,
                (data.get("html_file") or "").strip(),
                (data.get("source_url") or "").strip() or None,
                json.dumps(data.get("config_json") or {}, ensure_ascii=False),
                json.dumps(data.get("expected_json"), ensure_ascii=False) if data.get("expected_json") is not None else None,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        if own:
            conn.close()


def update_test_enabled(test_id: int, enabled: bool, conn: sqlite3.Connection | None = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute("UPDATE parser_test_scripts SET enabled = ?, updated_at = datetime('now') WHERE id = ?", (1 if enabled else 0, test_id))
        conn.commit()
    finally:
        if own:
            conn.close()


def delete_test(test_id: int, conn: sqlite3.Connection | None = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute("DELETE FROM parser_test_scripts WHERE id = ?", (test_id,))
        conn.commit()
    finally:
        if own:
            conn.close()



def update_test(test_id: int, data: dict[str, Any], conn: sqlite3.Connection | None = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        _ensure_table(conn)
        conn.execute(
            """
            UPDATE parser_test_scripts
               SET name = ?,
                   test_type = ?,
                   enabled = ?,
                   html_file = ?,
                   source_url = ?,
                   config_json = ?,
                   expected_json = ?,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (
                (data.get("name") or "").strip(),
                (data.get("test_type") or "table_config").strip(),
                1 if data.get("enabled", True) else 0,
                (data.get("html_file") or "").strip(),
                (data.get("source_url") or "").strip() or None,
                json.dumps(data.get("config_json") or {}, ensure_ascii=False),
                json.dumps(data.get("expected_json"), ensure_ascii=False) if data.get("expected_json") is not None else None,
                test_id,
            ),
        )
        conn.commit()
    finally:
        if own:
            conn.close()
