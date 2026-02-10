"""Shared DB helpers."""

import sqlite3
from typing import Any


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    """Convert sqlite3.Row to dict. Returns {} if row is None."""
    return dict(row) if row else {}
