"""Database connection and initialization."""

import os
import sqlite3
from pathlib import Path

from .schema import SCHEMA_SQL, OFFICES_PARTIES_INDEX_SQL

# Default DB path: data/ in project root (parent of src)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "office_holder.db"
LOG_DIR = DATA_DIR / "logs"


def get_db_path() -> Path:
    """Return the path to the SQLite database file."""
    return DB_PATH


def get_log_dir() -> Path:
    """Return the path to the logs directory."""
    return LOG_DIR


def ensure_data_dir() -> None:
    """Create data and logs directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_connection(path: Path | None = None) -> sqlite3.Connection:
    """Return a connection to the database, creating it and schema if needed."""
    ensure_data_dir()
    db_path = path or DB_PATH
    # Timeout (seconds) so we don't hang forever if the DB is locked by another process
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(path: Path | None = None) -> None:
    """Create database, run schema, seed reference data, and run FK migration if needed."""
    conn = get_connection(path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        from .seed import seed_reference_data
        from .migrate import migrate_to_fk
        seed_reference_data(conn=conn)
        migrate_to_fk(conn=conn)
        conn.executescript(OFFICES_PARTIES_INDEX_SQL)
        conn.commit()
    finally:
        conn.close()
