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
    """Return the path to the SQLite database file. Uses OFFICE_HOLDER_DB_PATH env var when set."""
    env_path = os.environ.get("OFFICE_HOLDER_DB_PATH")
    if env_path:
        return Path(env_path)
    return DB_PATH


def get_log_dir() -> Path:
    """Return the path to the logs directory. When OFFICE_HOLDER_DB_PATH is set, logs live next to the DB."""
    env_path = os.environ.get("OFFICE_HOLDER_DB_PATH")
    if env_path:
        return Path(env_path).parent / "logs"
    return LOG_DIR


def ensure_data_dir() -> None:
    """Create data and logs directories if they don't exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    get_log_dir().mkdir(parents=True, exist_ok=True)


def get_connection(path: Path | None = None) -> sqlite3.Connection:
    """Return a connection to the database, creating it and schema if needed.
    When path is None, uses OFFICE_HOLDER_DB_PATH env var if set, else DB_PATH."""
    ensure_data_dir()
    db_path = path if path is not None else get_db_path()
    # Timeout (seconds) so we don't hang forever if the DB is locked by another process
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(path: Path | None = None) -> None:
    """Create database, run schema, seed reference data, and run FK migration if needed.
    When path is None, uses OFFICE_HOLDER_DB_PATH env var if set, else DB_PATH."""
    conn = get_connection(path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        from .seed import seed_reference_data
        from .migrate import migrate_to_fk
        from . import test_scripts as db_test_scripts
        seed_reference_data(conn=conn)
        migrate_to_fk(conn=conn)
        db_test_scripts.seed_db_from_manifest_if_empty(conn=conn)
        conn.executescript(OFFICES_PARTIES_INDEX_SQL)
        conn.commit()
    finally:
        conn.close()
