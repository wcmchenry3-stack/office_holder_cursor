"""Database connection and initialization."""

import os
import sqlite3
from pathlib import Path

# Exception type tuples for use in CRUD modules — work for both backends.
# Catch _DB_UNIQUE_ERRORS and check "UNIQUE" / "duplicate key" in str(e).
# Catch _DB_OPERATIONAL_ERRORS for "table may not exist" guards (SQLite only in practice).
_DB_UNIQUE_ERRORS: tuple = (sqlite3.IntegrityError,)
_DB_OPERATIONAL_ERRORS: tuple = (sqlite3.OperationalError,)
try:
    import psycopg2
    import psycopg2.errors

    _DB_UNIQUE_ERRORS = _DB_UNIQUE_ERRORS + (psycopg2.errors.UniqueViolation,)
    _DB_OPERATIONAL_ERRORS = _DB_OPERATIONAL_ERRORS + (psycopg2.OperationalError,)
except ImportError:
    pass


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "office_holder.db"  # test-only default; production uses DATABASE_URL
_DEFAULT_LOG_DIR = DATA_DIR / "logs"
_DEFAULT_CACHE_DIR = DATA_DIR / "wiki_cache"


def is_postgres() -> bool:
    """Return True when DATABASE_URL is set (Render / local PG dev)."""
    return bool(os.environ.get("DATABASE_URL"))


def get_db_path() -> Path:
    """Return the SQLite DB path (test use only). Uses OFFICE_HOLDER_DB_PATH env var when set."""
    env_path = os.environ.get("OFFICE_HOLDER_DB_PATH")
    if env_path:
        return Path(env_path)
    return DB_PATH


def get_log_dir() -> Path:
    """Return the logs directory path."""
    env_path = os.environ.get("LOG_DIR")
    if env_path:
        return Path(env_path)
    return _DEFAULT_LOG_DIR


def get_cache_dir() -> Path:
    """Return the wiki cache directory path."""
    env_path = os.environ.get("WIKI_CACHE_DIR")
    if env_path:
        return Path(env_path)
    return _DEFAULT_CACHE_DIR


def ensure_data_dir() -> None:
    """Create cache and log directories if they don't exist."""
    get_log_dir().mkdir(parents=True, exist_ok=True)
    get_cache_dir().mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


class _PGConnWrapper:
    """Thin wrapper around a psycopg2 connection that adds .execute() and
    .executemany() shortcuts matching the sqlite3 connection API, so all CRUD
    modules can call conn.execute() / conn.executemany() without changes.
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params if params is not None else ())
        return cur

    def executemany(self, sql, seq_of_params):
        cur = self._conn.cursor()
        cur.executemany(sql, seq_of_params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def cursor(self):
        return self._conn.cursor()

    @property
    def closed(self):
        return self._conn.closed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False  # do not suppress exceptions


class _PrefetchedCursor:
    """Wraps a pre-fetched result set so callers can fetch after commit()."""

    def __init__(self, rows, rowcount, description):
        self._rows = list(rows)
        self._idx = 0
        self.rowcount = rowcount
        self.description = description

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def fetchall(self):
        rows = self._rows[self._idx :]
        self._idx = len(self._rows)
        return rows


class _SQLiteConnWrapper:
    """Wraps a sqlite3 connection to accept PostgreSQL-style SQL so CRUD modules
    only need one SQL dialect.  Translations applied to every statement:
      • %s  → ?          (psycopg2 → sqlite3 placeholder)
      • NOW() → CURRENT_TIMESTAMP   (timestamp function)
    SQLite 3.35+ RETURNING and 3.24+ ON CONFLICT upserts are supported natively.
    RETURNING results are pre-fetched so commit() can be called before fetchone().
    """

    def __init__(self, conn):
        self._conn = conn

    @staticmethod
    def _adapt(sql: str) -> str:
        """Translate PostgreSQL-style SQL to SQLite-compatible SQL.

        Translations:
          %s      → ?                  (placeholders)
          %%      → %                  (escaped literal % for psycopg2 → SQLite modulo)
          NOW()   → CURRENT_TIMESTAMP  (timestamp)
          ::TEXT  → (removed)          (PostgreSQL explicit cast — SQLite is dynamically typed)
          ::integer → (removed)        (same)
          ::date  → (removed)          (same)
        """
        import re

        return (
            sql.replace("NOW()", "CURRENT_TIMESTAMP")
            .replace("%s", "?")
            .replace("%%", "%")
            # Strip PostgreSQL type casts (::type) — SQLite is dynamically typed
            .replace("::TEXT", "")
            .replace("::text", "")
            .replace("::integer", "")
            .replace("::INTEGER", "")
            .replace("::date", "")
        )

    def execute(self, sql, params=None):
        adapted = self._adapt(sql)
        cur = (
            self._conn.execute(adapted, params)
            if params is not None
            else self._conn.execute(adapted)
        )
        # Pre-fetch RETURNING results so callers can do commit() before fetchone()
        if "RETURNING" in sql.upper():
            return _PrefetchedCursor(cur.fetchall(), cur.rowcount, cur.description)
        return cur

    def executemany(self, sql, seq_of_params):
        return self._conn.executemany(self._adapt(sql), seq_of_params)

    def executescript(self, sql):
        return self._conn.executescript(sql)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def cursor(self):
        return self._conn.cursor()

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value

    @property
    def closed(self):
        return not bool(self._conn)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._conn.__exit__(exc_type, exc_val, exc_tb)


def get_connection(path: Path | None = None):
    """Return a database connection.

    Production (DATABASE_URL set, no path): returns a _PGConnWrapper (psycopg2).
    Tests (path provided, or DATABASE_URL not set): returns a _SQLiteConnWrapper.
    """
    if is_postgres() and path is None:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(
            os.environ["DATABASE_URL"],
            cursor_factory=psycopg2.extras.DictCursor,
        )
        return _PGConnWrapper(conn)

    # Test-only: SQLite path — wrapped to accept PostgreSQL-style SQL from CRUD modules
    ensure_data_dir()
    db_path = path if path is not None else get_db_path()
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return _SQLiteConnWrapper(conn)


def init_db(path: Path | None = None) -> None:
    """Create / migrate the database on startup."""
    if is_postgres():
        _init_postgres()
    else:
        _init_sqlite(path)


def _init_postgres() -> None:
    """Apply schema and seed data to a fresh or existing PostgreSQL database."""
    from .schema import SCHEMA_PG_SQL, OFFICES_PARTIES_INDEX_PG_SQL
    from .seed import seed_reference_data
    from . import test_scripts as db_test_scripts

    conn = get_connection()
    try:
        # Execute each DDL statement individually (_PGConnWrapper.execute handles this)
        for statement in _split_sql(SCHEMA_PG_SQL):
            conn.execute(statement)
        for statement in _split_sql(OFFICES_PARTIES_INDEX_PG_SQL):
            conn.execute(statement)
        conn.commit()

        seed_reference_data(conn=conn)
        conn.commit()

        db_test_scripts.seed_db_from_manifest_if_empty(conn=conn)
        conn.commit()

        _run_pg_migrations(conn)

        # migrate_to_fk() is deliberately NOT called — PostgreSQL starts with the final schema
    finally:
        conn.close()


def _run_pg_migrations(conn) -> None:
    """Apply idempotent PostgreSQL-only schema corrections that cannot be expressed as
    CREATE TABLE IF NOT EXISTS (e.g. dropping stale FK constraints)."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
    )
    conn.commit()

    cur = conn.execute("SELECT id FROM schema_migrations")
    applied = {row[0] for row in cur.fetchall()}

    def _apply(name: str, sql: str) -> None:
        if name in applied:
            return
        conn.execute(sql)
        conn.execute("INSERT INTO schema_migrations (id) VALUES (%s)", (name,))
        conn.commit()

    # office_terms.office_id previously had REFERENCES offices(id), but in hierarchy
    # mode it stores office_table_config_id values — violating the FK. Drop it; the
    # office_table_config_id column already carries the proper referential integrity.
    _apply(
        "pg_drop_office_terms_office_id_fkey",
        "ALTER TABLE office_terms DROP CONSTRAINT IF EXISTS office_terms_office_id_fkey",
    )

    # source_pages.url must be unique — prevent duplicate pages from AI-driven inserts.
    _apply(
        "pg_source_pages_url_unique",
        "ALTER TABLE source_pages ADD CONSTRAINT source_pages_url_key UNIQUE (url)",
    )

    # parse_error_reports: new table for ParseErrorReporter deduplication.
    # Already created by SCHEMA_PG_SQL on fresh installs; this migration adds it
    # to existing production databases that pre-date this table.
    _apply(
        "pg_create_parse_error_reports",
        """
        CREATE TABLE IF NOT EXISTS parse_error_reports (
            id SERIAL PRIMARY KEY,
            fingerprint TEXT NOT NULL UNIQUE,
            function_name TEXT NOT NULL,
            error_type TEXT NOT NULL,
            wiki_url TEXT,
            office_name TEXT,
            github_issue_url TEXT,
            github_issue_number INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    )
    _apply(
        "pg_parse_error_reports_fingerprint_idx",
        "CREATE INDEX IF NOT EXISTS idx_parse_error_reports_fingerprint"
        " ON parse_error_reports(fingerprint)",
    )


def _init_sqlite(path: Path | None = None) -> None:
    """SQLite init for tests — applies the final schema directly (no migrations needed)."""
    from .schema import SCHEMA_SQL
    from .seed import seed_reference_data
    from . import test_scripts as db_test_scripts

    conn = get_connection(path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        seed_reference_data(conn=conn)
        db_test_scripts.seed_db_from_manifest_if_empty(conn=conn)
        conn.commit()
    finally:
        conn.close()


def _split_sql(sql: str) -> list[str]:
    """Split a multi-statement SQL string into individual statements."""
    return [s.strip() for s in sql.split(";") if s.strip()]
