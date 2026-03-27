"""Migration: ensure ref tables exist and offices/parties use FK columns."""

import re
import sqlite3
from datetime import datetime, timezone

from .connection import get_connection


def _columns(conn, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Migration version tracking
# ---------------------------------------------------------------------------

def _ensure_migrations_table(conn) -> None:
    """Create schema_migrations table if it doesn't exist."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations
           (id TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"""
    )
    conn.commit()


def _applied_migrations(conn) -> set:
    """Return set of already-applied migration IDs."""
    try:
        cur = conn.execute("SELECT id FROM schema_migrations")
        return {row[0] for row in cur.fetchall()}
    except Exception:
        return set()


def _record_migration(conn, name: str) -> None:
    """Record a migration as applied."""
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (id, applied_at) VALUES (%s, %s)",
        (name, ts),
    )
    conn.commit()


def _apply_migration(conn, name: str, fn, applied: set) -> None:
    """Run fn(conn) only if name is not already in applied; record it after."""
    if name in applied:
        return
    fn(conn)
    _record_migration(conn, name)
    applied.add(name)


def migrate_to_fk(conn=None):
    """
    If offices/parties have old text columns (country, level, branch, state),
    add FK columns, seed ref data, backfill, then replace tables with FK-only structure.
    New migrations are recorded in schema_migrations; already-applied ones are skipped.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        _ensure_migrations_table(conn)
        applied = _applied_migrations(conn)

        # Ensure ref tables exist (schema already created them)
        from .seed import seed_reference_data
        seed_reference_data(conn=conn)

        offices_cols = _columns(conn, "offices")
        parties_cols = _columns(conn, "parties")

        # New schema has country_id; old has country (text)
        if "country" in offices_cols and "country_id" not in offices_cols:
            _apply_migration(conn, "offices_to_fk", _migrate_offices_to_fk, applied)
        if "country" in parties_cols and "country_id" not in parties_cols:
            _apply_migration(conn, "parties_to_fk", _migrate_parties_to_fk, applied)

        # office_terms migrations
        ot_cols = _columns(conn, "office_terms")
        if "party_id" not in ot_cols:
            _apply_migration(conn, "office_terms_party_id", _migrate_office_terms_party_id, applied)
        ot_cols = _columns(conn, "office_terms")
        if "party" in ot_cols:
            _apply_migration(conn, "office_terms_drop_party", _migrate_office_terms_drop_party, applied)

        _apply_migration(conn, "imprecise_date_columns", _migrate_imprecise_date_columns, applied)
        _apply_migration(conn, "offices_enabled", _migrate_offices_enabled, applied)
        _apply_migration(conn, "offices_use_full_page_for_table", _migrate_offices_use_full_page_for_table, applied)
        _apply_migration(conn, "offices_years_only", _migrate_offices_years_only, applied)
        _apply_migration(conn, "office_terms_year_columns", _migrate_office_terms_year_columns, applied)
        _apply_migration(conn, "offices_parsing_options", _migrate_offices_parsing_options, applied)
        _apply_migration(conn, "ignore_non_links", _migrate_ignore_non_links, applied)
        _apply_migration(conn, "remove_duplicates", _migrate_remove_duplicates, applied)
        _apply_migration(conn, "row_filter_columns", _migrate_row_filter_columns, applied)
        _apply_migration(conn, "individuals_dead_link", _migrate_individuals_dead_link, applied)
        _apply_migration(conn, "individuals_is_living", _migrate_individuals_is_living, applied)
        _apply_migration(conn, "alt_links", _migrate_alt_links, applied)
        _apply_migration(conn, "page_office_table_hierarchy", _migrate_to_page_office_table_hierarchy, applied)
        _apply_migration(conn, "allow_reuse_tables_and_table_no_unique", _migrate_allow_reuse_tables_and_table_no_unique, applied)
        _apply_migration(conn, "office_table_config_name", _migrate_office_table_config_name, applied)
        _apply_migration(conn, "office_category", _migrate_office_category, applied)
        _apply_migration(conn, "infobox_role_key", _migrate_infobox_role_key, applied)
        _apply_migration(conn, "infobox_role_key_filter", _migrate_infobox_role_key_filter, applied)
        _apply_migration(conn, "office_table_config_infobox_role_key_filter_id", _migrate_office_table_config_infobox_role_key_filter_id, applied)
        _apply_migration(conn, "offices_infobox_role_key_filter_id", _migrate_offices_infobox_role_key_filter_id, applied)
        _apply_migration(conn, "infobox_role_key_filter_role_key_format", _migrate_infobox_role_key_filter_role_key_format, applied)
        _apply_migration(conn, "city", _migrate_city, applied)
        _apply_migration(conn, "source_pages_disable_auto_table_update", _migrate_source_pages_disable_auto_table_update, applied)
        _apply_migration(conn, "office_table_config_html_hash", _migrate_office_table_config_html_hash, applied)
        _apply_migration(conn, "individuals_bio_batch", _migrate_individuals_bio_batch, applied)
    finally:
        if own_conn:
            conn.close()


def _migrate_offices_to_fk(conn):
    """Create offices_new with FK columns, copy data by resolving text to ids, replace."""
    conn.execute("""
        CREATE TABLE offices_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL REFERENCES countries(id),
            state_id INTEGER REFERENCES states(id),
            level_id INTEGER REFERENCES levels(id),
            branch_id INTEGER REFERENCES branches(id),
            department TEXT,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            url TEXT NOT NULL,
            table_no INTEGER NOT NULL DEFAULT 1,
            table_rows INTEGER NOT NULL DEFAULT 4,
            link_column INTEGER NOT NULL DEFAULT 1,
            party_column INTEGER NOT NULL DEFAULT 0,
            term_start_column INTEGER NOT NULL DEFAULT 4,
            term_end_column INTEGER NOT NULL DEFAULT 5,
            district_column INTEGER NOT NULL DEFAULT 0,
            dynamic_parse INTEGER NOT NULL DEFAULT 1,
            read_right_to_left INTEGER NOT NULL DEFAULT 0,
            find_date_in_infobox INTEGER NOT NULL DEFAULT 0,
            parse_rowspan INTEGER NOT NULL DEFAULT 0,
            rep_link INTEGER NOT NULL DEFAULT 0,
            party_link INTEGER NOT NULL DEFAULT 0,
            alt_link TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    # Resolve country -> country_id; preserve office id for office_terms FK
    conn.execute("""
        INSERT INTO offices_new (
            id, country_id, state_id, level_id, branch_id, department, name, enabled, notes, url,
            table_no, table_rows, link_column, party_column, term_start_column, term_end_column,
            district_column, dynamic_parse, read_right_to_left, find_date_in_infobox,
            parse_rowspan, rep_link, party_link, alt_link, created_at
        )
        SELECT
            o.id,
            COALESCE((SELECT id FROM countries WHERE name = o.country LIMIT 1), (SELECT id FROM countries LIMIT 1)),
            (SELECT id FROM states s WHERE s.name = o.state AND s.country_id = COALESCE((SELECT id FROM countries WHERE name = o.country LIMIT 1), (SELECT id FROM countries LIMIT 1)) LIMIT 1),
            (SELECT id FROM levels WHERE name = o.level LIMIT 1),
            (SELECT id FROM branches WHERE name = o.branch LIMIT 1),
            o.department, o.name, 1, o.notes, o.url,
            o.table_no, o.table_rows, o.link_column, o.party_column, o.term_start_column, o.term_end_column,
            o.district_column, o.dynamic_parse, o.read_right_to_left, o.find_date_in_infobox,
            o.parse_rowspan, o.rep_link, o.party_link, o.alt_link, o.created_at
        FROM offices o
    """)
    conn.execute("DROP TABLE offices")
    conn.execute("ALTER TABLE offices_new RENAME TO offices")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_country_id ON offices(country_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_state_id ON offices(state_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_level_id ON offices(level_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_branch_id ON offices(branch_id)")
    conn.commit()


def _migrate_source_pages_disable_auto_table_update(conn):
    cols = _columns(conn, "source_pages")
    if "disable_auto_table_update" in cols:
        return
    conn.execute(
        "ALTER TABLE source_pages ADD COLUMN disable_auto_table_update INTEGER NOT NULL DEFAULT 0"
    )
    conn.commit()


def _migrate_parties_to_fk(conn):
    conn.execute("""
        CREATE TABLE parties_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL REFERENCES countries(id),
            party_name TEXT NOT NULL,
            party_link TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        INSERT INTO parties_new (country_id, party_name, party_link, created_at)
        SELECT
            COALESCE((SELECT id FROM countries WHERE name = p.country LIMIT 1), (SELECT id FROM countries LIMIT 1)),
            p.party_name, p.party_link, p.created_at
        FROM parties p
    """)
    conn.execute("DROP TABLE parties")
    conn.execute("ALTER TABLE parties_new RENAME TO parties")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_parties_country_id ON parties(country_id)")
    conn.commit()


def _migrate_office_terms_party_id(conn):
    """Add party_id to office_terms and backfill from party text (match by office country + party name/link)."""
    conn.execute("ALTER TABLE office_terms ADD COLUMN party_id INTEGER REFERENCES parties(id)")
    conn.execute("""
        UPDATE office_terms SET party_id = (
            SELECT p.id FROM parties p
            JOIN offices o ON o.id = office_terms.office_id AND o.country_id = p.country_id
            WHERE (p.party_name = trim(office_terms.party) OR p.party_link = trim(office_terms.party))
            LIMIT 1
        ) WHERE party_id IS NULL AND party IS NOT NULL AND trim(party) != ''
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id)")
    conn.commit()


def _migrate_office_terms_drop_party(conn):
    """Recreate office_terms without the party text column (use party_id only)."""
    ot_cols_before = _columns(conn, "office_terms")
    has_imprecise = "term_start_imprecise" in ot_cols_before
    conn.execute("""
        CREATE TABLE office_terms_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_id INTEGER NOT NULL REFERENCES offices(id),
            individual_id INTEGER REFERENCES individuals(id),
            party_id INTEGER REFERENCES parties(id),
            district TEXT,
            term_start TEXT,
            term_end TEXT,
            term_start_imprecise INTEGER NOT NULL DEFAULT 0,
            term_end_imprecise INTEGER NOT NULL DEFAULT 0,
            wiki_url TEXT NOT NULL,
            scraped_at TEXT DEFAULT (datetime('now')),
            UNIQUE(office_id, wiki_url, term_start, term_end)
        )
    """)
    if has_imprecise:
        conn.execute("""
            INSERT INTO office_terms_new (id, office_id, individual_id, party_id, district, term_start, term_end, term_start_imprecise, term_end_imprecise, wiki_url, scraped_at)
            SELECT id, office_id, individual_id, party_id, district, term_start, term_end, term_start_imprecise, term_end_imprecise, wiki_url, scraped_at
            FROM office_terms
        """)
    else:
        conn.execute("""
            INSERT INTO office_terms_new (id, office_id, individual_id, party_id, district, term_start, term_end, wiki_url, scraped_at)
            SELECT id, office_id, individual_id, party_id, district, term_start, term_end, wiki_url, scraped_at
            FROM office_terms
        """)
    conn.execute("DROP TABLE office_terms")
    conn.execute("ALTER TABLE office_terms_new RENAME TO office_terms")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_office_id ON office_terms(office_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_office_terms_individual_id ON office_terms(individual_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_wiki_url ON office_terms(wiki_url)")
    conn.commit()


def _migrate_offices_enabled(conn):
    """Add enabled column to offices if missing (1 = on, 0 = off)."""
    offices_cols = _columns(conn, "offices")
    if "enabled" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
        conn.commit()


def _migrate_offices_use_full_page_for_table(conn):
    """Add use_full_page_for_table to offices if missing (0 = REST default, 1 = full page fetch)."""
    offices_cols = _columns(conn, "offices")
    if "use_full_page_for_table" not in offices_cols:
        conn.execute(
            "ALTER TABLE offices ADD COLUMN use_full_page_for_table INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()


def _migrate_offices_years_only(conn):
    """Add years_only to offices if missing (0 = full dates or infobox, 1 = table has years only)."""
    offices_cols = _columns(conn, "offices")
    if "years_only" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN years_only INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def _migrate_offices_parsing_options(conn):
    """Add term_dates_merged, party_ignore, district_ignore, district_at_large, consolidate_rowspan_terms to offices if missing."""
    offices_cols = _columns(conn, "offices")
    for col, default in (
        ("term_dates_merged", 0),
        ("party_ignore", 0),
        ("district_ignore", 0),
        ("district_at_large", 0),
        ("consolidate_rowspan_terms", 0),
    ):
        if col not in offices_cols:
            conn.execute(f"ALTER TABLE offices ADD COLUMN {col} INTEGER NOT NULL DEFAULT {default}")
    conn.commit()


def _migrate_office_terms_year_columns(conn):
    """Add term_start_year, term_end_year to office_terms and extend UNIQUE. Recreates table."""
    ot_cols = _columns(conn, "office_terms")
    if "term_start_year" in ot_cols and "term_end_year" in ot_cols:
        return
    conn.execute("""
        CREATE TABLE office_terms_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_id INTEGER NOT NULL REFERENCES offices(id),
            individual_id INTEGER REFERENCES individuals(id),
            party_id INTEGER REFERENCES parties(id),
            district TEXT,
            term_start TEXT,
            term_end TEXT,
            term_start_year INTEGER,
            term_end_year INTEGER,
            term_start_imprecise INTEGER NOT NULL DEFAULT 0,
            term_end_imprecise INTEGER NOT NULL DEFAULT 0,
            wiki_url TEXT NOT NULL,
            scraped_at TEXT DEFAULT (datetime('now')),
            UNIQUE(office_id, wiki_url, term_start, term_end, term_start_year, term_end_year)
        )
    """)
    conn.execute("""
        INSERT INTO office_terms_new (id, office_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, term_start_imprecise, term_end_imprecise, wiki_url, scraped_at)
        SELECT id, office_id, individual_id, party_id, district, term_start, term_end, NULL, NULL, term_start_imprecise, term_end_imprecise, wiki_url, scraped_at
        FROM office_terms
    """)
    conn.execute("DROP TABLE office_terms")
    conn.execute("ALTER TABLE office_terms_new RENAME TO office_terms")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_office_id ON office_terms(office_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_office_terms_individual_id ON office_terms(individual_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_wiki_url ON office_terms(wiki_url)")
    conn.commit()


def _migrate_imprecise_date_columns(conn):
    """Add birth_date_imprecise, death_date_imprecise to individuals; term_start_imprecise, term_end_imprecise to office_terms. Backfill: set date to null and flag to 1 where date is not YYYY-MM-DD."""
    import re

    valid_date = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    ind_cols = _columns(conn, "individuals")
    if "birth_date_imprecise" not in ind_cols:
        conn.execute(
            "ALTER TABLE individuals ADD COLUMN birth_date_imprecise INTEGER NOT NULL DEFAULT 0"
        )
    if "death_date_imprecise" not in ind_cols:
        conn.execute(
            "ALTER TABLE individuals ADD COLUMN death_date_imprecise INTEGER NOT NULL DEFAULT 0"
        )
    # Backfill individuals: invalid date -> null + imprecise 1
    for row in conn.execute("SELECT id, birth_date, death_date FROM individuals").fetchall():
        rid, bd, dd = row["id"], row["birth_date"], row["death_date"]
        if bd is not None and not valid_date.match(str(bd).strip()):
            conn.execute(
                "UPDATE individuals SET birth_date = NULL, birth_date_imprecise = 1 WHERE id = ?",
                (rid,),
            )
        if dd is not None and not valid_date.match(str(dd).strip()):
            conn.execute(
                "UPDATE individuals SET death_date = NULL, death_date_imprecise = 1 WHERE id = ?",
                (rid,),
            )
    ot_cols = _columns(conn, "office_terms")
    if "term_start_imprecise" not in ot_cols:
        conn.execute(
            "ALTER TABLE office_terms ADD COLUMN term_start_imprecise INTEGER NOT NULL DEFAULT 0"
        )
    if "term_end_imprecise" not in ot_cols:
        conn.execute(
            "ALTER TABLE office_terms ADD COLUMN term_end_imprecise INTEGER NOT NULL DEFAULT 0"
        )
    for row in conn.execute("SELECT id, term_start, term_end FROM office_terms").fetchall():
        oid, ts, te = row["id"], row["term_start"], row["term_end"]
        if ts is not None and not valid_date.match(str(ts).strip()):
            conn.execute(
                "UPDATE office_terms SET term_start = NULL, term_start_imprecise = 1 WHERE id = ?",
                (oid,),
            )
        if te is not None and not valid_date.match(str(te).strip()):
            conn.execute(
                "UPDATE office_terms SET term_end = NULL, term_end_imprecise = 1 WHERE id = ?",
                (oid,),
            )
    conn.commit()


def _migrate_individuals_dead_link(conn):
    """Add is_dead_link to individuals if missing."""
    ind_cols = _columns(conn, "individuals")
    if "is_dead_link" not in ind_cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN is_dead_link INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def _migrate_individuals_is_living(conn):
    """Add is_living to individuals if missing and backfill based on death_date and earliest office term year.

    Rule:
    - If death_date is set -> is_living = 0.
    - Else if earliest known office term is >80 years ago -> is_living = 0.
    - Else is_living stays 1 (default).
    """
    from datetime import date

    ind_cols = _columns(conn, "individuals")
    if "is_living" not in ind_cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN is_living INTEGER NOT NULL DEFAULT 1")
        conn.commit()

    current_year = date.today().year
    # Compute earliest term year per individual from office_terms
    cur = conn.execute("""
        SELECT i.id AS individual_id,
               i.death_date AS death_date,
               MIN(
                 COALESCE(
                   ot.term_start_year,
                   CAST(strftime('%Y', ot.term_start) AS INTEGER)
                 )
               ) AS earliest_year
        FROM individuals i
        LEFT JOIN office_terms ot ON ot.individual_id = i.id
        GROUP BY i.id, i.death_date
        """)
    rows = cur.fetchall()
    for row in rows:
        ind_id = row["individual_id"]
        death_date = (row["death_date"] or "").strip() if row["death_date"] is not None else ""
        earliest_year = row["earliest_year"]

        # If already marked not living, skip recompute
        cur2 = conn.execute("SELECT is_living FROM individuals WHERE id = ?", (ind_id,))
        existing_flag = cur2.fetchone()[0]
        if existing_flag == 0:
            continue

        is_living = 1
        if death_date:
            is_living = 0
        elif earliest_year is not None:
            try:
                ey = int(earliest_year)
                if current_year - ey > 80:
                    is_living = 0
            except (TypeError, ValueError):
                pass
        conn.execute("UPDATE individuals SET is_living = ? WHERE id = ?", (is_living, ind_id))
    conn.commit()


def _normalize_alt_link_path(raw: str) -> str:
    """Normalize alt link to a path (e.g. /wiki/Foo or /wiki/Bar)."""
    if not raw or not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s or s.lower() in ("none", ""):
        return ""
    if s.startswith("http"):
        from urllib.parse import urlparse

        return urlparse(s).path or ""
    if not s.startswith("/"):
        return "/wiki/" + s.lstrip("/")
    return s if s.startswith("/wiki/") else "/wiki/" + s.lstrip("/")


def _migrate_alt_links(conn):
    """
    1) Create alt_links table if missing.
    2) Add alt_link_include_main to offices if missing.
    3) If offices has alt_link: backfill into alt_links (split comma/newline), verify, then drop alt_link column.
    """
    import re

    offices_cols = _columns(conn, "offices")

    # Ensure alt_links table exists (schema may have created it; CREATE IF NOT EXISTS for old DBs)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alt_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_id INTEGER NOT NULL REFERENCES offices(id),
            link_path TEXT NOT NULL,
            UNIQUE(office_id, link_path)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alt_links_office_id ON alt_links(office_id)")

    # Add alt_link_include_main to offices if missing
    if "alt_link_include_main" not in offices_cols:
        conn.execute(
            "ALTER TABLE offices ADD COLUMN alt_link_include_main INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()

    if "alt_link" not in offices_cols:
        conn.commit()
        return

    # Backfill: copy offices.alt_link into alt_links (split on comma and newline)
    for row in conn.execute(
        "SELECT id, alt_link FROM offices WHERE alt_link IS NOT NULL AND trim(alt_link) != ''"
    ).fetchall():
        office_id = row["id"]
        raw = (row["alt_link"] or "").strip()
        if not raw:
            continue
        parts = re.split(r"[,\n]+", raw)
        for part in parts:
            path = _normalize_alt_link_path(part)
            if path:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO alt_links (office_id, link_path) VALUES (?, ?)",
                        (office_id, path),
                    )
                except Exception:
                    pass
    conn.commit()

    # Verify: every office that had non-empty alt_link must have at least one row in alt_links
    count_offices_with_alt = conn.execute(
        "SELECT COUNT(*) FROM offices WHERE alt_link IS NOT NULL AND trim(alt_link) != ''"
    ).fetchone()[0]
    count_alt_rows = conn.execute("SELECT COUNT(*) FROM alt_links").fetchone()[0]
    # If any office had alt_link, we must have at least that many rows (each office >= 1)
    if count_offices_with_alt > 0 and count_alt_rows < count_offices_with_alt:
        # Verification failed: do not drop alt_link (backfill already committed)
        return

    # Drop alt_link: create offices_new without alt_link, copy data, replace
    all_cols = _columns(conn, "offices")
    cols_without_alt = [c for c in all_cols if c != "alt_link"]
    cols_without_alt_set = set(cols_without_alt)

    conn.execute("""
        CREATE TABLE offices_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL REFERENCES countries(id),
            state_id INTEGER REFERENCES states(id),
            level_id INTEGER REFERENCES levels(id),
            branch_id INTEGER REFERENCES branches(id),
            department TEXT,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            url TEXT NOT NULL,
            table_no INTEGER NOT NULL DEFAULT 1,
            table_rows INTEGER NOT NULL DEFAULT 4,
            link_column INTEGER NOT NULL DEFAULT 1,
            party_column INTEGER NOT NULL DEFAULT 0,
            term_start_column INTEGER NOT NULL DEFAULT 4,
            term_end_column INTEGER NOT NULL DEFAULT 5,
            district_column INTEGER NOT NULL DEFAULT 0,
            dynamic_parse INTEGER NOT NULL DEFAULT 1,
            read_right_to_left INTEGER NOT NULL DEFAULT 0,
            find_date_in_infobox INTEGER NOT NULL DEFAULT 0,
            parse_rowspan INTEGER NOT NULL DEFAULT 0,
            consolidate_rowspan_terms INTEGER NOT NULL DEFAULT 0,
            rep_link INTEGER NOT NULL DEFAULT 0,
            party_link INTEGER NOT NULL DEFAULT 0,
            alt_link_include_main INTEGER NOT NULL DEFAULT 0,
            use_full_page_for_table INTEGER NOT NULL DEFAULT 0,
            years_only INTEGER NOT NULL DEFAULT 0,
            term_dates_merged INTEGER NOT NULL DEFAULT 0,
            party_ignore INTEGER NOT NULL DEFAULT 0,
            district_ignore INTEGER NOT NULL DEFAULT 0,
            district_at_large INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    sel = ", ".join(c for c in all_cols if c != "alt_link")
    conn.execute(f"INSERT INTO offices_new ({sel}) SELECT {sel} FROM offices")
    conn.execute("DROP TABLE offices")
    conn.execute("ALTER TABLE offices_new RENAME TO offices")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_country_id ON offices(country_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_state_id ON offices(state_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_level_id ON offices(level_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_offices_branch_id ON offices(branch_id)")
    conn.commit()


def _migrate_to_page_office_table_hierarchy(conn):
    """
    Add office_details_id to alt_links and office_terms; add office_table_config_id to office_terms.
    Backfill from offices into source_pages, office_details, office_table_config (1:1), then set new FKs.
    Does not drop offices or office_id columns.
    """
    alt_cols = _columns(conn, "alt_links")
    if "office_details_id" not in alt_cols:
        conn.execute(
            "ALTER TABLE alt_links ADD COLUMN office_details_id INTEGER REFERENCES office_details(id)"
        )
        conn.commit()
        # Recreate alt_links so office_id is nullable (hierarchy uses office_details_id only)
        conn.execute("""CREATE TABLE alt_links_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                office_id INTEGER REFERENCES offices(id),
                office_details_id INTEGER REFERENCES office_details(id),
                link_path TEXT NOT NULL
            )""")
        conn.execute(
            "INSERT INTO alt_links_new (id, office_id, office_details_id, link_path) SELECT id, office_id, office_details_id, link_path FROM alt_links"
        )
        conn.execute("DROP TABLE alt_links")
        conn.execute("ALTER TABLE alt_links_new RENAME TO alt_links")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alt_links_office_id ON alt_links(office_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alt_links_office_details_id ON alt_links(office_details_id)"
        )
        conn.commit()
    ot_cols = _columns(conn, "office_terms")
    if "office_details_id" not in ot_cols:
        conn.execute(
            "ALTER TABLE office_terms ADD COLUMN office_details_id INTEGER REFERENCES office_details(id)"
        )
        conn.commit()
    if "office_table_config_id" not in ot_cols:
        conn.execute(
            "ALTER TABLE office_terms ADD COLUMN office_table_config_id INTEGER REFERENCES office_table_config(id)"
        )
        conn.commit()

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alt_links_office_details_id ON alt_links(office_details_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_office_terms_office_details_id ON office_terms(office_details_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_office_terms_office_table_config_id ON office_terms(office_table_config_id)"
    )

    # Ensure office_details has variant_name and department (schema may have been created without them)
    od_cols = _columns(conn, "office_details")
    if "variant_name" not in od_cols:
        conn.execute("ALTER TABLE office_details ADD COLUMN variant_name TEXT NOT NULL DEFAULT ''")
        conn.commit()
    if "department" not in od_cols:
        conn.execute("ALTER TABLE office_details ADD COLUMN department TEXT")
        conn.commit()

    # Backfill only when offices has rows and source_pages is empty
    n_offices = conn.execute("SELECT COUNT(*) FROM offices").fetchone()[0]
    n_pages = conn.execute("SELECT COUNT(*) FROM source_pages").fetchone()[0]
    if n_offices == 0 or n_pages > 0:
        conn.commit()
        return

    offices_rows = conn.execute(
        """SELECT id, country_id, state_id, level_id, branch_id, department, name, enabled, notes, url,
                  table_no, table_rows, link_column, party_column, term_start_column, term_end_column, district_column,
                  dynamic_parse, read_right_to_left, find_date_in_infobox, parse_rowspan, consolidate_rowspan_terms,
                  rep_link, party_link, alt_link_include_main, use_full_page_for_table, years_only,
                  term_dates_merged, party_ignore, district_ignore, district_at_large, remove_duplicates, created_at
           FROM offices ORDER BY id"""
    ).fetchall()

    for o in offices_rows:
        oid = o["id"]
        conn.execute(
            """INSERT INTO source_pages (country_id, state_id, level_id, branch_id, url, notes, enabled, last_scraped_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, datetime('now'))""",
            (
                o["country_id"],
                o["state_id"] or None,
                o["level_id"] or None,
                o["branch_id"] or None,
                o["url"] or "",
                o["notes"] or "",
                1 if o["enabled"] else 0,
                o["created_at"] or None,
            ),
        )
        page_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO office_details (source_page_id, name, variant_name, department, notes, alt_link_include_main, enabled, created_at, updated_at)
               VALUES (?, ?, '', ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                page_id,
                o["name"] or "",
                o["department"] or None,
                o["notes"] or None,
                1 if o["alt_link_include_main"] else 0,
                1 if o["enabled"] else 0,
            ),
        )
        od_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO office_table_config (office_details_id, table_no, table_rows, link_column, party_column,
                  term_start_column, term_end_column, district_column, dynamic_parse, read_right_to_left, find_date_in_infobox,
                  parse_rowspan, rep_link, party_link, enabled, use_full_page_for_table, years_only,
                  term_dates_merged, party_ignore, district_ignore, district_at_large, remove_duplicates, consolidate_rowspan_terms, notes, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (
                od_id,
                int(o["table_no"] or 1),
                int(o["table_rows"] or 4),
                int(o["link_column"] or 1),
                int(o["party_column"] or 0),
                int(o["term_start_column"] or 4),
                int(o["term_end_column"] or 5),
                int(o["district_column"] or 0),
                1 if o["dynamic_parse"] else 0,
                1 if o["read_right_to_left"] else 0,
                1 if o["find_date_in_infobox"] else 0,
                1 if o["parse_rowspan"] else 0,
                1 if o["rep_link"] else 0,
                1 if o["party_link"] else 0,
                1 if o["enabled"] else 0,
                1 if o["use_full_page_for_table"] else 0,
                1 if o["years_only"] else 0,
                1 if o["term_dates_merged"] else 0,
                1 if o["party_ignore"] else 0,
                1 if o["district_ignore"] else 0,
                1 if o["district_at_large"] else 0,
                1 if o.get("remove_duplicates") else 0,
                1 if o["consolidate_rowspan_terms"] else 0,
                o["notes"] or None,
            ),
        )
        tc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("UPDATE alt_links SET office_details_id = ? WHERE office_id = ?", (od_id, oid))
        conn.execute(
            "UPDATE office_terms SET office_details_id = ?, office_table_config_id = ? WHERE office_id = ?",
            (od_id, tc_id, oid),
        )
    conn.commit()


def _migrate_allow_reuse_tables_and_table_no_unique(conn):
    """Add allow_reuse_tables to source_pages; add UNIQUE(office_details_id, table_no) on office_table_config."""
    try:
        sp_cols = _columns(conn, "source_pages")
    except sqlite3.OperationalError:
        return
    if "allow_reuse_tables" not in sp_cols:
        conn.execute(
            "ALTER TABLE source_pages ADD COLUMN allow_reuse_tables INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_office_table_config_office_table_no ON office_table_config(office_details_id, table_no)"
    )
    conn.commit()


def _migrate_office_table_config_name(conn):
    """Add name column to office_table_config for table display name in outline."""
    try:
        tc_cols = _columns(conn, "office_table_config")
    except sqlite3.OperationalError:
        return
    if "name" not in tc_cols:
        conn.execute("ALTER TABLE office_table_config ADD COLUMN name TEXT")
        conn.commit()


def _migrate_office_category(conn):
    """Create office_category and junction tables if missing; add office_category_id to office_details."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS office_category (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS office_category_countries (
            category_id INTEGER NOT NULL REFERENCES office_category(id),
            country_id INTEGER NOT NULL REFERENCES countries(id),
            PRIMARY KEY (category_id, country_id)
        );
        CREATE TABLE IF NOT EXISTS office_category_levels (
            category_id INTEGER NOT NULL REFERENCES office_category(id),
            level_id INTEGER NOT NULL REFERENCES levels(id),
            PRIMARY KEY (category_id, level_id)
        );
        CREATE TABLE IF NOT EXISTS office_category_branches (
            category_id INTEGER NOT NULL REFERENCES office_category(id),
            branch_id INTEGER NOT NULL REFERENCES branches(id),
            PRIMARY KEY (category_id, branch_id)
        );
    """)
    conn.commit()
    try:
        od_cols = _columns(conn, "office_details")
    except sqlite3.OperationalError:
        return
    if "office_category_id" not in od_cols:
        conn.execute(
            "ALTER TABLE office_details ADD COLUMN office_category_id INTEGER REFERENCES office_category(id)"
        )
        conn.commit()


def _migrate_infobox_role_key_filter(conn):
    """Create infobox_role_key_filter and junction tables if missing."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS infobox_role_key_filter (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            role_key TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS infobox_role_key_filter_countries (
            filter_id INTEGER NOT NULL REFERENCES infobox_role_key_filter(id),
            country_id INTEGER NOT NULL REFERENCES countries(id),
            PRIMARY KEY (filter_id, country_id)
        );
        CREATE TABLE IF NOT EXISTS infobox_role_key_filter_levels (
            filter_id INTEGER NOT NULL REFERENCES infobox_role_key_filter(id),
            level_id INTEGER NOT NULL REFERENCES levels(id),
            PRIMARY KEY (filter_id, level_id)
        );
        CREATE TABLE IF NOT EXISTS infobox_role_key_filter_branches (
            filter_id INTEGER NOT NULL REFERENCES infobox_role_key_filter(id),
            branch_id INTEGER NOT NULL REFERENCES branches(id),
            PRIMARY KEY (filter_id, branch_id)
        );
    """)
    conn.commit()


def _normalize_role_key(role_key: str) -> str:
    return re.sub(r"\s+", " ", (role_key or "").strip().lower())


def _migrate_office_table_config_infobox_role_key_filter_id(conn):
    """Add office_table_config.infobox_role_key_filter_id and backfill from legacy infobox_role_key when present."""
    try:
        otc_cols = _columns(conn, "office_table_config")
    except sqlite3.OperationalError:
        return

    if "infobox_role_key_filter_id" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN infobox_role_key_filter_id INTEGER REFERENCES infobox_role_key_filter(id)"
        )
        conn.commit()

    if "infobox_role_key" not in otc_cols:
        return

    rows = conn.execute("""SELECT tc.id, tc.infobox_role_key, p.country_id, p.level_id, p.branch_id
               FROM office_table_config tc
               JOIN office_details od ON od.id = tc.office_details_id
               JOIN source_pages p ON p.id = od.source_page_id
              WHERE tc.infobox_role_key_filter_id IS NULL
                AND TRIM(COALESCE(tc.infobox_role_key, '')) != ''""").fetchall()

    def _ensure_filter(
        role_key: str, country_id: int | None, level_id: int | None, branch_id: int | None
    ) -> int:
        normalized = _normalize_role_key(role_key)
        candidate_ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM infobox_role_key_filter WHERE role_key = ?",
                (normalized,),
            ).fetchall()
        ]
        for fid in candidate_ids:
            c = {
                r[0]
                for r in conn.execute(
                    "SELECT country_id FROM infobox_role_key_filter_countries WHERE filter_id = ?",
                    (fid,),
                ).fetchall()
            }
            l = {
                r[0]
                for r in conn.execute(
                    "SELECT level_id FROM infobox_role_key_filter_levels WHERE filter_id = ?",
                    (fid,),
                ).fetchall()
            }
            b = {
                r[0]
                for r in conn.execute(
                    "SELECT branch_id FROM infobox_role_key_filter_branches WHERE filter_id = ?",
                    (fid,),
                ).fetchall()
            }
            if (
                c == ({country_id} if country_id else set())
                and l == ({level_id} if level_id else set())
                and b == ({branch_id} if branch_id else set())
            ):
                return int(fid)

        scope = f"c{country_id or 0}_l{level_id or 0}_b{branch_id or 0}"
        base_name = f"{normalized}__{scope}"
        name = base_name
        suffix = 2
        while conn.execute(
            "SELECT 1 FROM infobox_role_key_filter WHERE name = ?", (name,)
        ).fetchone():
            name = f"{base_name}_{suffix}"
            suffix += 1
        conn.execute(
            "INSERT INTO infobox_role_key_filter (name, role_key) VALUES (?, ?)",
            (name, normalized),
        )
        fid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        if country_id:
            conn.execute(
                "INSERT OR IGNORE INTO infobox_role_key_filter_countries (filter_id, country_id) VALUES (?, ?)",
                (fid, country_id),
            )
        if level_id:
            conn.execute(
                "INSERT OR IGNORE INTO infobox_role_key_filter_levels (filter_id, level_id) VALUES (?, ?)",
                (fid, level_id),
            )
        if branch_id:
            conn.execute(
                "INSERT OR IGNORE INTO infobox_role_key_filter_branches (filter_id, branch_id) VALUES (?, ?)",
                (fid, branch_id),
            )
        return fid

    for tc_id, role_key, country_id, level_id, branch_id in rows:
        fid = _ensure_filter(role_key, country_id, level_id, branch_id)
        conn.execute(
            "UPDATE office_table_config SET infobox_role_key_filter_id = ? WHERE id = ?",
            (fid, tc_id),
        )
    conn.commit()


def _migrate_offices_infobox_role_key_filter_id(conn):
    """Add offices.infobox_role_key_filter_id if missing (legacy offices table)."""
    try:
        offices_cols = _columns(conn, "offices")
    except sqlite3.OperationalError:
        return
    if "infobox_role_key_filter_id" not in offices_cols:
        conn.execute(
            "ALTER TABLE offices ADD COLUMN infobox_role_key_filter_id INTEGER REFERENCES infobox_role_key_filter(id)"
        )
        conn.commit()


def _migrate_infobox_role_key_filter_role_key_format(conn):
    """Repair legacy migrated role_key expressions that were normalized with underscores.

    Early migrations converted whitespace to underscores, which breaks quoted include/exclude
    parsing for expressions such as "associate justice" -"chief justice".
    """
    try:
        rows = conn.execute(
            "SELECT id, role_key FROM infobox_role_key_filter "
            "WHERE INSTR(role_key, CHAR(34)) > 0 AND INSTR(role_key, '_') > 0"
        ).fetchall()
    except sqlite3.OperationalError:
        return
    if not rows:
        return

    changed = False
    for fid, role_key in rows:
        original = (role_key or "").strip()
        if not original:
            continue
        fixed = re.sub(r"\s+", " ", original.replace("_", " ")).strip()
        if fixed and fixed != original:
            conn.execute(
                "UPDATE infobox_role_key_filter SET role_key = ? WHERE id = ?", (fixed, int(fid))
            )
            changed = True
    if changed:
        conn.commit()


def _migrate_city(conn):
    """Create cities table if missing; add city_id to source_pages if missing."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_id INTEGER NOT NULL REFERENCES states(id),
            name TEXT NOT NULL,
            UNIQUE(state_id, name)
        );
        CREATE INDEX IF NOT EXISTS idx_cities_state_id ON cities(state_id);
    """)
    conn.commit()
    try:
        sp_cols = _columns(conn, "source_pages")
    except sqlite3.OperationalError:
        return
    if "city_id" not in sp_cols:
        conn.execute("ALTER TABLE source_pages ADD COLUMN city_id INTEGER REFERENCES cities(id)")
        conn.commit()
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_pages_city_id ON source_pages(city_id)")
        conn.commit()
    except sqlite3.OperationalError:
        pass


def _migrate_ignore_non_links(conn):
    """Add ignore_non_links to offices and office_table_config if missing."""
    offices_cols = _columns(conn, "offices")
    if "ignore_non_links" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN ignore_non_links INTEGER NOT NULL DEFAULT 0")
    otc_cols = _columns(conn, "office_table_config")
    if "ignore_non_links" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN ignore_non_links INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()


def _migrate_remove_duplicates(conn):
    """Add remove_duplicates to offices and office_table_config if missing."""
    offices_cols = _columns(conn, "offices")
    if "remove_duplicates" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN remove_duplicates INTEGER NOT NULL DEFAULT 0")
    otc_cols = _columns(conn, "office_table_config")
    if "remove_duplicates" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN remove_duplicates INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()


def _migrate_row_filter_columns(conn):
    """Add optional row filter columns to offices and office_table_config if missing."""
    offices_cols = _columns(conn, "offices")
    if "filter_column" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN filter_column INTEGER NOT NULL DEFAULT 0")
    if "filter_criteria" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN filter_criteria TEXT NOT NULL DEFAULT ''")

    otc_cols = _columns(conn, "office_table_config")
    if "filter_column" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN filter_column INTEGER NOT NULL DEFAULT 0"
        )
    if "filter_criteria" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN filter_criteria TEXT NOT NULL DEFAULT ''"
        )
    conn.commit()


def _migrate_infobox_role_key(conn):
    """Add infobox_role_key to office_table_config if missing."""
    try:
        otc_cols = _columns(conn, "office_table_config")
    except sqlite3.OperationalError:
        return
    changed = False
    if "infobox_role_key" not in otc_cols:
        conn.execute(
            "ALTER TABLE office_table_config ADD COLUMN infobox_role_key TEXT NOT NULL DEFAULT ''"
        )
        changed = True
    offices_cols = _columns(conn, "offices")
    if "infobox_role_key" not in offices_cols:
        conn.execute("ALTER TABLE offices ADD COLUMN infobox_role_key TEXT NOT NULL DEFAULT ''")
        changed = True
    if changed:
        conn.commit()


def _migrate_office_table_config_html_hash(conn):
    """Add last_html_hash to office_table_config if missing."""
    try:
        cols = _columns(conn, "office_table_config")
    except sqlite3.OperationalError:
        return
    if "last_html_hash" not in cols:
        conn.execute("ALTER TABLE office_table_config ADD COLUMN last_html_hash TEXT")
        conn.commit()


def _migrate_individuals_bio_batch(conn):
    """Add bio_batch (0-6) and bio_refreshed_at to individuals if missing."""
    try:
        cols = _columns(conn, "individuals")
    except sqlite3.OperationalError:
        return
    if "bio_batch" not in cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN bio_batch INTEGER NOT NULL DEFAULT 0")
        conn.execute("UPDATE individuals SET bio_batch = id % 7")
        conn.commit()
    if "bio_refreshed_at" not in cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN bio_refreshed_at TEXT")
        conn.commit()
