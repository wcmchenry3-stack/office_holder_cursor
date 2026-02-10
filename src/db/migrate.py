"""Migration: ensure ref tables exist and offices/parties use FK columns."""

from .connection import get_connection


def _columns(conn, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def migrate_to_fk(conn=None):
    """
    If offices/parties have old text columns (country, level, branch, state),
    add FK columns, seed ref data, backfill, then replace tables with FK-only structure.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        # Ensure ref tables exist (schema already created them)
        from .seed import seed_reference_data
        seed_reference_data(conn=conn)

        offices_cols = _columns(conn, "offices")
        parties_cols = _columns(conn, "parties")

        # New schema has country_id; old has country (text)
        offices_has_fk = "country_id" in offices_cols
        parties_has_fk = "country_id" in parties_cols

        if not offices_has_fk and "country" in offices_cols:
            _migrate_offices_to_fk(conn)
        if not parties_has_fk and "country" in parties_cols:
            _migrate_parties_to_fk(conn)

        # office_terms: add party_id FK if missing and backfill from party text
        ot_cols = _columns(conn, "office_terms")
        if "party_id" not in ot_cols:
            _migrate_office_terms_party_id(conn)
        # office_terms: drop party column if present (use party_id only)
        ot_cols = _columns(conn, "office_terms")
        if "party" in ot_cols:
            _migrate_office_terms_drop_party(conn)

        # Add imprecise-date columns to individuals and office_terms if missing
        _migrate_imprecise_date_columns(conn)
        # Add enabled column to offices if missing
        _migrate_offices_enabled(conn)
        # Add use_full_page_for_table to offices if missing
        _migrate_offices_use_full_page_for_table(conn)
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_office_terms_individual_id ON office_terms(individual_id)")
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
        conn.execute("ALTER TABLE offices ADD COLUMN use_full_page_for_table INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def _migrate_imprecise_date_columns(conn):
    """Add birth_date_imprecise, death_date_imprecise to individuals; term_start_imprecise, term_end_imprecise to office_terms. Backfill: set date to null and flag to 1 where date is not YYYY-MM-DD."""
    import re
    valid_date = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    ind_cols = _columns(conn, "individuals")
    if "birth_date_imprecise" not in ind_cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN birth_date_imprecise INTEGER NOT NULL DEFAULT 0")
    if "death_date_imprecise" not in ind_cols:
        conn.execute("ALTER TABLE individuals ADD COLUMN death_date_imprecise INTEGER NOT NULL DEFAULT 0")
    # Backfill individuals: invalid date -> null + imprecise 1
    for row in conn.execute("SELECT id, birth_date, death_date FROM individuals").fetchall():
        rid, bd, dd = row["id"], row["birth_date"], row["death_date"]
        if bd is not None and not valid_date.match(str(bd).strip()):
            conn.execute("UPDATE individuals SET birth_date = NULL, birth_date_imprecise = 1 WHERE id = ?", (rid,))
        if dd is not None and not valid_date.match(str(dd).strip()):
            conn.execute("UPDATE individuals SET death_date = NULL, death_date_imprecise = 1 WHERE id = ?", (rid,))
    ot_cols = _columns(conn, "office_terms")
    if "term_start_imprecise" not in ot_cols:
        conn.execute("ALTER TABLE office_terms ADD COLUMN term_start_imprecise INTEGER NOT NULL DEFAULT 0")
    if "term_end_imprecise" not in ot_cols:
        conn.execute("ALTER TABLE office_terms ADD COLUMN term_end_imprecise INTEGER NOT NULL DEFAULT 0")
    for row in conn.execute("SELECT id, term_start, term_end FROM office_terms").fetchall():
        oid, ts, te = row["id"], row["term_start"], row["term_end"]
        if ts is not None and not valid_date.match(str(ts).strip()):
            conn.execute("UPDATE office_terms SET term_start = NULL, term_start_imprecise = 1 WHERE id = ?", (oid,))
        if te is not None and not valid_date.match(str(te).strip()):
            conn.execute("UPDATE office_terms SET term_end = NULL, term_end_imprecise = 1 WHERE id = ?", (oid,))
    conn.commit()
