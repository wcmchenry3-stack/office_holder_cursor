"""SQLite schema for office_holder database."""

SCHEMA_SQL = """
-- Reference: countries
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

-- Reference: states/provinces/territories (per country)
CREATE TABLE IF NOT EXISTS states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    name TEXT NOT NULL,
    UNIQUE(country_id, name)
);
CREATE INDEX IF NOT EXISTS idx_states_country_id ON states(country_id);

-- Reference: level (federal, state, local)
CREATE TABLE IF NOT EXISTS levels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

-- Reference: branch (executive, legislative, judicial)
CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

-- Individuals: one row per person (keyed by Wikipedia URL)
CREATE TABLE IF NOT EXISTS individuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wiki_url TEXT NOT NULL UNIQUE,
    page_path TEXT,
    full_name TEXT,
    birth_date TEXT,
    death_date TEXT,
    birth_date_imprecise INTEGER NOT NULL DEFAULT 0,
    death_date_imprecise INTEGER NOT NULL DEFAULT 0,
    birth_place TEXT,
    death_place TEXT,
    is_dead_link INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Offices: office definitions (what we scrape); link by FK to countries, states, levels, branches
CREATE TABLE IF NOT EXISTS offices (
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
    alt_link_include_main INTEGER NOT NULL DEFAULT 0,
    use_full_page_for_table INTEGER NOT NULL DEFAULT 0,
    years_only INTEGER NOT NULL DEFAULT 0,
    term_dates_merged INTEGER NOT NULL DEFAULT 0,
    party_ignore INTEGER NOT NULL DEFAULT 0,
    district_ignore INTEGER NOT NULL DEFAULT 0,
    district_at_large INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Alt links: one row per office alternate infobox link (offices may have many)
CREATE TABLE IF NOT EXISTS alt_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_id INTEGER NOT NULL REFERENCES offices(id),
    link_path TEXT NOT NULL,
    UNIQUE(office_id, link_path)
);
CREATE INDEX IF NOT EXISTS idx_alt_links_office_id ON alt_links(office_id);

-- Parties: party list for resolving party links (before office_terms for FK)
CREATE TABLE IF NOT EXISTS parties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    party_name TEXT NOT NULL,
    party_link TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Office terms: one row per scraped term (party via party_id FK only)
CREATE TABLE IF NOT EXISTS office_terms (
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
);

CREATE INDEX IF NOT EXISTS idx_office_terms_office_id ON office_terms(office_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_individual_id ON office_terms(individual_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_wiki_url ON office_terms(wiki_url);
"""

# Indexes on offices/parties/office_terms FK columns. Not in SCHEMA_SQL so that
# existing DBs with old schema don't fail; applied after migrate_to_fk() in init_db().
OFFICES_PARTIES_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_offices_country_id ON offices(country_id);
CREATE INDEX IF NOT EXISTS idx_offices_state_id ON offices(state_id);
CREATE INDEX IF NOT EXISTS idx_offices_level_id ON offices(level_id);
CREATE INDEX IF NOT EXISTS idx_offices_branch_id ON offices(branch_id);
CREATE INDEX IF NOT EXISTS idx_parties_country_id ON parties(country_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id);
"""
