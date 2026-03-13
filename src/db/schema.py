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

-- Reference: cities (per state; state implies country)
CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_id INTEGER NOT NULL REFERENCES states(id),
    name TEXT NOT NULL,
    UNIQUE(state_id, name)
);
CREATE INDEX IF NOT EXISTS idx_cities_state_id ON cities(state_id);

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

-- Office category: optional label per office; scoped by country/level/branch (empty = all)
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

-- Infobox role key filter: optional label per infobox role key; scoped by country/level/branch (empty = all)
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
    is_living INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Source pages: Wikipedia page (one per URL; country/state/level/branch/city from refs)
CREATE TABLE IF NOT EXISTS source_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER REFERENCES countries(id),
    state_id INTEGER REFERENCES states(id),
    city_id INTEGER REFERENCES cities(id),
    level_id INTEGER REFERENCES levels(id),
    branch_id INTEGER REFERENCES branches(id),
    url TEXT NOT NULL,
    notes TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    allow_reuse_tables INTEGER NOT NULL DEFAULT 0,
    disable_auto_table_update INTEGER NOT NULL DEFAULT 0,
    last_scraped_at TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_source_pages_country_id ON source_pages(country_id);
CREATE INDEX IF NOT EXISTS idx_source_pages_enabled ON source_pages(enabled);

-- Office details: logical office on a page (name, variant, alt_link behavior)
CREATE TABLE IF NOT EXISTS office_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_page_id INTEGER NOT NULL REFERENCES source_pages(id),
    name TEXT NOT NULL,
    variant_name TEXT NOT NULL DEFAULT '',
    department TEXT,
    notes TEXT,
    alt_link_include_main INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_office_details_source_page_id ON office_details(source_page_id);
CREATE INDEX IF NOT EXISTS idx_office_details_enabled ON office_details(enabled);

-- Office table config: one table's parsing config per office
CREATE TABLE IF NOT EXISTS office_table_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_details_id INTEGER NOT NULL REFERENCES office_details(id),
    table_no INTEGER NOT NULL DEFAULT 1,
    table_rows INTEGER NOT NULL DEFAULT 4,
    link_column INTEGER NOT NULL DEFAULT 1,
    party_column INTEGER NOT NULL DEFAULT 0,
    term_start_column INTEGER NOT NULL DEFAULT 4,
    term_end_column INTEGER NOT NULL DEFAULT 5,
    district_column INTEGER NOT NULL DEFAULT 0,
    filter_column INTEGER NOT NULL DEFAULT 0,
    filter_criteria TEXT NOT NULL DEFAULT '',
    dynamic_parse INTEGER NOT NULL DEFAULT 1,
    read_right_to_left INTEGER NOT NULL DEFAULT 0,
    find_date_in_infobox INTEGER NOT NULL DEFAULT 0,
    parse_rowspan INTEGER NOT NULL DEFAULT 0,
    rep_link INTEGER NOT NULL DEFAULT 0,
    party_link INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    use_full_page_for_table INTEGER NOT NULL DEFAULT 0,
    years_only INTEGER NOT NULL DEFAULT 0,
    term_dates_merged INTEGER NOT NULL DEFAULT 0,
    party_ignore INTEGER NOT NULL DEFAULT 0,
    district_ignore INTEGER NOT NULL DEFAULT 0,
    district_at_large INTEGER NOT NULL DEFAULT 0,
    ignore_non_links INTEGER NOT NULL DEFAULT 0,
    remove_duplicates INTEGER NOT NULL DEFAULT 0,
    consolidate_rowspan_terms INTEGER NOT NULL DEFAULT 0,
    infobox_role_key_filter_id INTEGER REFERENCES infobox_role_key_filter(id),
    notes TEXT,
    name TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_office_table_config_office_details_id ON office_table_config(office_details_id);
CREATE INDEX IF NOT EXISTS idx_office_table_config_enabled ON office_table_config(enabled);
CREATE UNIQUE INDEX IF NOT EXISTS idx_office_table_config_office_table_no ON office_table_config(office_details_id, table_no);

-- Offices: office definitions (what we scrape); link by FK to countries, states, levels, branches (legacy; migrated to hierarchy)
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
    filter_column INTEGER NOT NULL DEFAULT 0,
    filter_criteria TEXT NOT NULL DEFAULT '',
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
    ignore_non_links INTEGER NOT NULL DEFAULT 0,
    remove_duplicates INTEGER NOT NULL DEFAULT 0,
    infobox_role_key TEXT NOT NULL DEFAULT '',
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
