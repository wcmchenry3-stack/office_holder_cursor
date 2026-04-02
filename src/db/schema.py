"""Database schema for office_holder.

Two schemas are maintained in sync:
  • SCHEMA_SQL      — SQLite syntax, used by test fixtures only
  • SCHEMA_PG_SQL   — PostgreSQL syntax, used by the production Render app

IMPORTANT: When adding a column or table, update BOTH constants and add a
_run_pg_migrations() entry in connection.py for the live PostgreSQL database.
The test_schema_sync.py suite will fail CI if the two schemas drift.
"""

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

-- Reference: cities (per state, state implies country)
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
    bio_batch INTEGER NOT NULL DEFAULT 0,
    bio_refreshed_at TEXT,
    insufficient_vitals_checked_at TEXT,
    gemini_research_checked_at TEXT,
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
    url TEXT NOT NULL UNIQUE,
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
    office_category_id INTEGER REFERENCES office_category(id),
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
    infobox_role_key TEXT NOT NULL DEFAULT '',
    notes TEXT,
    name TEXT,
    last_html_hash TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_office_table_config_office_details_id ON office_table_config(office_details_id);
CREATE INDEX IF NOT EXISTS idx_office_table_config_enabled ON office_table_config(enabled);
CREATE UNIQUE INDEX IF NOT EXISTS idx_office_table_config_office_table_no ON office_table_config(office_details_id, table_no);

-- Offices: office definitions (what we scrape) — link by FK to countries, states, levels, branches (legacy, migrated to hierarchy)
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
    infobox_role_key_filter_id INTEGER REFERENCES infobox_role_key_filter(id),
    created_at TEXT DEFAULT (datetime('now'))
);

-- Alt links: one row per office alternate infobox link (offices may have many)
-- office_id is nullable: hierarchy entries use office_details_id and leave office_id NULL
CREATE TABLE IF NOT EXISTS alt_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_id INTEGER REFERENCES offices(id),
    office_details_id INTEGER REFERENCES office_details(id),
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

-- Office terms: one row per scraped term
CREATE TABLE IF NOT EXISTS office_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    office_id INTEGER NOT NULL,
    office_details_id INTEGER REFERENCES office_details(id),
    office_table_config_id INTEGER REFERENCES office_table_config(id),
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
CREATE INDEX IF NOT EXISTS idx_individuals_insuf_vitals_checked_at ON individuals(insufficient_vitals_checked_at);

-- Parser test scripts
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
);
CREATE INDEX IF NOT EXISTS idx_parser_test_scripts_enabled ON parser_test_scripts(enabled);

-- Persistent job records: run_scraper and preview jobs survive server restart.
CREATE TABLE IF NOT EXISTS scraper_jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    queued_at TEXT,
    job_params_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    result_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_status ON scraper_jobs(status);
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_created_at ON scraper_jobs(created_at);

-- Parse error reports: one record per distinct (function, error_type, wiki_url) fingerprint.
-- Used by ParseErrorReporter to deduplicate GitHub issue creation across runs.
CREATE TABLE IF NOT EXISTS parse_error_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    function_name TEXT NOT NULL,
    error_type TEXT NOT NULL,
    wiki_url TEXT,
    office_name TEXT,
    github_issue_url TEXT,
    github_issue_number INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_parse_error_reports_fingerprint ON parse_error_reports(fingerprint);

-- Research sources found by Gemini vitals research
CREATE TABLE IF NOT EXISTS individual_research_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    individual_id INTEGER NOT NULL REFERENCES individuals(id),
    source_url TEXT NOT NULL,
    source_type TEXT,
    found_data_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_individual_research_sources_individual_id ON individual_research_sources(individual_id);

-- Wiki draft proposals for human review
CREATE TABLE IF NOT EXISTS wiki_draft_proposals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    individual_id INTEGER NOT NULL REFERENCES individuals(id),
    proposal_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_wiki_draft_proposals_individual_id ON wiki_draft_proposals(individual_id);
CREATE INDEX IF NOT EXISTS idx_wiki_draft_proposals_status ON wiki_draft_proposals(status);

-- Reference documents: cached external content (e.g. Wikipedia Manual of Style)
CREATE TABLE IF NOT EXISTS reference_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_key TEXT NOT NULL UNIQUE,
    content TEXT NOT NULL,
    fetched_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes on offices/parties/office_terms FK columns
CREATE INDEX IF NOT EXISTS idx_offices_country_id ON offices(country_id);
CREATE INDEX IF NOT EXISTS idx_offices_state_id ON offices(state_id);
CREATE INDEX IF NOT EXISTS idx_offices_level_id ON offices(level_id);
CREATE INDEX IF NOT EXISTS idx_offices_branch_id ON offices(branch_id);
CREATE INDEX IF NOT EXISTS idx_parties_country_id ON parties(country_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id);

-- schema_migrations: tracks applied PostgreSQL-only corrections (used by _run_pg_migrations)
CREATE TABLE IF NOT EXISTS schema_migrations (
    id TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Kept as a separate constant so _init_sqlite() and _init_postgres() can both reference it.
# For SQLite (tests) these are now embedded in SCHEMA_SQL above; this constant is still used
# by _init_postgres() which passes it separately after the main schema.
OFFICES_PARTIES_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_offices_country_id ON offices(country_id);
CREATE INDEX IF NOT EXISTS idx_offices_state_id ON offices(state_id);
CREATE INDEX IF NOT EXISTS idx_offices_level_id ON offices(level_id);
CREATE INDEX IF NOT EXISTS idx_offices_branch_id ON offices(branch_id);
CREATE INDEX IF NOT EXISTS idx_parties_country_id ON parties(country_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_party_id ON office_terms(party_id);
"""

# ---------------------------------------------------------------------------
# PostgreSQL schema — production schema used by _init_postgres() in connection.py.
# Differences from SCHEMA_SQL:
#   • SERIAL PRIMARY KEY instead of INTEGER PRIMARY KEY AUTOINCREMENT
#   • TIMESTAMPTZ DEFAULT NOW() instead of TEXT DEFAULT (datetime('now'))
# ---------------------------------------------------------------------------
SCHEMA_PG_SQL = """
-- Reference: countries
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Reference: states/provinces/territories (per country)
CREATE TABLE IF NOT EXISTS states (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    name TEXT NOT NULL,
    UNIQUE(country_id, name)
);
CREATE INDEX IF NOT EXISTS idx_states_country_id ON states(country_id);

-- Reference: cities (per state, state implies country)
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    state_id INTEGER NOT NULL REFERENCES states(id),
    name TEXT NOT NULL,
    UNIQUE(state_id, name)
);
CREATE INDEX IF NOT EXISTS idx_cities_state_id ON cities(state_id);

-- Reference: level (federal, state, local)
CREATE TABLE IF NOT EXISTS levels (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Reference: branch (executive, legislative, judicial)
CREATE TABLE IF NOT EXISTS branches (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Office category
CREATE TABLE IF NOT EXISTS office_category (
    id SERIAL PRIMARY KEY,
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

-- Infobox role key filter
CREATE TABLE IF NOT EXISTS infobox_role_key_filter (
    id SERIAL PRIMARY KEY,
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

-- Individuals
CREATE TABLE IF NOT EXISTS individuals (
    id SERIAL PRIMARY KEY,
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
    bio_batch INTEGER NOT NULL DEFAULT 0,
    bio_refreshed_at TIMESTAMPTZ,
    insufficient_vitals_checked_at TIMESTAMPTZ,
    gemini_research_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Source pages
CREATE TABLE IF NOT EXISTS source_pages (
    id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES countries(id),
    state_id INTEGER REFERENCES states(id),
    city_id INTEGER REFERENCES cities(id),
    level_id INTEGER REFERENCES levels(id),
    branch_id INTEGER REFERENCES branches(id),
    url TEXT NOT NULL UNIQUE,
    notes TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    allow_reuse_tables INTEGER NOT NULL DEFAULT 0,
    disable_auto_table_update INTEGER NOT NULL DEFAULT 0,
    last_scraped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_source_pages_country_id ON source_pages(country_id);
CREATE INDEX IF NOT EXISTS idx_source_pages_enabled ON source_pages(enabled);

-- Office details
CREATE TABLE IF NOT EXISTS office_details (
    id SERIAL PRIMARY KEY,
    source_page_id INTEGER NOT NULL REFERENCES source_pages(id),
    name TEXT NOT NULL,
    variant_name TEXT NOT NULL DEFAULT '',
    department TEXT,
    notes TEXT,
    alt_link_include_main INTEGER NOT NULL DEFAULT 0,
    office_category_id INTEGER REFERENCES office_category(id),
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_office_details_source_page_id ON office_details(source_page_id);
CREATE INDEX IF NOT EXISTS idx_office_details_enabled ON office_details(enabled);

-- Office table config
CREATE TABLE IF NOT EXISTS office_table_config (
    id SERIAL PRIMARY KEY,
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
    infobox_role_key TEXT NOT NULL DEFAULT '',
    notes TEXT,
    name TEXT,
    last_html_hash TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_office_table_config_office_details_id ON office_table_config(office_details_id);
CREATE INDEX IF NOT EXISTS idx_office_table_config_enabled ON office_table_config(enabled);
CREATE UNIQUE INDEX IF NOT EXISTS idx_office_table_config_office_table_no ON office_table_config(office_details_id, table_no);

-- Offices (legacy — new data goes through source_pages → office_details → office_table_config)
CREATE TABLE IF NOT EXISTS offices (
    id SERIAL PRIMARY KEY,
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
    infobox_role_key_filter_id INTEGER REFERENCES infobox_role_key_filter(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Alt links
-- office_id is nullable: hierarchy entries use office_details_id and leave office_id NULL
CREATE TABLE IF NOT EXISTS alt_links (
    id SERIAL PRIMARY KEY,
    office_id INTEGER REFERENCES offices(id),
    office_details_id INTEGER REFERENCES office_details(id),
    link_path TEXT NOT NULL,
    UNIQUE(office_id, link_path)
);
CREATE INDEX IF NOT EXISTS idx_alt_links_office_id ON alt_links(office_id);

-- Parties
CREATE TABLE IF NOT EXISTS parties (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    party_name TEXT NOT NULL,
    party_link TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Office terms
CREATE TABLE IF NOT EXISTS office_terms (
    id SERIAL PRIMARY KEY,
    office_id INTEGER NOT NULL,
    office_details_id INTEGER REFERENCES office_details(id),
    office_table_config_id INTEGER REFERENCES office_table_config(id),
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
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(office_id, wiki_url, term_start, term_end, term_start_year, term_end_year)
);
CREATE INDEX IF NOT EXISTS idx_office_terms_office_id ON office_terms(office_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_individual_id ON office_terms(individual_id);
CREATE INDEX IF NOT EXISTS idx_office_terms_wiki_url ON office_terms(wiki_url);
-- idx_individuals_insuf_vitals_checked_at is created via _run_pg_migrations (pg migration)
-- so that ALTER TABLE ADD COLUMN runs first on pre-existing databases.

-- Parser test scripts
CREATE TABLE IF NOT EXISTS parser_test_scripts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    test_type TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    html_file TEXT NOT NULL,
    source_url TEXT,
    config_json TEXT NOT NULL,
    expected_json TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_parser_test_scripts_enabled ON parser_test_scripts(enabled);

-- Persistent job records: run_scraper and preview jobs survive server restart.
CREATE TABLE IF NOT EXISTS scraper_jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    queued_at TIMESTAMPTZ,
    job_params_json TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    result_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_status ON scraper_jobs(status);
CREATE INDEX IF NOT EXISTS idx_scraper_jobs_created_at ON scraper_jobs(created_at);

-- Parse error reports: one record per distinct (function, error_type, wiki_url) fingerprint.
-- Used by ParseErrorReporter to deduplicate GitHub issue creation across runs.
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
);
CREATE INDEX IF NOT EXISTS idx_parse_error_reports_fingerprint ON parse_error_reports(fingerprint);

-- Research sources found by Gemini vitals research
CREATE TABLE IF NOT EXISTS individual_research_sources (
    id SERIAL PRIMARY KEY,
    individual_id INTEGER NOT NULL REFERENCES individuals(id),
    source_url TEXT NOT NULL,
    source_type TEXT,
    found_data_json TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_individual_research_sources_individual_id ON individual_research_sources(individual_id);

-- Wiki draft proposals for human review
CREATE TABLE IF NOT EXISTS wiki_draft_proposals (
    id SERIAL PRIMARY KEY,
    individual_id INTEGER NOT NULL REFERENCES individuals(id),
    proposal_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wiki_draft_proposals_individual_id ON wiki_draft_proposals(individual_id);
CREATE INDEX IF NOT EXISTS idx_wiki_draft_proposals_status ON wiki_draft_proposals(status);

-- Reference documents: cached external content (e.g. Wikipedia Manual of Style)
CREATE TABLE IF NOT EXISTS reference_documents (
    id SERIAL PRIMARY KEY,
    doc_key TEXT NOT NULL UNIQUE,
    content TEXT NOT NULL,
    fetched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Same index SQL works for both backends (standard SQL).
OFFICES_PARTIES_INDEX_PG_SQL = OFFICES_PARTIES_INDEX_SQL
