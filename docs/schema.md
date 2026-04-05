# Database Schema Reference

SQLite database. Schema defined in `src/db/schema.py` (`SCHEMA_SQL`). Migrations in `src/db/migrate.py`.

---

## Relationship Diagram

```
countries ──┬── states ── cities
            │
            ├── source_pages ── office_details ──┬── office_table_config ──→ infobox_role_key_filter
            │                       │            └── alt_links
            │                   (office_category)
            │
            ├── parties
            │
            └── (via office_terms)
                    ↓
              office_terms ──→ office_table_config (via office_id)
                    ↓
              individuals    parties
```

**Key:** `office_terms.office_id` stores the `office_table_config.id` value in hierarchy mode.

---

## Reference Tables

### `countries`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `name` | TEXT UNIQUE | e.g. "United States" |

### `states`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `country_id` | INTEGER FK → countries | |
| `name` | TEXT | e.g. "Alaska"; UNIQUE per country |

### `cities`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `state_id` | INTEGER FK → states | |
| `name` | TEXT | UNIQUE per state |

### `levels`
Seeded values: Federal, State, Local.

### `branches`
Seeded values: Executive, Legislative, Judicial.

---

## Hierarchy Tables (Modern)

### `source_pages`
One row per Wikipedia URL.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `country_id` | FK → countries | |
| `state_id` | FK → states | Nullable |
| `city_id` | FK → cities | Nullable |
| `level_id` | FK → levels | Nullable |
| `branch_id` | FK → branches | Nullable |
| `url` | TEXT | Wikipedia page URL |
| `notes` | TEXT | |
| `enabled` | INTEGER | 0/1; default 1 |
| `allow_reuse_tables` | INTEGER | Allow multiple offices to share the same `table_no` |
| `disable_auto_table_update` | INTEGER | Skip auto-table-update algorithm for this page |
| `last_scraped_at` | TEXT | ISO datetime |
| `last_quality_checked_at` | TEXT/TIMESTAMPTZ | Set after each page quality inspection; used by `pick_next_page()` for LRU selection |

### `office_details`
One row per logical office on a page (e.g. "Governor of Alaska").

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `source_page_id` | FK → source_pages | |
| `name` | TEXT | Office name |
| `variant_name` | TEXT | Variant label (e.g. "Lieutenant Governor") |
| `department` | TEXT | Nullable |
| `notes` | TEXT | |
| `alt_link_include_main` | INTEGER | Include main page link in alt link lookups |
| `enabled` | INTEGER | 0/1 |

### `office_table_config`
One row per HTML table being parsed for an office.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `office_details_id` | FK → office_details | |
| `table_no` | INTEGER | 1-based table index on the page; UNIQUE per office |
| `name` | TEXT | Optional label for this table config |
| `notes` | TEXT | |
| ... | | See `docs/config-options.md` for all parsing flags |
| `infobox_role_key_filter_id` | FK → infobox_role_key_filter | Nullable |

**Unique constraint:** `(office_details_id, table_no)` — one config per table per office.

---

### `alt_links`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `office_details_id` | FK → office_details (NOT NULL) | |
| `link_path` | TEXT | Wikipedia path (e.g. `/wiki/John_Smith`) |
| | | UNIQUE(office_details_id, link_path) |

Alt links are alternate Wikipedia URLs associated with an office — used to look up infobox data from different pages than the main office holder link.

---

## People & Terms

### `individuals`
One row per person, keyed by `wiki_url`.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `wiki_url` | TEXT UNIQUE | Full Wikipedia URL |
| `page_path` | TEXT | `/wiki/<title>` path component |
| `full_name` | TEXT | From infobox |
| `birth_date` | TEXT | `YYYY-MM-DD` or NULL if imprecise |
| `death_date` | TEXT | `YYYY-MM-DD` or NULL if imprecise/living |
| `birth_date_imprecise` | INTEGER | 1 if birth date can't be resolved to YYYY-MM-DD |
| `death_date_imprecise` | INTEGER | 1 if death date can't be resolved to YYYY-MM-DD |
| `birth_place` | TEXT | |
| `death_place` | TEXT | |
| `is_dead_link` | INTEGER | 1 if Wikipedia link is a dead/red link |
| `is_living` | INTEGER | 1 if person believed to be living |
| `insufficient_vitals_checked_at` | TEXT/TIMESTAMPTZ | Cooldown timestamp for the insufficient vitals job |
| `gemini_research_checked_at` | TEXT/TIMESTAMPTZ | 90-day cooldown for Gemini deep research (Feature C) |
| `superseded_by_individual_id` | INTEGER FK → individuals | Set when a no-link placeholder is retired; points to the linked replacement |

### `office_terms`
One row per scraped term (a person holding an office for a period).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `office_id` | INTEGER (office_table_config.id in hierarchy mode) | Scraper output key |
| `individual_id` | FK → individuals | Nullable (if no wiki link) |
| `party_id` | FK → parties | Nullable |
| `district` | TEXT | |
| `term_start` | TEXT | `YYYY-MM-DD` or NULL |
| `term_end` | TEXT | `YYYY-MM-DD` or NULL |
| `term_start_year` | INTEGER | Year only (when `years_only=True`) |
| `term_end_year` | INTEGER | Year only (when `years_only=True`) |
| `term_start_imprecise` | INTEGER | 1 if start date is not YYYY-MM-DD |
| `term_end_imprecise` | INTEGER | 1 if end date is not YYYY-MM-DD |
| `wiki_url` | TEXT | Raw Wikipedia URL from table (may differ from `individuals.wiki_url` by case/encoding) |

**Unique constraint:** `(office_id, wiki_url, term_start, term_end, term_start_year, term_end_year)` — prevents exact duplicate terms. This is the deduplication key used by INSERT OR IGNORE.

### `parties`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `country_id` | FK → countries | Party is scoped to a country |
| `party_name` | TEXT | Display name |
| `party_link` | TEXT | Wikipedia path (for matching links found in tables) |

---

## Category & Filter Tables

### `office_category`
Optional label that can be assigned to `office_details`. Used to group offices for category-scoped runs.

### `infobox_role_key_filter`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `name` | TEXT UNIQUE | Display name |
| `role_key` | TEXT | Query string; see `docs/config-options.md` for syntax |

Each filter can be scoped to specific countries/levels/branches via `infobox_role_key_filter_countries`, `_levels`, `_branches` junction tables.

---

## Feature C Tables (Gemini Vitals Research)

### `individual_research_sources`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `individual_id` | INTEGER FK → individuals | |
| `source_url` | TEXT NOT NULL | URL of the research source |
| `source_type` | TEXT | government, academic, genealogical, news, other |
| `found_data_json` | TEXT | JSON with birth_date, death_date, notes |
| `origin` | TEXT DEFAULT 'manual' | `'manual'` = interactive UI; `'pipeline'` = automated research job |
| `created_at` | TIMESTAMP | |

### `wiki_draft_proposals`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `individual_id` | INTEGER FK → individuals | |
| `proposal_text` | TEXT NOT NULL | Wikitext article draft |
| `status` | TEXT DEFAULT 'pending' | pending, submitted, published, rejected |
| `origin` | TEXT DEFAULT 'manual' | `'manual'` = interactive UI; `'pipeline'` = automated research job |
| `created_at` | TIMESTAMP | |

### `reference_documents`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `doc_key` | TEXT UNIQUE | e.g. 'wikipedia_mos' |
| `content` | TEXT NOT NULL | Cached document content |
| `fetched_at` | TIMESTAMP | When content was last fetched/refreshed |
| `created_at` | TIMESTAMP | |
| `updated_at` | TIMESTAMP | |

---

## Quality & AI Tables

### `data_quality_reports`
Fingerprinted record-level data quality flags. Prevents duplicate issue creation across runs.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `fingerprint` | TEXT UNIQUE | Hash of `(record_type, record_id, check_type)` |
| `record_type` | TEXT | e.g. `'individual'` |
| `record_id` | INTEGER | FK to the flagged record |
| `check_type` | TEXT | e.g. `'name_check'` |
| `flagged_by` | TEXT | `'openai'`, `'gemini'`, `'claude'`, or `'deterministic'` |
| `concern_details` | TEXT | Human-readable concern description |
| `github_issue_url` | TEXT | URL of created GitHub issue (if any) |
| `github_issue_number` | INTEGER | |
| `created_at` | TEXT/TIMESTAMPTZ | |

### `parse_error_reports`
Fingerprinted parser error deduplication. Prevents creating a new GitHub issue every run for the same recurring parser failure.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `fingerprint` | TEXT UNIQUE | Hash of `(function_name, error_type, wiki_url, office_name)` |
| `function_name` | TEXT | Parser function where the error occurred |
| `error_type` | TEXT | Exception class name |
| `wiki_url` | TEXT | Source Wikipedia URL (nullable) |
| `office_name` | TEXT | Office name (nullable) |
| `github_issue_url` | TEXT | |
| `github_issue_number` | INTEGER | |
| `created_at` | TEXT/TIMESTAMPTZ | |

### `page_quality_checks`
One row per scheduled page quality inspection run.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `source_page_id` | INTEGER FK → source_pages | |
| `checked_at` | TEXT/TIMESTAMPTZ | When the check was performed |
| `html_char_count` | INTEGER | Character count of fetched Wikipedia HTML |
| `office_terms_count` | INTEGER | Number of `office_terms` rows in our DB for this page |
| `ai_votes` | TEXT | JSON array of per-provider votes |
| `result` | TEXT | `ok`, `reparse_ok`, `gh_issue`, `manual_review`, `no_data`, `fetch_failed` |
| `gh_issue_url` | TEXT | URL of created GitHub issue (if any) |
| `created_at` | TEXT/TIMESTAMPTZ | |

### `suspect_record_flags`
Audit log for the pre-insertion suspect pattern gate (`SuspectRecordFlagger`).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `individual_id` | INTEGER FK → individuals | Nullable (set after insert if `allowed`) |
| `office_id` | INTEGER | |
| `full_name` | TEXT | |
| `wiki_url` | TEXT | |
| `flag_reasons` | TEXT | JSON list of triggered pattern names |
| `ai_votes` | TEXT | JSON array of per-provider votes |
| `result` | TEXT | `allowed`, `skipped`, `gh_issue` |
| `gh_issue_url` | TEXT | URL of created GitHub issue (if any) |
| `created_at` | TEXT/TIMESTAMPTZ | |

---

## Operational Tables

### `scraper_jobs`
Persistent job records for scraper runs — both manual UI-triggered jobs and queued jobs. Survives server restart.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | UUID |
| `type` | TEXT | Run mode (e.g. `'delta'`, `'full'`) |
| `status` | TEXT | `queued`, `running`, `complete`, `error`, `expired`, `cancelled` |
| `queued_at` | TEXT/TIMESTAMPTZ | When the job entered the queue |
| `job_params_json` | TEXT | JSON of run parameters |
| `created_at` | TEXT/TIMESTAMPTZ | |
| `updated_at` | TEXT/TIMESTAMPTZ | |
| `result_json` | TEXT | JSON result from `run_with_db()` |

### `scheduled_job_runs`
One row per APScheduler job execution (daily_delta, insufficient_vitals, gemini_research, etc.).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `job_name` | TEXT | APScheduler job ID |
| `started_at` | TEXT/TIMESTAMPTZ | |
| `finished_at` | TEXT/TIMESTAMPTZ | Nullable (NULL while running) |
| `status` | TEXT | `running`, `complete`, `error` |
| `duration_s` | NUMERIC | Wall-clock seconds |
| `result_json` | TEXT | JSON summary (terms_parsed, bio counts, page quality result, etc.) |
| `error` | TEXT | Traceback if `status='error'` |

Surfaced at `/data/scheduled-job-runs`.

### `scheduler_settings`
Per-job pause state. Survives server restart.

| Column | Type | Notes |
|---|---|---|
| `job_id` | TEXT PK | APScheduler job ID |
| `paused` | INTEGER/BOOLEAN | |
| `updated_at` | TEXT/TIMESTAMPTZ | |

### `app_settings`
Operational constants editable via the `/data/scheduled-jobs` UI without a code change.

| Column | Type | Notes |
|---|---|---|
| `key` | TEXT PK | Setting name (e.g. `expiry_hours_queued`) |
| `value` | TEXT | String representation of the value |
| `value_type` | TEXT | `int`, `float`, or `str` |
| `description` | TEXT | Human-readable description |
| `updated_at` | TEXT/TIMESTAMPTZ | |

See `docs/operational-settings.md` for all 12 keys and their defaults.

### `nolink_supersede_log`
Audit trail for no-link placeholder lifecycle events (when a `wiki_url=NULL` placeholder is retired after the holder gains a Wikipedia link).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `old_individual_id` | INTEGER FK → individuals | The retired placeholder |
| `new_individual_id` | INTEGER FK → individuals | The linked replacement |
| `office_id` | INTEGER | |
| `old_wiki_url` | TEXT | NULL / placeholder key |
| `new_wiki_url` | TEXT | Real Wikipedia URL |
| `office_terms_reassigned` | INTEGER | Number of `office_terms` rows moved to the new individual |
| `created_at` | TEXT/TIMESTAMPTZ | |

### `schema_migrations`
PostgreSQL-only table tracking which `_run_pg_migrations()` entries have been applied. Prevents re-running migrations on subsequent startups.

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | Migration name string |
| `applied_at` | TIMESTAMPTZ | |

SQLite uses a different mechanism (`PRAGMA table_info` / `PRAGMA index_list` in `migrate.py`).

---

## Migration History

All migrations are in `src/db/migrate.py`, called via `migrate_to_fk()` at startup. They run in sequence and are idempotent.

| # | Migration | What changed |
|---|---|---|
| 1 | `_migrate_offices_to_fk` | Convert offices text columns (country, level, branch, state) to FK IDs |
| 2 | `_migrate_parties_to_fk` | Same for parties table |
| 3 | `_migrate_office_terms_party_id` | Add `party_id` FK to office_terms; backfill from party name/link |
| 4 | `_migrate_office_terms_drop_party` | Drop party text column from office_terms |
| 5 | `_migrate_offices_enabled` | Add `enabled` column to offices |
| 6 | `_migrate_offices_use_full_page_for_table` | Add `use_full_page_for_table` flag |
| 7 | `_migrate_offices_years_only` | Add `years_only` flag |
| 8 | `_migrate_offices_parsing_options` | Add `term_dates_merged`, `party_ignore`, `district_ignore`, `district_at_large`, `consolidate_rowspan_terms` |
| 9 | `_migrate_office_terms_year_columns` | Add `term_start_year`, `term_end_year`; extend UNIQUE constraint |
| 10 | `_migrate_imprecise_date_columns` | Add `*_imprecise` columns; backfill invalid dates |
| 11 | `_migrate_individuals_dead_link` | Add `is_dead_link` to individuals |
| 12 | `_migrate_individuals_is_living` | Add `is_living`; backfill (deceased if death_date or earliest term >80 years ago) |
| 13 | `_migrate_alt_links` | Create alt_links table; backfill from offices.alt_link; drop offices.alt_link |
| 14 | `_migrate_to_page_office_table_hierarchy` | Create source_pages, office_details, office_table_config; backfill from offices |
| 15 | `_migrate_allow_reuse_tables_and_table_no_unique` | Add allow_reuse_tables; enforce unique(office_details_id, table_no) |
| 16 | `_migrate_office_table_config_name` | Add name column to office_table_config |
| 17 | `_migrate_office_category` | Create office_category tables; add office_details.office_category_id |
| 18 | `_migrate_infobox_role_key_filter` | Create infobox_role_key_filter tables |
| 19 | `_migrate_office_table_config_infobox_role_key_filter_id` | Add infobox_role_key_filter_id FK to office_table_config |
| 20 | `_migrate_offices_infobox_role_key_filter_id` | Add infobox_role_key_filter_id to legacy offices table |
| 21 | `_migrate_infobox_role_key_filter_role_key_format` | Normalize role_key format |
| 22 | `_migrate_city` | Create cities table; add source_pages.city_id |
| 23 | `_migrate_source_pages_disable_auto_table_update` | Add disable_auto_table_update flag to source_pages |

The following are **PostgreSQL-only inline migrations** applied at startup via `_run_pg_migrations()` in `src/db/connection.py`. They use the `schema_migrations` table for idempotency.

| Name | What changed |
|---|---|
| `pg_drop_office_terms_office_id_fkey` | Drop stale FK constraint (office_terms.office_id stores office_table_config_id values in hierarchy mode) |
| `pg_source_pages_dedup` / `_delete` / `_url_unique` | Deduplicate source_pages; add UNIQUE constraint on url |
| `pg_create_parse_error_reports` | Add `parse_error_reports` table |
| `pg_scraper_jobs_queued_at` | Add `queued_at` column to `scraper_jobs` |
| `pg_scraper_jobs_job_params_json` | Add `job_params_json` column to `scraper_jobs` |
| `pg_individuals_insufficient_vitals_checked_at` | Add cooldown timestamp for vitals job |
| `pg_individuals_gemini_research_checked_at` | Add 90-day cooldown for Gemini research |
| `pg_create_individual_research_sources` | Add `individual_research_sources` table |
| `pg_create_wiki_draft_proposals` | Add `wiki_draft_proposals` table |
| `pg_research_sources_origin` | Add `origin` column to `individual_research_sources` |
| `pg_wiki_drafts_origin` | Add `origin` column to `wiki_draft_proposals` |
| `pg_create_reference_documents` | Add `reference_documents` table |
| `pg_create_data_quality_reports` | Add `data_quality_reports` table |
| `pg_source_pages_last_quality_checked_at` | Add `last_quality_checked_at` to `source_pages` |
| `pg_create_page_quality_checks` | Add `page_quality_checks` table |
| `pg_create_suspect_record_flags` | Add `suspect_record_flags` table |
| `pg_office_table_config_last_link_fill_rate` | Add `last_link_fill_rate` to `office_table_config` |
| `pg_individuals_superseded_by_individual_id` | Add `superseded_by_individual_id` FK to `individuals` |
| `pg_create_scheduled_job_runs` | Add `scheduled_job_runs` table + index |
| `pg_create_nolink_supersede_log` | Add `nolink_supersede_log` table |
| `pg_create_scheduler_settings` | Add `scheduler_settings` table |
| `pg_create_app_settings` | Add `app_settings` table |
| `pg_alt_links_backfill_office_details_id` | Backfill `alt_links.office_details_id` from legacy `office_id` via offices→source_pages mapping |
| `pg_alt_links_drop_unique_constraint` | Drop old UNIQUE(office_id, link_path) constraint |
| `pg_alt_links_drop_office_id_index` | Drop index on `alt_links.office_id` |
| `pg_alt_links_drop_office_id` | Drop `alt_links.office_id` column |
| `pg_alt_links_office_details_id_not_null` | Set `alt_links.office_details_id` NOT NULL |
| `pg_alt_links_dedup_before_unique` | Remove duplicate `(office_details_id, link_path)` rows, keeping max-id row per pair (issue #311/#317) |
| `pg_alt_links_add_unique_office_details_link_path` | Add UNIQUE(office_details_id, link_path) to `alt_links` |
| `pg_drop_offices_indexes` | Drop indexes on legacy `offices` table columns |
| `pg_drop_offices_table` | Drop legacy `offices` table (issue #313) |
