# Database Schema Reference

SQLite database. Schema defined in `src/db/schema.py` (`SCHEMA_SQL`). Migrations in `src/db/migrate.py`.

---

## Relationship Diagram

```
countries ──┬── states ── cities
            │
            ├── source_pages ── office_details ── office_table_config ──→ infobox_role_key_filter
            │                       │
            │                   (office_category)
            │
            ├── offices (LEGACY flat table — still in active use)
            │       └── alt_links
            │
            ├── parties
            │
            └── (via office_terms)
                    ↓
              office_terms ──→ offices (legacy)
                    ↓               ↓
              individuals       parties
```

**Key:** `office_terms` links to the legacy `offices` table (not `office_table_config`). All scraper run results write to `office_terms` via the legacy `office_id`.

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

## Legacy Flat Table

### `offices`
The original flat design. Still used by all scraper runs. Contains all fields from `source_pages` + `office_details` + `office_table_config` in one row.

**Important:** When adding new config fields, they must be added to `offices` AND `office_table_config` (with a migration for each).

### `alt_links`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `office_id` | FK → offices | |
| `link_path` | TEXT | Wikipedia path (e.g. `/wiki/John_Smith`) |
| | | UNIQUE(office_id, link_path) |

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

### `office_terms`
One row per scraped term (a person holding an office for a period).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `office_id` | INTEGER FK → offices | Legacy office ID |
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
