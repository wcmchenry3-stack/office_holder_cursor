# CLAUDE.md — Office Holder

This file provides context for Claude (and contributors) to work effectively in this codebase. Read this before making changes.

---

## Project Overview

**Office Holder** is a single-user web application that:
- Scrapes Wikipedia HTML tables to extract political office holder data
- Stores results in a SQLite database (individuals, office terms, parties)
- Provides a FastAPI/Jinja2 dark-mode web UI for managing configurations and triggering runs
- Runs on Render.com with a persistent 1 GB disk for the database

The tool is powerful and handles many Wikipedia table variations, which also makes it brittle. Know the test infrastructure before changing parsing logic.

---

## Architecture

```
Browser
  ↓ HTTP (FastAPI)
src/main.py          ← all routes, Google OAuth middleware, async job stores
  ↓                          ↓
src/scraper/             src/db/
  runner.py              connection.py   ← get_connection(), init_db()
  table_parser.py        schema.py       ← SCHEMA_SQL (CREATE TABLE statements)
  wiki_fetch.py          migrate.py      ← auto-runs at startup via init_db()
  table_cache.py         offices.py      ← office CRUD + office_row helpers
  config_test.py         individuals.py
  logger.py              office_terms.py
                         parties.py
                         bulk_import.py
                         date_utils.py
  ↓                          ↓
Wikipedia REST API      SQLite file
(en.wikipedia.org)      data/office_holder.db
```

**Auth:** Google OAuth via `authlib`. `require_login()` middleware gates all routes except `/login`, `/auth/google*`, `/static`. When `GOOGLE_CLIENT_ID` is not set, auth is fully bypassed (local dev).

**Async jobs:** Runs, bulk imports, previews, and UI tests each use an in-memory job store (`_run_job_store`, `_populate_job_store`, etc.) + background thread. Single-process only — no task queue.

---

## Key Mental Model: Page → Office → Table Hierarchy

The modern data model has three layers:

```
source_pages          ← a Wikipedia URL (e.g. /wiki/Governor_of_Alaska)
  └── office_details  ← a logical office on that page (e.g. "Governor")
        └── office_table_config  ← how to parse one HTML table for that office
```

**Legacy:** The flat `offices` table predates the hierarchy and is still in active use. Every scraper run reads from `offices` (via `db_offices.list_offices()`). The hierarchy tables (`source_pages`, `office_details`, `office_table_config`) were added later and are joined/synced during migrations. Both exist simultaneously.

When adding new config fields, they must be added to **both** `offices` AND `office_table_config` (and a migration written for each).

---

## Directory Structure

```
office_holder_cursor/
├── src/
│   ├── main.py                    # FastAPI app: all routes, auth, job stores
│   ├── scraper/
│   │   ├── runner.py              # Scraper orchestration; all run modes
│   │   ├── table_parser.py        # HTML table parsing; DataCleanup, Offices, Biography classes
│   │   ├── wiki_fetch.py          # URL normalization, REST API URL builder
│   │   ├── table_cache.py         # Per-key gzip cache for fetched table HTML
│   │   ├── config_test.py         # Config validation, raw table preview
│   │   ├── parse_core.py          # Re-exports DataCleanup, Offices, Biography
│   │   └── logger.py              # Logger class + HTTP_USER_AGENT constant
│   └── db/
│       ├── connection.py          # get_connection(), init_db(), get_db_path()
│       ├── schema.py              # SCHEMA_SQL string (all CREATE TABLE statements)
│       ├── migrate.py             # migrate_to_fk(); all schema migrations
│       ├── offices.py             # Office CRUD; office_row_to_table_config(), office_row_to_office_details()
│       ├── individuals.py         # Individual upsert/lookup
│       ├── office_terms.py        # Term insert/update/delete
│       ├── parties.py             # Party CRUD; get_party_list_for_scraper()
│       ├── bulk_import.py         # CSV bulk import
│       ├── date_utils.py          # normalize_date(); date parsing utilities
│       ├── refs.py                # Reference data (countries, states, cities, levels, branches)
│       ├── reports.py             # Report queries
│       ├── seed.py                # Seeds reference data (countries, levels, branches)
│       └── utils.py               # Shared DB utilities
├── templates/                     # Jinja2 HTML templates (dark mode UI)
├── static/                        # CSS, JS assets
├── scripts/                       # Utility/test scripts (not part of main app)
│   ├── run_scenarios_test.py      # Scenario integration tests (uses test DB + fixtures)
│   ├── validate_parser_fixtures.py # Validates parser_tests.json manifest integrity
│   └── infobox_role_key_cli.py    # CLI for setting infobox role key filters
├── test_scripts/
│   ├── manifest/
│   │   └── parser_tests.json      # Canonical parser test manifest
│   └── fixtures/                  # Committed HTML fixture files for tests
├── data/                          # Runtime data (gitignored)
│   ├── office_holder.db           # Production SQLite database
│   ├── test_run.db                # Test database (used by scenario runner)
│   ├── logs/                      # Log files from scraper runs
│   └── wiki_cache/                # Gzip cache of fetched Wikipedia HTML tables
├── render.yaml                    # Render.com deployment config
├── requirements.txt               # Python dependencies
└── runner_head.py                 # UTF-16 encoded backup of runner.py — NOT used at runtime; safe to ignore
```

---

## Development Setup

```bash
# 1. Create and activate virtual environment
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the dev server (auto-reloads on file changes)
uvicorn src.main:app --reload

# 4. Open http://127.0.0.1:8000
```

**Auth in dev:** When `GOOGLE_CLIENT_ID` is not set in the environment, all auth checks are bypassed. No OAuth setup needed locally.

**Database:** Created automatically at `data/office_holder.db` on first run. Schema + migrations run at startup via `init_db()`.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OFFICE_HOLDER_DB_PATH` | No | `data/office_holder.db` | Override DB file location |
| `SECRET_KEY` | Prod | `"dev-only-insecure-key"` | Session signing key |
| `GOOGLE_CLIENT_ID` | Prod | (unset = auth disabled) | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Prod | — | Google OAuth client secret |
| `ALLOWED_EMAIL` | Prod | — | Single authorized email address |
| `APP_BASE_URL` | Prod | — | Full base URL for OAuth callback (e.g. `https://myapp.onrender.com`) |
| `PLAYWRIGHT_BASE_URL` | Testing | `http://127.0.0.1:8000` | Base URL for Playwright tests |
| `PLAYWRIGHT_EDIT_OFFICE_ID` | Testing | — | Office ID used in Playwright UI tests |
| `PLAYWRIGHT_OFFICE_A_ID` | Testing | — | Office ID A for comparison tests |
| `PLAYWRIGHT_OFFICE_B_ID` | Testing | — | Office ID B for comparison tests |
| `PLAYWRIGHT_PAGE_EDIT_URL` | Testing | — | Source page URL for page edit tests |

---

## Database

**Location:** `data/office_holder.db` locally; `/data/office_holder.db` on Render (persistent disk).

**Startup flow:**
```
init_db()
  → create tables (SCHEMA_SQL)
  → seed reference data (seed.py)
  → migrate_to_fk() — runs all migrations in sequence, idempotent
  → seed parser test data (from parser_tests.json, only if table empty)
```

**Migration rule:** Never alter the schema manually. Always add a new migration function to `src/db/migrate.py` and call it from `migrate_to_fk()`. All migrations must be idempotent (check column/table existence before acting). See `docs/schema.md` for migration history.

**Connection:** `get_connection()` returns `sqlite3.Connection` with `row_factory=sqlite3.Row` (rows accessible as dicts). Timeout: 10 seconds.

---

## Run Modes

| Mode | How to trigger | What it does | When to use |
|---|---|---|---|
| `delta` | Default run | Parses all enabled offices; compares with existing terms; inserts/updates only changes | Routine incremental updates |
| `full` | UI: "Full Run" | Deletes all office_terms (optionally individuals too); re-parses everything fresh | After major config changes or data corruption |
| `live_person` | UI: "Live Person" | Like delta, but also refreshes bios for all individuals with no death_date | Keep living people's data current |
| `single_bio` | UI: individual page | Fetches biography for one individual by ID or wiki URL | Fix one person's bio |
| `selected_bios` | UI: batch action | Refreshes bios for a list of individual IDs | Bulk bio fix |
| `bios_only` | UI | Skips all office table parsing; updates all individuals' bios | Bio-only refresh |
| `category_bios` | UI: category page | Bio refresh for all individuals in a given office category | Category-scoped bio update |

See `docs/run-modes.md` for detailed behavior, edge cases, and the auto-table-update algorithm.

---

## Key Parsing Behaviors & Gotchas

**Column indices:** 1-based in UI/config (what users enter), 0-based in Python parsing code. The conversion happens in `runner.py`/`main.py` before calling the parser.

**Boolean flags:** Accept `True`, `1`, `"1"`, `"true"`, `"TRUE"`. Conversion done in `_office_draft_from_body()` in `main.py`.

**Imprecise dates:** If a date can't be parsed to `YYYY-MM-DD`, the date column is set to `NULL` and `*_imprecise=1` is set. This applies to birth/death dates and term start/end dates.

**`data-sort-value` attribute:** Wikipedia sortable tables embed `YYYY-MM-DD` dates as `data-sort-value` on `<td>` elements. The parser checks this attribute before falling back to cell text.

**Term dates merged:** When `term_dates_merged=True`, `term_end_column` is ignored and set equal to `term_start_column` (the column holds both start and end in a range format like "2010–2015").

**Holder matching:** Uses URL-only canonical keys: `canonical_holder_url()` produces `/wiki/<title>` (lowercased). Dead links (`?redlink=1`) are detected and skipped. Rows with no Wikipedia link are labeled `"No link:office_id:name"`.

**Auto-table-update:** When a delta run finds missing holders (existing terms not in new parse), it searches all other tables on the same page and picks the one minimizing missing holders. Controlled by `disable_auto_table_update` flag on `source_pages`. See `docs/run-modes.md`.

**Infobox lookup:** `find_date_in_infobox=True` triggers a second Wikipedia fetch per individual to extract birth/death dates from their personal infobox. This is slow — only enable when table dates are missing. `infobox_role_key_filter_id` further filters which infobox role entries to use (e.g. only "judge" roles, not "senator").

**Rowspan consolidation:** `consolidate_rowspan_terms=True` merges consecutive rows with the same holder into a single term. Different from `parse_rowspan` (which handles HTML `rowspan` attributes on cells).

**Dynamic parse:** `dynamic_parse=True` auto-detects whether a row is an office-header row or a term-data row, allowing mixed tables.

**Years-only mode:** `years_only=True` — rows must have `Term Start Year` or `Term End Year`; full dates stay null. Used for tables that only show year ranges.

---

## Testing Infrastructure

**Parser test manifest:** `test_scripts/manifest/parser_tests.json`
- Format: `{ name, test_type, html_file, source_url, config_json, expected_json, enabled }`
- `html_file` points to a committed fixture file in `test_scripts/fixtures/`
- `config_json` is the office table config (same shape as `offices` table row)
- `expected_json` is the expected array of parsed output rows

**Scenario runner:** `scripts/run_scenarios_test.py`
- Sets `OFFICE_HOLDER_DB_PATH=data/test_run.db` — never touches production DB
- Pre-fills `data/wiki_cache/` with fixture HTML so no live Wikipedia requests
- Runs the scraper against test configs and checks output vs. expected

**Unit tests:** Scattered across `src/scraper/test_*.py` and `src/db/test_*.py`. No shared `conftest.py` yet.

**pytest:** Not yet in `requirements.txt`. To run tests: `pip install pytest` first.

**Playwright tests:** In `src/test_ui_edit_office_playwright.py`. Require manual env var setup (`PLAYWRIGHT_*` vars). Not currently wired into CI.

**CI:** `.github/workflows/validate-parser-fixtures.yml` — validates `parser_tests.json` manifest integrity and fixture file existence only.

---

## Key Conventions

- **Migrations:** Always idempotent. Check `PRAGMA table_info(...)` or `PRAGMA index_list(...)` before altering.
- **DB rows:** `sqlite3.Row` — access columns by name like a dict. Passed around as `dict[str, Any]`.
- **Party matching:** By `country_id` + `party_name` or `party_link`. Unmatched parties stored as text; matched parties get `party_id` FK.
- **Office row helpers:** `db_offices.office_row_to_table_config()` and `office_row_to_office_details()` convert flat `offices` row → structured dicts for the parser. Use these; don't build config dicts manually.
- **Progress callbacks:** `run_with_db()` accepts an optional `progress_callback(phase, current, total, message, extra_dict)` for streaming progress to the UI.
- **Wikipedia requests:** Always use `WIKIPEDIA_REQUEST_HEADERS` from `wiki_fetch.py` (includes User-Agent + gzip per Wikimedia policy).
- **No DB write in dry_run/test_run:** `run_with_db(dry_run=True)` or `test_run=True` skips all DB writes.

---

## Known Issues / Technical Debt

- **`runner_head.py`** — UTF-16 encoded duplicate of `runner.py` at the repo root. Artifact from an old editor export. Not imported anywhere, not used. Safe to delete eventually.
- **pytest missing from requirements.txt** — Tests exist but `pytest` is not a declared dependency. Add it before Phase 4 (Testing).
- **Dual schema (offices + hierarchy)** — The flat `offices` table and the `source_pages → office_details → office_table_config` hierarchy coexist. All scraper runs still read from `offices`. New config fields must be added to both. This will be cleaned up in Phase 6 (Refactor).
- **No conftest.py** — Tests lack shared fixtures. Each test file manages its own DB setup. This will be addressed in Phase 4.
- **Playwright tests not in CI** — They exist but require manual env var setup and a running server. Phase 4 will address this.
- **In-memory job stores** — `_run_job_store` etc. in `main.py` are lost on server restart. A run in progress when Render restarts is silently abandoned.

---

## Git Workflow

**Branch protection:** `dev` is the main branch. Direct pushes to `dev` are blocked by GitHub. All work goes through feature or bug branches via pull request.

**Branch naming:**
- New features → `feature/<short-description>` (e.g. `feature/datasette-db-access`)
- Bug fixes → `bug/<short-description>` (e.g. `bug/delta-run-missing-holders`)

**Starting a new phase or task — always follow this sequence:**
```bash
# 1. Ensure all in-progress commits are pushed to the current remote branch
git push

# 2. Switch to dev and pull the latest (gets any merged PRs)
git checkout dev
git pull origin dev

# 3. Create a fresh branch for the new work
git checkout -b feature/<name>
git push -u origin feature/<name>
```

**Day-to-day:**
```bash
# Commit work to the feature branch as you go
git add <files>
git commit -m "descriptive message"
git push
```

**Finishing up:**
1. Push final commits: `git push`
2. Open a PR from `feature/<name>` → `dev` on GitHub
3. After merge, start the next task by following the "starting" sequence above

**Never:**
- Push directly to `dev`
- Start new work on a branch that has unpushed commits from a previous task
- Merge your own PR without review (when applicable)

---

## Supporting Documentation

- `docs/run-modes.md` — Detailed run mode reference including auto-table-update algorithm
- `docs/schema.md` — Database schema, relationships, migration history
- `docs/config-options.md` — All `office_table_config` fields with descriptions
- `README.md` — User-facing setup guide, UI walkthrough, route reference
