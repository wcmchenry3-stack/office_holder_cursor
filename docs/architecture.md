# Architecture & Setup

## System Diagram

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
                         parties.py / bulk_import.py / date_utils.py
  ↓                          ↓
Wikipedia REST API      SQLite file
(en.wikipedia.org)      data/office_holder.db
← rate-limited; retry + backoff on 429/503
```

**Auth:** Google OAuth via `authlib`. `require_login()` middleware gates all routes except `/login`, `/auth/google*`, `/static`. When `GOOGLE_CLIENT_ID` is not set, auth is fully bypassed (local dev).

**Async jobs:** Runs, bulk imports, previews, and UI tests each use an in-memory job store (`_run_job_store`, `_populate_job_store`, etc.) + background thread. Single-process only — no task queue. Job stores are lost on server restart.

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
└── runner_head.py                 # UTF-16 encoded backup of runner.py — NOT used at runtime
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

Auth is bypassed locally when `GOOGLE_CLIENT_ID` is not set. Database is created automatically at `data/office_holder.db` on first run.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OFFICE_HOLDER_DB_PATH` | No | `data/office_holder.db` | Override DB file location |
| `SECRET_KEY` | Prod | `"dev-only-insecure-key"` | Session signing key |
| `GOOGLE_CLIENT_ID` | Prod | (unset = auth disabled) | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Prod | — | Google OAuth client secret |
| `ALLOWED_EMAIL` | Prod | — | Single authorized email address |
| `APP_BASE_URL` | Prod | — | Full base URL for OAuth callback (e.g. `https://rulersai.buffingchi.com`) |
| `EMAIL_APP_PASSWORD` | For email | — | Gmail App Password for daily run summary email (myaccount.google.com/apppasswords) |
| `EMAIL_FROM` | No | `wcmchenry3@gmail.com` | Sender address for summary email |
| `EMAIL_TO` | No | `wcmchenry3@gmail.com` | Recipient address for summary email |
| `GEMINI_OFFICE_HOLDER` | For Gemini | — | Google Gemini API key for deep vitals research (Feature C). If unset, Gemini research is silently disabled. |
| `PLAYWRIGHT_BASE_URL` | Testing | `http://127.0.0.1:8000` | Base URL for Playwright tests |
| `PLAYWRIGHT_EDIT_OFFICE_ID` | Testing | — | Office ID used in Playwright UI tests |
| `PLAYWRIGHT_OFFICE_A_ID` | Testing | — | Office ID A for comparison tests |
| `PLAYWRIGHT_OFFICE_B_ID` | Testing | — | Office ID B for comparison tests |
| `PLAYWRIGHT_PAGE_EDIT_URL` | Testing | — | Source page URL for page edit tests |
