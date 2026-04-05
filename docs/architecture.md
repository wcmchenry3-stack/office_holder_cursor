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
← User-Agent header set per Wikimedia etiquette; rate-limited; retry + backoff on 429/503
```

**Auth:** Google OAuth via `authlib`. `require_login()` middleware gates all routes except `/login`, `/auth/google*`, `/static`. When `GOOGLE_CLIENT_ID` is not set, auth is fully bypassed (local dev).

**Async jobs — two execution models:**

| Job type | Execution model | Why |
|---|---|---|
| Scheduled jobs (`daily_delta`, `delta_insufficient_vitals`, `gemini_vitals_research`, `daily_page_quality`) | **Child subprocess** via `subprocess.run([sys.executable, "-c", payload])` | Memory is fully reclaimed after the job ends — critical on Render starter plan |
| Manual/UI-triggered jobs (`POST /api/run`) | **Background thread** in the FastAPI worker process | No persistent disk + faster startup; memory not reclaimed until server restart |

**Subprocess job flow:** Parent serializes `run_with_db()` call params into a Python one-liner, captures stdout/stderr, parses the JSON result printed to stdout. Non-zero exit, empty stdout, or invalid JSON → `finish_run(status="error")`. The child initializes its own Sentry SDK with `sentry_sdk.set_tag("subprocess_job", ...)` and `sentry_sdk.set_context("subprocess", {...})`.

**In-memory job stores** (`_run_job_store`, `_populate_job_store`, etc.) are lost on server restart. The `scraper_jobs` DB table provides a persistent fallback for UI-triggered jobs.

**UI pages:**

| URL | Description |
|---|---|
| `/run` | Trigger scraper runs; select run mode, office category, individual ref |
| `/offices` | List and search offices (legacy flat table) |
| `/offices/<id>` | Edit individual office config |
| `/source-pages` | List source pages (Wikipedia URLs) in the hierarchy |
| `/data/individuals` | Filterable table of all individuals with batch bio-refresh action |
| `/data/scheduled-jobs` | View/pause/resume scheduled jobs; edit cron times and operational settings inline |
| `/data/scheduled-job-runs` | APScheduler execution history with per-run result summaries |
| `/data/scraper-jobs` | Manual + queued scraper job history |
| `/data/runner-registry` | Static registry of run modes and expiry thresholds |
| `/data/ai-decisions` | AI decision log across data quality, parse errors, page quality, suspect flags |
| `/data/wiki-drafts` | Wikipedia draft proposals; list view |
| `/data/wiki-drafts/<id>` | Draft proposal detail; submit button |
| `/gemini-research` | Interactive Gemini+OpenAI vitals research test page |
| `/ai-offices` | AI batch office creation via OpenAI |
| `/refs` | Reference data index (countries, states, cities, levels, branches, parties, etc.) |
| `/refs/parties` | Party CRUD |

**UI pages (selected):**

| URL | Description |
|---|---|
| `/run` | Trigger scraper runs; select run mode, office category, individual ref |
| `/offices` | List and search offices (legacy flat table) |
| `/offices/<id>` | Edit individual office config |
| `/source-pages` | List source pages (Wikipedia URLs) in the hierarchy |
| `/data/individuals` | Filterable table of all individuals with batch bio-refresh action |
| `/data/scheduled-jobs` | Scheduled job status, cron times, and operational settings (expiry/queue) |
| `/data/scheduled-job-runs` | Per-run log for all scheduled jobs with result summaries |
| `/data/ai-decisions` | AI decision log across data quality, parse errors, page quality, suspect flags |
| `/refs` | Reference data index (countries, states, cities, levels, branches, parties, etc.) |
| `/refs/parties` | Party CRUD |

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
│   ├── services/
│   │   ├── ai_office_builder.py        # OpenAI client: office config generation, wiki article polish
│   │   ├── gemini_vitals_researcher.py # Gemini API: deep vitals research
│   │   ├── consensus_voter.py          # Parallel 3-AI consensus vote (VALID/INVALID/DISAGREEMENT)
│   │   ├── page_quality_inspector.py   # Daily page quality inspection; acts on consensus verdict
│   │   ├── suspect_record_flagger.py   # Pre-insertion data quality gate via check_and_gate()
│   │   ├── data_quality_checker.py     # Multi-model data quality validation
│   │   ├── auto_fix.py                 # Parser error auto-fix via Claude; opens draft PRs
│   │   ├── parse_error_reporter.py     # Parser error → GitHub issue pipeline
│   │   ├── quality_issue_reporter.py   # Data quality → GitHub issue pipeline (two-level dedup: record-level + content hash)
│   │   ├── github_client.py            # GitHub API client for issues/PRs
│   │   ├── claude_client.py            # Anthropic Claude API client (max_tokens, backoff)
│   │   ├── wikipedia_submit.py         # MediaWiki Action API: article submission (bot credentials)
│   │   └── orchestrator.py             # Singleton factory for AI services
│   └── db/
│       ├── connection.py               # get_connection(), init_db(), get_db_path(); PG + SQLite init
│       ├── schema.py                   # SCHEMA_SQL / SCHEMA_PG_SQL (all CREATE TABLE statements)
│       ├── migrate.py                  # migrate_to_fk(); SQLite migration sequence
│       ├── offices.py                  # Office CRUD; flat offices table helpers
│       ├── individuals.py              # Individual upsert/lookup; batch helpers
│       ├── office_terms.py             # Term insert/update/delete
│       ├── parties.py                  # Party CRUD; get_party_list_for_scraper()
│       ├── individual_research_sources.py # Research sources + wiki draft CRUD + notability threshold
│       ├── scraper_jobs.py             # Scraper job queue: create/update/pop/expire
│       ├── scheduled_job_runs.py       # Scheduled job run log: create_run(), finish_run()
│       ├── scheduler_settings.py       # Job pause/resume state; is_job_paused()
│       ├── app_settings.py             # Operational settings: get_setting(), set_setting(), seed
│       ├── data_quality_reports.py     # Data quality report CRUD
│       ├── parse_errors.py             # Parse error report CRUD
│       ├── page_quality_checks.py      # Page quality check result CRUD
│       ├── suspect_record_flags.py     # Suspect record flag CRUD
│       ├── ai_decisions.py             # Unified query across all AI decision tables
│       ├── nolink_supersede_log.py     # Nolink placeholder supersession log
│       ├── infobox_role_key_filter.py  # Infobox role key filter CRUD
│       ├── office_category.py          # Office category CRUD
│       ├── reference_documents.py      # Reference document cache (e.g. Wikipedia MOS)
│       ├── runner_registry.py          # Subprocess runner process registry
│       ├── bulk_import.py              # CSV bulk import
│       ├── date_utils.py               # normalize_date(); date parsing utilities
│       ├── refs.py                     # Reference data (countries, states, cities, levels, branches)
│       ├── reports.py                  # Report queries
│       ├── seed.py                     # Seeds reference data (countries, levels, branches)
│       ├── seed_reference_docs.py      # Seeds reference documents (e.g. Wikipedia MOS)
│       └── utils.py                    # Shared DB utilities
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
├── render.yaml                    # Deployment manifest: PostgreSQL service (office-holder-pg, starter plan);
│                                  #   web service with 1 GB persistent disk mounted at /data
│                                  #   (logs + wiki_cache survive deploys); DATABASE_URL auto-wired;
│                                  #   SECRET_KEY auto-generated; DATA_QUALITY_ENABLED=0 by default
├── requirements.txt               # Python dependencies
└── runner_head.py                 # UTF-16 encoded backup of runner.py — NOT used at runtime
```

---

## SQLite / PostgreSQL Dual Backend

The app supports two database backends selected by the `DATABASE_URL` env var:

| Environment | Backend | Schema constant | Connection |
|---|---|---|---|
| Local dev / CI | SQLite | `SCHEMA_SQL` in `schema.py` | `sqlite3`, file at `data/office_holder.db` |
| Production (Render) | PostgreSQL | `SCHEMA_PG_SQL` in `schema.py` | `psycopg2`, URL from `DATABASE_URL` |

**Key abstractions:**
- `_PGConnWrapper` — thin shim that gives psycopg2 connections a sqlite3-compatible `.execute("?")` API (converts `?` → `%s` placeholders and `.lastrowid` → `.fetchone()[0]`)
- `is_postgres()` — used throughout the DB layer to gate PostgreSQL-specific behavior (e.g. `RETURNING id`, `ON CONFLICT DO UPDATE`)
- `_run_pg_migrations()` — runs PostgreSQL-only `ALTER TABLE` migrations at startup (idempotent, tracked in `schema_migrations` table). **Rule: when adding a column, update both `SCHEMA_SQL` and `SCHEMA_PG_SQL`, and add an entry to `_run_pg_migrations()`.**
- `src/db/test_schema_sync.py` — CI check that fails if the two schemas drift; enforces the add-to-both rule

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
| `DAILY_DELTA_ENABLED` | Legacy/render.yaml | `1` | Present in `render.yaml` (value `"1"`) but **not read by any current Python source**. Kept in render.yaml as a placeholder; `RUNNERS_ENABLED` is the active kill switch for all scheduled jobs. |
| `GEMINI_OFFICE_HOLDER` | For Gemini | — | Google Gemini API key for deep vitals research (Feature C). If unset, Gemini research is silently disabled. |
| `OPENAI_API_KEY` | For OpenAI | — | OpenAI API key for AI office builder and consensus voter. If unset, OpenAI provider is skipped in consensus votes; AI office builder is disabled. |
| `ANTHROPIC_API_KEY` | For Claude | — | Anthropic API key for parser auto-fix and consensus voter. If unset, Claude provider is skipped in consensus votes; auto-fix is silently disabled. |
| `GITHUB_TOKEN` | For GitHub | — | GitHub personal access token for opening auto-fix and page-quality PRs/issues. If unset, GitHub integration is silently disabled. |
| `GITHUB_REPO` | For GitHub | `wcmchenry3-stack/office_holder_cursor` | `owner/repo` slug for GitHub API calls (issues, PRs). Only needed if `GITHUB_TOKEN` is set. |
| `RUNNERS_ENABLED` | No | `1` | Set to `0`, `false`, `no`, or `off` to globally disable all scheduled and user-triggered scraper jobs without a redeploy. Checked at job start time. |
| `DATA_QUALITY_ENABLED` | No | (unset = disabled) | Set to `1` to enable the `SuspectRecordFlagger` data quality gate during delta/full runs. Off by default; enable in production once baseline is established. |
| `DATABASE_URL` | Prod (PG) | — | PostgreSQL connection URL (e.g. `postgresql://user:pass@host/db`). If set, the app uses PostgreSQL instead of SQLite. |
| `LOG_DIR` | No | `data/logs` | Override the directory where per-run scraper log files are written. Must be an absolute path on Render persistent disk. |
| `WIKI_CACHE_DIR` | No | `data/wiki_cache` | Override the directory for cached Wikipedia HTML. Set to a path on Render persistent disk to survive deploys. |
| `WIKI_FETCH_WORKERS` | No | `1` | Number of parallel threads for Wikipedia infobox fetches. Increase with caution — Wikimedia rate limits apply. |
| `SCRAPER_LOG_LEVEL` | No | `INFO` | Log level for the scraper module (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `WIKIPEDIA_BOT_USERNAME` | For wiki submit | — | Wikipedia bot account username for article submission via MediaWiki Action API. If unset, submit is disabled. |
| `WIKIPEDIA_BOT_PASSWORD` | For wiki submit | — | Wikipedia bot account password. If unset, submit is disabled. |
| `APP_ENVIRONMENT` | No | `dev` | Environment name (`dev` or `prd`). Used by Sentry and logging. |
| `SENTRY_DSN` | For Sentry | — | Sentry DSN for error tracking. If unset, Sentry is disabled (local dev). |
| `SENTRY_TRACES_SAMPLE_RATE` | No | `0.1` | Fraction of requests to trace for Sentry performance monitoring (0.0–1.0). |
| `PLAYWRIGHT_BASE_URL` | Testing | `http://127.0.0.1:8000` | Base URL for Playwright tests. Set automatically by CI to `http://127.0.0.1:8000`. |
| `PLAYWRIGHT_EDIT_OFFICE_ID` | Local testing only | — | Office ID for edit-office Playwright tests. **Not required by CI** — CI uses a freshly seeded temp DB. Only set when running tests locally against a pre-populated database. |
| `PLAYWRIGHT_OFFICE_A_ID` | Local testing only | — | Office ID A for comparison tests. See note above. |
| `PLAYWRIGHT_OFFICE_B_ID` | Local testing only | — | Office ID B for comparison tests. See note above. |
| `PLAYWRIGHT_PAGE_EDIT_URL` | Local testing only | — | Source page URL for page edit tests. See note above. |

**Sentry instrumentation scope:** `FastApiIntegration(transaction_style="endpoint")` + `LoggingIntegration(level=WARNING, event_level=ERROR)`. `send_default_pii=False`. Subprocess jobs call `sentry_sdk.set_tag("subprocess_job", ...)` and `sentry_sdk.set_context("subprocess", {...})` at startup so errors surface with run-specific context in Sentry. Scraper jobs also call `sentry_sdk.set_context("scraper_job", {"job_id": ..., "run_mode": ...})` in the job worker thread.

---

## Database Access (pgAdmin Desktop)

The production PostgreSQL database on Render can be queried directly using [pgAdmin Desktop](https://www.pgadmin.org/download/) — useful for ad-hoc data analysis and debugging.

### Setup

1. **Get connection details** — In the [Render dashboard](https://dashboard.render.com), navigate to the `office-holder-db` database and copy the **External Database URL** (under "Connections").
2. **Install pgAdmin** — Download from [pgadmin.org/download](https://www.pgadmin.org/download/) (free, runs locally).
3. **Register a new server** — In pgAdmin: *Object → Register → Server…*, then fill in:

   | Tab | Field | Value |
   |---|---|---|
   | General | Name | `office-holder-prd` (any label you like) |
   | Connection | Host name/address | `dpg-d73k20ruibrs73auubeg-a.oregon-postgres.render.com` |
   | Connection | Port | `5432` |
   | Connection | Maintenance database | `office_holder_db` |
   | Connection | Username | `office_holder_user` |
   | Connection | Password | *(from Render dashboard — do not commit)* |
   | SSL | SSL mode | `Require` |

4. **Optional: mark read-only** — Under *Properties → Advanced*, set *Session role* or use pgAdmin's *Preferences → SQL Editor → Read-Only* to prevent accidental writes.

### Useful Queries

```sql
-- Records with missing wiki URLs
SELECT COUNT(*) FROM individuals
WHERE wiki_url LIKE 'No link:%' OR wiki_url = '';

-- Records flagged as dead links
SELECT id, name, wiki_url FROM individuals
WHERE is_dead_link = 1;

-- Office terms with no linked individual
SELECT ot.id, ot.office_id, ot.holder_name
FROM office_terms ot
LEFT JOIN individuals i ON ot.individual_id = i.id
WHERE ot.individual_id IS NULL;

-- Individuals with missing vital dates
SELECT id, name, birth_date, death_date FROM individuals
WHERE birth_date IS NULL OR birth_date = '';
```

> **Security note:** Never commit database credentials or connection strings to the repo. Always retrieve them from the Render dashboard.
