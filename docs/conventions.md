# Conventions, Gotchas & Technical Debt

## Key Conventions

- **Migrations:** Always idempotent. Check `PRAGMA table_info(...)` or `PRAGMA index_list(...)` before altering. Never alter schema manually — always add a function to `migrate_to_fk()` in `src/db/migrate.py`.
- **Dual backend schema rule:** When adding a column or table, update **both** `SCHEMA_SQL` (SQLite, `src/db/schema.py`) **and** `SCHEMA_PG_SQL` (PostgreSQL, same file), **and** add an entry to `_run_pg_migrations()` in `src/db/connection.py`. `test_schema_sync.py` in CI will fail if the two schemas drift.
- **DB rows:** `sqlite3.Row` with `row_factory` — access columns by name like a dict. Passed around as `dict[str, Any]`.
- **Party matching:** By `country_id` + `party_name` or `party_link`. Unmatched parties stored as text; matched parties get `party_id` FK.
- **Office row helpers:** Always use `db_offices.office_row_to_table_config()` and `office_row_to_office_details()` to convert a flat `offices` row into structured dicts for the parser. Don't build config dicts manually.
- **Progress callbacks:** `run_with_db()` accepts an optional `progress_callback(phase, current, total, message, extra_dict)` for streaming progress to the UI.
- **Wikipedia requests:** Always use `WIKIPEDIA_REQUEST_HEADERS` from `wiki_fetch.py` (includes User-Agent + gzip per Wikimedia policy).
- **Gemini API:** All calls go through `src/services/gemini_vitals_researcher.py`. API key from `GEMINI_OFFICE_HOLDER` env var (never hardcoded). Exponential backoff on HTTP 429 (`RESOURCE_EXHAUSTED`). `max_output_tokens` set on every call. See runner.py docstring for full policy details.
- **Wikipedia submit:** All MediaWiki Action API calls go through `src/services/wikipedia_submit.py`. Bot credentials from `WIKIPEDIA_BOT_USERNAME`/`WIKIPEDIA_BOT_PASSWORD` env vars (never hardcoded). User-Agent set per Wikimedia etiquette. Rate-limited to 1 req/sec minimum; respects `Retry-After`. If credentials not set, submit is silently disabled (503 from endpoint).
- **Notability threshold:** Deterministic gate in `individual_research_sources.check_notability_threshold()` — requires ≥2 independent sources (Wikipedia mirrors excluded), ≥1 government/academic source, and verifiable term dates. Applied before wiki draft generation in both nightly and interactive flows.
- **Claude auto-fix:** All Claude API calls go through `src/services/claude_client.py`. API key from `ANTHROPIC_API_KEY` env var (never hardcoded). Exponential backoff on HTTP 429. `max_tokens=4096` on every call. Auto-fix proposals are gated by 7 deterministic minimal-risk criteria in `src/services/auto_fix.py` before any PR is created. PRs are always opened as draft.
- **No DB write in dry_run/test_run:** `run_with_db(dry_run=True)` or `test_run=True` skips all DB writes.
- **Job store pattern — when to use each:**
  - *In-memory only* (`ai_offices`, `gemini_research`, `populate` preview): job state lives only in `_job_store` dict + lock + `_evict_old_jobs()`. Appropriate for ephemeral UI-feedback jobs where loss on server restart is tolerable.
  - *In-memory + persistent DB* (`run_scraper` via `scraper_jobs` table): state also written to DB. Required when jobs queue, need dequeue-on-restart, or need an audit trail. The DB record is the source of truth after a restart; in-memory is the source of truth during the same process lifetime.
- **`_PGSavepointContext` rule:** DB module functions that run INSERT/UPDATE on a **caller-owned connection** (`own_conn=False`) must wrap the operation in a savepoint to avoid poisoning the outer PostgreSQL transaction on constraint violations (e.g. `UniqueViolation`). Without this, a nested `UniqueViolation` aborts the entire outer transaction, causing all subsequent writes to fail silently (production incident — fixed in PR #268). Usage: `with _PGSavepointContext(conn, "savepoint_name"): conn.execute("INSERT INTO ...")`. SQLite connections are unaffected (savepoints are no-ops there).

---

## Parsing Gotchas

**Column indices:** 1-based in UI/config (what users enter), 0-based in Python parsing code. Conversion happens in `runner.py`/`main.py` before calling the parser.

**Boolean flags:** Accept `True`, `1`, `"1"`, `"true"`, `"TRUE"`. Conversion done in `_office_draft_from_body()` in `main.py`.

**Imprecise dates:** If a date can't be parsed to `YYYY-MM-DD`, the column is set to `NULL` and `*_imprecise=1` is set. Applies to birth/death dates and term start/end dates.

**`data-sort-value` attribute:** Wikipedia sortable tables embed `YYYY-MM-DD` dates as `data-sort-value` on `<td>` elements. The parser checks this attribute before falling back to cell text.

**Term dates merged:** When `term_dates_merged=True`, `term_end_column` is ignored; `term_start_column` holds a range like "2010–2015" that is split into start and end.

**Holder matching:** Uses URL-only canonical keys: `canonical_holder_url()` produces `/wiki/<title>` (lowercased). Dead links (`?redlink=1`) are skipped. No-link rows are labeled `"No link:office_id:name"`.

**Auto-table-update:** When a delta run finds missing holders, it searches other tables on the same page and picks the one minimizing missing holders. Controlled by `disable_auto_table_update` on `source_pages`. See `docs/run-modes.md`.

**Infobox lookup:** `find_date_in_infobox=True` triggers a second Wikipedia fetch per individual — slow, only enable when table dates are unavailable. `infobox_role_key_filter_id` filters which infobox role entries to use. See `docs/config-options.md`.

**Rowspan consolidation:** `consolidate_rowspan_terms=True` merges consecutive rows with the same holder into one term. Different from `parse_rowspan` (which handles HTML `rowspan` attributes on cells).

**Dynamic parse:** `dynamic_parse=True` auto-detects whether a row is an office-header row or a term-data row, allowing mixed tables with section headers.

**Years-only mode:** `years_only=True` — only year integers are extracted (`term_start_year`/`term_end_year`); full dates remain NULL.

---

## Testing Infrastructure

See [~/.claude/standards/testing.md](~/.claude/standards/testing.md) for universal testing conventions (coverage thresholds, what not to test, patterns).

**Parser test manifest:** `test_scripts/manifest/parser_tests.json`
- Format: `{ name, test_type, html_file, source_url, config_json, expected_json, enabled }`
- `html_file` → committed fixture in `test_scripts/fixtures/`
- `config_json` → office table config (same shape as `offices` row)
- `expected_json` → expected array of parsed output rows

**Scenario runner:** `scripts/run_scenarios_test.py`
- Sets `OFFICE_HOLDER_DB_PATH=data/test_run.db` — never touches production DB
- Pre-fills `data/wiki_cache/` with fixture HTML — no live Wikipedia requests

**Scenario tests as a PR expectation:** The `/run-scenarios-test` UI button has been removed from the nav. Scenario test cases (`tests/test_scenarios.py`) are expected as part of every PR that introduces new parsing functionality. Building the test case is part of the feature development, not a separate step — run them locally with `pytest tests/test_scenarios.py`.

**Unit tests:** Scattered across `src/scraper/test_*.py` and `src/db/test_*.py`. Shared fixtures for integration tests live in `tests/conftest.py`.

**Playwright tests:** `src/test_ui_edit_office_playwright.py`, `src/test_ui_offices_list_playwright.py`, `src/test_ui_run_playwright.py`. Run automatically on every PR via the `ui-tests` CI job. CI starts a fresh server against a temp DB — no office-ID vars required for CI. The `PLAYWRIGHT_EDIT_OFFICE_ID` / `PLAYWRIGHT_OFFICE_A_ID` / etc. vars are only needed for local runs against a pre-seeded database.

---

## Known Technical Debt

| Item | Detail | Planned fix |
|---|---|---|
| `runner_head.py` | UTF-16 encoded duplicate of `runner.py`. Not imported anywhere. | Delete |
| Dual schema | Flat `offices` table + `source_pages → office_details → office_table_config` coexist. All runs still read from `offices`. New fields must go in both. | Phase 6 (Refactor) |
| In-memory job stores | In-memory-only job stores (`_run_job_store` etc.) are lost on server restart; in-flight jobs show as abandoned. `scraper_jobs` table mitigates this for UI-triggered runs. | Phase 6 |
