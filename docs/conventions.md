# Conventions, Gotchas & Technical Debt

## Key Conventions

- **Migrations:** Always idempotent. Check `PRAGMA table_info(...)` or `PRAGMA index_list(...)` before altering. Never alter schema manually — always add a function to `migrate_to_fk()` in `src/db/migrate.py`.
- **DB rows:** `sqlite3.Row` with `row_factory` — access columns by name like a dict. Passed around as `dict[str, Any]`.
- **Party matching:** By `country_id` + `party_name` or `party_link`. Unmatched parties stored as text; matched parties get `party_id` FK.
- **Office row helpers:** Always use `db_offices.office_row_to_table_config()` and `office_row_to_office_details()` to convert a flat `offices` row into structured dicts for the parser. Don't build config dicts manually.
- **Progress callbacks:** `run_with_db()` accepts an optional `progress_callback(phase, current, total, message, extra_dict)` for streaming progress to the UI.
- **Wikipedia requests:** Always use `WIKIPEDIA_REQUEST_HEADERS` from `wiki_fetch.py` (includes User-Agent + gzip per Wikimedia policy).
- **No DB write in dry_run/test_run:** `run_with_db(dry_run=True)` or `test_run=True` skips all DB writes.

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

**Unit tests:** Scattered across `src/scraper/test_*.py` and `src/db/test_*.py`. No shared `conftest.py` yet.

**Playwright tests:** `src/test_ui_edit_office_playwright.py`. Require manual `PLAYWRIGHT_*` env var setup. Not currently wired into CI.

---

## Known Technical Debt

| Item | Detail | Planned fix |
|---|---|---|
| `runner_head.py` | UTF-16 encoded duplicate of `runner.py`. Not imported anywhere. | Delete |
| `pytest` missing | Tests exist but `pytest` is not in `requirements.txt`. | Phase 4 |
| Dual schema | Flat `offices` table + `source_pages → office_details → office_table_config` coexist. All runs still read from `offices`. New fields must go in both. | Phase 6 (Refactor) |
| No `conftest.py` | Tests lack shared fixtures; each file manages its own DB setup. | Phase 4 |
| Playwright not in CI | Requires manual env setup and a running server. | Phase 4 |
| In-memory job stores | `_run_job_store` etc. in `main.py` are lost on server restart; in-flight runs abandoned silently. | Phase 6 |
