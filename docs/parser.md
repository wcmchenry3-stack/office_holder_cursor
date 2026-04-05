# Parser Reference

The parser subsystem converts Wikipedia HTML tables into structured `office_terms` rows. It lives in `src/scraper/` and is the most complex part of the codebase.

---

## Core files

| File | Role |
|---|---|
| `src/scraper/table_parser.py` | All HTML parsing: `DataCleanup`, `Offices`, `Biography` classes |
| `src/scraper/parse_core.py` | Re-exports `DataCleanup`, `Offices`, `Biography` for external callers |
| `src/scraper/runner.py` | Orchestrates parse passes, structural change detection, nolink lifecycle |
| `src/scraper/wiki_fetch.py` | Wikipedia HTTP fetches with User-Agent, rate limiting, retry/backoff |

---

## Core classes

### `DataCleanup`
HTML pre-processing before any column extraction. Handles entity decoding, cell normalization, stripping citation superscripts, and resolving `rowspan` / `colspan` attributes into a flat cell grid.

### `Offices`
Top-level orchestrator. Drives per-office parsing via `process_table(table_config, office_details)`. Returns a list of parsed term rows ready for DB upsert.

### `Biography`
Fetches and parses Wikipedia infobox HTML for individual vital statistics (birth date, death date, place of birth, place of death). Only invoked when `find_date_in_infobox=True` — requires a second HTTP fetch per individual.

---

## Column mapping

All column indices are **1-based** in the UI and in `office_table_config` (the number users enter in the office config form). They are converted to **0-based** Python indices before the parser is called — this conversion happens in `runner.py`, not inside the parser.

| Config field | UI value | Python value |
|---|---|---|
| `name_column` | 1 = first column | 0 |
| `party_column` | 2 = second column | 1 |
| `term_start_column` | 3 = third column | 2 |

Required: `name_column`. All others are optional (0 means "not configured").

### `consolidate_rowspan_terms`
When `True`, consecutive rows with the same holder name are merged into a single term. Different from the `rowspan` HTML attribute handling in `DataCleanup` (which flattens the cell grid before column extraction).

### `term_dates_merged`
When `True`, `term_end_column` is ignored. `term_start_column` is expected to hold a date range like `"2010–2015"` that the parser splits into start and end dates.

---

## Date extraction

Date extraction uses a two-step strategy:

1. **`data-sort-value` attribute** — Wikipedia sortable tables embed machine-readable dates as `data-sort-value="YYYY-MM-DD"` on `<td>` elements. The parser checks this first.
2. **Cell text fallback** — regex and heuristic parsing of human-readable dates (`"January 5, 2010"`, `"2010"`, `"5 Jan 2010"`, etc.).

If a date cannot be resolved to `YYYY-MM-DD`, the date column is set to `NULL` and the corresponding `*_imprecise` flag is set to `1` (e.g. `term_start_imprecise=1`).

### `years_only` mode
When `years_only=True` on the office config, only the year integer is extracted (`term_start_year`, `term_end_year`). Full `YYYY-MM-DD` columns remain `NULL`.

---

## Parse bounds and dynamic parsing

### `parse_start_row` / `parse_end_row`
Slice the table to a specific row range before parsing. Used to skip header or footer rows that are structurally part of the `<table>` element but are not term data.

### `dynamic_parse`
When `True`, each row is classified as either an office-header row or a term-data row, allowing mixed tables with section headers interspersed with data rows.

---

## Infobox lookup

`find_date_in_infobox=True` triggers a second Wikipedia fetch per individual to extract birth/death data from the infobox. This is slow — only enable it when the HTML table does not contain date columns.

`infobox_role_key_filter_id` (FK to `infobox_role_key_filters` table) narrows which infobox role entries to accept. For example, a filter set to "Governor" will only use infobox entries labelled with that role key, ignoring entries for other offices the individual may have held.

The legacy `infobox_role_key` text field on `offices` / `office_table_config` is a simpler free-text predecessor to the FK-based filter system. Both may be active on older records.

---

## HTML hash (unchanged-page skip)

After fetching the Wikipedia table HTML, the runner computes a SHA-256 hash of the raw HTML. If the hash matches `office_table_config.last_html_hash` from the previous run, the page is skipped entirely — no parse, no DB write. The hash is updated in `office_table_config` after every successful write.

---

## Structural change detection (link fill rate)

After parsing a table, the runner computes the **link fill rate**: the fraction of parsed holder rows that have a Wikipedia link (0.0–1.0). This is stored in `office_table_config.last_link_fill_rate`.

On the next run, if the new fill rate drops more than **30 percentage points** below the stored baseline, the runner:
1. Logs a warning.
2. Creates a GitHub issue labelled `structural-change` with office name, source page URL, and before/after fill rates.

This signals that the Wikipedia table's column layout has likely changed (e.g. a new column was inserted before the name column, shifting all indices). No DB write is blocked — the detection is advisory.

The threshold is `_FILL_RATE_DROP_THRESHOLD = 0.30` in `runner.py`.

---

## No-link placeholder lifecycle

When a holder row has no Wikipedia link, the parser creates a **no-link placeholder individual** with `wiki_url = NULL` and a synthetic name like `"No link:office_id:Holder Name"`.

On a subsequent parse, if the same holder now has a Wikipedia link, `runner.py` calls `_maybe_supersede_nolink()` which:

1. Looks up the existing no-link placeholder by `(office_id, name)` via `db_individuals.find_nolink_by_name_and_office()`.
2. Marks the old placeholder as superseded: `db_individuals.mark_superseded(old_id, new_individual_id)` sets `superseded_by_individual_id` on the old row.
3. Reassigns any `office_terms` rows referencing the old placeholder to the new linked individual.
4. Writes an audit row to `nolink_supersede_log`: `old_individual_id`, `new_individual_id`, `office_id`, `old_wiki_url`, `new_wiki_url`, `office_terms_reassigned`, `superseded_at`.

The log provides full audit visibility for no-link retirements.

---

## Holder key and deduplication

The parser uses URL-based canonical keys to detect existing holders. `canonical_holder_url()` produces `/wiki/<title>` (lowercased). Dead links (`?redlink=1`) are skipped. No-link rows produce keys of the form `"No link:office_id:name"`.

Holder matching is URL-only — name changes on Wikipedia do not create duplicates as long as the URL redirect is resolved.

---

## Parser test fixtures

### Manifest
`test_scripts/manifest/parser_tests.json` is the canonical parser regression suite. Each entry:

```json
{
  "name": "descriptive test name",
  "test_type": "table",
  "html_file": "fixtures/my_table.html",
  "source_url": "https://en.wikipedia.org/wiki/...",
  "config_json": { "name_column": 1, "term_start_column": 2, ... },
  "expected_json": [ { "full_name": "...", "term_start": "2010-01-01", ... } ],
  "enabled": true
}
```

- `html_file` — committed HTML fixture in `test_scripts/fixtures/`; a snapshot of the Wikipedia table at a point in time
- `config_json` — same shape as an `offices` row; mirrors what you would configure in the UI
- `expected_json` — expected array of parsed output rows; the test asserts exact equality

### CI validation
The `Validate parser fixtures` CI job runs `validate_parser_fixtures.py` on every PR. It checks manifest integrity (all referenced HTML files exist, all fields present) but does not re-run the parse assertions — those run in `pytest`.

### Workflow
When fixing a parser bug:
1. Add a new HTML fixture capturing the broken table.
2. Add a manifest entry with the expected corrected output.
3. Run `pytest tests/test_scenarios.py` locally to confirm the fix.
4. Include the fixture in the PR — scenario tests are expected as part of the feature, not a follow-up.

---

## GitHub issue pipeline for parse errors

`src/scraper/parse_error_reporter.py` captures parser exceptions and creates fingerprinted GitHub issues (label: `parser-bug`). Deduplication uses a fingerprint hash of `(function_name, error_type, wiki_url, office_name)` — the same error from the same source only creates one issue regardless of how many runs encounter it. Sentry breadcrumbs and exception captures are added at the same call site.
