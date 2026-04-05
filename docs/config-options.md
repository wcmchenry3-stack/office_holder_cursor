# Office Table Config Options

All fields on `office_table_config` (and their equivalents on the legacy `offices` table). These control how the HTML table parser extracts office holder data from a Wikipedia page.

---

## Column Index Fields

> **Important:** Column indices are **1-based** in the UI and in the database. The parser internally converts to **0-based** before processing. Column 0 means "not used."

| Field | Default | Description |
|---|---|---|
| `table_no` | 1 | Which table on the Wikipedia page to parse (1 = first table) |
| `table_rows` | 4 | Number of header rows to skip at the top of the table |
| `link_column` | 1 | Column containing the Wikipedia link for the office holder |
| `party_column` | 0 | Column containing party affiliation (0 = not used) |
| `term_start_column` | 4 | Column containing term start date |
| `term_end_column` | 5 | Column containing term end date (ignored if `term_dates_merged=True`) |
| `district_column` | 0 | Column containing district/constituency info (0 = not used) |
| `filter_column` | 0 | Column to apply `filter_criteria` against (0 = not used) |

---

## Row Filtering

| Field | Default | Description |
|---|---|---|
| `filter_column` | 0 | Column index to check (1-based; 0 = disabled) |
| `filter_criteria` | `""` | Substring that must appear in `filter_column` cell text; rows not matching are skipped |

Example: `filter_column=3, filter_criteria="Governor"` keeps only rows where column 3 contains "Governor".

---

## Date Handling

| Field | Default | Description |
|---|---|---|
| `term_dates_merged` | 0 | When 1: `term_end_column` is ignored; `term_start_column` contains a date range (e.g. "2010–2015") that is split into start and end |
| `years_only` | 0 | When 1: only extract year integers (stored in `term_start_year`/`term_end_year`); full dates remain NULL |
| `find_date_in_infobox` | 0 | When 1: fetch the individual's Wikipedia infobox page to extract birth/death dates when missing from the table |

**Date parsing fallback order:**
1. `data-sort-value` attribute on `<td>` element (Wikipedia sortable tables)
2. Cell text parsed with `DataCleanup.format_date()`
3. Infobox fetch (if `find_date_in_infobox=True`)
4. NULL + `*_imprecise=1` if none of the above yield a valid `YYYY-MM-DD`

---

## Parsing Behavior Flags

| Field | Default | Description |
|---|---|---|
| `dynamic_parse` | 1 | Auto-detect whether a row is an office-header row or a term-data row; allows mixed tables with section headers |
| `parse_rowspan` | 0 | Handle HTML `rowspan` attributes — merge cell values across spanned rows before parsing |
| `consolidate_rowspan_terms` | 0 | After parsing, merge consecutive rows with the same holder URL into a single term (different from `parse_rowspan`) |
| `read_right_to_left` | 0 | Reverse column order before parsing (for right-to-left layout tables) |
| `use_full_page_for_table` | 0 | Fetch full Wikipedia page HTML (includes nav/sidebar) instead of REST API content-only HTML. Use when `table_no` should match the full page table index, not just the content area |
| `remove_duplicates` | 0 | After parsing, remove rows with identical `(wiki_link, term_start, term_end, party, district)` |
| `ignore_non_links` | 0 | Skip rows that have no Wikipedia link in `link_column` |

---

## Link & Party Options

| Field | Default | Description |
|---|---|---|
| `rep_link` | 0 | When 1: the link in `link_column` is a representative/person link (standard). When 0: link may be to an office page |
| `party_link` | 0 | When 1: match party by the link URL in `party_column` in addition to text matching |
| `alt_link_include_main` | 0 | On `office_details`: also include the main office page URL when searching for infobox alt links |

---

## District / Party Ignore Flags

| Field | Default | Description |
|---|---|---|
| `party_ignore` | 0 | When 1: ignore `party_column` entirely (don't attempt party matching) |
| `district_ignore` | 0 | When 1: ignore `district_column` entirely |
| `district_at_large` | 0 | When 1: set all district values to "At Large" regardless of what's in `district_column` |

---

## Infobox Role Key

Two systems exist for filtering infobox entries. The FK-based system (`infobox_role_key_filter_id`) is preferred for new configs.

| Field | Default | Description |
|---|---|---|
| `infobox_role_key_filter_id` | NULL | FK to `infobox_role_key_filter.id`; when set, filters which infobox role entries are used for date extraction (preferred) |
| `infobox_role_key` | `""` | Legacy free-text role key; a substring to match against infobox role entry keys. Predates the FK-based system. Still active on older records; new configs should use `infobox_role_key_filter_id` instead |

If both are set, `infobox_role_key_filter_id` takes precedence.

### Query Syntax (`role_key` field)

The `role_key` field on `infobox_role_key_filter` uses a simple include/exclude syntax:

```
"judge"                        → include entries with role key matching "judge"
"judge" "associate justice"    → include entries matching "judge" OR "associate justice"
"judge" -"chief judge"         → include "judge" but exclude "chief judge"
"senator" "representative" -"senator pro tempore"  → multiple includes + exclude
```

Rules:
- Terms in `"double quotes"` are matched as substrings (case-insensitive)
- Terms prefixed with `-"..."` are excluded
- Include terms are OR'd; a row matches if it contains any include term
- Exclude terms are AND'd; a row is excluded if it matches any exclude term
- Exclusions take priority over inclusions

---

## Computed / Cache Fields (read-only)

These fields are written by the scraper after each run. They are not user-configurable.

| Field | Description |
|---|---|
| `last_html_hash` | SHA-256 hash of the last fetched table HTML. If the hash matches on the next run, the page is skipped entirely (no parse, no DB write). |
| `last_link_fill_rate` | Fraction (0.0–1.0) of parsed holder rows that had a Wikipedia link. Stored after each parse. A drop of >30 percentage points triggers a structural-change GitHub issue. See `docs/parser.md`. |

---

## `name` and `notes`

| Field | Description |
|---|---|
| `name` | Optional human-readable label for this table config (e.g. "Senate terms 1960–present") |
| `notes` | Free-text notes for the config |

---

## Enabled Flag

| Field | Default | Description |
|---|---|---|
| `enabled` | 1 | When 0: this table config is skipped during scraper runs |

The `enabled` flag exists on `offices`, `office_details`, `office_table_config`, and `source_pages`. A page being disabled does not automatically disable its child offices.
