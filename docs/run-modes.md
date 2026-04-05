# Run Modes — Detailed Reference

This document describes each scraper run mode in detail, including what DB writes are made, when work is skipped, and edge cases.

All modes are triggered via `run_with_db(run_mode=..., ...)` in `src/scraper/runner.py`.

---

## `delta` (default)

**Trigger:** Default mode; used for routine updates.

**Behavior:**
1. Load all enabled offices from DB (`db_offices.list_offices()`)
2. Load party list for the scraper's country scope
3. For each office:
   a. Fetch table HTML (via cache or live fetch)
   b. Parse the table → list of row dicts
   c. Deduplicate rows if `remove_duplicates=True`
   d. For each row: normalize dates, skip invalid rows
   e. Compare parsed rows against existing `office_terms` for that office
   f. **Insert** new terms not in DB; **update** changed terms; **leave** unchanged terms alone
   g. If `find_date_in_infobox=True`: fetch infobox bio for each new individual
4. Biography refresh: only for individuals added this run or with `is_living=1` and no `death_date`

**DB writes:**
- `office_terms`: INSERT or UPDATE only changed/new rows; existing unchanged rows are untouched
- `individuals`: INSERT new; UPDATE if bio data changed
- `source_pages.last_scraped_at`: updated per page

**When work is skipped:**
- Rows that parse identically to existing terms → no write
- Individuals with `death_date` set and not newly added → no infobox fetch

**Edge cases:**
- If a new parse has fewer holders than existing terms → triggers auto-table-update logic (see below)

---

## `full`

**Trigger:** UI "Full Run" button.

**Behavior:**
1. Delete all `office_terms` for all enabled offices
2. Optionally delete all `individuals` (if `purge_individuals=True`)
3. Re-parse all enabled offices (same as delta steps 3a–3g)
4. Full biography refresh for all individuals

**DB writes:**
- DELETE all existing `office_terms` for enabled offices before re-inserting
- INSERT all parsed rows as new terms
- UPDATE all individual bios

**When to use:** After major config changes, schema changes, or when you suspect data corruption. Not for routine updates.

---

## `live_person`

**Trigger:** UI "Live Person Update" button.

**Behavior:** Identical to `delta` for office table parsing, but additionally:
- After parsing, refreshes biography for **all** individuals with `is_living=1` and no `death_date`
- This catches living people whose Wikipedia pages may have been updated since last run

**DB writes:** Same as delta, plus bio updates for all living individuals.

---

## `single_bio`

**Trigger:** UI: individual detail page "Refresh Bio" button.

**Parameters:** `individual_ref` — either an integer ID or a Wikipedia URL string.

**Behavior:**
1. Look up individual by ID or `wiki_url`
2. Fetch their Wikipedia infobox page
3. Extract biography data (birth/death dates, places, full name)
4. Update `individuals` row

**DB writes:** UPDATE one `individuals` row.

**No office table parsing.** Only bio data is updated.

---

## `selected_bios`

**Trigger:** UI: batch selection action on individuals list.

**Parameters:** `individual_ids` — list of integer IDs.

**Behavior:**
1. For each individual ID: fetch infobox, extract bio, update DB
2. Optional: `limit_no_death_date=True` — skip individuals who already have `death_date`
3. Optional: `filter_valid_page_path=True` — skip individuals with invalid/dead page paths
4. Optional: `force_update=True` — update even if individual already has `death_date`

**DB writes:** UPDATE `individuals` rows for all specified IDs.

---

## `bios_only`

**Trigger:** UI "Bios Only" button.

**Behavior:**
1. Skip all office table parsing entirely
2. For every individual in DB: fetch infobox, update bio

**DB writes:** UPDATE all `individuals` rows.

**When to use:** When you only need to refresh biographical data without re-scraping office holder tables.

---

## `category_bios`

**Trigger:** UI: category page "Run Bios" action.

**Parameters:** `category_id` — integer ID of an `office_category`.

**Behavior:**
1. Find all individuals linked to offices in the specified category
2. Run bio refresh for those individuals only
3. Respects same optional filters as `selected_bios`

**DB writes:** UPDATE `individuals` rows for individuals in the category.

---

## `delta_insufficient_vitals`

**Trigger:** Scheduled daily at 07:00 UTC (APScheduler).

**Behavior:**
1. Calculate today's batch: `date.today().day % 30`
2. Query individuals with insufficient vitals in that batch (broadened criteria: `birth_date IS NULL` OR `death_date IS NULL AND is_living = 0`)
3. For each individual: fetch Wikipedia bio to find vitals
4. Mark `insufficient_vitals_checked_at = NOW()` (30-day cooldown)

**DB writes:** UPDATE `individuals` with found vitals.

---

## `gemini_vitals_research`

**Trigger:** Scheduled daily at 08:00 UTC (APScheduler), or manual via `run_mode="gemini_vitals_research"`.

**Behavior:**
1. Calculate today's batch: `date.today().day % 30`
2. Query individuals with insufficient vitals in that batch (same broadened criteria as above) but with **90-day** cooldown via `gemini_research_checked_at`
3. For each individual:
   a. **Gemini API** researches vitals from government, academic, genealogical sources
   b. If vitals found → `upsert_individual()` immediately (individual drops out of future batches)
   c. Store research sources in `individual_research_sources`
   d. **OpenAI** polishes Gemini's findings into a wikitext Wikipedia article
   e. Store article draft in `wiki_draft_proposals` (status: pending)
   f. Mark `gemini_research_checked_at = NOW()`

**DB writes:**
- `individuals`: UPDATE with found vitals (birth/death dates, places)
- `individual_research_sources`: INSERT found sources
- `wiki_draft_proposals`: INSERT article drafts

**Two-stage AI pipeline:** Gemini does research → OpenAI writes the article. OpenAI uses ONLY Gemini's findings (no independent research). Sources flow through as `<ref>` tags.

**Env var:** `GEMINI_OFFICE_HOLDER` — if not set, feature is silently disabled.

**Policy:** See runner.py docstring for full Gemini API policy compliance details.

---

## `dead_link_research`

**Trigger:** Manual via `run_mode="dead_link_research"`, or schedulable via APScheduler.

**Behavior:**
1. Calculate today's batch: `date.today().day % 30`
2. Query individuals where `is_dead_link = 1` OR `wiki_url LIKE 'No link:%'` in that batch, with **90-day** cooldown via `gemini_research_checked_at`
3. For each individual:
   a. **Gemini API** researches vitals and biographical data from external sources
   b. If vitals found → `upsert_individual()` immediately
   c. Store research sources in `individual_research_sources`
   d. **Notability threshold** (deterministic, no AI): requires ≥2 independent sources (Wikipedia mirrors excluded), ≥1 government/academic source, and verifiable term dates
   e. If notable AND enough data → **OpenAI** polishes findings into a wikitext Wikipedia article
   f. Store article draft in `wiki_draft_proposals` (status: pending)
   g. Mark `gemini_research_checked_at = NOW()`

**DB writes:**
- `individuals`: UPDATE with found vitals (birth/death dates, places)
- `individual_research_sources`: INSERT found sources
- `wiki_draft_proposals`: INSERT article drafts (only if notability threshold met)

**Notability gate:** Articles are only generated when the deterministic threshold passes. This prevents low-quality drafts from individuals with insufficient sourcing.

**Env var:** `GEMINI_OFFICE_HOLDER` — if not set, feature is silently disabled.

**Policy:** Same Gemini/OpenAI policy compliance as `gemini_vitals_research`. Wikipedia submit (when used) sets User-Agent per Wikimedia API:Etiquette and respects rate limits.

---

## `data_quality`

**Trigger:** Manual via `run_mode="data_quality"`.

**Behavior:**
1. Query `individuals` with missing or placeholder `wiki_url` values
2. Run full data quality pipeline: deterministic checks → OpenAI → Gemini → Claude
3. Persist flagged issues to `data_quality_reports` table
4. No scraping or bio updates — quality checks only

**DB writes:**
- `data_quality_reports`: INSERT flagged issues (fingerprint-deduplicated)

**AI token usage:** This is the only mode that invokes AI quality checks. Requires at least one AI API key (`OPENAI_API_KEY`, `GEMINI_OFFICE_HOLDER`, or `ANTHROPIC_API_KEY`). If no keys are set, the mode exits immediately. OpenAI calls use max_completion_tokens to cap cost and include RateLimitError retry with exponential backoff (see `data_quality_checker.py`).

**Auto mode (end-of-run):** When `DATA_QUALITY_ENABLED=1`, deterministic-only quality checks run automatically at the end of every `delta`, `full`, or `live_person` run. These checks use zero AI tokens — only date validation, wiki URL checks, and party resolution are performed.

---

## `auto_fix`

**Trigger:** Manual via `run_mode="auto_fix"`.

**Behavior:**
1. Query GitHub for open issues with `parser-bug` label
2. Filter to issues created by `ParseErrorReporter` (must have `parse-error:pf-*` label)
3. For each qualifying issue:
   a. Read the source file from the repo (`src/scraper/table_parser.py`)
   b. Send issue + source to **Claude API** for a fix proposal
   c. Check proposal against 7 **minimal risk criteria** (deterministic, no AI):
      - Files changed exclusively within `src/scraper/`
      - Diff < 50 lines
      - No DDL statements (ALTER/CREATE/DROP TABLE)
      - No new imports for packages not in requirements.txt
      - No changes to public function signatures
      - At least one new `def test_` function
      - `error_type` is ValueError, TypeError, IndexError, or AttributeError
   d. If all criteria pass → create branch `fix/parser-auto-<fingerprint>`, apply fix, open **draft PR** targeting `dev`

**Safety:** PR is always opened as **draft** — CI runs but auto-merge is never triggered. Criteria check is pure Python — AI never decides what's "safe."

**Env vars:** `ANTHROPIC_API_KEY` + `GITHUB_TOKEN` — if either is unset, feature is silently disabled. No issue is modified if criteria fail.

**Policy:** See `claude_client.py` docstring for Anthropic API compliance. See `github_client.py` for GitHub API rate-limit handling.

---

## Auto-Table-Update Algorithm

When a delta (or full) run parses a table and finds that some existing `office_terms` holders are **missing** from the new parse, the runner checks whether the table numbering has changed on the Wikipedia page (this happens when Wikipedia editors add or remove tables).

**Trigger condition:** `disable_auto_table_update=0` (default) on `source_pages`, AND new parse is missing at least one existing holder.

**Algorithm (`_try_auto_update_table_no()`):**
1. Fetch all tables on the same Wikipedia page
2. For each candidate table (excluding the current `table_no`):
   a. Parse it with the same office config
   b. Count how many existing holders are still missing
3. Pick the candidate table that **minimizes** missing holders
4. If the best candidate has fewer missing holders than the current table → auto-update `table_no` in DB and use the new table's parse results
5. If no candidate improves the match → log `missing_holders` but proceed with the current parse (safe default: keeps existing terms)

**Disable per-page:** Set `disable_auto_table_update=1` on `source_pages` to skip this logic for that page.

**Note:** With the per-run HTML cache (Phase 5), all tables on a page are already in memory, so step 1 is a re-parse, not a re-fetch.

---

## Infobox Lookup Conditions

Infobox fetches are the slowest part of a run (one HTTP request per individual). They are triggered when:

1. `find_date_in_infobox=True` is set on the office config, AND
2. One of:
   - The individual was newly added in this run
   - The individual has no `death_date` and `is_living=1`
   - The run mode is `live_person`, `bios_only`, `selected_bios`, `category_bios`, or `single_bio`

**Infobox role key filtering:** When `infobox_role_key_filter_id` is set, only infobox entries matching the role key query are used. See `docs/config-options.md` for query syntax.

---

## `page_quality` (Scheduled Only)

**Trigger:** APScheduler cron job `daily_page_quality` — default 09:00 UTC. Not exposed as a user-selectable run mode in the UI.

**Purpose:** Verify that the office holder data we have parsed matches the current Wikipedia page. Uses `ConsensusVoter` (3-AI parallel vote) to detect drift between our DB records and live Wikipedia content.

**Guard conditions (run is skipped entirely if any fail):**
- `RUNNERS_ENABLED` env var is truthy
- `daily_page_quality` job is not paused in `scheduler_settings`

**Algorithm (`run_daily_page_quality()` in `src/scheduled_tasks.py`):**
1. Pick one `source_page` using unchecked-first LRU order (pages never checked are prioritized; among checked, oldest `last_quality_checked_at` first)
2. Fetch current Wikipedia HTML via the REST API (first 50,000 chars)
3. Load our parsed `office_terms` + `individuals` for that page from DB
4. If no records exist (`no_data`): skip this page and try the next one — up to 10 attempts (`_PAGE_QUALITY_MAX_ATTEMPTS`)
5. Build a prompt with the Wikipedia HTML and our JSON records
6. Call `ConsensusVoter` — three providers vote in parallel (OpenAI, Gemini, Claude)
7. Act on the verdict:
   - **VALID (all agree accurate):** mark `source_pages.last_quality_checked_at`, log `result='ok'`
   - **INVALID (all agree inaccurate):** trigger AI re-parse via `AIOfficeBuilder`; re-vote:
     - Re-vote VALID → `result='reparse_ok'`
     - Re-vote INVALID → create GitHub issue with `page-quality` label → `result='gh_issue'`
   - **DISAGREEMENT / INSUFFICIENT_QUORUM:** create GitHub issue for manual review → `result='manual_review'`
   - **Fetch failure:** log `result='fetch_failed'`

**Result codes written to `scheduled_job_runs.result_json`:**

| `result` | Meaning |
|---|---|
| `ok` | Consensus valid — data matches Wikipedia |
| `reparse_ok` | Was invalid; re-parsed and re-voted valid |
| `gh_issue` | Invalid after re-parse; GitHub issue opened |
| `manual_review` | Mixed/quorum-fail verdict; GitHub issue opened for human review |
| `fetch_failed` | Could not fetch Wikipedia HTML |
| `no_data` | Single-page result only; outer loop retries |
| `skipped_no_data` | All 10 attempts returned `no_data`; run recorded but no AI called |
| `no_pages` | No enabled `source_pages` exist |

The `attempts` field in `result_json` always reflects how many source pages were tried before a final result was reached.

**DB writes:**
- `page_quality_checks`: one row per page inspected (verdict, result, concerns JSON)
- `source_pages.last_quality_checked_at`: updated after a successful inspection
- `scheduled_job_runs`: one row created at start, updated with final result on finish

**Env vars required:** `OPENAI_API_KEY`, `GEMINI_OFFICE_HOLDER`, `ANTHROPIC_API_KEY` (consensus voter uses all three). `GITHUB_TOKEN` + `GITHUB_REPO` required only if a GitHub issue needs to be opened. Missing keys cause that provider to be skipped; quorum rules still apply.

**Cron time:** Configured via `cron_daily_page_quality_hour` / `cron_daily_page_quality_minute` in `app_settings` (see `docs/operational-settings.md`). Change takes effect on next restart.

---

## Progress Callback

`run_with_db()` accepts an optional `progress_callback(phase, current, total, message, extra_dict)` callable. The UI polls a job endpoint that reads from the in-memory job store updated by this callback.

Phases reported:
- `"parse"` — parsing office tables
- `"infobox"` — fetching individual infoboxes
- `"bio"` — updating biographies
- `"complete"` — run finished
- `"error"` — run failed
