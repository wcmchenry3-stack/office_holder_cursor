# -*- coding: utf-8 -*-
"""
AI-assisted office builder: uses OpenAI to analyze a Wikipedia page and
determine the table parsing config, then validates via preview and saves.
No FastAPI dependency — fully unit-testable.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Callable

import openai
from pydantic import BaseModel

from bs4 import BeautifulSoup

from src.db import offices as db_offices
from src.db import refs as db_refs
from src.scraper.config_test import get_all_tables_preview
from src.scraper.runner import preview_with_config
from src.scraper.table_cache import write_table_html_cache

# Wikipedia HTTP requests are made via wiki_session() (src/scraper/wiki_fetch.py),
# which sets the User-Agent header per Wikimedia API:Etiquette policy.

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OpenAI response schema — one entry per holder table found on the page
# ---------------------------------------------------------------------------


class AITableConfig(BaseModel):
    table_no: int
    table_rows: int = 1
    name: str
    link_column: int
    term_start_column: int = 0
    term_end_column: int = 0
    party_column: int = 0
    district_column: int = 0
    filter_column: int = 0
    filter_criteria: str = ""
    term_dates_merged: bool = False
    years_only: bool = False
    dynamic_parse: bool = True
    parse_rowspan: bool = False
    consolidate_rowspan_terms: bool = False
    read_right_to_left: bool = False
    use_full_page_for_table: bool = False
    remove_duplicates: bool = False
    ignore_non_links: bool = False
    party_ignore: bool = False
    district_ignore: bool = False
    district_at_large: bool = False
    reasoning: str = ""  # debug only — not saved to DB


class AIOfficePageResponse(BaseModel):
    tables: list[AITableConfig]  # empty list = no holder tables found on this page


# ---------------------------------------------------------------------------
# Parse failure analysis schema
# ---------------------------------------------------------------------------


class ParseGroupAnalysis(BaseModel):
    group_id: str  # fingerprint from ParseErrorReporter
    title: str  # short GitHub issue title (≤72 chars)
    root_cause: str  # why the parser fails on this HTML pattern
    suggested_fix: str  # code-level fix for the parser (not a data patch)
    suggested_tests: str  # unit tests + integration tests to write for the fix
    reproduction_steps: str  # how to reproduce: URL, function, input string


class ParseFailureAnalysisResponse(BaseModel):
    analyses: list[ParseGroupAnalysis]


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are an expert at reading Wikipedia HTML table structure.
Your job is to identify which tables on a Wikipedia page list people who held
a political or governmental office, and to determine the exact parsing
configuration needed to extract that data.

You will be given:
  1. A formatted text preview of each table (column indices shown on every
     row, cells containing a Wikipedia hyperlink marked with [LINK]).
  2. A raw HTML snippet (first few rows) of each table so you can inspect
     <a href>, rowspan, colspan, and <th> structure directly.

IDENTIFYING THE CORRECT COLUMNS:
- link_column: the column whose data cells contain <a href="/wiki/..."> links
  to individual people. In the text preview these cells are marked [LINK].
  This is the most important field — get it right or the preview will be empty.
- term_start_column / term_end_column: columns with start/end dates or years.
- party_column: column showing political party affiliation.
- district_column: column showing electoral district.
- filter_column / filter_criteria: use only when the table mixes holder types
  and you need to filter rows (e.g. filter_criteria="Senator").

RULES:
1. Only include tables that list PEOPLE WHO HELD AN OFFICE (i.e. office
   holders). Skip tables showing statistics, geography, election results by
   district, footnotes, navboxes, or any other general information.
2. For each holder table, output one entry in the 'tables' list.
3. ALL column indices are 1-BASED (first column = 1). Use 0 to mean "absent".
4. link_column REQUIRED: must be >= 1. It is the column marked [LINK] in the
   text preview, or the column whose HTML cells contain <a href="/wiki/...">.
5. table_rows: number of <tr> rows at the top to skip before data begins.
   Count ALL header rows including sub-headers. Usually 1; sometimes 2 or 3
   when the table has a multi-row header or a section label as the first row.
6. Set term_dates_merged = true when a single column contains a merged date
   range like "1990–1998". Set term_start_column = that column, term_end_column = 0.
7. Set parse_rowspan = true when <td rowspan="N"> merges a cell vertically
   across multiple rows (the same person listed across N consecutive rows).
   Example HTML:
     <tr><td rowspan="2"><a href="/wiki/Smith">Smith</a></td><td>1990</td><td>1994</td></tr>
     <tr><td>1994</td><td>1998</td></tr>
   → parse_rowspan=true, consolidate_rowspan_terms=true
8. Set consolidate_rowspan_terms = true when the same person appears in
   multiple consecutive rows that should be merged into one term record.
9. Set years_only = true when date columns contain only year numbers
   (e.g. "1998") rather than full dates ("January 3, 1998").
10. Set dynamic_parse = true (the default) to auto-detect section headers
    within the table body. Leave true unless you have a specific reason.
11. Infer office name from the Wikipedia page title or table caption. Use the
    most specific name possible (e.g. "Governor of California" not "Governor").
12. Populate reasoning with a brief explanation of your choices.
13. Return an empty tables list if no holder tables are found.

WHAT A SUCCESSFUL PARSE LOOKS LIKE:
The config is correct when:
  - At least half of data rows have a non-empty Wiki Link (a /wiki/Person URL).
  - Term Start / Term End columns show dates or years (not names or party info).
  - Party column (if set) shows party names, not dates or names.
If link_column is wrong, every row will have an empty Wiki Link — the most
common failure. Always double-check which column has the [LINK] markers.

EXAMPLE — typical holders table:
  Row 1 (header): [1] Name  [2] Party  [3] Term start  [4] Term end
  Row 2: [1] John Smith [LINK]  [2] Democrat  [3] January 3, 1991  [4] January 3, 1995
  Row 3: [1] Jane Doe [LINK]    [2] Republican  [3] January 3, 1995  [4] January 3, 1999
→ link_column=1, party_column=2, term_start_column=3, term_end_column=4,
  table_rows=1
"""


# ---------------------------------------------------------------------------
# AIOfficeBuilder
# ---------------------------------------------------------------------------


class AIOfficeBuilder:
    """
    Analyzes Wikipedia pages with OpenAI and creates office configs.

    model: gpt-4o-2024-08-06 supports structured outputs with Pydantic schemas,
    has a 128k context window (handles large table previews), and has strong
    HTML/table reasoning.
    """

    def __init__(self, api_key: str, model: str = "gpt-4o-2024-08-06"):
        self._client = openai.OpenAI(api_key=api_key)
        self._model = model

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def process_url_with_retries(
        self,
        url: str,
        batch_defaults: dict,
        max_retries: int = 5,
        cancel_check: "Callable[[], bool] | None" = None,
    ) -> dict:
        """
        Process one Wikipedia URL: analyze, validate, save, retry on failure.

        Returns:
        {
          "url": str,
          "status": "success" | "failed" | "no_tables" | "cancelled",
          "offices_created": [int, ...],
          "error": str | None,
          "attempts": int,
        }
        """
        result_base = {
            "url": url,
            "offices_created": [],
            "attempts": 0,
            "error": None,
            "status": "failed",
        }

        # ---- Step 1: Fetch all tables from the page (HTML included for cache priming) ----
        try:
            tables_preview = get_all_tables_preview(url, confirmed=True, include_html=True)
        except Exception as e:
            return {**result_base, "error": f"Page fetch error: {e}"}

        if tables_preview.get("error"):
            return {**result_base, "error": tables_preview["error"]}
        if not tables_preview.get("tables"):
            return {**result_base, "error": "No tables found on page"}

        # Prime the disk cache with already-fetched HTML so retries never re-fetch Wikipedia
        self._prime_table_cache(url, tables_preview)

        # ---- Step 2: Initial OpenAI analysis ----
        messages: list[dict] = []
        try:
            ai_response = self._analyze_page(url, tables_preview, messages)
        except Exception as e:
            return {**result_base, "error": str(e), "attempts": 1}

        if not ai_response.tables:
            return {**result_base, "status": "no_tables", "attempts": 1}

        # ---- Step 3: Validate + retry loop ----
        offices_created: list[int] = []
        all_errors: list[str] = []
        pending = list(ai_response.tables)
        attempt = 1

        while attempt <= max_retries:
            if cancel_check and cancel_check():
                return {
                    **result_base,
                    "status": "cancelled",
                    "offices_created": offices_created,
                    "attempts": attempt,
                }

            still_failing: list[tuple[AITableConfig, str, dict]] = []

            for config in pending:
                office_row = self._build_office_row(url, config, batch_defaults)
                try:
                    ok, err, preview = self._validate_config(office_row)
                except Exception as e:
                    ok, err = False, str(e)
                    preview = {"preview_rows": [], "error": str(e)}

                if ok:
                    try:
                        oid = db_offices.create_office(office_row)
                        offices_created.append(oid)
                        logger.info("AI builder: created office %d for %s", oid, url)
                    except Exception as e:
                        err_msg = str(e)
                        if "unique" in err_msg.lower() or "duplicate" in err_msg.lower():
                            all_errors.append(
                                f"Table {config.table_no}: office with this URL already "
                                "exists — add manually if needed"
                            )
                        else:
                            all_errors.append(f"Table {config.table_no}: DB error: {err_msg}")
                else:
                    still_failing.append((config, err, preview))

            attempt += 1

            if not still_failing:
                break

            if attempt > max_retries:
                for config, err, _ in still_failing:
                    all_errors.append(f"Table {config.table_no}: {err}")
                break

            # Build retry message and get corrected configs from OpenAI
            retry_content = self._build_retry_message(attempt - 1, still_failing)
            messages.append({"role": "user", "content": retry_content})
            try:
                corrected = self._call_openai(messages)
                corrected_by_no = {tc.table_no: tc for tc in corrected.tables}
                new_pending: list[AITableConfig] = []
                for old_config, _, _ in still_failing:
                    replacement = corrected_by_no.get(old_config.table_no)
                    if replacement is None and corrected.tables:
                        # Model may have renumbered; use first available
                        replacement = corrected.tables[0]
                    if replacement:
                        new_pending.append(replacement)
                    else:
                        all_errors.append(
                            f"Table {old_config.table_no}: OpenAI returned no corrected config"
                        )
                pending = new_pending
            except openai.AuthenticationError:
                raise  # propagate auth errors immediately
            except Exception as e:
                for config, _, _ in still_failing:
                    all_errors.append(f"Table {config.table_no}: OpenAI retry error: {e}")
                break

        final_status: str
        if offices_created:
            final_status = "success"
        elif not all_errors and not offices_created:
            final_status = "no_tables"
        else:
            final_status = "failed"

        return {
            "url": url,
            "status": final_status,
            "offices_created": offices_created,
            "error": "; ".join(all_errors) if all_errors else None,
            "attempts": attempt - 1,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _analyze_page(
        self,
        url: str,
        tables_preview: dict,
        messages: list[dict],
    ) -> AIOfficePageResponse:
        """Build initial messages and call OpenAI. Mutates messages in place."""
        messages.append({"role": "system", "content": _SYSTEM_PROMPT})
        messages.append(
            {"role": "user", "content": self._format_tables_message(url, tables_preview)}
        )
        return self._call_openai(messages)

    def _call_openai(self, messages: list[dict]) -> AIOfficePageResponse:
        """Call OpenAI with structured output, with exponential backoff on RateLimitError.

        Retries up to 3 times on HTTP 429 / openai.RateLimitError, doubling the
        backoff delay each attempt (1 s → 2 s → 4 s).  All other errors propagate
        immediately so callers can handle them.
        """
        backoff = 1.0
        for attempt in range(3):
            try:
                completion = self._client.beta.chat.completions.parse(
                    model=self._model,
                    messages=messages,
                    response_format=AIOfficePageResponse,
                    max_completion_tokens=4096,
                )
                choice = completion.choices[0]
                # Append assistant message for multi-turn continuity
                messages.append({"role": "assistant", "content": choice.message.content or ""})
                return choice.message.parsed  # type: ignore[return-value]
            except openai.RateLimitError:
                if attempt == 2:
                    raise
                logger.warning(
                    "_call_openai: RateLimitError (HTTP 429); retrying in %.0f s (attempt %d/3)",
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")

    def _check_success_criteria(self, preview_result: dict) -> tuple[bool, str]:
        """
        Returns (True, "") on success.
        Returns (False, reason) if:
          - preview has an error string
          - no preview rows returned
          - fewer than 50% of rows have a non-empty Wiki Link
        """
        if preview_result.get("error"):
            return False, f"Preview error: {preview_result['error']}"
        rows = preview_result.get("preview_rows") or []
        if not rows:
            return False, "Preview returned no rows"
        non_empty = sum(1 for r in rows if (r.get("Wiki Link") or "").strip())
        pct = non_empty / len(rows)
        if pct < 0.5:
            return (
                False,
                f"Only {non_empty}/{len(rows)} rows ({pct:.0%}) have a Wiki Link; "
                "need at least 50%. Check link_column.",
            )
        return True, ""

    def _build_office_row(
        self,
        url: str,
        ai_config: AITableConfig,
        batch_defaults: dict,
    ) -> dict:
        """Build a flat office dict compatible with preview_with_config and create_office."""
        country_id = int(batch_defaults.get("country_id") or 0)
        level_id = int(batch_defaults.get("level_id") or 0) or None
        branch_id = int(batch_defaults.get("branch_id") or 0) or None
        state_id = int(batch_defaults.get("state_id") or 0) or None
        city_id = int(batch_defaults.get("city_id") or 0) or None

        term_start = ai_config.term_start_column
        # Enforce the term_dates_merged invariant: end column must equal start column
        term_end = term_start if ai_config.term_dates_merged else ai_config.term_end_column

        row: dict = {
            "url": url.strip(),
            "name": (ai_config.name or "").strip(),
            "department": "",
            "notes": "",
            "enabled": 1,
            # Table config
            "table_no": ai_config.table_no,
            "table_rows": ai_config.table_rows,
            "link_column": ai_config.link_column,
            "party_column": ai_config.party_column,
            "term_start_column": term_start,
            "term_end_column": term_end,
            "district_column": ai_config.district_column,
            "filter_column": ai_config.filter_column,
            "filter_criteria": ai_config.filter_criteria,
            # Flags
            "dynamic_parse": ai_config.dynamic_parse,
            "read_right_to_left": ai_config.read_right_to_left,
            "find_date_in_infobox": False,  # never auto-enabled in batch (too slow)
            "years_only": ai_config.years_only,
            "parse_rowspan": ai_config.parse_rowspan,
            "consolidate_rowspan_terms": ai_config.consolidate_rowspan_terms,
            "rep_link": False,
            "party_link": False,
            "alt_links": [],
            "alt_link_include_main": False,
            "use_full_page_for_table": ai_config.use_full_page_for_table,
            "term_dates_merged": ai_config.term_dates_merged,
            "party_ignore": ai_config.party_ignore,
            "district_ignore": ai_config.district_ignore,
            "district_at_large": ai_config.district_at_large,
            "ignore_non_links": ai_config.ignore_non_links,
            "remove_duplicates": ai_config.remove_duplicates,
            "infobox_role_key_filter_id": None,
            "infobox_role_key": "",
            # Hierarchy IDs
            "country_id": country_id,
            "level_id": level_id,
            "branch_id": branch_id,
            "state_id": state_id,
            "city_id": city_id,
        }

        # Reference names required by preview_with_config for office_details
        row["country_name"] = db_refs.get_country_name(country_id)
        row["level_name"] = db_refs.get_level_name(level_id)
        row["branch_name"] = db_refs.get_branch_name(branch_id)
        row["state_name"] = db_refs.get_state_name(state_id)

        return row

    def _validate_config(self, office_row: dict) -> tuple[bool, str, dict]:
        """
        Validate then preview.
        Returns (success, error_message, preview_result).
        """
        # Cheap structural validation first (no network)
        try:
            db_offices.validate_office_table_config(
                office_row,
                term_dates_merged=bool(office_row.get("term_dates_merged")),
                party_ignore=bool(office_row.get("party_ignore")),
                district_ignore=bool(office_row.get("district_ignore")),
                district_at_large=bool(office_row.get("district_at_large")),
            )
        except ValueError as e:
            return False, str(e), {"preview_rows": [], "error": str(e)}

        # Full preview (fetches Wikipedia, parses table)
        preview = preview_with_config(office_row, max_rows=10)
        ok, reason = self._check_success_criteria(preview)
        return ok, reason, preview

    # ------------------------------------------------------------------
    # Prompt formatting
    # ------------------------------------------------------------------

    def _format_tables_message(self, url: str, tables_preview: dict) -> str:
        """
        Format table data from get_all_tables_preview() into a readable message for OpenAI.

        Each table section includes:
        - Column-indexed rows (every row, not just the header)
        - [LINK] marker on cells containing a /wiki/ hyperlink
        - A raw HTML snippet (first 3 rows) for rowspan/colspan inspection
        """
        num_tables = tables_preview.get("num_tables", 0)
        tables = tables_preview.get("tables") or []

        lines = [
            f"Wikipedia URL: {url}",
            f"The page has {num_tables} table(s). Column numbers are 1-based.",
            "Cells marked [LINK] contain a Wikipedia hyperlink (/wiki/...) — "
            "these cells identify the link_column.",
            "",
        ]

        for tbl in tables:
            idx = tbl.get("table_index", "?")
            rows = tbl.get("rows") or []
            raw_html = tbl.get("html") or ""
            lines.append(f"--- TABLE {idx} ---")

            if not rows:
                lines.append("  (empty table)")
            else:
                # Build per-cell link presence map from raw HTML when available
                link_map: dict[tuple[int, int], bool] = {}
                if raw_html:
                    link_map = self._build_link_map(raw_html)

                for row_idx, row in enumerate(rows):
                    label = "Row 1 (header)" if row_idx == 0 else f"Row {row_idx + 1}"
                    cells = []
                    for col_idx, cell in enumerate(row):
                        has_link = link_map.get((row_idx, col_idx), False)
                        marker = " [LINK]" if has_link else ""
                        cells.append(f"[{col_idx + 1}] {cell}{marker}")
                    lines.append(f"  {label}: {'  '.join(cells)}")

            # Raw HTML snippet — first 3 rows, so OpenAI can see rowspan/colspan/href
            if raw_html:
                soup = BeautifulSoup(raw_html, "html.parser")
                snippet_rows = soup.find_all("tr")[:3]
                if snippet_rows:
                    lines.append("  Raw HTML (first 3 rows):")
                    for tr in snippet_rows:
                        lines.append(f"    {tr}")

            lines.append("")

        lines.append(
            "Identify every table that lists office holders and return the JSON response. "
            "Return an empty tables list if none found."
        )
        return "\n".join(lines)

    def _build_link_map(self, table_html: str) -> dict[tuple[int, int], bool]:
        """
        Parse table HTML and return a dict mapping (row_idx, col_idx) → True
        for any cell that contains an <a href="/wiki/..."> link.
        row_idx and col_idx are 0-based, matching the rows list from get_all_tables_preview().
        Skips the first row (header).
        """
        link_map: dict[tuple[int, int], bool] = {}
        try:
            soup = BeautifulSoup(table_html, "html.parser")
            trs = soup.find_all("tr")
            for row_idx, tr in enumerate(trs):
                cells = tr.find_all(["td", "th"])
                for col_idx, cell in enumerate(cells):
                    if cell.find("a", href=lambda h: h and h.startswith("/wiki/")):
                        link_map[(row_idx, col_idx)] = True
        except Exception:
            pass
        return link_map

    def _prime_table_cache(self, url: str, tables_preview: dict) -> None:
        """
        Write each table's HTML into the disk cache so that _validate_config()
        never re-fetches Wikipedia during the retry loop.
        """
        num_tables = tables_preview.get("num_tables", 0)
        for tbl in tables_preview.get("tables") or []:
            html = tbl.get("html") or ""
            table_no = tbl.get("table_index", 1)
            if html:
                try:
                    write_table_html_cache(url, table_no, html, num_tables)
                except Exception as e:
                    logger.warning("_prime_table_cache: table %d: %s", table_no, e)

    def _build_retry_message(
        self,
        attempt: int,
        failing: list[tuple[AITableConfig, str, dict]],
    ) -> str:
        """Build a retry message explaining what went wrong and asking for corrections."""
        lines = [
            f"Attempt {attempt} failed for {len(failing)} table(s). "
            "Please review and correct the configurations:",
            "",
        ]
        for config, err, preview in failing:
            rows = preview.get("preview_rows") or []
            non_empty = sum(1 for r in rows if (r.get("Wiki Link") or "").strip())
            pct_str = f"{non_empty}/{len(rows)}" if rows else "0/0"
            lines += [
                f"TABLE {config.table_no}:",
                f"  Error: {err}",
                f"  Wiki Link fill rate: {pct_str} rows",
                "  First 5 preview rows (if any):",
            ]
            for r in rows[:5]:
                lines.append(
                    f"    Wiki Link={r.get('Wiki Link', '')!r}  "
                    f"Party={r.get('Party', '')!r}  "
                    f"Start={r.get('Term Start', '')!r}  "
                    f"End={r.get('Term End', '')!r}"
                )
            lines.append(
                f"  Previous config: {json.dumps(config.model_dump(exclude={'reasoning'}))}"
            )
            lines.append("")

        lines += [
            "Common fixes:",
            "- link_column wrong: find the column with /wiki/Person_Name hyperlinks",
            "- table_rows too low: increase to skip all header/sub-header rows",
            "- parse_rowspan needed: table uses HTML rowspan merging cells vertically",
            "- years_only: set true when cells show '1998' not 'January 1, 1998'",
            "- term_dates_merged: set true when one column shows '1990–1998'",
            "",
            "Return corrected AIOfficePageResponse with updated table_no entries.",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Parse failure analysis
    # ------------------------------------------------------------------

    def analyze_parse_failures(self, groups_data: list[dict]) -> list[ParseGroupAnalysis]:
        """Analyze parser failure groups and return structured analysis for GitHub issues.

        Each entry in groups_data represents one distinct failure group (same
        function + error type). One representative HTML snippet is included per
        group to keep token usage low.

        Rate limit / retry / backoff: exponential backoff on HTTP 429
        (3 retries, 1 s → 2 s → 4 s) via _call_parse_failure_openai.
        max_completion_tokens: 4096 (same cap as all other calls).
        """
        if not groups_data:
            return []

        lines = [
            f"You are analyzing {len(groups_data)} distinct parser failure group(s) from a "
            "Wikipedia scraper. Each group represents a unique (function, error_type) "
            "combination that silently failed. Your job is to:\n"
            "1. Identify the ROOT CAUSE in the parser code (not in the data).\n"
            "2. Suggest a SPECIFIC CODE-LEVEL FIX to the parser function.\n"
            "3. Describe TESTS to write (unit + integration) that cover the fix.\n"
            "4. Provide REPRODUCTION STEPS (Wikipedia URL, function name, input string).\n\n"
            "The goal is improving the parser for all future pages, not patching individual records.\n\n"
            "--- FAILURE GROUPS ---\n",
        ]

        for i, g in enumerate(groups_data, 1):
            lines.append(f"### Group {i} (id: {g['group_id']})")
            lines.append(f"Function: {g['function_name']}")
            lines.append(f"Error type: {g['error_type']}")
            lines.append(f"Occurrences this run: {g.get('occurrence_count', 1)}")
            if g.get("wiki_url"):
                lines.append(f"Wikipedia URL: {g['wiki_url']}")
            if g.get("office_name"):
                lines.append(f"Office: {g['office_name']}")
            if g.get("date_str"):
                lines.append(f"Input string that failed: {g['date_str']!r}")
            lines.append(f"Traceback (last 1000 chars):\n{g.get('traceback', '')}")
            lines.append(f"HTML snippet (up to 2000 chars):\n{g.get('html_snippet', '')}")
            lines.append("")

        user_content = "\n".join(lines)
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a senior Python developer specializing in HTML parsing and "
                    "Wikipedia data extraction. Analyze parser failures and produce actionable "
                    "GitHub issues with code-level fixes and test specifications."
                ),
            },
            {"role": "user", "content": user_content},
        ]
        response = self._call_parse_failure_openai(messages)
        return response.analyses

    def _call_parse_failure_openai(
        self, messages: list[dict]
    ) -> ParseFailureAnalysisResponse:
        """Call OpenAI with structured output for parse failure analysis.

        Rate limit / retry / backoff: exponential backoff on HTTP 429
        (3 retries, 1 s → 2 s → 4 s).
        """
        backoff = 1.0
        for attempt in range(3):
            try:
                completion = self._client.beta.chat.completions.parse(
                    model=self._model,
                    messages=messages,
                    response_format=ParseFailureAnalysisResponse,
                    max_completion_tokens=4096,
                )
                return completion.choices[0].message.parsed  # type: ignore[return-value]
            except openai.RateLimitError:
                if attempt == 2:
                    raise
                logger.warning(
                    "_call_parse_failure_openai: RateLimitError; retrying in %.0f s (attempt %d/3)",
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")
