# -*- coding: utf-8 -*-
"""
AI-assisted office builder: uses OpenAI to analyze a Wikipedia page and
determine the table parsing config, then validates via preview and saves.
No FastAPI dependency — fully unit-testable.
"""

from __future__ import annotations

import json
import logging
from typing import Callable

import openai
from pydantic import BaseModel

from src.db import offices as db_offices
from src.db import refs as db_refs
from src.scraper.config_test import get_all_tables_preview
from src.scraper.runner import preview_with_config

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
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are an expert at reading Wikipedia HTML table structure.
Your job is to identify which tables on a Wikipedia page list people who held
a political or governmental office, and to determine the exact parsing
configuration needed to extract that data.

RULES:
1. Only include tables that list PEOPLE WHO HELD AN OFFICE (i.e. office
   holders). Skip tables showing statistics, geography, election results by
   district, footnotes, navboxes, or any other general information.
2. For each holder table, output one entry in the 'tables' list.
3. ALL column indices are 1-BASED (first column = 1).
4. link_column REQUIRED: the column whose cells contain Wikipedia hyperlinks
   to individual office holders' pages (e.g. /wiki/John_Smith). Must be >= 1.
5. term_start_column / term_end_column: columns with start/end dates. Set to
   0 if absent.
6. party_column, district_column, filter_column: 1-based; 0 = not present.
7. table_rows: number of header rows to skip at the top before data starts.
   Usually 1, sometimes 2 when there is a sub-header row.
8. Set term_dates_merged = true when a single column contains a merged date
   range like "1990–1998". In that case set term_start_column = that column
   and term_end_column = 0 (it will be set equal to term_start_column
   automatically).
9. Set parse_rowspan = true when the table uses HTML rowspan to merge
   consecutive cells for the same person across multiple rows.
10. Set consolidate_rowspan_terms = true when the same person appears in
    multiple consecutive rows and those rows should be merged into one term.
11. Set years_only = true when date columns contain only year numbers (e.g.
    "1998") rather than full dates.
12. Set dynamic_parse = true (the default) to auto-detect section headers
    within the table. Leave true unless you have a specific reason to disable.
13. Infer office name from the Wikipedia page title or table caption. Use the
    most specific name possible (e.g. "Governor of California" not "Governor").
14. Populate reasoning with a brief explanation of your choices.
15. Return an empty tables list if no holder tables are found.

EXAMPLE — a typical holders table looks like:
  Col 1: Name (with /wiki/ link)
  Col 2: Party
  Col 3: Term start
  Col 4: Term end
→ link_column=1, party_column=2, term_start_column=3, term_end_column=4,
  table_rows=1 (skip the header row)
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

        # ---- Step 1: Fetch all tables from the page ----
        try:
            tables_preview = get_all_tables_preview(url, confirmed=True)
        except Exception as e:
            return {**result_base, "error": f"Page fetch error: {e}"}

        if tables_preview.get("error"):
            return {**result_base, "error": tables_preview["error"]}
        if not tables_preview.get("tables"):
            return {**result_base, "error": "No tables found on page"}

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
        """Call OpenAI with structured output, append assistant message to messages."""
        completion = self._client.beta.chat.completions.parse(
            model=self._model,
            messages=messages,
            response_format=AIOfficePageResponse,
        )
        choice = completion.choices[0]
        # Append assistant message for multi-turn continuity
        messages.append({"role": "assistant", "content": choice.message.content or ""})
        return choice.message.parsed  # type: ignore[return-value]

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
        """Format table data from get_all_tables_preview() into a readable message."""
        num_tables = tables_preview.get("num_tables", 0)
        tables = tables_preview.get("tables") or []

        lines = [
            f"Wikipedia URL: {url}",
            f"The page has {num_tables} table(s). "
            "Shown below: first 10 data rows per table (header row is row 1). "
            "Column numbers are 1-based.",
            "",
        ]

        for tbl in tables:
            idx = tbl.get("table_index", "?")
            rows = tbl.get("rows") or []
            lines.append(f"--- TABLE {idx} ---")
            if not rows:
                lines.append("  (empty table)")
            else:
                # Show column indices above first row
                if rows[0]:
                    col_header = " | ".join(f"[{i + 1}] {cell}" for i, cell in enumerate(rows[0]))
                    lines.append(f"  Header: {col_header}")
                for row in rows[1:]:
                    lines.append("  " + " | ".join(row))
            lines.append("")

        lines.append(
            "Identify every table that lists office holders and return the JSON response. "
            "Return an empty tables list if none found."
        )
        return "\n".join(lines)

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
