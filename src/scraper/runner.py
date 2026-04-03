# -*- coding: utf-8 -*-
"""
Run scraper using config and party list from DB, write results to DB.
Supports: dry_run / test_run (no DB write), row limits, Full / Delta / Live person modes.

--- Policy compliance ---

OpenAI API (via src/services/orchestrator.py → AIOfficeBuilder.analyze_parse_failures):
  - rate_limit / RateLimitError (HTTP 429) handling: exponential backoff in
    AIOfficeBuilder._call_parse_failure_openai (3 retries, 1 s → 2 s → 4 s).
  - max_completion_tokens=4096 set on every call to cap response size and cost.
  - OPENAI_API_KEY never hardcoded; always read via os.environ at runtime.
  See: https://platform.openai.com/docs/guides/rate-limits

Wikimedia REST API (via src/scraper/wiki_fetch.py):
  - User-Agent header set on every request (app name + contact) per Wikimedia etiquette.
  - rate_limit / throttle: wiki_throttle() enforces per-request delay so combined
    throughput never exceeds Wikipedia's policy limit.
  See: https://www.mediawiki.org/wiki/API:Etiquette

Google Gemini API (via src/services/gemini_vitals_researcher.py):
  - SDK: google-genai (unified GenAI SDK).
  - rate_limit / RESOURCE_EXHAUSTED (HTTP 429) handling: exponential backoff
    (3 retries, 1 s → 2 s → 4 s).
  - max_output_tokens set on every generate_content call.
  - GEMINI_OFFICE_HOLDER never hardcoded; always read via os.environ at runtime.
  - Unpaid tier: prompts/responses may be used by Google per ToS.
  - 55-day data retention by Google for abuse monitoring.
  See: https://ai.google.dev/gemini-api/terms
  See: https://ai.google.dev/gemini-api/docs/rate-limits
  See: https://ai.google.dev/gemini-api/docs/usage-policies
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

# Add project root so we can import db and sample-based scraper
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Use local logger and DB
from src.db.connection import get_connection, get_log_dir, init_db
from src.db import offices as db_offices
from src.db import parties as db_parties
from src.db import individuals as db_individuals
from src.db import office_terms as db_office_terms
from src.db.date_utils import normalize_date
from src.scraper.logger import HTTP_USER_AGENT, Logger
from src.scraper.config_test import get_raw_table_preview
from src.scraper.table_cache import get_table_html_cached
from src.scraper.run_cache import RunPageCache
from src.scraper.wiki_fetch import canonical_holder_url, normalize_wiki_url

from src.scraper import parse_core


def parse_full_table_for_export(
    office_row: dict[str, Any],
    table_html: str,
    url: str,
    progress_callback: Callable[[str, int, int, str, dict], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Parse full table HTML with the given office config (no row limit).
    Used by debug export so the exported EXTRACTED TABLE shows all parsed rows, not just the first 10.
    When progress_callback is provided, it is called during infobox processing (e.g. Processing x of y).
    Returns list of row dicts (same shape as preview_rows).
    """
    init_db()
    log_dir = get_log_dir()
    logger = Logger("export", "Office", log_dir=log_dir)
    party_list = db_parties.get_party_list_for_scraper()
    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup)
    # reporter=None for export: single-record export, failures are acceptable silently
    table_data = _parse_office_html(
        office_row,
        "",
        url,
        party_list,
        offices_parser,
        cached_table_html=table_html,
        progress_callback=progress_callback,
    )
    years_only = bool(office_row.get("years_only"))
    if bool(office_row.get("remove_duplicates")):
        table_data = _dedupe_parsed_rows(table_data, years_only=years_only)
    rows_out = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = (
            normalized
        )
        out = {
            "Wiki Link": row.get("Wiki Link") or "",
            "Party": row.get("Party") or "",
            "District": row.get("District") or "",
            "Term Start": term_start_val if term_start_val else "",
            "Term End": term_end_val if term_end_val else "",
            "Term Start Year": term_start_year,
            "Term End Year": term_end_year,
            "Infobox items": row.get("Infobox items") or "",
        }
        rows_out.append(out)
    return rows_out


def _parse_office_html(
    office_row: dict[str, Any],
    html_content: str,
    url: str,
    party_list: list,
    offices_parser: Any,
    cached_table_html: str | None = None,
    progress_callback: Callable[[str, int, int, str, dict], None] | None = None,
    progress_extra: dict[str, Any] | None = None,
    max_rows: int | None = None,
    run_cache: Any = None,
) -> list[dict[str, Any]]:
    """Single code path: build config from office_row and run parser. Returns list of row dicts (parser output).
    When cached_table_html is provided, use it as the page content and table_no=1 (single table).
    progress_callback(phase, current, total, message, extra) is called when find_date_in_infobox and processing each row.
    When max_rows is set, only the first max_rows table rows are parsed (so infobox is only fetched for those rows).
    """
    alt_links = (
        office_row.get("alt_links")
        if "alt_links" in office_row
        else (db_offices.list_alt_links(office_row["id"]) if office_row.get("id") else [])
    )
    table_config = db_offices.office_row_to_table_config(office_row, alt_links=alt_links)
    office_details = db_offices.office_row_to_office_details(office_row)
    if cached_table_html is not None:
        html_content = cached_table_html
        table_config = {**table_config, "table_no": 1}

    infobox_extra = dict(progress_extra or {})

    def infobox_progress(current: int, total: int, message: str):
        if progress_callback:
            progress_callback("infobox", current, total, message, infobox_extra)

    return offices_parser.process_table(
        html_content,
        table_config,
        office_details,
        url,
        party_list,
        progress_callback=infobox_progress if progress_callback else None,
        max_rows=max_rows,
        run_cache=run_cache,
    )


def _normalize_row_for_import(
    row: dict[str, Any], years_only: bool = False, include_no_link: bool = False
) -> tuple[dict, str | None, str | None, bool, bool, int | None, int | None] | None:
    """
    Same filter/normalize logic as the DB write path. Returns None if row should be skipped,
    else (row, term_start_val, term_end_val, term_start_imprecise, term_end_imprecise, term_start_year, term_end_year).
    When years_only is True (from row["_years_only"] or caller), accept rows with Term Start Year / Term End Year and leave dates null.
    When include_no_link is True (e.g. debug export), include rows with Wiki Link "No link" using their term dates/years.
    """
    wiki_url = row.get("Wiki Link") or ""
    if not wiki_url or wiki_url == "No link":
        if not include_no_link:
            return None
    use_years_only = years_only or bool(row.get("_years_only"))
    if use_years_only:
        term_start_year = row.get("Term Start Year")
        term_end_year = row.get("Term End Year")
        if term_start_year is None and term_end_year is None:
            return None
        return (row, None, None, False, False, term_start_year, term_end_year)
    term_start_val, term_start_imp = normalize_date(row.get("Term Start"))
    term_end_val, term_end_imp = normalize_date(row.get("Term End"))
    if term_start_val is None and term_end_val is None:
        # find_date_in_infobox fallback: row has only year columns (same as years-only for this record)
        term_start_year = row.get("Term Start Year")
        term_end_year = row.get("Term End Year")
        if term_start_year is not None or term_end_year is not None:
            return (row, None, None, False, False, term_start_year, term_end_year)
        # include_no_link: include name-only rows even with no parseable dates so they appear in preview and DB
        if include_no_link and row.get("_name_from_table"):
            return (row, None, None, False, False, None, None)
        return None
    return (row, term_start_val, term_end_val, term_start_imp, term_end_imp, None, None)


def _holder_key_from_existing_term(term: dict[str, Any]) -> tuple[str, str, str]:
    """Build a comparable key from an existing office_term row (URL-only matching)."""
    raw = (term.get("wiki_url") or "").strip()
    if _is_dead_wiki_url(raw):
        return ("", "", "")
    url = canonical_holder_url(raw)
    return (url, "", "")


def _holder_key_from_existing_term_years(term: dict[str, Any]) -> tuple[str, str, str]:
    """Build a URL-only key for table-first validation (no infobox)."""
    return _holder_key_from_existing_term(term)


def _format_missing_holders(labels: list[str], max_show: int = 20) -> str:
    """Format a list of missing holder labels; truncate with '… and N more' if long."""
    if not labels:
        return ""
    if len(labels) <= max_show:
        return ", ".join(labels)
    return ", ".join(labels[:max_show]) + f" … and {len(labels) - max_show} more"


def _missing_holders_display(
    existing_terms: list[dict[str, Any]],
    missing_keys: set[tuple[str, str, str]],
    key_from_term: Callable[[dict[str, Any]], tuple[str, str, str]],
) -> list[str]:
    """Return human-readable labels for existing terms whose key is in missing_keys."""
    labels: list[str] = []
    for t in existing_terms:
        k = key_from_term(t)
        if not k[0]:
            # Ignore deadlinks/no-link placeholders for revalidation display.
            continue
        if k not in missing_keys:
            continue
        url = (t.get("wiki_url") or "").strip()
        name = url.split("/")[-1].replace("_", " ") if url else "(no link)"
        start = t.get("term_start") or t.get("term_start_year")
        end = t.get("term_end") or t.get("term_end_year")
        if start is not None or end is not None:
            labels.append(f"{name} ({start or '?'}–{end or '?'})")
        else:
            labels.append(name)
    return labels


def _filtered_existing_holder_keys(
    existing_terms: list[dict[str, Any]],
    key_from_term: Callable[[dict[str, Any]], tuple[str, str, str]],
) -> set[tuple[str, str, str]]:
    """Build existing-holder key set while excluding empty/deadlink keys."""
    return {k for k in (key_from_term(t) for t in existing_terms) if k[0]}


def _holder_keys_from_parsed_rows(
    table_data: list[dict],
    office_id: int,
    years_only: bool,
    key_years_only: bool = False,
) -> set[tuple[str, str, str]]:
    """Build set of holder keys from parsed table rows (URL-only matching).
    key_years_only is accepted for compatibility and ignored."""
    keys: set[tuple[str, str, str]] = set()
    for row in table_data:
        # For table matching/revalidation we only care about active holder URLs,
        # not whether date parsing succeeded.
        raw_link = (row.get("Wiki Link") or "").strip()
        if (
            raw_link
            and raw_link != "No link"
            and not row.get("_dead_link")
            and not _is_dead_wiki_url(raw_link)
        ):
            keys.add((canonical_holder_url(raw_link), "", ""))
            continue
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if (
            normalized is None
            and (row.get("Wiki Link") or "") in ("", "No link")
            and row.get("_name_from_table")
        ):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = (
            normalized
        )
        wiki_url = row.get("Wiki Link") or ""
        if wiki_url in ("", "No link") and row.get("_name_from_table"):
            wiki_url = (
                "No link:" + str(office_id) + ":" + (row.get("_name_from_table") or "Unknown")
            )
        keys.add((canonical_holder_url(wiki_url), "", ""))
    return keys


def _is_dead_wiki_url(url: str) -> bool:
    u = (url or "").lower()
    return "redlink=1" in u


def _fetch_bio_batch(
    urls: list[str],
    biography,
    cancel_check,
    progress_fn,
    on_success,
    on_error,
    run_cache=None,
    max_workers: int | None = None,
) -> bool:
    """Fetch biographies for *urls* with up to *max_workers* concurrent HTTP calls.

    Rate-limiting (≤1 req/s) is enforced globally inside biography_extract via
    wiki_throttle() so the combined throughput never exceeds Wikipedia's policy limit.

    ``progress_fn(done, total)`` is called on the main thread after each result.
    ``on_success(wiki_url, bio_info)`` is called on the main thread for each good result.
    ``on_error(wiki_url, error_str)`` is called on the main thread for each failure.

    Returns True if the run was cancelled, False if all URLs were processed.
    """

    if max_workers is None:
        max_workers = int(os.environ.get("WIKI_FETCH_WORKERS", "1"))

    def _worker(url: str) -> tuple[str, dict | None, str | None]:
        try:
            return url, biography.biography_extract(url, run_cache=run_cache), None
        except Exception as exc:
            return url, None, str(exc)

    total = len(urls)
    cancelled = False
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker, url): url for url in urls}
        done = 0
        for future in as_completed(futures):
            if cancel_check and cancel_check():
                cancelled = True
                executor.shutdown(wait=False, cancel_futures=True)
                break
            url, bio_info, err = future.result()
            done += 1
            progress_fn(done, total)
            if err:
                on_error(url, err)
            elif bio_info:
                on_success(url, bio_info)
            else:
                on_error(url, "No bio data extracted")
    return cancelled


def _dedupe_parsed_rows(table_data: list[dict], years_only: bool) -> list[dict]:
    """Remove duplicate parsed rows by (wiki link, term start, term end, party, district).
    Uses normalized term values so the behavior matches the DB-write path."""
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[dict] = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if (
            normalized is None
            and (row.get("Wiki Link") or "") in ("", "No link")
            and row.get("_name_from_table")
        ):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            out.append(row)
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = (
            normalized
        )
        wiki_link = row.get("Wiki Link") or ""
        term_start_key = (
            term_start_val
            if term_start_val is not None
            else (str(term_start_year) if term_start_year is not None else "")
        )
        term_end_key = (
            term_end_val
            if term_end_val is not None
            else (str(term_end_year) if term_end_year is not None else "")
        )
        key = (
            wiki_link,
            term_start_key,
            term_end_key,
            (row.get("Party") or ""),
            (row.get("District") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _missing_holder_keys(
    existing_terms: list[dict[str, Any]],
    table_data: list[dict[str, Any]],
    office_id: int,
    years_only: bool,
    *,
    key_years_only: bool = False,
) -> set[tuple]:
    old_holders = (
        {_holder_key_from_existing_term_years(t) for t in existing_terms}
        if key_years_only
        else {_holder_key_from_existing_term(t) for t in existing_terms}
    )
    new_holders = _holder_keys_from_parsed_rows(
        table_data, office_id, years_only, key_years_only=key_years_only
    )
    old_holders = {k for k in old_holders if k[0]}
    new_holders = {k for k in new_holders if k[0]}
    return old_holders - new_holders


def _try_auto_update_table_no(
    office_row: dict[str, Any],
    existing_terms: list[dict[str, Any]],
    party_list: list,
    offices_parser: Any,
    *,
    refresh_table_cache: bool,
    years_only: bool,
    key_years_only: bool,
    current_missing_count: int,
) -> tuple[int | None, list[dict[str, Any]] | None]:
    if bool(office_row.get("disable_auto_table_update")):
        return (None, None)
    url = (office_row.get("url") or "").strip()
    current_table_no = int(office_row.get("table_no") or 1)
    page_result = get_table_html_cached(
        url,
        1,
        refresh=refresh_table_cache,
        use_full_page=bool(office_row.get("use_full_page_for_table")),
    )
    num_tables = int(page_result.get("num_tables") or 0)
    if num_tables <= 1:
        return (None, None)
    best_table_no = None
    best_rows = None
    best_missing = current_missing_count
    # Secondary score: compare using year-only keys to handle date-precision differences
    # (e.g., current table has exact dates but candidate table has month/day differences).
    current_missing_years = current_missing_count
    try:
        current_html = (
            get_table_html_cached(
                url,
                current_table_no,
                refresh=refresh_table_cache,
                use_full_page=bool(office_row.get("use_full_page_for_table")),
            ).get("html")
            or ""
        )
        if current_html:
            current_rows = _parse_office_html(
                {**office_row, "find_date_in_infobox": False},
                current_html,
                url,
                party_list,
                offices_parser,
                cached_table_html=current_html,
                progress_callback=None,
            )
            current_missing_years = len(
                _missing_holder_keys(
                    existing_terms,
                    current_rows,
                    int(office_row.get("id") or 0),
                    years_only,
                    key_years_only=True,
                )
            )
    except Exception:
        pass
    for candidate_no in range(1, num_tables + 1):
        if candidate_no == current_table_no:
            continue
        candidate_result = get_table_html_cached(
            url,
            candidate_no,
            refresh=refresh_table_cache,
            use_full_page=bool(office_row.get("use_full_page_for_table")),
        )
        html = candidate_result.get("html") or ""
        if not html:
            continue
        candidate_office = {**office_row, "table_no": candidate_no, "find_date_in_infobox": False}
        table_data = _parse_office_html(
            candidate_office,
            html,
            url,
            party_list,
            offices_parser,
            cached_table_html=html,
            progress_callback=None,
        )
        if not table_data:
            continue
        missing = _missing_holder_keys(
            existing_terms,
            table_data,
            int(office_row.get("id") or 0),
            years_only,
            key_years_only=key_years_only,
        )
        missing_exact = len(missing)
        missing_years = len(
            _missing_holder_keys(
                existing_terms,
                table_data,
                int(office_row.get("id") or 0),
                years_only,
                key_years_only=True,
            )
        )
        improved = (
            (missing_exact < best_missing)
            or (missing_exact == best_missing and missing_years < current_missing_years)
            or (
                missing_exact == best_missing
                and missing_years == current_missing_years
                and best_rows is not None
                and len(table_data) > len(best_rows)
            )
        )
        if improved:
            best_missing = missing_exact
            current_missing_years = missing_years
            best_table_no = candidate_no
            best_rows = table_data
            if best_missing == 0 and current_missing_years == 0:
                break
    return (best_table_no, best_rows)


def find_best_matching_table_for_existing_terms(
    office_row: dict[str, Any],
    existing_terms: list[dict[str, Any]],
    *,
    refresh_table_cache: bool = False,
    key_years_only: bool = False,
) -> dict[str, Any]:
    """Find better table_no on the same page by minimizing missing-holder validation failures.

    Returns {
      "found_table_no": int|None,
      "missing_before": int,
      "missing_after": int|None,
      "missing_labels_after": [str],
      "rows": list[dict]|None,
    }.
    """
    init_db()
    log_dir = get_log_dir()
    logger = Logger("table_search", "Office", log_dir=log_dir)
    party_list = db_parties.get_party_list_for_scraper()
    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup)
    years_only = bool(office_row.get("years_only"))
    office_id = int(office_row.get("id") or 0)

    url = (office_row.get("url") or "").strip()
    table_no = int(office_row.get("table_no") or 1)
    current_html = (
        get_table_html_cached(
            url,
            table_no,
            refresh=refresh_table_cache,
            use_full_page=bool(office_row.get("use_full_page_for_table")),
        ).get("html")
        or ""
    )
    current_rows = _parse_office_html(
        {**office_row, "find_date_in_infobox": False},
        current_html,
        url,
        party_list,
        offices_parser,
        cached_table_html=current_html if current_html else None,
        progress_callback=None,
    )
    missing_before_set = _missing_holder_keys(
        existing_terms, current_rows, office_id, years_only, key_years_only=key_years_only
    )
    found_table_no, found_rows = _try_auto_update_table_no(
        office_row,
        existing_terms,
        party_list,
        offices_parser,
        refresh_table_cache=refresh_table_cache,
        years_only=years_only,
        key_years_only=key_years_only,
        current_missing_count=len(missing_before_set),
    )
    if not found_table_no or found_rows is None:
        return {
            "found_table_no": None,
            "missing_before": len(missing_before_set),
            "missing_after": None,
            "missing_labels_after": [],
            "rows": None,
        }
    missing_after_set = _missing_holder_keys(
        existing_terms, found_rows, office_id, years_only, key_years_only=key_years_only
    )
    key_fn = (
        _holder_key_from_existing_term_years if key_years_only else _holder_key_from_existing_term
    )
    return {
        "found_table_no": int(found_table_no),
        "missing_before": len(missing_before_set),
        "missing_after": len(missing_after_set),
        "missing_labels_after": _missing_holders_display(existing_terms, missing_after_set, key_fn),
        "rows": found_rows,
    }


@dataclass
class _RunConfig:
    """Immutable per-run settings passed into _process_single_office."""

    run_mode: str
    refresh_table_cache: bool
    dry_run: bool
    test_run: bool
    party_list: list
    offices_parser: Any
    force_replace_office_ids: list[int] | None
    force_overwrite: bool
    max_rows_per_table: int | None
    cancel_check: Callable[[], bool] | None
    logger: Any
    report: Callable
    run_cache: Any = None
    bio_batch: int | None = None


@dataclass
class _RunContext:
    """All call-time parameters for run_with_db, passed into each mode function."""

    run_mode: str
    run_bio: bool
    run_office_bio: bool
    refresh_table_cache: bool
    dry_run: bool
    test_run: bool
    max_rows_per_table: int | None
    office_ids: list[int] | None
    individual_ref: str | None
    individual_ids: list[int] | None
    cancel_check: Callable[[], bool] | None
    force_replace_office_ids: list[int] | None
    force_overwrite: bool
    bio_batch: int | None


@dataclass
class _OfficeResult:
    """Outcome of processing one office. The outer loop interprets these fields."""

    cancel: bool = False  # break — stop the run
    skip: bool = False  # continue — no data, no accumulation
    offices_unchanged_inc: bool = False  # hash matched, existing terms kept
    rows: list[dict] = field(default_factory=list)  # parsed table rows to accumulate
    html_hash: str | None = None  # hash to store after write
    revalidate_failure: tuple[int, str] | None = None  # (office_id, message) to append
    missing_holders: list[str] | None = None  # for revalidate_missing_holders_list
    replaceable: bool = False  # add office_id to replaceable set


def _process_single_office(
    office_row: dict,
    cfg: _RunConfig,
    office_index: int,
    office_total: int,
) -> _OfficeResult:
    """Parse one office's HTML table and validate the result against existing terms.

    Returns an _OfficeResult that the outer loop in run_with_db interprets to
    update shared state (counters, sets, lists) and decide whether to break/continue.
    """
    office_id = office_row["id"]
    url = office_row.get("url") or ""
    office_name = office_row.get("name") or f"Office {office_id}"
    table_no = int(office_row.get("table_no") or 1)
    table_progress_extra = {
        "office_id": office_id,
        "office_name": office_name,
        "office_index": office_index,
        "office_total": office_total,
        "table_no": table_no,
        "table_index": office_index,
        "table_total": office_total,
    }
    cfg.report(
        "table",
        office_index,
        office_total,
        f"{office_name} (table {table_no})",
        table_progress_extra,
    )

    if not url:
        cfg.logger.log(f"Skipping office id {office_id}: no URL", True)
        cfg.report(
            "office",
            office_index,
            office_total,
            f"Skipped (no URL): {office_name}",
            {"terms_so_far": 0, **table_progress_extra},
        )
        return _OfficeResult(skip=True)

    cfg.report(
        "office",
        office_index,
        office_total,
        office_name,
        {"terms_so_far": 0, **table_progress_extra},
    )
    cfg.logger.log(f"Processing office {office_index}/{office_total}: {office_name} ({url})", True)

    existing_terms = db_office_terms.get_existing_terms_for_office(office_id)
    has_existing = len(existing_terms) > 0

    use_full_page = bool(office_row.get("use_full_page_for_table"))
    cache_result = get_table_html_cached(
        url.strip(),
        table_no,
        refresh=cfg.refresh_table_cache,
        use_full_page=use_full_page,
        run_cache=cfg.run_cache,
    )
    if "error" in cache_result:
        cfg.logger.log(f"Failed to get table for {url}: {cache_result['error']}", True)
        if has_existing:
            return _OfficeResult(
                skip=True,
                revalidate_failure=(
                    office_id,
                    f"Page or table failed: {cache_result['error']}. Kept existing terms.",
                ),
            )
        return _OfficeResult(skip=True)

    if cfg.cancel_check and cfg.cancel_check():
        cfg.logger.log("Run cancelled by user.", True)
        return _OfficeResult(cancel=True)

    if "cache_file" in cache_result:
        cfg.logger.log(f"Cached table: {cache_result['cache_file']}", True)
    html_content = cache_result.get("html") or ""
    cached_table_html = html_content if html_content else None

    # Hash-based skip: if HTML unchanged since last run and office has terms, skip re-parse
    html_hash = hashlib.sha256(html_content.encode("utf-8")).hexdigest() if html_content else None
    stored_hash = office_row.get("last_html_hash")
    if (
        cfg.run_mode == "delta"
        and not cfg.refresh_table_cache
        and not cfg.dry_run
        and not cfg.test_run
        and html_hash
        and html_hash == stored_hash
        and has_existing
    ):
        cfg.logger.log(
            f"Skipped (HTML unchanged): {office_name} — table HTML matches last run hash. No write.",
            True,
        )
        return _OfficeResult(offices_unchanged_inc=True)

    # When office has existing terms and find_date_in_infobox is on: validate from
    # table-only parse first so we don't fetch infoboxes only to fail validation later.
    use_infobox = bool(office_row.get("find_date_in_infobox"))
    if has_existing and use_infobox:
        office_row_no_infobox = {**office_row, "find_date_in_infobox": False}
        table_data_pre = _parse_office_html(
            office_row_no_infobox,
            html_content,
            url,
            cfg.party_list,
            cfg.offices_parser,
            cached_table_html=cached_table_html,
            progress_callback=None,
            max_rows=cfg.max_rows_per_table,
            run_cache=cfg.run_cache,
        )
        if len(table_data_pre) == 0:
            cfg.logger.log(
                f"Repopulate validation failed for {office_name}: table parsed to zero rows (existing had {len(existing_terms)}). Keeping existing terms.",
                True,
            )
            return _OfficeResult(
                skip=True,
                revalidate_failure=(office_id, "Table parsed to zero rows. Kept existing terms."),
            )
        old_holders_years = _filtered_existing_holder_keys(
            existing_terms, _holder_key_from_existing_term_years
        )
        years_only_pre = bool(office_row.get("years_only"))
        new_holders_years = _holder_keys_from_parsed_rows(
            table_data_pre, office_id, years_only_pre, key_years_only=True
        )
        missing_years = old_holders_years - new_holders_years
        if missing_years:
            if cfg.run_mode in ("full", "delta", "live_person"):
                found_table_no, found_rows = _try_auto_update_table_no(
                    office_row,
                    existing_terms,
                    cfg.party_list,
                    cfg.offices_parser,
                    refresh_table_cache=cfg.refresh_table_cache,
                    years_only=years_only_pre,
                    key_years_only=True,
                    current_missing_count=len(missing_years),
                )
                if found_table_no and found_rows is not None:
                    cfg.logger.log(
                        f"Auto-updated table_no for {office_name}: {table_no} -> {found_table_no} based on validation match.",
                        True,
                    )
                    office_row["table_no"] = int(found_table_no)
                    table_no = int(found_table_no)
                    table_data_pre = found_rows
                    missing_years = _missing_holder_keys(
                        existing_terms,
                        table_data_pre,
                        office_id,
                        years_only_pre,
                        key_years_only=True,
                    )
                    if not (cfg.dry_run or cfg.test_run):
                        od_id_for_tc = office_row.get("office_details_id")
                        if od_id_for_tc is not None:
                            with get_connection() as conn:
                                db_offices._safe_renumber_table_nos(
                                    int(od_id_for_tc),
                                    {int(office_id): int(table_no)},
                                    conn,
                                )
            missing_list = _missing_holders_display(
                existing_terms, missing_years, _holder_key_from_existing_term_years
            )
            missing_str = _format_missing_holders(missing_list)
            force_replace_early = cfg.force_overwrite or (
                cfg.force_replace_office_ids and office_id in cfg.force_replace_office_ids
            )
            if force_replace_early:
                cfg.logger.log(
                    f"Force overwrite for {office_name}: table-only check found new list missing {len(missing_years)} holder(s); replacing anyway. Missing: {missing_str}",
                    True,
                )
            elif missing_years:
                cfg.logger.log(
                    f"Repopulate validation failed for {office_name}: table-only check found new list missing {len(missing_years)} office holder(s). Skipping infobox fetch. Keeping existing terms. Missing: {missing_str}",
                    True,
                )
                return _OfficeResult(
                    skip=True,
                    revalidate_failure=(
                        office_id,
                        "New list is missing office holders that were in existing data. Kept existing terms.",
                    ),
                    missing_holders=missing_list,
                )

        # Delta: if holder set is identical (no missing, no new), skip infobox — existing
        # terms already have accurate dates from the previous infobox run.
        if cfg.run_mode == "delta" and not missing_years:
            current_new_holders_years = _holder_keys_from_parsed_rows(
                table_data_pre, office_id, years_only_pre, key_years_only=True
            )
            if current_new_holders_years == old_holders_years:
                cfg.logger.log(
                    f"Skipped (holders unchanged): {office_name} — holder set identical to existing terms. No write.",
                    True,
                )
                return _OfficeResult(offices_unchanged_inc=True, html_hash=html_hash)

    # Parse table (shared code path); report infobox progress when find_date_in_infobox
    table_data = _parse_office_html(
        office_row,
        html_content,
        url,
        cfg.party_list,
        cfg.offices_parser,
        cached_table_html=cached_table_html,
        progress_callback=cfg.report,
        progress_extra=table_progress_extra,
        run_cache=cfg.run_cache,
    )
    if cfg.max_rows_per_table is not None and cfg.max_rows_per_table >= 0:
        table_data = table_data[: cfg.max_rows_per_table]
    if bool(office_row.get("remove_duplicates")):
        table_data = _dedupe_parsed_rows(table_data, years_only=bool(office_row.get("years_only")))

    if has_existing and len(table_data) == 0:
        cfg.logger.log(
            f"Repopulate validation failed for {office_name}: table parsed to zero rows (existing had {len(existing_terms)}). Keeping existing terms.",
            True,
        )
        return _OfficeResult(
            skip=True,
            revalidate_failure=(office_id, "Table parsed to zero rows. Kept existing terms."),
        )

    replaceable = False
    revalidate_failure = None
    missing_holders_out: list[str] | None = None

    if has_existing and table_data:
        force_replace = (
            cfg.force_replace_office_ids and office_id in cfg.force_replace_office_ids
        ) or cfg.force_overwrite
        old_holders = _filtered_existing_holder_keys(existing_terms, _holder_key_from_existing_term)
        years_only = bool(office_row.get("years_only"))
        new_holders = _holder_keys_from_parsed_rows(table_data, office_id, years_only)
        missing = old_holders - new_holders
        if missing:
            if cfg.run_mode in ("full", "delta", "live_person"):
                found_table_no, found_rows = _try_auto_update_table_no(
                    office_row,
                    existing_terms,
                    cfg.party_list,
                    cfg.offices_parser,
                    refresh_table_cache=cfg.refresh_table_cache,
                    years_only=years_only,
                    key_years_only=False,
                    current_missing_count=len(missing),
                )
                if found_table_no and found_rows is not None:
                    cfg.logger.log(
                        f"Auto-updated table_no for {office_name}: {table_no} -> {found_table_no} based on holder match.",
                        True,
                    )
                    office_row["table_no"] = int(found_table_no)
                    table_no = int(found_table_no)
                    table_data = found_rows
                    missing = _missing_holder_keys(
                        existing_terms, table_data, office_id, years_only
                    )
                    if not (cfg.dry_run or cfg.test_run):
                        od_id_for_tc = office_row.get("office_details_id")
                        if od_id_for_tc is not None:
                            with get_connection() as conn:
                                db_offices._safe_renumber_table_nos(
                                    int(od_id_for_tc),
                                    {int(office_id): int(table_no)},
                                    conn,
                                )
            missing_list = _missing_holders_display(
                existing_terms, missing, _holder_key_from_existing_term
            )
            missing_str = _format_missing_holders(missing_list)
            if force_replace:
                cfg.logger.log(
                    f"Force override for {office_name}: replacing despite {len(missing)} holder(s) missing from new list. Missing: {missing_str}",
                    True,
                )
                replaceable = True
            elif missing:
                cfg.logger.log(
                    f"Repopulate validation failed for {office_name}: new list is missing {len(missing)} office holder(s) that were in existing data. Keeping existing terms. Missing: {missing_str}",
                    True,
                )
                revalidate_failure = (
                    office_id,
                    "New list is missing office holders that were in existing data. Kept existing terms.",
                )
                missing_holders_out = missing_list
                return _OfficeResult(
                    skip=True,
                    revalidate_failure=revalidate_failure,
                    missing_holders=missing_holders_out,
                )
            else:
                replaceable = True
        else:
            replaceable = True

    if cfg.cancel_check and cfg.cancel_check():
        cfg.logger.log("Run cancelled by user.", True)
        return _OfficeResult(cancel=True)

    return _OfficeResult(
        rows=table_data,
        html_hash=html_hash,
        replaceable=replaceable,
    )


def _build_preview_rows(all_office_data: list[dict], max_rows: int = 50) -> list[dict]:
    """Build the preview row list shown in dry-run / test-run results.

    Applies the same filter/normalize logic as the live import path so the UI
    shows exactly what would end up in the database.
    """
    rows: list[dict] = []
    for row in all_office_data:
        normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")))
        if (
            normalized is None
            and (row.get("Wiki Link") or "") in ("", "No link")
            and row.get("_name_from_table")
        ):
            normalized = _normalize_row_for_import(
                row, years_only=bool(row.get("_years_only")), include_no_link=True
            )
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = (
            normalized
        )
        wiki_link = row.get("Wiki Link") or ""
        dead_link = bool(
            row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table"))
        )
        rows.append(
            {
                "Wiki Link": wiki_link,
                "Party": row.get("Party") or "",
                "District": row.get("District") or "",
                "Term Start": term_start_val if term_start_val else "",
                "Term End": term_end_val if term_end_val else "",
                "Term Start Year": term_start_year,
                "Term End Year": term_end_year,
                "Dead link": dead_link,
                "Name (no link)": (
                    row.get("_name_from_table")
                    if dead_link and wiki_link in ("", "No link")
                    else None
                ),
            }
        )
    return rows[:max_rows]


def _build_result_dict(
    *,
    office_count: int,
    offices_unchanged: int,
    total_terms: int,
    unique_wiki_urls: set,
    bio_success_count: int,
    bio_error_count: int,
    bio_errors: list,
    bio_skipped_count: int,
    living_success_count: int,
    living_error_count: int,
    living_errors: list,
    dry_run: bool,
    test_run: bool,
    preview_rows: list | None,
    revalidate_failed: bool,
    revalidate_message: str | None,
    revalidate_missing_holders: list | None,
    office_errors: list,
    cancelled: bool = False,
    message: str | None = None,
) -> dict[str, Any]:
    """Assemble the standard run_with_db return dict."""
    result: dict[str, Any] = {
        "office_count": office_count,
        "offices_unchanged": offices_unchanged,
        "terms_parsed": total_terms,
        "unique_wiki_urls": len(unique_wiki_urls),
        "bio_success_count": bio_success_count,
        "bio_error_count": bio_error_count,
        "bio_errors": bio_errors,
        "bio_skipped_count": bio_skipped_count,
        "living_success_count": living_success_count,
        "living_error_count": living_error_count,
        "living_errors": living_errors,
        "dry_run": dry_run,
        "test_run": test_run,
        "preview_rows": preview_rows,
        "revalidate_failed": revalidate_failed,
        "revalidate_message": revalidate_message,
        "revalidate_missing_holders": revalidate_missing_holders,
        "office_errors": office_errors,
    }
    if cancelled:
        result["cancelled"] = True
    if message is not None:
        result["message"] = message
    return result


def _cleanup_disk_cache(max_age_days: int = 30) -> int:
    """Delete wiki_cache/*.json.gz files not modified in max_age_days days. Returns count deleted."""
    import time as _time
    from src.scraper.table_cache import _cache_dir

    cutoff = _time.time() - max_age_days * 86400
    deleted = 0
    for f in _cache_dir().glob("*.json.gz"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        except OSError:
            pass
    return deleted


def _run_single_bio(ctx: _RunContext, logger: Logger, report: Callable) -> dict[str, Any]:
    """Run a biography fetch for one individual (single_bio mode)."""
    ref = (ctx.individual_ref or "").strip()
    if not ref:
        logger.log("single_bio requires individual_ref (id or Wikipedia URL).", True)
        logger.close()
        return {
            "office_count": 0,
            "message": "Individual (ID or URL) required.",
            "bio_success_count": 0,
            "bio_error_count": 1,
            "bio_errors": [{"url": "", "error": "individual_ref required"}],
        }
    if ref.isdigit():
        ind = db_individuals.get_individual(int(ref))
        if not ind:
            logger.log(f"No individual with id={ref}.", True)
            logger.close()
            return {
                "office_count": 0,
                "message": f"No individual with id {ref}.",
                "bio_success_count": 0,
                "bio_error_count": 1,
                "bio_errors": [{"url": ref, "error": "Individual not found"}],
            }
        wiki_url = ind.get("wiki_url") or ""
    else:
        wiki_url = ref
        if not wiki_url.startswith("http"):
            wiki_url = (
                ("https://en.wikipedia.org" + wiki_url)
                if wiki_url.startswith("/")
                else f"https://en.wikipedia.org/wiki/{wiki_url}"
            )
    report("bio", 1, 1, "Fetching biography…", {})
    from src.scraper import parse_core

    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    bio_success_count = 0
    bio_error_count = 0
    bio_errors: list[dict[str, str]] = []
    try:
        bio_info = biography.biography_extract(wiki_url)
        if bio_info:
            bio_info["wiki_url"] = wiki_url
            bd, bd_imp = normalize_date(bio_info.get("birth_date"))
            dd, dd_imp = normalize_date(bio_info.get("death_date"))
            bio_info["birth_date"] = bd
            bio_info["death_date"] = dd
            bio_info["birth_date_imprecise"] = bd_imp
            bio_info["death_date_imprecise"] = dd_imp
            db_individuals.upsert_individual(bio_info)
            bio_success_count = 1
        else:
            bio_error_count = 1
            bio_errors.append({"url": wiki_url, "error": "No bio data extracted"})
    except Exception as e:
        bio_error_count = 1
        bio_errors.append({"url": wiki_url, "error": str(e)})
    logger.close()
    return {
        "office_count": 0,
        "terms_parsed": 0,
        "unique_wiki_urls": 1,
        "bio_success_count": bio_success_count,
        "bio_error_count": bio_error_count,
        "bio_errors": bio_errors,
        "bio_skipped_count": 0,
        "living_success_count": 0,
        "living_error_count": 0,
        "living_errors": [],
    }


def _run_selected_bios(ctx: _RunContext, logger: Logger, report: Callable) -> dict[str, Any]:
    """Run biography fetch for a specific set of individual IDs (selected_bios mode)."""
    from src.scraper import parse_core

    selected_ids = sorted({int(i) for i in (ctx.individual_ids or []) if int(i) > 0})
    if not selected_ids:
        logger.close()
        return {
            "office_count": 0,
            "terms_parsed": 0,
            "unique_wiki_urls": 0,
            "bio_success_count": 0,
            "bio_error_count": 0,
            "bio_errors": [],
            "bio_skipped_count": 0,
            "living_success_count": 0,
            "living_error_count": 0,
            "living_errors": [],
            "message": "No individuals selected.",
        }
    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    bio_success_count = 0
    bio_error_count = 0
    bio_errors: list[dict[str, str]] = []
    total_bios = len(selected_ids)
    for bio_idx, individual_id in enumerate(selected_ids):
        if ctx.cancel_check and ctx.cancel_check():
            logger.log("Selected bios run cancelled by user.", True)
            break
        report(
            "bio",
            bio_idx + 1,
            total_bios,
            "Updating selected individuals…",
            {"current": bio_idx + 1, "total": total_bios},
        )
        individual = db_individuals.get_individual(individual_id)
        if not individual:
            bio_error_count += 1
            bio_errors.append({"url": str(individual_id), "error": "Individual not found"})
            continue
        wiki_url = (individual.get("wiki_url") or "").strip()
        if not wiki_url:
            page_path = (individual.get("page_path") or "").strip()
            if page_path:
                wiki_url = (
                    page_path
                    if page_path.startswith("http")
                    else f"https://en.wikipedia.org/wiki/{page_path.lstrip('/')}"
                )
        if not wiki_url:
            bio_error_count += 1
            bio_errors.append({"url": str(individual_id), "error": "Missing wiki_url/page_path"})
            continue
        try:
            bio_info = biography.biography_extract(wiki_url)
            if bio_info:
                bio_info["wiki_url"] = wiki_url
                bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                dd, dd_imp = normalize_date(bio_info.get("death_date"))
                bio_info["birth_date"] = bd
                bio_info["death_date"] = dd
                bio_info["birth_date_imprecise"] = bd_imp
                bio_info["death_date_imprecise"] = dd_imp
                db_individuals.upsert_individual(bio_info)
                bio_success_count += 1
            else:
                bio_error_count += 1
                bio_errors.append({"url": wiki_url, "error": "No bio data extracted"})
        except Exception as e:
            bio_error_count += 1
            bio_errors.append({"url": wiki_url, "error": str(e)})
    logger.close()
    return {
        "office_count": 0,
        "terms_parsed": 0,
        "unique_wiki_urls": total_bios,
        "bio_success_count": bio_success_count,
        "bio_error_count": bio_error_count,
        "bio_errors": bio_errors,
        "bio_skipped_count": 0,
        "living_success_count": 0,
        "living_error_count": 0,
        "living_errors": [],
        "dry_run": False,
        "test_run": False,
        "preview_rows": None,
    }


def _run_bios_only(ctx: _RunContext, logger: Logger, report: Callable) -> dict[str, Any]:
    """Refresh biography data for every individual in the DB (bios_only mode)."""
    from src.scraper import parse_core

    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    to_fetch = list(db_individuals.get_all_individual_wiki_urls())
    to_fetch = [u for u in to_fetch if (u or "").strip()]
    total_bios = len(to_fetch)
    bio_errors: list[dict[str, str]] = []
    unique_to_fetch = list(dict.fromkeys(to_fetch))
    _counts = [0, 0]  # [success, error]

    def _success(wiki_url: str, bio_info: dict) -> None:
        bio_info["wiki_url"] = wiki_url
        bd, bd_imp = normalize_date(bio_info.get("birth_date"))
        dd, dd_imp = normalize_date(bio_info.get("death_date"))
        bio_info["birth_date"] = bd
        bio_info["death_date"] = dd
        bio_info["birth_date_imprecise"] = bd_imp
        bio_info["death_date_imprecise"] = dd_imp
        db_individuals.upsert_individual(bio_info)
        _counts[0] += 1

    def _error(wiki_url: str, err: str) -> None:
        _counts[1] += 1
        bio_errors.append({"url": wiki_url, "error": err})

    def _progress(done: int, total: int) -> None:
        report("bio", done, total, "Updating all individuals…", {"current": done, "total": total})

    if _fetch_bio_batch(unique_to_fetch, biography, ctx.cancel_check, _progress, _success, _error):
        logger.log("Bios only run cancelled by user.", True)
    logger.close()
    return {
        "office_count": 0,
        "terms_parsed": 0,
        "unique_wiki_urls": 0,
        "bio_success_count": _counts[0],
        "bio_error_count": _counts[1],
        "bio_errors": bio_errors,
        "bio_skipped_count": 0,
        "living_success_count": 0,
        "living_error_count": 0,
        "living_errors": [],
        "dry_run": False,
        "test_run": False,
        "preview_rows": None,
    }


def _run_insufficient_vitals(ctx: _RunContext, logger: Logger, report: Callable) -> dict[str, Any]:
    """Fetch bios for today's 1/30 slice of individuals missing birth/death dates.

    Batch assignment: id % 30 (computed in DB; never stored).
    Daily pick: date.today().day % 30  (or ctx.bio_batch if explicitly set).

    Each processed individual gets insufficient_vitals_checked_at stamped so it is
    skipped for the next 30 days — whether the bio fetch succeeded or failed.
    """
    from datetime import date
    from src.scraper import parse_core

    today_batch = ctx.bio_batch if ctx.bio_batch is not None else date.today().day % 30
    to_fetch_rows = db_individuals.get_insufficient_vitals_individuals_for_batch(today_batch)
    url_to_id = {r["wiki_url"]: r["id"] for r in to_fetch_rows}
    unique_to_fetch = list(dict.fromkeys(r["wiki_url"] for r in to_fetch_rows if r.get("wiki_url")))
    total = len(unique_to_fetch)
    bio_errors: list[dict[str, str]] = []
    _counts = [0, 0]  # [success, error]

    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)

    def _success(wiki_url: str, bio_info: dict) -> None:
        bio_info["wiki_url"] = wiki_url
        bd, bd_imp = normalize_date(bio_info.get("birth_date"))
        dd, dd_imp = normalize_date(bio_info.get("death_date"))
        bio_info["birth_date"] = bd
        bio_info["death_date"] = dd
        bio_info["birth_date_imprecise"] = bd_imp
        bio_info["death_date_imprecise"] = dd_imp
        db_individuals.upsert_individual(bio_info)
        ind_id = url_to_id.get(wiki_url)
        if ind_id:
            db_individuals.mark_insufficient_vitals_checked(ind_id)
        _counts[0] += 1

    def _error(wiki_url: str, err: str) -> None:
        _counts[1] += 1
        bio_errors.append({"url": wiki_url, "error": err})
        ind_id = url_to_id.get(wiki_url)
        if ind_id:
            db_individuals.mark_insufficient_vitals_checked(ind_id)

    def _progress(done: int, total: int) -> None:
        report(
            "bio",
            done,
            total,
            f"Checking insufficient vitals batch {today_batch}…",
            {"batch": today_batch, "current": done, "total": total},
        )

    report("bio", 0, total, f"Insufficient vitals batch {today_batch}: {total} to check", {})
    if _fetch_bio_batch(unique_to_fetch, biography, ctx.cancel_check, _progress, _success, _error):
        logger.log("Insufficient vitals run cancelled.", True)
    logger.close()
    return {
        "office_count": 0,
        "terms_parsed": 0,
        "unique_wiki_urls": 0,
        "bio_success_count": _counts[0],
        "bio_error_count": _counts[1],
        "bio_errors": bio_errors,
        "bio_skipped_count": 0,
        "living_success_count": 0,
        "living_error_count": 0,
        "living_errors": [],
        "insufficient_vitals_batch": today_batch,
        "insufficient_vitals_checked": total,
        "dry_run": False,
        "test_run": False,
        "preview_rows": None,
    }


def _run_gemini_vitals_research(ctx: _RunContext, logger, report: Callable) -> dict[str, Any]:
    """Use Gemini API to research vitals for today's batch of individuals.

    Batch assignment: id % 30 (computed in DB). 90-day cooldown per individual.
    For each candidate: Gemini researches → vitals saved → OpenAI polishes article.
    """
    from datetime import date

    from src.services.gemini_vitals_researcher import get_gemini_researcher, GeminiModelDeprecatedError
    from src.db import individual_research_sources as db_research
    from src.db import reference_documents as db_ref_docs

    researcher = get_gemini_researcher()
    if researcher is None:
        logger.log("Gemini research skipped: GEMINI_OFFICE_HOLDER not configured.", True)
        logger.close()
        return {
            "office_count": 0,
            "terms_parsed": 0,
            "unique_wiki_urls": 0,
            "bio_success_count": 0,
            "bio_error_count": 0,
            "bio_errors": [],
            "bio_skipped_count": 0,
            "living_success_count": 0,
            "living_error_count": 0,
            "living_errors": [],
            "gemini_research_batch": -1,
            "gemini_research_checked": 0,
            "gemini_research_found": 0,
            "gemini_articles_generated": 0,
            "dry_run": False,
            "test_run": False,
            "preview_rows": None,
        }

    today_batch = ctx.bio_batch if ctx.bio_batch is not None else date.today().day % 30
    candidates = db_individuals.get_gemini_research_candidates_for_batch(today_batch)
    total = len(candidates)
    report("gemini", 0, total, f"Gemini research batch {today_batch}: {total} candidates", {})

    found_count = 0
    articles_count = 0
    errors: list[dict[str, str]] = []

    # Load cached Wikipedia formatting guidelines (if available)
    ref_doc = db_ref_docs.get_reference_document("wikipedia_mos")
    formatting_guidelines = ref_doc["content"] if ref_doc else ""

    for i, row in enumerate(candidates):
        if ctx.cancel_check and ctx.cancel_check():
            logger.log("Gemini research cancelled.", True)
            break

        ind_id = row["id"]
        full_name = row.get("full_name") or ""
        wiki_url = row.get("wiki_url") or ""

        # Fetch office context for richer prompts
        office_context = _get_office_context_for_individual(ind_id)

        try:
            result = researcher.research_individual(
                individual_id=ind_id,
                full_name=full_name,
                office_name=office_context.get("office_name", ""),
                term_dates=office_context.get("term_dates", ""),
                party=office_context.get("party", ""),
                district=office_context.get("district", ""),
                location=office_context.get("location", ""),
                level=office_context.get("level", ""),
                branch=office_context.get("branch", ""),
                wiki_url=wiki_url,
                known_birth_date=office_context.get("birth_date", ""),
                known_death_date=office_context.get("death_date", ""),
                known_birth_place=office_context.get("birth_place", ""),
                known_death_place=office_context.get("death_place", ""),
            )
        except GeminiModelDeprecatedError:
            # Abort the entire batch — model needs manual update
            raise

        # Save vitals if found (individual drops out of future batches)
        vitals_found = False
        if result.birth_date or result.death_date:
            vitals_found = True
            found_count += 1
            update_data = {"wiki_url": wiki_url}
            if result.birth_date:
                update_data["birth_date"] = result.birth_date
            if result.death_date:
                update_data["death_date"] = result.death_date
            if result.birth_place:
                update_data["birth_place"] = result.birth_place
            if result.death_place:
                update_data["death_place"] = result.death_place
            try:
                db_individuals.upsert_individual(update_data)
            except Exception as exc:
                errors.append({"url": wiki_url, "error": f"upsert failed: {exc}"})

        # Store sources
        import json as _json

        for src in result.sources:
            try:
                db_research.insert_research_source(
                    individual_id=ind_id,
                    source_url=src.url,
                    source_type=src.source_type,
                    found_data_json=_json.dumps(
                        {
                            "birth_date": result.birth_date,
                            "death_date": result.death_date,
                            "notes": src.notes,
                        }
                    ),
                    origin="nightly",
                )
            except Exception as exc:
                errors.append({"url": wiki_url, "error": f"source insert failed: {exc}"})

        # OpenAI polish → wiki draft (only if enough data)
        if result.biographical_notes or vitals_found:
            try:
                from src.services.orchestrator import get_ai_builder

                builder = get_ai_builder()
                article = builder.polish_wiki_article(
                    full_name=full_name,
                    office_name=office_context.get("office_name", ""),
                    term_dates=office_context.get("term_dates", ""),
                    party=office_context.get("party", ""),
                    location=office_context.get("location", ""),
                    research_result=result,
                    formatting_guidelines=formatting_guidelines,
                )
                if article:
                    db_research.insert_wiki_draft_proposal(
                        individual_id=ind_id,
                        proposal_text=article,
                        origin="nightly",
                    )
                    articles_count += 1
            except Exception as exc:
                errors.append({"url": wiki_url, "error": f"OpenAI polish failed: {exc}"})

        # Mark checked regardless of outcome
        db_individuals.mark_gemini_research_checked(ind_id)

        report(
            "gemini",
            i + 1,
            total,
            f"Gemini research batch {today_batch}: {i + 1}/{total}",
            {"batch": today_batch, "current": i + 1, "total": total},
        )

    logger.close()
    return {
        "office_count": 0,
        "terms_parsed": 0,
        "unique_wiki_urls": 0,
        "bio_success_count": 0,
        "bio_error_count": len(errors),
        "bio_errors": errors,
        "bio_skipped_count": 0,
        "living_success_count": 0,
        "living_error_count": 0,
        "living_errors": [],
        "gemini_research_batch": today_batch,
        "gemini_research_checked": total,
        "gemini_research_found": found_count,
        "gemini_articles_generated": articles_count,
        "dry_run": False,
        "test_run": False,
        "preview_rows": None,
    }


def _get_office_context_for_individual(individual_id: int) -> dict[str, str]:
    """Fetch office/location context for an individual to enrich Gemini prompts."""
    from src.db import office_terms as db_office_terms
    from src.db import offices as db_offices
    from src.db import refs as db_refs

    context: dict[str, str] = {}
    try:
        from src.db.connection import get_connection

        conn = get_connection()
        try:
            # Get individual's existing data
            cur = conn.execute(
                "SELECT birth_date, death_date, birth_place, death_place"
                " FROM individuals WHERE id = %s",
                (individual_id,),
            )
            row = cur.fetchone()
            if row:
                context["birth_date"] = row["birth_date"] or ""
                context["death_date"] = row["death_date"] or ""
                context["birth_place"] = row["birth_place"] or ""
                context["death_place"] = row["death_place"] or ""

            # Get first office term with location context
            cur = conn.execute(
                """SELECT od.name AS office_name,
                          ot.term_start, ot.term_end,
                          ot.district,
                          p.party_name,
                          c.name AS country, s.name AS state, ci.name AS city,
                          l.name AS level, b.name AS branch
                   FROM office_terms ot
                   JOIN office_details od ON od.id = ot.office_details_id
                   JOIN source_pages sp ON sp.id = od.source_page_id
                   LEFT JOIN countries c ON c.id = sp.country_id
                   LEFT JOIN states s ON s.id = sp.state_id
                   LEFT JOIN cities ci ON ci.id = sp.city_id
                   LEFT JOIN levels l ON l.id = sp.level_id
                   LEFT JOIN branches b ON b.id = sp.branch_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   WHERE ot.individual_id = %s
                   ORDER BY ot.term_start
                   LIMIT 1""",
                (individual_id,),
            )
            term_row = cur.fetchone()
            if term_row:
                context["office_name"] = term_row["office_name"] or ""
                start = term_row["term_start"] or ""
                end = term_row["term_end"] or ""
                context["term_dates"] = f"{start} – {end}" if start else ""
                context["party"] = term_row["party_name"] or ""
                context["district"] = term_row["district"] or ""
                context["level"] = term_row["level"] or ""
                context["branch"] = term_row["branch"] or ""

                loc_parts = []
                if term_row["city"]:
                    loc_parts.append(term_row["city"])
                if term_row["state"]:
                    loc_parts.append(term_row["state"])
                if term_row["country"]:
                    loc_parts.append(term_row["country"])
                context["location"] = ", ".join(loc_parts)
        finally:
            conn.close()
    except Exception:
        pass  # Best-effort — empty context is fine
    return context


def run_with_db(
    run_mode: str = "delta",  # full | delta | live_person | single_bio | bios_only
    run_bio: bool = False,
    run_office_bio: bool = True,
    refresh_table_cache: bool = False,
    dry_run: bool = False,
    test_run: bool = False,
    max_rows_per_table: int | None = None,
    office_ids: list[int] | None = None,
    individual_ref: str | None = None,
    individual_ids: list[int] | None = None,
    progress_callback: Callable[[str, int, int, str, dict], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
    force_replace_office_ids: list[int] | None = None,
    force_overwrite: bool = False,
    bio_batch: int | None = None,
) -> dict[str, Any]:
    """
    Main entry: load offices and party list from DB, run scraper, write to DB (unless dry_run/test_run).
    If run_mode == single_bio, individual_ref (id or Wikipedia URL) is required; runs bio for that one only.
    If run_mode == bios_only, only update bios for all individuals (no office table parsing).
    run_office_bio=False skips all bio phases after office parsing. refresh_table_cache=True refetches table HTML from Wikipedia.
    force_overwrite=True: when validation fails (new list missing existing holders), replace anyway for all offices.
    """
    init_db()
    log_dir = get_log_dir()
    run_type = "test run" if test_run else "full run"
    logger = Logger(run_type, "Office", log_dir=log_dir)

    def report(phase: str, current: int, total: int, message: str, extra: dict | None = None):
        if progress_callback:
            progress_callback(phase, current, total, message, extra or {})

    ctx = _RunContext(
        run_mode=run_mode,
        run_bio=run_bio,
        run_office_bio=run_office_bio,
        refresh_table_cache=refresh_table_cache,
        dry_run=dry_run,
        test_run=test_run,
        max_rows_per_table=max_rows_per_table,
        office_ids=office_ids,
        individual_ref=individual_ref,
        individual_ids=individual_ids,
        cancel_check=cancel_check,
        force_replace_office_ids=force_replace_office_ids,
        force_overwrite=force_overwrite,
        bio_batch=bio_batch,
    )

    # Dispatch short-circuit modes that don't need office table parsing.
    if run_mode == "single_bio":
        return _run_single_bio(ctx, logger, report)
    if run_mode == "selected_bios":
        return _run_selected_bios(ctx, logger, report)
    if run_mode == "bios_only":
        return _run_bios_only(ctx, logger, report)
    if run_mode == "delta_insufficient_vitals":
        return _run_insufficient_vitals(ctx, logger, report)
    if run_mode == "gemini_vitals_research":
        return _run_gemini_vitals_research(ctx, logger, report)

    # Main loop modes (full | delta | live_person): load offices and dispatch.
    party_list = db_parties.get_party_list_for_scraper()
    offices = db_offices.list_runnable_units()
    if not offices:
        offices = [o for o in db_offices.list_offices() if o.get("enabled", 1) == 1]
    if office_ids:
        offices = [o for o in offices if o["id"] in office_ids]
    if not offices:
        logger.log("No offices to process.", True)
        logger.close()
        return {"office_count": 0, "message": "No offices to process."}

    report("init", 0, len(offices), "Starting…", {"total_offices": len(offices)})

    # We need the actual parsing logic. Use the sample script's classes by loading
    # a modified copy that has no Colab/Sheets. Create that copy on the fly.
    from src.scraper import parse_core  # noqa: F401

    # Build parse error reporter (disabled if GITHUB_TOKEN or OPENAI_API_KEY not set)
    try:
        from src.services.parse_error_reporter import ParseErrorReporter
        from src.services.github_client import get_github_client

        _reporter = ParseErrorReporter() if get_github_client() is not None else None
    except Exception as _reporter_init_err:
        logger.log(
            f"ParseErrorReporter init failed (reporting disabled for this run): {_reporter_init_err}",
            True,
        )
        _reporter = None

    data_cleanup = parse_core.DataCleanup(logger, reporter=_reporter)
    biography = parse_core.Biography(logger, data_cleanup, reporter=_reporter)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup, reporter=_reporter)

    # Full run: purge office_terms first (FK constraint), then individuals; terms are re-populated per-office
    if run_mode == "full" and not dry_run and not test_run:
        db_office_terms.purge_all_office_terms()
        db_individuals.purge_all_individuals()
        existing_individual_wiki_urls: set[str] = set()
    else:
        existing_individual_wiki_urls = db_individuals.get_all_individual_wiki_urls()

    total_terms = 0
    unique_wiki_urls: set[str] = set()
    all_office_data: list[dict] = []
    replaceable_office_ids: set[int] = set()
    revalidate_failed_offices: list[tuple[int, str]] = []
    revalidate_missing_holders_list: list[list[str]] = (
        []
    )  # full list per office when failure is "missing holders"
    office_errors: list[dict[str, Any]] = []  # offices that raised; run continues for others
    cancelled_early = False
    offices_unchanged = 0
    html_hashes_to_update: dict[int, str] = {}
    bio_success_count = 0
    bio_error_count = 0
    bio_errors: list[dict[str, str]] = []
    bio_skipped_count = 0
    living_success_count = 0
    living_error_count = 0
    living_errors: list[dict[str, str]] = []
    bio_cancelled = False

    run_cache = RunPageCache()
    run_cfg = _RunConfig(
        run_mode=run_mode,
        refresh_table_cache=refresh_table_cache,
        dry_run=dry_run,
        test_run=test_run,
        party_list=party_list,
        offices_parser=offices_parser,
        force_replace_office_ids=force_replace_office_ids,
        force_overwrite=force_overwrite,
        max_rows_per_table=max_rows_per_table,
        cancel_check=cancel_check,
        logger=logger,
        report=report,
        run_cache=run_cache,
        bio_batch=bio_batch,
    )

    for idx, office_row in enumerate(offices):
        office_index = idx + 1
        office_total = len(offices)
        if cancel_check and cancel_check():
            logger.log("Run cancelled by user.", True)
            report("office", idx, office_total, "Cancelled", {"terms_so_far": total_terms})
            # Build partial result (no DB write or bio after cancel)
            preview_rows = _build_preview_rows(all_office_data) if (dry_run or test_run) else None
            logger.close()
            rf = len(revalidate_failed_offices) > 0
            rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
            r_missing = (
                revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
            )
            return _build_result_dict(
                office_count=idx,
                offices_unchanged=offices_unchanged,
                total_terms=total_terms,
                unique_wiki_urls=unique_wiki_urls,
                bio_success_count=0,
                bio_error_count=0,
                bio_errors=[],
                bio_skipped_count=0,
                living_success_count=0,
                living_error_count=0,
                living_errors=[],
                dry_run=dry_run,
                test_run=test_run,
                preview_rows=preview_rows,
                revalidate_failed=rf,
                revalidate_message=rm,
                revalidate_missing_holders=r_missing,
                office_errors=office_errors,
                cancelled=True,
                message=f"Stopped after {idx} offices.",
            )

        office_id = office_row["id"]
        result = _process_single_office(office_row, run_cfg, office_index, office_total)

        if result.cancel:
            cancelled_early = True
            break
        if result.offices_unchanged_inc:
            offices_unchanged += 1
            if result.html_hash:
                html_hashes_to_update[office_id] = result.html_hash
            continue
        if result.revalidate_failure:
            revalidate_failed_offices.append(result.revalidate_failure)
        if result.missing_holders is not None:
            revalidate_missing_holders_list.append(result.missing_holders)
        if result.skip:
            continue
        if result.html_hash:
            html_hashes_to_update[office_id] = result.html_hash
        if result.replaceable:
            replaceable_office_ids.add(office_id)

        for row in result.rows:
            wiki_link = row.get("Wiki Link")
            if wiki_link and wiki_link != "No link":
                unique_wiki_urls.add(wiki_link)
            row["_office_id"] = office_id
            if office_row.get("office_details_id") is not None:
                row["_office_details_id"] = office_row["office_details_id"]
                row["_office_table_config_id"] = (
                    office_row.get("office_table_config_id") or office_id
                )
                row["_country_id"] = office_row.get("country_id")
            row["_years_only"] = bool(office_row.get("years_only"))
            all_office_data.append(row)
        total_terms += len(result.rows)

    if cancelled_early:
        report("office", idx, len(offices), "Cancelled", {"terms_so_far": total_terms})
        if dry_run or test_run:
            # Dry/test runs have no write phase — return immediately with what was collected
            preview_rows = _build_preview_rows(all_office_data)
            logger.close()
            rf = len(revalidate_failed_offices) > 0
            rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
            r_missing = (
                revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
            )
            return _build_result_dict(
                office_count=idx,
                offices_unchanged=offices_unchanged,
                total_terms=total_terms,
                unique_wiki_urls=unique_wiki_urls,
                bio_success_count=0,
                bio_error_count=0,
                bio_errors=[],
                bio_skipped_count=0,
                living_success_count=0,
                living_error_count=0,
                living_errors=[],
                dry_run=dry_run,
                test_run=test_run,
                preview_rows=preview_rows,
                revalidate_failed=rf,
                revalidate_message=rm,
                revalidate_missing_holders=r_missing,
                office_errors=office_errors,
                cancelled=True,
                message=f"Stopped after {idx} offices.",
            )
        # Live run: fall through to write what was collected so far

    report(
        "office", len(offices), len(offices), "All offices parsed", {"terms_so_far": total_terms}
    )

    # Write to DB unless dry_run or test_run (same filter/normalize as preview via _normalize_row_for_import)
    if not dry_run and not test_run and not all_office_data and not html_hashes_to_update:
        logger.log(
            f"Nothing written to DB — all {len(offices)} office(s) were skipped "
            f"(unchanged HTML/holders or validation failures). "
            f"Revalidation failures: {len(revalidate_failed_offices)}. "
            f"Offices unchanged: {offices_unchanged}.",
            True,
        )
    if not dry_run and not test_run and (all_office_data or html_hashes_to_update):
        report("saving", 0, 1, "Writing to database…", {"terms": total_terms})
        conn = get_connection()
        try:
            for oid in replaceable_office_ids:
                db_office_terms.delete_office_terms_for_office(oid, conn=conn)

            for row in all_office_data:
                office_id = row.get("_office_id")
                if office_id is None:
                    continue
                normalized = _normalize_row_for_import(row)
                # Include "No link" rows that have a name (e.g. Charles W. Wright) as dead-link individuals
                if (
                    normalized is None
                    and (row.get("Wiki Link") or "") in ("", "No link")
                    and row.get("_name_from_table")
                ):
                    normalized = _normalize_row_for_import(row, include_no_link=True)
                if normalized is None:
                    continue
                (
                    _,
                    term_start_val,
                    term_end_val,
                    term_start_imp,
                    term_end_imp,
                    term_start_year,
                    term_end_year,
                ) = normalized
                wiki_url = row.get("Wiki Link") or ""
                no_link_placeholder = wiki_url in ("", "No link") and row.get("_name_from_table")
                if no_link_placeholder:
                    wiki_url = (
                        "No link:"
                        + str(office_id)
                        + ":"
                        + (row.get("_name_from_table") or "Unknown")
                    )
                # Resolve or create individual
                ind = db_individuals.get_individual_by_wiki_url(wiki_url, conn=conn)
                individual_id = ind["id"] if ind else None
                if not ind:
                    # Create placeholder or dead-link individual so we can link office_term
                    payload = {
                        "wiki_url": wiki_url,
                        "page_path": wiki_url.split("/")[-1] if "/" in wiki_url else None,
                    }
                    if row.get("_dead_link") or no_link_placeholder:
                        payload["full_name"] = row.get("_name_from_table")
                        payload["is_dead_link"] = 1
                    individual_id = db_individuals.upsert_individual(payload, conn=conn)
                party_text = row.get("Party")
                od_id = row.get("_office_details_id")
                tc_id = row.get("_office_table_config_id")
                country_id = row.get("_country_id")
                if od_id is not None and tc_id is not None and country_id is not None:
                    party_id = db_parties.resolve_party_id_by_country(
                        country_id, party_text, conn=conn
                    )
                    db_office_terms.insert_office_term(
                        office_details_id=od_id,
                        office_table_config_id=tc_id,
                        individual_id=individual_id,
                        wiki_url=wiki_url,
                        party_id=party_id,
                        district=row.get("District"),
                        term_start=term_start_val,
                        term_end=term_end_val,
                        term_start_year=term_start_year,
                        term_end_year=term_end_year,
                        term_start_imprecise=term_start_imp,
                        term_end_imprecise=term_end_imp,
                        conn=conn,
                    )
                else:
                    party_id = db_parties.resolve_party_id(office_id, party_text, conn=conn)
                    db_office_terms.insert_office_term(
                        office_id=office_id,
                        individual_id=individual_id,
                        wiki_url=wiki_url,
                        party_id=party_id,
                        district=row.get("District"),
                        term_start=term_start_val,
                        term_end=term_end_val,
                        term_start_year=term_start_year,
                        term_end_year=term_end_year,
                        term_start_imprecise=term_start_imp,
                        term_end_imprecise=term_end_imp,
                        conn=conn,
                    )
                if individual_id:
                    db_individuals._recompute_is_living_for_individual(individual_id, conn)
            for tc_id, h in html_hashes_to_update.items():
                db_offices.update_html_hash(tc_id, h, conn=conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # Bio run (new individuals only): only fetch bio for people not already in the individuals table.
    # Full/delta: to_fetch = unique_wiki_urls - existing_individual_wiki_urls. Report skipped count.
    # Skip entirely when run_office_bio is False (office-only run).
    if not dry_run and not test_run and run_office_bio:
        if run_mode == "live_person":
            # Live person mode: only refresh bios for living individuals (no new-individual bio).
            to_fetch = list(db_individuals.get_living_individual_wiki_urls()) if run_bio else []
            bio_skipped_count = 0
            total_bios = len(to_fetch)
            unique_living = list(dict.fromkeys(to_fetch))
            _lp_counts = [0, 0]  # [success, error]

            def _lp_success(wiki_url: str, bio_info: dict) -> None:
                bio_info["wiki_url"] = wiki_url
                bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                dd, dd_imp = normalize_date(bio_info.get("death_date"))
                bio_info["birth_date"] = bd
                bio_info["death_date"] = dd
                bio_info["birth_date_imprecise"] = bd_imp
                bio_info["death_date_imprecise"] = dd_imp
                db_individuals.upsert_individual(bio_info)
                _lp_counts[0] += 1

            def _lp_error(wiki_url: str, err: str) -> None:
                _lp_counts[1] += 1
                logger.log(f"Living update failed for {wiki_url}: {err}", True)
                living_errors.append({"url": wiki_url, "error": err})

            def _lp_progress(done: int, total: int) -> None:
                report(
                    "living",
                    done,
                    total,
                    "Updating living individuals…",
                    {"current": done, "total": total},
                )

            if _fetch_bio_batch(
                unique_living, biography, cancel_check, _lp_progress, _lp_success, _lp_error
            ):
                logger.log("Run cancelled by user (during living update).", True)
                bio_cancelled = True
            living_success_count += _lp_counts[0]
            living_error_count += _lp_counts[1]
        else:
            # Full/delta: bio only for new individuals (not already in DB).
            to_fetch = list(unique_wiki_urls - existing_individual_wiki_urls)
            bio_skipped_count = len(unique_wiki_urls) - len(to_fetch)
            if bio_skipped_count > 0:
                logger.log(
                    f"Skipping {bio_skipped_count} individuals (already in DB); fetching bio for {len(to_fetch)} new.",
                    True,
                )
                report(
                    "bio",
                    0,
                    len(to_fetch),
                    f"Skipped {bio_skipped_count} (in DB). Fetching {len(to_fetch)} new…",
                    {"bio_skipped": bio_skipped_count},
                )
            total_bios = len(to_fetch)
            # Build bio cache from table parse so we skip re-fetch when find_date_in_infobox was used (key by normalized URL)
            bio_cache: dict[str, dict] = {}
            for row in all_office_data:
                wiki_url_row = row.get("Wiki Link")
                if wiki_url_row and wiki_url_row != "No link" and row.get("_bio_details"):
                    key = normalize_wiki_url(wiki_url_row) or wiki_url_row
                    if key not in bio_cache:
                        bio_cache[key] = row["_bio_details"]
            # Split: bio_cache hits (no HTTP) vs URLs that need HTTP fetch.
            cache_hits = [u for u in to_fetch if (normalize_wiki_url(u) or u) in bio_cache]
            http_urls = [u for u in to_fetch if (normalize_wiki_url(u) or u) not in bio_cache]
            # Process bio_cache hits sequentially (no rate limiting needed).
            for wiki_url in cache_hits:
                bio_cache_key = normalize_wiki_url(wiki_url) or wiki_url
                bio_info = dict(bio_cache[bio_cache_key])
                bio_info["wiki_url"] = wiki_url
                if bio_info.get("page_path") is None:
                    bio_info["page_path"] = (wiki_url or "").rstrip("/").split("/")[-1] or ""
                bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                dd, dd_imp = normalize_date(bio_info.get("death_date"))
                bio_info["birth_date"] = bd
                bio_info["death_date"] = dd
                bio_info["birth_date_imprecise"] = bd_imp
                bio_info["death_date_imprecise"] = dd_imp
                db_individuals.upsert_individual(bio_info)
                bio_success_count += 1
            # Fetch remaining URLs in parallel (rate-limited via wiki_throttle in biography_extract).
            _new_bio_counts = [0, 0]
            _new_bio_done = [len(cache_hits)]

            def _new_bio_success(wiki_url: str, bio_info: dict) -> None:
                bio_info["wiki_url"] = wiki_url
                bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                dd, dd_imp = normalize_date(bio_info.get("death_date"))
                bio_info["birth_date"] = bd
                bio_info["death_date"] = dd
                bio_info["birth_date_imprecise"] = bd_imp
                bio_info["death_date_imprecise"] = dd_imp
                db_individuals.upsert_individual(bio_info)
                _new_bio_counts[0] += 1

            def _new_bio_error(wiki_url: str, err: str) -> None:
                _new_bio_counts[1] += 1
                logger.log(f"Bio failed for {wiki_url}: {err}", True)
                bio_errors.append({"url": wiki_url, "error": err})

            def _new_bio_progress(done: int, total: int) -> None:
                combined = _new_bio_done[0] + done
                report(
                    "bio",
                    combined,
                    total_bios,
                    "Fetching biographies (new individuals)…",
                    {"current": combined, "total": total_bios, "bio_skipped": bio_skipped_count},
                )

            unique_http_urls = list(dict.fromkeys(http_urls))
            if _fetch_bio_batch(
                unique_http_urls,
                biography,
                cancel_check,
                _new_bio_progress,
                _new_bio_success,
                _new_bio_error,
            ):
                logger.log("Run cancelled by user (during bio fetch).", True)
                bio_cancelled = True
            bio_success_count += _new_bio_counts[0]
            bio_error_count += _new_bio_counts[1]

            # Optional second pass: update living individuals (death_date null). Full refresh of bio fields.
            if run_bio:
                if bio_batch is not None:
                    to_fetch_living = db_individuals.get_living_individuals_for_batch(bio_batch)
                else:
                    to_fetch_living = []
                total_living = len(to_fetch_living)
                if total_living > 0:
                    logger.log(
                        f"Update living individuals: refreshing bio for {total_living} people (death_date null).",
                        True,
                    )
                    report(
                        "living",
                        0,
                        total_living,
                        f"Updating {total_living} living individuals…",
                        {},
                    )
                    unique_living2 = list(dict.fromkeys(to_fetch_living))
                    _liv2_counts = [0, 0]

                    def _liv2_success(wiki_url: str, bio_info: dict) -> None:
                        bio_info["wiki_url"] = wiki_url
                        bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                        dd, dd_imp = normalize_date(bio_info.get("death_date"))
                        bio_info["birth_date"] = bd
                        bio_info["death_date"] = dd
                        bio_info["birth_date_imprecise"] = bd_imp
                        bio_info["death_date_imprecise"] = dd_imp
                        db_individuals.upsert_individual(bio_info)
                        db_individuals.mark_bio_refreshed(wiki_url)
                        _liv2_counts[0] += 1

                    def _liv2_error(wiki_url: str, err: str) -> None:
                        _liv2_counts[1] += 1
                        logger.log(f"Living update failed for {wiki_url}: {err}", True)
                        living_errors.append({"url": wiki_url, "error": err})

                    def _liv2_progress(done: int, total: int) -> None:
                        report(
                            "living",
                            done,
                            total,
                            "Updating living individuals…",
                            {"current": done, "total": total},
                        )

                    if _fetch_bio_batch(
                        unique_living2,
                        biography,
                        cancel_check,
                        _liv2_progress,
                        _liv2_success,
                        _liv2_error,
                        run_cache=run_cache,
                    ):
                        logger.log("Run cancelled by user (during living update).", True)
                        bio_cancelled = True
                    living_success_count += _liv2_counts[0]
                    living_error_count += _liv2_counts[1]

    if bio_cancelled:
        logger.close()
        report(
            "complete",
            1,
            1,
            "Stopped (bio)",
            {"terms_parsed": total_terms, "unique_wiki_urls": len(unique_wiki_urls)},
        )
        preview_rows = _build_preview_rows(all_office_data) if (dry_run or test_run) else None
        rf = len(revalidate_failed_offices) > 0
        rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
        r_missing = revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
        return _build_result_dict(
            office_count=len(offices),
            offices_unchanged=offices_unchanged,
            total_terms=total_terms,
            unique_wiki_urls=unique_wiki_urls,
            bio_success_count=bio_success_count,
            bio_error_count=bio_error_count,
            bio_errors=bio_errors,
            bio_skipped_count=bio_skipped_count,
            living_success_count=living_success_count,
            living_error_count=living_error_count,
            living_errors=living_errors,
            dry_run=dry_run,
            test_run=test_run,
            preview_rows=preview_rows,
            revalidate_failed=rf,
            revalidate_message=rm,
            revalidate_missing_holders=r_missing,
            office_errors=office_errors,
            cancelled=True,
            message="Stopped during bio/living update.",
        )

    if _reporter is not None:
        try:
            _reporter.flush()
        except Exception as _flush_err:
            logger.log(
                f"ParseErrorReporter flush failed (run result not affected): {_flush_err}", True
            )
    logger.close()
    report(
        "complete",
        1,
        1,
        "Done",
        {"terms_parsed": total_terms, "unique_wiki_urls": len(unique_wiki_urls)},
    )

    # Preview rows: same filter/normalize as import so UI shows exactly what would be in the table (include dead-link / name-only rows)
    preview_rows = _build_preview_rows(all_office_data) if (dry_run or test_run) else None

    revalidate_failed = len(revalidate_failed_offices) > 0
    revalidate_message = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
    if revalidate_failed and len(revalidate_failed_offices) > 1:
        revalidate_message = f"{len(revalidate_failed_offices)} office(s) skipped (validation failed). {revalidate_message}"
    revalidate_missing_holders = (
        revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
    )

    return _build_result_dict(
        office_count=len(offices),
        offices_unchanged=offices_unchanged,
        total_terms=total_terms,
        unique_wiki_urls=unique_wiki_urls,
        bio_success_count=bio_success_count,
        bio_error_count=bio_error_count,
        bio_errors=bio_errors,
        bio_skipped_count=bio_skipped_count,
        living_success_count=living_success_count,
        living_error_count=living_error_count,
        living_errors=living_errors,
        dry_run=dry_run,
        test_run=test_run,
        preview_rows=preview_rows,
        revalidate_failed=revalidate_failed,
        revalidate_message=revalidate_message,
        revalidate_missing_holders=revalidate_missing_holders,
        office_errors=office_errors,
    )


def preview_with_config(
    office_row: dict[str, Any],
    max_rows: int | None = 10,
    progress_callback: Callable[[str, int, int, str, dict], None] | None = None,
) -> dict[str, Any]:
    """
    Run preview for a single office config (e.g. draft from form). Uses same parse path as run_with_db
    and same filter/normalize as import so preview shows exactly what would be written to the table.
    office_row must have: url, table_no, table_rows, link_column, party_column, term_start_column,
    term_end_column, district_column, and optional booleans; plus country_name, level_name, branch_name
    (and name, department, state_name, notes) for office_details.
    max_rows: cap on preview rows (default 10). None = return all rows.
    progress_callback(phase, current, total, message, extra): optional; called when find_date_in_infobox (Processing x of y).
    Returns {"preview_rows": [...], "raw_table_preview": {...} or None, "error": None or str}.
    """
    init_db()
    log_dir = get_log_dir()
    logger = Logger("preview", "Office", log_dir=log_dir)
    party_list = db_parties.get_party_list_for_scraper()
    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup)

    url = (office_row.get("url") or "").strip()
    if not url:
        return {"preview_rows": [], "raw_table_preview": None, "error": "No URL configured"}

    table_no = int(office_row.get("table_no") or 1)
    use_full_page = bool(office_row.get("use_full_page_for_table"))
    cache_result = get_table_html_cached(url, table_no, refresh=False, use_full_page=use_full_page)
    if "error" in cache_result:
        return {"preview_rows": [], "raw_table_preview": None, "error": cache_result["error"]}
    html_content = cache_result.get("html") or ""
    cached_table_html = html_content if html_content else None

    try:
        table_data = _parse_office_html(
            office_row,
            html_content,
            url,
            party_list,
            offices_parser,
            cached_table_html=cached_table_html,
            progress_callback=progress_callback,
            max_rows=max_rows,
        )
        if bool(office_row.get("remove_duplicates")):
            table_data = _dedupe_parsed_rows(
                table_data, years_only=bool(office_row.get("years_only"))
            )
    except Exception as e:
        raw_max = max_rows if max_rows is not None else 100
        raw = get_raw_table_preview(url, int(office_row.get("table_no") or 1), raw_max)
        return {"preview_rows": [], "raw_table_preview": raw, "error": str(e)}

    # Same filter/normalize as import: only rows that would be inserted (include dead-link / name-only rows)
    years_only = bool(office_row.get("years_only"))
    preview_rows = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if (
            normalized is None
            and (row.get("Wiki Link") or "") in ("", "No link")
            and row.get("_name_from_table")
        ):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = (
            normalized
        )
        wiki_link = row.get("Wiki Link") or ""
        dead_link = bool(
            row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table"))
        )
        preview_rows.append(
            {
                "Wiki Link": wiki_link,
                "Party": row.get("Party") or "",
                "District": row.get("District") or "",
                "Term Start": term_start_val if term_start_val else "",
                "Term End": term_end_val if term_end_val else "",
                "Term Start Year": term_start_year,
                "Term End Year": term_end_year,
                "Dead link": dead_link,
                "Name (no link)": (
                    row.get("_name_from_table")
                    if dead_link and wiki_link in ("", "No link")
                    else None
                ),
            }
        )
    if max_rows is not None:
        preview_rows = preview_rows[:max_rows]

    revalidate_failed = False
    revalidate_missing_holders = None
    revalidate_message = None
    tc_id = office_row.get("office_table_config_id") or office_row.get("id")
    if tc_id:
        try:
            existing_terms = db_office_terms.get_existing_terms_for_office(int(tc_id))
        except Exception:
            existing_terms = []
        if existing_terms and table_data:
            missing = _missing_holder_keys(
                existing_terms, table_data, int(tc_id), years_only, key_years_only=False
            )
            if missing:
                revalidate_failed = True
                revalidate_message = (
                    "New list found. Existing office holders are missing from this preview list."
                )
                revalidate_missing_holders = _missing_holders_display(
                    existing_terms, missing, _holder_key_from_existing_term
                )

    raw_table_preview = None
    if not preview_rows and not table_data:
        raw_max = max_rows if max_rows is not None else 100
        raw_table_preview = get_raw_table_preview(
            url, int(office_row.get("table_no") or 1), raw_max
        )
    return {
        "preview_rows": preview_rows,
        "raw_table_preview": raw_table_preview,
        "error": None,
        "revalidate_failed": revalidate_failed,
        "revalidate_message": revalidate_message,
        "revalidate_missing_holders": revalidate_missing_holders,
    }
