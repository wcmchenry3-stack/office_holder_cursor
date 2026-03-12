# -*- coding: utf-8 -*-
"""
Run scraper using config and party list from DB, write results to DB.
Supports: dry_run / test_run (no DB write), row limits, Full / Delta / Live person modes.
"""

from __future__ import annotations

import sqlite3
import sys
import time
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
from src.scraper.wiki_fetch import normalize_wiki_url

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
    table_data = _parse_office_html(
        office_row, "", url, party_list, offices_parser,
        cached_table_html=table_html, progress_callback=progress_callback,
    )
    years_only = bool(office_row.get("years_only"))
    if bool(office_row.get("remove_duplicates")):
        table_data = _dedupe_parsed_rows(table_data, years_only=years_only)
    rows_out = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
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
) -> list[dict[str, Any]]:
    """Single code path: build config from office_row and run parser. Returns list of row dicts (parser output).
    When cached_table_html is provided, use it as the page content and table_no=1 (single table).
    progress_callback(phase, current, total, message, extra) is called when find_date_in_infobox and processing each row.
    When max_rows is set, only the first max_rows table rows are parsed (so infobox is only fetched for those rows)."""
    alt_links = office_row.get("alt_links") if "alt_links" in office_row else (db_offices.list_alt_links(office_row["id"]) if office_row.get("id") else [])
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
        html_content, table_config, office_details, url, party_list,
        progress_callback=infobox_progress if progress_callback else None,
        max_rows=max_rows,
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
    url = _canonical_holder_url(raw)
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
        if raw_link and raw_link != "No link" and not row.get("_dead_link") and not _is_dead_wiki_url(raw_link):
            keys.add((_canonical_holder_url(raw_link), "", ""))
            continue
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
        wiki_url = row.get("Wiki Link") or ""
        if wiki_url in ("", "No link") and row.get("_name_from_table"):
            wiki_url = "No link:" + str(office_id) + ":" + (row.get("_name_from_table") or "Unknown")
        keys.add((_canonical_holder_url(wiki_url), "", ""))
    return keys


def _is_dead_wiki_url(url: str) -> bool:
    u = (url or "").lower()
    return "redlink=1" in u


def _canonical_holder_url(url: str) -> str:
    """Canonicalize holder URL for comparisons.

    For Wikipedia links, normalize and strip query/fragment so redlink/edit query params
    don't break holder matching across table variants.
    """
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("No link:"):
        return u
    normalized = normalize_wiki_url(u)
    if normalized:
        try:
            from urllib.parse import urlparse, urlunparse
            p = urlparse(normalized)
            path = (p.path or "").rstrip("/")
            # Compare Wikipedia pages by canonical /wiki/<title> key so
            # scheme/host/query/encoding/case differences don't create false mismatches.
            parts = [x for x in path.split("/") if x]
            if len(parts) >= 2 and parts[0].lower() == "wiki":
                from urllib.parse import unquote
                title = unquote(parts[1]).replace(" ", "_").strip().lower()
                return f"/wiki/{title}"
            return urlunparse(("https", (p.netloc or "").lower(), path, "", "", ""))
        except Exception:
            return normalized
    return u


def _dedupe_parsed_rows(table_data: list[dict], years_only: bool) -> list[dict]:
    """Remove duplicate parsed rows by (wiki link, term start, term end, party, district).
    Uses normalized term values so the behavior matches the DB-write path."""
    seen: set[tuple[str, str, str, str, str]] = set()
    out: list[dict] = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            out.append(row)
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
        wiki_link = row.get("Wiki Link") or ""
        term_start_key = term_start_val if term_start_val is not None else (str(term_start_year) if term_start_year is not None else "")
        term_end_key = term_end_val if term_end_val is not None else (str(term_end_year) if term_end_year is not None else "")
        key = (wiki_link, term_start_key, term_end_key, (row.get("Party") or ""), (row.get("District") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out




def _missing_holder_keys(existing_terms: list[dict[str, Any]], table_data: list[dict[str, Any]], office_id: int, years_only: bool, *, key_years_only: bool = False) -> set[tuple]:
    old_holders = {_holder_key_from_existing_term_years(t) for t in existing_terms} if key_years_only else {_holder_key_from_existing_term(t) for t in existing_terms}
    new_holders = _holder_keys_from_parsed_rows(table_data, office_id, years_only, key_years_only=key_years_only)
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
    page_result = get_table_html_cached(url, 1, refresh=refresh_table_cache, use_full_page=bool(office_row.get("use_full_page_for_table")))
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
        current_html = get_table_html_cached(url, current_table_no, refresh=refresh_table_cache, use_full_page=bool(office_row.get("use_full_page_for_table"))).get("html") or ""
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
                _missing_holder_keys(existing_terms, current_rows, int(office_row.get("id") or 0), years_only, key_years_only=True)
            )
    except Exception:
        pass
    for candidate_no in range(1, num_tables + 1):
        if candidate_no == current_table_no:
            continue
        candidate_result = get_table_html_cached(url, candidate_no, refresh=refresh_table_cache, use_full_page=bool(office_row.get("use_full_page_for_table")))
        html = candidate_result.get("html") or ""
        if not html:
            continue
        candidate_office = {**office_row, "table_no": candidate_no, "find_date_in_infobox": False}
        table_data = _parse_office_html(
            candidate_office, html, url, party_list, offices_parser,
            cached_table_html=html, progress_callback=None,
        )
        if not table_data:
            continue
        missing = _missing_holder_keys(existing_terms, table_data, int(office_row.get("id") or 0), years_only, key_years_only=key_years_only)
        missing_exact = len(missing)
        missing_years = len(
            _missing_holder_keys(existing_terms, table_data, int(office_row.get("id") or 0), years_only, key_years_only=True)
        )
        improved = (
            (missing_exact < best_missing) or
            (missing_exact == best_missing and missing_years < current_missing_years) or
            (missing_exact == best_missing and missing_years == current_missing_years and best_rows is not None and len(table_data) > len(best_rows))
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
    current_html = get_table_html_cached(
        url,
        table_no,
        refresh=refresh_table_cache,
        use_full_page=bool(office_row.get("use_full_page_for_table")),
    ).get("html") or ""
    current_rows = _parse_office_html(
        {**office_row, "find_date_in_infobox": False},
        current_html,
        url,
        party_list,
        offices_parser,
        cached_table_html=current_html if current_html else None,
        progress_callback=None,
    )
    missing_before_set = _missing_holder_keys(existing_terms, current_rows, office_id, years_only, key_years_only=key_years_only)
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
    missing_after_set = _missing_holder_keys(existing_terms, found_rows, office_id, years_only, key_years_only=key_years_only)
    key_fn = _holder_key_from_existing_term_years if key_years_only else _holder_key_from_existing_term
    return {
        "found_table_no": int(found_table_no),
        "missing_before": len(missing_before_set),
        "missing_after": len(missing_after_set),
        "missing_labels_after": _missing_holders_display(existing_terms, missing_after_set, key_fn),
        "rows": found_rows,
    }
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

    # Single-individual bio run: resolve ref to wiki_url, run bio once, return
    if run_mode == "single_bio":
        ref = (individual_ref or "").strip()
        if not ref:
            logger.log("single_bio requires individual_ref (id or Wikipedia URL).", True)
            logger.close()
            return {"office_count": 0, "message": "Individual (ID or URL) required.", "bio_success_count": 0, "bio_error_count": 1, "bio_errors": [{"url": "", "error": "individual_ref required"}]}
        if ref.isdigit():
            ind = db_individuals.get_individual(int(ref))
            if not ind:
                logger.log(f"No individual with id={ref}.", True)
                logger.close()
                return {"office_count": 0, "message": f"No individual with id {ref}.", "bio_success_count": 0, "bio_error_count": 1, "bio_errors": [{"url": ref, "error": "Individual not found"}]}
            wiki_url = ind.get("wiki_url") or ""
        else:
            wiki_url = ref
            if not wiki_url.startswith("http"):
                wiki_url = ("https://en.wikipedia.org" + wiki_url) if wiki_url.startswith("/") else f"https://en.wikipedia.org/wiki/{wiki_url}"
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

    if run_mode == "selected_bios":
        from src.scraper import parse_core
        selected_ids = sorted({int(i) for i in (individual_ids or []) if int(i) > 0})
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
            if cancel_check and cancel_check():
                logger.log("Selected bios run cancelled by user.", True)
                break
            report("bio", bio_idx + 1, total_bios, "Updating selected individuals…", {"current": bio_idx + 1, "total": total_bios})
            individual = db_individuals.get_individual(individual_id)
            if not individual:
                bio_error_count += 1
                bio_errors.append({"url": str(individual_id), "error": "Individual not found"})
                continue
            wiki_url = (individual.get("wiki_url") or "").strip()
            if not wiki_url:
                page_path = (individual.get("page_path") or "").strip()
                if page_path:
                    wiki_url = page_path if page_path.startswith("http") else f"https://en.wikipedia.org/wiki/{page_path.lstrip('/')}"
            if not wiki_url:
                bio_error_count += 1
                bio_errors.append({"url": str(individual_id), "error": "Missing wiki_url/page_path"})
                continue
            if bio_idx > 0:
                time.sleep(1.5)
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

    # Bios only: update biography for every individual in the DB (no office table parsing).
    if run_mode == "bios_only":
        from src.scraper import parse_core
        data_cleanup = parse_core.DataCleanup(logger)
        biography = parse_core.Biography(logger, data_cleanup)
        to_fetch = list(db_individuals.get_all_individual_wiki_urls())
        to_fetch = [u for u in to_fetch if (u or "").strip()]
        total_bios = len(to_fetch)
        bio_success_count = 0
        bio_error_count = 0
        bio_errors: list[dict[str, str]] = []
        bio_fetched: set[str] = set()
        for bio_idx, wiki_url in enumerate(to_fetch):
            if cancel_check and cancel_check():
                logger.log("Bios only run cancelled by user.", True)
                break
            report("bio", bio_idx + 1, total_bios, "Updating all individuals…", {"current": bio_idx + 1, "total": total_bios})
            if wiki_url in bio_fetched:
                continue
            if bio_idx > 0:
                time.sleep(1.5)
            try:
                bio_info = biography.biography_extract(wiki_url)
                bio_fetched.add(wiki_url)
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
                bio_fetched.add(wiki_url)
                bio_error_count += 1
                bio_errors.append({"url": wiki_url, "error": str(e)})
        logger.close()
        return {
            "office_count": 0,
            "terms_parsed": 0,
            "unique_wiki_urls": 0,
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

    # Get party list from DB (scraper format: { country: [ {name, link}, ... ] })
    party_list = db_parties.get_party_list_for_scraper()
    # Get runnable units from DB (hierarchy: page + office + table all enabled; else legacy offices)
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

    data_cleanup = parse_core.DataCleanup(logger)
    biography = parse_core.Biography(logger, data_cleanup)
    offices_parser = parse_core.Offices(logger, biography, data_cleanup)

    # Full run: purge individuals only; office_terms are replaced per-office after validation
    if run_mode == "full" and not dry_run and not test_run:
        db_individuals.purge_all_individuals()
        existing_individual_wiki_urls: set[str] = set()
    else:
        existing_individual_wiki_urls = db_individuals.get_all_individual_wiki_urls()

    total_terms = 0
    unique_wiki_urls: set[str] = set()
    all_office_data: list[dict] = []
    replaceable_office_ids: set[int] = set()
    revalidate_failed_offices: list[tuple[int, str]] = []
    revalidate_missing_holders_list: list[list[str]] = []  # full list per office when failure is "missing holders"
    office_errors: list[dict[str, Any]] = []  # offices that raised; run continues for others
    cancelled_early = False
    bio_success_count = 0
    bio_error_count = 0
    bio_errors: list[dict[str, str]] = []
    bio_skipped_count = 0
    living_success_count = 0
    living_error_count = 0
    living_errors: list[dict[str, str]] = []
    bio_cancelled = False

    for idx, office_row in enumerate(offices):
        office_index = idx + 1
        office_total = len(offices)
        if cancel_check and cancel_check():
            logger.log("Run cancelled by user.", True)
            report("office", idx, office_total, "Cancelled", {"terms_so_far": total_terms})
            # Build partial result (no DB write or bio after cancel)
            preview_rows = None
            if dry_run or test_run:
                preview_rows = []
                for row in all_office_data:
                    normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")))
                    if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
                        normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")), include_no_link=True)
                    if normalized is None:
                        continue
                    _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
                    wiki_link = row.get("Wiki Link") or ""
                    dead_link = bool(row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table")))
                    preview_rows.append({
                        "Wiki Link": wiki_link,
                        "Party": row.get("Party") or "",
                        "District": row.get("District") or "",
                        "Term Start": term_start_val if term_start_val else "",
                        "Term End": term_end_val if term_end_val else "",
                        "Term Start Year": term_start_year,
                        "Term End Year": term_end_year,
                        "Dead link": dead_link,
                        "Name (no link)": row.get("_name_from_table") if dead_link and wiki_link in ("", "No link") else None,
                    })
                preview_rows = preview_rows[:50]
            logger.close()
            rf = len(revalidate_failed_offices) > 0
            rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
            r_missing = revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
            return {
                "office_count": idx,
                "terms_parsed": total_terms,
                "unique_wiki_urls": len(unique_wiki_urls),
                "bio_success_count": 0,
                "bio_error_count": 0,
                "bio_errors": [],
                "bio_skipped_count": 0,
                "living_success_count": 0,
                "living_error_count": 0,
                "living_errors": [],
                "dry_run": dry_run,
                "test_run": test_run,
                "preview_rows": preview_rows,
                "cancelled": True,
                "message": f"Stopped after {idx} offices.",
                "revalidate_failed": rf,
                "revalidate_message": rm,
                "revalidate_missing_holders": r_missing,
                "office_errors": office_errors,
            }
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
        report(
            "table",
            office_index,
            office_total,
            f"{office_name} (table {table_no})",
            table_progress_extra,
        )
        if not url:
            logger.log(f"Skipping office id {office_id}: no URL", True)
            report("office", office_index, office_total, f"Skipped (no URL): {office_name}", {"terms_so_far": total_terms, **table_progress_extra})
            continue
        report("office", office_index, office_total, office_name, {"terms_so_far": total_terms, **table_progress_extra})
        logger.log(f"Processing office {office_index}/{office_total}: {office_name} ({url})", True)

        existing_terms = db_office_terms.get_existing_terms_for_office(office_id)
        has_existing = len(existing_terms) > 0

        use_full_page = bool(office_row.get("use_full_page_for_table"))
        cache_result = get_table_html_cached(url.strip(), table_no, refresh=refresh_table_cache, use_full_page=use_full_page)
        if "error" in cache_result:
            logger.log(f"Failed to get table for {url}: {cache_result['error']}", True)
            if has_existing:
                revalidate_failed_offices.append((office_id, f"Page or table failed: {cache_result['error']}. Kept existing terms."))
            continue
        if cancel_check and cancel_check():
            logger.log("Run cancelled by user.", True)
            cancelled_early = True
            break
        if "cache_file" in cache_result:
            logger.log(f"Cached table: {cache_result['cache_file']}", True)
        html_content = cache_result.get("html") or ""
        cached_table_html = html_content if html_content else None

        # When office has existing terms and find_date_in_infobox is on: validate from table-only parse first
        # so we don't fetch infoboxes only to fail validation later.
        use_infobox = bool(office_row.get("find_date_in_infobox"))
        if has_existing and use_infobox:
            office_row_no_infobox = {**office_row, "find_date_in_infobox": False}
            table_data_pre = _parse_office_html(
                office_row_no_infobox, html_content, url, party_list, offices_parser,
                cached_table_html=cached_table_html, progress_callback=None,
                max_rows=max_rows_per_table,
            )
            if len(table_data_pre) == 0:
                logger.log(f"Repopulate validation failed for {office_name}: table parsed to zero rows (existing had {len(existing_terms)}). Keeping existing terms.", True)
                revalidate_failed_offices.append((office_id, "Table parsed to zero rows. Kept existing terms."))
                continue
            old_holders_years = _filtered_existing_holder_keys(existing_terms, _holder_key_from_existing_term_years)
            years_only_pre = bool(office_row.get("years_only"))
            new_holders_years = _holder_keys_from_parsed_rows(table_data_pre, office_id, years_only_pre, key_years_only=True)
            missing_years = old_holders_years - new_holders_years
            if missing_years:
                if run_mode in ("full", "delta", "live_person"):
                    found_table_no, found_rows = _try_auto_update_table_no(
                        office_row, existing_terms, party_list, offices_parser,
                        refresh_table_cache=refresh_table_cache,
                        years_only=years_only_pre,
                        key_years_only=True,
                        current_missing_count=len(missing_years),
                    )
                    if found_table_no and found_rows is not None:
                        logger.log(f"Auto-updated table_no for {office_name}: {table_no} -> {found_table_no} based on validation match.", True)
                        office_row["table_no"] = int(found_table_no)
                        table_no = int(found_table_no)
                        table_data_pre = found_rows
                        missing_years = _missing_holder_keys(existing_terms, table_data_pre, office_id, years_only_pre, key_years_only=True)
                        if not (dry_run or test_run):
                            try:
                                with get_connection() as conn:
                                    conn.execute("UPDATE office_table_config SET table_no = ?, updated_at=datetime('now') WHERE id = ?", (table_no, office_id))
                                    conn.commit()
                            except sqlite3.IntegrityError as e:
                                logger.log(f"Could not update table_no for {office_name} (id={office_id}): {e}. Skipping this table.", True)
                                office_errors.append({"office_id": office_id, "office_name": office_name, "error": str(e)})
                                continue
                missing_list = _missing_holders_display(existing_terms, missing_years, _holder_key_from_existing_term_years)
                missing_str = _format_missing_holders(missing_list)
                force_replace_early = force_overwrite or (force_replace_office_ids and office_id in force_replace_office_ids)
                if force_replace_early:
                    logger.log(f"Force overwrite for {office_name}: table-only check found new list missing {len(missing_years)} holder(s); replacing anyway. Missing: {missing_str}", True)
                    replaceable_office_ids.add(office_id)
                elif missing_years:
                    logger.log(f"Repopulate validation failed for {office_name}: table-only check found new list missing {len(missing_years)} office holder(s). Skipping infobox fetch. Keeping existing terms. Missing: {missing_str}", True)
                    revalidate_failed_offices.append((office_id, "New list is missing office holders that were in existing data. Kept existing terms."))
                    revalidate_missing_holders_list.append(missing_list)
                    continue

        # Parse table (shared code path); report infobox progress when find_date_in_infobox
        table_data = _parse_office_html(
            office_row, html_content, url, party_list, offices_parser,
            cached_table_html=cached_table_html, progress_callback=report,
            progress_extra=table_progress_extra,
        )
        if max_rows_per_table is not None and max_rows_per_table >= 0:
            table_data = table_data[: max_rows_per_table]
        if bool(office_row.get("remove_duplicates")):
            table_data = _dedupe_parsed_rows(table_data, years_only=bool(office_row.get("years_only")))

        if has_existing and len(table_data) == 0:
            logger.log(f"Repopulate validation failed for {office_name}: table parsed to zero rows (existing had {len(existing_terms)}). Keeping existing terms.", True)
            revalidate_failed_offices.append((office_id, "Table parsed to zero rows. Kept existing terms."))
            continue

        if has_existing and table_data:
            force_replace = (force_replace_office_ids and office_id in force_replace_office_ids) or force_overwrite
            old_holders = _filtered_existing_holder_keys(existing_terms, _holder_key_from_existing_term)
            years_only = bool(office_row.get("years_only"))
            new_holders = _holder_keys_from_parsed_rows(table_data, office_id, years_only)
            missing = old_holders - new_holders
            if missing:
                if run_mode in ("full", "delta", "live_person"):
                    found_table_no, found_rows = _try_auto_update_table_no(
                        office_row, existing_terms, party_list, offices_parser,
                        refresh_table_cache=refresh_table_cache,
                        years_only=years_only,
                        key_years_only=False,
                        current_missing_count=len(missing),
                    )
                    if found_table_no and found_rows is not None:
                        logger.log(f"Auto-updated table_no for {office_name}: {table_no} -> {found_table_no} based on holder match.", True)
                        office_row["table_no"] = int(found_table_no)
                        table_no = int(found_table_no)
                        table_data = found_rows
                        missing = _missing_holder_keys(existing_terms, table_data, office_id, years_only)
                        if not (dry_run or test_run):
                            try:
                                with get_connection() as conn:
                                    conn.execute("UPDATE office_table_config SET table_no = ?, updated_at=datetime('now') WHERE id = ?", (table_no, office_id))
                                    conn.commit()
                            except sqlite3.IntegrityError as e:
                                logger.log(f"Could not update table_no for {office_name} (id={office_id}): {e}. Skipping this table.", True)
                                office_errors.append({"office_id": office_id, "office_name": office_name, "error": str(e)})
                                # Remove this office's rows from all_office_data so we don't write them
                                n_remove = sum(1 for r in all_office_data if r.get("_office_id") == office_id)
                                all_office_data[:] = [r for r in all_office_data if r.get("_office_id") != office_id]
                                total_terms -= n_remove
                                continue
                missing_list = _missing_holders_display(existing_terms, missing, _holder_key_from_existing_term)
                missing_str = _format_missing_holders(missing_list)
                if force_replace:
                    logger.log(f"Force override for {office_name}: replacing despite {len(missing)} holder(s) missing from new list. Missing: {missing_str}", True)
                    replaceable_office_ids.add(office_id)
                elif missing:
                    logger.log(f"Repopulate validation failed for {office_name}: new list is missing {len(missing)} office holder(s) that were in existing data. Keeping existing terms. Missing: {missing_str}", True)
                    revalidate_failed_offices.append((office_id, "New list is missing office holders that were in existing data. Kept existing terms."))
                    revalidate_missing_holders_list.append(missing_list)
                    continue
                else:
                    replaceable_office_ids.add(office_id)
            else:
                replaceable_office_ids.add(office_id)
        if cancel_check and cancel_check():
            logger.log("Run cancelled by user.", True)
            cancelled_early = True
            break

        for row in table_data:
            wiki_link = row.get("Wiki Link")
            if wiki_link and wiki_link != "No link":
                unique_wiki_urls.add(wiki_link)
            row["_office_id"] = office_id
            if office_row.get("office_details_id") is not None:
                row["_office_details_id"] = office_row["office_details_id"]
                row["_office_table_config_id"] = office_row.get("office_table_config_id") or office_id
                row["_country_id"] = office_row.get("country_id")
            row["_years_only"] = bool(office_row.get("years_only"))
            all_office_data.append(row)
        total_terms += len(table_data)

    if cancelled_early:
        report("office", idx, len(offices), "Cancelled", {"terms_so_far": total_terms})
        preview_rows = None
        if dry_run or test_run:
            preview_rows = []
            for row in all_office_data:
                normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")))
                if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
                    normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")), include_no_link=True)
                if normalized is None:
                    continue
                _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
                wiki_link = row.get("Wiki Link") or ""
                dead_link = bool(row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table")))
                preview_rows.append({
                    "Wiki Link": wiki_link,
                    "Party": row.get("Party") or "",
                    "District": row.get("District") or "",
                    "Term Start": term_start_val if term_start_val else "",
                    "Term End": term_end_val if term_end_val else "",
                    "Term Start Year": term_start_year,
                    "Term End Year": term_end_year,
                    "Dead link": dead_link,
                    "Name (no link)": row.get("_name_from_table") if dead_link and wiki_link in ("", "No link") else None,
                })
                preview_rows = preview_rows[:50]
            logger.close()
            rf = len(revalidate_failed_offices) > 0
            rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
            r_missing = revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
            return {
                "office_count": idx,
                "terms_parsed": total_terms,
                "unique_wiki_urls": len(unique_wiki_urls),
                "bio_success_count": 0,
                "bio_error_count": 0,
                "bio_errors": [],
                "bio_skipped_count": 0,
                "living_success_count": 0,
                "living_error_count": 0,
                "living_errors": [],
                "dry_run": dry_run,
                "test_run": test_run,
                "preview_rows": preview_rows,
                "cancelled": True,
                "message": f"Stopped after {idx} offices.",
                "revalidate_failed": rf,
                "revalidate_message": rm,
                "revalidate_missing_holders": r_missing,
                "office_errors": office_errors,
            }

    report("office", len(offices), len(offices), "All offices parsed", {"terms_so_far": total_terms})

    # Write to DB unless dry_run or test_run (same filter/normalize as preview via _normalize_row_for_import)
    if not dry_run and not test_run and all_office_data:
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
                if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
                    normalized = _normalize_row_for_import(row, include_no_link=True)
                if normalized is None:
                    continue
                _, term_start_val, term_end_val, term_start_imp, term_end_imp, term_start_year, term_end_year = normalized
                wiki_url = row.get("Wiki Link") or ""
                no_link_placeholder = wiki_url in ("", "No link") and row.get("_name_from_table")
                if no_link_placeholder:
                    wiki_url = "No link:" + str(office_id) + ":" + (row.get("_name_from_table") or "Unknown")
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
                    party_id = db_parties.resolve_party_id_by_country(country_id, party_text, conn=conn)
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
            bio_fetched_this_run: set[str] = set()
            for bio_idx, wiki_url in enumerate(to_fetch):
                if cancel_check and cancel_check():
                    logger.log("Run cancelled by user (during living update).", True)
                    bio_cancelled = True
                    break
                report("living", bio_idx + 1, total_bios, "Updating living individuals…", {"current": bio_idx + 1, "total": total_bios})
                if wiki_url in bio_fetched_this_run:
                    continue
                if bio_idx > 0:
                    time.sleep(1.5)
                try:
                    bio_info = biography.biography_extract(wiki_url)
                    bio_fetched_this_run.add(wiki_url)
                    if bio_info:
                        bio_info["wiki_url"] = wiki_url
                        bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                        dd, dd_imp = normalize_date(bio_info.get("death_date"))
                        bio_info["birth_date"] = bd
                        bio_info["death_date"] = dd
                        bio_info["birth_date_imprecise"] = bd_imp
                        bio_info["death_date_imprecise"] = dd_imp
                        db_individuals.upsert_individual(bio_info)
                        living_success_count += 1
                    else:
                        living_error_count += 1
                        err_msg = "No bio data extracted"
                        logger.log(f"Living update failed for {wiki_url}: {err_msg}", True)
                        living_errors.append({"url": wiki_url, "error": err_msg})
                except Exception as e:
                    bio_fetched_this_run.add(wiki_url)
                    living_error_count += 1
                    err_msg = str(e)
                    logger.log(f"Living update exception for {wiki_url}: {err_msg}", True)
                    living_errors.append({"url": wiki_url, "error": err_msg})
        else:
            # Full/delta: bio only for new individuals (not already in DB).
            to_fetch = list(unique_wiki_urls - existing_individual_wiki_urls)
            bio_skipped_count = len(unique_wiki_urls) - len(to_fetch)
            if bio_skipped_count > 0:
                logger.log(f"Skipping {bio_skipped_count} individuals (already in DB); fetching bio for {len(to_fetch)} new.", True)
                report("bio", 0, len(to_fetch), f"Skipped {bio_skipped_count} (in DB). Fetching {len(to_fetch)} new…", {"bio_skipped": bio_skipped_count})
            total_bios = len(to_fetch)
            # Build bio cache from table parse so we skip re-fetch when find_date_in_infobox was used (key by normalized URL)
            bio_cache: dict[str, dict] = {}
            for row in all_office_data:
                wiki_url_row = row.get("Wiki Link")
                if wiki_url_row and wiki_url_row != "No link" and row.get("_bio_details"):
                    key = normalize_wiki_url(wiki_url_row) or wiki_url_row
                    if key not in bio_cache:
                        bio_cache[key] = row["_bio_details"]
            bio_fetched_this_run: set[str] = set()
            for bio_idx, wiki_url in enumerate(to_fetch):
                if cancel_check and cancel_check():
                    logger.log("Run cancelled by user (during bio fetch).", True)
                    bio_cancelled = True
                    break
                report("bio", bio_idx + 1, total_bios, "Fetching biographies (new individuals)…", {"current": bio_idx + 1, "total": total_bios, "bio_skipped": bio_skipped_count})
                if wiki_url in bio_fetched_this_run:
                    continue
                try:
                    bio_cache_key = normalize_wiki_url(wiki_url) or wiki_url
                    if bio_cache_key in bio_cache:
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
                        bio_fetched_this_run.add(wiki_url)
                        bio_success_count += 1
                    else:
                        if bio_idx > 0:
                            time.sleep(1.5)
                        bio_info = biography.biography_extract(wiki_url)
                        bio_fetched_this_run.add(wiki_url)
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
                            err_msg = "No bio data extracted (e.g. 403 or empty page)"
                            logger.log(f"Bio failed for {wiki_url}: {err_msg}", True)
                            bio_errors.append({"url": wiki_url, "error": err_msg})
                except Exception as e:
                    bio_fetched_this_run.add(wiki_url)
                    bio_error_count += 1
                    err_msg = str(e)
                    logger.log(f"Bio exception for {wiki_url}: {err_msg}", True)
                    bio_errors.append({"url": wiki_url, "error": err_msg})

            # Optional second pass: update living individuals (death_date null). Full refresh of bio fields.
            if run_bio:
                to_fetch_living = list(db_individuals.get_living_individual_wiki_urls())
                total_living = len(to_fetch_living)
                if total_living > 0:
                    logger.log(f"Update living individuals: refreshing bio for {total_living} people (death_date null).", True)
                    report("living", 0, total_living, f"Updating {total_living} living individuals…", {})
                    living_fetched_this_run: set[str] = set()
                    for liv_idx, wiki_url in enumerate(to_fetch_living):
                        if cancel_check and cancel_check():
                            logger.log("Run cancelled by user (during living update).", True)
                            bio_cancelled = True
                            break
                        report("living", liv_idx + 1, total_living, "Updating living individuals…", {"current": liv_idx + 1, "total": total_living})
                        if wiki_url in living_fetched_this_run:
                            continue
                        if liv_idx > 0:
                            time.sleep(1.5)
                        try:
                            bio_info = biography.biography_extract(wiki_url)
                            living_fetched_this_run.add(wiki_url)
                            if bio_info:
                                bio_info["wiki_url"] = wiki_url
                                bd, bd_imp = normalize_date(bio_info.get("birth_date"))
                                dd, dd_imp = normalize_date(bio_info.get("death_date"))
                                bio_info["birth_date"] = bd
                                bio_info["death_date"] = dd
                                bio_info["birth_date_imprecise"] = bd_imp
                                bio_info["death_date_imprecise"] = dd_imp
                                db_individuals.upsert_individual(bio_info)
                                living_success_count += 1
                            else:
                                living_error_count += 1
                                err_msg = "No bio data extracted"
                                logger.log(f"Living update failed for {wiki_url}: {err_msg}", True)
                                living_errors.append({"url": wiki_url, "error": err_msg})
                        except Exception as e:
                            living_fetched_this_run.add(wiki_url)
                            living_error_count += 1
                            err_msg = str(e)
                            logger.log(f"Living update exception for {wiki_url}: {err_msg}", True)
                            living_errors.append({"url": wiki_url, "error": err_msg})

    if bio_cancelled:
        logger.close()
        report("complete", 1, 1, "Stopped (bio)", {"terms_parsed": total_terms, "unique_wiki_urls": len(unique_wiki_urls)})
        preview_rows = None
        if dry_run or test_run:
            preview_rows = []
            for row in all_office_data:
                normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")))
                if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
                    normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")), include_no_link=True)
                if normalized is None:
                    continue
                _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
                wiki_link = row.get("Wiki Link") or ""
                dead_link = bool(row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table")))
                preview_rows.append({
                    "Wiki Link": wiki_link,
                    "Party": row.get("Party") or "",
                    "District": row.get("District") or "",
                    "Term Start": term_start_val if term_start_val else "",
                    "Term End": term_end_val if term_end_val else "",
                    "Term Start Year": term_start_year,
                    "Term End Year": term_end_year,
                    "Dead link": dead_link,
                    "Name (no link)": row.get("_name_from_table") if dead_link and wiki_link in ("", "No link") else None,
                })
            preview_rows = preview_rows[:50]
        rf = len(revalidate_failed_offices) > 0
        rm = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
        r_missing = revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None
        return {
            "office_count": len(offices),
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
            "cancelled": True,
            "message": "Stopped during bio/living update.",
            "revalidate_failed": rf,
            "revalidate_message": rm,
            "revalidate_missing_holders": r_missing,
            "office_errors": office_errors,
        }

    logger.close()
    report("complete", 1, 1, "Done", {"terms_parsed": total_terms, "unique_wiki_urls": len(unique_wiki_urls)})

    # Preview rows: same filter/normalize as import so UI shows exactly what would be in the table (include dead-link / name-only rows)
    preview_rows = None
    if dry_run or test_run:
        preview_rows = []
        for row in all_office_data:
            normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")))
            if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
                normalized = _normalize_row_for_import(row, years_only=bool(row.get("_years_only")), include_no_link=True)
            if normalized is None:
                continue
            _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
            wiki_link = row.get("Wiki Link") or ""
            dead_link = bool(row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table")))
            preview_rows.append({
                "Wiki Link": wiki_link,
                "Party": row.get("Party") or "",
                "District": row.get("District") or "",
                "Term Start": term_start_val if term_start_val else "",
                "Term End": term_end_val if term_end_val else "",
                "Term Start Year": term_start_year,
                "Term End Year": term_end_year,
                "Dead link": dead_link,
                "Name (no link)": row.get("_name_from_table") if dead_link and wiki_link in ("", "No link") else None,
            })
        preview_rows = preview_rows[:50]

    revalidate_failed = len(revalidate_failed_offices) > 0
    revalidate_message = revalidate_failed_offices[0][1] if revalidate_failed_offices else None
    if revalidate_failed and len(revalidate_failed_offices) > 1:
        revalidate_message = f"{len(revalidate_failed_offices)} office(s) skipped (validation failed). {revalidate_message}"
    revalidate_missing_holders = revalidate_missing_holders_list[0] if revalidate_missing_holders_list else None

    return {
        "office_count": len(offices),
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
            office_row, html_content, url, party_list, offices_parser,
            cached_table_html=cached_table_html, progress_callback=progress_callback,
            max_rows=max_rows,
        )
        if bool(office_row.get("remove_duplicates")):
            table_data = _dedupe_parsed_rows(table_data, years_only=bool(office_row.get("years_only")))
    except Exception as e:
        raw_max = max_rows if max_rows is not None else 100
        raw = get_raw_table_preview(url, int(office_row.get("table_no") or 1), raw_max)
        return {"preview_rows": [], "raw_table_preview": raw, "error": str(e)}

    # Same filter/normalize as import: only rows that would be inserted (include dead-link / name-only rows)
    years_only = bool(office_row.get("years_only"))
    preview_rows = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if normalized is None and (row.get("Wiki Link") or "") in ("", "No link") and row.get("_name_from_table"):
            normalized = _normalize_row_for_import(row, years_only=years_only, include_no_link=True)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
        wiki_link = row.get("Wiki Link") or ""
        dead_link = bool(row.get("_dead_link") or (wiki_link in ("", "No link") and row.get("_name_from_table")))
        # #region agent log
        if dead_link and wiki_link in ("", "No link"):
            try:
                import json
                from pathlib import Path
                _dp = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                open(_dp, "a", encoding="utf-8").write(json.dumps({"location": "runner:preview_with_config", "message": "adding no-link row to preview", "data": {"name_from_table": (row.get("_name_from_table") or "")[:80], "term_start_year": term_start_year, "term_end_year": term_end_year}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
            except Exception:
                pass
        # #endregion
        preview_rows.append({
            "Wiki Link": wiki_link,
            "Party": row.get("Party") or "",
            "District": row.get("District") or "",
            "Term Start": term_start_val if term_start_val else "",
            "Term End": term_end_val if term_end_val else "",
            "Term Start Year": term_start_year,
            "Term End Year": term_end_year,
            "Dead link": dead_link,
            "Name (no link)": row.get("_name_from_table") if dead_link and wiki_link in ("", "No link") else None,
        })
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
            missing = _missing_holder_keys(existing_terms, table_data, int(tc_id), years_only, key_years_only=False)
            if missing:
                revalidate_failed = True
                revalidate_message = "New list found. Existing office holders are missing from this preview list."
                revalidate_missing_holders = _missing_holders_display(existing_terms, missing, _holder_key_from_existing_term)

    raw_table_preview = None
    if not preview_rows and not table_data:
        raw_max = max_rows if max_rows is not None else 100
        raw_table_preview = get_raw_table_preview(url, int(office_row.get("table_no") or 1), raw_max)
    return {
        "preview_rows": preview_rows,
        "raw_table_preview": raw_table_preview,
        "error": None,
        "revalidate_failed": revalidate_failed,
        "revalidate_message": revalidate_message,
        "revalidate_missing_holders": revalidate_missing_holders,
    }
