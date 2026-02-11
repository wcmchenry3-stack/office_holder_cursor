# -*- coding: utf-8 -*-
"""
Run scraper using config and party list from DB, write results to DB.
Supports: dry_run / test_run (no DB write), row limits, Full / Delta / Live person modes.
"""

from __future__ import annotations

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
    rows_out = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
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
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    """Single code path: build config from office_row and run parser. Returns list of row dicts (parser output).
    When cached_table_html is provided, use it as the page content and table_no=1 (single table).
    progress_callback(phase, current, total, message, extra) is called when find_date_in_infobox and processing each row.
    When max_rows is set, only the first max_rows table rows are parsed (so infobox is only fetched for those rows)."""
    table_config = db_offices.office_row_to_table_config(office_row)
    office_details = db_offices.office_row_to_office_details(office_row)
    if cached_table_html is not None:
        html_content = cached_table_html
        table_config = {**table_config, "table_no": 1}
    def infobox_progress(current: int, total: int, message: str):
        if progress_callback:
            progress_callback("infobox", current, total, message, {})
    return offices_parser.process_table(
        html_content, table_config, office_details, url, party_list,
        progress_callback=infobox_progress if progress_callback else None,
        max_rows=max_rows,
    )


def _normalize_row_for_import(row: dict[str, Any], years_only: bool = False) -> tuple[dict, str | None, str | None, bool, bool, int | None, int | None] | None:
    """
    Same filter/normalize logic as the DB write path. Returns None if row should be skipped,
    else (row, term_start_val, term_end_val, term_start_imprecise, term_end_imprecise, term_start_year, term_end_year).
    When years_only is True (from row["_years_only"] or caller), accept rows with Term Start Year / Term End Year and leave dates null.
    """
    wiki_url = row.get("Wiki Link") or ""
    if not wiki_url or wiki_url == "No link":
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
        return None
    return (row, term_start_val, term_end_val, term_start_imp, term_end_imp, None, None)


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
    progress_callback: Callable[[str, int, int, str, dict], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """
    Main entry: load offices and party list from DB, run scraper, write to DB (unless dry_run/test_run).
    If run_mode == single_bio, individual_ref (id or Wikipedia URL) is required; runs bio for that one only.
    If run_mode == bios_only, only update bios for all individuals (no office table parsing).
    run_office_bio=False skips all bio phases after office parsing. refresh_table_cache=True refetches table HTML from Wikipedia.
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
    # Get offices from DB (only enabled ones are included in runs)
    offices = db_offices.list_offices()
    offices = [o for o in offices if o.get("enabled", 1) == 1]
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

    # Full run: purge office_terms and individuals first
    if run_mode == "full" and not dry_run and not test_run:
        db_office_terms.purge_all_office_terms()
        db_individuals.purge_all_individuals()
        existing_individual_wiki_urls: set[str] = set()
    else:
        existing_individual_wiki_urls = db_individuals.get_all_individual_wiki_urls()

    total_terms = 0
    unique_wiki_urls: set[str] = set()
    all_office_data: list[dict] = []
    bio_success_count = 0
    bio_error_count = 0
    bio_errors: list[dict[str, str]] = []
    bio_skipped_count = 0
    living_success_count = 0
    living_error_count = 0
    living_errors: list[dict[str, str]] = []

    for idx, office_row in enumerate(offices):
        if cancel_check and cancel_check():
            logger.log("Run cancelled by user.", True)
            report("office", idx, len(offices), "Cancelled", {"terms_so_far": total_terms})
            # Build partial result (no DB write or bio after cancel)
            preview_rows = None
            if dry_run or test_run:
                preview_rows = []
                for row in all_office_data:
                    normalized = _normalize_row_for_import(row)
                    if normalized is None:
                        continue
                    _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
                    preview_rows.append({
                        "Wiki Link": row.get("Wiki Link") or "",
                        "Party": row.get("Party") or "",
                        "District": row.get("District") or "",
                        "Term Start": term_start_val if term_start_val else "",
                        "Term End": term_end_val if term_end_val else "",
                        "Term Start Year": term_start_year,
                        "Term End Year": term_end_year,
                    })
                preview_rows = preview_rows[:50]
            logger.close()
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
            }
        office_id = office_row["id"]
        url = office_row.get("url") or ""
        office_name = office_row.get("name") or f"Office {office_id}"
        if not url:
            logger.log(f"Skipping office id {office_id}: no URL", True)
            report("office", idx + 1, len(offices), f"Skipped (no URL): {office_name}", {"terms_so_far": total_terms})
            continue
        report("office", idx + 1, len(offices), office_name, {"terms_so_far": total_terms})
        logger.log(f"Processing office {idx+1}/{len(offices)}: {office_name} ({url})", True)

        table_no = int(office_row.get("table_no") or 1)
        use_full_page = bool(office_row.get("use_full_page_for_table"))
        # #region agent log
        import json
        from pathlib import Path
        _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
        with open(_log_path, "a", encoding="utf-8") as _f:
            _f.write(json.dumps({"location": "runner.py:run_with_db", "message": "before get_table_html_cached", "data": {"office_id": office_id, "url": (url or "")[:80], "table_no": table_no, "refresh_table_cache": refresh_table_cache, "use_full_page": use_full_page, "hypothesisId": "H3"}, "timestamp": __import__("time").time() * 1000}) + "\n")
        # #endregion
        cache_result = get_table_html_cached(url.strip(), table_no, refresh=refresh_table_cache, use_full_page=use_full_page)
        if "error" in cache_result:
            logger.log(f"Failed to get table for {url}: {cache_result['error']}", True)
            continue
        if "cache_file" in cache_result:
            logger.log(f"Cached table: {cache_result['cache_file']}", True)
        html_content = cache_result.get("html") or ""
        cached_table_html = html_content if html_content else None

        # Parse table (shared code path); report infobox progress when find_date_in_infobox
        table_data = _parse_office_html(
            office_row, html_content, url, party_list, offices_parser,
            cached_table_html=cached_table_html, progress_callback=report,
        )
        if max_rows_per_table is not None and max_rows_per_table >= 0:
            table_data = table_data[: max_rows_per_table]

        for row in table_data:
            wiki_link = row.get("Wiki Link")
            if wiki_link and wiki_link != "No link":
                unique_wiki_urls.add(wiki_link)
            row["_office_id"] = office_id
            row["_years_only"] = bool(office_row.get("years_only"))
            all_office_data.append(row)
        total_terms += len(table_data)

    report("office", len(offices), len(offices), "All offices parsed", {"terms_so_far": total_terms})

    # Write to DB unless dry_run or test_run (same filter/normalize as preview via _normalize_row_for_import)
    if not dry_run and not test_run and all_office_data:
        report("saving", 0, 1, "Writing to database…", {"terms": total_terms})
        conn = get_connection()
        try:
            for row in all_office_data:
                office_id = row.get("_office_id")
                if office_id is None:
                    continue
                normalized = _normalize_row_for_import(row)
                if normalized is None:
                    continue
                _, term_start_val, term_end_val, term_start_imp, term_end_imp, term_start_year, term_end_year = normalized
                wiki_url = row.get("Wiki Link") or ""
                # Resolve or create individual
                ind = db_individuals.get_individual_by_wiki_url(wiki_url, conn=conn)
                individual_id = ind["id"] if ind else None
                if not ind:
                    # Create placeholder individual so we can link office_term
                    individual_id = db_individuals.upsert_individual(
                        {"wiki_url": wiki_url, "page_path": wiki_url.split("/")[-1] if wiki_url else None},
                        conn=conn,
                    )
                party_text = row.get("Party")
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
            bio_fetched_this_run: set[str] = set()
            for bio_idx, wiki_url in enumerate(to_fetch):
                report("bio", bio_idx + 1, total_bios, "Fetching biographies (new individuals)…", {"current": bio_idx + 1, "total": total_bios, "bio_skipped": bio_skipped_count})
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

    logger.close()
    report("complete", 1, 1, "Done", {"terms_parsed": total_terms, "unique_wiki_urls": len(unique_wiki_urls)})

    # Preview rows: same filter/normalize as import so UI shows exactly what would be in the table
    preview_rows = None
    if dry_run or test_run:
        preview_rows = []
        for row in all_office_data:
            normalized = _normalize_row_for_import(row)
            if normalized is None:
                continue
            _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
            preview_rows.append({
                "Wiki Link": row.get("Wiki Link") or "",
                "Party": row.get("Party") or "",
                "District": row.get("District") or "",
                "Term Start": term_start_val if term_start_val else "",
                "Term End": term_end_val if term_end_val else "",
                "Term Start Year": term_start_year,
                "Term End Year": term_end_year,
            })
        preview_rows = preview_rows[:50]

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
    # #region agent log
    import json
    from pathlib import Path
    _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
    with open(_log_path, "a", encoding="utf-8") as _f:
        _f.write(json.dumps({"location": "runner.py:preview_with_config", "message": "before get_table_html_cached", "data": {"url": url[:80], "table_no": table_no, "use_full_page": use_full_page, "hypothesisId": "H3"}, "timestamp": __import__("time").time() * 1000}) + "\n")
    # #endregion
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
    except Exception as e:
        raw_max = max_rows if max_rows is not None else 100
        raw = get_raw_table_preview(url, int(office_row.get("table_no") or 1), raw_max)
        return {"preview_rows": [], "raw_table_preview": raw, "error": str(e)}

    # Same filter/normalize as import: only rows that would be inserted, with normalized Term Start/End and optional years
    years_only = bool(office_row.get("years_only"))
    preview_rows = []
    for row in table_data:
        normalized = _normalize_row_for_import(row, years_only=years_only)
        if normalized is None:
            continue
        _, term_start_val, term_end_val, _ts_imp, _te_imp, term_start_year, term_end_year = normalized
        preview_rows.append({
            "Wiki Link": row.get("Wiki Link") or "",
            "Party": row.get("Party") or "",
            "District": row.get("District") or "",
            "Term Start": term_start_val if term_start_val else "",
            "Term End": term_end_val if term_end_val else "",
            "Term Start Year": term_start_year,
            "Term End Year": term_end_year,
        })
    if max_rows is not None:
        preview_rows = preview_rows[:max_rows]

    raw_table_preview = None
    if not preview_rows and not table_data:
        raw_max = max_rows if max_rows is not None else 100
        raw_table_preview = get_raw_table_preview(url, int(office_row.get("table_no") or 1), raw_max)
    return {"preview_rows": preview_rows, "raw_table_preview": raw_table_preview, "error": None}
