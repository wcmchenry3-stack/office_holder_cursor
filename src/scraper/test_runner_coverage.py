"""Coverage-gap tests for runner.py.

Targets branches not exercised by the existing test suite:
  - _format_missing_holders: truncation logic
  - _missing_holders_display: empty set, deadlink filtering
  - _holder_keys_from_parsed_rows: name-from-table placeholder keys
  - _build_result_dict: cancelled/message fields
  - _cleanup_disk_cache: file deletion by age
  - preview_with_config: happy path, parse error fallback
  - run_with_db: single_bio mode, selected_bios empty, bios_only mode,
    no-offices early return

Run: pytest src/scraper/test_runner_coverage.py -v
"""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# _format_missing_holders
# ---------------------------------------------------------------------------

from src.scraper.runner import _format_missing_holders


def test_format_missing_holders_empty():
    assert _format_missing_holders([]) == ""


def test_format_missing_holders_within_limit():
    labels = ["Alice", "Bob", "Carol"]
    result = _format_missing_holders(labels, max_show=5)
    assert result == "Alice, Bob, Carol"


def test_format_missing_holders_exceeds_limit():
    labels = [f"Person {i}" for i in range(25)]
    result = _format_missing_holders(labels, max_show=20)
    assert "and 5 more" in result
    assert "Person 0" in result
    assert "Person 24" not in result


# ---------------------------------------------------------------------------
# _missing_holders_display
# ---------------------------------------------------------------------------

from src.scraper.runner import (
    _holder_key_from_existing_term,
    _missing_holders_display,
)


def _term(wiki_url: str, start: str | None = "2000", end: str | None = "2004") -> dict:
    return {"wiki_url": wiki_url, "term_start": start, "term_end": end}


def test_missing_holders_display_empty_missing_keys():
    terms = [_term("https://en.wikipedia.org/wiki/Alice")]
    result = _missing_holders_display(terms, set(), _holder_key_from_existing_term)
    assert result == []


def test_missing_holders_display_skips_deadlinks():
    """Terms with empty wiki_url (key[0]=='') are ignored."""
    terms = [_term("")]  # empty URL → deadlink key → skipped
    from src.scraper.wiki_fetch import canonical_holder_url as _canonical_holder_url

    key = (_canonical_holder_url(""), "", "")
    result = _missing_holders_display(terms, {key}, _holder_key_from_existing_term)
    assert result == []


def test_missing_holders_display_returns_label_for_matched_key():
    """Term whose key is in missing_keys → appears in labels."""
    from src.scraper.wiki_fetch import canonical_holder_url as _canonical_holder_url

    url = "https://en.wikipedia.org/wiki/John_Smith"
    terms = [_term(url, start="2001", end="2005")]
    key = (_canonical_holder_url(url), "", "")
    result = _missing_holders_display(terms, {key}, _holder_key_from_existing_term)
    assert len(result) == 1
    assert "2001" in result[0] and "2005" in result[0]


def test_missing_holders_display_term_without_dates():
    """Term with no start/end dates → label is just the name."""
    from src.scraper.wiki_fetch import canonical_holder_url as _canonical_holder_url

    url = "https://en.wikipedia.org/wiki/Jane"
    terms = [{"wiki_url": url, "term_start": None, "term_end": None}]
    key = (_canonical_holder_url(url), "", "")
    result = _missing_holders_display(terms, {key}, _holder_key_from_existing_term)
    assert len(result) == 1
    assert "(" not in result[0]  # no date parentheses


# ---------------------------------------------------------------------------
# _holder_keys_from_parsed_rows — name-from-table branch
# ---------------------------------------------------------------------------

from src.scraper.runner import _holder_keys_from_parsed_rows


def test_holder_keys_name_from_table_no_link():
    """Row with no wiki_link + _name_from_table → builds 'No link:' placeholder key."""
    rows = [
        {
            "Wiki Link": "",
            "_dead_link": False,
            "_name_from_table": "Unknown Person",
            "Term Start": "2000-01-01",
            "Term End": "2004-01-01",
            "Party": "",
            "District": "",
        }
    ]
    keys = _holder_keys_from_parsed_rows(rows, office_id=42, years_only=False)
    assert len(keys) == 1
    key_url = next(iter(keys))[0]
    assert "No link:" in key_url or key_url == ""


def test_holder_keys_with_valid_wiki_link():
    """Row with wiki_link → adds canonical URL key."""
    rows = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Alice",
            "_dead_link": False,
            "_name_from_table": None,
            "Term Start": "2000-01-01",
            "Term End": "2004-01-01",
            "Party": "",
            "District": "",
        }
    ]
    keys = _holder_keys_from_parsed_rows(rows, office_id=1, years_only=False)
    assert len(keys) == 1
    key_url = next(iter(keys))[0]
    assert "/wiki/alice" in key_url


def test_holder_keys_empty_rows():
    keys = _holder_keys_from_parsed_rows([], office_id=1, years_only=False)
    assert keys == set()


# ---------------------------------------------------------------------------
# _build_result_dict
# ---------------------------------------------------------------------------

from src.scraper.runner import _build_result_dict


def _base_result_kwargs(**overrides) -> dict:
    defaults = dict(
        office_count=3,
        offices_unchanged=1,
        total_terms=10,
        unique_wiki_urls={"https://en.wikipedia.org/wiki/A"},
        bio_success_count=2,
        bio_error_count=0,
        bio_errors=[],
        bio_skipped_count=0,
        living_success_count=1,
        living_error_count=0,
        living_errors=[],
        dry_run=False,
        test_run=False,
        preview_rows=None,
        revalidate_failed=False,
        revalidate_message=None,
        revalidate_missing_holders=None,
        office_errors=[],
    )
    defaults.update(overrides)
    return defaults


def test_build_result_dict_basic():
    result = _build_result_dict(**_base_result_kwargs())
    assert result["office_count"] == 3
    assert result["terms_parsed"] == 10
    assert result["unique_wiki_urls"] == 1  # len of set
    assert "cancelled" not in result
    assert "message" not in result


def test_build_result_dict_with_cancelled():
    result = _build_result_dict(**_base_result_kwargs(cancelled=True))
    assert result["cancelled"] is True


def test_build_result_dict_with_message():
    result = _build_result_dict(**_base_result_kwargs(message="Run complete"))
    assert result["message"] == "Run complete"


def test_build_result_dict_dry_run_flag():
    result = _build_result_dict(**_base_result_kwargs(dry_run=True))
    assert result["dry_run"] is True


# ---------------------------------------------------------------------------
# _cleanup_disk_cache
# ---------------------------------------------------------------------------

from src.scraper.runner import _cleanup_disk_cache


def test_cleanup_disk_cache_deletes_old_files(tmp_path, monkeypatch):
    """Files older than max_age_days are deleted; returns count."""
    cache_dir = tmp_path / "wiki_cache"
    cache_dir.mkdir()
    old_file = cache_dir / "abc123.json.gz"
    with gzip.open(old_file, "wt") as f:
        f.write("{}")
    # Back-date mtime by 31 days
    old_mtime = time.time() - 31 * 86400
    os.utime(old_file, (old_mtime, old_mtime))

    monkeypatch.setattr("src.scraper.table_cache._cache_dir", lambda: cache_dir)
    deleted = _cleanup_disk_cache(max_age_days=30)
    assert deleted == 1
    assert not old_file.exists()


def test_cleanup_disk_cache_keeps_recent_files(tmp_path, monkeypatch):
    """Files newer than max_age_days are kept."""
    cache_dir = tmp_path / "wiki_cache"
    cache_dir.mkdir()
    new_file = cache_dir / "recent.json.gz"
    with gzip.open(new_file, "wt") as f:
        f.write("{}")

    monkeypatch.setattr("src.scraper.table_cache._cache_dir", lambda: cache_dir)
    deleted = _cleanup_disk_cache(max_age_days=30)
    assert deleted == 0
    assert new_file.exists()


def test_cleanup_disk_cache_empty_dir(tmp_path, monkeypatch):
    cache_dir = tmp_path / "wiki_cache"
    cache_dir.mkdir()
    monkeypatch.setattr("src.scraper.table_cache._cache_dir", lambda: cache_dir)
    assert _cleanup_disk_cache() == 0


# ---------------------------------------------------------------------------
# Helpers for DB-dependent tests
# ---------------------------------------------------------------------------


def _init_test_db(tmp_path, monkeypatch):
    """Create a fresh DB in tmp_path and set env var. Returns db_path."""
    db_path = tmp_path / "runner_test.db"
    monkeypatch.setenv("OFFICE_HOLDER_DB_PATH", str(db_path))
    from src.db.connection import init_db

    init_db(path=db_path)
    return db_path


# ---------------------------------------------------------------------------
# run_with_db — single_bio mode
# ---------------------------------------------------------------------------


def test_run_with_db_single_bio_empty_ref_returns_error(tmp_path, monkeypatch):
    """single_bio with empty individual_ref returns error dict immediately."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="single_bio", individual_ref="")
    assert result["bio_error_count"] == 1
    assert result["bio_success_count"] == 0
    assert any("individual_ref required" in e["error"] for e in result["bio_errors"])


def test_run_with_db_single_bio_wiki_url_mocked(tmp_path, monkeypatch):
    """single_bio with a wiki URL: biography_extract mocked to return bio data."""
    _init_test_db(tmp_path, monkeypatch)

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Test Person",
            "birth_date": "1950-01-01",
            "death_date": None,
            "page_path": "Test_Person",
            "wiki_url": url,
        },
    )

    from src.scraper.runner import run_with_db

    result = run_with_db(
        run_mode="single_bio",
        individual_ref="https://en.wikipedia.org/wiki/Test_Person",
    )
    assert result["bio_success_count"] == 1
    assert result["bio_error_count"] == 0


def test_run_with_db_single_bio_unknown_digit_id(tmp_path, monkeypatch):
    """single_bio with a digit that doesn't match any individual → error."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="single_bio", individual_ref="99999")
    assert result["bio_error_count"] == 1
    assert "No individual" in result["message"]


def test_run_with_db_single_bio_bio_extract_fails(tmp_path, monkeypatch):
    """single_bio when biography_extract returns {} → bio_error_count=1."""
    _init_test_db(tmp_path, monkeypatch)

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {},
    )

    from src.scraper.runner import run_with_db

    result = run_with_db(
        run_mode="single_bio",
        individual_ref="https://en.wikipedia.org/wiki/Nobody",
    )
    assert result["bio_error_count"] == 1
    assert result["bio_success_count"] == 0


# ---------------------------------------------------------------------------
# run_with_db — selected_bios mode (empty list)
# ---------------------------------------------------------------------------


def test_run_with_db_selected_bios_empty_list(tmp_path, monkeypatch):
    """selected_bios with no individual_ids returns early with message."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[])
    assert result["bio_success_count"] == 0
    assert "message" in result


# ---------------------------------------------------------------------------
# run_with_db — bios_only mode
# ---------------------------------------------------------------------------


def test_run_with_db_bios_only_no_individuals(tmp_path, monkeypatch):
    """bios_only with empty DB → returns with bio counts all 0."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="bios_only")
    assert result["bio_success_count"] == 0
    assert result["bio_error_count"] == 0


def test_run_with_db_bios_only_with_individuals(tmp_path, monkeypatch):
    """bios_only with individuals in DB: biography_extract mocked, bio_success_count=1."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    # Insert one individual directly
    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/Test_Bio", "Test Bio", 0, 1),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Test Bio",
            "birth_date": "1950-01-01",
            "death_date": None,
            "page_path": "Test_Bio",
            "wiki_url": url,
        },
    )
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="bios_only")
    assert result["bio_success_count"] == 1
    assert result["bio_error_count"] == 0


# ---------------------------------------------------------------------------
# run_with_db — no offices early return
# ---------------------------------------------------------------------------


def test_run_with_db_no_offices_returns_message(tmp_path, monkeypatch):
    """run_with_db with no enabled offices → early return with message."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import run_with_db

    # Fresh DB has no offices; use office_ids=[] filter to force empty
    result = run_with_db(run_mode="delta", office_ids=[99999])
    assert result["office_count"] == 0
    assert "message" in result


# ---------------------------------------------------------------------------
# preview_with_config — happy path and error fallback
# ---------------------------------------------------------------------------


def _minimal_office_row(url: str = "https://en.wikipedia.org/wiki/Test") -> dict:
    return {
        "url": url,
        "table_no": 1,
        "table_rows": 0,
        "link_column": 0,
        "party_column": -1,
        "party_link": False,
        "term_start_column": 1,
        "term_end_column": 2,
        "district_column": -1,
        "years_only": False,
        "remove_duplicates": False,
        "use_full_page_for_table": False,
        "consolidate_rowspan_terms": False,
        "infobox_role_key": "",
        "infobox_role_key_filter_id": None,
        "row_filter": None,
        "country_name": "United States of America",
        "level_name": "Federal",
        "branch_name": "Legislative",
        "state_name": "",
        "name": "Test Office",
        "department": "",
        "notes": "",
        "office_state": "",
        "office_country": "United States of America",
        "office_level": "Federal",
        "office_branch": "Legislative",
    }


def test_preview_with_config_no_url_returns_error(tmp_path, monkeypatch):
    """office_row with no URL → immediate error response."""
    _init_test_db(tmp_path, monkeypatch)
    from src.scraper.runner import preview_with_config

    result = preview_with_config({"url": ""})
    assert result["error"] == "No URL configured"
    assert result["preview_rows"] == []


def test_preview_with_config_cache_error_returns_error(tmp_path, monkeypatch):
    """get_table_html_cached returning an error → propagated to caller."""
    _init_test_db(tmp_path, monkeypatch)

    monkeypatch.setattr(
        "src.scraper.runner.get_table_html_cached",
        lambda *a, **kw: {"error": "Network unreachable"},
    )

    from src.scraper.runner import preview_with_config

    result = preview_with_config(_minimal_office_row())
    assert result["error"] == "Network unreachable"
    assert result["preview_rows"] == []


def test_preview_with_config_parse_exception_returns_fallback(tmp_path, monkeypatch):
    """If _parse_office_html raises, preview returns raw_table_preview fallback."""
    _init_test_db(tmp_path, monkeypatch)

    monkeypatch.setattr(
        "src.scraper.runner.get_table_html_cached",
        lambda *a, **kw: {"html": "<table><tr><td>x</td></tr></table>"},
    )
    monkeypatch.setattr(
        "src.scraper.runner._parse_office_html",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("parse failed")),
    )
    monkeypatch.setattr(
        "src.scraper.runner.get_raw_table_preview",
        lambda *a, **kw: {"headers": [], "rows": []},
    )

    from src.scraper.runner import preview_with_config

    result = preview_with_config(_minimal_office_row())
    assert result["error"] == "parse failed"
    assert result["raw_table_preview"] is not None


def test_preview_with_config_happy_path(tmp_path, monkeypatch):
    """Happy path: get_table_html_cached + _parse_office_html both mocked → preview_rows returned."""
    _init_test_db(tmp_path, monkeypatch)

    _FAKE_ROW = {
        "Wiki Link": "https://en.wikipedia.org/wiki/Alice",
        "_dead_link": False,
        "_name_from_table": None,
        "Party": "",
        "District": "",
        "Term Start": "2000-01-01",
        "Term End": "2004-01-01",
        "term_start_year": 2000,
        "term_end_year": 2004,
    }

    monkeypatch.setattr(
        "src.scraper.runner.get_table_html_cached",
        lambda *a, **kw: {"html": "<table><tr><td>x</td></tr></table>"},
    )
    monkeypatch.setattr(
        "src.scraper.runner._parse_office_html",
        lambda *a, **kw: [_FAKE_ROW],
    )

    from src.scraper.runner import preview_with_config

    result = preview_with_config(_minimal_office_row())
    assert result["error"] is None
    assert isinstance(result["preview_rows"], list)
    assert len(result["preview_rows"]) >= 1


# ---------------------------------------------------------------------------
# run_with_db — selected_bios with actual individuals (loop body coverage)
# ---------------------------------------------------------------------------


def test_run_with_db_selected_bios_with_individual(tmp_path, monkeypatch):
    """selected_bios with a real individual in DB: biography_extract mocked → bio_success_count=1."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/SelectedPerson", "Selected Person", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE wiki_url = ?",
            ("https://en.wikipedia.org/wiki/SelectedPerson",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Selected Person",
            "birth_date": "1960-01-01",
            "death_date": None,
            "page_path": "SelectedPerson",
            "wiki_url": url,
        },
    )
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[individual_id])
    assert result["bio_success_count"] == 1
    assert result["bio_error_count"] == 0


def test_run_with_db_selected_bios_individual_not_found(tmp_path, monkeypatch):
    """selected_bios with non-existent individual_id → bio_error_count=1."""
    _init_test_db(tmp_path, monkeypatch)
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[99999])
    assert result["bio_error_count"] == 1
    assert result["bio_success_count"] == 0


# ---------------------------------------------------------------------------
# _normalize_row_for_import — include_no_link and years_only fallback branches
# ---------------------------------------------------------------------------

from src.scraper.runner import _normalize_row_for_import


def test_normalize_row_include_no_link_with_name():
    """include_no_link=True + _name_from_table set → row included even with no dates (line 165-166)."""
    row = {
        "Wiki Link": "No link",
        "_dead_link": False,
        "_name_from_table": "John Doe",
        "Term Start": None,
        "Term End": None,
        "Term Start Year": None,
        "Term End Year": None,
    }
    result = _normalize_row_for_import(row, years_only=False, include_no_link=True)
    assert result is not None
    _, ts, te, _, _, tsy, tey = result
    assert ts is None and te is None


def test_normalize_row_no_link_without_name_returns_none():
    """include_no_link=True but no _name_from_table and no dates → None (line 167)."""
    row = {
        "Wiki Link": "No link",
        "_dead_link": False,
        "_name_from_table": None,
        "Term Start": None,
        "Term End": None,
        "Term Start Year": None,
        "Term End Year": None,
    }
    result = _normalize_row_for_import(row, years_only=False, include_no_link=True)
    assert result is None


def test_normalize_row_years_only_fallback_when_no_dates():
    """No date values but Term Start Year set → falls back to year-only return (lines 160-163)."""
    row = {
        "Wiki Link": "https://en.wikipedia.org/wiki/Alice",
        "_dead_link": False,
        "_name_from_table": None,
        "Term Start": None,
        "Term End": None,
        "Term Start Year": 2005,
        "Term End Year": 2009,
    }
    result = _normalize_row_for_import(row, years_only=False)
    assert result is not None
    _, ts, te, _, _, tsy, tey = result
    assert ts is None
    assert tsy == 2005
    assert tey == 2009


# ---------------------------------------------------------------------------
# _canonical_holder_url — exception branch
# ---------------------------------------------------------------------------

from src.scraper.wiki_fetch import canonical_holder_url as _canonical_holder_url


def test_canonical_holder_url_empty_returns_empty():
    assert _canonical_holder_url("") == ""
    assert _canonical_holder_url(None) == ""


def test_canonical_holder_url_no_link_prefix():
    """'No link:' prefixed URL returned as-is."""
    url = "No link:42:John Doe"
    assert _canonical_holder_url(url) == url


def test_canonical_holder_url_wikipedia_normalized():
    """Standard wiki URL returns canonical /wiki/title form."""
    url = "https://en.wikipedia.org/wiki/Joe_Biden"
    result = _canonical_holder_url(url)
    assert result == "/wiki/joe_biden"


# ---------------------------------------------------------------------------
# _build_preview_rows — direct coverage
# ---------------------------------------------------------------------------

from src.scraper.runner import _build_preview_rows


def test_build_preview_rows_with_valid_row():
    """Row with a wiki link and dates → appears in preview output."""
    rows = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Alice",
            "_dead_link": False,
            "_name_from_table": None,
            "_years_only": False,
            "Party": "Democratic",
            "District": "",
            "Term Start": "2000-01-01",
            "Term End": "2004-01-01",
        }
    ]
    result = _build_preview_rows(rows)
    assert len(result) == 1
    assert result[0]["Wiki Link"] == "https://en.wikipedia.org/wiki/Alice"


def test_build_preview_rows_skips_no_dates_row():
    """Row with no dates and no year fallback → skipped."""
    rows = [
        {
            "Wiki Link": "https://en.wikipedia.org/wiki/Bob",
            "_dead_link": False,
            "_name_from_table": None,
            "_years_only": False,
            "Party": "",
            "District": "",
            "Term Start": None,
            "Term End": None,
            "Term Start Year": None,
            "Term End Year": None,
        }
    ]
    result = _build_preview_rows(rows)
    assert result == []


def test_build_preview_rows_name_from_table_included():
    """No-link row with _name_from_table is included via include_no_link fallback."""
    rows = [
        {
            "Wiki Link": "No link",
            "_dead_link": False,
            "_name_from_table": "Jane Doe",
            "_years_only": False,
            "Party": "",
            "District": "",
            "Term Start": None,
            "Term End": None,
            "Term Start Year": None,
            "Term End Year": None,
        }
    ]
    result = _build_preview_rows(rows)
    assert len(result) == 1
    assert result[0]["Name (no link)"] == "Jane Doe"
    assert result[0]["Dead link"] is True


def test_build_preview_rows_respects_max_rows():
    """max_rows limits output."""
    rows = [
        {
            "Wiki Link": f"https://en.wikipedia.org/wiki/Person{i}",
            "_dead_link": False,
            "_name_from_table": None,
            "_years_only": True,
            "Party": "",
            "District": "",
            "Term Start": None,
            "Term End": None,
            "Term Start Year": 2000 + i,
            "Term End Year": 2004 + i,
        }
        for i in range(10)
    ]
    result = _build_preview_rows(rows, max_rows=3)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# run_with_db — cancel_check fires during office loop
# ---------------------------------------------------------------------------


def test_run_with_db_cancel_check_cancels_during_loop(tmp_path, monkeypatch):
    """cancel_check returning True on first call → cancelled=True result."""
    _init_test_db(tmp_path, monkeypatch)

    # Provide a fake office list so the loop starts
    fake_office = {
        "id": 1,
        "url": "https://en.wikipedia.org/wiki/Test",
        "name": "Test Office",
        "enabled": 1,
        "table_no": 1,
        "years_only": False,
        "remove_duplicates": False,
        "link_column": 0,
        "party_column": -1,
        "term_start_column": 1,
        "term_end_column": 2,
        "district_column": -1,
        "use_full_page_for_table": False,
        "infobox_role_key": "",
        "infobox_role_key_filter_id": None,
        "row_filter": None,
        "consolidate_rowspan_terms": False,
        "party_link": False,
        "country_name": "United States of America",
        "level_name": "Federal",
        "branch_name": "Legislative",
        "state_name": "",
        "office_country": "United States of America",
        "office_state": "",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_details_id": None,
        "office_table_config_id": None,
    }
    monkeypatch.setattr("src.scraper.runner.db_offices.list_runnable_units", lambda: [fake_office])
    monkeypatch.setattr("src.scraper.runner.db_offices.list_offices", lambda: [fake_office])

    from src.scraper.runner import run_with_db

    calls = [0]

    def cancel_on_first():
        calls[0] += 1
        return True  # cancel immediately on first check

    result = run_with_db(run_mode="delta", cancel_check=cancel_on_first)
    assert result.get("cancelled") is True


# ---------------------------------------------------------------------------
# run_with_db — selected_bios edge cases (loop body branches)
# ---------------------------------------------------------------------------


def test_run_with_db_selected_bios_individual_no_wiki_url(tmp_path, monkeypatch):
    """Individual with no wiki_url but has page_path → constructs wiki URL."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            """INSERT INTO individuals (wiki_url, page_path, full_name, bio_batch, is_living)
               VALUES (?, ?, ?, ?, ?)""",
            ("", "PageOnly_Person", "Page Only", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE page_path = ?",
            ("PageOnly_Person",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Page Only",
            "birth_date": None,
            "death_date": None,
            "page_path": "PageOnly_Person",
            "wiki_url": url,
        },
    )
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[individual_id])
    assert result["bio_success_count"] == 1


def test_run_with_db_selected_bios_bio_returns_empty(tmp_path, monkeypatch):
    """selected_bios where biography_extract returns {} → bio_error_count incremented."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/NoBio", "No Bio", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE wiki_url = ?",
            ("https://en.wikipedia.org/wiki/NoBio",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {},
    )
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[individual_id])
    assert result["bio_error_count"] == 1
    assert result["bio_success_count"] == 0


def test_run_with_db_bios_only_bio_returns_empty(tmp_path, monkeypatch):
    """bios_only where biography_extract returns {} → bio_error_count incremented."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/EmptyBio", "Empty Bio", 0, 1),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {},
    )
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="bios_only")
    assert result["bio_error_count"] == 1
    assert result["bio_success_count"] == 0


def test_run_with_db_bios_only_bio_raises_exception(tmp_path, monkeypatch):
    """bios_only where biography_extract raises → exception caught, bio_error_count incremented."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/ExcBio", "Exc Bio", 0, 1),
        )
        conn.commit()
    finally:
        conn.close()

    def _raise(self, url, **kw):
        raise RuntimeError("network failure")

    monkeypatch.setattr("src.scraper.table_parser.Biography.biography_extract", _raise)
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="bios_only")
    assert result["bio_error_count"] == 1


def test_run_with_db_bios_only_two_individuals_processes_both(tmp_path, monkeypatch):
    """bios_only with two individuals: both are fetched and written to the DB."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/Person1", "Person 1", 0, 1),
        )
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/Person2", "Person 2", 0, 1),
        )
        conn.commit()
    finally:
        conn.close()

    # Rate limiting is now handled by wiki_throttle() inside biography_extract; patch it out.
    monkeypatch.setattr("src.scraper.wiki_fetch.wiki_throttle", lambda: None)
    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Person",
            "birth_date": None,
            "death_date": None,
            "page_path": "P",
            "wiki_url": url,
        },
    )

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="bios_only")
    assert result["bio_success_count"] == 2


def test_run_with_db_selected_bios_cancel_check(tmp_path, monkeypatch):
    """selected_bios cancel_check fires during loop → loop exits early."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/CancelBio", "Cancel Bio", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE wiki_url = ?",
            ("https://en.wikipedia.org/wiki/CancelBio",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(
        run_mode="selected_bios",
        individual_ids=[individual_id],
        cancel_check=lambda: True,  # cancel immediately
    )
    # bio_success_count==0 because cancel fires before biography_extract
    assert result["bio_success_count"] == 0


def test_run_with_db_selected_bios_individual_no_wiki_or_page(tmp_path, monkeypatch):
    """selected_bios: individual with no wiki_url and no page_path → bio_error_count incremented."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            """INSERT INTO individuals (wiki_url, page_path, full_name, bio_batch, is_living)
               VALUES (?, ?, ?, ?, ?)""",
            ("", "", "No URL Person", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE full_name = ?",
            ("No URL Person",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[individual_id])
    assert result["bio_error_count"] == 1
    assert "Missing" in result["bio_errors"][0]["error"]


def test_run_with_db_selected_bios_exception_from_bio(tmp_path, monkeypatch):
    """selected_bios where biography_extract raises → exception caught."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/ExcPerson", "Exc Person", 0, 1),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM individuals WHERE wiki_url = ?",
            ("https://en.wikipedia.org/wiki/ExcPerson",),
        ).fetchone()
        individual_id = row[0]
    finally:
        conn.close()

    def _raise(self, url, **kw):
        raise ConnectionError("timeout")

    monkeypatch.setattr("src.scraper.table_parser.Biography.biography_extract", _raise)
    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=[individual_id])
    assert result["bio_error_count"] == 1


def test_run_with_db_selected_bios_two_individuals_processes_both(tmp_path, monkeypatch):
    """selected_bios with two individuals: both are fetched and written to the DB."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/SelPerson1", "Sel Person 1", 0, 1),
        )
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/SelPerson2", "Sel Person 2", 0, 1),
        )
        conn.commit()
        rows = conn.execute(
            "SELECT id FROM individuals WHERE wiki_url IN (?, ?)",
            (
                "https://en.wikipedia.org/wiki/SelPerson1",
                "https://en.wikipedia.org/wiki/SelPerson2",
            ),
        ).fetchall()
        individual_ids = [r[0] for r in rows]
    finally:
        conn.close()

    # Rate limiting is now handled by wiki_throttle() inside biography_extract; patch it out.
    monkeypatch.setattr("src.scraper.wiki_fetch.wiki_throttle", lambda: None)
    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {
            "full_name": "Sel",
            "birth_date": None,
            "death_date": None,
            "page_path": "Sel",
            "wiki_url": url,
        },
    )

    from src.scraper.runner import run_with_db

    result = run_with_db(run_mode="selected_bios", individual_ids=individual_ids)
    assert result["bio_success_count"] == 2


def test_run_with_db_bios_only_cancel_check(tmp_path, monkeypatch):
    """bios_only cancel_check fires during loop → loop exits early."""
    db_path = _init_test_db(tmp_path, monkeypatch)

    from src.db.connection import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO individuals (wiki_url, full_name, bio_batch, is_living) VALUES (?, ?, ?, ?)",
            ("https://en.wikipedia.org/wiki/CancelBiosOnly", "Cancel Bios Only", 0, 1),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("src.scraper.runner.time.sleep", lambda s: None)
    monkeypatch.setattr(
        "src.scraper.table_parser.Biography.biography_extract",
        lambda self, url, **kw: {"full_name": "x", "bio_batch": 0, "wiki_url": url},
    )

    from src.scraper.runner import run_with_db

    # cancel_check returns True immediately → bios_only loop exits on first check
    result = run_with_db(run_mode="bios_only", cancel_check=lambda: True)
    # bio_success_count is 0 because cancel fires before biography_extract
    assert result["bio_success_count"] == 0
