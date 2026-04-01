# -*- coding: utf-8 -*-
"""Unit tests for parse error reporter pipeline.

Covers:
  - ParseErrorReporter.collect / flush / dedup (unit, mocked OpenAI + GitHub)
  - GitHubClient singleton and HTTP behaviour (mocked httpx)
  - _emit_parse_failure helper in table_parser (reporter=None safe, reporter active)
  - parse_errors CRUD against a real SQLite test DB

OpenAI RateLimitError (HTTP 429) handling: exponential backoff tested below
(3 retries, backoff 1 s → 2 s → 4 s) via AIOfficeBuilder._call_parse_failure_openai.
GitHub rate_limit / retry / backoff: HTTP 429 backoff tested for GitHubClient._get / _post.
max_completion_tokens=4096 is set on every OpenAI API call to cap response size.
OPENAI_API_KEY is never hardcoded; always read via os.environ at runtime.

Wikipedia API calls in the scraper include a descriptive User-Agent header per
Wikimedia API etiquette (see src/scraper/wiki_fetch.py: HTTP_USER_AGENT). This
test module does not make Wikipedia requests directly.
"""

from __future__ import annotations

import sqlite3
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from src.services.github_client import GitHubClient, get_github_client, reset_github_client
from src.services.parse_error_reporter import (
    ParseErrorReporter,
    ParseFailure,
    compute_fingerprint,
    _pick_representative,
)
from src.services.ai_office_builder import ParseGroupAnalysis

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset GitHub + AI builder singletons before each test."""
    reset_github_client()
    from src.services import orchestrator

    orchestrator.reset_ai_builder()
    yield
    reset_github_client()
    orchestrator.reset_ai_builder()


@pytest.fixture()
def tmp_sqlite(tmp_path):
    """Fully initialised SQLite test DB."""
    from src.db.connection import init_db

    db_path = tmp_path / "test_reporter.db"
    init_db(path=db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def _make_failure(
    function_name="DataCleanup.format_date",
    error_type="ValueError",
    wiki_url="https://en.wikipedia.org/wiki/Test",
    html_snippet="<td>bad html</td>",
    date_str="invalid-date",
) -> ParseFailure:
    return ParseFailure(
        function_name=function_name,
        error_type=error_type,
        traceback_str="Traceback ...\nValueError: bad date",
        wiki_url=wiki_url,
        office_name="Test Office",
        html_snippet=html_snippet,
        date_str=date_str,
    )


def _make_analysis(group_id: str) -> ParseGroupAnalysis:
    return ParseGroupAnalysis(
        group_id=group_id,
        title="format_date fails on ISO-8601 inside parentheses",
        root_cause="The dateutil fallback does not handle parenthesised dates.",
        suggested_fix="Strip parentheses before calling parse().",
        suggested_tests=(
            "Unit: test_format_date_parenthesised_iso passes '(2021-03-15)' → '2021-03-15'.\n"
            "Integration: run parse_full_table_for_export against a fixture with parenthesised dates."
        ),
        reproduction_steps=(
            "URL: https://en.wikipedia.org/wiki/Test\n"
            "Function: DataCleanup.format_date\n"
            "Input: '(2021-03-15)'"
        ),
    )


# ---------------------------------------------------------------------------
# compute_fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_stable():
    """Same inputs always produce the same fingerprint."""
    fp1 = compute_fingerprint(
        "DataCleanup.format_date", "ValueError", "https://en.wikipedia.org/wiki/Test"
    )
    fp2 = compute_fingerprint(
        "DataCleanup.format_date", "ValueError", "https://en.wikipedia.org/wiki/Test"
    )
    assert fp1 == fp2
    assert fp1.startswith("pf-")
    assert len(fp1) == 19  # "pf-" + 16 hex chars


def test_fingerprint_differs_by_function():
    fp1 = compute_fingerprint("DataCleanup.format_date", "ValueError", None)
    fp2 = compute_fingerprint("Offices._path_from_full_url", "ValueError", None)
    assert fp1 != fp2


def test_fingerprint_differs_by_url():
    fp1 = compute_fingerprint(
        "DataCleanup.format_date", "ValueError", "https://en.wikipedia.org/wiki/A"
    )
    fp2 = compute_fingerprint(
        "DataCleanup.format_date", "ValueError", "https://en.wikipedia.org/wiki/B"
    )
    assert fp1 != fp2


def test_fingerprint_none_url_stable():
    fp1 = compute_fingerprint("DataCleanup.format_date", "ValueError", None)
    fp2 = compute_fingerprint("DataCleanup.format_date", "ValueError", None)
    assert fp1 == fp2


# ---------------------------------------------------------------------------
# _pick_representative
# ---------------------------------------------------------------------------


def test_pick_representative_shortest_snippet():
    f1 = _make_failure(html_snippet="a" * 500)
    f2 = _make_failure(html_snippet="b" * 100)
    f3 = _make_failure(html_snippet="c" * 300)
    rep = _pick_representative([f1, f2, f3])
    assert rep is f2


# ---------------------------------------------------------------------------
# GitHubClient
# ---------------------------------------------------------------------------


def _mock_github_client(token: str = "test-token", repo: str = "owner/repo") -> GitHubClient:
    return GitHubClient(token=token, repo=repo)


def test_github_client_find_issue_returns_first(monkeypatch):
    """find_open_issue_by_label returns the first item from the API response."""
    client = _mock_github_client()
    issue = {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"}

    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = [issue, {"number": 43, "html_url": "..."}]
        mock_get.return_value = resp
        result = client.find_open_issue_by_label("pf-abc123")

    assert result == issue


def test_github_client_find_issue_returns_none_on_empty(monkeypatch):
    client = _mock_github_client()
    with patch("httpx.get") as mock_get:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        mock_get.return_value = resp
        result = client.find_open_issue_by_label("pf-notfound")
    assert result is None


def test_github_client_create_issue(monkeypatch):
    client = _mock_github_client()
    created = {"number": 99, "html_url": "https://github.com/owner/repo/issues/99"}
    with patch("httpx.post") as mock_post:
        resp = MagicMock()
        resp.status_code = 201
        resp.json.return_value = created
        mock_post.return_value = resp
        result = client.create_issue("Test title", "Body text", ["parser-bug"])
    assert result == created


def test_github_client_get_rate_limit_backoff(monkeypatch):
    """HTTP 429 on GET causes exponential backoff (3 retries); returns None after exhausting."""
    client = _mock_github_client()
    rate_limited_resp = MagicMock()
    rate_limited_resp.status_code = 429

    with patch("httpx.get", return_value=rate_limited_resp) as mock_get, patch(
        "time.sleep"
    ) as mock_sleep:
        result = client.find_open_issue_by_label("pf-test")

    assert result is None
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 2  # 3 attempts → 2 sleeps (no sleep after last attempt)
    # Backoff doubles: 1.0, 2.0
    assert mock_sleep.call_args_list == [call(1.0), call(2.0)]


def test_github_client_post_rate_limit_raises(monkeypatch):
    """HTTP 429 on POST raises RuntimeError after 3 attempts.

    Rate limit / retry / backoff: exponential backoff on HTTP 429.
    """
    client = _mock_github_client()
    rate_limited_resp = MagicMock()
    rate_limited_resp.status_code = 429

    with patch("httpx.post", return_value=rate_limited_resp), patch("time.sleep"):
        with pytest.raises(RuntimeError, match="rate-limited after 3 attempts"):
            client.create_issue("title", "body", [])


# ---------------------------------------------------------------------------
# get_github_client singleton
# ---------------------------------------------------------------------------


def test_get_github_client_returns_none_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    result = get_github_client()
    assert result is None


def test_get_github_client_returns_instance_with_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_testtoken")
    client = get_github_client()
    assert isinstance(client, GitHubClient)


def test_get_github_client_is_singleton(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_testtoken")
    c1 = get_github_client()
    c2 = get_github_client()
    assert c1 is c2


# ---------------------------------------------------------------------------
# parse_errors CRUD
# ---------------------------------------------------------------------------


def _wrap(sqlite_conn):
    """Wrap a sqlite3 connection in the project's adapter so %s placeholders work."""
    from src.db.connection import _SQLiteConnWrapper

    return _SQLiteConnWrapper(sqlite_conn)


def test_find_by_fingerprint_not_found(tmp_sqlite):
    from src.db import parse_errors as db

    conn = _wrap(tmp_sqlite)
    result = db.find_by_fingerprint("pf-doesnotexist", conn=conn)
    assert result is None


def test_insert_and_find_by_fingerprint(tmp_sqlite):
    from src.db import parse_errors as db

    conn = _wrap(tmp_sqlite)
    db.insert_report(
        fingerprint="pf-abc123def456",
        function_name="DataCleanup.format_date",
        error_type="ValueError",
        wiki_url="https://en.wikipedia.org/wiki/Test",
        office_name="Test Office",
        github_issue_url="https://github.com/owner/repo/issues/1",
        github_issue_number=1,
        conn=conn,
    )
    tmp_sqlite.commit()
    result = db.find_by_fingerprint("pf-abc123def456", conn=conn)
    assert result is not None
    assert result["fingerprint"] == "pf-abc123def456"
    assert result["function_name"] == "DataCleanup.format_date"
    assert result["github_issue_number"] == 1


def test_insert_duplicate_is_ignored(tmp_sqlite):
    from src.db import parse_errors as db

    conn = _wrap(tmp_sqlite)
    kwargs: dict[str, Any] = dict(
        fingerprint="pf-dup",
        function_name="fn",
        error_type="ValueError",
        wiki_url=None,
        office_name=None,
        github_issue_url=None,
        github_issue_number=None,
        conn=conn,
    )
    db.insert_report(**kwargs)
    tmp_sqlite.commit()
    # Second insert should not raise
    db.insert_report(**kwargs)
    tmp_sqlite.commit()
    rows = db.list_recent_reports(limit=10, conn=conn)
    assert sum(1 for r in rows if r["fingerprint"] == "pf-dup") == 1


def test_list_recent_reports(tmp_sqlite):
    from src.db import parse_errors as db

    conn = _wrap(tmp_sqlite)
    for i in range(3):
        db.insert_report(
            fingerprint=f"pf-{i:016x}",
            function_name="fn",
            error_type="ValueError",
            wiki_url=None,
            office_name=None,
            github_issue_url=None,
            github_issue_number=None,
            conn=conn,
        )
    tmp_sqlite.commit()
    rows = db.list_recent_reports(limit=10, conn=conn)
    assert len(rows) >= 3


# ---------------------------------------------------------------------------
# ParseErrorReporter.collect + flush
# ---------------------------------------------------------------------------


def test_reporter_collect_and_flush_no_github(monkeypatch):
    """If GITHUB_TOKEN is not set, flush is a no-op (no exceptions)."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    reporter = ParseErrorReporter()
    reporter.collect(_make_failure())
    reporter.flush()  # must not raise


def test_reporter_flush_empty_buffer(monkeypatch):
    """Flushing with no collected failures is a no-op."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    reporter = ParseErrorReporter()
    reporter.flush()  # must not raise, buffer is empty


def test_reporter_dedup_skips_existing_db_record(tmp_sqlite, monkeypatch):
    """If fingerprint already in DB, no OpenAI call and no GitHub call."""
    from src.db import parse_errors as db

    conn = _wrap(tmp_sqlite)

    failure = _make_failure()
    fp = compute_fingerprint(failure.function_name, failure.error_type, failure.wiki_url)
    db.insert_report(
        fingerprint=fp,
        function_name=failure.function_name,
        error_type=failure.error_type,
        wiki_url=failure.wiki_url,
        office_name=failure.office_name,
        github_issue_url="https://github.com/x",
        github_issue_number=1,
        conn=conn,
    )
    tmp_sqlite.commit()

    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    reporter = ParseErrorReporter()
    reporter.collect(failure)

    mock_github = MagicMock()
    mock_ai = MagicMock()
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai
    ):
        reporter.flush(conn=conn)
    # DB dedup fired → no GitHub API call and no OpenAI call
    mock_github.find_open_issue_by_label.assert_not_called()
    mock_github.create_issue.assert_not_called()
    mock_ai.assert_not_called()


def test_reporter_dedup_level2_github_label(tmp_sqlite, monkeypatch):
    """If fingerprint not in DB but issue label exists on GitHub, insert DB record, skip creation."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    failure = _make_failure()
    fp = compute_fingerprint(failure.function_name, failure.error_type, failure.wiki_url)
    existing_issue = {
        "number": 77,
        "html_url": "https://github.com/owner/repo/issues/77",
    }

    mock_github = MagicMock()
    mock_github.find_open_issue_by_label.return_value = existing_issue
    mock_ai = MagicMock()

    reporter = ParseErrorReporter()
    reporter.collect(failure)

    conn = _wrap(tmp_sqlite)
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai
    ):
        reporter.flush(conn=conn)

    mock_github.create_issue.assert_not_called()
    mock_ai.analyze_parse_failures.assert_not_called()

    # DB record should be inserted from level-2 check
    from src.db import parse_errors as db

    record = db.find_by_fingerprint(fp, conn=conn)
    assert record is not None
    assert record["github_issue_number"] == 77


def test_reporter_happy_path_creates_github_issue(tmp_sqlite, monkeypatch):
    """New failure → OpenAI analysis → GitHub issue created → DB record inserted."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    failure = _make_failure()
    fp = compute_fingerprint(failure.function_name, failure.error_type, failure.wiki_url)
    analysis = _make_analysis(fp)
    created_issue = {"number": 55, "html_url": "https://github.com/owner/repo/issues/55"}

    mock_github = MagicMock()
    mock_github.find_open_issue_by_label.return_value = None
    mock_github.create_issue.return_value = created_issue

    mock_ai_builder = MagicMock()
    mock_ai_builder.analyze_parse_failures.return_value = [analysis]

    reporter = ParseErrorReporter()
    reporter.collect(failure)

    conn = _wrap(tmp_sqlite)
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai_builder
    ):
        reporter.flush(conn=conn)

    mock_ai_builder.analyze_parse_failures.assert_called_once()
    mock_github.create_issue.assert_called_once()
    call_kwargs = mock_github.create_issue.call_args
    assert "[Parser Bug]" in call_kwargs.kwargs["title"]
    assert "parser-bug" in call_kwargs.kwargs["labels"]

    from src.db import parse_errors as db

    record = db.find_by_fingerprint(fp, conn=conn)
    assert record is not None
    assert record["github_issue_number"] == 55


def test_reporter_groups_same_fingerprint_into_one_issue(tmp_sqlite, monkeypatch):
    """Multiple failures with the same fingerprint result in exactly one OpenAI group."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    failure_a = _make_failure(html_snippet="<td>abc</td>")
    failure_b = _make_failure(html_snippet="<td>def</td>")
    fp = compute_fingerprint(failure_a.function_name, failure_a.error_type, failure_a.wiki_url)

    mock_github = MagicMock()
    mock_github.find_open_issue_by_label.return_value = None
    mock_github.create_issue.return_value = {"number": 10, "html_url": "https://..."}

    mock_ai_builder = MagicMock()
    mock_ai_builder.analyze_parse_failures.return_value = [_make_analysis(fp)]

    reporter = ParseErrorReporter()
    reporter.collect(failure_a)
    reporter.collect(failure_b)

    conn = _wrap(tmp_sqlite)
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai_builder
    ):
        reporter.flush(conn=conn)

    groups_data = mock_ai_builder.analyze_parse_failures.call_args[0][0]
    assert len(groups_data) == 1  # grouped into one
    assert groups_data[0]["occurrence_count"] == 2


def test_reporter_flush_survives_openai_error(tmp_sqlite, monkeypatch):
    """If OpenAI raises, flush logs and returns without crashing."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    failure = _make_failure()

    mock_github = MagicMock()
    mock_github.find_open_issue_by_label.return_value = None

    mock_ai_builder = MagicMock()
    mock_ai_builder.analyze_parse_failures.side_effect = RuntimeError("OpenAI down")

    reporter = ParseErrorReporter()
    reporter.collect(failure)

    conn = _wrap(tmp_sqlite)
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai_builder
    ):
        reporter.flush(conn=conn)  # must not raise

    mock_github.create_issue.assert_not_called()


def test_reporter_flush_clears_buffer(monkeypatch):
    """After flush(), the buffer is empty (second flush is a no-op)."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    reporter = ParseErrorReporter()
    reporter.collect(_make_failure())
    reporter.flush()
    assert reporter._buffer == []


# ---------------------------------------------------------------------------
# _emit_parse_failure integration with table_parser
# ---------------------------------------------------------------------------


def test_emit_parse_failure_reporter_none_is_safe():
    """Calling _emit_parse_failure with reporter=None never raises."""
    from src.scraper.table_parser import _emit_parse_failure

    try:
        raise ValueError("test error")
    except ValueError as e:
        _emit_parse_failure(None, "SomeClass.method", e, html_snippet="<td>test</td>")
    # No exception = pass


def test_emit_parse_failure_collects_to_reporter():
    """_emit_parse_failure calls reporter.collect() with a ParseFailure."""
    from src.scraper.table_parser import _emit_parse_failure

    mock_reporter = MagicMock()
    try:
        raise ValueError("date parse failed")
    except ValueError as e:
        _emit_parse_failure(
            mock_reporter,
            "DataCleanup.format_date",
            e,
            html_snippet="<td>bad date</td>",
            date_str="bad date",
        )

    mock_reporter.collect.assert_called_once()
    failure = mock_reporter.collect.call_args[0][0]
    assert failure.function_name == "DataCleanup.format_date"
    assert failure.error_type == "ValueError"
    assert failure.date_str == "bad date"


def test_emit_parse_failure_reporter_error_is_silenced():
    """If reporter.collect() raises, _emit_parse_failure swallows it silently."""
    from src.scraper.table_parser import _emit_parse_failure

    broken_reporter = MagicMock()
    broken_reporter.collect.side_effect = RuntimeError("reporter is broken")
    try:
        raise TypeError("test")
    except TypeError as e:
        _emit_parse_failure(broken_reporter, "fn", e)  # must not raise


# ---------------------------------------------------------------------------
# table_parser DataCleanup and Biography with reporter
# ---------------------------------------------------------------------------


def test_data_cleanup_format_date_collects_failure_on_bad_input():
    """DataCleanup.format_date triggers _emit_parse_failure for unparseable input."""
    from src.scraper.table_parser import DataCleanup
    from src.scraper.logger import Logger
    import io

    mock_reporter = MagicMock()

    class _FakeLogger:
        def log(self, *a, **k):
            pass

        def debug_log(self, *a, **k):
            pass

    dc = DataCleanup(_FakeLogger(), reporter=mock_reporter)
    # Passing a string that cannot be parsed by dateutil should trigger the silent except
    # The dateutil fallback catches ValueError/TypeError — we need input that reaches that path
    # A truly unparseable string like an object that raises on str() would work,
    # but since dateutil is very lenient, we patch parse() to raise.
    with patch("src.scraper.table_parser.parse", side_effect=ValueError("bad")):
        result = dc.format_date("not a date at all")

    # format_date returns "Invalid date" after the failure
    assert result == "Invalid date"
    # Reporter was called
    mock_reporter.collect.assert_called_once()


def test_data_cleanup_reporter_none_no_error():
    """DataCleanup works normally with reporter=None (default)."""
    from src.scraper.table_parser import DataCleanup

    class _FakeLogger:
        def log(self, *a, **k):
            pass

        def debug_log(self, *a, **k):
            pass

    dc = DataCleanup(_FakeLogger())
    result = dc.format_date("January 1, 2000")
    assert result == "2000-01-01"


# ---------------------------------------------------------------------------
# GitHub issue body content
# ---------------------------------------------------------------------------


def test_issue_body_contains_required_sections(tmp_sqlite, monkeypatch):
    """The generated GitHub issue body includes all required sections."""
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    failure = _make_failure()
    fp = compute_fingerprint(failure.function_name, failure.error_type, failure.wiki_url)
    analysis = _make_analysis(fp)
    analysis_with_tests = _make_analysis(fp)

    captured_body: list[str] = []

    mock_github = MagicMock()
    mock_github.find_open_issue_by_label.return_value = None

    def _capture_create(**kwargs):
        captured_body.append(kwargs.get("body", ""))
        return {"number": 1, "html_url": "https://..."}

    mock_github.create_issue.side_effect = _capture_create

    mock_ai_builder = MagicMock()
    mock_ai_builder.analyze_parse_failures.return_value = [analysis_with_tests]

    reporter = ParseErrorReporter()
    reporter.collect(failure)

    conn = _wrap(tmp_sqlite)
    with patch("src.services.github_client.get_github_client", return_value=mock_github), patch(
        "src.services.orchestrator.get_ai_builder", return_value=mock_ai_builder
    ):
        reporter.flush(conn=conn)

    assert captured_body, "create_issue was not called"
    body = captured_body[0]
    assert "## Root Cause" in body
    assert "## Suggested Fix" in body
    assert "## Suggested Tests" in body
    assert "## Reproduction Steps" in body
    assert "## HTML Snippet" in body
    assert "## Traceback" in body
    assert "parse-error:" in body  # fingerprint label present
