# -*- coding: utf-8 -*-
"""Unit tests for src/services/quality_issue_reporter.py.

Tests cover:
- _fingerprint_label format
- _build_issue_title format
- _build_issue_body with and without record_data
- QualityIssueReporter.report: no-GitHub graceful degrade
- QualityIssueReporter._create_issues: DB-hit dedup skips creation
- QualityIssueReporter._create_issues: GitHub-label-hit dedup skips creation
- QualityIssueReporter._create_issues: new issue created and URL returned
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.quality_issue_reporter import (
    QualityIssueReporter,
    _build_issue_body,
    _build_issue_title,
    _fingerprint_label,
)
from src.services.data_quality_checker import QualityCheckResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singletons():
    from src.services.github_client import reset_github_client
    from src.services import orchestrator

    reset_github_client()
    orchestrator.reset_ai_builder()
    yield
    reset_github_client()
    orchestrator.reset_ai_builder()


def _make_result(
    record_type="individual",
    record_id=42,
    check_type="bad_dates",
    flagged_by="openai",
    concerns=None,
) -> QualityCheckResult:
    return QualityCheckResult(
        record_type=record_type,
        record_id=record_id,
        check_type=check_type,
        flagged_by=flagged_by,
        concerns=concerns or ["Birth date after death date"],
    )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def test_fingerprint_label_format():
    label = _fingerprint_label("dq-abc123")
    assert label == "dq:dq-abc123"


def test_build_issue_title_format():
    result = _make_result()
    title = _build_issue_title(result)
    assert "[Data Quality]" in title
    assert "bad_dates" in title
    assert "42" in title


def test_build_issue_body_contains_required_fields():
    result = _make_result(concerns=["Bad date", "Missing URL"])
    body = _build_issue_body(result)
    assert "individual" in body
    assert "42" in body
    assert "bad_dates" in body
    assert "openai" in body
    assert "Bad date" in body
    assert "Missing URL" in body


def test_build_issue_body_with_record_data():
    result = _make_result()
    record_data = {"full_name": "Jane Doe", "wiki_url": "/wiki/Jane_Doe", "office_name": "Mayor"}
    body = _build_issue_body(result, record_data=record_data)
    assert "Jane Doe" in body
    assert "/wiki/Jane_Doe" in body
    assert "Mayor" in body


def test_build_issue_body_without_record_data_no_context_section():
    result = _make_result()
    body = _build_issue_body(result, record_data=None)
    assert "Record Context" not in body


# ---------------------------------------------------------------------------
# QualityIssueReporter.report — no GitHub client
# ---------------------------------------------------------------------------


def test_report_returns_empty_list_when_no_github_token():
    with patch("src.services.github_client.get_github_client", return_value=None):
        reporter = QualityIssueReporter()
        urls = reporter.report([_make_result()])
    assert urls == []


def test_report_returns_empty_list_for_empty_results():
    reporter = QualityIssueReporter()
    urls = reporter.report([])
    assert urls == []


# ---------------------------------------------------------------------------
# QualityIssueReporter._create_issues — dedup paths
# ---------------------------------------------------------------------------


def _make_github_mock():
    github = MagicMock()
    github.find_open_issue_by_label.return_value = None
    github.create_issue.return_value = {
        "html_url": "https://github.com/org/repo/issues/99",
        "number": 99,
    }
    return github


def test_create_issues_db_hit_dedup_skips_creation(tmp_path):
    """If DB already has a github_issue_url for this fingerprint, skip creation."""
    result = _make_result()
    github = _make_github_mock()

    existing_row = {
        "fingerprint": "dq-abc",
        "github_issue_url": "https://github.com/org/repo/issues/1",
    }

    with (
        patch("src.services.quality_issue_reporter.db_dqr.make_fingerprint", return_value="dq-abc"),
        patch(
            "src.services.quality_issue_reporter.db_dqr.find_by_fingerprint",
            return_value=existing_row,
        ),
    ):
        reporter = QualityIssueReporter()
        conn = MagicMock()
        urls = reporter._create_issues([result], github, conn, {})

    assert urls == []
    github.create_issue.assert_not_called()


def test_create_issues_github_label_hit_dedup_skips_creation():
    """If GitHub has an open issue with the label, skip creation and back-fill DB."""
    result = _make_result()
    github = _make_github_mock()
    github.find_open_issue_by_label.return_value = {
        "html_url": "https://github.com/org/repo/issues/5",
        "number": 5,
    }

    with (
        patch("src.services.quality_issue_reporter.db_dqr.make_fingerprint", return_value="dq-abc"),
        patch("src.services.quality_issue_reporter.db_dqr.find_by_fingerprint", return_value=None),
        patch("src.services.quality_issue_reporter.db_dqr.update_github_issue") as mock_update,
    ):
        reporter = QualityIssueReporter()
        conn = MagicMock()
        urls = reporter._create_issues([result], github, conn, {})

    assert urls == []
    github.create_issue.assert_not_called()
    mock_update.assert_called_once()


def test_create_issues_new_issue_created_returns_url():
    """No existing DB row and no GitHub label → create issue, return URL."""
    result = _make_result()
    github = _make_github_mock()

    with (
        patch("src.services.quality_issue_reporter.db_dqr.make_fingerprint", return_value="dq-new"),
        patch("src.services.quality_issue_reporter.db_dqr.find_by_fingerprint", return_value=None),
        patch("src.services.quality_issue_reporter.db_dqr.update_github_issue") as mock_update,
    ):
        reporter = QualityIssueReporter()
        conn = MagicMock()
        urls = reporter._create_issues([result], github, conn, {})

    assert urls == ["https://github.com/org/repo/issues/99"]
    github.create_issue.assert_called_once()
    mock_update.assert_called_once_with(
        "dq-new", "https://github.com/org/repo/issues/99", 99, conn=conn
    )


def test_create_issues_github_runtime_error_skipped():
    """If create_issue raises RuntimeError, the issue is skipped (no crash)."""
    result = _make_result()
    github = _make_github_mock()
    github.create_issue.side_effect = RuntimeError("API failure")

    with (
        patch("src.services.quality_issue_reporter.db_dqr.make_fingerprint", return_value="dq-new"),
        patch("src.services.quality_issue_reporter.db_dqr.find_by_fingerprint", return_value=None),
    ):
        reporter = QualityIssueReporter()
        conn = MagicMock()
        urls = reporter._create_issues([result], github, conn, {})

    assert urls == []
