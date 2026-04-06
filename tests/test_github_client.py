# -*- coding: utf-8 -*-
"""Unit tests for src/services/github_client.py.

Covers:
- list_open_issues_by_label: empty result, populated result
- _get: 429 exponential backoff (retries, sleeps, then gives up)
- _post: 429 raises RuntimeError after 3 attempts
- get_github_client: returns None without GITHUB_TOKEN
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from src.services.github_client import GitHubClient, get_github_client, reset_github_client


@pytest.fixture(autouse=True)
def _reset():
    reset_github_client()
    yield
    reset_github_client()


# ---------------------------------------------------------------------------
# get_github_client singleton
# ---------------------------------------------------------------------------


def test_get_github_client_returns_none_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    assert get_github_client() is None


def test_get_github_client_returns_instance_with_token(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_fake_token")
    monkeypatch.setenv("GITHUB_REPO", "org/repo")
    client = get_github_client()
    assert isinstance(client, GitHubClient)


def test_get_github_client_caches_singleton(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_fake_token")
    monkeypatch.setenv("GITHUB_REPO", "org/repo")
    c1 = get_github_client()
    c2 = get_github_client()
    assert c1 is c2


# ---------------------------------------------------------------------------
# list_open_issues_by_label
# ---------------------------------------------------------------------------


def test_list_open_issues_by_label_empty_result():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=[]) as mock_get:
        result = client.list_open_issues_by_label("some-label")
    assert result == []
    mock_get.assert_called_once()


def test_list_open_issues_by_label_none_result():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=None):
        result = client.list_open_issues_by_label("some-label")
    assert result == []


def test_list_open_issues_by_label_returns_list():
    client = GitHubClient(token="tok", repo="org/repo")
    issues = [{"number": 1, "title": "Issue 1"}, {"number": 2, "title": "Issue 2"}]
    with patch.object(client, "_get", return_value=issues):
        result = client.list_open_issues_by_label("dq:dq-abc")
    assert result == issues


def test_list_open_issues_by_label_passes_correct_params():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=[]) as mock_get:
        client.list_open_issues_by_label("my-label")
    _, kwargs = mock_get.call_args
    assert kwargs["params"]["labels"] == "my-label"
    assert kwargs["params"]["state"] == "open"
    assert kwargs["params"]["per_page"] == 100


# ---------------------------------------------------------------------------
# _get: 429 backoff
# ---------------------------------------------------------------------------


def test_get_retries_on_429_and_returns_none_after_3():
    client = GitHubClient(token="tok", repo="org/repo")

    mock_resp = MagicMock()
    mock_resp.status_code = 429

    with (
        patch("src.services.github_client.httpx.get", return_value=mock_resp) as mock_get,
        patch("src.services.github_client.time.sleep") as mock_sleep,
    ):
        result = client._get("https://api.github.com/repos/org/repo/issues")

    assert result is None
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 2  # sleep after attempt 1 and 2; not after 3rd


def test_get_returns_json_on_success():
    client = GitHubClient(token="tok", repo="org/repo")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = [{"number": 1}]
    mock_resp.raise_for_status.return_value = None

    with patch("src.services.github_client.httpx.get", return_value=mock_resp):
        result = client._get("https://api.github.com/repos/org/repo/issues")

    assert result == [{"number": 1}]


def test_get_returns_none_on_http_status_error():
    import httpx

    client = GitHubClient(token="tok", repo="org/repo")

    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not found", request=MagicMock(), response=mock_resp
    )

    with patch("src.services.github_client.httpx.get", return_value=mock_resp):
        result = client._get("https://api.github.com/repos/org/repo/issues/999")

    assert result is None


# ---------------------------------------------------------------------------
# _post: 429 raises RuntimeError
# ---------------------------------------------------------------------------


def test_post_raises_runtime_error_after_3_429s():
    client = GitHubClient(token="tok", repo="org/repo")

    mock_resp = MagicMock()
    mock_resp.status_code = 429

    with (
        patch("src.services.github_client.httpx.post", return_value=mock_resp),
        patch("src.services.github_client.time.sleep"),
        pytest.raises(RuntimeError, match="rate-limited"),
    ):
        client._post("https://api.github.com/repos/org/repo/issues", json={})
