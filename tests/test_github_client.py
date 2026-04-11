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


def test_post_returns_json_on_success():
    client = GitHubClient(token="tok", repo="org/repo")
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.json.return_value = {"number": 42, "html_url": "https://github.com/org/repo/issues/42"}
    mock_resp.raise_for_status.return_value = None
    with patch("src.services.github_client.httpx.post", return_value=mock_resp):
        result = client._post("https://api.github.com/repos/org/repo/issues", json={"title": "X"})
    assert result["number"] == 42


def test_post_raises_on_http_error():
    import httpx

    client = GitHubClient(token="tok", repo="org/repo")
    mock_resp = MagicMock()
    mock_resp.status_code = 422
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Unprocessable", request=MagicMock(), response=mock_resp
    )
    with (
        patch("src.services.github_client.httpx.post", return_value=mock_resp),
        pytest.raises(RuntimeError, match="HTTP 422"),
    ):
        client._post("https://api.github.com/repos/org/repo/issues", json={})


# ---------------------------------------------------------------------------
# _put: backoff and success
# ---------------------------------------------------------------------------


def test_put_returns_json_on_success():
    client = GitHubClient(token="tok", repo="org/repo")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"commit": {"sha": "abc123"}}
    mock_resp.raise_for_status.return_value = None
    with patch("src.services.github_client.httpx.put", return_value=mock_resp):
        result = client._put("https://api.github.com/repos/org/repo/contents/file.py", json={})
    assert result["commit"]["sha"] == "abc123"


def test_put_raises_runtime_error_after_3_429s():
    client = GitHubClient(token="tok", repo="org/repo")
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    with (
        patch("src.services.github_client.httpx.put", return_value=mock_resp),
        patch("src.services.github_client.time.sleep"),
        pytest.raises(RuntimeError, match="rate-limited"),
    ):
        client._put("https://api.github.com/repos/org/repo/contents/file.py", json={})


def test_put_raises_on_http_error():
    import httpx

    client = GitHubClient(token="tok", repo="org/repo")
    mock_resp = MagicMock()
    mock_resp.status_code = 409
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Conflict", request=MagicMock(), response=mock_resp
    )
    with (
        patch("src.services.github_client.httpx.put", return_value=mock_resp),
        pytest.raises(RuntimeError, match="HTTP 409"),
    ):
        client._put("https://api.github.com/repos/org/repo/contents/file.py", json={})


# ---------------------------------------------------------------------------
# _get: RequestError path
# ---------------------------------------------------------------------------


def test_get_returns_none_on_request_error():
    import httpx

    client = GitHubClient(token="tok", repo="org/repo")
    with patch(
        "src.services.github_client.httpx.get",
        side_effect=httpx.RequestError("connect timeout"),
    ):
        result = client._get("https://api.github.com/repos/org/repo/issues")
    assert result is None


# ---------------------------------------------------------------------------
# find_open_issue_by_label
# ---------------------------------------------------------------------------


def test_find_open_issue_by_label_returns_first():
    client = GitHubClient(token="tok", repo="org/repo")
    issues = [{"number": 10, "title": "First"}, {"number": 11, "title": "Second"}]
    with patch.object(client, "_get", return_value=issues):
        result = client.find_open_issue_by_label("my-label")
    assert result == issues[0]


def test_find_open_issue_by_label_returns_none_on_empty():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=[]):
        result = client.find_open_issue_by_label("my-label")
    assert result is None


def test_find_open_issue_by_label_returns_none_on_none():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=None):
        result = client.find_open_issue_by_label("my-label")
    assert result is None


# ---------------------------------------------------------------------------
# get_default_branch_sha
# ---------------------------------------------------------------------------


def test_get_default_branch_sha_returns_sha():
    client = GitHubClient(token="tok", repo="org/repo")
    data = {"object": {"sha": "deadbeef"}}
    with patch.object(client, "_get", return_value=data):
        result = client.get_default_branch_sha("dev")
    assert result == "deadbeef"


def test_get_default_branch_sha_returns_none_on_failure():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=None):
        result = client.get_default_branch_sha("dev")
    assert result is None


# ---------------------------------------------------------------------------
# create_branch
# ---------------------------------------------------------------------------


def test_create_branch_returns_response_on_success():
    client = GitHubClient(token="tok", repo="org/repo")
    expected = {"ref": "refs/heads/fix/test", "object": {"sha": "abc"}}
    with patch.object(client, "_post", return_value=expected):
        result = client.create_branch("fix/test", "abc")
    assert result == expected


def test_create_branch_returns_none_on_runtime_error():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_post", side_effect=RuntimeError("POST failed")):
        result = client.create_branch("fix/test", "abc")
    assert result is None


# ---------------------------------------------------------------------------
# get_file_content
# ---------------------------------------------------------------------------


def test_get_file_content_decodes_base64():
    import base64

    client = GitHubClient(token="tok", repo="org/repo")
    encoded = base64.b64encode(b"print('hello')").decode("ascii")
    data = {"content": encoded + "\n", "sha": "fileshaabc"}
    with patch.object(client, "_get", return_value=data):
        result = client.get_file_content("src/hello.py")
    assert result is not None
    assert result["content"] == "print('hello')"
    assert result["sha"] == "fileshaabc"


def test_get_file_content_returns_none_when_no_content():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_get", return_value=None):
        result = client.get_file_content("src/hello.py")
    assert result is None


# ---------------------------------------------------------------------------
# create_issue
# ---------------------------------------------------------------------------


def test_create_issue_returns_response():
    client = GitHubClient(token="tok", repo="org/repo")
    expected = {"number": 99, "html_url": "https://github.com/org/repo/issues/99"}
    with patch.object(client, "_post", return_value=expected):
        result = client.create_issue("Bug title", "Body text", ["bug"])
    assert result["number"] == 99


# ---------------------------------------------------------------------------
# create_pull_request
# ---------------------------------------------------------------------------


def test_create_pull_request_returns_response_on_success():
    client = GitHubClient(token="tok", repo="org/repo")
    expected = {"number": 5, "html_url": "https://github.com/org/repo/pull/5"}
    with patch.object(client, "_post", return_value=expected):
        result = client.create_pull_request("Fix X", "Body", "fix/x", draft=True)
    assert result["number"] == 5


def test_create_pull_request_returns_none_on_failure():
    client = GitHubClient(token="tok", repo="org/repo")
    with patch.object(client, "_post", side_effect=RuntimeError("POST failed")):
        result = client.create_pull_request("Fix X", "Body", "fix/x")
    assert result is None
