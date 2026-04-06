# -*- coding: utf-8 -*-
"""Unit tests for src/services/wikipedia_submit.py.

All tests mock requests.Session — no live HTTP to Wikipedia.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.wikipedia_submit import (
    WikipediaSubmitError,
    WikipediaSubmitter,
    get_submitter,
    reset_submitter,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_submitter()
    yield
    reset_submitter()


# ---------------------------------------------------------------------------
# get_submitter — no credentials
# ---------------------------------------------------------------------------


def test_get_submitter_returns_none_without_credentials(monkeypatch):
    monkeypatch.delenv("WIKIPEDIA_BOT_USERNAME", raising=False)
    monkeypatch.delenv("WIKIPEDIA_BOT_PASSWORD", raising=False)
    assert get_submitter() is None


def test_get_submitter_returns_none_with_only_username(monkeypatch):
    monkeypatch.setenv("WIKIPEDIA_BOT_USERNAME", "bot_user")
    monkeypatch.delenv("WIKIPEDIA_BOT_PASSWORD", raising=False)
    assert get_submitter() is None


def test_get_submitter_returns_none_when_login_fails(monkeypatch):
    monkeypatch.setenv("WIKIPEDIA_BOT_USERNAME", "bot_user")
    monkeypatch.setenv("WIKIPEDIA_BOT_PASSWORD", "bot_pass")
    with patch("src.services.wikipedia_submit.WikipediaSubmitter.login", side_effect=Exception("fail")):
        result = get_submitter()
    assert result is None


def test_get_submitter_caches_after_success(monkeypatch):
    monkeypatch.setenv("WIKIPEDIA_BOT_USERNAME", "bot_user")
    monkeypatch.setenv("WIKIPEDIA_BOT_PASSWORD", "bot_pass")
    with patch("src.services.wikipedia_submit.WikipediaSubmitter.login"):
        s1 = get_submitter()
        s2 = get_submitter()
    assert s1 is s2


# ---------------------------------------------------------------------------
# _throttle
# ---------------------------------------------------------------------------


def test_throttle_sleeps_when_called_too_soon():
    submitter = WikipediaSubmitter("u", "p")
    submitter._last_request_at = 1e18  # far future — always too soon

    with patch("src.services.wikipedia_submit.time.sleep") as mock_sleep:
        import time

        with patch("src.services.wikipedia_submit.time.time", return_value=1e18):
            submitter._last_request_at = 1e18 - 0.1  # 0.1s ago
            submitter._throttle()
    # sleep should have been called since elapsed < 1.0s
    mock_sleep.assert_called_once()
    sleep_duration = mock_sleep.call_args[0][0]
    assert sleep_duration > 0


def test_throttle_does_not_sleep_when_enough_time_elapsed():
    submitter = WikipediaSubmitter("u", "p")
    submitter._last_request_at = 0.0  # epoch — lots of time has passed

    with patch("src.services.wikipedia_submit.time.sleep") as mock_sleep:
        submitter._throttle()
    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# login — failure path
# ---------------------------------------------------------------------------


def test_login_raises_on_failed_result():
    submitter = WikipediaSubmitter("u", "p")
    mock_session = MagicMock()

    # Step 1: get login token
    token_resp = MagicMock()
    token_resp.json.return_value = {"query": {"tokens": {"logintoken": "tok123"}}}

    # Step 2: login fails
    login_resp = MagicMock()
    login_resp.json.return_value = {"login": {"result": "Failed", "reason": "Bad password"}}

    mock_session.get.return_value = token_resp
    mock_session.post.return_value = login_resp

    submitter._session = mock_session

    with patch("src.services.wikipedia_submit.time.sleep"):
        with pytest.raises(WikipediaSubmitError, match="Wikipedia login failed"):
            submitter.login()


# ---------------------------------------------------------------------------
# submit_article — HTTP error path
# ---------------------------------------------------------------------------


def test_submit_article_raises_on_http_error():
    submitter = WikipediaSubmitter("u", "p")
    submitter._csrf_token = "csrf_tok"

    mock_session = MagicMock()
    import requests

    err_resp = MagicMock()
    err_resp.status_code = 403
    mock_session.post.return_value = err_resp
    err_resp.raise_for_status.side_effect = requests.HTTPError("403")

    submitter._session = mock_session

    with patch("src.services.wikipedia_submit.time.sleep"):
        with pytest.raises(requests.HTTPError):
            submitter.submit_article("Test Title", "== Wikitext ==")


def test_submit_article_raises_on_api_error_in_response():
    submitter = WikipediaSubmitter("u", "p")
    submitter._csrf_token = "csrf_tok"

    mock_session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers.get.return_value = None  # no Retry-After
    resp.json.return_value = {"error": {"code": "articleexists", "info": "Already exists"}}
    mock_session.post.return_value = resp

    submitter._session = mock_session

    with patch("src.services.wikipedia_submit.time.sleep"):
        with pytest.raises(WikipediaSubmitError, match="articleexists"):
            submitter.submit_article("Test Title", "== Wikitext ==")


def test_submit_article_raises_when_result_not_success():
    submitter = WikipediaSubmitter("u", "p")
    submitter._csrf_token = "csrf_tok"

    mock_session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers.get.return_value = None
    resp.json.return_value = {"edit": {"result": "Failure"}}
    mock_session.post.return_value = resp

    submitter._session = mock_session

    with patch("src.services.wikipedia_submit.time.sleep"):
        with pytest.raises(WikipediaSubmitError, match="did not succeed"):
            submitter.submit_article("Test Title", "== Wikitext ==")


def test_submit_article_success():
    submitter = WikipediaSubmitter("u", "p")
    submitter._csrf_token = "csrf_tok"

    mock_session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.headers.get.return_value = None
    resp.json.return_value = {"edit": {"result": "Success", "newrevid": 12345}}
    mock_session.post.return_value = resp

    submitter._session = mock_session

    with patch("src.services.wikipedia_submit.time.sleep"):
        result = submitter.submit_article("Test Title", "== Wikitext ==")

    assert result["result"] == "Success"
    assert result["newrevid"] == 12345
