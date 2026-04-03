# -*- coding: utf-8 -*-
"""
GitHub REST API client singleton.

All GitHub API calls in this project must go through this module — no direct
httpx/requests calls to api.github.com from routers, scrapers, or DB layer.

Provides:
  get_github_client() -> GitHubClient | None
      Lazy singleton. Returns None if GITHUB_TOKEN or GITHUB_REPO is not set,
      so callers can degrade gracefully without raising.

  reset_github_client() -> None
      Resets the singleton for tests.

--- Policy compliance ---

GitHub REST API:
  - Authentication: Bearer token from GITHUB_TOKEN env var (never hardcoded).
  - Rate limiting: exponential backoff on HTTP 429 (3 retries, 1 s → 2 s → 4 s).
  - Pagination: label-filter search uses per_page=10; only first page is read
    (we only need existence, not all matches).
  - GITHUB_TOKEN is never logged or included in error messages.
  See: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
"""

from __future__ import annotations

import logging
import os
import threading
import time

import httpx

logger = logging.getLogger(__name__)

_GITHUB_API_BASE = "https://api.github.com"
_DEFAULT_REPO = "wcmchenry3-stack/office_holder_cursor"

_client_lock = threading.Lock()
_client: GitHubClient | None = None


class GitHubClient:
    """Thin wrapper around the GitHub REST API.

    Uses httpx for HTTP calls. All methods implement exponential backoff on
    HTTP 429 (3 retries, 1 s → 2 s → 4 s) matching the OpenAI client pattern.

    Rate limit / retry / backoff: exponential backoff on HTTP 429.
    """

    def __init__(self, token: str, repo: str) -> None:
        self._repo = repo
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def find_open_issue_by_label(self, label: str) -> dict | None:
        """Return the first open issue with the given label, or None.

        Rate limit / retry / backoff: exponential backoff on HTTP 429.
        """
        url = f"{_GITHUB_API_BASE}/repos/{self._repo}/issues"
        params = {"labels": label, "state": "open", "per_page": 10}
        data = self._get(url, params=params)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None

    def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        """Create a new GitHub issue. Returns the response dict.

        Rate limit / retry / backoff: exponential backoff on HTTP 429.
        Raises RuntimeError if the API call fails after all retries.
        """
        url = f"{_GITHUB_API_BASE}/repos/{self._repo}/issues"
        payload = {"title": title, "body": body, "labels": labels}
        return self._post(url, json=payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> list | dict | None:
        backoff = 1.0
        for attempt in range(3):
            try:
                resp = httpx.get(url, headers=self._headers, params=params, timeout=15.0)
                if resp.status_code == 429:
                    if attempt == 2:
                        logger.error("GitHub GET rate-limited after 3 attempts: %s", url)
                        return None
                    logger.warning(
                        "GitHub GET rate-limited; retrying in %.0f s (attempt %d/3)",
                        backoff,
                        attempt + 1,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                logger.warning("GitHub GET HTTP error %s: %s", exc.response.status_code, url)
                return None
            except httpx.RequestError as exc:
                logger.warning("GitHub GET request error: %s", exc)
                return None
        return None

    def _post(self, url: str, json: dict) -> dict:
        backoff = 1.0
        for attempt in range(3):
            try:
                resp = httpx.post(url, headers=self._headers, json=json, timeout=15.0)
                if resp.status_code == 429:
                    if attempt == 2:
                        raise RuntimeError(f"GitHub POST rate-limited after 3 attempts: {url}")
                    logger.warning(
                        "GitHub POST rate-limited; retrying in %.0f s (attempt %d/3)",
                        backoff,
                        attempt + 1,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(
                    f"GitHub POST failed with HTTP {exc.response.status_code}"
                ) from exc
        raise RuntimeError("GitHub POST: unreachable after retry loop")


def get_github_client() -> GitHubClient | None:
    """Return the cached GitHubClient singleton, or None if env vars are not set.

    Thread-safe via double-checked locking (matches orchestrator.py pattern).
    Returns None (rather than raising) so callers can degrade gracefully.
    """
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        token = os.environ.get("GITHUB_TOKEN", "")
        repo = os.environ.get("GITHUB_REPO", _DEFAULT_REPO)
        if not token:
            logger.debug("GITHUB_TOKEN not set; GitHub issue creation disabled")
            return None
        _client = GitHubClient(token=token, repo=repo)
    return _client


def reset_github_client() -> None:
    """Reset the singleton — used in tests. Not called in production code."""
    global _client
    with _client_lock:
        _client = None
