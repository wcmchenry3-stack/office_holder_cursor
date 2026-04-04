# -*- coding: utf-8 -*-
"""Wikipedia article submission via the MediaWiki Action API.

--- Policy compliance ---

MediaWiki Action API:
  - Auth via WIKIPEDIA_BOT_USERNAME + WIKIPEDIA_BOT_PASSWORD env vars (never hardcoded).
  - User-Agent set per Wikimedia API:Etiquette policy on every request.
  - Rate limit: minimum 1 request/second, respects Retry-After header.
  - Uses action=edit with createonly=true for new articles only.
  See: https://www.mediawiki.org/wiki/API:Edit
  See: https://www.mediawiki.org/wiki/API:Etiquette

All Wikipedia HTTP requests use the shared User-Agent from src/scraper/logger.py.
"""

from __future__ import annotations

import logging
import os
import time

import requests

from src.scraper.logger import HTTP_USER_AGENT

logger = logging.getLogger(__name__)

_API_URL = "https://en.wikipedia.org/w/api.php"

_HEADERS = {
    "User-Agent": HTTP_USER_AGENT,
}

# Minimum delay between API calls (Wikimedia rate limit)
_MIN_REQUEST_INTERVAL = 1.0


class WikipediaSubmitError(Exception):
    """Raised when a Wikipedia submission fails."""

    pass


class WikipediaSubmitter:
    """Wrapper for the MediaWiki Action API to create new Wikipedia articles.

    Requires WIKIPEDIA_BOT_USERNAME and WIKIPEDIA_BOT_PASSWORD env vars.
    Returns None from get_submitter() if credentials are not configured.
    """

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        self._csrf_token: str | None = None
        self._last_request_at: float = 0.0

    def _throttle(self) -> None:
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_at
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at = time.time()

    def _handle_retry_after(self, response: requests.Response) -> bool:
        """If response has Retry-After header, sleep and return True."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                delay = int(retry_after)
            except ValueError:
                delay = 5
            logger.warning("Wikipedia API Retry-After: sleeping %d seconds", delay)
            time.sleep(delay)
            return True
        return False

    def login(self) -> None:
        """Authenticate with the MediaWiki Action API (two-step login).

        Step 1: Fetch a login token.
        Step 2: POST credentials with the token.
        """
        # Step 1: get login token
        self._throttle()
        resp = self._session.get(
            _API_URL,
            params={
                "action": "query",
                "meta": "tokens",
                "type": "login",
                "format": "json",
            },
        )
        resp.raise_for_status()
        login_token = resp.json()["query"]["tokens"]["logintoken"]

        # Step 2: login
        self._throttle()
        resp = self._session.post(
            _API_URL,
            data={
                "action": "login",
                "lgname": self._username,
                "lgpassword": self._password,
                "lgtoken": login_token,
                "format": "json",
            },
        )
        resp.raise_for_status()
        result = resp.json().get("login", {})
        if result.get("result") != "Success":
            raise WikipediaSubmitError(
                f"Wikipedia login failed: {result.get('result')} — {result.get('reason', '')}"
            )
        logger.info("Wikipedia login successful for user %s", self._username)

    def _get_csrf_token(self) -> str:
        """Fetch a CSRF token for editing (cached after first call)."""
        if self._csrf_token:
            return self._csrf_token
        self._throttle()
        resp = self._session.get(
            _API_URL,
            params={
                "action": "query",
                "meta": "tokens",
                "format": "json",
            },
        )
        resp.raise_for_status()
        self._csrf_token = resp.json()["query"]["tokens"]["csrftoken"]
        return self._csrf_token

    def submit_article(
        self,
        title: str,
        wikitext: str,
        summary: str = "New article created from research data",
    ) -> dict:
        """Create a new Wikipedia article via action=edit, createonly=true.

        Returns the API response dict on success.
        Raises WikipediaSubmitError on failure.
        """
        token = self._get_csrf_token()
        self._throttle()
        resp = self._session.post(
            _API_URL,
            data={
                "action": "edit",
                "title": title,
                "text": wikitext,
                "summary": summary,
                "createonly": "true",
                "token": token,
                "format": "json",
            },
        )
        if self._handle_retry_after(resp):
            # Retry once after Retry-After
            resp = self._session.post(
                _API_URL,
                data={
                    "action": "edit",
                    "title": title,
                    "text": wikitext,
                    "summary": summary,
                    "createonly": "true",
                    "token": token,
                    "format": "json",
                },
            )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise WikipediaSubmitError(
                f"Wikipedia edit failed: {data['error'].get('code')} — "
                f"{data['error'].get('info', '')}"
            )
        edit_result = data.get("edit", {})
        if edit_result.get("result") != "Success":
            raise WikipediaSubmitError(f"Wikipedia edit did not succeed: {edit_result}")
        logger.info("Wikipedia article created: %s", title)
        return edit_result


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_submitter: WikipediaSubmitter | None = None
_submitter_checked = False


def get_submitter() -> WikipediaSubmitter | None:
    """Return a logged-in WikipediaSubmitter, or None if credentials are not set.

    Caches the instance after first successful login.
    """
    global _submitter, _submitter_checked
    if _submitter_checked:
        return _submitter
    username = os.environ.get("WIKIPEDIA_BOT_USERNAME", "")
    password = os.environ.get("WIKIPEDIA_BOT_PASSWORD", "")
    if not username or not password:
        logger.info("WIKIPEDIA_BOT_USERNAME/PASSWORD not set — Wikipedia submit disabled")
        _submitter_checked = True
        return None
    try:
        sub = WikipediaSubmitter(username, password)
        sub.login()
        _submitter = sub
    except Exception:
        logger.exception("Wikipedia login failed — submit disabled")
        _submitter = None
    _submitter_checked = True
    return _submitter


def reset_submitter() -> None:
    """Reset the singleton — used in tests."""
    global _submitter, _submitter_checked
    _submitter = None
    _submitter_checked = False
