# -*- coding: utf-8 -*-
"""
External API orchestrator: SSRF validation + AIOfficeBuilder singleton.

Provides:
  validate_and_normalize_wiki_url(url) -> str
      Single SSRF enforcement point for all outbound Wikipedia requests triggered
      by user-supplied URLs. Raises ValueError for non-wikipedia.org domains.

  get_ai_builder() -> AIOfficeBuilder
      Lazy singleton — creates one AIOfficeBuilder per process and reuses it.
      Raises RuntimeError if OPENAI_API_KEY is not set.

  reset_ai_builder() -> None
      Resets the singleton for tests. Not called in production code.

Design: module-level functions with a lazy singleton protected by a threading.Lock,
matching the wiki_session() pattern in src/scraper/wiki_fetch.py.
"""

from __future__ import annotations

import os
import threading

from src.scraper.wiki_fetch import normalize_wiki_url
from src.services.ai_office_builder import AIOfficeBuilder

_builder_lock = threading.Lock()
_builder: AIOfficeBuilder | None = None


def validate_and_normalize_wiki_url(url: str) -> str:
    """Validate that url is a Wikipedia URL and return its normalized form.

    Raises ValueError if url is not a valid wikipedia.org URL.
    Reuses normalize_wiki_url() from wiki_fetch.py which already validates the domain.
    """
    normalized = normalize_wiki_url(url)
    if normalized is None:
        raise ValueError(
            f"URL must be a Wikipedia URL (https://en.wikipedia.org/wiki/...), got: {url!r}"
        )
    return normalized


def get_ai_builder() -> AIOfficeBuilder:
    """Return the cached AIOfficeBuilder singleton, creating it on first call.

    Thread-safe via double-checked locking (matches wiki_session() pattern).
    Raises RuntimeError if OPENAI_API_KEY is not set in the environment.
    """
    global _builder
    if _builder is not None:
        return _builder
    with _builder_lock:
        if _builder is not None:  # double-checked locking
            return _builder
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        _builder = AIOfficeBuilder(api_key=api_key)
    return _builder


def reset_ai_builder() -> None:
    """Reset the singleton — used in tests to inject a new key or clear state.

    Not called in production code.
    """
    global _builder
    with _builder_lock:
        _builder = None
