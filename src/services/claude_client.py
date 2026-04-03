# -*- coding: utf-8 -*-
"""Anthropic Claude API client for data quality assessment.

--- Policy compliance ---

Anthropic Claude API (anthropic SDK):
  - rate_limit (HTTP 429) handling: exponential backoff
    (3 retries, 1 s → 2 s → 4 s).
  - max_tokens set on every API call.
  - ANTHROPIC_API_KEY never hardcoded; always read via os.environ at runtime.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic model for structured output
# ---------------------------------------------------------------------------


class DataQualityResult(BaseModel):
    is_valid: bool
    concerns: list[str]
    confidence: str  # "high", "medium", "low"


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_client_lock = threading.Lock()
_client: ClaudeClient | None = None


def get_claude_client() -> ClaudeClient | None:
    """Return the cached ClaudeClient singleton, or None if not configured.

    Thread-safe via double-checked locking (matches gemini_vitals_researcher.py pattern).
    Returns None (not raises) when ANTHROPIC_API_KEY is not set — feature
    is silently disabled.
    """
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.info("ANTHROPIC_API_KEY not set — Claude data quality disabled")
            return None
        _client = ClaudeClient(api_key=api_key)
    return _client


def reset_claude_client() -> None:
    """Reset the singleton — used in tests to inject a new key or clear state."""
    global _client
    with _client_lock:
        _client = None


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a data quality analyst for a political office holders database.
Given a record about a political office holder, assess whether the data is valid
and consistent.

RULES:
1. Check for obvious errors: impossible dates, inconsistent data, placeholder text.
2. Flag missing critical fields (name, office) as concerns.
3. Check date consistency: birth before death, term dates within plausible ranges.
4. Flag suspiciously duplicate or template-like data.
5. Report confidence as: high (clear assessment), medium (some ambiguity), \
low (insufficient data to judge).

Return a JSON object with these fields:
  is_valid (boolean: true if the record appears correct),
  concerns (array of strings: list of specific issues found, empty if none),
  confidence (string: "low", "medium", or "high")
"""


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class ClaudeClient:
    """Anthropic Claude API client for data quality checks.

    All anthropic SDK usage is contained in this class — no direct SDK imports
    should exist elsewhere in the codebase.
    """

    def __init__(self, api_key: str):
        import anthropic

        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = "claude-sonnet-4-20250514"

    def check_data_quality(self, prompt: str, context: dict) -> DataQualityResult:
        """Assess data quality for a record. Returns structured result."""
        try:
            user_prompt = self._build_prompt(prompt, context)
            return self._call_claude(user_prompt)
        except Exception:
            logger.exception("Claude data quality check failed")
            return DataQualityResult(is_valid=True, concerns=[], confidence="low")

    def _build_prompt(self, prompt: str, context: dict) -> str:
        lines = [prompt]
        if context:
            lines.append("\nRecord context:")
            for key, value in context.items():
                lines.append(f"  {key}: {value}")
        return "\n".join(lines)

    def _call_claude(self, user_prompt: str) -> DataQualityResult:
        """Call Claude with exponential backoff on rate limit (HTTP 429).

        Retries up to 3 times, doubling the backoff delay each attempt (1 s → 2 s → 4 s).
        """
        import anthropic

        backoff = 1.0
        for attempt in range(3):
            try:
                response = self._client.messages.create(
                    model=self._model,
                    max_tokens=1024,
                    system=_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                return self._parse_response(response)
            except anthropic.RateLimitError:
                if attempt == 2:
                    import sentry_sdk

                    sentry_sdk.add_breadcrumb(
                        message="Claude rate limit exhausted after 3 retries",
                        level="error",
                    )
                    raise
                logger.warning(
                    "_call_claude: rate limited (HTTP 429); retrying in %.0f s (attempt %d/3)",
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")

    def _parse_response(self, response) -> DataQualityResult:
        """Parse Claude JSON response into DataQualityResult."""
        text = response.content[0].text if response.content else ""
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Claude returned non-JSON response: %s", text[:200])
            return DataQualityResult(is_valid=True, concerns=[], confidence="low")

        return DataQualityResult(
            is_valid=data.get("is_valid", True),
            concerns=data.get("concerns", []),
            confidence=data.get("confidence", "low"),
        )
