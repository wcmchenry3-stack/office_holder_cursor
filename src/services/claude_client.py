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


class ParserFixProposal(BaseModel):
    file_path: str
    diff: str  # unified diff to apply
    test_code: str  # new test function(s)
    explanation: str  # human-readable explanation of the fix


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

    def check_data_quality(
        self, prompt: str, context: dict, system_prompt: str | None = None
    ) -> DataQualityResult:
        """Assess data quality for a record or page. Returns structured result.

        Args:
            system_prompt: Override the default individual-record system prompt.
                Pass consensus_voter._SYSTEM_PROMPT when used for page-level checks
                so Claude uses the same framing as OpenAI and Gemini.
        """
        try:
            user_prompt = self._build_prompt(prompt, context)
            return self._call_claude(user_prompt, system_prompt=system_prompt or _SYSTEM_PROMPT)
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

    def _call_claude(
        self, user_prompt: str, system_prompt: str = _SYSTEM_PROMPT
    ) -> DataQualityResult:
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
                    system=system_prompt,
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

    # ------------------------------------------------------------------
    # Parser auto-fix
    # ------------------------------------------------------------------

    def propose_parser_fix(
        self,
        issue_title: str,
        issue_body: str,
        file_content: str,
    ) -> ParserFixProposal | None:
        """Generate a parser fix proposal from a GitHub issue.

        Returns a ParserFixProposal with the diff and test code,
        or None if the fix cannot be generated.
        """
        try:
            return self._call_claude_fix(issue_title, issue_body, file_content)
        except Exception:
            logger.exception("Claude auto-fix proposal failed")
            return None

    def _call_claude_fix(
        self,
        issue_title: str,
        issue_body: str,
        file_content: str,
    ) -> ParserFixProposal | None:
        """Call Claude to propose a parser fix. Exponential backoff on 429.

        max_tokens=4096 on every call.
        """
        import anthropic

        system = (
            "You are a senior Python developer fixing parser bugs in a web scraping application.\n"
            "You will be given a GitHub issue describing a parser bug and the current source file.\n"
            "Generate a minimal, targeted fix.\n\n"
            "RULES:\n"
            "1. Only modify code in src/scraper/ files.\n"
            "2. Keep changes minimal — fix the bug, nothing else.\n"
            "3. The diff should be < 50 lines (additions + deletions).\n"
            "4. Do NOT add new import statements for packages not in requirements.txt.\n"
            "5. Do NOT change function signatures that are called by other modules.\n"
            "6. Include at least one test function (def test_...) that verifies the fix.\n"
            "7. Use the traceback and HTML snippet from the issue to understand the failure.\n\n"
            "Return a JSON object with:\n"
            '  file_path (string): relative path to the file to modify (e.g. "src/scraper/table_parser.py")\n'
            "  diff (string): unified diff of the change (--- a/file, +++ b/file format)\n"
            "  test_code (string): complete test function(s) to add\n"
            "  explanation (string): brief explanation of what the fix does and why\n"
        )

        user_prompt = (
            f"## GitHub Issue\n**Title:** {issue_title}\n\n{issue_body}\n\n"
            f"## Current Source File\n```python\n{file_content}\n```\n\n"
            "Generate a fix as a JSON object."
        )

        backoff = 1.0
        for attempt in range(3):
            try:
                response = self._client.messages.create(
                    model=self._model,
                    max_tokens=4096,
                    system=system,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                return self._parse_fix_response(response)
            except anthropic.RateLimitError:
                if attempt == 2:
                    import sentry_sdk

                    sentry_sdk.add_breadcrumb(
                        message="Claude rate limit exhausted after 3 retries (auto-fix)",
                        level="error",
                    )
                    raise
                logger.warning(
                    "_call_claude_fix: rate limited (HTTP 429); retrying in %.0f s (attempt %d/3)",
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")

    def _parse_fix_response(self, response) -> ParserFixProposal | None:
        """Parse Claude response into a ParserFixProposal."""
        text = response.content[0].text if response.content else ""
        # Strip markdown code fences if present
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0]
        try:
            data = json.loads(text.strip())
        except json.JSONDecodeError:
            logger.warning("Claude auto-fix returned non-JSON: %s", text[:200])
            return None

        if not data.get("file_path") or not data.get("diff"):
            return None

        return ParserFixProposal(
            file_path=data["file_path"],
            diff=data["diff"],
            test_code=data.get("test_code", ""),
            explanation=data.get("explanation", ""),
        )
