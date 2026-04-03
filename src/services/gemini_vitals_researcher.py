# -*- coding: utf-8 -*-
"""Gemini API client for deep vitals research on individuals with missing data.

--- Policy compliance ---

Google Gemini API (google-genai SDK):
  - rate_limit / RESOURCE_EXHAUSTED (HTTP 429) handling: exponential backoff
    (3 retries, 1 s → 2 s → 4 s).
  - max_output_tokens set on every generate_content call.
  - GEMINI_OFFICE_HOLDER never hardcoded; always read via os.environ at runtime.
  - Unpaid tier: prompts/responses may be used by Google per ToS.
  - 55-day data retention by Google for abuse monitoring.
  See: https://ai.google.dev/gemini-api/terms
  See: https://ai.google.dev/gemini-api/docs/rate-limits
  See: https://ai.google.dev/gemini-api/docs/usage-policies
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class GeminiModelDeprecatedError(Exception):
    """Raised when the Gemini model is not found, retired, or deprecated."""

    pass


# ---------------------------------------------------------------------------
# Pydantic models for structured output
# ---------------------------------------------------------------------------


class SourceRecord(BaseModel):
    url: str
    source_type: str = "other"  # government, academic, genealogical, news, other
    notes: str = ""


class VitalsResearchResult(BaseModel):
    birth_date: str | None = None
    death_date: str | None = None
    birth_place: str | None = None
    death_place: str | None = None
    sources: list[SourceRecord] = []
    confidence: str = "low"  # low, medium, high
    biographical_notes: str = ""


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_researcher_lock = threading.Lock()
_researcher: GeminiVitalsResearcher | None = None


def get_gemini_researcher() -> GeminiVitalsResearcher | None:
    """Return the cached GeminiVitalsResearcher singleton, or None if not configured.

    Thread-safe via double-checked locking (matches orchestrator.py pattern).
    Returns None (not raises) when GEMINI_OFFICE_HOLDER is not set — feature
    is silently disabled.
    """
    global _researcher
    if _researcher is not None:
        return _researcher
    with _researcher_lock:
        if _researcher is not None:
            return _researcher
        api_key = os.environ.get("GEMINI_OFFICE_HOLDER", "")
        if not api_key:
            logger.info("GEMINI_OFFICE_HOLDER not set — Gemini research disabled")
            return None
        _researcher = GeminiVitalsResearcher(api_key=api_key)
    return _researcher


def reset_gemini_researcher() -> None:
    """Reset the singleton — used in tests to inject a new key or clear state."""
    global _researcher
    with _researcher_lock:
        _researcher = None


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a historical researcher specializing in political office holders.
Given an individual who held political office, research their vital statistics and
biographical details from authoritative external sources.

RULES:
1. Only report facts you can attribute to a specific source URL.
2. Classify each source as: government, academic, genealogical, news, or other.
3. Prefer government and academic sources over news.
4. If dates are approximate, note "circa" or "approximately" in the notes field.
5. Use the office context and location to disambiguate common names.
6. Report confidence as: high (multiple corroborating sources), medium (single \
authoritative source), low (indirect/inferred).
7. Include enough biographical detail for a Wikipedia article: early life, education, \
career before office, office tenure, post-office career, death/legacy.
8. The more detail you provide, the better. Even if you cannot find dates, provide \
whatever biographical context you can find.

Return a JSON object with these fields:
  birth_date (string or null, YYYY-MM-DD if known),
  death_date (string or null, YYYY-MM-DD if known),
  birth_place (string or null),
  death_place (string or null),
  sources (array of objects with: url, source_type, notes),
  confidence (string: "low", "medium", or "high"),
  biographical_notes (string: detailed narrative for article writing)
"""


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class GeminiVitalsResearcher:
    """Gemini API client for researching individual vitals.

    All Gemini SDK usage is contained in this class — no direct SDK imports
    should exist elsewhere in the codebase.
    """

    def __init__(self, api_key: str):
        from google import genai

        self._client = genai.Client(api_key=api_key)
        self._model = "gemini-3.1-pro"

    def research_individual(
        self,
        individual_id: int,
        full_name: str,
        office_name: str = "",
        term_dates: str = "",
        party: str = "",
        district: str = "",
        location: str = "",
        level: str = "",
        branch: str = "",
        wiki_url: str = "",
        known_birth_date: str = "",
        known_death_date: str = "",
        known_birth_place: str = "",
        known_death_place: str = "",
    ) -> VitalsResearchResult:
        """Research one individual. Returns empty result on failure.

        Raises GeminiModelDeprecatedError if the model is retired/not-found
        so callers can abort the batch and send an alert.
        """
        try:
            prompt = self._build_prompt(
                full_name=full_name,
                office_name=office_name,
                term_dates=term_dates,
                party=party,
                district=district,
                location=location,
                level=level,
                branch=branch,
                wiki_url=wiki_url,
                known_birth_date=known_birth_date,
                known_death_date=known_death_date,
                known_birth_place=known_birth_place,
                known_death_place=known_death_place,
            )
            return self._call_gemini(prompt)
        except GeminiModelDeprecatedError:
            raise
        except Exception:
            logger.exception(
                "Gemini research failed for individual %d (%s)", individual_id, full_name
            )
            return VitalsResearchResult()

    def _build_prompt(
        self,
        full_name: str,
        office_name: str,
        term_dates: str,
        party: str,
        district: str,
        location: str,
        level: str,
        branch: str,
        wiki_url: str,
        known_birth_date: str,
        known_death_date: str,
        known_birth_place: str,
        known_death_place: str,
    ) -> str:
        lines = [
            "Research this individual:",
            f"  Name: {full_name}",
        ]
        if office_name:
            lines.append(f"  Office held: {office_name}")
        if term_dates:
            lines.append(f"  Term: {term_dates}")
        if party:
            lines.append(f"  Party: {party}")
        if district:
            lines.append(f"  District: {district}")
        if location:
            lines.append(f"  Location: {location}")
        if level or branch:
            parts = []
            if level:
                parts.append(f"Government level: {level}")
            if branch:
                parts.append(f"Branch: {branch}")
            lines.append(f"  {' | '.join(parts)}")
        if wiki_url:
            lines.append(f"  Wikipedia URL: {wiki_url}")
        lines.append(f"  Known birth date: {known_birth_date or 'Unknown'}")
        lines.append(f"  Known death date: {known_death_date or 'Unknown'}")
        lines.append(f"  Known birth place: {known_birth_place or 'Unknown'}")
        lines.append(f"  Known death place: {known_death_place or 'Unknown'}")
        return "\n".join(lines)

    def _call_gemini(self, user_prompt: str) -> VitalsResearchResult:
        """Call Gemini with exponential backoff on RESOURCE_EXHAUSTED (HTTP 429).

        Retries up to 3 times, doubling the backoff delay each attempt (1 s → 2 s → 4 s).
        """
        from google.genai import types, errors

        backoff = 1.0
        for attempt in range(3):
            try:
                response = self._client.models.generate_content(
                    model=self._model,
                    contents=[
                        types.Content(
                            role="user",
                            parts=[types.Part(text=user_prompt)],
                        ),
                    ],
                    config=types.GenerateContentConfig(
                        system_instruction=_SYSTEM_PROMPT,
                        max_output_tokens=4096,
                        response_mime_type="application/json",
                        tools=[types.Tool(google_search=types.GoogleSearch())],
                        thinking_config=types.ThinkingConfig(thinking_budget=8192),
                    ),
                )
                return self._parse_response(response)
            except errors.ClientError as exc:
                exc_str = str(exc).lower()
                # Detect model deprecation / not-found errors
                if any(
                    kw in exc_str
                    for kw in ("not found", "not_found", "retired", "deprecated", "decommissioned")
                ):
                    raise GeminiModelDeprecatedError(
                        f"Gemini model '{self._model}' is no longer available: {exc}"
                    ) from exc
                if getattr(exc, "code", 0) == 429 or "RESOURCE_EXHAUSTED" in str(exc):
                    if attempt == 2:
                        import sentry_sdk

                        sentry_sdk.add_breadcrumb(
                            message="Gemini rate limit exhausted after 3 retries", level="error"
                        )
                        raise
                    logger.warning(
                        "_call_gemini: RESOURCE_EXHAUSTED (HTTP 429); retrying in %.0f s (attempt %d/3)",
                        backoff,
                        attempt + 1,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    raise
        raise RuntimeError("unreachable")

    def _parse_response(self, response) -> VitalsResearchResult:
        """Parse Gemini JSON response into VitalsResearchResult."""
        text = response.text or ""
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Gemini returned non-JSON response: %s", text[:200])
            return VitalsResearchResult()

        sources = []
        for s in data.get("sources") or []:
            sources.append(
                SourceRecord(
                    url=s.get("url", ""),
                    source_type=s.get("source_type", "other"),
                    notes=s.get("notes", ""),
                )
            )

        return VitalsResearchResult(
            birth_date=data.get("birth_date"),
            death_date=data.get("death_date"),
            birth_place=data.get("birth_place"),
            death_place=data.get("death_place"),
            sources=sources,
            confidence=data.get("confidence", "low"),
            biographical_notes=data.get("biographical_notes", ""),
        )
