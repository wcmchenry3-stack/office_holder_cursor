# -*- coding: utf-8 -*-
"""Parallel 3-AI consensus voting service.

Calls OpenAI, Gemini, and Claude simultaneously via ThreadPoolExecutor and
aggregates their verdicts.  Architecturally distinct from DataQualityChecker
(which is sequential and short-circuits on the first flag) — this service
always calls all available providers and returns a single consensus verdict.

--- Policy compliance ---

OpenAI API (openai SDK):
  - rate_limit (HTTP 429) handling: exponential backoff (3 retries, 1 s → 2 s → 4 s).
  - max_completion_tokens set on every API call.
  - OPENAI_API_KEY never hardcoded; always read via os.environ at runtime.

Google Gemini API (google-genai SDK):
  - RESOURCE_EXHAUSTED (HTTP 429) handling: exponential backoff (3 retries).
  - max_output_tokens set on every generate_content call.
  - GEMINI_OFFICE_HOLDER never hardcoded; always read via os.environ at runtime.

Anthropic Claude API (anthropic SDK):
  - RateLimitError (HTTP 429) handling: exponential backoff (3 retries).
  - max_tokens set on every API call.
  - ANTHROPIC_API_KEY never hardcoded; always read via os.environ at runtime.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class Verdict(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    DISAGREEMENT = "disagreement"
    INSUFFICIENT_QUORUM = "insufficient_quorum"


@dataclass
class AIVote:
    provider: str  # "openai", "gemini", "claude"
    is_valid: bool | None  # None if provider unavailable or errored
    concerns: list[str] = field(default_factory=list)
    confidence: str = "low"
    error: str | None = None


@dataclass
class ConsensusVerdict:
    verdict: Verdict
    votes: list[AIVote] = field(default_factory=list)

    @property
    def available_votes(self) -> list[AIVote]:
        return [v for v in self.votes if v.is_valid is not None]

    @property
    def all_concerns(self) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for v in self.votes:
            for c in v.concerns:
                if c not in seen:
                    seen.add(c)
                    out.append(c)
        return out


# ---------------------------------------------------------------------------
# System prompt shared across all providers
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a data quality analyst for a political office holders database. "
    "Assess the provided record or page data and return JSON with these fields: "
    '{"is_valid": bool, "concerns": [str], "confidence": "high"|"medium"|"low"}. '
    "is_valid is true if the data appears correct and accurate."
)

# ---------------------------------------------------------------------------
# Per-provider vote functions (all return AIVote)
# ---------------------------------------------------------------------------


def _vote_openai(prompt: str, context: dict) -> AIVote:
    """Call OpenAI gpt-4o-mini and return a vote."""
    try:
        from src.services.orchestrator import get_ai_builder
        import json
        import openai
        import time

        builder = get_ai_builder()
        if builder is None:
            return AIVote(provider="openai", is_valid=None, error="client not configured")

        backoff = 1.0
        for attempt in range(3):
            try:
                response = builder._client.chat.completions.create(
                    model="gpt-4o-mini",
                    max_completion_tokens=512,
                    messages=[
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                )
                text = response.choices[0].message.content or ""
                data = json.loads(text)
                return AIVote(
                    provider="openai",
                    is_valid=bool(data.get("is_valid", True)),
                    concerns=data.get("concerns", []),
                    confidence=data.get("confidence", "low"),
                )
            except openai.RateLimitError:
                if attempt == 2:
                    raise
                logger.warning(
                    "_vote_openai: rate limited; retrying in %.0f s (attempt %d/3)",
                    backoff,
                    attempt + 1,
                )
                time.sleep(backoff)
                backoff *= 2
    except Exception as exc:
        logger.exception("ConsensusVoter: OpenAI vote failed")
        return AIVote(provider="openai", is_valid=None, error=str(exc))
    return AIVote(provider="openai", is_valid=None, error="unreachable")


def _vote_gemini(prompt: str, context: dict) -> AIVote:
    """Call Gemini and return a vote."""
    try:
        from src.services.gemini_vitals_researcher import get_gemini_researcher

        researcher = get_gemini_researcher()
        if researcher is None:
            return AIVote(provider="gemini", is_valid=None, error="client not configured")

        result = researcher.check_data_quality(prompt, system_prompt=_SYSTEM_PROMPT)
        if result is None:
            return AIVote(provider="gemini", is_valid=None, error="empty response")

        return AIVote(
            provider="gemini",
            is_valid=bool(result.get("is_valid", True)),
            concerns=result.get("concerns", []),
            confidence=result.get("confidence", "low"),
        )
    except Exception as exc:
        logger.exception("ConsensusVoter: Gemini vote failed")
        return AIVote(provider="gemini", is_valid=None, error=str(exc))


def _vote_claude(prompt: str, context: dict) -> AIVote:
    """Call Claude and return a vote."""
    try:
        from src.services.claude_client import get_claude_client

        client = get_claude_client()
        if client is None:
            return AIVote(provider="claude", is_valid=None, error="client not configured")

        result = client.check_data_quality(prompt, context, system_prompt=_SYSTEM_PROMPT)
        return AIVote(
            provider="claude",
            is_valid=result.is_valid,
            concerns=result.concerns,
            confidence=result.confidence,
        )
    except Exception as exc:
        logger.exception("ConsensusVoter: Claude vote failed")
        return AIVote(provider="claude", is_valid=None, error=str(exc))


# ---------------------------------------------------------------------------
# Verdict aggregation
# ---------------------------------------------------------------------------


def _aggregate(votes: list[AIVote]) -> Verdict:
    """Derive a consensus verdict from a list of votes.

    Rules:
    - Requires ≥ 2 available providers for a verdict.
    - < 2 available → INSUFFICIENT_QUORUM
    - All available agree valid   → VALID
    - All available agree invalid → INVALID
    - Mixed                       → DISAGREEMENT
    """
    available = [v for v in votes if v.is_valid is not None]
    if len(available) < 2:
        return Verdict.INSUFFICIENT_QUORUM

    valid_count = sum(1 for v in available if v.is_valid)
    invalid_count = len(available) - valid_count

    if invalid_count == 0:
        return Verdict.VALID
    if valid_count == 0:
        return Verdict.INVALID
    return Verdict.DISAGREEMENT


# ---------------------------------------------------------------------------
# Main service class
# ---------------------------------------------------------------------------


class ConsensusVoter:
    """Parallel 3-AI consensus voting service.

    Calls all three AI providers simultaneously and aggregates their verdicts.
    Any provider that is unavailable or errors is excluded from quorum counting
    (requires ≥ 2 responding providers for a non-quorum result).

    Usage::

        voter = ConsensusVoter()
        verdict = voter.vote(prompt="Is this record valid?", context={...})
        if verdict.verdict == Verdict.INVALID:
            ...
    """

    def vote(
        self,
        prompt: str,
        context: dict,
        timeout_s: float = 30.0,
    ) -> ConsensusVerdict:
        """Call all 3 AI providers in parallel and return a consensus verdict.

        Args:
            prompt: The question / data to evaluate.
            context: Additional structured context passed to each provider.
            timeout_s: Per-call timeout in seconds (default 30).

        Returns:
            ConsensusVerdict with a Verdict enum and the individual AIVotes.
        """
        providers = [
            ("openai", _vote_openai),
            ("gemini", _vote_gemini),
            ("claude", _vote_claude),
        ]

        votes: list[AIVote] = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_provider = {
                executor.submit(fn, prompt, context): name for name, fn in providers
            }
            pending_futures = set(future_to_provider)

            def _collect(future: object) -> None:
                pending_futures.discard(future)
                provider_name = future_to_provider[future]
                try:
                    vote = future.result(timeout=0)
                except FuturesTimeoutError:
                    logger.warning(
                        "ConsensusVoter: %s timed out after %.0f s", provider_name, timeout_s
                    )
                    vote = AIVote(
                        provider=provider_name,
                        is_valid=None,
                        error=f"timed out after {timeout_s:.0f}s",
                    )
                except Exception as exc:
                    logger.exception("ConsensusVoter: %s raised unexpected error", provider_name)
                    vote = AIVote(provider=provider_name, is_valid=None, error=str(exc))
                votes.append(vote)

            try:
                for future in as_completed(future_to_provider, timeout=timeout_s + 5):
                    _collect(future)
            except FuturesTimeoutError:
                pass

            # Drain any futures that finished but weren't yielded before the timeout fired
            # (sub-millisecond overshoot can leave done futures in pending)
            for future in list(pending_futures):
                if future.done():
                    _collect(future)
                else:
                    provider_name = future_to_provider[future]
                    logger.warning(
                        "ConsensusVoter: %s timed out after %.0f s", provider_name, timeout_s
                    )
                    votes.append(
                        AIVote(
                            provider=provider_name,
                            is_valid=None,
                            error=f"timed out after {timeout_s:.0f}s",
                        )
                    )

        # Sort for deterministic ordering in tests / logs
        votes.sort(key=lambda v: v.provider)

        verdict = _aggregate(votes)
        return ConsensusVerdict(verdict=verdict, votes=votes)
