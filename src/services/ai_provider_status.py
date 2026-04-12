# -*- coding: utf-8 -*-
"""AI provider availability — kill switches and balance probes.

Single source of truth for whether each AI provider is currently available.
All scheduled jobs and ad-hoc callers check this before making any AI calls.

Kill-switch env vars (set to 0/false/no/off to disable):
  GEMINI_ENABLED      — controls google-genai (Gemini)
  ANTHROPIC_ENABLED   — controls anthropic SDK
  OPENAI_ENABLED      — controls openai SDK

Balance probes make a minimal 1-token call and cache the result for 1 hour.
Non-quota errors (network, 5xx) return True and log at WARNING to avoid
false-blocking.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_PROBE_CACHE_TTL = 3600  # seconds
_probe_cache: dict[str, tuple[bool, float]] = {}


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ProviderCheckResult:
    all_available: bool
    disabled_providers: list[str] = field(default_factory=list)
    exhausted_providers: list[str] = field(default_factory=list)
    skip_reason: str | None = None


# ---------------------------------------------------------------------------
# Kill-switch check
# ---------------------------------------------------------------------------


def is_provider_enabled(name: str) -> bool:
    """Return False if the {NAME}_ENABLED env var is set to a false-like value.

    Reads GEMINI_ENABLED, ANTHROPIC_ENABLED, or OPENAI_ENABLED.
    Defaults to True (enabled) when the var is unset.  Same pattern as
    is_runners_enabled() in scheduled_tasks.py.
    """
    raw = os.environ.get(f"{name.upper()}_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


# ---------------------------------------------------------------------------
# Probe cache helpers
# ---------------------------------------------------------------------------


def _is_probe_cached(name: str) -> tuple[bool, bool]:
    """Return (cached_value, cache_hit). Cache hit only when within TTL."""
    if name in _probe_cache:
        value, ts = _probe_cache[name]
        if time.monotonic() - ts < _PROBE_CACHE_TTL:
            return value, True
    return False, False


def _set_probe_cache(name: str, value: bool) -> None:
    _probe_cache[name] = (value, time.monotonic())


def _clear_probe_cache() -> None:
    """Clear the probe cache — used in tests."""
    _probe_cache.clear()


# ---------------------------------------------------------------------------
# Per-provider balance probes
# ---------------------------------------------------------------------------


def poll_gemini_balance() -> bool:
    """Probe Gemini with a minimal 1-token call. Returns False on RESOURCE_EXHAUSTED.

    Non-quota errors (network, 5xx, etc.) return True and log at WARNING so
    that transient infrastructure problems don't false-block the provider.
    Result is cached for 1 hour.
    """
    cached, hit = _is_probe_cached("gemini")
    if hit:
        return cached

    result = True
    try:
        from google import genai
        from google.genai import types

        api_key = os.environ.get("GEMINI_OFFICE_HOLDER", "")
        if not api_key:
            # No key configured — cannot probe; assume available so job can
            # fail naturally with "client not configured".
            _set_probe_cache("gemini", True)
            return True

        client = genai.Client(api_key=api_key)
        client.models.generate_content(
            model="gemini-2.0-flash",
            contents="hi",
            config=types.GenerateContentConfig(max_output_tokens=1),
        )
        result = True
    except Exception as exc:
        exc_str = str(exc)
        exc_type = type(exc).__name__
        if "RESOURCE_EXHAUSTED" in exc_str or "ResourceExhausted" in exc_type:
            logger.warning("poll_gemini_balance: RESOURCE_EXHAUSTED — Gemini quota exhausted")
            result = False
        else:
            logger.warning(
                "poll_gemini_balance: non-quota error (assuming available): %s", exc
            )
            result = True

    _set_probe_cache("gemini", result)
    return result


def poll_anthropic_balance() -> bool:
    """Probe Anthropic with a minimal 1-token call. Returns False on RateLimitError.

    Non-quota errors return True and log at WARNING.
    Result is cached for 1 hour.
    """
    cached, hit = _is_probe_cached("anthropic")
    if hit:
        return cached

    result = True
    try:
        import anthropic

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            _set_probe_cache("anthropic", True)
            return True

        client = anthropic.Anthropic(api_key=api_key)
        client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
        result = True
    except anthropic.RateLimitError:
        logger.warning(
            "poll_anthropic_balance: RateLimitError — Anthropic quota exhausted"
        )
        result = False
    except Exception as exc:
        logger.warning(
            "poll_anthropic_balance: non-quota error (assuming available): %s", exc
        )
        result = True

    _set_probe_cache("anthropic", result)
    return result


def poll_openai_balance() -> bool:
    """Probe OpenAI with a minimal 1-token call. Returns False on RateLimitError.

    Non-quota errors return True and log at WARNING.
    Result is cached for 1 hour.
    """
    cached, hit = _is_probe_cached("openai")
    if hit:
        return cached

    result = True
    try:
        import openai

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            _set_probe_cache("openai", True)
            return True

        client = openai.OpenAI(api_key=api_key)
        client.chat.completions.create(
            model="gpt-4o-mini",
            max_completion_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
        result = True
    except openai.RateLimitError:
        logger.warning(
            "poll_openai_balance: RateLimitError — OpenAI quota exhausted"
        )
        result = False
    except Exception as exc:
        logger.warning(
            "poll_openai_balance: non-quota error (assuming available): %s", exc
        )
        result = True

    _set_probe_cache("openai", result)
    return result


# ---------------------------------------------------------------------------
# Main check function
# ---------------------------------------------------------------------------


def _run_probe(name: str) -> bool | None:
    """Run the balance probe for a named provider. Returns None for unknown providers."""
    if name == "gemini":
        return poll_gemini_balance()
    if name == "anthropic":
        return poll_anthropic_balance()
    if name == "openai":
        return poll_openai_balance()
    return None


def check_providers(required: list[str]) -> ProviderCheckResult:
    """Check availability of the required AI providers.

    Algorithm:
    1. Check kill switches for all required providers first.  If any is
       disabled, return immediately — no balance probes are wasted.
    2. If all kill switches pass, run balance probes for each provider.
       If any probe returns False, mark as exhausted.
    3. Return a ProviderCheckResult with full context for logging /
       result_json / summary emails.

    Args:
        required: List of provider names to check, e.g. ["gemini", "openai"].
                  Use "anthropic" (not "claude") as the canonical name for
                  the Anthropic / Claude provider.
    """
    disabled: list[str] = []
    for name in required:
        if not is_provider_enabled(name):
            disabled.append(name)

    if disabled:
        return ProviderCheckResult(
            all_available=False,
            disabled_providers=disabled,
            skip_reason=f"disabled by env var: {', '.join(disabled)}",
        )

    exhausted: list[str] = []
    for name in required:
        result = _run_probe(name)
        if result is None:
            logger.warning(
                "check_providers: no probe function for provider %r — skipping probe", name
            )
            continue
        if not result:
            exhausted.append(name)

    if exhausted:
        return ProviderCheckResult(
            all_available=False,
            exhausted_providers=exhausted,
            skip_reason=f"quota exhausted: {', '.join(exhausted)}",
        )

    return ProviderCheckResult(all_available=True)
