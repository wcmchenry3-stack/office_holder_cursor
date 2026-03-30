# -*- coding: utf-8 -*-
"""Unit tests for src/services/orchestrator.py.

No network, no DB, no FastAPI dependency. All external calls are patched.

Run: pytest src/services/test_orchestrator.py -v
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.services.orchestrator import (
    get_ai_builder,
    reset_ai_builder,
    validate_and_normalize_wiki_url,
)

# ---------------------------------------------------------------------------
# validate_and_normalize_wiki_url
# ---------------------------------------------------------------------------


def test_valid_english_wikipedia_url_passes():
    url = validate_and_normalize_wiki_url("https://en.wikipedia.org/wiki/Barack_Obama")
    assert "wikipedia.org" in url
    assert "Barack_Obama" in url


def test_valid_https_url_passes():
    url = validate_and_normalize_wiki_url("https://en.wikipedia.org/wiki/Test_Page")
    assert url.startswith("https://")


def test_http_wikipedia_url_accepted():
    url = validate_and_normalize_wiki_url("http://en.wikipedia.org/wiki/Test")
    assert "wikipedia.org" in url


def test_non_english_wikipedia_url_accepted():
    url = validate_and_normalize_wiki_url("https://de.wikipedia.org/wiki/Test")
    assert "wikipedia.org" in url


def test_url_normalization_strips_trailing_dot():
    url = validate_and_normalize_wiki_url("https://en.wikipedia.org./wiki/Test")
    host = url.split("/")[2]
    assert not host.endswith(".")


def test_non_wikipedia_domain_raises():
    with pytest.raises(ValueError, match="Wikipedia"):
        validate_and_normalize_wiki_url("https://evil.example.com/steal")


def test_localhost_url_raises():
    with pytest.raises(ValueError):
        validate_and_normalize_wiki_url("http://localhost:5432/")


def test_aws_metadata_url_raises():
    with pytest.raises(ValueError):
        validate_and_normalize_wiki_url("https://169.254.169.254/latest/meta-data/")


def test_empty_url_raises():
    with pytest.raises(ValueError):
        validate_and_normalize_wiki_url("")


def test_whitespace_only_url_raises():
    with pytest.raises(ValueError):
        validate_and_normalize_wiki_url("   ")


def test_non_wikipedia_https_url_raises():
    with pytest.raises(ValueError):
        validate_and_normalize_wiki_url("https://attacker.co/wiki/Fake")


# ---------------------------------------------------------------------------
# get_ai_builder / reset_ai_builder
# ---------------------------------------------------------------------------


def test_get_ai_builder_no_key_raises():
    reset_ai_builder()
    with patch.dict(os.environ, {}, clear=True):
        # Ensure the key is absent even if set in the outer env
        env = dict(os.environ)
        env.pop("OPENAI_API_KEY", None)
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
                get_ai_builder()
    reset_ai_builder()


def test_get_ai_builder_with_key_returns_builder():
    reset_ai_builder()
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-unit"}):
        builder = get_ai_builder()
    assert builder is not None
    reset_ai_builder()


def test_get_ai_builder_singleton():
    reset_ai_builder()
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-singleton"}):
        b1 = get_ai_builder()
        b2 = get_ai_builder()
    assert b1 is b2
    reset_ai_builder()


def test_reset_ai_builder_clears_singleton():
    reset_ai_builder()
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-reset"}):
        b1 = get_ai_builder()
        reset_ai_builder()
        b2 = get_ai_builder()
    assert b1 is not b2
    reset_ai_builder()


def test_reset_ai_builder_is_idempotent():
    reset_ai_builder()
    reset_ai_builder()  # second call should not raise
    with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test-key-idempotent"}):
        builder = get_ai_builder()
    assert builder is not None
    reset_ai_builder()
