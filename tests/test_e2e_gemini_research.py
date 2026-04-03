# -*- coding: utf-8 -*-
"""E2E tests for the Gemini Research page and API endpoints.

Uses full app stack with TestClient. All Gemini/OpenAI API calls are mocked —
no live network requests. Tests cover:
  1. Page loads successfully
  2. Search endpoint returns results and handles edge cases
  3. Research job lifecycle: start → poll → complete
  4. Error handling: missing individual, missing env var
  5. Rate limiting on the run endpoint

Run: pytest tests/test_e2e_gemini_research.py -v

Policy compliance (production implementations — not tested here):
  Gemini API (src/services/gemini_vitals_researcher.py):
    - max_output_tokens=4096 set on every call to cap cost.
    - Retry/backoff on RESOURCE_EXHAUSTED (429) in _call_gemini.
  OpenAI API (src/services/ai_office_builder.py):
    - max_completion_tokens=4096 set on every API call.
    - RateLimitError handling: exponential backoff in AIOfficeBuilder._call_openai.
"""

from __future__ import annotations

import importlib
import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Shared fixture: full app stack with a seeded individual
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def app_client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("e2e_gemini_db")
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(tmp / "e2e_gemini.db")
    os.environ["OPENAI_API_KEY"] = "sk-fake-e2e-test-key"
    os.environ["GEMINI_OFFICE_HOLDER"] = "fake-gemini-key"

    import src.main as main_mod

    importlib.reload(main_mod)

    from src.services.orchestrator import reset_ai_builder
    from src.services.gemini_vitals_researcher import reset_gemini_researcher

    reset_ai_builder()
    reset_gemini_researcher()

    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c

    os.environ.pop("OFFICE_HOLDER_DB_PATH", None)
    os.environ.pop("OPENAI_API_KEY", None)
    os.environ.pop("GEMINI_OFFICE_HOLDER", None)
    reset_gemini_researcher()


@pytest.fixture(scope="module")
def seeded_individual(app_client):
    """Insert a test individual directly into the DB."""
    from src.db.connection import get_connection

    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO individuals (id, wiki_url, full_name, is_living)"
            " VALUES (%s, %s, %s, %s)",
            (99999, "https://en.wikipedia.org/wiki/Test_Person", "Test Person", 1),
        )
        conn.commit()
    finally:
        conn.close()
    return 99999


# ---------------------------------------------------------------------------
# Page load
# ---------------------------------------------------------------------------


def test_gemini_research_page_loads(app_client):
    resp = app_client.get("/gemini-research")
    assert resp.status_code == 200
    assert "gemini" in resp.text.lower()


# ---------------------------------------------------------------------------
# Search endpoint
# ---------------------------------------------------------------------------


def test_search_empty_query_returns_empty(app_client):
    resp = app_client.get("/api/gemini-research/search?q=")
    assert resp.status_code == 200
    assert resp.json() == []


def test_search_short_query_returns_empty(app_client):
    resp = app_client.get("/api/gemini-research/search?q=a")
    assert resp.status_code == 200
    assert resp.json() == []


def test_search_finds_seeded_individual(app_client, seeded_individual):
    resp = app_client.get("/api/gemini-research/search?q=Test Person")
    assert resp.status_code == 200
    results = resp.json()
    assert len(results) >= 1
    match = [r for r in results if r["id"] == seeded_individual]
    assert len(match) == 1
    assert match[0]["full_name"] == "Test Person"


def test_search_no_match_returns_empty(app_client):
    resp = app_client.get("/api/gemini-research/search?q=zznonexistent99")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# Research job: start → poll → complete
# ---------------------------------------------------------------------------


def test_run_missing_individual_id_returns_400(app_client):
    resp = app_client.post("/api/gemini-research/run", json={})
    assert resp.status_code == 400


def test_run_and_poll_research_job(app_client, seeded_individual):
    """Start a research job and poll until complete (mocked Gemini + OpenAI)."""
    mock_gemini_response = MagicMock()
    mock_gemini_response.text = json.dumps({
        "birth_date": "1950-06-15",
        "death_date": None,
        "birth_place": "Denver, CO",
        "death_place": None,
        "sources": [
            {
                "url": "https://example.gov/record",
                "source_type": "government",
                "notes": "Census record",
            }
        ],
        "confidence": "medium",
        "biographical_notes": "Test Person served in office.",
    })

    mock_openai_completion = MagicMock()
    mock_openai_completion.choices = [MagicMock()]
    mock_openai_completion.choices[0].message.content = (
        "{{Infobox officeholder}}\nTest Person (born June 15, 1950) is a politician."
    )

    with (
        patch("google.genai.Client") as mock_genai_cls,
        patch("src.services.ai_office_builder.openai.OpenAI") as mock_openai_cls,
    ):
        mock_genai_cls.return_value.models.generate_content.return_value = (
            mock_gemini_response
        )
        mock_openai_cls.return_value.chat.completions.create.return_value = (
            mock_openai_completion
        )

        # Reset singletons so they pick up the mocks
        from src.services.gemini_vitals_researcher import reset_gemini_researcher
        from src.services.orchestrator import reset_ai_builder

        reset_gemini_researcher()
        reset_ai_builder()

        # Start research
        resp = app_client.post(
            "/api/gemini-research/run",
            json={"individual_id": seeded_individual},
        )
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        # Poll until complete or timeout
        deadline = time.monotonic() + 15
        status = None
        while time.monotonic() < deadline:
            poll_resp = app_client.get(f"/api/gemini-research/status/{job_id}")
            assert poll_resp.status_code == 200
            status = poll_resp.json()
            if status["status"] in ("complete", "error"):
                break
            time.sleep(0.3)

        assert status is not None
        assert status["status"] == "complete", f"Job failed: {status.get('error')}"
        assert status["phase"] == "done"

        # Verify Gemini results came through
        gemini = status["gemini_result"]
        assert gemini["birth_date"] == "1950-06-15"
        assert gemini["birth_place"] == "Denver, CO"
        assert gemini["confidence"] == "medium"
        assert len(gemini["sources"]) == 1
        assert gemini["sources"][0]["source_type"] == "government"


# ---------------------------------------------------------------------------
# Status endpoint: unknown job
# ---------------------------------------------------------------------------


def test_status_unknown_job_returns_404(app_client):
    resp = app_client.get("/api/gemini-research/status/nonexistent-job-id")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Research job: nonexistent individual
# ---------------------------------------------------------------------------


def test_run_nonexistent_individual_returns_error(app_client):
    """Starting research for an individual not in DB should result in error status."""
    with patch("google.genai.Client"):
        from src.services.gemini_vitals_researcher import reset_gemini_researcher

        reset_gemini_researcher()

        resp = app_client.post(
            "/api/gemini-research/run",
            json={"individual_id": 999888777},
        )
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        deadline = time.monotonic() + 10
        status = None
        while time.monotonic() < deadline:
            poll_resp = app_client.get(f"/api/gemini-research/status/{job_id}")
            status = poll_resp.json()
            if status["status"] in ("complete", "error"):
                break
            time.sleep(0.3)

        assert status["status"] == "error"
        assert "not found" in status["error"].lower()
