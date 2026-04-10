# -*- coding: utf-8 -*-
"""Tests for the Wikipedia submission and preview endpoints in src/routers/data.py.

Covers:
- GET /api/wikipedia/status
- POST /api/wiki-drafts/{proposal_id}/submit
- GET /api/wiki-drafts/{proposal_id}/preview
- GET /data/wiki-drafts/{proposal_id} — validation panel rendering

No live HTTP requests are made to Wikipedia in these tests.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VALID_WIKITEXT = """\
{{Infobox officeholder
| name       = John Test
| birth_date = {{birth date|1950|03|15}}
}}
John Test was a politician.<ref>https://example.com</ref>
==References==
{{reflist}}
[[Category:People]]
"""

_INVALID_WIKITEXT = "Just some plain text with no wikitext structure."


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("wiki_submit_db")
    db_path = tmp / "test.db"
    cache_dir = tmp / "wiki_cache"
    cache_dir.mkdir()

    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    os.environ["WIKI_CACHE_DIR"] = str(cache_dir)
    # Ensure no real Wikipedia credentials in the test environment
    os.environ.pop("WIKIPEDIA_BOT_USERNAME", None)
    os.environ.pop("WIKIPEDIA_BOT_PASSWORD", None)

    from src.main import app
    from src.db.connection import init_db

    init_db()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(scope="module")
def seeded_draft_id(client):
    """Create a pending wiki draft for a real individual. Returns proposal_id."""
    from src.db import individuals as db_individuals
    from src.db import individual_research_sources as db_research

    ind_id = db_individuals.upsert_individual({
        "wiki_url": "https://en.wikipedia.org/wiki/John_Test",
        "full_name": "John Test",
    })
    proposal_id = db_research.insert_wiki_draft_proposal(
        individual_id=ind_id,
        proposal_text=_VALID_WIKITEXT,
        status="pending",
    )
    return proposal_id


@pytest.fixture(scope="module")
def submitted_draft_id(client):
    """Create an already-submitted draft. Returns proposal_id."""
    from src.db import individuals as db_individuals
    from src.db import individual_research_sources as db_research

    ind_id = db_individuals.upsert_individual({
        "wiki_url": "https://en.wikipedia.org/wiki/Already_Submitted",
        "full_name": "Already Submitted",
    })
    proposal_id = db_research.insert_wiki_draft_proposal(
        individual_id=ind_id,
        proposal_text=_VALID_WIKITEXT,
        status="submitted",
    )
    return proposal_id


@pytest.fixture(scope="module")
def nameless_draft_id(client):
    """Create a draft for an individual with no full_name. Returns proposal_id."""
    from src.db import individuals as db_individuals
    from src.db import individual_research_sources as db_research

    ind_id = db_individuals.upsert_individual({
        "wiki_url": "https://en.wikipedia.org/wiki/Nameless_Person",
        "full_name": None,
    })
    proposal_id = db_research.insert_wiki_draft_proposal(
        individual_id=ind_id,
        proposal_text=_VALID_WIKITEXT,
        status="pending",
    )
    return proposal_id


@pytest.fixture(scope="module")
def invalid_wikitext_draft_id(client):
    """Create a draft with invalid (unstructured) wikitext. Returns proposal_id."""
    from src.db import individuals as db_individuals
    from src.db import individual_research_sources as db_research

    ind_id = db_individuals.upsert_individual({
        "wiki_url": "https://en.wikipedia.org/wiki/Bad_Format_Person",
        "full_name": "Bad Format Person",
    })
    proposal_id = db_research.insert_wiki_draft_proposal(
        individual_id=ind_id,
        proposal_text=_INVALID_WIKITEXT,
        status="pending",
    )
    return proposal_id


# ---------------------------------------------------------------------------
# GET /api/wikipedia/status
# ---------------------------------------------------------------------------


class TestWikipediaStatus:
    def test_not_configured_when_no_env_vars(self, client, monkeypatch):
        monkeypatch.delenv("WIKIPEDIA_BOT_USERNAME", raising=False)
        monkeypatch.delenv("WIKIPEDIA_BOT_PASSWORD", raising=False)
        r = client.get("/api/wikipedia/status")
        assert r.status_code == 200
        assert r.json() == {"configured": False}

    def test_configured_when_both_vars_set(self, client, monkeypatch):
        monkeypatch.setenv("WIKIPEDIA_BOT_USERNAME", "TestBot@mybot")
        monkeypatch.setenv("WIKIPEDIA_BOT_PASSWORD", "super-secret")
        r = client.get("/api/wikipedia/status")
        assert r.status_code == 200
        assert r.json() == {"configured": True}

    def test_not_configured_when_only_username_set(self, client, monkeypatch):
        monkeypatch.setenv("WIKIPEDIA_BOT_USERNAME", "TestBot@mybot")
        monkeypatch.delenv("WIKIPEDIA_BOT_PASSWORD", raising=False)
        r = client.get("/api/wikipedia/status")
        assert r.status_code == 200
        assert r.json() == {"configured": False}

    def test_not_configured_when_only_password_set(self, client, monkeypatch):
        monkeypatch.delenv("WIKIPEDIA_BOT_USERNAME", raising=False)
        monkeypatch.setenv("WIKIPEDIA_BOT_PASSWORD", "secret")
        r = client.get("/api/wikipedia/status")
        assert r.status_code == 200
        assert r.json() == {"configured": False}


# ---------------------------------------------------------------------------
# POST /api/wiki-drafts/{proposal_id}/submit
# ---------------------------------------------------------------------------


def _mock_submitter():
    submitter = MagicMock()
    submitter.submit_article.return_value = {"result": "Success"}
    return submitter


class TestSubmitWikiDraft:
    def test_returns_503_when_no_credentials(self, client, seeded_draft_id):
        with patch("src.services.wikipedia_submit.get_submitter", return_value=None):
            r = client.post(f"/api/wiki-drafts/{seeded_draft_id}/submit")
        assert r.status_code == 503

    def test_returns_404_for_missing_draft(self, client):
        with patch("src.services.wikipedia_submit.get_submitter", return_value=_mock_submitter()):
            r = client.post("/api/wiki-drafts/99999/submit")
        assert r.status_code == 404

    def test_returns_409_for_non_pending_draft(self, client, submitted_draft_id):
        with patch("src.services.wikipedia_submit.get_submitter", return_value=_mock_submitter()):
            r = client.post(f"/api/wiki-drafts/{submitted_draft_id}/submit")
        assert r.status_code == 409

    def test_returns_400_for_nameless_individual(self, client, nameless_draft_id):
        with patch("src.services.wikipedia_submit.get_submitter", return_value=_mock_submitter()):
            r = client.post(f"/api/wiki-drafts/{nameless_draft_id}/submit")
        assert r.status_code == 400

    def test_submit_succeeds_with_draft_namespace_default(self, client, seeded_draft_id):
        mock_sub = _mock_submitter()
        with patch("src.services.wikipedia_submit.get_submitter", return_value=mock_sub):
            r = client.post(
                f"/api/wiki-drafts/{seeded_draft_id}/submit",
                json={"use_draft_namespace": True},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["title"].startswith("Draft:")
        assert "John Test" in data["title"]
        assert "wikipedia.org" in data["url"]

    def test_submit_succeeds_with_main_namespace(self, client):
        # Need a fresh pending draft for this test
        from src.db import individuals as db_individuals
        from src.db import individual_research_sources as db_research

        ind_id = db_individuals.upsert_individual({
            "wiki_url": "https://en.wikipedia.org/wiki/Direct_Submit_Person",
            "full_name": "Direct Submit Person",
        })
        pid = db_research.insert_wiki_draft_proposal(
            individual_id=ind_id,
            proposal_text=_VALID_WIKITEXT,
            status="pending",
        )
        mock_sub = _mock_submitter()
        with patch("src.services.wikipedia_submit.get_submitter", return_value=mock_sub):
            r = client.post(
                f"/api/wiki-drafts/{pid}/submit",
                json={"use_draft_namespace": False},
            )
        assert r.status_code == 200
        data = r.json()
        assert not data["title"].startswith("Draft:")
        assert data["title"] == "Direct Submit Person"

    def test_submit_sets_status_to_submitted_on_success(self, client):
        from src.db import individuals as db_individuals
        from src.db import individual_research_sources as db_research

        ind_id = db_individuals.upsert_individual({
            "wiki_url": "https://en.wikipedia.org/wiki/Status_Check_Person",
            "full_name": "Status Check Person",
        })
        pid = db_research.insert_wiki_draft_proposal(
            individual_id=ind_id,
            proposal_text=_VALID_WIKITEXT,
            status="pending",
        )
        mock_sub = _mock_submitter()
        with patch("src.services.wikipedia_submit.get_submitter", return_value=mock_sub):
            client.post(f"/api/wiki-drafts/{pid}/submit", json={"use_draft_namespace": True})
        # Check DB status was updated
        draft = db_research.get_wiki_draft_proposal(pid)
        assert draft["status"] == "submitted"

    def test_submit_returns_502_and_sets_rejected_on_wikipedia_error(self, client):
        from src.db import individuals as db_individuals
        from src.db import individual_research_sources as db_research
        from src.services.wikipedia_submit import WikipediaSubmitError

        ind_id = db_individuals.upsert_individual({
            "wiki_url": "https://en.wikipedia.org/wiki/Error_Person",
            "full_name": "Error Person",
        })
        pid = db_research.insert_wiki_draft_proposal(
            individual_id=ind_id,
            proposal_text=_VALID_WIKITEXT,
            status="pending",
        )
        mock_sub = _mock_submitter()
        mock_sub.submit_article.side_effect = WikipediaSubmitError("articleexists")
        with patch("src.services.wikipedia_submit.get_submitter", return_value=mock_sub):
            r = client.post(f"/api/wiki-drafts/{pid}/submit", json={"use_draft_namespace": True})
        assert r.status_code == 502
        draft = db_research.get_wiki_draft_proposal(pid)
        assert draft["status"] == "rejected"


# ---------------------------------------------------------------------------
# GET /api/wiki-drafts/{proposal_id}/preview
# ---------------------------------------------------------------------------


class TestWikiDraftPreview:
    def test_returns_404_for_missing_draft(self, client):
        r = client.get("/api/wiki-drafts/99999/preview")
        assert r.status_code == 404

    def test_returns_html_from_wikipedia_api(self, client, seeded_draft_id):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "parse": {"text": {"*": "<div><p>Rendered article</p></div>"}}
        }
        with patch("src.routers.data._requests") as mock_req:
            mock_req.post.return_value = mock_response
            r = client.get(f"/api/wiki-drafts/{seeded_draft_id}/preview")
        assert r.status_code == 200
        data = r.json()
        assert "html" in data
        assert "<p>Rendered article</p>" in data["html"]

    def test_returns_503_on_wikipedia_api_failure(self, client, seeded_draft_id):
        import requests as real_requests
        with patch("src.routers.data._requests") as mock_req:
            mock_req.post.side_effect = real_requests.RequestException("timeout")
            r = client.get(f"/api/wiki-drafts/{seeded_draft_id}/preview")
        assert r.status_code == 503


# ---------------------------------------------------------------------------
# GET /data/wiki-drafts/{proposal_id} — validation panel
# ---------------------------------------------------------------------------


class TestDraftDetailValidationPanel:
    def test_detail_page_shows_validation_panel(self, client, seeded_draft_id):
        r = client.get(f"/data/wiki-drafts/{seeded_draft_id}")
        assert r.status_code == 200
        assert "Wikitext Validation" in r.text

    def test_valid_draft_shows_pass(self, client, seeded_draft_id):
        r = client.get(f"/data/wiki-drafts/{seeded_draft_id}")
        assert r.status_code == 200
        assert "PASS" in r.text

    def test_invalid_draft_shows_errors(self, client, invalid_wikitext_draft_id):
        r = client.get(f"/data/wiki-drafts/{invalid_wikitext_draft_id}")
        assert r.status_code == 200
        assert "error" in r.text.lower()
        # Must not show PASS for a structurally broken draft
        # (check that the danger color class appears, not a green pass)
        assert "missing_infobox" not in r.text  # code not shown, message is
        assert "Infobox" in r.text  # error message mentions Infobox

    def test_detail_page_includes_preview_tab(self, client, seeded_draft_id):
        r = client.get(f"/data/wiki-drafts/{seeded_draft_id}")
        assert r.status_code == 200
        assert "Preview" in r.text
        assert "switchTab" in r.text
