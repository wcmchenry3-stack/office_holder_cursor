# -*- coding: utf-8 -*-
"""Static registry of all scraper run modes.

Used by the /data/runner-registry UI page. Expiry timeouts must stay in sync
with the rules in scraper_jobs.expire_stale_jobs().
"""

from __future__ import annotations

RUNNER_REGISTRY: list[dict] = [
    {
        "id": "full",
        "label": "Full Run",
        "description": (
            "Deletes all office_terms and re-scrapes every enabled office from scratch. "
            "Use after major config changes or to recover from widespread data corruption."
        ),
        "expiry_hours": 24,
        "trigger": "Manual only",
    },
    {
        "id": "delta",
        "label": "Delta Run",
        "description": (
            "Incremental update: parses all enabled offices, inserting/updating terms "
            "that have changed. The standard nightly job."
        ),
        "expiry_hours": 8,
        "trigger": "Manual + scheduled daily (06:00 UTC)",
    },
    {
        "id": "delta_insufficient_vitals",
        "label": "Insufficient Vitals Delta",
        "description": (
            "Re-scrapes only the offices that have individuals with missing vital dates "
            "(birth/death). Runs after the main delta to fill gaps."
        ),
        "expiry_hours": 8,
        "trigger": "Manual + scheduled daily (07:00 UTC)",
    },
    {
        "id": "gemini_vitals_research",
        "label": "Gemini Vitals Research",
        "description": (
            "Queries Gemini to find verified birth/death dates for individuals lacking "
            "confirmed vitals. Writes results back to the DB."
        ),
        "expiry_hours": 8,
        "trigger": "Manual + scheduled daily (08:00 UTC)",
    },
    {
        "id": "live_person",
        "label": "Live Person Run",
        "description": (
            "Scrapes only offices that have at least one currently-serving individual. "
            "Used to keep active officeholders up to date between full runs."
        ),
        "expiry_hours": 8,
        "trigger": "Manual only",
    },
    {
        "id": "single_bio",
        "label": "Single Bio",
        "description": (
            "Runs the bio scraper for one specific individual by wiki URL or ID. "
            "Used for targeted updates without running a full bio pass."
        ),
        "expiry_hours": 8,
        "trigger": "Manual only",
    },
    {
        "id": "selected_bios",
        "label": "Selected Bios",
        "description": (
            "Runs the bio scraper for a filtered set of individuals "
            "(e.g. those missing birth dates for a specific office category)."
        ),
        "expiry_hours": 8,
        "trigger": "Manual only",
    },
]
