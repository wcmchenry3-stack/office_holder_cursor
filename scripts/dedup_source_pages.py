#!/usr/bin/env python3
"""
Deduplicate source_pages by URL: keep one page per URL (smallest id), relink office_details
to that page, and disable duplicate source_pages. Uses the app DB at data/office_holder.db.

Usage: python scripts/dedup_source_pages.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db.offices import deduplicate_source_pages_by_url


def main() -> None:
    result = deduplicate_source_pages_by_url()
    print(json.dumps(result, indent=2))
    if result.get("errors"):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
