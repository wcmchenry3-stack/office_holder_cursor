"""
cleanup_congressional_district_individuals.py

One-time cleanup: remove individuals whose wiki_url is a congressional district
page rather than a person's page. These were created by a bug in patterns_to_ignore()
where [\\w%]+ failed to match apostrophes in possessive state names like
"Mississippi's_4th_congressional_district". See GitHub issue #212.

Usage:
    python cleanup_congressional_district_individuals.py [--dry-run]

Set DATABASE_URL env var to point at the target database (production or local).
Defaults to local DB if DATABASE_URL is not set.
"""

import argparse
import re
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from src.db.connection import get_connection

# Matches /wiki/<state's>_<N>(th|st|nd|rd)_congressional_district
# and /wiki/<state's>_at-large_congressional_district
# Same pattern as the fixed patterns_to_ignore()
_DISTRICT_RE = re.compile(
    r"/wiki/[^/]*_(\d{1,2}(th|st|nd|rd)|at-large)_congressional_district",
    re.IGNORECASE,
)

ALSO_FLAG_PATTERNS = [
    # Catch any remaining edge cases: district pages not matched by the main pattern
    re.compile(r"/wiki/[^/]+_congressional_district", re.IGNORECASE),
]


def is_district_url(wiki_url: str) -> bool:
    if not wiki_url:
        return False
    if _DISTRICT_RE.search(wiki_url):
        return True
    for pat in ALSO_FLAG_PATTERNS:
        if pat.search(wiki_url):
            return True
    return False


def main(dry_run: bool) -> None:
    conn = get_connection()

    # --- Step 1: find all matching individuals ---
    cur = conn.execute("SELECT id, wiki_url, full_name FROM individuals")
    all_individuals = [dict(r) for r in cur.fetchall()]

    district_ids = [
        row["id"] for row in all_individuals if is_district_url(row.get("wiki_url") or "")
    ]

    if not district_ids:
        print("No congressional district individuals found. Nothing to clean up.")
        conn.close()
        return

    print(f"\nFound {len(district_ids)} individual(s) with congressional district wiki_url:\n")
    for iid in district_ids:
        row = next(r for r in all_individuals if r["id"] == iid)
        print(f"  id={iid}  url={row['wiki_url']}")

    # --- Step 2: count their office_terms ---
    placeholders = ",".join(["%s"] * len(district_ids))
    cur2 = conn.execute(
        f"SELECT individual_id, COUNT(*) AS cnt FROM office_terms "
        f"WHERE individual_id IN ({placeholders}) GROUP BY individual_id",
        district_ids,
    )
    term_counts = {r["individual_id"]: r["cnt"] for r in cur2.fetchall()}
    total_terms = sum(term_counts.values())

    print(f"\nAssociated office_terms rows to delete: {total_terms}")

    if dry_run:
        print("\n[DRY RUN] No changes made. Re-run without --dry-run to apply.")
        conn.close()
        return

    # --- Step 3: confirm ---
    answer = (
        input(
            f"\nDelete {len(district_ids)} individual(s) and {total_terms} office_term(s)? [yes/N] "
        )
        .strip()
        .lower()
    )
    if answer != "yes":
        print("Aborted.")
        conn.close()
        return

    # --- Step 4: delete in a transaction ---
    try:
        conn.execute("BEGIN")

        # Delete office_terms first (FK constraint)
        conn.execute(
            f"DELETE FROM office_terms WHERE individual_id IN ({placeholders})",
            district_ids,
        )

        # Delete the individuals
        conn.execute(
            f"DELETE FROM individuals WHERE id IN ({placeholders})",
            district_ids,
        )

        conn.execute("COMMIT")
        print(
            f"\nDeleted {len(district_ids)} individual(s) and {total_terms} office_term(s). Done."
        )
    except Exception as exc:
        conn.execute("ROLLBACK")
        print(f"\nERROR: {exc}\nRolled back. No changes made.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted without making changes",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
