"""Individuals (bio) CRUD and helpers for scraper."""

from datetime import date
from typing import Any

from .connection import get_connection, is_postgres, _DB_UNIQUE_ERRORS, _PGSavepointContext
from .utils import _row_to_dict
from . import office_terms as db_office_terms


def list_individuals(
    limit: int = 500,
    offset: int = 0,
    q: str | None = None,
    is_living: int | None = None,
    is_dead_link: int | None = None,
    conn=None,
) -> list[dict[str, Any]]:
    """Return individuals with optional pagination and filters.

    Args:
        q: Partial case-insensitive match on full_name.
        is_living: 1 for living, 0 for deceased.
        is_dead_link: 1 to show only dead-link records.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        where_clauses: list[str] = []
        params: list[Any] = []

        if q:
            if is_postgres():
                where_clauses.append("full_name ILIKE %s")
            else:
                where_clauses.append("full_name LIKE %s")
            params.append(f"%{q}%")

        if is_living is not None:
            where_clauses.append("is_living = %s")
            params.append(is_living)

        if is_dead_link is not None:
            where_clauses.append("is_dead_link = %s")
            params.append(is_dead_link)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur = conn.execute(
            f"""SELECT id, wiki_url, page_path, full_name, birth_date, death_date,
                      birth_date_imprecise, death_date_imprecise,
                      birth_place, death_place, is_living, is_dead_link, created_at, updated_at
               FROM individuals {where_sql} ORDER BY full_name LIMIT %s OFFSET %s""",
            (*params, limit, offset),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def get_individual_by_wiki_url(wiki_url: str, conn=None) -> dict[str, Any] | None:
    """Return one individual by wiki_url."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM individuals WHERE wiki_url = %s", (wiki_url,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def get_individual(individual_id: int, conn=None) -> dict[str, Any] | None:
    """Return one individual by id."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT * FROM individuals WHERE id = %s", (individual_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def upsert_individual(data: dict[str, Any], conn=None) -> int:
    """Insert or update individual by wiki_url. Returns id. Accepts birth_date_imprecise, death_date_imprecise (0/1)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        wiki_url = (data.get("wiki_url") or "").strip()
        if not wiki_url:
            raise ValueError("wiki_url required")
        bd_imprecise = 1 if data.get("birth_date_imprecise") else 0
        dd_imprecise = 1 if data.get("death_date_imprecise") else 0
        cur = conn.execute("SELECT id FROM individuals WHERE wiki_url = %s", (wiki_url,))
        row = cur.fetchone()
        is_dead_link = 1 if data.get("is_dead_link") else 0
        if row:
            conn.execute(
                """UPDATE individuals SET
                    page_path=%s, full_name=%s, birth_date=%s, death_date=%s,
                    birth_date_imprecise=%s, death_date_imprecise=%s,
                    birth_place=%s, death_place=%s, is_dead_link=%s, updated_at=NOW()
                WHERE id=%s""",
                (
                    data.get("page_path"),
                    data.get("full_name"),
                    data.get("birth_date"),
                    data.get("death_date"),
                    bd_imprecise,
                    dd_imprecise,
                    data.get("birth_place"),
                    data.get("death_place"),
                    is_dead_link,
                    row["id"],
                ),
            )
            # Recompute is_living only when currently marked living
            _recompute_is_living_for_individual(row["id"], conn)
            if own_conn:
                conn.commit()
            return row["id"]
        # Use a savepoint so a race-condition UniqueViolation on the INSERT only rolls
        # back this sub-unit — not the caller's entire transaction.  Without this, any
        # UniqueViolation puts a PostgreSQL shared connection into the aborted state and
        # every subsequent statement fails with InFailedSqlTransaction.
        # SQLite: _PGSavepointContext is a no-op (IntegrityError doesn't abort the conn).
        try:
            with _PGSavepointContext(conn, "_upsert_individual"):
                cur = conn.execute(
                    """INSERT INTO individuals (wiki_url, page_path, full_name, birth_date, death_date, birth_date_imprecise, death_date_imprecise, birth_place, death_place, is_dead_link)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (
                        wiki_url,
                        data.get("page_path"),
                        data.get("full_name"),
                        data.get("birth_date"),
                        data.get("death_date"),
                        bd_imprecise,
                        dd_imprecise,
                        data.get("birth_place"),
                        data.get("death_place"),
                        is_dead_link,
                    ),
                )
                ind_id = cur.fetchone()["id"]
            conn.execute("UPDATE individuals SET bio_batch = id %% 7 WHERE id = %s", (ind_id,))
            # New individuals start as living by default; recompute may downgrade to not living
            _recompute_is_living_for_individual(ind_id, conn)
            if own_conn:
                conn.commit()
            return ind_id
        except _DB_UNIQUE_ERRORS:
            # Race condition: another insert beat us — fall back to UPDATE path.
            # The savepoint above ensures the outer transaction is still healthy here.
            cur = conn.execute("SELECT id FROM individuals WHERE wiki_url = %s", (wiki_url,))
            row = cur.fetchone()
            if row is None:
                raise
            conn.execute(
                """UPDATE individuals SET
                    page_path=%s, full_name=%s, birth_date=%s, death_date=%s,
                    birth_date_imprecise=%s, death_date_imprecise=%s,
                    birth_place=%s, death_place=%s, is_dead_link=%s, updated_at=NOW()
                WHERE id=%s""",
                (
                    data.get("page_path"),
                    data.get("full_name"),
                    data.get("birth_date"),
                    data.get("death_date"),
                    bd_imprecise,
                    dd_imprecise,
                    data.get("birth_place"),
                    data.get("death_place"),
                    is_dead_link,
                    row["id"],
                ),
            )
            _recompute_is_living_for_individual(row["id"], conn)
            if own_conn:
                conn.commit()
            return row["id"]
    finally:
        if own_conn:
            conn.close()


def purge_all_individuals(conn=None) -> int:
    """Delete all individuals. Returns count deleted. Call after purge_all_office_terms if doing full reset."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM individuals")
        if own_conn:
            conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def get_all_individual_wiki_urls(conn=None) -> set[str]:
    """Return set of all wiki_urls in the individuals table."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT wiki_url FROM individuals")
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_existing_wiki_urls(wiki_urls: set[str], conn=None) -> set[str]:
    """Return the subset of *wiki_urls* that already exist in the individuals table.

    Use this instead of get_all_individual_wiki_urls() for delta/fresh runs where
    only a small number of URLs were scraped — avoids loading the full ~50 K-row
    set into memory.
    """
    if not wiki_urls:
        return set()
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        placeholders = ",".join(["%s"] * len(wiki_urls))
        cur = conn.execute(
            f"SELECT wiki_url FROM individuals WHERE wiki_url IN ({placeholders})",
            list(wiki_urls),
        )
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def _earliest_term_year_for_individual(individual_id: int, conn) -> int | None:
    """Return earliest known term year for an individual (from office_terms), or None if none."""
    # Prefer term_start_year; fall back to year component of term_start (YYYY-MM-DD).
    if is_postgres():
        year_expr = "EXTRACT(YEAR FROM term_start::date)::integer"
    else:
        year_expr = "CAST(strftime('%Y', term_start) AS INTEGER)"
    cur = conn.execute(
        f"""
        SELECT MIN(
                 COALESCE(term_start_year, {year_expr})
               ) AS y
        FROM office_terms
        WHERE individual_id = %s
        """,
        (individual_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    y = row[0]
    try:
        return int(y) if y is not None else None
    except (TypeError, ValueError):
        return None


def _recompute_is_living_for_individual(individual_id: int, conn) -> None:
    """Recompute is_living for one individual, but only if currently marked living (1).

    Rules:
    - If death_date is set -> is_living = 0.
    - Else if earliest known office term is >80 years ago -> is_living = 0.
    - Else keep is_living = 1.
    """
    cur = conn.execute(
        "SELECT is_living, death_date FROM individuals WHERE id = %s",
        (individual_id,),
    )
    row = cur.fetchone()
    if not row:
        return
    is_living = row["is_living"]
    death_date = (row["death_date"] or "").strip() if row["death_date"] is not None else ""
    # Never flip someone back to living
    if is_living == 0:
        return

    # Default: stay living unless we have strong evidence otherwise
    new_flag = 1
    if death_date:
        new_flag = 0
    else:
        earliest_year = _earliest_term_year_for_individual(individual_id, conn)
        if earliest_year is not None:
            current_year = date.today().year
            if current_year - earliest_year > 80:
                new_flag = 0
    if new_flag != is_living:
        conn.execute(
            "UPDATE individuals SET is_living = %s WHERE id = %s", (new_flag, individual_id)
        )


def recompute_is_living_batch(individual_ids: list[int], conn) -> int:
    """Batch recompute is_living for a set of individuals. Returns count updated.

    Equivalent to calling _recompute_is_living_for_individual for each id, but
    uses two queries total (one SELECT, one bulk earliest-year query) instead of
    two queries per individual.
    """
    if not individual_ids:
        return 0

    placeholders = ",".join(["%s"] * len(individual_ids))

    # Load current is_living + death_date for all touched individuals in one query.
    rows = conn.execute(
        f"SELECT id, is_living, death_date FROM individuals WHERE id IN ({placeholders})",
        individual_ids,
    ).fetchall()

    # Only consider individuals still marked living — never flip back.
    living_ids = [
        r["id"] for r in rows if r["is_living"] == 1 and not (r["death_date"] or "").strip()
    ]
    dead_by_death_date = [
        r["id"] for r in rows if r["is_living"] == 1 and (r["death_date"] or "").strip()
    ]

    # Bulk earliest-term-year for still-living individuals (no death_date set).
    earliest_by_id: dict[int, int] = {}
    if living_ids:
        lp = ",".join(["%s"] * len(living_ids))
        if is_postgres():
            year_expr = "EXTRACT(YEAR FROM term_start::date)::integer"
        else:
            year_expr = "CAST(strftime('%Y', term_start) AS INTEGER)"
        for ey_row in conn.execute(
            f"""SELECT individual_id,
                       MIN(COALESCE(term_start_year, {year_expr})) AS y
                FROM office_terms
                WHERE individual_id IN ({lp})
                GROUP BY individual_id""",
            living_ids,
        ).fetchall():
            try:
                if ey_row["y"] is not None:
                    earliest_by_id[ey_row["individual_id"]] = int(ey_row["y"])
            except (TypeError, ValueError):
                pass

    current_year = date.today().year
    to_flip = list(dead_by_death_date)
    for ind_id in living_ids:
        earliest = earliest_by_id.get(ind_id)
        if earliest is not None and current_year - earliest > 80:
            to_flip.append(ind_id)

    if not to_flip:
        return 0

    fp = ",".join(["%s"] * len(to_flip))
    conn.execute(
        f"UPDATE individuals SET is_living = 0 WHERE id IN ({fp})",
        to_flip,
    )
    return len(to_flip)


def get_living_individual_wiki_urls(conn=None) -> set[str]:
    """Return set of wiki_urls for individuals considered living (is_living = 1, not dead-link, not No link: placeholder)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT wiki_url FROM individuals WHERE is_living = 1 AND is_dead_link = 0 AND wiki_url NOT LIKE 'No link:%%'"
        )
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_living_individuals_for_batch(batch: int, conn=None) -> list[str]:
    """Return wiki_urls of living individuals in bio_batch 0–6, ordered so never-refreshed come first."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT wiki_url FROM individuals
               WHERE is_living = 1 AND is_dead_link = 0
                 AND wiki_url NOT LIKE 'No link:%%'
                 AND bio_batch = %s
               ORDER BY bio_refreshed_at ASC NULLS FIRST""",
            (batch,),
        )
        return [row["wiki_url"] for row in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def mark_bio_refreshed(wiki_url: str, conn=None) -> None:
    """Stamp bio_refreshed_at = now for the given wiki_url."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE individuals SET bio_refreshed_at = NOW() WHERE wiki_url = %s",
            (wiki_url,),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_dead_link_wiki_urls(conn=None) -> set[str]:
    """Return set of wiki_urls for individuals marked as dead link (no bio page)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT wiki_url FROM individuals WHERE is_dead_link = 1")
        return {row["wiki_url"] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def list_individuals_for_office_category(
    office_category_id: int,
    living_only: bool = False,
    valid_page_paths_only: bool = False,
    conn=None,
) -> list[dict[str, Any]]:
    """Return individuals connected to office terms in a given office category."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        where = ["od.office_category_id = %s", "i.id IS NOT NULL"]
        params: list[Any] = [office_category_id]
        if living_only:
            where.append("i.is_living = 1")
        if valid_page_paths_only:
            where.append("i.page_path IS NOT NULL")
            where.append("TRIM(i.page_path) <> ''")
        cur = conn.execute(
            f"""
            SELECT
                s.name AS state_name,
                od.name AS senate_class,
                i.page_path,
                ot.term_start,
                ot.term_end,
                i.birth_date,
                i.death_date,
                i.id,
                i.wiki_url,
                i.full_name
            FROM office_details od
            LEFT JOIN office_terms ot ON ot.office_details_id = od.id
            LEFT JOIN individuals i ON ot.individual_id = i.id
            LEFT JOIN source_pages sp ON od.source_page_id = sp.id
            LEFT JOIN states s ON sp.state_id = s.id
            WHERE {' AND '.join(where)}
            ORDER BY i.id, ot.term_start
            """,
            tuple(params),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def _insufficient_vitals_where_clause() -> str:
    """Return the WHERE fragment identifying individuals with incomplete vital data.

    Currently flags:
      - birth_date IS NULL  (missing birth date)
      - death_date IS NULL AND is_living = 0  (confirmed dead but no death date)

    Extend this function when additional vitals fields are tracked (e.g. birth/death place).
    """
    return "(birth_date IS NULL OR (death_date IS NULL AND is_living = 0))"


def get_insufficient_vitals_individuals_for_batch(batch: int, conn=None) -> list[dict]:
    """Return individuals in *batch* with insufficient vitals that need a recheck.

    Batch assignment: id % 30  (computed in the DB; never stored).
    Daily pick:       date.today().day % 30

    Included when ALL of these hold:
      - insufficient vitals (see _insufficient_vitals_where_clause)
      - is_dead_link = 0                       (has a Wikipedia page to look up)
      - wiki_url NOT LIKE 'No link:%'          (not a manual no-link entry)
      - insufficient_vitals_checked_at IS NULL OR checked > 30 days ago

    Returns list of dicts with id, wiki_url, full_name.
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id, wiki_url, full_name
            FROM individuals
            WHERE {_insufficient_vitals_where_clause()}
              AND is_dead_link = 0
              AND wiki_url NOT LIKE %s
              AND (id %% 30) = %s
              AND (
                  insufficient_vitals_checked_at IS NULL
                  OR insufficient_vitals_checked_at < %s
              )
            ORDER BY id
            """,
            ("No link:%", batch, cutoff),
        ).fetchall()
        return [dict(zip(("id", "wiki_url", "full_name"), row)) for row in rows]
    finally:
        if own_conn:
            conn.close()


def mark_insufficient_vitals_checked(individual_id: int, conn=None) -> None:
    """Set insufficient_vitals_checked_at = NOW() for *individual_id*.

    Called after every insufficient-vitals bio attempt (success or error) so the
    individual is skipped for the next 30 days.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE individuals SET insufficient_vitals_checked_at = NOW() WHERE id = %s",
            (individual_id,),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_gemini_research_candidates_for_batch(batch: int, conn=None) -> list[dict]:
    """Return individuals in *batch* eligible for Gemini deep research.

    Same bucketing as insufficient-vitals (id % 30) but with a **90-day** cooldown
    via gemini_research_checked_at, reflecting the higher cost of API research.

    Uses _insufficient_vitals_where_clause() for the vitals criteria so both
    the Wikipedia recheck and Gemini research stay in sync.

    Returns list of dicts with id, wiki_url, full_name.
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id, wiki_url, full_name
            FROM individuals
            WHERE {_insufficient_vitals_where_clause()}
              AND is_dead_link = 0
              AND wiki_url NOT LIKE %s
              AND (id %% 30) = %s
              AND (
                  gemini_research_checked_at IS NULL
                  OR gemini_research_checked_at < %s
              )
            ORDER BY id
            """,
            ("No link:%", batch, cutoff),
        ).fetchall()
        return [dict(zip(("id", "wiki_url", "full_name"), row)) for row in rows]
    finally:
        if own_conn:
            conn.close()


def get_dead_link_research_candidates_for_batch(batch: int, conn=None) -> list[dict]:
    """Return dead-link individuals in *batch* eligible for Gemini deep research.

    Targets individuals where is_dead_link=1 OR wiki_url LIKE 'No link:%'.
    Same id % 30 bucketing and 90-day cooldown as regular Gemini research.

    Returns list of dicts with id, wiki_url, full_name.
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, wiki_url, full_name
            FROM individuals
            WHERE (is_dead_link = 1 OR wiki_url LIKE %s)
              AND (id %% 30) = %s
              AND (
                  gemini_research_checked_at IS NULL
                  OR gemini_research_checked_at < %s
              )
            ORDER BY id
            """,
            ("No link:%", batch, cutoff),
        ).fetchall()
        return [dict(zip(("id", "wiki_url", "full_name"), row)) for row in rows]
    finally:
        if own_conn:
            conn.close()


def find_nolink_by_name_and_office(office_id: int, name: str, conn=None) -> dict | None:
    """Find a "No link:{office_id}:{name}" placeholder individual by office and name.

    Matches case-insensitively with whitespace normalization. Returns the first
    matching individual dict, or None if not found.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        prefix = f"No link:{office_id}:"
        cur = conn.execute(
            "SELECT id, wiki_url, full_name FROM individuals WHERE wiki_url LIKE %s",
            (prefix + "%",),
        )
        target = " ".join(name.lower().split())
        for row in cur.fetchall():
            ind_id, wiki_url, full_name = row[0], row[1], row[2]
            embedded = wiki_url[len(prefix) :]
            if " ".join(embedded.lower().split()) == target:
                return {"id": ind_id, "wiki_url": wiki_url, "full_name": full_name}
        return None
    finally:
        if own_conn:
            conn.close()


def mark_superseded(old_id: int, new_id: int, conn=None) -> int:
    """Retire a no-link placeholder by reassigning its office_terms and marking it superseded.

    Steps (within caller's transaction):
    1. Reassign office_terms.individual_id from old_id → new_id.
    2. Set individuals.superseded_by_individual_id = new_id on the old row.
    3. Set individuals.is_dead_link = 1 on the old row.

    Returns the number of office_terms rows reassigned.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE office_terms SET individual_id = %s WHERE individual_id = %s",
            (new_id, old_id),
        )
        reassigned = cur.rowcount

        conn.execute(
            "UPDATE individuals"
            " SET superseded_by_individual_id = %s, is_dead_link = 1"
            " WHERE id = %s",
            (new_id, old_id),
        )

        if own_conn:
            conn.commit()
        return reassigned
    finally:
        if own_conn:
            conn.close()


def mark_gemini_research_checked(individual_id: int, conn=None) -> None:
    """Set gemini_research_checked_at = NOW() for *individual_id*.

    Called after every Gemini research attempt (success or failure) so the
    individual is skipped for the next 90 days.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE individuals SET gemini_research_checked_at = NOW() WHERE id = %s",
            (individual_id,),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()
