"""Office terms (scraped results) write/read and delta logic. Supports hierarchy: office_details_id, office_table_config_id."""

from typing import Any

from .connection import get_connection, is_postgres, _DB_OPERATIONAL_ERRORS
from .utils import _row_to_dict


def _has_hierarchy_terms(conn) -> bool:
    """True if office_terms has office_table_config_id column."""
    if is_postgres():
        return True
    try:
        cur = conn.execute("PRAGMA table_info(office_terms)")
        cols = [row[1] for row in cur.fetchall()]
        return "office_table_config_id" in cols
    except _DB_OPERATIONAL_ERRORS:
        return False


def insert_office_term(
    office_id: int | None = None,
    individual_id: int | None = None,
    wiki_url: str = "",
    party_id: int | None = None,
    district: str | None = None,
    term_start: str | None = None,
    term_end: str | None = None,
    term_start_year: int | None = None,
    term_end_year: int | None = None,
    term_start_imprecise: bool = False,
    term_end_imprecise: bool = False,
    office_details_id: int | None = None,
    office_table_config_id: int | None = None,
    conn=None,
) -> int:
    """Insert one office term. Returns id. With hierarchy pass office_details_id and office_table_config_id; else office_id (legacy)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        ts_imp = 1 if term_start_imprecise else 0
        te_imp = 1 if term_end_imprecise else 0
        # #region agent log
        try:
            from pathlib import Path as _P
            import json as _J
            import time as _T

            _log_path = _P(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
            open(_log_path, "a", encoding="utf-8").write(
                _J.dumps(
                    {
                        "id": "log_insert_office_term",
                        "timestamp": _T.time() * 1000,
                        "location": "db/office_terms.py:insert_office_term",
                        "message": "insert_office_term call",
                        "data": {
                            "office_id": office_id,
                            "office_details_id": office_details_id,
                            "office_table_config_id": office_table_config_id,
                            "has_hierarchy_terms": _has_hierarchy_terms(conn),
                            "wiki_url": (wiki_url or "")[:80],
                        },
                        "runId": "pre-fix",
                        "hypothesisId": "H4",
                    }
                )
                + "\n"
            )
        except Exception:
            pass
        # #endregion
        if (
            _has_hierarchy_terms(conn)
            and office_details_id is not None
            and office_table_config_id is not None
        ):
            # office_id is NOT NULL; use office_table_config_id so the runnable-unit id is consistent.
            cur = conn.execute(
                """INSERT INTO office_terms
                   (office_id, office_details_id, office_table_config_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, term_start_imprecise, term_end_imprecise, wiki_url)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (office_id, wiki_url, term_start, term_end, term_start_year, term_end_year) DO UPDATE SET
                     individual_id=EXCLUDED.individual_id,
                     party_id=EXCLUDED.party_id,
                     district=EXCLUDED.district,
                     term_start_imprecise=EXCLUDED.term_start_imprecise,
                     term_end_imprecise=EXCLUDED.term_end_imprecise,
                     office_details_id=EXCLUDED.office_details_id,
                     office_table_config_id=EXCLUDED.office_table_config_id
                   RETURNING id""",
                (
                    office_table_config_id,
                    office_details_id,
                    office_table_config_id,
                    individual_id,
                    party_id,
                    district or None,
                    term_start,
                    term_end,
                    term_start_year,
                    term_end_year,
                    ts_imp,
                    te_imp,
                    wiki_url,
                ),
            )
        else:
            cur = conn.execute(
                """INSERT INTO office_terms
                   (office_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, term_start_imprecise, term_end_imprecise, wiki_url)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (office_id, wiki_url, term_start, term_end, term_start_year, term_end_year) DO UPDATE SET
                     individual_id=EXCLUDED.individual_id,
                     party_id=EXCLUDED.party_id,
                     district=EXCLUDED.district,
                     term_start_imprecise=EXCLUDED.term_start_imprecise,
                     term_end_imprecise=EXCLUDED.term_end_imprecise,
                     office_details_id=EXCLUDED.office_details_id,
                     office_table_config_id=EXCLUDED.office_table_config_id
                   RETURNING id""",
                (
                    office_id,
                    individual_id,
                    party_id,
                    district or None,
                    term_start,
                    term_end,
                    term_start_year,
                    term_end_year,
                    ts_imp,
                    te_imp,
                    wiki_url,
                ),
            )
        conn.commit()
        return cur.fetchone()["id"]
    finally:
        if own_conn:
            conn.close()


def count_terms_for_office(office_id: int, conn=None) -> int:
    """Return the number of office_terms for an office (office_id is office_details_id in hierarchy)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _has_hierarchy_terms(conn):
            cur = conn.execute(
                "SELECT COUNT(*) FROM office_terms WHERE office_details_id = %s", (office_id,)
            )
        else:
            cur = conn.execute(
                "SELECT COUNT(*) FROM office_terms WHERE office_id = %s", (office_id,)
            )
        return cur.fetchone()[0]
    finally:
        if own_conn:
            conn.close()


def get_terms_counts_by_office(conn=None) -> dict[int, int]:
    """Return dict mapping office_id to count (office_details_id in hierarchy, else legacy office_id)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _has_hierarchy_terms(conn):
            cur = conn.execute(
                "SELECT office_details_id, COUNT(*) FROM office_terms WHERE office_details_id IS NOT NULL GROUP BY office_details_id"
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        cur = conn.execute("SELECT office_id, COUNT(*) FROM office_terms GROUP BY office_id")
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def get_existing_terms_for_office(
    office_id: int, conn=None
) -> list[dict[str, Any]]:
    """Return existing office_terms for a unit (office_id is office_table_config_id in hierarchy, for delta compare)."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _has_hierarchy_terms(conn):
            cur = conn.execute(
                """SELECT id, office_id, office_details_id, office_table_config_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, wiki_url
                   FROM office_terms WHERE office_table_config_id = %s""",
                (office_id,),
            )
        else:
            cur = conn.execute(
                """SELECT id, office_id, individual_id, party_id, district, term_start, term_end, term_start_year, term_end_year, wiki_url
                   FROM office_terms WHERE office_id = %s""",
                (office_id,),
            )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()


def delete_office_terms_for_office(office_id: int, conn=None) -> int:
    """Delete all office_terms for a unit (office_id is office_table_config_id in hierarchy). Returns count deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if _has_hierarchy_terms(conn):
            cur = conn.execute(
                "DELETE FROM office_terms WHERE office_table_config_id = %s", (office_id,)
            )
        else:
            cur = conn.execute("DELETE FROM office_terms WHERE office_id = %s", (office_id,))
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def purge_all_office_terms(conn=None) -> int:
    """Delete all office_terms. Returns count deleted."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM office_terms")
        conn.commit()
        return cur.rowcount
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
        conn.commit()
        return cur.rowcount
    finally:
        if own_conn:
            conn.close()


def list_office_terms(
    limit: int = 200,
    offset: int = 0,
    office_id: int | None = None,
    conn=None,
) -> list[dict[str, Any]]:
    """List office terms with optional office filter (office_id is office_details_id in hierarchy) and pagination."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        if office_id is not None and _has_hierarchy_terms(conn):
            cur = conn.execute(
                """SELECT ot.id, ot.office_details_id AS office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          od.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   JOIN office_details od ON od.id = ot.office_details_id
                   JOIN source_pages sp ON sp.id = od.source_page_id
                   LEFT JOIN countries c ON c.id = sp.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   WHERE ot.office_details_id = %s
                   ORDER BY COALESCE(ot.term_start, ot.term_start_year::TEXT) DESC LIMIT %s OFFSET %s""",
                (office_id, limit, offset),
            )
        elif office_id is not None:
            cur = conn.execute(
                """SELECT ot.id, ot.office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          o.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   JOIN offices o ON o.id = ot.office_id
                   LEFT JOIN countries c ON c.id = o.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   WHERE ot.office_id = %s
                   ORDER BY COALESCE(ot.term_start, ot.term_start_year::TEXT) DESC LIMIT %s OFFSET %s""",
                (office_id, limit, offset),
            )
        elif _has_hierarchy_terms(conn):
            cur = conn.execute(
                """SELECT ot.id, ot.office_details_id AS office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          od.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   LEFT JOIN office_details od ON od.id = ot.office_details_id
                   LEFT JOIN source_pages sp ON sp.id = od.source_page_id
                   LEFT JOIN countries c ON c.id = sp.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   ORDER BY ot.scraped_at DESC LIMIT %s OFFSET %s""",
                (limit, offset),
            )
        else:
            cur = conn.execute(
                """SELECT ot.id, ot.office_id, ot.individual_id, ot.party_id, ot.district,
                          ot.term_start, ot.term_end, ot.term_start_year, ot.term_end_year,
                          ot.term_start_imprecise, ot.term_end_imprecise,
                          ot.wiki_url, ot.scraped_at,
                          o.name AS office_name, c.name AS country,
                          p.party_name AS party_display
                   FROM office_terms ot
                   JOIN offices o ON o.id = ot.office_id
                   LEFT JOIN countries c ON c.id = o.country_id
                   LEFT JOIN parties p ON p.id = ot.party_id
                   ORDER BY ot.scraped_at DESC LIMIT %s OFFSET %s""",
                (limit, offset),
            )
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        if own_conn:
            conn.close()
