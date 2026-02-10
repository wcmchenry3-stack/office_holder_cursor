"""Quick office config test: verify table exists and configured columns are in range."""

import requests
from bs4 import BeautifulSoup

from src.db import offices as db_offices
from src.scraper.wiki_fetch import WIKIPEDIA_REQUEST_HEADERS, wiki_url_to_rest_html_url
from src.scraper.table_cache import get_table_html_cached

TIMEOUT = 10


def test_office_config(office_row: dict) -> tuple[bool, str]:
    """
    Fetch the office URL, parse tables, and verify table_no is in range and
    configured column indices (link, term_start, term_end, party, district) are in range.
    Returns (True, "OK") or (False, "error message"). Uses cached table HTML when available.
    """
    url = (office_row.get("url") or "").strip()
    if not url:
        return (False, "No URL configured")

    table_config = db_offices.office_row_to_table_config(office_row)
    table_no = int(table_config.get("table_no", 1))

    result = get_table_html_cached(url, table_no, refresh=False)
    if "error" in result:
        return (False, result["error"])

    table_html = result.get("html") or ""
    if not table_html.strip():
        return (False, "No table HTML")
    soup = BeautifulSoup(table_html, "html.parser")
    tables = soup.find_all("table")
    if not (1 <= table_no <= len(tables)):
        return (False, f"Table not found (table_no={table_no}, found {len(tables)} tables)")
    target_table = tables[table_no - 1]
    rows = target_table.find_all("tr")[1:]  # exclude header
    if not rows:
        return (False, "No data rows")

    first_row = rows[0]
    cells = first_row.find_all(["td", "th"])
    num_cols = len(cells)

    # Required: link_column must be >= 0 and in range
    link_col = table_config.get("link_column", -1)
    if link_col < 0:
        return (False, "Link column not configured")
    if link_col >= num_cols:
        return (False, f"Link column (index {link_col}) out of range (row has {num_cols} columns)")

    # Optional columns: if configured (>= 0), must be in range
    for name, col in [
        ("term_start", table_config.get("term_start_column", -1)),
        ("term_end", table_config.get("term_end_column", -1)),
        ("party", table_config.get("party_column", -1)),
        ("district", table_config.get("district_column", -1)),
    ]:
        if col >= 0 and col >= num_cols:
            return (False, f"Column {name} (index {col}) out of range (row has {num_cols} columns)")

    return (True, "OK")


def get_raw_table_preview(
    url: str,
    table_no: int = 1,
    max_rows: int = 10,
) -> dict | None:
    """
    Fetch URL (or use cache), find the table by table_no (1-based), return first max_rows as raw cell text.
    Returns { "table_no", "num_tables", "rows": [[cell1, cell2, ...], ...] } or None on fetch error.
    """
    url = (url or "").strip()
    if not url:
        return None
    result = get_table_html_cached(url, table_no, refresh=False)
    if "error" in result:
        return None
    table_html = result.get("html") or ""
    num_tables = result.get("num_tables", 0)
    if not table_html.strip():
        return {"table_no": table_no, "num_tables": num_tables, "rows": []}
    soup = BeautifulSoup(table_html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return {"table_no": table_no, "num_tables": num_tables, "rows": []}
    target = tables[0]
    data_rows = target.find_all("tr")[1:][:max_rows]
    rows: list[list[str]] = []
    for row in data_rows:
        cells = row.find_all(["td", "th"])
        rows.append([(c.get_text(strip=True) or "").replace("\n", " ").strip() for c in cells])
    return {"table_no": table_no, "num_tables": num_tables, "rows": rows}


def get_all_tables_preview(
    url: str,
    max_rows_per_table: int = 10,
    confirm_threshold: int = 10,
    confirmed: bool = False,
) -> dict:
    """
    Fetch URL and return all tables, each with first max_rows_per_table rows as raw cell text.
    If num_tables > confirm_threshold and not confirmed, return only { "num_tables", "confirm_required": True }.
    Otherwise return { "num_tables", "tables": [ {"table_index": 1, "rows": [[...], ...] }, ... ] }.
    On fetch error returns { "num_tables": 0, "error": "..." }.
    """
    url = (url or "").strip()
    if not url:
        return {"num_tables": 0, "error": "No URL"}
    fetch_url = wiki_url_to_rest_html_url(url) or url
    try:
        resp = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return {"num_tables": 0, "error": f"HTTP {resp.status_code}"}
        html_content = resp.text
    except requests.RequestException as e:
        return {"num_tables": 0, "error": str(e)}
    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")
    num_tables = len(tables)
    if num_tables > confirm_threshold and not confirmed:
        return {"num_tables": num_tables, "confirm_required": True}
    result_tables: list[dict] = []
    for i, target in enumerate(tables):
        data_rows = target.find_all("tr")[1:][:max_rows_per_table]
        rows: list[list[str]] = []
        for row in data_rows:
            cells = row.find_all(["td", "th"])
            rows.append([(c.get_text(strip=True) or "").replace("\n", " ").strip() for c in cells])
        result_tables.append({"table_index": i + 1, "rows": rows})
    return {"num_tables": num_tables, "tables": result_tables}


def get_table_html(url: str, table_no: int = 1, refresh: bool = False, use_full_page: bool = False) -> dict:
    """
    Return the outer HTML of the table at 1-based table_no. Uses local cache unless refresh=True.
    use_full_page: if True, fetch full page URL (table indices match full Wikipedia page); default False uses REST API.
    Returns { "table_no", "num_tables", "html": "<table>...</table>" } or { "error": "..." }.
    """
    return get_table_html_cached(url, table_no, refresh=refresh, use_full_page=use_full_page)


def get_table_header_from_html(table_html: str) -> list[tuple[int, str]]:
    """
    Parse table HTML (single <table>...</table>) and return the header row as
    list of (0-based index, cell text) for the first <tr> (th or td).
    Returns [] on parse failure or empty table.
    """
    if not (table_html or "").strip():
        return []
    try:
        soup = BeautifulSoup(table_html, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return []
        target = tables[0]
        rows = target.find_all("tr")
        if not rows:
            return []
        cells = rows[0].find_all(["td", "th"])
        return [(i, (c.get_text(strip=True) or "").strip()) for i, c in enumerate(cells)]
    except Exception:
        return []
