# -*- coding: utf-8 -*-
"""Table parsing: DataCleanup, Offices, Biography. In-repo implementation (sample file ignored).

Wikipedia API compliance: all HTTP requests to the Wikimedia REST API are made via
src/scraper/wiki_fetch.py, which sets a descriptive User-Agent header (HTTP_USER_AGENT)
per Wikimedia API etiquette. No direct HTTP requests are made from this module.
See: https://www.mediawiki.org/wiki/API:Etiquette#The_User-Agent_header
"""

import copy
import logging
import re
from datetime import datetime, date
from urllib.parse import urlparse, quote

from requests.exceptions import RequestException as _RequestException

from src.scraper.wiki_fetch import (
    WIKI_BASE_URL,
    WIKI_DOMAIN,
    canonical_holder_url,
    normalize_wiki_url,
    wiki_session,
    wiki_throttle,
    wiki_url_to_rest_html_url,
)
from bs4 import BeautifulSoup
from dateutil.parser import parse

logger = logging.getLogger(__name__)


def _parse_date(s):
    """Parse term date string to date or None. Handles None, 'Invalid date', and YYYY-MM-DD."""
    if not s or (isinstance(s, str) and s.strip() in ("", "Invalid date")):
        return None
    try:
        if isinstance(s, date):
            return s
        if isinstance(s, datetime):
            return s.date()
        parsed = parse(str(s).strip(), default=datetime(2000, 1, 1))
        return parsed.date() if parsed else None
    except (ValueError, TypeError):
        return None


def _dates_from_cell_data_sort_value(cell):
    """Extract (start_str, end_str) from data-sort-value in cell (Wikipedia sortable tables). Returns (None, None) if none found."""
    if cell is None:
        return (None, None)
    # Find all elements with data-sort-value (e.g. "000000001964-01-06")
    vals = []
    for el in cell.find_all(attrs={"data-sort-value": True}):
        v = (el.get("data-sort-value") or "").strip()
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", v)
        if m:
            vals.append(m.group(0))
    if not vals:
        return (None, None)
    if len(vals) == 1:
        return (vals[0], vals[0])
    return (vals[0], vals[-1])


def parse_infobox_role_key_query(raw_query: str) -> tuple[list[str], list[str]]:
    """Parse infobox role query into (includes, excludes) with strict quoting.

    Accepted examples:
    - "judge"
    - "judge" "associate justice" -"chief judge" -"senior judge"
    """
    expr = (raw_query or "").strip()
    if not expr:
        return ([], [])

    includes: list[str] = []
    excludes: list[str] = []
    pos = 0
    n = len(expr)

    def _normalize_role_text(text: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())).strip()

    while pos < n:
        while pos < n and expr[pos].isspace():
            pos += 1
        if pos >= n:
            break
        neg = False
        if expr[pos] == "-":
            neg = True
            pos += 1
            if pos >= n:
                raise ValueError(
                    "Invalid infobox role key: trailing '-' must be followed by a quoted term."
                )
        if expr[pos] == '"':
            pos += 1
            end = expr.find('"', pos)
            if end == -1:
                raise ValueError("Invalid infobox role key: unclosed quoted term.")
            raw_term = expr[pos:end]
            pos = end + 1
        else:
            # Backward compatibility: allow unquoted single-token includes like: judge -"chief judge"
            # Excludes still must be quoted to avoid ambiguous parsing.
            if neg:
                raise ValueError('Invalid infobox role key: excludes must be quoted (use -"term").')
            end = pos
            while end < n and not expr[end].isspace():
                end += 1
            raw_term = expr[pos:end]
            pos = end
        term = _normalize_role_text(raw_term)
        if not term:
            raise ValueError("Invalid infobox role key: empty quoted terms are not allowed.")
        if neg:
            excludes.append(term)
        else:
            includes.append(term)

    return (includes, excludes)


def _emit_merged_run(run, years_only, out):
    """Merge a run of consecutive term rows into one row; append to out."""
    if not run:
        return
    merged = copy.deepcopy(run[0])
    if len(run) == 1:
        out.append(merged)
        return
    if years_only:
        starts = [r.get("Term Start Year") for r in run if r.get("Term Start Year") is not None]
        ends = [r.get("Term End Year") for r in run if r.get("Term End Year") is not None]
        merged["Term Start Year"] = min(starts) if starts else run[0].get("Term Start Year")
        merged["Term End Year"] = max(ends) if ends else run[-1].get("Term End Year")
        merged["Term Start"] = run[0].get("Term Start")
        merged["Term End"] = run[-1].get("Term End")
    else:
        dates_start = [_parse_date(r.get("Term Start")) for r in run]
        dates_end = [_parse_date(r.get("Term End")) for r in run]
        ds = min(d for d in dates_start if d is not None) if any(dates_start) else None
        de = max(d for d in dates_end if d is not None) if any(dates_end) else None
        merged["Term Start"] = ds.strftime("%Y-%m-%d") if ds else run[0].get("Term Start")
        merged["Term End"] = de.strftime("%Y-%m-%d") if de else run[-1].get("Term End")
        merged["Term Start Year"] = ds.year if ds else run[0].get("Term Start Year")
        merged["Term End Year"] = de.year if de else run[-1].get("Term End Year")
    out.append(merged)


def _emit_parse_failure(
    reporter,
    function_name: str,
    exc: Exception,
    html_snippet: str = "",
    wiki_url: str | None = None,
    office_name: str | None = None,
    date_str: str | None = None,
) -> None:
    """Collect one parser failure into the reporter's buffer (fire-and-forget).

    This helper is called from inside except-blocks. It wraps the reporter call
    in its own try/except so that a broken reporter never propagates an error
    into the parser. If reporter is None (tests / no env vars), this is a no-op.
    """
    if reporter is None:
        return
    import traceback as _tb

    # Lazy import avoids circular dependency (table_parser ← runner ← ai_office_builder ← runner)
    from src.services.parse_error_reporter import ParseFailure

    failure = ParseFailure(
        function_name=function_name,
        error_type=type(exc).__name__,
        traceback_str=_tb.format_exc(),
        wiki_url=wiki_url,
        office_name=office_name,
        html_snippet=(str(html_snippet) or "")[:2000],
        date_str=date_str,
    )
    try:
        reporter.collect(failure)
    except Exception as _collect_err:
        logger.debug("_emit_parse_failure: reporter.collect() failed: %s", _collect_err)
        # Silenced — reporter must never crash the parser


class DataCleanup:

    def __init__(self, reporter=None):

        self._reporter = reporter

    def format_date(self, date_str):

        # List of regex patterns to match different date formats
        date_patterns = [
            r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",  # Matches full Month DD, YYYY
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),\s+(\d{4})\b",  # Matches abbreviated Month DD, YYYY
            r"\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b",  # DD Month YYYY (e.g. 18 June 1798)
            r"\((\d{4}-\d{2}-\d{2})\)",  # Matches YYYY-MM-DD within parentheses
        ]

        # List of datetime formats corresponding to the regex patterns
        datetime_formats = [
            "%B %d, %Y",  # Correct format for full Month names
            "%b %d, %Y",  # Correct format for abbreviated Month names
            "%d %B %Y",  # DD Month YYYY (e.g. 18 June 1798)
            "%Y-%m-%d",  # Corresponds to YYYY-MM-DD
        ]

        logger.debug(f"starting format_date {date_str}")

        # Attempt to find and parse a date using each pattern
        for pattern, date_format in zip(date_patterns, datetime_formats):
            logger.debug(f"starting to search for pattern: {pattern} and date format {date_format}")

            match = re.search(pattern, date_str)
            logger.debug(f"identified match {match}")

            if match:
                date_part = match.group(0)
                logger.debug(f"date part {date_part}")

                try:
                    # Parse the found date using the corresponding datetime format
                    parsed_date = datetime.strptime(date_part, date_format)
                    logger.debug(f"parsed_date {parsed_date}")

                    return parsed_date.strftime("%Y-%m-%d")

                except (ValueError, TypeError, IndexError) as e:
                    logger.warning(f"Value error {e} found in {date_str} while running format_date")
                    continue  # Try the next pattern if parsing fails

        # Never use today for missing parts: year-only must not become a full date; use imprecise path instead.
        s = date_str.strip() if date_str else ""
        if re.match(r"^\s*(17|18|19|20)\d{2}\s*$", s):
            logger.debug(
                "year-only date in format_date; returning Invalid date so year+imprecise path is used"
            )
            return "Invalid date"

        # Fallback: dateutil.parser for flexible parsing (e.g. "18 June 1798", "4 March 1809"). Use fixed default so missing day/month are never today.
        _parse_default = datetime(2000, 1, 1)
        try:
            parsed = parse(s, default=_parse_default)
            if parsed:
                return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError) as e:
            _emit_parse_failure(
                self._reporter,
                "DataCleanup.format_date",
                e,
                html_snippet=s,
                date_str=s,
            )

        # If no date is found or if it cannot be parsed, return a default value
        logger.debug(f"invalid date in {date_str}")
        return "Invalid date"

    def parse_date_info(self, date_str, date_type):

        logger.debug(f"Running parse_date_info: {date_str} {date_type}")

        # remove footnotes and parenthesis from date fields
        date_str = self.remove_footnote(date_str)
        date_str = self.remove_parenthesis(date_str)
        date_str = re.sub(r"\s*\[[^\]]*O\.?S\.[^\]]*\]", "", date_str, flags=re.IGNORECASE).strip()
        date_str = re.sub(
            r"(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2})\s+(\d{4}\b)",
            r"\1, \2",
            date_str,
        )
        date_range_patterns = [
            r"\s*([A-Za-z]+ \d{1,2}, \d{4})(, (.*?))? – ([A-Za-z]+ \d{1,2}, \d{4})(, (.*?))?\)?"
        ]  # why is this not in format_date?

        compiled_patterns = [re.compile(pattern) for pattern in date_range_patterns]

        # Define delimiters for ranges and special cases to check after splitting
        delimiters = ["–", "-", " —<br/>", " to ", " – ", ", to "]
        special_cases = ["incumbent", "n/a", "present", "Incumbent"]

        try:
            if date_type == "both":
                for delimiter in delimiters:
                    logger.debug(f"found delimiter {delimiter}")
                    if delimiter in date_str:

                        parts = date_str.split(delimiter)
                        logger.debug(f"date parts {parts}")
                        start_str = parts[0].strip()
                        end_str = parts[1].strip() if len(parts) > 1 else "N/A"
                        # Strip "In office " prefix so "In office 1989" parses as 1989
                        if start_str.lower().startswith("in office "):
                            start_str = start_str[10:].strip()
                        if end_str.lower().startswith("in office "):
                            end_str = end_str[10:].strip()
                        logger.debug(f"date parts start: {start_str} end: {end_str}")

                        # Strip trailing footnote refs (e.g. "Incumbent [ t ]") before special-case check
                        start_clean = re.sub(r"\s*\[\s*\w+\s*\]\s*$", "", start_str).strip()
                        end_clean = re.sub(r"\s*\[\s*\w+\s*\]\s*$", "", end_str).strip()
                        # Check if start or end part matches special cases
                        start_date = (
                            start_clean
                            if start_clean.lower() in special_cases
                            else self.format_date(start_str)
                        )
                        end_date = (
                            end_clean
                            if end_clean.lower() in special_cases
                            else self.format_date(end_str)
                        )

                        logger.debug(f"Range detected - Start: {start_date}, End: {end_date}")
                        return (
                            (start_date, end_date)
                            if date_type == "both"
                            else (start_date, "N/A") if date_type == "start" else ("N/A", end_date)
                        )

        except (ValueError, IndexError, TypeError) as e:
            logger.warning(f"Error {e} found in {date_str} while parsing by delimiter {delimiter}")

        if date_type == "both":
            for pattern in compiled_patterns:
                logger.debug(f"trying to search compiled patterns {pattern}")
                try:
                    logger.debug(f"pattern {pattern}")
                    logger.debug(f"date_str {date_str}")
                    # Ensure date_str is a string; remove .text if date_str is already a string
                    match = pattern.search(date_str)
                    logger.debug(f"match {match}")
                    if match:
                        logger.debug(f"Found match within pattern {pattern.pattern}")
                        start_date_str = match.group(1)  # The start date
                        end_date_str = match.group(4)  # The end date
                        _def = datetime(2000, 1, 1)  # Never use today for missing parts

                        def _safe_parse(s):
                            if not s or not s.strip():
                                return "Invalid date"
                            if re.match(r"^\s*(17|18|19|20)\d{2}\s*$", s.strip()):
                                return "Invalid date"
                            try:
                                return parse(s.strip(), default=_def).strftime("%Y-%m-%d")
                            except (ValueError, TypeError):
                                return "Invalid date"

                        start_date = _safe_parse(start_date_str)
                        end_date = _safe_parse(end_date_str)
                        logger.debug(f"Birth date: {start_date}, Death date: {end_date}")
                        if date_type == "both":
                            return (start_date, end_date)
                        return (start_date, "N/A") if date_type == "start" else ("N/A", end_date)
                except (ValueError, IndexError, TypeError) as e:
                    logger.warning(
                        f"Value Error {e} found in {date_str} while parsing by date pattern"
                    )

        # If no range delimiter is found, or if the date_type is 'start' or 'end', process the whole string
        if date_type in ["start", "end"]:

            try:
                logger.debug("no delimiter found")
                date = (
                    date_str
                    if date_str.lower().strip() in special_cases
                    else self.format_date(date_str)
                )
                logger.debug(f"Single date or special case: {date}")
                return date if date_type == "start" else date if date_type == "end" else date

            except (ValueError, IndexError, TypeError) as e:
                logger.warning(f"Error {e} found in {date_str} while parsing by date pattern")

        logger.debug(f"invalid date found in {date_str}")
        return "Invalid date", "Invalid date"

    def parse_year_range(self, text: str):
        """Parse a year range from table text (e.g. '1966–1974', '2009–present'). Returns (start_year int|None, end_year int|None)."""
        if not text or not isinstance(text, str):
            return (None, None)
        text = self.remove_footnote(text)
        text = text.strip()
        year_match = re.search(r"\b(17|18|19|20)\d{2}\b", text)
        if not year_match:
            return (None, None)
        delimiters = ["–", "-", " —<br/>", " to ", " – ", ", to "]
        present_cases = ["incumbent", "n/a", "present", "Incumbent"]
        for d in delimiters:
            if d in text:
                parts = text.split(d, 1)
                start_str = (parts[0].strip() if parts else "").strip()
                end_str = (parts[1].strip() if len(parts) > 1 else "").strip()
                start_m = re.search(r"\b(17|18|19|20)\d{2}\b", start_str)
                start_year = int(start_m.group(0)) if start_m else None
                if not end_str or end_str.lower().strip() in present_cases:
                    return (start_year, None)
                end_m = re.search(r"\b(17|18|19|20)\d{2}\b", end_str)
                end_year = int(end_m.group(0)) if end_m else None
                return (start_year, end_year)
        sy = int(year_match.group(0))
        return (sy, sy)

    def find_link_and_data_columns(self, row, max_column_index=None, min_column_index=None):
        # Dynamic identification of the link column and subsequent data columns.
        # If max_column_index is set (0-based), only consider cells up to that index so we never
        # pick a link from a non-data column (e.g. President column) when it appears after term dates.
        for i, cell in enumerate(row):
            if min_column_index is not None and i < min_column_index:
                continue
            if max_column_index is not None and i > max_column_index:
                break
            try:
                cell_str = str(
                    cell
                )  # Convert BeautifulSoup object or similar to string, if necessary
            except Exception as e:
                raise
            # Wiki article link: absolute path, relative (./), or full URL
            has_absolute = 'href="/wiki/' in cell_str
            has_relative = (
                'href="./' in cell_str
                and 'href="./File:' not in cell_str
                and 'href="./Special:' not in cell_str
            )
            has_full_url = (
                f"{WIKI_DOMAIN}/wiki/" in cell_str
                and "/wiki/File:" not in cell_str
                and "/wiki/Special:" not in cell_str
            )
            has_wiki_link = has_absolute or has_relative or has_full_url
            has_fragment_link = '#"' in cell_str or "#cite_note" in cell_str
            # Exclude file/special links in any form
            has_file_link = (
                'href="/wiki/File:' in cell_str
                or 'href="./File:' in cell_str
                or "/wiki/File:" in cell_str
                or 'href="./Special:' in cell_str
                or "/wiki/Special:" in cell_str
            )
            if has_wiki_link and not has_file_link and not has_fragment_link:
                logger.debug(f"Wiki link (not a file link) found at column {i}: {cell}")
                return i  # Return the index of the column containing the link
        logger.debug(f"Wiki did not find a link in {row}")
        return None  # If no matching link column is found, or data structure is different

    def remove_footnote(self, content, extract_text=False, strip_text=False):

        logger.debug("removing footnote")

        """
      Remove footnote references from text and optionally extract and strip text from BeautifulSoup objects.

      Args:
      - content: The content from which to remove footnotes. This can be a string or a BeautifulSoup object.
      - extract_text: Boolean indicating whether to extract text from a BeautifulSoup object.
      - strip_text: Boolean indicating whether to strip the text of leading and trailing whitespace.

      Returns:
      - The cleaned text with footnotes removed.
      """
        try:

            if extract_text and hasattr(content, "get_text"):
                # If content is a BeautifulSoup object and text extraction is requested
                text = content.get_text(strip=strip_text)
            else:
                text = content

            # Remove footnote references
            cleaned_text = re.sub(r"\[\w+\]", "", text)

            if strip_text and not extract_text:
                # If text stripping is requested but text was not extracted (meaning content was already a string)
                cleaned_text = cleaned_text.strip()

            logger.debug(f" removed footnote \n\n before {content} \n\n after {cleaned_text}")

            return cleaned_text

        except (TypeError, ValueError, IndexError) as e:
            logger.warning(f"error {e} parsing footnote")

    def remove_parenthesis(self, content):

        logger.debug("removing parenthesis")

        try:
            cleaned_text = re.sub(r"\([^)]*\)", "", content)

            logger.debug(f" removed parenthesis \n\n before {content} \n\n after {cleaned_text}")

            return cleaned_text

        except (TypeError, ValueError, IndexError) as e:
            logger.warning(f"error {e} parsing parenthesis")


class Offices:

    def __init__(self, biography, data_cleanup, reporter=None):

        self.DataCleanup = data_cleanup
        self.Biography = biography
        self._reporter = reporter

    def _is_valid_wiki_link(self, link):
        if not isinstance(link, str):
            return False
        candidate = link.strip()
        if not candidate or candidate == "No link":
            return False
        if not candidate.startswith(f"{WIKI_BASE_URL}/wiki/"):
            return False
        # Keep ignore_non_links useful for parser junk rows too (e.g. congress/election links).
        if any(re.search(pattern, candidate) for pattern in self.patterns_to_ignore()):
            return False
        if re.search(r"/wiki/[\w%]+_Party(?:_\([^)]*\))?$", candidate):
            return False
        if "/wiki/File:" in candidate or "/wiki/Special:" in candidate:
            return False
        return True

    def _row_matches_filter(self, row, table_config):
        """Return True when row passes optional single-column text filter."""
        filter_col = table_config.get("row_filter_column", -1)
        criteria = (table_config.get("row_filter_criteria") or "").strip()
        if filter_col is None:
            filter_col = -1
        try:
            filter_col = int(filter_col)
        except (TypeError, ValueError):
            filter_col = -1
        if filter_col < 0 or not criteria:
            return True
        cells = row.find_all(["td", "th"])
        if filter_col >= len(cells):
            return False
        cell_text = (
            re.sub(r"\s+", " ", cells[filter_col].get_text(" ", strip=True) or "").strip().lower()
        )
        target = re.sub(r"\s+", " ", criteria).strip().lower()
        return target in cell_text

    def process_table(
        self,
        html_content,
        table_config,
        office_details,
        url,
        party_list,
        progress_callback=None,
        max_rows=None,
        run_cache=None,
    ):
        logger.debug(f"---------------\n\n Processing table with config: {table_config}")

        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")
        tables = soup.find_all("table")

        # Check if specified table number is within bounds
        if not (0 <= table_config["table_no"] - 1 < len(tables)):
            logger.warning("Table number out of bounds.")
            return []

        target_table = tables[table_config["table_no"] - 1]
        rows = target_table.find_all("tr")[1:]  # Exclude the header row
        if max_rows is not None and max_rows >= 0:
            rows = rows[:max_rows]
        accumulated_results = []
        total_rows = len(rows)
        report_infobox = table_config.get("find_date_in_infobox") and progress_callback is not None

        # Per-table cache so we only call find_term_dates once per wiki_link (same person in multiple rows)
        self._infobox_cache = {}
        # tracks the previous entry --> this helps the rowspan function track
        previous_row_wiki_link = None
        previous_row_district = None
        previous_row_party = None
        table_rows_val = table_config.get("table_rows", 4)
        term_end_col = table_config.get("term_end_column", -1)

        for row_index, row in enumerate(rows):
            try:
                if report_infobox:
                    progress_callback(
                        row_index + 1, total_rows, f"Processing {row_index + 1} of {total_rows}"
                    )
                cells = row.find_all(["td", "th"])
                logger.debug(f"cells from process table {cells}")

                cells_td = row.find_all("td")
                if not self._row_matches_filter(row, table_config):
                    continue
                row_results = self.parse_table_row(
                    row,
                    table_config,
                    office_details,
                    url,
                    previous_row_wiki_link,
                    previous_row_district,
                    previous_row_party,
                    party_list,
                    run_cache=run_cache,
                )
                if row_results and table_config.get("ignore_non_links"):
                    row_results = [
                        r for r in row_results if self._is_valid_wiki_link(r.get("Wiki Link"))
                    ]
                logger.debug(f"results from process table {row_results}")
                appended = bool(row_results)
                if row_results:
                    accumulated_results.extend(row_results)

                    # Update the "previous row" variables from the last result (one row can yield multiple term rows)
                    last_result = row_results[-1]
                    previous_row_wiki_link = last_result.get("Wiki Link")
                    previous_row_district = last_result.get("District")
                    previous_row_party = last_result.get("Party")

            except (
                IndexError,
                AttributeError,
                TypeError,
                ValueError,
                UnicodeEncodeError,
                UnicodeDecodeError,
            ) as e:
                logger.warning(f" found error {e} when processing row {row_index}")

        if table_config.get("consolidate_rowspan_terms"):
            accumulated_results = self._consolidate_rowspan_terms(accumulated_results, table_config)

        return accumulated_results

    def _consolidate_rowspan_terms(self, rows: list, table_config: dict) -> list:
        """Group rows by holder (Wiki Link or _name_from_table), sort by term start, merge consecutive terms (gap <= 1 day or year)."""
        if not rows:
            return rows
        years_only = table_config.get("years_only", False)

        def holder_key(r):
            link = (r.get("Wiki Link") or "").strip()
            if link and link != "No link":
                return link
            return "_name_:" + (r.get("_name_from_table") or "")

        def sort_key(r):
            if years_only:
                sy = r.get("Term Start Year")
                ey = r.get("Term End Year")
                return (sy if sy is not None else 0, ey if ey is not None else 0)
            start = r.get("Term Start")
            end = r.get("Term End")
            ds = _parse_date(start)
            de = _parse_date(end)
            return (ds or date(9999, 12, 31), de or date(9999, 12, 31))

        def gap_consecutive(prev, curr):
            if years_only:
                pey = prev.get("Term End Year")
                csy = curr.get("Term Start Year")
                if pey is None or csy is None:
                    return False
                return (csy - pey) <= 1
            pe = _parse_date(prev.get("Term End"))
            cs = _parse_date(curr.get("Term Start"))
            if pe is None or cs is None:
                return False
            return (cs - pe).days <= 1

        grouped = {}
        for r in rows:
            k = holder_key(r)
            grouped.setdefault(k, []).append(copy.deepcopy(r))

        out = []
        for group in grouped.values():
            group.sort(key=sort_key)
            run = [group[0]]
            for i in range(1, len(group)):
                if gap_consecutive(run[-1], group[i]):
                    run.append(group[i])
                else:
                    _emit_merged_run(run, years_only, out)
                    run = [group[i]]
            _emit_merged_run(run, years_only, out)
        return out

    def parse_table_row(
        self,
        row,
        table_config,
        office_details,
        url,
        previous_row_wiki_link,
        previous_row_district,
        previous_row_party,
        party_list,
        run_cache=None,
    ):
        """
        This function parses out the specific table.
        """

        logger.debug(
            f"---------------\n\n table config in parse_table_row: \n\n {table_config} \n\n row: {row} "
        )

        logger.debug(
            f"previous values: \n wiki_link: {previous_row_wiki_link} \n district: {previous_row_district} \n party: {previous_row_party}"
        )

        cells = row.find_all(["td", "th"])
        logger.debug(f" cells {cells} \n\n")

        # total columns primarily works with right_to_left function
        total_columns = len(cells)
        logger.debug(f"total columns {total_columns}")

        # create a duplicate version of table_config. This duplicate version could be changed by other functions, without updating table_config
        table_config_to_parse = copy.deepcopy(table_config)
        # Keep original term_end_column for short-row continuation check (before RTL or dynamic_parse change it)
        term_end_column_orig = table_config.get("term_end_column", -1)

        logger.debug(
            f"original table config {table_config} \n table config to parse {table_config_to_parse}"
        )

        row_data = {}

        """
      max_columns = max(table_config_to_parse['link_column'], table_config_to_parse['party_column'], table_config_to_parse['term_start_column'], table_config_to_parse['district_column'])
      believe this is now obsolete (replaced by range_total_columns?)
      """

        # variable to control rowspan function
        found_rowspan = False

        term_start, term_end = (
            "Invalid date",
            "Invalid date",
        )  # Default values before calling extract_term_dates
        district = "No district"
        party = "No party"

        district_no_value = "No district"
        party_no_value = "No party"

        # Initialize the data structure for the row's results.
        results = {
            "Country": office_details["office_country"],
            "Level": office_details["office_level"],
            "Branch": office_details["office_branch"],
            "Department": office_details["office_department"],
            "Office Name": office_details["office_name"],
            "State": office_details["office_state"],
            "Office Notes": office_details["office_notes"],
            "Wiki Link": None,
            "Party": None,
            "District": None,
            "Term Start": None,
            "Term End": None,
        }

        # columns_RTL function reads columns in reverse. This is primarily used for senate offices - which has the url at the rightmost end.

        if table_config_to_parse["read_columns_right_to_left"] == True:
            table_config_to_parse = self.process_columns_right_to_left(
                table_config_to_parse, total_columns
            )
        else:
            logger.debug("not running read_columns_right_to_left")

        # dynamic parse function to dynamically determine other columns based on url column
        # When parse_rowspan and this row has too few cells for the full table layout, skip dynamic_parse so we don't return None on failure; treat as continuation below.
        # Use term_end_column_orig (before RTL/dynamic_parse) so short-row detection is reliable.
        is_short_continuation = (
            table_config_to_parse.get("parse_rowspan")
            and previous_row_wiki_link
            and term_end_column_orig >= 0
            and len(cells) <= term_end_column_orig
        )
        if table_config_to_parse["run_dynamic_parse"] == True and not is_short_continuation:
            success, table_config_to_parse = self.process_dynamic_parse(
                cells, table_config_to_parse
            )
            logger.debug(
                f"table config return in process_table_row after dynamic parse {table_config_to_parse} \n success: {success}"
            )

            if not success:
                # Skip processing this row as the link column wasn't found
                return None

        else:
            logger.debug("not running run_dynamic_parse")

        # Ensure there are enough cells to avoid IndexError - do not use for rowspan, as it often will cause an error
        if (
            len(cells) < table_config_to_parse["table_rows"]
            and table_config_to_parse["parse_rowspan"] == False
        ):  # Adjust this number based on the expected minimum number of cells
            logger.warning("issue with table rows")
            return None  # or some default data structure
        # With rowspan, skip only rows that have too few cells to parse any term (need at least 2 to try; continuation rows often have 3)
        if table_config_to_parse["parse_rowspan"] == True and len(cells) < 2:
            logger.warning("skipping rowspan continuation row (too few cells)")
            return None
        # Skip rows that don't have enough columns to include term_end (e.g. President-only continuation rows). When parse_rowspan, continuation rows have fewer cells so column indices don't align—skip check.
        term_end_col = table_config_to_parse.get("term_end_column", -1)
        if (
            term_end_col >= 0
            and len(cells) <= term_end_col
            and not table_config_to_parse.get("parse_rowspan")
            and not table_config_to_parse.get("find_date_in_infobox")
        ):
            logger.warning("skipping row (too few columns for term_end)")
            return None

        logger.debug("column numbers determined, extracting information")

        # range total columns determines the number of columns in cells. This is used for the rowspan function.
        range_total_columns = range(total_columns)
        logger.debug(f"range of columns: {range_total_columns}")

        # Before iterating over columns, ensure you have the initial data or use last known values
        wiki_link = self.find_link(table_config_to_parse, office_details, cells, party_list)
        logger.debug(f"wiki link results before iteration: {wiki_link} ")

        # update wiki link on second iteration of rospan and beyond
        if wiki_link == None and table_config_to_parse["parse_rowspan"] == True:
            logger.debug("No wiki link found in row")
            wiki_link = previous_row_wiki_link
            logger.debug(f"Adding previous link {previous_row_wiki_link} as link {wiki_link}")
            found_rowspan = True
        # When parse_rowspan and this row has too few cells for the configured term columns, treat as continuation row
        # (otherwise dynamic_parse may find a link in a date cell and we skip rowspan term loop, then IndexError on cells[term_end_column])
        if (
            table_config_to_parse.get("parse_rowspan")
            and previous_row_wiki_link
            and term_end_col >= 0
            and len(cells) <= term_end_col
        ):
            wiki_link = previous_row_wiki_link
            found_rowspan = True
        if wiki_link is None or (wiki_link or "").strip() == "":
            wiki_link = "No link"

        """
      The following three logic chains deal with the rowspan function. Rowspan works for office holders with multiple terms.
      Often the url is listed in the first column, but not repeated in subsequent column. The rowspan helps keep track of previous values.
      When rowspan == true, it reviews each column in cells to find the value. If not value is found, it will apply previous value.
      When rowspan == false. it will simply call the appropriate function based on the column_no.
      """

        # figure out party (skip and keep null when party_ignore)
        # Only treat as rowspan continuation when row is truly short; otherwise avoid carrying stale values across full rows.
        if found_rowspan and not is_short_continuation:
            logger.debug(
                "row has no link but is not a short continuation; skipping row to avoid stale carry-over"
            )
            return None

        if table_config_to_parse.get("party_ignore"):
            party = None
            logger.debug("party_ignore: not extracting party")
        elif found_rowspan == True:
            logger.debug("running parse rowspan on party")
            for col_no in range_total_columns:

                logger.debug(f"running parse iteration {col_no} with party")
                party = self.extract_party(
                    wiki_link,
                    cells,
                    office_details,
                    table_config_to_parse,
                    col_no,
                    party_list,
                    party_no_value,
                )
                if party not in (None, "No Party"):
                    logger.debug(f"found results for party {party}")
                    break
                else:
                    party = previous_row_party
                    logger.debug(
                        f"could not find party, so keeping old version {previous_row_party}"
                    )
        else:
            party = self.extract_party(
                wiki_link,
                cells,
                office_details,
                table_config_to_parse,
                None,
                party_list,
                party_no_value,
            )
            logger.debug(f"no rowspan, results for party: {party}")

        # figure out district (override when district_ignore or district_at_large)
        if table_config_to_parse.get("district_ignore"):
            district = "No District"
            logger.debug("district_ignore: using No District")
        elif table_config_to_parse.get("district_at_large"):
            district = "At-Large"
            logger.debug("district_at_large: using At-Large")
        elif found_rowspan == True:
            logger.debug("running parse rowspan on district")
            for col_no in range_total_columns:

                logger.debug(f"running parse iteration {col_no} with district")
                district = self.extract_district(
                    wiki_link,
                    cells,
                    office_details,
                    table_config_to_parse,
                    col_no,
                    district_no_value,
                )
                if district not in (None, "No district"):
                    logger.debug(f"found results for district {district}")
                    break
                else:
                    district = previous_row_district
                    logger.debug(
                        f"could not find district, so keeping old version {previous_row_district}"
                    )
        else:
            district = self.extract_district(
                wiki_link, cells, office_details, table_config_to_parse, None, district_no_value
            )
            logger.debug(f"no rowspan, results for district: {district}")

        # figure out term dates
        term_start_year = None
        term_end_year = None
        if found_rowspan == True:
            logger.debug("running parse rowspan on term")
            term_tuples = []
            best_single = None  # fallback when no range found

            # For true short continuation rows we may need to probe each cell because configured indices no longer align.
            # For non-short rows, avoid scanning every column (can accidentally parse election-year columns as terms).
            if is_short_continuation:
                for col_no in range_total_columns:

                    logger.debug(f"running parse iteration {col_no} with term")
                    raw = self.extract_term_dates(
                        wiki_link,
                        cells,
                        office_details,
                        table_config_to_parse,
                        col_no,
                        url,
                        district,
                        run_cache=run_cache,
                    )
                    if isinstance(raw, list):
                        term_tuples = raw
                        break
                    term_start, term_end, term_start_year, term_end_year = raw

                    ignore_terms = (None, "Invalid date")
                    if table_config_to_parse.get("years_only"):
                        if term_start_year is not None or term_end_year is not None:
                            logger.debug(
                                f"found years-only term start year {term_start_year} end year {term_end_year}"
                            )
                            term_tuples = [(term_start, term_end, term_start_year, term_end_year)]
                            break
                    elif term_start not in ignore_terms and term_end not in ignore_terms:
                        logger.debug(
                            f"found results for term start {term_start} and term end {term_end}"
                        )
                        # Prefer a range (start != end); otherwise keep as fallback and try next column
                        if term_start != term_end:
                            term_tuples = [(term_start, term_end, term_start_year, term_end_year)]
                            break
                        if best_single is None:
                            best_single = (term_start, term_end, term_start_year, term_end_year)
            else:
                raw = self.extract_term_dates(
                    wiki_link,
                    cells,
                    office_details,
                    table_config_to_parse,
                    None,
                    url,
                    district,
                    run_cache=run_cache,
                )
                term_tuples = raw if isinstance(raw, list) else [raw]

            # For short rowspan rows: try start from one cell, end from next (e.g. 3-cell row has start col0, end col1)
            n_cells = len(cells)
            if (
                is_short_continuation
                and (
                    not term_tuples
                    or (len(term_tuples) == 1 and term_tuples[0][0] == term_tuples[0][1])
                )
                and n_cells >= 2
            ):
                for sc, ec in [(0, 1), (1, 2)]:
                    if ec >= n_cells:
                        continue
                    raw = self.extract_term_dates(
                        wiki_link,
                        cells,
                        office_details,
                        table_config_to_parse,
                        (sc, ec),
                        url,
                        district,
                        run_cache=run_cache,
                    )
                    if isinstance(raw, list):
                        continue
                    term_start, term_end, _, _ = raw
                    if (
                        term_start
                        and term_end
                        and term_start != "Invalid date"
                        and term_end != "Invalid date"
                        and term_start != term_end
                    ):
                        term_tuples = [(term_start, term_end, None, None)]
                        break
            if not term_tuples and best_single is not None:
                term_tuples = [best_single]
            if not term_tuples:
                term_tuples = [(None, None, None, None)]
        else:
            raw_terms = self.extract_term_dates(
                wiki_link,
                cells,
                office_details,
                table_config_to_parse,
                None,
                url,
                district,
                run_cache=run_cache,
            )
            term_tuples = raw_terms if isinstance(raw_terms, list) else [raw_terms]
            logger.debug(f"\n\n no rowspan, got {len(term_tuples)} term(s) from extract_term_dates")

        link_column = table_config_to_parse.get("link_column", 0)
        name_from_table = None
        if 0 <= link_column < len(cells):
            first_a = cells[link_column].find("a")
            name_from_table = first_a.get_text(strip=True) if first_a else None
            if name_from_table is None:
                name_from_table = cells[link_column].get_text(strip=True) or None
        infobox_debug = getattr(self, "_last_infobox_items", None)
        if infobox_debug is not None:
            self._last_infobox_items = None  # Consume so next row does not inherit
        results_list = []
        for term_start, term_end, term_start_year, term_end_year in term_tuples:
            row_dict = {
                "Country": office_details["office_country"],
                "Level": office_details["office_level"],
                "Branch": office_details["office_branch"],
                "Department": office_details["office_department"],
                "Office Name": office_details["office_name"],
                "State": office_details["office_state"],
                "Office Notes": office_details["office_notes"],
                "Wiki Link": wiki_link,
                "Term Start": term_start,
                "Term End": term_end,
                "Term Start Year": term_start_year,
                "Term End Year": term_end_year,
                "Party": party,
                "District": district,
            }
            row_dict["_name_from_table"] = name_from_table
            last_dead_link = getattr(self.Biography, "_last_dead_link", False)
            row_dict["_dead_link"] = bool(last_dead_link and wiki_link and wiki_link != "No link")
            if last_dead_link:
                self.Biography._last_dead_link = False  # Consume so next row does not inherit
            if infobox_debug is not None:
                row_dict["Infobox items"] = (
                    "\n".join(infobox_debug)
                    if isinstance(infobox_debug, list)
                    else str(infobox_debug)
                )
                last_bio = getattr(self.Biography, "_last_bio_details", None)
                if last_bio is not None:
                    row_dict["_bio_details"] = last_bio
                    self.Biography._last_bio_details = (
                        None  # Attach only to first result row for this person
                    )
            results_list.append(row_dict)
        for results in results_list:
            logger.debug(f"results {results}")
        return results_list

    def patterns_to_ignore(self):
        """
        List of patterns to ignore if found by the find_link function.
        """

        return (
            r"/wiki/\d{1,3}(th|st|nd|rd)_United_States_Congress",
            r"/wiki/[^/]*_(\d{1,2}(th|st|nd|rd)|at-large)_congressional_district",
            r"/wiki/\d{4}_[\w\d]+_elections_in_[\w\d]+",
            r"/wiki/\d{4}_[\w\d]+_election",
            r"/wiki/(19|20)\d{2}(_\d)?$",  # year links e.g. /wiki/2024 from date columns
        )

    def column_present(self, column_index, cells):
        """
        Check if the specified column index is within the bounds of the cells in the current row.

        :param column_index: The 1-based index of the column to check.
        :param cells: The list of cells (<td> elements) in the current row.
        :return: True if the column is present, False otherwise.
        """
        # Adjust for zero-based indexing

        return column_index < len(cells)

    def find_link(self, table_config_to_parse, office_details, cells, party_list):

        logger.debug("find link")

        link_column = table_config_to_parse["link_column"]
        country = office_details["office_country"]
        alt_links = set((table_config_to_parse.get("alt_links") or []))

        _reporter = self._reporter  # capture for closure

        def _path_from_full_url(full_url: str) -> str:
            try:
                parsed = urlparse(full_url or "")
                return parsed.path or ""
            except Exception as e:
                _emit_parse_failure(
                    _reporter,
                    "Offices._path_from_full_url",
                    e,
                    html_snippet=full_url or "",
                )
                return ""

        def _candidate_from_link_tag(link_tag):
            href = (link_tag.get("href") or "") if link_tag else ""
            if "/File:" in href:
                return None
            raw_href = href.strip()
            # Relative hrefs like "./Title" produce invalid URLs; normalize to /wiki/Title
            if raw_href.startswith("./"):
                path = "/wiki/" + raw_href[2:].lstrip("/")
            elif raw_href.startswith("/wiki/"):
                path = raw_href
            elif raw_href.startswith("/"):
                path = "/wiki" + raw_href
            else:
                path = "/wiki/" + raw_href
            full_url = normalize_wiki_url(f"{WIKI_BASE_URL}{path}") or f"{WIKI_BASE_URL}{path}"
            full_path = _path_from_full_url(full_url)
            has_fragment = "#" in full_url
            should_ignore = (
                any(re.search(pattern, full_url) for pattern in self.patterns_to_ignore())
                or has_fragment
            )
            party_links = {p.get("link") for p in party_list.get(country, []) if p.get("link")}
            if full_path and full_path in alt_links:
                return None
            if should_ignore or full_url in party_links:
                return None
            return full_url

        logger.debug(f"country in find_link: {country}")

        had_links_in_configured_col = False
        if self.column_present(link_column, cells):
            logger.debug("url column present")
            link_tags = cells[link_column].find_all("a", href=True)
            had_links_in_configured_col = len(link_tags) > 0
            try:
                for link_tag in link_tags:
                    logger.debug(f"looking at {link_tag} in {link_tags}")
                    full_url = _candidate_from_link_tag(link_tag)
                    if full_url:
                        logger.debug(f"URL passed all checks: {full_url}")
                        return full_url

            except (ValueError, TypeError, IndexError, AttributeError) as e:
                logger.warning(f"found error when finding url for {full_url} in {cells}")

        # Fallback: wrong link column often points at footnote-only cells.
        # Only run fallback when configured column had link markup but no acceptable candidate.
        if not had_links_in_configured_col:
            return None

        # Probe holder side of row based on configured direction to avoid unrelated link columns.
        term_start_col = table_config_to_parse.get("term_start_column")
        try:
            term_start_col = int(term_start_col) if term_start_col is not None else -1
        except (TypeError, ValueError):
            term_start_col = -1
        rtl = bool(table_config_to_parse.get("read_columns_right_to_left"))
        if rtl:
            start_ci = (
                min(max(0, term_start_col), max(0, len(cells) - 1)) if term_start_col >= 0 else 0
            )
            probe_indices = range(start_ci, len(cells))
        else:
            max_probe_col = term_start_col if term_start_col >= 0 else len(cells)
            max_probe_col = min(max_probe_col, len(cells))
            probe_indices = range(max_probe_col)
        for ci in probe_indices:
            if ci == link_column:
                continue
            for link_tag in cells[ci].find_all("a", href=True):
                full_url = _candidate_from_link_tag(link_tag)
                if full_url:
                    logger.debug(f"find_link fallback matched col {ci}: {full_url}")
                    return full_url

        return None

    def extract_term_dates(
        self,
        wiki_link,
        cells,
        office_details,
        table_config_to_parse,
        parse_row_no,
        url,
        district,
        run_cache=None,
    ):

        logger.debug("running extract terms")
        self._last_infobox_items = (
            None  # Cleared each call; set only when find_date_in_infobox used
        )

        # parse_row_no == None means the rowspan function is working and needs to iterate. Otherwise, choose the column_no.
        # parse_row_no can be a tuple (start_col, end_col) for adjacent-column date range in short rowspan rows.
        if parse_row_no is None:
            term_start_column = table_config_to_parse["term_start_column"]
            term_end_column = table_config_to_parse["term_end_column"]
        elif isinstance(parse_row_no, (list, tuple)) and len(parse_row_no) == 2:
            term_start_column, term_end_column = parse_row_no[0], parse_row_no[1]
        else:
            term_start_column = parse_row_no
            term_end_column = parse_row_no

        # Extract and format the term start and end dates

        try:
            start_cell = cells[term_start_column] if 0 <= term_start_column < len(cells) else None
            end_cell = cells[term_end_column] if 0 <= term_end_column < len(cells) else None
            logger.debug(f"start date column {term_start_column} results: {start_cell}")
            logger.debug(f"end date column {term_end_column} results: {end_cell}")

            # Years only: table has year ranges only; do not call infobox. Parse year range and leave dates unpopulated.
            if table_config_to_parse.get("years_only") == True:
                cell_text = start_cell.get_text(separator=" ").strip() if start_cell else ""
                term_start_year, term_end_year = self.DataCleanup.parse_year_range(cell_text)
                logger.debug(
                    f" years_only: parsed year range {term_start_year}–{term_end_year} from {cell_text!r}"
                )
                return (None, None, term_start_year, term_end_year)

            # Find date in infobox: fetch full dates from person's bio; collect all matching terms from infobox
            if table_config_to_parse["find_date_in_infobox"] == True:
                logger.debug(
                    f" parse_table_row found TRUE in find_date_in_infobox \n\n about to process {start_cell}"
                )
                existing_dates_lookup = table_config_to_parse.get("existing_dates_lookup") or {}
                if existing_dates_lookup:
                    cell_text_start = (
                        start_cell.get_text(separator=" ").strip() if start_cell else ""
                    )
                    same_column = term_start_column == term_end_column
                    table_start_year, table_end_year = self.DataCleanup.parse_year_range(
                        cell_text_start
                    )
                    if not same_column and end_cell is not None:
                        cell_text_end = end_cell.get_text(separator=" ").strip()
                        _sy_end, _ey_end = self.DataCleanup.parse_year_range(cell_text_end)
                        if _ey_end is not None or _sy_end is not None:
                            table_end_year = _ey_end if _ey_end is not None else _sy_end
                    key = (canonical_holder_url(wiki_link), table_start_year, table_end_year)
                    if key in existing_dates_lookup:
                        existing = existing_dates_lookup[key]
                        return [existing]
                cache = getattr(self, "_infobox_cache", None)
                if cache is not None and wiki_link in cache:
                    cached = cache[wiki_link]
                    terms_list = cached["terms"]
                    infobox_items = cached["infobox_items"]
                    self._last_infobox_items = infobox_items
                    self.Biography._last_bio_details = cached.get("bio_details")
                else:
                    terms_list, infobox_items = self.Biography.find_term_dates(
                        wiki_link,
                        url,
                        table_config_to_parse,
                        office_details,
                        district,
                        run_cache=run_cache,
                    )
                    self._last_infobox_items = infobox_items  # For debug export
                    if cache is not None:
                        cache[wiki_link] = {
                            "terms": terms_list,
                            "infobox_items": infobox_items,
                            "bio_details": getattr(self.Biography, "_last_bio_details", None),
                        }
                logger.debug(f" find_term_dates returned {len(terms_list)} term(s) from infobox")
                # When infobox had no dates (placeholder), use table years only for this record (same as "table has years only")
                if (
                    len(terms_list) == 1
                    and terms_list[0][0] == "YYYY-00-00"
                    and terms_list[0][1] == "YYYY-00-00"
                ):
                    cell_text_start = (
                        start_cell.get_text(separator=" ").strip() if start_cell else ""
                    )
                    same_column = term_start_column == term_end_column
                    cell_text_end = (
                        end_cell.get_text(separator=" ").strip()
                        if (not same_column and end_cell is not None)
                        else None
                    )
                    term_start_year, term_end_year = self.DataCleanup.parse_year_range(
                        cell_text_start
                    )
                    # When start and end are in different columns, parse end cell for term_end_year instead of reusing start
                    if not same_column and cell_text_end is not None:
                        _sy_end, _ey_end = self.DataCleanup.parse_year_range(cell_text_end)
                        term_end_year = _ey_end if _ey_end is not None else _sy_end
                    logger.debug(
                        f" find_date_in_infobox: no infobox dates; using table years only for this record {term_start_year}–{term_end_year} from {cell_text_start!r}"
                    )
                    return [(None, None, term_start_year, term_end_year)]
                return [(s, e, None, None) for (s, e) in terms_list]

            # determine what to do if the term_start and term_end appear in the same columns
            if term_start_column == term_end_column:
                logger.debug("parse_table_row found start and end dates in same column")
                logger.debug(f" cell with date {start_cell}")
                cell = start_cell
                cell_text = cell.get_text(separator=" ").strip() if cell else ""
                logger.debug(f" cell with date with separator {cell_text}")
                term_start, term_end = self.DataCleanup.parse_date_info(
                    cell_text, "both"
                )  # Use separator to handle <br/>
                # Fallback: Wikipedia sortable tables often put dates in data-sort-value when visible text is template/empty
                if (
                    not term_start
                    or term_start == "Invalid date"
                    or not term_end
                    or term_end == "Invalid date"
                ) and cell:
                    ds, de = _dates_from_cell_data_sort_value(cell)
                    if ds and de:
                        term_start, term_end = ds, de
                # Fallback: misconfigured term column on some tables; search row for a true date range cell.
                if (
                    not term_start
                    or term_start == "Invalid date"
                    or not term_end
                    or term_end == "Invalid date"
                ):
                    for i, c in enumerate(cells):
                        if i == term_start_column:
                            continue
                        txt = c.get_text(separator=" ").strip() if c else ""
                        if not txt:
                            continue
                        if not any(d in txt for d in ["–", " - ", " to "]):
                            continue
                        cand_start, cand_end = self.DataCleanup.parse_date_info(txt, "both")
                        if (
                            cand_start
                            and cand_end
                            and cand_start != "Invalid date"
                            and cand_end != "Invalid date"
                            and cand_start != cand_end
                        ):
                            logger.debug(f"extract_term_dates fallback: using range from col {i}")
                            term_start, term_end = cand_start, cand_end
                            break
                return (term_start, term_end, None, None)

            logger.debug("parse_table_row found start and end dates not in same column")
            logger.debug(f" cell with start date {cells[term_start_column]}")
            logger.debug(f" cell with end date {cells[term_end_column].get_text(strip=True)}")
            term_start = self.DataCleanup.parse_date_info(
                cells[term_start_column].get_text(strip=True), "start"
            )
            term_end = self.DataCleanup.parse_date_info(
                cells[term_end_column].get_text(strip=True), "end"
            )
            # Fallback: data-sort-value when visible text is template/empty (e.g. continuation rows)
            if (not term_start or term_start == "Invalid date") and term_start_column < len(cells):
                ds, _ = _dates_from_cell_data_sort_value(cells[term_start_column])
                if ds:
                    term_start = ds
            if (not term_end or term_end == "Invalid date") and term_end_column < len(cells):
                _, de = _dates_from_cell_data_sort_value(cells[term_end_column])
                if de:
                    term_end = de
            logger.debug(f" finished extracting term start and end: {term_start} {term_end}  ")
            return (term_start, term_end, None, None)

        except (ValueError, TypeError, AttributeError, IndexError) as e:
            logger.warning(f" error {e} when parsing {wiki_link}")
            return ("Invalid date", "Invalid date", None, None)

    def extract_party(
        self,
        wiki_link,
        cells,
        office_details,
        table_config_to_parse,
        parse_row_no,
        party_list,
        no_value_return,
    ):

        logger.debug(f"running extract_party \n table config {table_config_to_parse}")

        # parse_row_no == None means the rowspan function is working and needs to iterate. Otherwise, choose the column_no.
        if parse_row_no == None:
            party_column = table_config_to_parse["party_column"]
        else:
            party_column = parse_row_no

        country = office_details["office_country"]
        use_party_link = table_config_to_parse["party_link"]

        # Ensure the column exists
        if party_column >= len(cells):
            return no_value_return

        cell = cells[party_column]

        if use_party_link == True:
            link_tags = cell.find_all("a", href=True)

            try:
                for link_tag in link_tags:
                    full_url_unclean = f"{WIKI_BASE_URL}{link_tag['href']}"
                    full_url = self.DataCleanup.remove_footnote(full_url_unclean)
                    logger.debug(
                        f"full url in extract_party: {full_url} /n country in party list: {country in party_list}"
                    )

                    # Check if the URL is in the party_list for the given country
                    if country in party_list:
                        for party_info in party_list[country]:
                            logger.debug(
                                f"Checking party: {party_info['name']} with link: {party_info['link']} \n url: {full_url}"
                            )
                            if full_url == party_info["link"]:
                                logger.debug(f"Match found for party: {party_info['name']}")
                                return party_info["name"]
            except (ValueError, IndexError, TypeError) as e:
                logger.warning(f"found error {e} in party_extract when searching for party link")

            # Fallback: when party_link=True but cell has no link (or no match), match by text
            if country in party_list:
                party_text = cell.get_text(strip=True)
                if party_text:
                    try:
                        for party_info in party_list[country]:
                            if re.search(re.escape(party_info["name"]), party_text, re.IGNORECASE):
                                logger.debug(
                                    f"Match found for party (text fallback): {party_info['name']} in {party_text!r}"
                                )
                                return party_info["name"]
                    except (ValueError, TypeError, IndexError) as e:
                        _emit_parse_failure(
                            self._reporter,
                            "Offices.find_link.party_text_fallback",
                            e,
                            html_snippet=party_text or "",
                        )

        if use_party_link != True and country in party_list:
            party_text = cells[party_column].get_text(strip=True)
            logger.debug(f"Extracted party text: {party_text}")

            try:
                for party_info in party_list[country]:
                    # Using case-insensitive search to improve matching chances
                    if re.search(re.escape(party_info["name"]), party_text, re.IGNORECASE):
                        logger.debug(
                            f"Match found for party: {party_info['name']} using text: {party_text}"
                        )
                        return party_info["name"]
                logger.debug(f"No party match found in party list for text: {party_text}")

            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"Error {e} while searching for party text")

        return no_value_return

    def extract_district(
        self, wiki_link, cells, office_details, table_config_to_parse, parse_row_no, no_value_return
    ):

        logger.debug(f"running extract_district \n table config {table_config_to_parse}")

        # parse_row_no == None means the rowspan function is working and needs to iterate. Otherwise, choose the column_no.
        if parse_row_no == None:
            district_column = table_config_to_parse["district_column"]
        else:
            district_column = parse_row_no

        logger.debug(
            f"district column: {district_column} \n table config: {table_config_to_parse['district_column']} \n parse row no {parse_row_no}"
        )

        # Initialize district to 'No district' by default
        district = "No district"

        # Ensure the district_column index is within bounds
        if 0 <= district_column < len(cells):
            district_text = cells[district_column].get_text(strip=True)

            # Check if district_text matches the pattern for ordinal numbers (1st, 2nd, 3rd, etc.) or "At-large"
            ordinal_pattern = r"\b\d+(st|nd|rd|th)\b"
            at_large_pattern = r"At-large"
            territory_pattern = r"Territory"

            # If district_text matches the ordinal pattern or is exactly "At-large"
            if (
                re.search(ordinal_pattern, district_text)
                or re.match(at_large_pattern, district_text, re.IGNORECASE)
                or re.match(territory_pattern, district_text, re.IGNORECASE)
            ):
                district = district_text
            else:
                logger.debug(f"District text '{district_text}' does not match expected patterns.")
        else:
            logger.debug("District column index is out of bounds.")

        logger.debug(f"Extracted district info: {district}")

        return district

    def process_columns_right_to_left(self, table_config_to_parse, total_columns):

        logger.debug("running process_columns right to left")

        # RTL should mirror LTR column counting: user enters columns from the right edge,
        # but parser still indexes cells left->right. So col 0 (form: 1) means rightmost cell.
        # Keep negative columns (e.g. -1 = unconfigured) unchanged.
        def _rtl_to_ltr_index(col_index):
            if col_index is None:
                return None
            if col_index < 0:
                return col_index
            return total_columns - (col_index + 1)

        link_column_old = table_config_to_parse.get("link_column")
        party_column_old = table_config_to_parse.get("party_column")
        term_start_column_old = table_config_to_parse.get("term_start_column")
        term_end_column_old = table_config_to_parse.get("term_end_column")
        district_column_old = table_config_to_parse.get("district_column")

        link_column = _rtl_to_ltr_index(link_column_old)
        party_column = _rtl_to_ltr_index(party_column_old)
        term_start_column = _rtl_to_ltr_index(term_start_column_old)
        term_end_column = _rtl_to_ltr_index(term_end_column_old)
        district_column = _rtl_to_ltr_index(district_column_old)

        logger.debug(
            f"new link column {link_column} after rtl conversion from {link_column_old} with total {total_columns}"
        )
        logger.debug(
            f"party column {party_column} after rtl conversion from {party_column_old} with total {total_columns}"
        )
        logger.debug(
            f"term start {term_start_column} after rtl conversion from {term_start_column_old} with total {total_columns}"
        )
        logger.debug(
            f"term end {term_end_column} after rtl conversion from {term_end_column_old} with total {total_columns}"
        )
        logger.debug(
            f"district column {district_column} after rtl conversion from {district_column_old} with total {total_columns}"
        )

        table_config_to_parse["link_column"] = link_column
        table_config_to_parse["party_column"] = party_column
        table_config_to_parse["term_start_column"] = term_start_column
        table_config_to_parse["term_end_column"] = term_end_column
        table_config_to_parse["district_column"] = district_column

        return table_config_to_parse

    def process_dynamic_parse(self, cells, table_config_to_parse):

        logger.debug("running process_dynamic_parse")

        link_column = table_config_to_parse["link_column"]
        party_column = table_config_to_parse["party_column"]
        term_start_column = table_config_to_parse["term_start_column"]
        term_end_column = table_config_to_parse["term_end_column"]
        district_column = table_config_to_parse["district_column"]

        row_len = len(cells)
        if row_len == 0:
            return False, table_config_to_parse

        # Explicit bounds override default behavior.
        dynamic_link_min_col = table_config_to_parse.get("dynamic_link_min_col")
        dynamic_link_max_col = table_config_to_parse.get("dynamic_link_max_col")
        if dynamic_link_min_col is not None or dynamic_link_max_col is not None:
            min_link_col = dynamic_link_min_col if dynamic_link_min_col is not None else 0
            max_link_col = (
                dynamic_link_max_col if dynamic_link_max_col is not None else (row_len - 1)
            )
        else:
            # Backward compatibility: bound by term_end and parse direction.
            if table_config_to_parse.get("read_columns_right_to_left") == True:
                min_link_col = term_end_column if term_end_column is not None else 0
                max_link_col = row_len - 1
            else:
                min_link_col = 0
                max_link_col = (
                    term_end_column
                    if term_end_column is not None and term_end_column >= 0
                    else (row_len - 1)
                )

        # Clamp to row bounds.
        min_link_col = max(0, min(int(min_link_col), row_len - 1))
        max_link_col = max(0, min(int(max_link_col), row_len - 1))

        if min_link_col > max_link_col:
            min_link_col, max_link_col = 0, row_len - 1

        link_column_old = link_column
        link_column_result = self.DataCleanup.find_link_and_data_columns(
            cells,
            max_column_index=max_link_col,
            min_column_index=min_link_col,
        )

        used_fallback_scan = False
        if link_column_result is None:
            used_fallback_scan = True
            link_column_result = self.DataCleanup.find_link_and_data_columns(
                cells, max_column_index=row_len - 1, min_column_index=0
            )

        # Stop loop if no link is found
        if link_column_result is None:
            table_no = table_config_to_parse.get("table_no", "unknown")
            logger.debug(
                f"DynamicParse table={table_no}: no link found (bounds {min_link_col}..{max_link_col}, fallback full-row {'failed' if used_fallback_scan else 'not-run'}). Row skipped."
            )
            # Return False indicating the link column was not found, alongside original columns
            table_config_to_parse["link_column"] = link_column
            table_config_to_parse["party_column"] = party_column
            table_config_to_parse["term_start_column"] = term_start_column
            table_config_to_parse["term_end_column"] = term_end_column
            table_config_to_parse["district_column"] = district_column

            logger.debug(f"returning table config {table_config_to_parse}")

            return False, table_config_to_parse

        link_column = link_column_result

        logger.debug(
            f"Link column determined dynamically: {link_column} old link {link_column_old} \n table config {table_config_to_parse}"
        )

        # When link_column_old was -1 (unconfigured), other columns are absolute indices — use as-is
        if link_column_old < 0:
            table_config_to_parse["link_column"] = link_column
            table_config_to_parse["party_column"] = party_column
            table_config_to_parse["term_start_column"] = term_start_column
            table_config_to_parse["term_end_column"] = term_end_column
            table_config_to_parse["district_column"] = district_column
            return True, table_config_to_parse

        # Calculate differences based on old link column
        diff_term_start = term_start_column - link_column_old
        logger.debug(f" term_start current: {term_start_column} diff: {diff_term_start} ")
        diff_term_end = term_end_column - link_column_old
        logger.debug(f" term_end current: {term_end_column} diff: {diff_term_end} ")
        diff_district = district_column - link_column_old if district_column not in [0, 1000] else 0
        logger.debug(f" district current: {district_column} diff: {diff_district} ")
        diff_party = party_column - link_column_old if party_column > 0 else 0
        logger.debug(f" party current: {party_column} diff: {diff_party} ")

        # Update columns based on differences
        party_column = link_column + diff_party if diff_party != 0 else party_column
        logger.debug(f"update party column: {party_column}")
        district_column = link_column + diff_district if diff_district != 0 else district_column
        logger.debug(f"update district column: {district_column}")
        term_start_column = link_column + diff_term_start
        logger.debug(f"update term start column: {term_start_column}")
        term_end_column = link_column + diff_term_end
        logger.debug(f"update term end column: {term_end_column}")

        """
    #Error handling when there are issues with parsing
    party_column = "N/A" if party_column > len(cells) else party_column
    district_column = "N/A" if district_column > len(cells) else district_column
    term_start_column = "N/A" if term_start_column > len(cells) else term_start_column
    term_end_column = "N/A" if term_end_column > len(cells) else term_end_column
    logger.debug( f" expected cells {len(cells)} " )
    """

        # Return True indicating successful parsing, alongside updated columns
        table_config_to_parse["link_column"] = link_column
        table_config_to_parse["party_column"] = party_column
        table_config_to_parse["term_start_column"] = term_start_column
        table_config_to_parse["term_end_column"] = term_end_column
        table_config_to_parse["district_column"] = district_column

        logger.debug(f"table config at the end of dynamic parse: \n {table_config_to_parse} \n\n")

        return True, table_config_to_parse


class Biography:

    def __init__(self, data_cleanup, reporter=None):

        self.DataCleanup = data_cleanup
        self._reporter = reporter

    def parse_infobox(self, infobox):
        """
        This function searches for information in the biography's infobox.
        This function does not yet find birth_place and death_place.
        """

        logger.debug("start running parse_infobox")

        details = {
            "full_name": None,
            "name": None,
            "birth_date": None,
            "birth_place": None,
            "death_date": None,
            "death_place": None,
        }

        # Look for name within the infobox
        name_row = infobox.find("th", {"class": "infobox-above"})
        if name_row:
            details["name"] = self.DataCleanup.remove_footnote(name_row.get_text(strip=True))

        # Nickname is not usually a nickname, but rather the full name
        nickname_div = infobox.find("div", {"class": "nickname"})
        if nickname_div:
            details["full_name"] = self.DataCleanup.remove_footnote(
                nickname_div.get_text(strip=True)
            )
        else:
            details["full_name"] = details.get("name", None)

        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")

            if th and td:
                if "Born" in th.text:
                    logger.debug(th.text)
                    birth_date_text = td.get_text(" ", strip=True)
                    birth_date = self.DataCleanup.parse_date_info(birth_date_text, "start")
                    details["birth_date"] = birth_date

                elif "Died" in th.text:
                    logger.debug(th.text)
                    death_date_text = td.get_text(" ", strip=True)
                    death_date = self.DataCleanup.parse_date_info(death_date_text, "end")
                    details["death_date"] = death_date

        logger.debug("completd running parse_infobox")
        return details

    def parse_first_paragraph(self, paragraph):

        logger.debug("running first paragraphy method")

        logger.debug(f"running first paragraph \n\n {paragraph}")

        """
      This function searches for information in the biography's first paragraphy.
      This function does not yet find birth_place and death_place.
      """

        details = {
            "full_name": None,
            "name": None,
            "birth_date": None,
            "birth_place": None,
            "death_date": None,
            "death_place": None,
        }

        # Find bold text for full name
        bold_text = paragraph.find("b")
        if bold_text:
            details["full_name"] = self.DataCleanup.remove_footnote(bold_text.text)
            logger.debug(f" full name {details['full_name']} ")

        details["birth_date"], details["death_date"] = self.DataCleanup.parse_date_info(
            paragraph, "both"
        )

        logger.debug(f"first paragraph details {details}")
        return details

    def biography_extract(self, wiki_link, run_cache=None):

        logger.debug("-------- \n\n Running biography extract")

        normalized_link = normalize_wiki_url(wiki_link) or wiki_link
        fetch_url = wiki_url_to_rest_html_url(normalized_link) or normalized_link
        try:
            _cached_html_be = run_cache.get(fetch_url) if run_cache is not None else None
            if _cached_html_be is not None:
                response = type("_R", (), {"status_code": 200, "text": _cached_html_be})()
            else:
                wiki_throttle()
                response = wiki_session().get(fetch_url, timeout=30)
                if response.status_code == 200 and run_cache is not None:
                    run_cache.set(fetch_url, response.text)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, "html.parser")

                infobox = soup.find(
                    "table", {"class": ["infobox vcard", "infobox biography vcard"]}
                )
                details = None
                if infobox:
                    details = self.parse_infobox(infobox)
                else:
                    first_paragraph = soup.find("p")
                    if first_paragraph:
                        details = self.parse_first_paragraph(first_paragraph)
                    else:
                        return {}

                if details:
                    details["page_path"] = urlparse(wiki_link).path.split("/")[-1].strip()
                    if not details["full_name"]:
                        details["full_name"] = details.get("name", "")
                else:
                    details = {"page_path": urlparse(wiki_link).path.split("/")[-1].strip()}
                return details
            else:
                logger.error(
                    f"Failed to fetch biography URL with status code: {response.status_code}"
                )
                return {}
        except _RequestException as e:
            logger.error(f"Request failed: {e}")
            return {}

    def find_term_dates(
        self, wiki_link, url, table_config_to_parse, office_details, district, run_cache=None
    ):

        logger.debug(f"running find_term_dates \n url value {url}")

        """
      Replink == true is used for US representative tables with only years in the table, such as New Jersey.
      This function searches the infobox of the biographuies for dates.
      """

        state = office_details["office_state"].replace(" ", "_")
        encoded_state = quote(state)
        district = district.lower()

        # Build list of partial URLs to try (main and/or alt_links); then build match_candidates from all
        urls_to_try = []
        if table_config_to_parse["rep_link"] == True:
            urls_to_try = ["/wiki/United_States_House_of_Representatives"]
            logger.debug(f"Running find_term_dates for {wiki_link} with congressional URL")
        else:
            alt_links = table_config_to_parse.get("alt_links") or []
            alt_ok = bool(alt_links)
            if alt_ok:
                urls_to_try = [
                    (p if p.startswith("/") else "/wiki/" + p.lstrip("/"))
                    for p in alt_links
                    if (p or "").strip()
                ]
                if table_config_to_parse.get("alt_link_include_main"):
                    urls_to_try = urls_to_try + [urlparse(url).path]
                logger.debug(
                    f"Running find_term_dates for {wiki_link} with alt_links %r"
                    % (urls_to_try[:3],)
                )
            if not urls_to_try:
                urls_to_try = [urlparse(url).path]
                logger.debug(
                    f"Running find_term_dates for {wiki_link} with office URL {urls_to_try[0]}"
                )

        current_office_holder = ["assumed office", "incumbent", "invalid date"]

        infobox_items = []  # For debug export: what we found in the infobox
        self._last_bio_details = None
        self._last_dead_link = False
        fetch_url = wiki_url_to_rest_html_url(wiki_link) or wiki_link
        try:
            _cached_html_ftd = run_cache.get(fetch_url) if run_cache is not None else None
            if _cached_html_ftd is not None:
                response = type("_R", (), {"status_code": 200, "text": _cached_html_ftd})()
            else:
                wiki_throttle()
                response = wiki_session().get(fetch_url, timeout=30)
                if response.status_code == 200 and run_cache is not None:
                    run_cache.set(fetch_url, response.text)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, "html.parser")
                infobox = soup.find(
                    "table", {"class": ["infobox vcard", "infobox biography vcard"]}
                )

                if infobox:
                    logger.debug(f"Found infobox \n {infobox}")

                    def _normalize_infobox_href(href: str) -> str:
                        if not href:
                            return ""
                        h = (href or "").strip()
                        if h.startswith("./"):
                            return "/wiki/" + h[2:].lstrip("/")
                        if h.startswith("/wiki/"):
                            return h
                        if h.startswith("/"):
                            return "/wiki" + h
                        return "/wiki/" + h

                    def _slug_variants_for_path(partial_url: str) -> set:
                        """Return set of lowercased strings to match (path, slug, slug_alt, slug_alt2)."""
                        s = set()
                        path = (partial_url or "").strip()
                        if not path:
                            return s
                        path_lower = path.lower()
                        s.add(path_lower)
                        office_slug = path.rstrip("/").split("/")[-1]
                        if office_slug:
                            s.add(office_slug.lower())
                        office_slug_alt = None
                        office_slug_alt2 = None
                        _slug_to_check = office_slug
                        if office_slug.startswith("List_of_"):
                            _slug_to_check = office_slug[len("List_of_") :]
                            office_slug_alt = _slug_to_check
                        if "_of_" in _slug_to_check:
                            parts = _slug_to_check.rsplit("_of_", 1)
                            prefix, state_part = parts[0], parts[1]
                            office_slug_alt2 = state_part + "_" + prefix
                            if office_slug.startswith("List_of_") and prefix:
                                _segments = prefix.split("_")
                                _title_parts = [
                                    "of" if p.lower() == "of" else (p.title() if p.islower() else p)
                                    for p in _segments
                                ]
                                if _title_parts:
                                    _first = _title_parts[0].lower()
                                    if _first.endswith("ies") and len(_first) > 3:
                                        _title_parts[0] = (_first[:-3] + "y").title()
                                    elif (
                                        _first.endswith("s")
                                        and not _first.endswith("ss")
                                        and len(_first) > 1
                                    ):
                                        _title_parts[0] = (_first[:-1]).title()
                                    _title = "_".join(_title_parts)
                                    if _title != prefix:
                                        office_slug_alt = (
                                            office_slug_alt or prefix + "_of_" + state_part
                                        )
                                        office_slug_alt2 = state_part + "_" + _title
                        if office_slug_alt:
                            s.add(office_slug_alt.lower())
                        if office_slug_alt2:
                            s.add(office_slug_alt2.lower())
                        return s

                    match_candidates = set()
                    for partial_url in urls_to_try:
                        match_candidates |= _slug_variants_for_path(partial_url)

                    role_key_raw = (table_config_to_parse.get("infobox_role_key") or "").strip()
                    role_key = re.sub(r"\s+", " ", role_key_raw.lower())

                    def _normalize_role_text(text: str) -> str:
                        return re.sub(
                            r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
                        ).strip()

                    def _contains_phrase(hay: str, phrase: str) -> bool:
                        if not phrase:
                            return True
                        # `hay` and `phrase` are already normalized to lowercase words/spaces.
                        return re.search(r"(^|\s)" + re.escape(phrase) + r"(\s|$)", hay) is not None

                    role_includes, role_excludes = parse_infobox_role_key_query(role_key_raw)

                    def _role_matches(text: str) -> bool:
                        if not role_key:
                            return True
                        hay = _normalize_role_text(text)
                        if not hay:
                            return False
                        if role_includes and not any(
                            _contains_phrase(hay, inc) for inc in role_includes
                        ):
                            return False
                        for exc in role_excludes:
                            if _contains_phrase(hay, exc):
                                return False
                        return True

                    all_terms = (
                        []
                    )  # Collect all matching term (start, end) from every matching row in the infobox
                    for tr in infobox.find_all("tr"):
                        logger.debug(f"found tr \n {tr}")
                        links = tr.find_all("a", href=True)
                        row_text = tr.get_text(" ", strip=True)
                        link_matches = False
                        for a in links:
                            raw_href = a.get("href", "") if a else ""
                            norm_path = _normalize_infobox_href(raw_href)
                            norm_path_lower = (norm_path or "").lower()
                            if norm_path_lower in match_candidates or (
                                norm_path_lower.rsplit("/", 1)[-1] in match_candidates
                                if norm_path_lower
                                else False
                            ):
                                link_matches = True
                                break
                        role_matches = _role_matches(row_text)
                        if link_matches and role_matches:
                            # Examine the next two sibling rows for date information
                            logger.debug("found match. starting to iterate")
                            tr_cur = tr
                            row_desc = "Office row: %r" % (
                                row_text[:80] + ("..." if len(row_text) > 80 else "")
                            )
                            term_added_this_tr = False
                            for _ in range(2):  # Check the next row and the one after that
                                tr_cur = tr_cur.find_next_sibling("tr") if tr_cur else None
                                if tr_cur:
                                    logger.debug(f"find next tr {tr_cur}")
                                    date_text = tr_cur.get_text(" ", strip=True)
                                    logger.debug(f"date text {date_text}")
                                    try:
                                        _res = self.DataCleanup.parse_date_info(date_text, "both")
                                        start_date = (
                                            _res[0]
                                            if isinstance(_res, (tuple, list)) and len(_res) >= 1
                                            else _res
                                        )
                                        end_date = (
                                            _res[1]
                                            if isinstance(_res, (tuple, list)) and len(_res) >= 2
                                            else "present"
                                        )
                                    except (ValueError, TypeError, IndexError) as e:
                                        start_date, end_date = "Invalid date", "Invalid date"
                                        _emit_parse_failure(
                                            self._reporter,
                                            "Biography.find_term_dates.parse_date_info_both",
                                            e,
                                            html_snippet=str(tr_cur)[:2000] if tr_cur else "",
                                            date_str=date_text,
                                        )
                                    if (
                                        start_date
                                        and end_date
                                        and start_date.lower() not in current_office_holder
                                        and end_date.lower() not in current_office_holder
                                    ):
                                        logger.debug(f"Found term dates: {start_date}, {end_date}")
                                        all_terms.append((start_date, end_date))
                                        term_added_this_tr = True
                                        infobox_items.append(
                                            "%s -> date row: %r -> parsed: %s, %s"
                                            % (row_desc, date_text[:100], start_date, end_date)
                                        )
                                        break  # Found valid dates for this office row; move to next matching tr
                                    try:
                                        _res = self.DataCleanup.parse_date_info(date_text, "start")
                                        if isinstance(_res, (tuple, list)) and len(_res) >= 2:
                                            start_date, end_date = _res[0], _res[1]
                                        elif isinstance(_res, (tuple, list)) and len(_res) >= 1:
                                            start_date, end_date = _res[0], "present"
                                        else:
                                            start_date, end_date = _res, "present"
                                    except (ValueError, TypeError, IndexError) as e:
                                        start_date, end_date = "Invalid date", "Invalid date"
                                        _emit_parse_failure(
                                            self._reporter,
                                            "Biography.find_term_dates.parse_date_info_start",
                                            e,
                                            html_snippet=str(tr_cur)[:2000] if tr_cur else "",
                                            date_str=date_text,
                                        )
                                    if (
                                        start_date
                                        and start_date.lower() not in current_office_holder
                                    ):
                                        logger.debug(f"Found term dates: {start_date}")
                                        all_terms.append((start_date, end_date))
                                        term_added_this_tr = True
                                        infobox_items.append(
                                            "%s -> date row: %r -> parsed: %s, %s"
                                            % (row_desc, date_text[:100], start_date, end_date)
                                        )
                                        break  # Found valid dates for this office row; move to next matching tr
                                    # If dates are invalid, continue to the next sibling row
                            if not term_added_this_tr:
                                infobox_items.append(
                                    "%s -> checked 2 sibling rows; no valid dates" % row_desc
                                )
                        elif link_matches and role_key and not role_matches:
                            infobox_items.append(
                                "Skipped row for role key %r: %r" % (role_key_raw, row_text[:100])
                            )
                    # Single fetch: also collect birth/death from same infobox so runner can skip second fetch
                    details = self.parse_infobox(infobox)
                    details["wiki_url"] = normalize_wiki_url(wiki_link) or wiki_link
                    details["page_path"] = (urlparse(wiki_link).path.split("/")[-1] or "").strip()
                    if not details.get("full_name"):
                        details["full_name"] = details.get("name") or ""
                    self._last_bio_details = details
                    if all_terms:
                        return (
                            all_terms,
                            (
                                infobox_items
                                if infobox_items
                                else ["Infobox: matched rows returned terms above."]
                            ),
                        )

                if not infobox:
                    self._last_bio_details = None
                    infobox_items.append("No infobox in page.")
                elif not infobox_items:
                    infobox_items.append(
                        "Infobox found; no rows matching office link/name%s."
                        % (" + role key" if role_key else "")
                    )
            else:
                self._last_bio_details = None
                infobox_items.append("Failed to fetch page: HTTP %s" % response.status_code)
                if (wiki_link or "").strip() and (wiki_link or "").strip() != "No link":
                    self._last_dead_link = True
        except _RequestException as e:
            self._last_bio_details = None
            logger.error(f"Request failed: {e}")
            infobox_items.append("Request failed: %s" % str(e))
            if (wiki_link or "").strip() and (wiki_link or "").strip() != "No link":
                self._last_dead_link = True

        # Placeholder return: no infobox or no matching terms. Do NOT set dead link here — dead link means
        # the page does not exist (404, request failed). Missing/mismatched infobox means the page exists.
        return (
            [("YYYY-00-00", "YYYY-00-00")],
            infobox_items if infobox_items else ["No dates found (placeholder)."],
        )
