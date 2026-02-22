# -*- coding: utf-8 -*-
"""Table parsing: DataCleanup, Offices, Biography. In-repo implementation (sample file ignored)."""
import copy
import json
import re
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlparse, quote

import requests

from src.scraper.wiki_fetch import WIKIPEDIA_REQUEST_HEADERS, normalize_wiki_url, wiki_url_to_rest_html_url
from bs4 import BeautifulSoup
from dateutil.parser import parse


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


def _emit_merged_run(run, years_only, out):
  """Merge a run of consecutive term rows into one row; append to out."""
  # #region agent log
  _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
  try:
    with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_emit_merged_run", "message": "emit run", "data": {"run_len": len(run), "first_start": run[0].get("Term Start") if run else None, "last_end": run[-1].get("Term End") if run else None}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H5"}) + "\n")
  except Exception:
    pass
  # #endregion
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
  # #region agent log
  try:
    with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_emit_merged_run", "message": "merged output", "data": {"merged_term_start": merged.get("Term Start"), "merged_term_end": merged.get("Term End")}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H5"}) + "\n")
  except Exception:
    pass
  # #endregion
  out.append(merged)


class DataCleanup:

  def __init__(self , logger ):


    self.Logger = logger



  def format_date( self , date_str ):



    # List of regex patterns to match different date formats
    date_patterns = [
    r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b',  # Matches full Month DD, YYYY
    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),\s+(\d{4})\b',  # Matches abbreviated Month DD, YYYY
    r'\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',  # DD Month YYYY (e.g. 18 June 1798)
    r'\((\d{4}-\d{2}-\d{2})\)'  # Matches YYYY-MM-DD within parentheses
    ]

    # List of datetime formats corresponding to the regex patterns
    datetime_formats = [
        '%B %d, %Y',  # Correct format for full Month names
        '%b %d, %Y',  # Correct format for abbreviated Month names
        '%d %B %Y',   # DD Month YYYY (e.g. 18 June 1798)
        '%Y-%m-%d'    # Corresponds to YYYY-MM-DD
    ]




    self.Logger.debug_log( f"starting format_date {date_str}" , True )

    # Attempt to find and parse a date using each pattern
    for pattern, date_format in zip(date_patterns, datetime_formats):
        self.Logger.debug_log( f"starting to search for pattern: {pattern} and date format {date_format}" , True )

        match = re.search(pattern, date_str)
        self.Logger.debug_log( f"identified match {match}" , True )

        if match:
            date_part = match.group(0)
            self.Logger.debug_log( f"date part {date_part}" , True )

            try:
                # Parse the found date using the corresponding datetime format
                parsed_date = datetime.strptime(date_part, date_format)
                self.Logger.debug_log( f"parsed_date {parsed_date}" , True )

                return parsed_date.strftime('%Y-%m-%d')

            except ( ValueError , TypeError , IndexError ) as e:
                self.Logger.log( f"Value error {e} found in {date_str} while running format_date" , True )
                continue  # Try the next pattern if parsing fails


    # Never use today for missing parts: year-only must not become a full date; use imprecise path instead.
    s = date_str.strip() if date_str else ""
    if re.match(r"^\s*(17|18|19|20)\d{2}\s*$", s):
        self.Logger.debug_log("year-only date in format_date; returning Invalid date so year+imprecise path is used", True)
        return "Invalid date"

    # Fallback: dateutil.parser for flexible parsing (e.g. "18 June 1798", "4 March 1809"). Use fixed default so missing day/month are never today.
    _parse_default = datetime(2000, 1, 1)
    try:
        parsed = parse(s, default=_parse_default)
        if parsed:
            return parsed.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass

    # If no date is found or if it cannot be parsed, return a default value
    self.Logger.debug_log( f"invalid date in {date_str}" , True )
    return "Invalid date"


  def parse_date_info( self , date_str, date_type ):


      self.Logger.log( f"Running parse_date_info: {date_str} {date_type}" , True )

      # remove footnotes and parenthesis from date fields
      date_str = self.remove_footnote( date_str )
      date_str = self.remove_parenthesis( date_str )
      date_str = re.sub(r'\s*\[[^\]]*O\.?S\.[^\]]*\]', '', date_str, flags=re.IGNORECASE).strip()
      date_str = re.sub(r'(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2})\s+(\d{4}\b)', r'\1, \2', date_str)
      date_range_patterns = [
          r'\s*([A-Za-z]+ \d{1,2}, \d{4})(, (.*?))? – ([A-Za-z]+ \d{1,2}, \d{4})(, (.*?))?\)?'
      ] # why is this not in format_date?

      compiled_patterns = [re.compile(pattern) for pattern in date_range_patterns]


      # Define delimiters for ranges and special cases to check after splitting
      delimiters = ['–' , '-' , ' —<br/>',  ' to ' , ' – ' , ', to ' ]
      special_cases = ["incumbent", "n/a", "present", "Incumbent"]

      try:
        if date_type == "both":
          for delimiter in delimiters:
            self.Logger.debug_log( f"found delimiter {delimiter}" , True )
            if delimiter in date_str:

                  parts = date_str.split(delimiter)
                  self.Logger.debug_log( f"date parts {parts}" , True )
                  start_str = parts[0].strip()
                  end_str = parts[1].strip() if len(parts) > 1 else 'N/A'
                  # Strip "In office " prefix so "In office 1989" parses as 1989
                  if start_str.lower().startswith("in office "):
                      start_str = start_str[10:].strip()
                  if end_str.lower().startswith("in office "):
                      end_str = end_str[10:].strip()
                  self.Logger.debug_log( f"date parts start: {start_str} end: {end_str}" , True )

                  # Strip trailing footnote refs (e.g. "Incumbent [ t ]") before special-case check
                  start_clean = re.sub(r'\s*\[\s*\w+\s*\]\s*$', '', start_str).strip()
                  end_clean = re.sub(r'\s*\[\s*\w+\s*\]\s*$', '', end_str).strip()
                  # Check if start or end part matches special cases
                  start_date = start_clean if start_clean.lower() in special_cases else self.format_date(start_str)
                  end_date = end_clean if end_clean.lower() in special_cases else self.format_date(end_str)

                  self.Logger.debug_log(f"Range detected - Start: {start_date}, End: {end_date}" , True )
                  return (start_date, end_date) if date_type == 'both' else (start_date, 'N/A') if date_type == 'start' else ('N/A', end_date)

      except ( ValueError , IndexError , TypeError ) as e:
                  self.Logger.log( f"Error {e} found in {date_str} while parsing by delimiter {delimiter}" , True )



      if date_type == "both":
        for pattern in compiled_patterns:
            self.Logger.debug_log( f"trying to search compiled patterns {pattern}" , True )
            try:
                self.Logger.debug_log(f"pattern {pattern}" , True )
                self.Logger.debug_log(f"date_str {date_str}" , True )
                # Ensure date_str is a string; remove .text if date_str is already a string
                match = pattern.search(date_str)
                self.Logger.debug_log( f"match {match}" , True )
                if match:
                    self.Logger.debug_log(f"Found match within pattern {pattern.pattern}" , True )
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
                    self.Logger.debug_log(f"Birth date: {start_date}, Death date: {end_date}" , True )
                    if date_type == 'both':
                        return (start_date, end_date)
                    return (start_date, 'N/A') if date_type == 'start' else ('N/A', end_date)
            except ( ValueError , IndexError , TypeError ) as e:
                  self.Logger.log( f"Value Error {e} found in {date_str} while parsing by date pattern" , True )


      # If no range delimiter is found, or if the date_type is 'start' or 'end', process the whole string
      if date_type in ['start', 'end']:

          try:
            self.Logger.debug_log( "no delimiter found"  , True )
            date = date_str if date_str.lower().strip() in special_cases else self.format_date(date_str)
            self.Logger.debug_log(f"Single date or special case: {date}" , True )
            return date if date_type == 'start' else date if date_type == 'end' else date

          except ( ValueError , IndexError , TypeError) as e:
                  self.Logger.log( f"Error {e} found in {date_str} while parsing by date pattern" , True )



      self.Logger.debug_log( f"invalid date found in {date_str}" , True )
      return 'Invalid date', 'Invalid date'

  def parse_year_range(self, text: str):
      """Parse a year range from table text (e.g. '1966–1974', '2009–present'). Returns (start_year int|None, end_year int|None)."""
      if not text or not isinstance(text, str):
          return (None, None)
      text = self.remove_footnote(text)
      text = text.strip()
      year_match = re.search(r'\b(17|18|19|20)\d{2}\b', text)
      if not year_match:
          return (None, None)
      delimiters = ['–', '-', ' —<br/>', ' to ', ' – ', ', to ']
      present_cases = ["incumbent", "n/a", "present", "Incumbent"]
      for d in delimiters:
          if d in text:
              parts = text.split(d, 1)
              start_str = (parts[0].strip() if parts else "").strip()
              end_str = (parts[1].strip() if len(parts) > 1 else "").strip()
              start_m = re.search(r'\b(17|18|19|20)\d{2}\b', start_str)
              start_year = int(start_m.group(0)) if start_m else None
              if not end_str or end_str.lower().strip() in present_cases:
                  return (start_year, None)
              end_m = re.search(r'\b(17|18|19|20)\d{2}\b', end_str)
              end_year = int(end_m.group(0)) if end_m else None
              return (start_year, end_year)
      sy = int(year_match.group(0))
      return (sy, sy)

  def find_link_and_data_columns( self , row , max_column_index = None, min_column_index = None ):
      # Dynamic identification of the link column and subsequent data columns.
      # If max_column_index is set (0-based), only consider cells up to that index so we never
      # pick a link from a non-data column (e.g. President column) when it appears after term dates.
      # #region agent log
      try:
          _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
          open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_link_and_data_columns", "message": "entry", "data": {"len_row": len(row), "max_column_index": max_column_index, "min_column_index": min_column_index}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
      except Exception:
          pass
      # #endregion
      for i, cell in enumerate(row):
          if min_column_index is not None and i < min_column_index:
              continue
          if max_column_index is not None and i > max_column_index:
              break
          try:
              cell_str = str(cell)  # Convert BeautifulSoup object or similar to string, if necessary
          except Exception as e:
              # #region agent log
              try:
                  _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                  open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_link_and_data_columns", "message": "str_cell_error", "data": {"i": i, "error": str(e), "type": type(e).__name__}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
              except Exception:
                  pass
              # #endregion
              raise
          # Wiki article link: absolute path, relative (./), or full URL
          has_absolute = 'href="/wiki/' in cell_str
          has_relative = 'href="./' in cell_str and 'href="./File:' not in cell_str and 'href="./Special:' not in cell_str
          has_full_url = 'en.wikipedia.org/wiki/' in cell_str and '/wiki/File:' not in cell_str and '/wiki/Special:' not in cell_str
          has_wiki_link = has_absolute or has_relative or has_full_url
          has_fragment_link = '#"' in cell_str or '#cite_note' in cell_str
          # Exclude file/special links in any form
          has_file_link = (
              'href="/wiki/File:' in cell_str
              or 'href="./File:' in cell_str
              or '/wiki/File:' in cell_str
              or 'href="./Special:' in cell_str
              or '/wiki/Special:' in cell_str
          )
          if has_wiki_link and not has_file_link and not has_fragment_link:
              # #region agent log
              try:
                  _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                  open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_link_and_data_columns", "message": "return_column", "data": {"column": i, "has_absolute": has_absolute, "has_relative": has_relative, "has_full_url": has_full_url, "cell_snippet": cell_str[:200]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
              except Exception:
                  pass
              # #endregion
              self.Logger.debug_log( f"Wiki link (not a file link) found at column {i}: {cell}" , True )
              return i  # Return the index of the column containing the link
      # #region agent log
      try:
          _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
          open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_link_and_data_columns", "message": "return_none", "data": {"reason": "no_cell_matched"}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H4"}) + "\n")
      except Exception:
          pass
      # #endregion
      self.Logger.debug_log( f"Wiki did not find a link in {row}" , True )
      return None  # If no matching link column is found, or data structure is different



  def remove_footnote( self , content , extract_text=False , strip_text=False ):

      self.Logger.debug_log ( "removing footnote" , False )

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

        if extract_text and hasattr( content, 'get_text' ):
            # If content is a BeautifulSoup object and text extraction is requested
            text = content.get_text(strip=strip_text)
        else:
            text = content

        # Remove footnote references
        cleaned_text = re.sub(r'\[\w+\]', '', text)

        if strip_text and not extract_text:
            # If text stripping is requested but text was not extracted (meaning content was already a string)
            cleaned_text = cleaned_text.strip()

        self.Logger.debug_log( f" removed footnote \n\n before {content} \n\n after {cleaned_text}" , False )

        return cleaned_text

      except ( TypeError , ValueError , IndexError ) as e:
        self.Logger.log( f"error {e} parsing footnote" , True )

  def remove_parenthesis( self , content  ):

    self.Logger.debug_log ( "removing parenthesis" , True )

    try:
        cleaned_text = re.sub(r'\([^)]*\)', '', content)


        self.Logger.debug_log( f" removed parenthesis \n\n before {content} \n\n after {cleaned_text}" , False )

        return cleaned_text

    except ( TypeError , ValueError , IndexError ) as e:
        self.Logger.log( f"error {e} parsing parenthesis" , True )

class Offices:

  def __init__(self , logger , biography , data_cleanup ):


    self.Logger = logger
    self.DataCleanup = data_cleanup
    self.Biography = biography


  def _is_valid_wiki_link(self, link):
    if not isinstance(link, str):
      return False
    candidate = link.strip()
    if not candidate or candidate == "No link":
      return False
    if not candidate.startswith("https://en.wikipedia.org/wiki/"):
      return False
    # Keep ignore_non_links useful for parser junk rows too (e.g. congress/election links).
    if any(re.search(pattern, candidate) for pattern in self.patterns_to_ignore()):
      return False
    if "/wiki/File:" in candidate or "/wiki/Special:" in candidate:
      return False
    return True


  def process_table(self, html_content, table_config, office_details, url, party_list, progress_callback=None, max_rows=None):
    self.Logger.log(f"---------------\n\n Processing table with config: {table_config}", True)

    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')

    # Check if specified table number is within bounds
    if not (0 <= table_config['table_no'] - 1 < len(tables)):
        self.Logger.log("Table number out of bounds.", False)
        return []

    target_table = tables[table_config['table_no'] - 1]
    rows = target_table.find_all('tr')[1:]  # Exclude the header row
    if max_rows is not None and max_rows >= 0:
        rows = rows[:max_rows]
    accumulated_results = []
    total_rows = len(rows)
    report_infobox = table_config.get("find_date_in_infobox") and progress_callback is not None

    # #region agent log
    _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
    try:
        _f = open(_log_path, "a", encoding="utf-8")
        _f.write(json.dumps({"location": "table_parser:process_table", "message": "table rows total", "data": {"total_rows": total_rows, "table_rows_config": table_config.get("table_rows"), "term_end_column": table_config.get("term_end_column")}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
        _f.close()
    except Exception:
        pass
    # #endregion

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
                progress_callback(row_index + 1, total_rows, f"Processing {row_index + 1} of {total_rows}")
            cells = row.find_all(['td', 'th'])
            self.Logger.debug_log( f"cells from process table {cells}" , True )

            cells_td = row.find_all('td')
            row_results = self.parse_table_row(row, table_config, office_details, url,  previous_row_wiki_link, previous_row_district, previous_row_party, party_list)
            if row_results and table_config.get("ignore_non_links"):
                row_results = [r for r in row_results if self._is_valid_wiki_link(r.get("Wiki Link"))]
            self.Logger.debug_log( f"results from process table {row_results}" , True )
            appended = bool(row_results)
            if row_results:
                accumulated_results.extend(row_results)

                # Update the "previous row" variables from the last result (one row can yield multiple term rows)
                last_result = row_results[-1]
                previous_row_wiki_link = last_result.get('Wiki Link')
                previous_row_district = last_result.get('District')
                previous_row_party = last_result.get('Party')

            # #region agent log
            skip_reason = None
            if not appended:
                if len(cells_td) <= table_rows_val:
                    skip_reason = "table_rows"
                elif term_end_col >= 0 and len(cells_td) <= term_end_col:
                    skip_reason = "term_end_col"
                else:
                    skip_reason = "other"
            try:
                _f = open(_log_path, "a", encoding="utf-8")
                _f.write(json.dumps({"location": "table_parser:process_table", "message": "row", "data": {"row_index": row_index, "len_td": len(cells_td), "appended": appended, "skip_reason": skip_reason}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1" if skip_reason == "table_rows" else "H2" if skip_reason == "term_end_col" else "H4"}) + "\n")
                _f.close()
            except Exception:
                pass
            # #endregion
        except ( IndexError , AttributeError , TypeError , ValueError , UnicodeEncodeError , UnicodeDecodeError ) as e:
            self.Logger.log( f" found error {e} when processing row {row_index}" , True )
            # #region agent log
            try:
                _f = open(_log_path, "a", encoding="utf-8")
                _f.write(json.dumps({"location": "table_parser:process_table", "message": "exception", "data": {"row_index": row_index, "error": str(e)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H4"}) + "\n")
                _f.close()
            except Exception:
                pass
            # #endregion

    if table_config.get("consolidate_rowspan_terms"):
        accumulated_results = self._consolidate_rowspan_terms(accumulated_results, table_config)

    return accumulated_results

  def _consolidate_rowspan_terms(self, rows: list, table_config: dict) -> list:
    """Group rows by holder (Wiki Link or _name_from_table), sort by term start, merge consecutive terms (gap <= 1 day or year)."""
    # #region agent log
    _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
    try:
      with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_consolidate_rowspan_terms", "message": "input rows", "data": {"n": len(rows), "rows": [{"holder": (r.get("Wiki Link") or "").strip() or ("_name_:" + (r.get("_name_from_table") or "")), "term_start": r.get("Term Start"), "term_end": r.get("Term End")} for r in rows]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
    except Exception:
      pass
    # #endregion
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

    # #region agent log
    try:
      with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_consolidate_rowspan_terms", "message": "grouped", "data": {"groups": {k: [{"term_start": r.get("Term Start"), "term_end": r.get("Term End")} for r in v] for k, v in grouped.items()}}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
    except Exception:
      pass
    # #endregion

    out = []
    for group in grouped.values():
      group.sort(key=sort_key)
      # #region agent log
      try:
        with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_consolidate_rowspan_terms", "message": "after sort", "data": {"ordered": [{"term_start": r.get("Term Start"), "term_end": r.get("Term End")} for r in group]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H4"}) + "\n")
      except Exception:
        pass
      # #endregion
      run = [group[0]]
      for i in range(1, len(group)):
        if gap_consecutive(run[-1], group[i]):
          run.append(group[i])
        else:
          # #region agent log
          try:
            with open(_log_path, "a", encoding="utf-8") as _f: _f.write(json.dumps({"location": "table_parser:_consolidate_rowspan_terms", "message": "gap break", "data": {"prev_end": run[-1].get("Term End"), "curr_start": group[i].get("Term Start"), "prev_end_parsed": str(_parse_date(run[-1].get("Term End"))), "curr_start_parsed": str(_parse_date(group[i].get("Term Start"))), "run_len": len(run)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
          except Exception:
            pass
          # #endregion
          _emit_merged_run(run, years_only, out)
          run = [group[i]]
      _emit_merged_run(run, years_only, out)
    return out


  def parse_table_row( self , row , table_config , office_details , url , previous_row_wiki_link, previous_row_district, previous_row_party , party_list ):

      '''
      This function parses out the specific table.
      '''

      self.Logger.log( f"---------------\n\n table config in parse_table_row: \n\n {table_config} \n\n row: {row} " , True )

      self.Logger.debug_log( f"previous values: \n wiki_link: {previous_row_wiki_link} \n district: {previous_row_district} \n party: {previous_row_party}" , True )

      cells = row.find_all(['td', 'th'])
      self.Logger.debug_log( f" cells {cells} \n\n" , True )

      # total columns primarily works with right_to_left function
      total_columns = len( cells )
      self.Logger.log( f"total columns {total_columns}" , True )

      # create a duplicate version of table_config. This duplicate version could be changed by other functions, without updating table_config
      table_config_to_parse = copy.deepcopy(table_config)
      # Keep original term_end_column for short-row continuation check (before RTL or dynamic_parse change it)
      term_end_column_orig = table_config.get("term_end_column", -1)

      self.Logger.debug_log( f"original table config {table_config} \n table config to parse {table_config_to_parse}" , True )


      row_data = {}

      '''
      max_columns = max(table_config_to_parse['link_column'], table_config_to_parse['party_column'], table_config_to_parse['term_start_column'], table_config_to_parse['district_column'])
      believe this is now obsolete (replaced by range_total_columns?)
      '''

      # variable to control rowspan function
      found_rowspan = False

      term_start, term_end = "Invalid date", "Invalid date"  # Default values before calling extract_term_dates
      district = "No district"
      party = "No party"

      district_no_value = "No district"
      party_no_value = "No party"



      # Initialize the data structure for the row's results.
      results = {
          'Country': office_details['office_country'],
          'Level': office_details['office_level'],
          'Branch': office_details['office_branch'],
          'Department': office_details['office_department'],
          'Office Name': office_details['office_name'],
          'State': office_details['office_state'],
          'Office Notes': office_details['office_notes'],
          'Wiki Link': None,
          'Party': None,
          'District': None,
          'Term Start': None,
          'Term End': None,
      }


      # columns_RTL function reads columns in reverse. This is primarily used for senate offices - which has the url at the rightmost end.

      if table_config_to_parse["read_columns_right_to_left"] == True:
          table_config_to_parse = self.process_columns_right_to_left( table_config_to_parse , total_columns )
      else:
        self.Logger.debug_log( "not running read_columns_right_to_left" , True )


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
          success , table_config_to_parse = self.process_dynamic_parse(cells, table_config_to_parse)
          self.Logger.debug_log( f"table config return in process_table_row after dynamic parse {table_config_to_parse} \n success: {success}" , True )

          if not success:
            # Skip processing this row as the link column wasn't found
            return None

      else:
        self.Logger.debug_log( "not running run_dynamic_parse" , True )

      # Ensure there are enough cells to avoid IndexError - do not use for rowspan, as it often will cause an error
      if len(cells) <= table_config_to_parse["table_rows"] and table_config_to_parse["parse_rowspan"] == False :  # Adjust this number based on the expected minimum number of cells
          self.Logger.log( 'issue with table rows' , True )
          return None  # or some default data structure
      # With rowspan, skip only rows that have too few cells to parse any term (need at least 2 to try; continuation rows often have 3)
      if table_config_to_parse["parse_rowspan"] == True and len(cells) < 2:
          self.Logger.log( 'skipping rowspan continuation row (too few cells)' , True )
          return None
      # Skip rows that don't have enough columns to include term_end (e.g. President-only continuation rows). When parse_rowspan, continuation rows have fewer cells so column indices don't align—skip check.
      term_end_col = table_config_to_parse.get("term_end_column", -1)
      if term_end_col >= 0 and len(cells) <= term_end_col and not table_config_to_parse.get("parse_rowspan"):
          self.Logger.log( 'skipping row (too few columns for term_end)' , True )
          return None

      self.Logger.debug_log( "column numbers determined, extracting information" , True )

      # range total columns determines the number of columns in cells. This is used for the rowspan function.
      range_total_columns = range(total_columns)
      self.Logger.debug_log( f"range of columns: {range_total_columns}", True )



      # Before iterating over columns, ensure you have the initial data or use last known values
      wiki_link = self.find_link(table_config_to_parse, office_details, cells, party_list)
      self.Logger.debug_log( f"wiki link results before iteration: {wiki_link} " , True )

      # update wiki link on second iteration of rospan and beyond
      if wiki_link == None and table_config_to_parse["parse_rowspan"] == True:
        self.Logger.debug_log( f"No wiki link found in row" , True )
        wiki_link = previous_row_wiki_link
        self.Logger.debug_log( f"Adding previous link {previous_row_wiki_link} as link {wiki_link}" , True )
        found_rowspan = True
      # When parse_rowspan and this row has too few cells for the configured term columns, treat as continuation row
      # (otherwise dynamic_parse may find a link in a date cell and we skip rowspan term loop, then IndexError on cells[term_end_column])
      if table_config_to_parse.get("parse_rowspan") and previous_row_wiki_link and term_end_col >= 0 and len(cells) <= term_end_col:
        wiki_link = previous_row_wiki_link
        found_rowspan = True
      if wiki_link is None or (wiki_link or "").strip() == "":
        wiki_link = "No link"

      '''
      The following three logic chains deal with the rowspan function. Rowspan works for office holders with multiple terms.
      Often the url is listed in the first column, but not repeated in subsequent column. The rowspan helps keep track of previous values.
      When rowspan == true, it reviews each column in cells to find the value. If not value is found, it will apply previous value.
      When rowspan == false. it will simply call the appropriate function based on the column_no.
      '''

      # figure out party (skip and keep null when party_ignore)
      if table_config_to_parse.get("party_ignore"):
        party = None
        self.Logger.debug_log( "party_ignore: not extracting party" , True )
      elif found_rowspan == True :
        self.Logger.debug_log( f"running parse rowspan on party" , True )
        for col_no in range_total_columns:

            self.Logger.debug_log( f"running parse iteration {col_no} with party" , True )
            party  = self.extract_party( wiki_link , cells , office_details , table_config_to_parse , col_no , party_list , party_no_value )
            if party not in ( None , "No Party" ):
              self.Logger.debug_log( f"found results for party {party}" , True )
              break
            else:
              party = previous_row_party
              self.Logger.debug_log( f"could not find party, so keeping old version {previous_row_party}" , True )
      else:
        party  = self.extract_party( wiki_link , cells , office_details , table_config_to_parse , None , party_list , party_no_value )
        self.Logger.debug_log( f"no rowspan, results for party: {party}" , True )

      # figure out district (override when district_ignore or district_at_large)
      if table_config_to_parse.get("district_ignore"):
        district = "No District"
        self.Logger.debug_log( "district_ignore: using No District" , True )
      elif table_config_to_parse.get("district_at_large"):
        district = "At-Large"
        self.Logger.debug_log( "district_at_large: using At-Large" , True )
      elif found_rowspan == True :
        self.Logger.debug_log( f"running parse rowspan on district" , True )
        for col_no in range_total_columns:

            self.Logger.debug_log( f"running parse iteration {col_no} with district" , True )
            district  = self.extract_district( wiki_link , cells , office_details , table_config_to_parse , col_no , district_no_value )
            if district not in ( None , "No district" ):
              self.Logger.debug_log( f"found results for district {district}" , True )
              break
            else:
              district = previous_row_district
              self.Logger.debug_log( f"could not find district, so keeping old version {previous_row_district}" , True )
      else:
        district  = self.extract_district( wiki_link , cells , office_details , table_config_to_parse , None , district_no_value  )
        self.Logger.debug_log( f"no rowspan, results for district: {district}" , True )

      #figure out term dates
      term_start_year = None
      term_end_year = None
      if found_rowspan == True :
        self.Logger.debug_log("running parse rowspan on term", True)
        term_tuples = []
        best_single = None  # fallback when no range found
        for col_no in range_total_columns:  # Assuming total_columns is correctly calculated elsewhere

              self.Logger.debug_log(f"running parse iteration {col_no} with term", True)
              raw = self.extract_term_dates( wiki_link , cells , office_details , table_config_to_parse , col_no , url , district )
              if isinstance(raw, list):
                  term_tuples = raw
                  break
              term_start, term_end, term_start_year, term_end_year = raw

              ignore_terms = ( None , "Invalid date" )
              if table_config_to_parse.get("years_only"):
                  if term_start_year is not None or term_end_year is not None:
                      self.Logger.debug_log(f"found years-only term start year {term_start_year} end year {term_end_year}", True)
                      term_tuples = [(term_start, term_end, term_start_year, term_end_year)]
                      break
              elif term_start not in ignore_terms and term_end not in ignore_terms:
                  self.Logger.debug_log(f"found results for term start {term_start} and term end {term_end}", True)
                  # Prefer a range (start != end); otherwise keep as fallback and try next column
                  if term_start != term_end:
                      term_tuples = [(term_start, term_end, term_start_year, term_end_year)]
                      break
                  if best_single is None:
                      best_single = (term_start, term_end, term_start_year, term_end_year)
        # For short rowspan rows: try start from one cell, end from next (e.g. 3-cell row has start col0, end col1)
        n_cells = len(cells)
        if (not term_tuples or (len(term_tuples) == 1 and term_tuples[0][0] == term_tuples[0][1])) and n_cells >= 2:
          for (sc, ec) in [(0, 1), (1, 2)]:
            if ec >= n_cells:
              continue
            raw = self.extract_term_dates(wiki_link, cells, office_details, table_config_to_parse, (sc, ec), url, district)
            if isinstance(raw, list):
              continue
            term_start, term_end, _, _ = raw
            if term_start and term_end and term_start != "Invalid date" and term_end != "Invalid date" and term_start != term_end:
              term_tuples = [(term_start, term_end, None, None)]
              break
        if not term_tuples and best_single is not None:
            term_tuples = [best_single]
        if not term_tuples:
            term_tuples = [(None, None, None, None)]
      else:
          raw_terms = self.extract_term_dates( wiki_link , cells , office_details , table_config_to_parse , None , url , district )
          term_tuples = raw_terms if isinstance(raw_terms, list) else [raw_terms]
          self.Logger.debug_log(f"\n\n no rowspan, got {len(term_tuples)} term(s) from extract_term_dates", True)



      link_column = table_config_to_parse.get("link_column", 0)
      name_from_table = None
      if 0 <= link_column < len(cells):
          first_a = cells[link_column].find("a")
          name_from_table = first_a.get_text(strip=True) if first_a else None
          if name_from_table is None:
              name_from_table = cells[link_column].get_text(strip=True) or None
              # #region agent log
              if name_from_table and (wiki_link or "").strip() in ("", "No link"):
                  try:
                      import json
                      _dp = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                      open(_dp, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:parse_table_row", "message": "name_from_table from cell text (no link)", "data": {"wiki_link": (wiki_link or "")[:60], "name_from_table": (name_from_table or "")[:80]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
                  except Exception:
                      pass
                  # #endregion
      infobox_debug = getattr(self, "_last_infobox_items", None)
      if infobox_debug is not None:
          self._last_infobox_items = None  # Consume so next row does not inherit
      results_list = []
      for (term_start, term_end, term_start_year, term_end_year) in term_tuples:
          row_dict = {
              'Country': office_details['office_country'] ,
              'Level': office_details['office_level'] ,
              'Branch': office_details['office_branch'] ,
              'Department': office_details['office_department'] ,
              'Office Name': office_details['office_name'] ,
              'State': office_details['office_state'] ,
              'Office Notes': office_details['office_notes'] ,
              'Wiki Link': wiki_link,
              'Term Start': term_start,
              'Term End': term_end,
              'Term Start Year': term_start_year,
              'Term End Year': term_end_year,
              'Party': party,
              'District': district
          }
          row_dict["_name_from_table"] = name_from_table
          last_dead_link = getattr(self.Biography, "_last_dead_link", False)
          row_dict["_dead_link"] = bool(last_dead_link and wiki_link and wiki_link != "No link")
          if last_dead_link:
              self.Biography._last_dead_link = False  # Consume so next row does not inherit
          if infobox_debug is not None:
              row_dict['Infobox items'] = "\n".join(infobox_debug) if isinstance(infobox_debug, list) else str(infobox_debug)
              last_bio = getattr(self.Biography, "_last_bio_details", None)
              if last_bio is not None:
                  row_dict["_bio_details"] = last_bio
                  self.Biography._last_bio_details = None  # Attach only to first result row for this person
          results_list.append(row_dict)
      for results in results_list:
          self.Logger.log( f"results {results}" , True )
      return results_list

  def patterns_to_ignore( self ):

    '''
    List of patterns to ignore if found by the find_link function.
    '''

    return (
        r'/wiki/\d{1,3}(th|st|nd|rd)_United_States_Congress' ,
        r"/wiki/([\w%]+)_\d{1,2}(th|st|nd|rd)_congressional_district",
        r'/wiki/\d{4}_[\w\d]+_elections_in_[\w\d]+',
        r'/wiki/\d{4}_[\w\d]+_election',
        r'/wiki/(19|20)\d{2}(_\d)?$',  # year links e.g. /wiki/2024 from date columns
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

    self.Logger.debug_log("find link", True)

    link_column = table_config_to_parse["link_column"]
    country = office_details["office_country"]

    self.Logger.debug_log(f"country in find_link: {country}", True)

    # #region agent log
    _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
    _num_links = 0
    if self.column_present(link_column, cells):
        _link_tags = cells[link_column].find_all('a', href=True)
        _num_links = len(_link_tags)
    try:
        _f = open(_log_path, "a", encoding="utf-8")
        _f.write(json.dumps({"location": "table_parser:find_link", "message": "entry", "data": {"link_column": link_column, "num_links_in_cell": _num_links}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
        _f.close()
    except Exception:
        pass
    # #endregion

    if self.column_present(link_column, cells):
        self.Logger.debug_log("url column present", True)
        link_tags = cells[link_column].find_all('a', href=True)
        try:
          for link_tag in link_tags:
              self.Logger.debug_log(f"looking at {link_tag} in {link_tags}", True)
              if '/File:' not in link_tag['href']:
                  raw_href = (link_tag['href'] or "").strip()
                  # Relative hrefs like "./Title" produce invalid URLs; normalize to /wiki/Title
                  if raw_href.startswith("./"):
                      path = "/wiki/" + raw_href[2:].lstrip("/")
                  elif raw_href.startswith("/wiki/"):
                      path = raw_href
                  elif raw_href.startswith("/"):
                      path = "/wiki" + raw_href
                  else:
                      path = "/wiki/" + raw_href
                  full_url = normalize_wiki_url(f"https://en.wikipedia.org{path}") or f"https://en.wikipedia.org{path}"
                  self.Logger.debug_log(f"found full url {full_url}", True)
                  has_fragment = "#" in full_url
                  should_ignore = any(re.search(pattern, full_url) for pattern in self.patterns_to_ignore()) or has_fragment
                  party_links = {p.get('link') for p in party_list.get(country, []) if p.get('link')}
                  if not should_ignore and full_url not in party_links:
                      self.Logger.debug_log(f"URL passed all checks: {full_url}", True)
                      # #region agent log
                      try:
                          _f = open(_log_path, "a", encoding="utf-8")
                          _f.write(json.dumps({"location": "table_parser:find_link", "message": "returned", "data": {"find_link_returned": True, "url": full_url}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
                          _f.close()
                      except Exception:
                          pass
                      # #endregion
                      return full_url
                  # #region agent log
                  try:
                      _f = open(_log_path, "a", encoding="utf-8")
                      _f.write(json.dumps({"location": "table_parser:find_link", "message": "skip", "data": {"reason": "should_ignore" if should_ignore else "in_party_links", "url": full_url}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
                      _f.close()
                  except Exception:
                      pass
                  # #endregion

        except ( ValueError , TypeError , IndexError , AttributeError ) as e:
          self.Logger.log( f"found error when finding url for {full_url} in {cells}" , True )
          # #region agent log
          try:
              _f = open(_log_path, "a", encoding="utf-8")
              _f.write(json.dumps({"location": "table_parser:find_link", "message": "exception", "data": {"error": str(e)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
              _f.close()
          except Exception:
              pass
          # #endregion
    # #region agent log
    try:
        _f = open(_log_path, "a", encoding="utf-8")
        _f.write(json.dumps({"location": "table_parser:find_link", "message": "returned None", "data": {"find_link_returned": False, "reason": "no_valid_link"}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H1"}) + "\n")
        _f.close()
    except Exception:
        pass
    # #endregion


  def extract_term_dates( self , wiki_link , cells , office_details , table_config_to_parse , parse_row_no , url , district ):

    self.Logger.debug_log( "running extract terms" , True )
    self._last_infobox_items = None  # Cleared each call; set only when find_date_in_infobox used

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

    # #region agent log
    try:
        _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
        open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:extract_term_dates", "message": "entry", "data": {"term_start_column": term_start_column, "term_end_column": term_end_column, "len_cells": len(cells), "would_oob": term_start_column >= len(cells) or term_end_column >= len(cells)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
    except Exception:
        pass
    # #endregion

    # Extract and format the term start and end dates

    try:
      self.Logger.debug_log( f"start date column {term_start_column} results: {cells[term_start_column]}" , True )
      self.Logger.debug_log( f"end date column {term_end_column} results: {cells[term_end_column]}" , True )

      # Years only: table has year ranges only; do not call infobox. Parse year range and leave dates unpopulated.
      if table_config_to_parse.get("years_only") == True:
        cell_text = cells[term_start_column].get_text(separator=' ').strip()
        term_start_year, term_end_year = self.DataCleanup.parse_year_range(cell_text)
        self.Logger.debug_log( f" years_only: parsed year range {term_start_year}–{term_end_year} from {cell_text!r}" , True )
        return (None, None, term_start_year, term_end_year)

      # Find date in infobox: fetch full dates from person's bio; collect all matching terms from infobox
      if table_config_to_parse["find_date_in_infobox"] == True:
        self.Logger.debug_log( f" parse_table_row found TRUE in find_date_in_infobox \n\n about to process {cells[term_start_column]}" , True )
        cache = getattr(self, "_infobox_cache", None)
        if cache is not None and wiki_link in cache:
          cached = cache[wiki_link]
          terms_list = cached["terms"]
          infobox_items = cached["infobox_items"]
          self._last_infobox_items = infobox_items
          self.Biography._last_bio_details = cached.get("bio_details")
        else:
          terms_list, infobox_items = self.Biography.find_term_dates( wiki_link , url , table_config_to_parse , office_details , district )
          self._last_infobox_items = infobox_items  # For debug export
          if cache is not None:
            cache[wiki_link] = {
              "terms": terms_list,
              "infobox_items": infobox_items,
              "bio_details": getattr(self.Biography, "_last_bio_details", None),
            }
        self.Logger.debug_log( f" find_term_dates returned {len(terms_list)} term(s) from infobox" , True )
        # When infobox had no dates (placeholder), use table years only for this record (same as "table has years only")
        if len(terms_list) == 1 and terms_list[0][0] == "YYYY-00-00" and terms_list[0][1] == "YYYY-00-00":
          cell_text_start = cells[term_start_column].get_text(separator=' ').strip()
          same_column = (term_start_column == term_end_column)
          cell_text_end = cells[term_end_column].get_text(separator=' ').strip() if not same_column and term_end_column < len(cells) else None
          term_start_year, term_end_year = self.DataCleanup.parse_year_range(cell_text_start)
          # When start and end are in different columns, parse end cell for term_end_year instead of reusing start
          if not same_column and cell_text_end is not None:
              _sy_end, _ey_end = self.DataCleanup.parse_year_range(cell_text_end)
              term_end_year = _ey_end if _ey_end is not None else _sy_end
          self.Logger.debug_log( f" find_date_in_infobox: no infobox dates; using table years only for this record {term_start_year}–{term_end_year} from {cell_text_start!r}" , True )
          return [(None, None, term_start_year, term_end_year)]
        return [(s, e, None, None) for (s, e) in terms_list]

      # determine what to do if the term_start and term_end appear in the same columns
      if term_start_column == term_end_column:
        self.Logger.debug_log( f"parse_table_row found start and end dates in same column" , True )
        self.Logger.debug_log( f" cell with date {cells[term_start_column]}" , True )
        cell = cells[term_start_column]
        cell_text = cell.get_text(separator=' ').strip() if cell else ""
        self.Logger.debug_log( f" cell with date with separator {cell_text}" , True )
        term_start, term_end = self.DataCleanup.parse_date_info(cell_text , "both" )  # Use separator to handle <br/>
        # Fallback: Wikipedia sortable tables often put dates in data-sort-value when visible text is template/empty
        if (not term_start or term_start == "Invalid date" or not term_end or term_end == "Invalid date") and cell:
          ds, de = _dates_from_cell_data_sort_value(cell)
          if ds and de:
            term_start, term_end = ds, de
        return (term_start, term_end, None, None)

      self.Logger.debug_log( f"parse_table_row found start and end dates not in same column" , True )
      self.Logger.debug_log( f" cell with start date {cells[term_start_column]}" , True )
      self.Logger.debug_log( f" cell with end date {cells[term_end_column].get_text(strip=True)}" , True )
      term_start = self.DataCleanup.parse_date_info(cells[ term_start_column ].get_text(strip=True) , "start" )
      term_end = self.DataCleanup.parse_date_info(cells[ term_end_column ].get_text(strip=True) , "end" )
      # Fallback: data-sort-value when visible text is template/empty (e.g. continuation rows)
      if (not term_start or term_start == "Invalid date") and term_start_column < len(cells):
        ds, _ = _dates_from_cell_data_sort_value(cells[term_start_column])
        if ds:
          term_start = ds
      if (not term_end or term_end == "Invalid date") and term_end_column < len(cells):
        _, de = _dates_from_cell_data_sort_value(cells[term_end_column])
        if de:
          term_end = de
      self.Logger.debug_log( f" finished extracting term start and end: {term_start} {term_end}  " , True )
      return (term_start, term_end, None, None)

    except ( ValueError , TypeError , AttributeError , IndexError ) as e:
      # #region agent log
      try:
          _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
          open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:extract_term_dates", "message": "caught_exception", "data": {"error": str(e), "type": type(e).__name__, "term_start_column": term_start_column, "term_end_column": term_end_column, "len_cells": len(cells)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
      except Exception:
          pass
      # #endregion
      self.Logger.log( f" error {e} when parsing {wiki_link}" , True )
      return ("Invalid date", "Invalid date", None, None)

  def extract_party( self , wiki_link , cells , office_details , table_config_to_parse , parse_row_no , party_list , no_value_return  ):

    self.Logger.debug_log( f"running extract_party \n table config {table_config_to_parse}" , True )

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
      link_tags = cell.find_all('a', href=True)

      try:
        for link_tag in link_tags:
            full_url_unclean = f"https://en.wikipedia.org{link_tag['href']}"
            full_url = self.DataCleanup.remove_footnote( full_url_unclean )
            self.Logger.debug_log( f"full url in extract_party: {full_url} /n country in party list: {country in party_list}" , True )

                # Check if the URL is in the party_list for the given country
            if country in party_list:
              for party_info in party_list[country]:
                  self.Logger.debug_log(f"Checking party: {party_info['name']} with link: {party_info['link']} \n url: {full_url}", True)
                  if full_url == party_info['link']:
                      self.Logger.debug_log(f"Match found for party: {party_info['name']}", True)
                      return party_info['name']
      except ( ValueError , IndexError , TypeError ) as e:
        self.Logger.log( f"found error {e} in party_extract when searching for party link" , True )

      # Fallback: when party_link=True but cell has no link (or no match), match by text
      if country in party_list:
        party_text = cell.get_text(strip=True)
        if party_text:
          try:
            for party_info in party_list[country]:
              if re.search(re.escape(party_info["name"]), party_text, re.IGNORECASE):
                self.Logger.debug_log(f"Match found for party (text fallback): {party_info['name']} in {party_text!r}", True)
                return party_info['name']
          except (ValueError, TypeError, IndexError):
            pass

    if use_party_link != True and country in party_list:
        party_text = cells[party_column].get_text(strip=True)
        self.Logger.debug_log(f"Extracted party text: {party_text}", True)

        try:
          for party_info in party_list[country]:
            # Using case-insensitive search to improve matching chances
            if re.search(re.escape(party_info["name"]), party_text, re.IGNORECASE):
              self.Logger.debug_log(f"Match found for party: {party_info['name']} using text: {party_text}", True)
              return party_info['name']
          self.Logger.debug_log(f"No party match found in party list for text: {party_text}", True)

        except (ValueError, TypeError, IndexError) as e:
          self.Logger.log(f"Error {e} while searching for party text", True)


    return no_value_return



  def extract_district( self , wiki_link , cells , office_details , table_config_to_parse , parse_row_no , no_value_return ):

      self.Logger.debug_log( f"running extract_district \n table config {table_config_to_parse}" , True )

      # parse_row_no == None means the rowspan function is working and needs to iterate. Otherwise, choose the column_no.
      if parse_row_no == None:
        district_column = table_config_to_parse["district_column"]
      else:
        district_column = parse_row_no

      self.Logger.debug_log( f"district column: {district_column} \n table config: {table_config_to_parse['district_column']} \n parse row no {parse_row_no}" , True )

      # Initialize district to 'No district' by default
      district = 'No district'

      # Ensure the district_column index is within bounds
      if 0 <= district_column < len(cells):
          district_text = cells[district_column].get_text(strip=True)

          # Check if district_text matches the pattern for ordinal numbers (1st, 2nd, 3rd, etc.) or "At-large"
          ordinal_pattern = r'\b\d+(st|nd|rd|th)\b'
          at_large_pattern = r'At-large'
          territory_pattern = r'Territory'

          # If district_text matches the ordinal pattern or is exactly "At-large"
          if re.search(ordinal_pattern, district_text) or re.match(at_large_pattern, district_text, re.IGNORECASE) or re.match(territory_pattern, district_text, re.IGNORECASE) :
              district = district_text
          else:
              self.Logger.debug_log(f"District text '{district_text}' does not match expected patterns.", True)
      else:
          self.Logger.debug_log("District column index is out of bounds.", True)

      self.Logger.debug_log(f"Extracted district info: {district}", True)


      return district


  def process_columns_right_to_left( self , table_config_to_parse , total_columns ):

    self.Logger.debug_log( f"running process_columns right to left" , True )

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

    self.Logger.debug_log( f"new link column {link_column} after rtl conversion from {link_column_old} with total {total_columns}", True )
    self.Logger.debug_log( f"party column {party_column} after rtl conversion from {party_column_old} with total {total_columns}", True )
    self.Logger.debug_log( f"term start {term_start_column} after rtl conversion from {term_start_column_old} with total {total_columns}", True )
    self.Logger.debug_log( f"term end {term_end_column} after rtl conversion from {term_end_column_old} with total {total_columns}", True )
    self.Logger.debug_log( f"district column {district_column} after rtl conversion from {district_column_old} with total {total_columns}", True )

    table_config_to_parse["link_column"] = link_column
    table_config_to_parse["party_column"] = party_column
    table_config_to_parse["term_start_column"] = term_start_column
    table_config_to_parse["term_end_column"] = term_end_column
    table_config_to_parse["district_column"] = district_column

    return table_config_to_parse

  def process_dynamic_parse( self , cells , table_config_to_parse ):

    self.Logger.debug_log( "running process_dynamic_parse" , True )

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
      max_link_col = dynamic_link_max_col if dynamic_link_max_col is not None else (row_len - 1)
    else:
      # Backward compatibility: bound by term_end and parse direction.
      if table_config_to_parse.get("read_columns_right_to_left") == True:
        min_link_col = term_end_column if term_end_column is not None else 0
        max_link_col = row_len - 1
      else:
        min_link_col = 0
        max_link_col = term_end_column if term_end_column is not None and term_end_column >= 0 else (row_len - 1)

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
      link_column_result = self.DataCleanup.find_link_and_data_columns(cells, max_column_index=row_len - 1, min_column_index=0)

    # Stop loop if no link is found
    if link_column_result is None:
        table_no = table_config_to_parse.get("table_no", "unknown")
        self.Logger.debug_log(
          f"DynamicParse table={table_no}: no link found (bounds {min_link_col}..{max_link_col}, fallback full-row {'failed' if used_fallback_scan else 'not-run'}). Row skipped.",
          True,
        )
        # Return False indicating the link column was not found, alongside original columns
        table_config_to_parse["link_column"] = link_column
        table_config_to_parse["party_column"] = party_column
        table_config_to_parse["term_start_column"] = term_start_column
        table_config_to_parse["term_end_column"] = term_end_column
        table_config_to_parse["district_column"] = district_column

        self.Logger.debug_log( f"returning table config {table_config_to_parse}" , True )

        return False, table_config_to_parse

    link_column = link_column_result

    self.Logger.debug_log(f"Link column determined dynamically: {link_column} old link {link_column_old} \n table config {table_config_to_parse}" , True )


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
    self.Logger.debug_log( f" term_start current: {term_start_column} diff: {diff_term_start} " , True )
    diff_term_end = term_end_column - link_column_old
    self.Logger.debug_log( f" term_end current: {term_end_column} diff: {diff_term_end} " , True )
    diff_district = district_column - link_column_old if district_column not in [0, 1000] else 0
    self.Logger.debug_log( f" district current: {district_column} diff: {diff_district} " , True )
    diff_party = party_column - link_column_old if party_column > 0 else 0
    self.Logger.debug_log( f" party current: {party_column} diff: {diff_party} " , True )

    # Update columns based on differences
    party_column = link_column + diff_party if diff_party != 0 else party_column
    self.Logger.debug_log( f"update party column: {party_column}" , True )
    district_column = link_column + diff_district if diff_district != 0 else district_column
    self.Logger.debug_log( f"update district column: {district_column}" , True )
    term_start_column = link_column + diff_term_start
    self.Logger.debug_log( f"update term start column: {term_start_column}" , True )
    term_end_column = link_column + diff_term_end
    self.Logger.debug_log( f"update term end column: {term_end_column}" , True )

    '''
    #Error handling when there are issues with parsing
    party_column = "N/A" if party_column > len(cells) else party_column
    district_column = "N/A" if district_column > len(cells) else district_column
    term_start_column = "N/A" if term_start_column > len(cells) else term_start_column
    term_end_column = "N/A" if term_end_column > len(cells) else term_end_column
    self.Logger.debug_log( f" expected cells {len(cells)} " , True)
    '''

    # Return True indicating successful parsing, alongside updated columns
    table_config_to_parse["link_column"] = link_column
    table_config_to_parse["party_column"] = party_column
    table_config_to_parse["term_start_column"] = term_start_column
    table_config_to_parse["term_end_column"] = term_end_column
    table_config_to_parse["district_column"] = district_column

    # #region agent log
    try:
        _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
        open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:process_dynamic_parse", "message": "columns_updated", "data": {"link_column": link_column, "term_start_column": term_start_column, "term_end_column": term_end_column, "len_cells": len(cells)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
    except Exception:
        pass
    # #endregion

    self.Logger.debug_log( f"table config at the end of dynamic parse: \n {table_config_to_parse} \n\n" , True )

    return True, table_config_to_parse



class Biography:

  def __init__( self, logger , data_cleanup ):


    self.Logger = logger
    self.DataCleanup = data_cleanup


  def parse_infobox( self , infobox ):

      '''
      This function searches for information in the biography's infobox.
      This function does not yet find birth_place and death_place.
      '''

      self.Logger.log( "start running parse_infobox" , False )

      details = {
          'full_name': None,
          'name': None,
          'birth_date': None,
          'birth_place': None,
          'death_date': None,
          'death_place': None,
      }

      # Look for name within the infobox
      name_row = infobox.find('th', {'class': 'infobox-above'})
      if name_row:
          details['name'] = self.DataCleanup.remove_footnote(name_row.get_text(strip=True))

      # Nickname is not usually a nickname, but rather the full name
      nickname_div = infobox.find('div', {'class': 'nickname'})
      if nickname_div:
          details['full_name'] = self.DataCleanup.remove_footnote(nickname_div.get_text(strip=True))
      else:
          details['full_name'] = details.get('name', None)

      for tr in infobox.findAll('tr'):
          th = tr.find('th')
          td = tr.find('td')

          if th and td:
              if 'Born' in th.text:
                  self.Logger.debug_log( th.text , False )
                  birth_date_text = td.get_text(" ", strip=True)
                  birth_date = self.DataCleanup.parse_date_info( birth_date_text , "start" )
                  details['birth_date'] = birth_date

              elif 'Died' in th.text:
                  self.Logger.debug_log( th.text , False )
                  death_date_text = td.get_text(" ", strip=True)
                  death_date = self.DataCleanup.parse_date_info( death_date_text , "end" )
                  details['death_date'] = death_date

      self.Logger.log( "completd running parse_infobox" , False )
      return details



  def parse_first_paragraph( self , paragraph ):

      self.Logger.log( "running first paragraphy method" , True )

      self.Logger.debug_log( f"running first paragraph \n\n {paragraph}" , True )

      '''
      This function searches for information in the biography's first paragraphy.
      This function does not yet find birth_place and death_place.
      '''

      details = {
          'full_name': None,
          'name': None,
          'birth_date': None,
          'birth_place': None,
          'death_date': None,
          'death_place': None,
      }

      # Find bold text for full name
      bold_text = paragraph.find('b')
      if bold_text:
          details['full_name'] = self.DataCleanup.remove_footnote(bold_text.text)
          self.Logger.debug_log( f" full name {details['full_name']} " , True )

      details['birth_date'] , details['death_date'] = self.DataCleanup.parse_date_info( paragraph , "both" )

      self.Logger.debug_log( f"first paragraph details {details}" , True )
      return details




  def biography_extract(self, wiki_link ):

      self.Logger.log( "-------- \n\n Running biography extract", True )

      normalized_link = normalize_wiki_url(wiki_link) or wiki_link
      fetch_url = wiki_url_to_rest_html_url(normalized_link) or normalized_link
      try:
          response = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=30)
          if response.status_code == 200:
              html_content = response.text
              soup = BeautifulSoup(html_content, 'html.parser')

              infobox = soup.find('table', {'class': ['infobox vcard', 'infobox biography vcard']})
              details = None
              if infobox:
                  details = self.parse_infobox(infobox)
              else:
                  first_paragraph = soup.find('p')
                  if first_paragraph:
                      details = self.parse_first_paragraph(first_paragraph)
                  else:
                      return {}

              if details:
                  details['page_path'] = urlparse(wiki_link).path.split('/')[-1].strip()
                  if not details['full_name']:
                      details['full_name'] = details.get('name', '')
              else:
                  details = {'page_path': urlparse(wiki_link).path.split('/')[-1].strip()}
              return details
          else:
              self.Logger.log(f"Failed to fetch biography URL with status code: {response.status_code}" , True )
              return {}
      except requests.exceptions.RequestException as e:
          self.Logger.log(f"Request failed: {e}" , True )
          return {}

  def find_term_dates(self, wiki_link, url, table_config_to_parse, office_details, district):

      self.Logger.debug_log( f"running find_term_dates \n url value {url}" , True )

      # #region agent log
      _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
      try:
          _f = open(_log_path, "a", encoding="utf-8")
          _f.write(json.dumps({"location": "table_parser:find_term_dates", "message": "entry", "data": {"wiki_link": wiki_link, "url": url}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H2"}) + "\n")
          _f.close()
      except Exception:
          pass
      # #endregion

      '''
      Replink == true is used for US representative tables with only years in the table, such as New Jersey.
      This function searches the infobox of the biographuies for dates.
      '''

      state = office_details["office_state"].replace( " " , "_" )
      encoded_state = quote(state)
      district = district.lower()

      # Build list of partial URLs to try (main and/or alt_links); then build match_candidates from all
      urls_to_try = []
      if table_config_to_parse["rep_link"] == True:
        urls_to_try = [f"/wiki/United_States_House_of_Representatives"]
        self.Logger.debug_log(f"Running find_term_dates for {wiki_link} with congressional URL", True )
      else:
        alt_links = table_config_to_parse.get("alt_links") or []
        alt_ok = bool(alt_links)
        if alt_ok:
          urls_to_try = [(p if p.startswith("/") else "/wiki/" + p.lstrip("/")) for p in alt_links if (p or "").strip()]
          if table_config_to_parse.get("alt_link_include_main"):
            urls_to_try = urls_to_try + [urlparse(url).path]
          self.Logger.debug_log(f"Running find_term_dates for {wiki_link} with alt_links %r" % (urls_to_try[:3],), True )
        if not urls_to_try:
          urls_to_try = [urlparse(url).path]
          self.Logger.debug_log(f"Running find_term_dates for {wiki_link} with office URL {urls_to_try[0]}", True )

      current_office_holder = [ "assumed office" , "incumbent" , "invalid date" ]




      infobox_items = []  # For debug export: what we found in the infobox
      self._last_bio_details = None
      self._last_dead_link = False
      fetch_url = wiki_url_to_rest_html_url(wiki_link) or wiki_link
      try:
          response = requests.get(fetch_url, headers=WIKIPEDIA_REQUEST_HEADERS, timeout=30)
          if response.status_code == 200:
              html_content = response.text
              soup = BeautifulSoup(html_content, 'html.parser')
              infobox = soup.find('table', {'class': ['infobox vcard', 'infobox biography vcard']})

              if infobox:
                  self.Logger.debug_log( f"Found infobox \n {infobox}" , True )
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
                          _slug_to_check = office_slug[len("List_of_"):]
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
                                  elif _first.endswith("s") and not _first.endswith("ss") and len(_first) > 1:
                                      _title_parts[0] = (_first[:-1]).title()
                                  _title = "_".join(_title_parts)
                                  if _title != prefix:
                                      office_slug_alt = (office_slug_alt or prefix + "_of_" + state_part)
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
                      return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())).strip()

                  def _contains_phrase(hay: str, phrase: str) -> bool:
                      if not phrase:
                          return True
                      # `hay` and `phrase` are already normalized to lowercase words/spaces.
                      return re.search(r"(^|\s)" + re.escape(phrase) + r"(\s|$)", hay) is not None

                  def _parse_role_query(expr: str) -> tuple[list[str], list[str]]:
                      includes: list[str] = []
                      excludes: list[str] = []
                      # Supports terms like: judge -"chief judge" -"senior judge"
                      for m in re.finditer(r'(-?)"([^"]+)"|(-?)(\S+)', expr or ""):
                          neg = bool((m.group(1) or m.group(3) or "").strip())
                          raw = (m.group(2) or m.group(4) or "").strip()
                          term = _normalize_role_text(raw)
                          if not term:
                              continue
                          if neg:
                              excludes.append(term)
                          else:
                              includes.append(term)
                      return includes, excludes

                  role_includes, role_excludes = _parse_role_query(role_key)

                  def _role_matches(text: str) -> bool:
                      if not role_key:
                          return True
                      hay = _normalize_role_text(text)
                      if not hay:
                          return False
                      if not role_includes and not role_excludes:
                          needle = _normalize_role_text(role_key)
                          return _contains_phrase(hay, needle) if needle else True
                      for inc in role_includes:
                          if not _contains_phrase(hay, inc):
                              return False
                      for exc in role_excludes:
                          if _contains_phrase(hay, exc):
                              return False
                      return True

                  all_terms = []  # Collect all matching term (start, end) from every matching row in the infobox
                  for tr in infobox.find_all('tr'):
                      self.Logger.debug_log( f"found tr \n {tr}" , True )
                      links = tr.find_all('a', href=True)
                      row_text = tr.get_text(" ", strip=True)
                      link_matches = False
                      for a in links:
                          raw_href = a.get("href", "") if a else ""
                          norm_path = _normalize_infobox_href(raw_href)
                          norm_path_lower = (norm_path or "").lower()
                          if norm_path_lower in match_candidates or (norm_path_lower.rsplit("/", 1)[-1] in match_candidates if norm_path_lower else False):
                              link_matches = True
                              break
                      role_matches = _role_matches(row_text)
                      if link_matches and role_matches:
                          # Examine the next two sibling rows for date information
                          self.Logger.debug_log( f"found match. starting to iterate" , True )
                          tr_cur = tr
                          row_desc = "Office row: %r" % (row_text[:80] + ("..." if len(row_text) > 80 else ""))
                          term_added_this_tr = False
                          for _ in range(2):  # Check the next row and the one after that
                              tr_cur = tr_cur.find_next_sibling('tr') if tr_cur else None
                              if tr_cur:
                                  self.Logger.debug_log(f"find next tr {tr_cur}", True)
                                  date_text = tr_cur.get_text(" ", strip=True)
                                  self.Logger.debug_log(f"date text {date_text}", True)
                                  try:
                                      _res = self.DataCleanup.parse_date_info(date_text, "both")
                                      start_date = _res[0] if isinstance(_res, (tuple, list)) and len(_res) >= 1 else _res
                                      end_date = _res[1] if isinstance(_res, (tuple, list)) and len(_res) >= 2 else "present"
                                  except (ValueError, TypeError, IndexError):
                                      start_date, end_date = "Invalid date", "Invalid date"
                                  if start_date and end_date and start_date.lower() not in current_office_holder and end_date.lower() not in current_office_holder:
                                      self.Logger.debug_log(f"Found term dates: {start_date}, {end_date}", True)
                                      # #region agent log
                                      try:
                                          _f = open(_log_path, "a", encoding="utf-8")
                                          _f.write(json.dumps({"location": "table_parser:find_term_dates", "message": "success return", "data": {"term_start": start_date, "term_end": end_date}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
                                          _f.close()
                                      except Exception:
                                          pass
                                      # #endregion
                                      all_terms.append((start_date, end_date))
                                      term_added_this_tr = True
                                      infobox_items.append("%s -> date row: %r -> parsed: %s, %s" % (row_desc, date_text[:100], start_date, end_date))
                                      break  # Found valid dates for this office row; move to next matching tr
                                  try:
                                      _res = self.DataCleanup.parse_date_info(date_text, "start")
                                      if isinstance(_res, (tuple, list)) and len(_res) >= 2:
                                          start_date, end_date = _res[0], _res[1]
                                      elif isinstance(_res, (tuple, list)) and len(_res) >= 1:
                                          start_date, end_date = _res[0], "present"
                                      else:
                                          start_date, end_date = _res, "present"
                                  except (ValueError, TypeError, IndexError):
                                      start_date, end_date = "Invalid date", "Invalid date"
                                  if start_date and start_date.lower() not in current_office_holder :
                                      self.Logger.debug_log(f"Found term dates: {start_date}", True)
                                      # #region agent log
                                      try:
                                          _f = open(_log_path, "a", encoding="utf-8")
                                          _f.write(json.dumps({"location": "table_parser:find_term_dates", "message": "success return", "data": {"term_start": start_date, "term_end": end_date}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
                                          _f.close()
                                      except Exception:
                                          pass
                                      # #endregion
                                      all_terms.append((start_date, end_date))
                                      term_added_this_tr = True
                                      infobox_items.append("%s -> date row: %r -> parsed: %s, %s" % (row_desc, date_text[:100], start_date, end_date))
                                      break  # Found valid dates for this office row; move to next matching tr
                                  # If dates are invalid, continue to the next sibling row
                          if not term_added_this_tr:
                              infobox_items.append("%s -> checked 2 sibling rows; no valid dates" % row_desc)
                      elif link_matches and role_key and not role_matches:
                          infobox_items.append("Skipped row for role key %r: %r" % (role_key_raw, row_text[:100]))
                  # Single fetch: also collect birth/death from same infobox so runner can skip second fetch
                  details = self.parse_infobox(infobox)
                  details["wiki_url"] = normalize_wiki_url(wiki_link) or wiki_link
                  details["page_path"] = (urlparse(wiki_link).path.split("/")[-1] or "").strip()
                  if not details.get("full_name"):
                      details["full_name"] = details.get("name") or ""
                  self._last_bio_details = details
                  if all_terms:
                      return (all_terms, infobox_items if infobox_items else ["Infobox: matched rows returned terms above."])

              if not infobox:
                  self._last_bio_details = None
                  infobox_items.append("No infobox in page.")
              elif not infobox_items:
                  infobox_items.append("Infobox found; no rows matching office link/name%s." % (" + role key" if role_key else ""))
          else:
              self._last_bio_details = None
              infobox_items.append("Failed to fetch page: HTTP %s" % response.status_code)
              if (wiki_link or "").strip() and (wiki_link or "").strip() != "No link":
                  self._last_dead_link = True
                  # #region agent log
                  try:
                      _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                      open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_term_dates", "message": "dead_link_reason", "data": {"wiki_link": (wiki_link or "")[:120], "reason": "http_error", "status": response.status_code}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
                  except Exception:
                      pass
                  # #endregion
      except requests.exceptions.RequestException as e:
          self._last_bio_details = None
          self.Logger.log(f"Request failed: {e}", False)
          infobox_items.append("Request failed: %s" % str(e))
          if (wiki_link or "").strip() and (wiki_link or "").strip() != "No link":
              self._last_dead_link = True
              # #region agent log
              try:
                  _log_path = Path(__file__).resolve().parent.parent.parent / ".cursor" / "debug.log"
                  open(_log_path, "a", encoding="utf-8").write(json.dumps({"location": "table_parser:find_term_dates", "message": "dead_link_reason", "data": {"wiki_link": (wiki_link or "")[:120], "reason": "request_exception", "error": str(e)[:100]}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H5"}) + "\n")
              except Exception:
                  pass
              # #endregion
          # #region agent log
          try:
              _f = open(_log_path, "a", encoding="utf-8")
              _f.write(json.dumps({"location": "table_parser:find_term_dates", "message": "RequestException", "data": {"error": str(e)}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H5"}) + "\n")
              _f.close()
          except Exception:
              pass
          # #endregion

      # Placeholder return: no infobox or no matching terms. Do NOT set dead link here — dead link means
      # the page does not exist (404, request failed). Missing/mismatched infobox means the page exists.
      # #region agent log
      try:
          _f = open(_log_path, "a", encoding="utf-8")
          infobox_reason = (infobox_items[-1][:150] if infobox_items else "no_items") if isinstance(infobox_items, list) else str(infobox_items)[:150]
          _f.write(json.dumps({"location": "table_parser:find_term_dates", "message": "placeholder return", "data": {"term_start": "YYYY-00-00", "term_end": "YYYY-00-00", "wiki_link": (wiki_link or "")[:120], "dead_link_set": False, "infobox_reason": infobox_reason}, "timestamp": __import__("time").time() * 1000, "hypothesisId": "H3"}) + "\n")
          _f.close()
      except Exception:
          pass
      # #endregion
      return ([("YYYY-00-00", "YYYY-00-00")], infobox_items if infobox_items else ["No dates found (placeholder)."])
