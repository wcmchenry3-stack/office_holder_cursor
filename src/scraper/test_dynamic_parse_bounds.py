from bs4 import BeautifulSoup

from src.scraper.table_parser import Offices, DataCleanup


class _Logger:
    def debug_log(self, *_args, **_kwargs):
        return None

    def log(self, *_args, **_kwargs):
        return None


def _make_cells():
    html = """
    <tr>
      <td>1</td>
      <td><a href="/wiki/Left_Senator">Left Senator</a></td>
      <td>Democratic</td>
      <td>Jan 1, 2000</td>
      <td>Jan 1, 2006</td>
      <td>Republican</td>
      <td><a href="/wiki/Right_Senator">Right Senator</a></td>
      <td>Jan 1, 2012</td>
    </tr>
    """
    soup = BeautifulSoup(html, "html.parser")
    return soup.find("tr").find_all(["td", "th"])


def _base_config():
    return {
        "link_column": 1,
        "party_column": 2,
        "term_start_column": 3,
        "term_end_column": 4,
        "district_column": 0,
        "read_columns_right_to_left": False,
        "table_no": 1,
    }


def test_dynamic_parse_uses_left_bounds():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    config = _base_config()
    config["dynamic_link_min_col"] = 0
    config["dynamic_link_max_col"] = 4

    success, parsed = offices.process_dynamic_parse(_make_cells(), config)

    assert success is True
    assert parsed["link_column"] == 1


def test_dynamic_parse_uses_right_bounds():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    config = _base_config()
    config["dynamic_link_min_col"] = 5
    config["dynamic_link_max_col"] = 7

    success, parsed = offices.process_dynamic_parse(_make_cells(), config)

    assert success is True
    assert parsed["link_column"] == 6


def test_dynamic_parse_falls_back_to_full_row_when_bounds_miss_links():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    config = _base_config()
    config["dynamic_link_min_col"] = 2
    config["dynamic_link_max_col"] = 5

    success, parsed = offices.process_dynamic_parse(_make_cells(), config)

    assert success is True
    assert parsed["link_column"] == 1


def test_process_table_ignores_rows_without_valid_wiki_links_when_enabled():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Senator</th><th>Party</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/Linked_Senator">Linked Senator</a></td><td>Democratic</td><td>Jan 1, 2000 – Jan 1, 2006</td></tr>
      <tr><td>2</td><td><i>Vacant</i></td><td></td><td>Jan 1, 2006 – Jan 2, 2006</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 3,
        "link_column": 1,
        "party_column": 2,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": 0,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": False,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": True,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "Senate",
        "office_name": "Class 2",
        "office_state": "Alaska",
        "office_notes": "",
    }
    party_list = {"United States": []}

    rows = offices.process_table(html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list)

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Linked_Senator")


def test_process_columns_right_to_left_maps_first_column_to_rightmost():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    config = {
        "link_column": 0,
        "party_column": 1,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": -1,
    }

    mapped = offices.process_columns_right_to_left(config, total_columns=8)

    assert mapped["link_column"] == 7
    assert mapped["party_column"] == 6
    assert mapped["term_start_column"] == 5
    assert mapped["term_end_column"] == 5
    assert mapped["district_column"] == -1


def test_rtl_parse_reads_rightmost_link_when_link_column_is_one_based_1():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>A</th><th>B</th><th>C</th></tr>
      <tr>
        <td>Jan 1, 2001 – Jan 1, 2002</td>
        <td>Democratic</td>
        <td><a href="/wiki/Rightmost_Senator">Rightmost Senator</a></td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 0,
        "party_column": 1,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": 0,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": True,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": False,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": False,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "Senate",
        "office_name": "RTL Test",
        "office_state": "",
        "office_notes": "",
    }
    party_list = {"United States": []}

    rows = offices.process_table(html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list)

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Rightmost_Senator")


def test_ignore_non_links_drops_non_person_wiki_links():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Link</th><th>Party</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/118th_United_States_Congress">118th Congress</a></td><td>N/A</td><td>Jan 1, 2023 – Jan 1, 2025</td></tr>
      <tr><td>2</td><td><a href="/wiki/Real_Person">Real Person</a></td><td>Democratic</td><td>Jan 1, 2025 – present</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,
        "party_column": 2,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": 0,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": False,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": True,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "Senate",
        "office_name": "Ignore test",
        "office_state": "",
        "office_notes": "",
    }

    rows = offices.process_table(html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", {"United States": []})

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Real_Person")


def test_dynamic_parse_ignores_fragment_links_when_finding_link_column():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <tr>
      <td><a href="/wiki/Mayor_of_Philadelphia#cite_note-17">ref</a></td>
      <td><a href="/wiki/Real_Holder">Real Holder</a></td>
      <td>Democratic</td>
      <td>Jan 1, 1800</td>
      <td>Jan 1, 1801</td>
    </tr>
    """
    from bs4 import BeautifulSoup
    cells = BeautifulSoup(html, "html.parser").find("tr").find_all(["td", "th"])
    config = {
        "link_column": 1,
        "party_column": 2,
        "term_start_column": 3,
        "term_end_column": 4,
        "district_column": 0,
        "read_columns_right_to_left": False,
        "table_no": 1,
    }

    success, parsed = offices.process_dynamic_parse(cells, config)

    assert success is True
    assert parsed["link_column"] == 1


def test_process_table_applies_optional_row_filter():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Name</th><th>Position</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/A">A</a></td><td>Associate Justice</td><td>Jan 1, 2000 – Jan 1, 2006</td></tr>
      <tr><td>2</td><td><a href="/wiki/B">B</a></td><td>Chief Justice</td><td>Jan 1, 2006 – Jan 1, 2010</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,
        "party_column": 0,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": 0,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": True,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": False,
        "row_filter_column": 2,
        "row_filter_criteria": "Associate Justice",
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Judicial",
        "office_department": "Supreme Court",
        "office_name": "Justice",
        "office_state": "",
        "office_notes": "",
    }
    party_list = {"United States": []}

    rows = offices.process_table(html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list)

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/A")


def test_process_table_without_row_filter_returns_all_rows():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Name</th><th>Position</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/A">A</a></td><td>Associate Justice</td><td>Jan 1, 2000 – Jan 1, 2006</td></tr>
      <tr><td>2</td><td><a href="/wiki/B">B</a></td><td>Chief Justice</td><td>Jan 1, 2006 – Jan 1, 2010</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,
        "party_column": 0,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": 0,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": True,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": False,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Judicial",
        "office_department": "Supreme Court",
        "office_name": "Justice",
        "office_state": "",
        "office_notes": "",
    }
    party_list = {"United States": []}

    rows = offices.process_table(html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list)

    assert len(rows) == 2
