from bs4 import BeautifulSoup

from src.scraper.table_parser import Offices, DataCleanup


class _Logger:
    def debug_log(self, *_args, **_kwargs):
        return None

    def log(self, *_args, **_kwargs):
        return None


class _BioInfoboxStub:
    def __init__(self):
        self._last_dead_link = False
        self._last_bio_details = None

    def find_term_dates(self, _wiki_link, _url, _table_config, _office_details, _district, run_cache=None):
        return [("1978-07-14", "1988-11-18")], [
            "Office row: 'Judge ...' -> date row: 'In office July 14, 1978 – November 18, 1988' -> parsed: 1978-07-14, 1988-11-18"
        ]


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

    rows = offices.process_table(
        html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Linked_Senator")


def test_process_table_accepts_rows_when_cell_count_equals_table_rows_threshold():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Governor</th><th>Term in office</th></tr>
      <tr><td>1</td><td><a href="/wiki/Example_Governor">Example Governor</a></td><td>January 1, 1900 – January 1, 1904</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 3,
        "link_column": 1,
        "party_column": -1,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": -1,
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
        "office_level": "State",
        "office_branch": "Executive",
        "office_department": "Governor",
        "office_name": "Governor",
        "office_state": "Michigan",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Example_Governor")
    assert rows[0]["Term Start"] == "1900-01-01"
    assert rows[0]["Term End"] == "1904-01-01"


def test_parse_rowspan_does_not_carry_previous_holder_on_non_short_rows_without_link():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Governor</th><th>Term in office</th><th>Party</th><th>Election</th></tr>
      <tr><td>1</td><td><a href="/wiki/First_Governor">First Governor</a></td><td>January 1, 1900 – January 1, 1904</td><td>Republican</td><td>1900</td></tr>
      <tr><td>2</td><td>Unknown</td><td>January 1, 1904 – January 1, 1908</td><td>Democratic</td><td>1904</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 3,
        "link_column": 1,
        "party_column": 3,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": -1,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": False,
        "years_only": False,
        "parse_rowspan": True,
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
        "office_level": "State",
        "office_branch": "Executive",
        "office_department": "Governor",
        "office_name": "Governor",
        "office_state": "Michigan",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/First_Governor")
    assert rows[0]["Term Start"] == "1900-01-01"
    assert rows[0]["Term End"] == "1904-01-01"


def test_find_link_fallback_recovers_holder_when_configured_link_column_is_footnote_cell():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Governor</th><th>Term in office</th><th>Party</th><th>Election</th></tr>
      <tr>
        <td>1</td>
        <td><a href="/wiki/Stevens_T._Mason">Stevens T. Mason</a></td>
        <td>November 3, 1835 – January 7, 1840</td>
        <td>Democrat</td>
        <td><sup><a href="#cite_note-43">[43]</a></sup></td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 4,
        "link_column": 4,  # misconfigured to Election/footnote column
        "party_column": 3,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": -1,
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
        "ignore_non_links": False,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "State",
        "office_branch": "Executive",
        "office_department": "Governor",
        "office_name": "Governor",
        "office_state": "Michigan",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Stevens_T._Mason")
    assert rows[0]["Term Start"] == "1835-11-03"
    assert rows[0]["Term End"] == "1840-01-07"


def test_ignore_non_links_drops_party_organization_links():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Link</th><th>Party</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/Republican_Party_(United_States)">Republican Party</a></td><td>Republican</td><td>Jan 1, 2000 – Jan 1, 2004</td></tr>
      <tr><td>2</td><td><a href="/wiki/Real_Person">Real Person</a></td><td>Democratic</td><td>Jan 1, 2004 – Jan 1, 2008</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,
        "party_column": 2,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": -1,
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

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Real_Person")


def test_find_link_fallback_works_with_rtl_by_probing_right_side_columns():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>A</th><th>B</th><th>C</th><th>D</th></tr>
      <tr>
        <td>Jan 1, 2001 – Jan 1, 2002</td>
        <td>Democratic</td>
        <td><sup><a href="#cite_note-1">[1]</a></sup></td>
        <td><a href="/wiki/Right_Holder">Right Holder</a></td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,  # maps to footnote column after RTL transform when total_columns=4
        "party_column": 1,
        "term_start_column": 3,  # maps to date column 0 after RTL transform
        "term_end_column": 3,
        "district_column": -1,
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

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Right_Holder")


def test_find_link_fallback_rtl_clamps_when_term_start_column_is_out_of_bounds():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>A</th><th>B</th><th>C</th><th>D</th></tr>
      <tr>
        <td>Jan 1, 2001 – Jan 1, 2002</td>
        <td>Democratic</td>
        <td><sup><a href="#cite_note-1">[1]</a></sup></td>
        <td><a href="/wiki/Right_Holder">Right Holder</a></td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,  # maps to footnote column after RTL transform when total_columns=4
        "party_column": 1,
        "term_start_column": 99,  # out of bounds should still allow RTL fallback probe
        "term_end_column": 99,
        "district_column": -1,
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

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Right_Holder")


def test_find_link_does_not_fallback_when_configured_column_has_no_links():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Class A</th><th>Class B</th><th>Term</th></tr>
      <tr>
        <td>1</td>
        <td><a href="/wiki/Class_A_Senator">Class A Senator</a></td>
        <td>Vacant</td>
        <td>Jan 1, 2000 – Jan 1, 2004</td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 2,  # configured side has no links in this row
        "party_column": -1,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": -1,
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
        "ignore_non_links": True,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Legislative",
        "office_department": "Senate",
        "office_name": "Class Test",
        "office_state": "",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert rows == []


def test_find_link_keeps_party_links_when_ignore_non_links_is_false():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Link</th><th>Dates</th></tr>
      <tr><td>1</td><td><a href="/wiki/Michigan_Democratic_Party">Michigan Democratic Party</a></td><td>Jan 1, 2000 – Jan 1, 2002</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,
        "party_column": -1,
        "term_start_column": 2,
        "term_end_column": 2,
        "district_column": -1,
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
        "office_level": "State",
        "office_branch": "Executive",
        "office_department": "Governor",
        "office_name": "State of Michigan",
        "office_state": "Michigan",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Michigan_Democratic_Party")


def test_find_link_ignores_alt_link_targets_and_uses_person_link():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Role</th><th>Name</th><th>Term</th></tr>
      <tr>
        <td>1</td>
        <td><a href="/wiki/District_Court_for_the_Northern_Mariana_Islands">District Court</a></td>
        <td><a href="/wiki/Alfred_Laureta">Alfred Laureta</a></td>
        <td>Jan 1, 1978 – Jan 1, 1988</td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 1,
        "link_column": 1,  # points at office/alt-link target
        "party_column": -1,
        "term_start_column": 3,
        "term_end_column": 3,
        "district_column": -1,
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
        "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Judicial",
        "office_department": "District Court",
        "office_name": "Past",
        "office_state": "",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Alfred_Laureta")


def test_term_range_fallback_recovers_when_term_column_is_misconfigured():
    logger = _Logger()
    offices = Offices(logger, biography=None, data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Governor</th><th>Term in office</th><th>Party</th><th>Election</th></tr>
      <tr>
        <td>1</td>
        <td><a href="/wiki/Stevens_T._Mason">Stevens T. Mason</a></td>
        <td>November 3, 1835 – January 7, 1840</td>
        <td>Democratic</td>
        <td>1835</td>
      </tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 4,
        "link_column": 1,
        "party_column": 3,
        "term_start_column": 4,  # misconfigured to Election column
        "term_end_column": 4,
        "district_column": -1,
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
        "ignore_non_links": False,
    }
    office_details = {
        "office_country": "United States",
        "office_level": "State",
        "office_branch": "Executive",
        "office_department": "Governor",
        "office_name": "Governor",
        "office_state": "Michigan",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Term Start"] == "1835-11-03"
    assert rows[0]["Term End"] == "1840-01-07"


def test_find_date_in_infobox_still_runs_when_term_column_is_out_of_bounds():
    logger = _Logger()
    offices = Offices(logger, biography=_BioInfoboxStub(), data_cleanup=DataCleanup(logger))
    html = """
    <table>
      <tr><th>#</th><th>Name</th><th>Role</th><th>Notes</th></tr>
      <tr><td>1</td><td><a href="/wiki/Alfred_Laureta">Alfred Laureta</a></td><td>Judge</td><td>n/a</td></tr>
    </table>
    """
    table_config = {
        "table_no": 1,
        "table_rows": 3,
        "link_column": 1,
        "party_column": -1,
        "term_start_column": 5,  # intentionally out of bounds
        "term_end_column": 5,
        "district_column": -1,
        "run_dynamic_parse": False,
        "read_columns_right_to_left": False,
        "find_date_in_infobox": True,
        "years_only": False,
        "parse_rowspan": False,
        "consolidate_rowspan_terms": False,
        "rep_link": False,
        "party_link": False,
        "party_ignore": True,
        "district_ignore": True,
        "district_at_large": False,
        "ignore_non_links": False,
        "infobox_role_key": 'judge -"chief judge" -"senior judge"',
        "alt_links": ["/wiki/District_Court_for_the_Northern_Mariana_Islands"],
    }
    office_details = {
        "office_country": "United States",
        "office_level": "Federal",
        "office_branch": "Judicial",
        "office_department": "District Court",
        "office_name": "Past",
        "office_state": "",
        "office_notes": "",
    }

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

    assert len(rows) == 1
    assert rows[0]["Wiki Link"].endswith("/wiki/Alfred_Laureta")
    assert rows[0]["Term Start"] == "1978-07-14"
    assert rows[0]["Term End"] == "1988-11-18"
    assert "Office row:" in (rows[0].get("Infobox items") or "")


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

    rows = offices.process_table(
        html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list
    )

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

    rows = offices.process_table(
        html,
        table_config,
        office_details,
        "https://en.wikipedia.org/wiki/Test",
        {"United States": []},
    )

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

    rows = offices.process_table(
        html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list
    )

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

    rows = offices.process_table(
        html, table_config, office_details, "https://en.wikipedia.org/wiki/Test", party_list
    )

    assert len(rows) == 2
