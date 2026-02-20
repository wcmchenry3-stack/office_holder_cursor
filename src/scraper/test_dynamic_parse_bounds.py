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
