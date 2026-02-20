from bs4 import BeautifulSoup

from src.scraper.table_parser import DataCleanup, Offices


class _NoopLogger:
    def log(self, *_args, **_kwargs):
        return None

    def debug_log(self, *_args, **_kwargs):
        return None


def _cells_from_row_html(row_html: str):
    soup = BeautifulSoup(f"<table><tr>{row_html}</tr></table>", "html.parser")
    return soup.find("tr").find_all(["td", "th"])


def test_find_link_and_data_columns_respects_bounds():
    cleanup = DataCleanup(_NoopLogger())
    cells = _cells_from_row_html(
        """
        <td><a href='/wiki/Ignore'>Ignore</a></td>
        <td>Term start</td>
        <td>Term end</td>
        <td><a href='/wiki/Expected'>Expected</a></td>
        """
    )

    assert cleanup.find_link_and_data_columns(cells, min_column_index=2) == 3
    assert cleanup.find_link_and_data_columns(cells, max_column_index=2) == 0


def test_dynamic_parse_rtl_searches_to_the_right_of_term_columns():
    logger = _NoopLogger()
    cleanup = DataCleanup(logger)
    offices = Offices(logger, biography=None, data_cleanup=cleanup)

    # In RTL tables, person link appears to the right of term columns. There is an earlier
    # non-person link to the left that should be ignored by dynamic parse.
    cells = _cells_from_row_html(
        """
        <td><a href='/wiki/NotPerson'>Not person</a></td>
        <td>Democratic</td>
        <td>Jan 3, 2013 – Jan 3, 2025</td>
        <td><a href='/wiki/Brian_Schatz'>Brian Schatz</a></td>
        """
    )

    ok, cfg = offices.process_dynamic_parse(
        cells,
        {
            "link_column": -1,
            "party_column": 1,
            "term_start_column": 2,
            "term_end_column": 2,
            "district_column": 0,
            "read_columns_right_to_left": True,
        },
    )

    assert ok is True
    assert cfg["link_column"] == 3
