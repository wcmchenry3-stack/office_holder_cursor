"""Unit tests for src/db/bulk_import.py.

Tests bulk_import_offices_from_csv and bulk_import_parties_from_csv
using temp CSV files and SQLite in-memory DB.

Policy note: all Wikipedia HTTP requests in this application use wiki_session()
from src/scraper/wiki_fetch.py, which sets the User-Agent header per Wikimedia
API:Etiquette policy.

Run: pytest src/db/test_bulk_import.py -v
"""

from __future__ import annotations

import os
import textwrap

import pytest

from src.db.bulk_import import (
    _bool_from_cell,
    _int_from_cell,
    bulk_import_offices_from_csv,
    bulk_import_parties_from_csv,
)
from src.db.connection import init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("bulk_import_db")
    path = tmp / "bulk_import_test.db"
    init_db(path=path)
    return path


@pytest.fixture(autouse=True)
def set_db_env(db_path):
    os.environ["OFFICE_HOLDER_DB_PATH"] = str(db_path)
    yield


# ---------------------------------------------------------------------------
# _bool_from_cell
# ---------------------------------------------------------------------------


def test_bool_from_cell_true():
    assert _bool_from_cell("TRUE") == 1


def test_bool_from_cell_yes():
    assert _bool_from_cell("YES") == 1


def test_bool_from_cell_1():
    assert _bool_from_cell("1") == 1


def test_bool_from_cell_false():
    assert _bool_from_cell("FALSE") == 0


def test_bool_from_cell_none():
    assert _bool_from_cell(None) == 0


def test_bool_from_cell_empty():
    assert _bool_from_cell("") == 0


# ---------------------------------------------------------------------------
# _int_from_cell
# ---------------------------------------------------------------------------


def test_int_from_cell_valid():
    assert _int_from_cell("3") == 3


def test_int_from_cell_none_returns_default():
    assert _int_from_cell(None, default=7) == 7


def test_int_from_cell_empty_string_returns_default():
    assert _int_from_cell("", default=4) == 4


def test_int_from_cell_invalid_string_returns_default():
    assert _int_from_cell("abc", default=2) == 2


# ---------------------------------------------------------------------------
# bulk_import_parties_from_csv
# ---------------------------------------------------------------------------


def _write_csv(tmp_path, name: str, content: str):
    p = tmp_path / name
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


def test_parties_import_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        bulk_import_parties_from_csv(tmp_path / "missing.csv", overwrite=False)


def test_parties_import_valid_csv(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parties.csv",
        """\
        Country,Party name,Party link
        United States of America,Test Party,/wiki/Test_Party
        """,
    )
    imported, errors = bulk_import_parties_from_csv(csv, overwrite=False)
    assert imported == 1
    assert errors == 0


def test_parties_import_unknown_country_counts_as_error(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parties_bad.csv",
        """\
        Country,Party name,Party link
        Nonexistent Country XYZ,Some Party,/wiki/Some
        """,
    )
    imported, errors = bulk_import_parties_from_csv(csv, overwrite=False)
    assert imported == 0
    assert errors == 1


def test_parties_import_missing_party_name_counts_as_error(tmp_path):
    csv = _write_csv(
        tmp_path,
        "parties_no_name.csv",
        """\
        Country,Party name,Party link
        United States of America,,/wiki/Some_Party
        """,
    )
    imported, errors = bulk_import_parties_from_csv(csv, overwrite=False)
    assert errors >= 1


def test_parties_import_overwrite_clears_existing(tmp_path):
    # First import
    csv = _write_csv(
        tmp_path,
        "parties_over1.csv",
        """\
        Country,Party name,Party link
        United States of America,Overwrite Party A,/wiki/A
        """,
    )
    bulk_import_parties_from_csv(csv, overwrite=False)

    # Overwrite import — should delete all and import fresh
    csv2 = _write_csv(
        tmp_path,
        "parties_over2.csv",
        """\
        Country,Party name,Party link
        United States of America,Overwrite Party B,/wiki/B
        """,
    )
    imported, errors = bulk_import_parties_from_csv(csv2, overwrite=True)
    assert imported == 1
    assert errors == 0


# ---------------------------------------------------------------------------
# bulk_import_offices_from_csv
# ---------------------------------------------------------------------------


def test_offices_import_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        bulk_import_offices_from_csv(tmp_path / "missing.csv")


def test_offices_import_valid_row(tmp_path):
    csv = _write_csv(
        tmp_path,
        "offices.csv",
        """\
        Country,Level,Branch,Department,Name,State,URL,Table No,Table Rows,Link Column,Party Column,Term Start Column,Term End Column,District,Dynamic Parse,Read columns right to left,Find Date,Years Only,Parse Rowspan,Consolidate Rowspan Terms,Rep Link,Party Link,Notes,Alt Link,Alt Link Include Main
        United States of America,,,,"Import Office",,"https://en.wikipedia.org/wiki/Import_Office",1,4,1,0,4,5,0,TRUE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,,
        """,
    )
    imported, errors = bulk_import_offices_from_csv(csv)
    assert imported == 1
    assert errors == 0


def test_offices_import_unknown_country_counts_as_error(tmp_path):
    csv = _write_csv(
        tmp_path,
        "offices_bad.csv",
        """\
        Country,Level,Branch,Department,Name,State,URL,Table No,Table Rows,Link Column,Party Column,Term Start Column,Term End Column,District,Dynamic Parse,Read columns right to left,Find Date,Years Only,Parse Rowspan,Consolidate Rowspan Terms,Rep Link,Party Link,Notes,Alt Link,Alt Link Include Main
        Nonexistent Country XYZ,,,,,,"https://en.wikipedia.org/wiki/Bad_Office",1,4,1,0,4,5,0,TRUE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,,
        """,
    )
    imported, errors = bulk_import_offices_from_csv(csv)
    assert imported == 0
    assert errors == 1


def test_offices_import_missing_name_counts_as_error(tmp_path):
    csv = _write_csv(
        tmp_path,
        "offices_no_name.csv",
        """\
        Country,Level,Branch,Department,Name,State,URL,Table No,Table Rows,Link Column,Party Column,Term Start Column,Term End Column,District,Dynamic Parse,Read columns right to left,Find Date,Years Only,Parse Rowspan,Consolidate Rowspan Terms,Rep Link,Party Link,Notes,Alt Link,Alt Link Include Main
        United States of America,,,,,"","https://en.wikipedia.org/wiki/No_Name",1,4,1,0,4,5,0,TRUE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,,,
        """,
    )
    imported, errors = bulk_import_offices_from_csv(csv)
    assert errors >= 1
