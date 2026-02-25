from src.scraper import test_script_runner


def test_fixture_path_accepts_test_scripts_prefix() -> None:
    p = test_script_runner._fixture_path("test_scripts/fixtures/example.html")
    assert p == test_script_runner.TEST_SCRIPTS_DIR / "fixtures/example.html"


def test_fixture_path_accepts_relative_fixture_path() -> None:
    p = test_script_runner._fixture_path("fixtures/example.html")
    assert p == test_script_runner.TEST_SCRIPTS_DIR / "fixtures/example.html"
