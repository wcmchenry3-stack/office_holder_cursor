from src.scraper import test_script_runner


def test_fixture_path_accepts_test_scripts_prefix() -> None:
    p = test_script_runner._fixture_path("test_scripts/fixtures/example.html")
    assert p == test_script_runner.TEST_SCRIPTS_DIR / "fixtures/example.html"


def test_fixture_path_accepts_relative_fixture_path() -> None:
    p = test_script_runner._fixture_path("fixtures/example.html")
    assert p == test_script_runner.TEST_SCRIPTS_DIR / "fixtures/example.html"


def test_load_html_supports_legacy_basename_pointing_to_fixtures(tmp_path, monkeypatch) -> None:
    scripts_dir = tmp_path / "test_scripts"
    fixtures_dir = scripts_dir / "fixtures"
    fixtures_dir.mkdir(parents=True)
    sample = fixtures_dir / "legacy_name.html"
    sample.write_text("<html>ok</html>", encoding="utf-8")

    monkeypatch.setattr(test_script_runner, "TEST_SCRIPTS_DIR", scripts_dir)

    out = test_script_runner._load_html("legacy_name.html")
    assert out == "<html>ok</html>"
