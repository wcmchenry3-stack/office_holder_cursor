import re

from src.routers.run import _build_primary_fixture_rel_path


def test_primary_fixture_uses_canonical_slug() -> None:
    rel = _build_primary_fixture_rel_path(
        test_name="My Fancy Test!",
        source_url="https://en.wikipedia.org/wiki/Sample",
        canonical_fixture_mode=True,
    )
    assert rel == "fixtures/my_fancy_test.html"


def test_primary_fixture_uuid_mode_preserved() -> None:
    rel = _build_primary_fixture_rel_path(
        test_name="My Fancy Test!",
        source_url="https://en.wikipedia.org/wiki/Sample",
        canonical_fixture_mode=False,
    )
    assert re.match(r"^[a-f0-9]{32}_Sample\.html$", rel)
