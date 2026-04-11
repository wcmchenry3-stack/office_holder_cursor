#!/usr/bin/env python3
"""
i18n_update.py — extract translatable strings and update all .po catalogs.

Usage (from project root):
    python scripts/i18n_update.py

Steps performed:
  1. pybabel extract  → src/locales/messages.pot  (master template)
  2. pybabel update   → updates every locale's messages.po with new/changed strings
  3. Summary report   → new strings, removed strings, fuzzy strings per locale
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCALES_DIR = ROOT / "src" / "locales"
POT_FILE = LOCALES_DIR / "messages.pot"
BABEL_CFG = ROOT / "src" / "babel.cfg"


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
    if result.stdout:
        print(result.stdout, end="")


def count_fuzzy(po_text: str) -> int:
    return len(re.findall(r"^#, fuzzy", po_text, re.MULTILINE))


def count_untranslated(po_text: str) -> int:
    """Count msgstr entries that are empty (excluding the header entry)."""
    entries = re.split(r"\n\n", po_text)
    return sum(
        1
        for e in entries
        if 'msgid ""' not in e and re.search(r'msgstr ""\s*$', e, re.MULTILINE)
    )


def main() -> None:
    print("==> Extracting strings from templates and Python source files…")
    run(
        [
            sys.executable, "-m", "babel.messages.frontend", "extract",
            "-F", str(BABEL_CFG),
            "--project=RulersAI",
            "--version=1.0",
            "-o", str(POT_FILE),
            "src/",
        ]
    )
    pot_text = POT_FILE.read_text(encoding="utf-8")
    total_strings = len(re.findall(r"^msgid ", pot_text, re.MULTILINE)) - 1  # subtract header
    print(f"    {total_strings} translatable string(s) in master template.\n")

    print("==> Updating locale catalogs…")
    run(
        [
            sys.executable, "-m", "babel.messages.frontend", "update",
            "-i", str(POT_FILE),
            "-d", str(LOCALES_DIR),
        ]
    )

    print("\n==> Summary per locale:\n")
    locale_dirs = sorted(
        p for p in LOCALES_DIR.iterdir()
        if p.is_dir() and not p.name.startswith("_")
    )
    for loc_dir in locale_dirs:
        po_file = loc_dir / "LC_MESSAGES" / "messages.po"
        if not po_file.exists():
            continue
        text = po_file.read_text(encoding="utf-8")
        fuzzy = count_fuzzy(text)
        untranslated = count_untranslated(text)
        status = "ok" if not fuzzy and not untranslated else "needs attention"
        print(
            f"  {loc_dir.name:<10}  fuzzy={fuzzy}  untranslated={untranslated}  [{status}]"
        )

    print("\nDone. Edit .po files, then run: pybabel compile -d src/locales")


if __name__ == "__main__":
    main()
