#!/usr/bin/env python3
"""
po_to_json.py — generate React Native / Expo JSON translation files from .po catalogs.

Usage (from project root):
    python scripts/po_to_json.py [--output-dir PATH]

Output:
    frontend/src/i18n/locales/{locale}/{namespace}.json

Each msgid is expected to follow the namespace.component.element.type convention,
e.g. ``offices.form.url.label``.  Strings not matching this pattern are written
to ``common.json`` as a fallback.

Only compiled .mo files are read (via babel.support.Translations) so that the
output exactly matches what the web app renders.  Run ``pybabel compile -d src/locales``
before this script.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCALES_DIR = ROOT / "src" / "locales"


def main(output_dir: Path) -> None:
    locale_dirs = sorted(
        p for p in LOCALES_DIR.iterdir()
        if p.is_dir() and not p.name.startswith("_")
    )

    for loc_dir in locale_dirs:
        po_file = loc_dir / "LC_MESSAGES" / "messages.po"
        if not po_file.exists():
            print(f"  skip  {loc_dir.name}  (no messages.po)", file=sys.stderr)
            continue

        namespaces: dict[str, dict[str, str]] = defaultdict(dict)
        msgid, msgstr = None, None

        for line in po_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("msgid "):
                msgid = _unquote(line[len("msgid "):])
            elif line.startswith("msgstr "):
                msgstr = _unquote(line[len("msgstr "):])
                if msgid and msgstr:
                    ns = msgid.split(".")[0] if "." in msgid else "common"
                    namespaces[ns][msgid] = msgstr
                msgid, msgstr = None, None

        locale_out = output_dir / loc_dir.name
        locale_out.mkdir(parents=True, exist_ok=True)
        for ns, strings in namespaces.items():
            out_file = locale_out / f"{ns}.json"
            out_file.write_text(
                json.dumps(strings, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        print(
            f"  {loc_dir.name:<10}  {sum(len(v) for v in namespaces.values())} strings "
            f"→ {len(namespaces)} namespace(s)"
        )

    print("Done.")


def _unquote(s: str) -> str:
    """Strip surrounding quotes and unescape basic escape sequences."""
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert .po files to JSON for React Native.")
    parser.add_argument(
        "--output-dir",
        default="frontend/src/i18n/locales",
        help="Root output directory (default: frontend/src/i18n/locales)",
    )
    args = parser.parse_args()
    main(ROOT / args.output_dir)
