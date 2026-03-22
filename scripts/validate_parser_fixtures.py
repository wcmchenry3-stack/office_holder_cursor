#!/usr/bin/env python3
"""Validate parser fixture manifest references and JSON value shapes."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = PROJECT_ROOT / "test_scripts" / "manifest" / "parser_tests.json"
REQUIRED_KEYS = (
    "name",
    "test_type",
    "html_file",
    "source_url",
    "config_json",
    "expected_json",
    "enabled",
)


def _entry_label(index: int, entry: Any) -> str:
    if isinstance(entry, dict):
        name = entry.get("name")
        if isinstance(name, str) and name.strip():
            return f"entry[{index}] '{name}'"
    return f"entry[{index}]"


def validate_manifest(manifest_path: Path) -> list[str]:
    errors: list[str] = []

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [f"Manifest not found: {manifest_path}"]
    except json.JSONDecodeError as exc:
        return [f"Manifest is not valid JSON: {manifest_path} ({exc})"]

    if not isinstance(payload, list):
        return [f"Manifest root must be a JSON array: {manifest_path}"]

    for index, entry in enumerate(payload):
        label = _entry_label(index, entry)

        if not isinstance(entry, dict):
            errors.append(f"{label}: manifest entry must be a JSON object")
            continue

        missing_keys = [key for key in REQUIRED_KEYS if key not in entry]
        if missing_keys:
            errors.append(f"{label}: missing required keys: {', '.join(missing_keys)}")

        html_file = entry.get("html_file")
        if not isinstance(html_file, str) or not html_file.strip():
            errors.append(f"{label}: html_file must be a non-empty string")
        else:
            html_path = Path(html_file.strip())
            if html_path.is_absolute():
                errors.append(
                    f"{label}: html_file must be a relative path, got absolute path '{html_file}'"
                )
            elif not (PROJECT_ROOT / html_path).is_file():
                errors.append(f"{label}: html_file does not exist at '{html_file}'")

        for key in ("config_json", "expected_json"):
            value = entry.get(key)
            if not isinstance(value, (dict, list)):
                actual = type(value).__name__
                errors.append(f"{label}: {key} must be a JSON object or array, got {actual}")

    return errors


def main() -> int:
    manifest_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_MANIFEST
    errors = validate_manifest(manifest_path)

    if errors:
        print(f"Parser fixture validation failed for {manifest_path}:")
        for err in errors:
            print(f"- {err}")
        return 1

    print(
        f"Parser fixture validation passed for {manifest_path} ({len(json.loads(manifest_path.read_text(encoding='utf-8')))} entries)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
