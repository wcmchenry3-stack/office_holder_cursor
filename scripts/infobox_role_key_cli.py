#!/usr/bin/env python3
"""Set and verify infobox_role_key via HTTP API.

Usage:
  python scripts/infobox_role_key_cli.py --office-id 1 --table-no 1 --role-key "chief judge"
"""

from __future__ import annotations

import argparse
import json
import sys
from urllib import request


def _http_json(url: str, method: str = "GET", payload: dict | None = None) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers, method=method)
    with request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base app URL")
    p.add_argument("--office-id", type=int, required=True)
    p.add_argument("--table-no", type=int, default=1)
    p.add_argument("--role-key", default="")
    args = p.parse_args()

    set_url = f"{args.base_url}/api/offices/{args.office_id}/set-infobox-role-key"
    get_url = f"{args.base_url}/api/offices/{args.office_id}/table-configs?table_no={args.table_no}"

    try:
        save_res = _http_json(set_url, method="POST", payload={"table_no": args.table_no, "infobox_role_key": args.role_key})
        print("Save response:")
        print(json.dumps(save_res, indent=2))

        detail_res = _http_json(get_url)
        print("\nSaved table details:")
        print(json.dumps(detail_res, indent=2))
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
