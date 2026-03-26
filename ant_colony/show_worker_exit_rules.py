from __future__ import annotations

import json
from pathlib import Path

RULES_PATH = Path(r"C:\Trading\ANT_OUT\worker_exit_rules.json")


def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def s(v):
    if v is None:
        return ""
    return str(v)


def main():
    data = load_json(RULES_PATH)

    print("")
    print("WORKER EXIT RULES PANEL")
    print("")

    if not data:
        print(f"missing or unreadable: {RULES_PATH}")
        return 1

    print("version:", s(data.get("version")))
    print("")

    headers = [
        "market",
        "position",
        "pnl",
        "tp_level",
        "sl_level",
        "rule_action",
        "rule_reason",
    ]

    rows = []
    markets = data.get("markets", [])

    if isinstance(markets, list):
        for row in markets:
            if not isinstance(row, dict):
                continue
            rows.append([
                s(row.get("market")),
                s(row.get("position")),
                s(row.get("pnl")),
                s(row.get("tp_level")),
                s(row.get("sl_level")),
                s(row.get("rule_action")),
                s(row.get("rule_reason")),
            ])

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row):
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    print(fmt(headers))
    print("  ".join("-" * w for w in widths))

    for row in rows:
        print(fmt(row))

    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())