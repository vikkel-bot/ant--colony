from __future__ import annotations

import json
from pathlib import Path

LIFE_PATH = Path(r"C:\Trading\ANT_OUT\worker_trade_lifecycle.json")


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
    data = load_json(LIFE_PATH)

    print("")
    print("WORKER TRADE LIFECYCLE PANEL")
    print("")

    if not data:
        print(f"missing or unreadable: {LIFE_PATH}")
        return 1

    print("version:", s(data.get("version")))
    print("")

    headers = [
        "market",
        "current_pos",
        "next_pos",
        "trade_state",
        "action",
        "realized_pnl",
        "equity",
        "reason",
    ]

    rows = []
    markets = data.get("markets", [])

    if isinstance(markets, list):
        for row in markets:
            if not isinstance(row, dict):
                continue
            rows.append([
                s(row.get("market")),
                s(row.get("current_position")),
                s(row.get("next_position")),
                s(row.get("trade_state")),
                s(row.get("lifecycle_action")),
                s(row.get("realized_pnl")),
                s(row.get("equity")),
                s(row.get("lifecycle_reason")),
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