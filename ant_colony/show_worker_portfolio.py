from __future__ import annotations

import json
from pathlib import Path

PORT_PATH = Path(r"C:\Trading\ANT_OUT\worker_portfolio_state.json")


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
    data = load_json(PORT_PATH)

    print("")
    print("WORKER PORTFOLIO PANEL")
    print("")

    if not data:
        print(f"missing or unreadable: {PORT_PATH}")
        return 1

    print("version:", s(data.get("version")))
    print("")

    headers = [
        "market",
        "position",
        "entry_price",
        "mark_price",
        "pnl",
        "equity",
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
                s(row.get("entry_price")),
                s(row.get("mark_price")),
                s(row.get("pnl")),
                s(row.get("equity")),
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