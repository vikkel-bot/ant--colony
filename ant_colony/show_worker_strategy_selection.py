from __future__ import annotations

import json
from pathlib import Path

OUT_PATH = Path(r"C:\Trading\ANT_OUT\worker_strategy_selection.json")


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def s(value) -> str:
    if value is None:
        return ""
    return str(value)


def main() -> int:
    data = load_json(OUT_PATH)

    print("WORKER STRATEGY SELECTION PANEL")
    print()

    if not data:
        print(f"missing or unreadable: {OUT_PATH}")
        return 1

    print(f'ts_utc: {s(data.get("ts_utc"))}')
    print(f'version: {s(data.get("version"))}')
    print()

    headers = [
        "market",
        "selected_strategy",
        "selected_bias",
        "selected_size_mult",
        "selection_reason",
    ]

    rows = []
    markets = data.get("markets", {})
    if isinstance(markets, dict):
        for market in sorted(markets.keys()):
            row = markets.get(market, {}) or {}
            rows.append([
                market,
                s(row.get("selected_strategy")),
                s(row.get("selected_bias")),
                s(row.get("selected_size_mult")),
                s(row.get("selection_reason")),
            ])

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(row):
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    print(fmt_row(headers))
    print("  ".join("-" * w for w in widths))

    for row in rows:
        print(fmt_row(row))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())