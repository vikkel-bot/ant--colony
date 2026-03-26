from __future__ import annotations

import json
from pathlib import Path

CONSUMER_PATH = Path(r"C:\Trading\ANT_OUT\worker_consumer_stub.json")


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
    data = load_json(CONSUMER_PATH)

    print("")
    print("WORKER CONSUMER STUB PANEL")
    print("")

    if not data:
        print(f"missing or unreadable: {CONSUMER_PATH}")
        return 1

    print("ts_utc:", s(data.get("ts_utc")))
    print("version:", s(data.get("version")))
    print("")

    headers = [
        "market",
        "enabled",
        "strategy",
        "bias",
        "size",
        "action",
        "reason",
    ]

    rows = []
    markets = data.get("markets", {})

    if isinstance(markets, dict):
        for market in sorted(markets.keys()):
            row = markets.get(market, {}) or {}
            rows.append([
                market,
                s(row.get("consumer_enabled")),
                s(row.get("consumer_strategy")),
                s(row.get("consumer_bias")),
                s(row.get("consumer_size_mult")),
                s(row.get("consumer_action")),
                s(row.get("consumer_reason")),
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