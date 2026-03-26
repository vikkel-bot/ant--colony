from __future__ import annotations

import json
from pathlib import Path

HIST_PATH = Path(r"C:\Trading\ANT_OUT\worker_trade_history.json")


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
    data = load_json(HIST_PATH)

    print("")
    print("WORKER TRADE HISTORY PANEL")
    print("")

    if not data:
        print(f"missing or unreadable: {HIST_PATH}")
        return 1

    print("version:", s(data.get("version")))
    print("")

    headers = [
        "trade_id",
        "market",
        "side",
        "realized_pnl",
        "equity_after",
        "close_reason",
        "closed_ts_utc",
    ]

    rows = []
    trades = data.get("trades", [])

    if isinstance(trades, list):
        for row in trades:
            if not isinstance(row, dict):
                continue
            rows.append([
                s(row.get("trade_id")),
                s(row.get("market")),
                s(row.get("side")),
                s(row.get("realized_pnl")),
                s(row.get("equity_after")),
                s(row.get("close_reason")),
                s(row.get("closed_ts_utc")),
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

    if not rows:
        print("(no closed trades yet)")

    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())