from __future__ import annotations
import json
from pathlib import Path

PLAN_PATH = Path(r"C:\Trading\ANT_OUT\worker_execution_plan.json")


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

    data = load_json(PLAN_PATH)

    print("")
    print("WORKER EXECUTION PLAN PANEL")
    print("")

    if not data:
        print("missing execution plan file")
        return

    print("ts_utc:", s(data.get("ts_utc")))
    print("version:", s(data.get("version")))
    print("")

    headers = [
        "market",
        "enabled",
        "strategy",
        "bias",
        "size",
        "mode",
        "reason"
    ]

    rows = []

    markets = data.get("markets", {})

    for market in sorted(markets.keys()):
        row = markets.get(market, {})

        rows.append([
            market,
            s(row.get("enabled")),
            s(row.get("selected_strategy")),
            s(row.get("execution_bias")),
            s(row.get("execution_size_mult")),
            s(row.get("execution_mode")),
            s(row.get("plan_reason")),
        ])

    widths = [len(h) for h in headers]

    for row in rows:
        for i, c in enumerate(row):
            widths[i] = max(widths[i], len(c))

    def fmt(row):
        return "  ".join(c.ljust(widths[i]) for i, c in enumerate(row))

    print(fmt(headers))
    print("  ".join("-" * w for w in widths))

    for r in rows:
        print(fmt(r))

    print("")


if __name__ == "__main__":
    main()