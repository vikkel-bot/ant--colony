import json
from pathlib import Path


STATUS_PATH = Path(r"C:\Trading\ANT_OUT\combined_colony_status.json")


def s(v):
    if v is None:
        return ""
    return str(v)


def pad(text, width):
    text = s(text)
    if len(text) >= width:
        return text[:width]
    return text + (" " * (width - len(text)))


def main():
    if not STATUS_PATH.exists():
        print(f"MISSING {STATUS_PATH}")
        return

    data = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    ts_utc = data.get("ts_utc", "")
    component = data.get("component", "")
    market_count = data.get("market_count", 0)
    markets = data.get("markets", {})

    print("")
    print(f"COMBINED COLONY STATUS  |  {component}")
    print(f"ts_utc={ts_utc}  market_count={market_count}")
    print("")

    headers = [
        ("market", 10),
        ("e3_gate", 10),
        ("e3_trend", 10),
        ("e4_state", 10),
        ("h_gate", 10),
        ("h_mult", 8),
        ("h_reason", 48),
        ("e4_long", 8),
        ("e4_short", 9),
    ]

    header_line = " ".join(pad(name, width) for name, width in headers)
    sep_line = " ".join("-" * width for _, width in headers)

    print(header_line)
    print(sep_line)

    for market in sorted(markets.keys()):
        row = markets[market]
        edge3 = row.get("edge3") or {}
        edge4 = row.get("edge4") or {}
        health = row.get("health") or {}

        values = [
            market,
            edge3.get("gate"),
            edge3.get("cb20_trend"),
            edge4.get("state"),
            health.get("health_gate"),
            health.get("health_size_mult"),
            health.get("health_reason"),
            edge4.get("long_signals"),
            edge4.get("short_signals"),
        ]

        print(" ".join(pad(v, w) for v, (_, w) in zip(values, headers)))

    print("")

    sources = data.get("sources", {})
    print("SOURCES")
    print(f"edge3_source={sources.get('edge3_source', '')}")
    print(f"edge4_source={sources.get('edge4_source', '')}")
    print(f"health_source={sources.get('health_source', '')}")
    print(f"edge3_error={sources.get('edge3_error', '')}")
    print(f"edge4_error={sources.get('edge4_error', '')}")
    print(f"health_error={sources.get('health_error', '')}")
    print("")


if __name__ == "__main__":
    main()