import json
from pathlib import Path

OUT = Path(r"C:\Trading\ANT_OUT")

ROUTER = OUT / "queen_strategy_router.json"


def load(p):
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def normalize_markets(doc):
    markets = doc.get("markets", {}) or {}

    if isinstance(markets, dict):
        return {str(k): v for k, v in markets.items() if isinstance(v, dict)}

    out = {}
    if isinstance(markets, list):
        for row in markets:
            if not isinstance(row, dict):
                continue
            market = str(row.get("market", "") or "")
            if not market:
                continue
            out[market] = row
    return out


def pad(v, w):
    s = str(v)
    return s[:w].ljust(w)


def main():
    doc = load(ROUTER)
    markets = normalize_markets(doc)

    print("")
    print("QUEEN STRATEGY ROUTER PANEL")
    print("")

    headers = [
        ("market", 10),
        ("strategy", 8),
        ("family", 8),
        ("class", 16),
        ("prio", 5),
        ("bias", 7),
        ("conf", 6),
        ("reason", 24),
        ("trend", 9),
        ("e4_bias", 7),
        ("colony", 8),
        ("final", 6)
    ]

    print(" ".join(pad(h, w) for h, w in headers))
    print(" ".join("-" * w for _, w in headers))

    for m, row in sorted(markets.items()):
        strategy = row.get("active_strategy", "")
        family = row.get("strategy_family", "")
        sclass = row.get("strategy_class", "")
        prio = row.get("strategy_priority", "")

        bias = row.get("strategy_bias", row.get("route_bias", ""))
        conf = row.get("strategy_conf", row.get("route_confidence", ""))
        reason = row.get("route_reason", "")

        trend = row.get("trend", "")
        e4_bias = row.get("edge4_bias", "")
        colony = row.get("colony_bias", "")
        final = row.get("final_size_mult", "")

        values = [
            m,
            strategy,
            family,
            sclass,
            prio,
            bias,
            conf,
            reason,
            trend,
            e4_bias,
            colony,
            final
        ]

        print(" ".join(pad(v, w) for v, (_, w) in zip(values, headers)))

    print("")


if __name__ == "__main__":
    main()