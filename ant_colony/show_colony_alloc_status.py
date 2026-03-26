import json
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

ALLOC_PATH = OUT_DIR / "alloc_targets.json"
RISK_PATH = OUT_DIR / "colony_risk_targets.json"
COMBINED_PATH = OUT_DIR / "alloc_targets_combined.json"
HEALTH_PATH = OUT_DIR / "market_health.json"


def s(v):
    if v is None:
        return ""
    return str(v)


def pad(text, width):
    text = s(text)
    if len(text) >= width:
        return text[:width]
    return text + (" " * (width - len(text)))


def load_json(path: Path):
    if not path.exists():
        return None, f"missing:{path.name}"

    encodings = ["utf-8-sig", "utf-8"]
    last_error = None

    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            return json.loads(text), None
        except Exception as e:
            last_error = e

    return None, f"read_error:{path.name}:{last_error}"


def get_market_map(doc):
    if not isinstance(doc, dict):
        return {}
    markets = doc.get("markets", {})
    if isinstance(markets, dict):
        return markets
    return {}


def as_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "."))
    except Exception:
        return default


def alloc_gate_from_mult(v):
    x = as_float(v, None)
    if x is None:
        return ""
    if x <= 0:
        return "BLOCK"
    return "ALLOW"


def first_present(d, keys, default=""):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


def main():
    alloc_json, alloc_err = load_json(ALLOC_PATH)
    risk_json, risk_err = load_json(RISK_PATH)
    combined_json, combined_err = load_json(COMBINED_PATH)
    health_json, health_err = load_json(HEALTH_PATH)

    alloc_markets = get_market_map(alloc_json)
    risk_markets = get_market_map(risk_json)
    combined_markets = get_market_map(combined_json)
    health_markets = get_market_map(health_json)

    all_markets = sorted(
        set(alloc_markets.keys()) |
        set(risk_markets.keys()) |
        set(combined_markets.keys()) |
        set(health_markets.keys())
    )

    ts_utc = (
        first_present(combined_json or {}, ["ts_utc"], "") or
        first_present(health_json or {}, ["ts_utc"], "") or
        first_present(risk_json or {}, ["ts_utc"], "") or
        first_present(alloc_json or {}, ["ts_utc"], "")
    )

    print("")
    print("COLONY CONTROL PANEL  |  alloc + risk + health + combined")
    print(f"ts_utc={ts_utc}  market_count={len(all_markets)}")
    print("")

    headers = [
        ("market", 10),
        ("a_gate", 8),
        ("a_mult", 8),
        ("a_reason", 14),
        ("r_gate", 8),
        ("r_mult", 8),
        ("h_gate", 8),
        ("h_mult", 8),
        ("f_gate", 8),
        ("f_mult", 8),
        ("reason", 34),
    ]

    header_line = " ".join(pad(name, width) for name, width in headers)
    sep_line = " ".join("-" * width for _, width in headers)

    print(header_line)
    print(sep_line)

    for market in all_markets:
        alloc_row = alloc_markets.get(market, {}) or {}
        risk_row = risk_markets.get(market, {}) or {}
        combined_row = combined_markets.get(market, {}) or {}
        health_row = health_markets.get(market, {}) or {}

        alloc_mult = first_present(alloc_row, ["target_size_mult", "size_mult", "alloc_mult"], "")
        alloc_gate = alloc_gate_from_mult(alloc_mult)
        alloc_reason = first_present(alloc_row, ["reason"], "")

        risk_gate = first_present(risk_row, ["gate", "risk_gate"], "")
        risk_mult = first_present(
            risk_row,
            ["target_size_mult", "size_mult", "risk_mult", "risk_size_mult"],
            ""
        )

        health_gate = first_present(health_row, ["health_gate"], "")
        health_mult = first_present(health_row, ["health_size_mult"], "")

        final_gate = first_present(combined_row, ["gate", "alloc_gate", "combined_gate"], "")
        final_mult = first_present(
            combined_row,
            ["target_size_mult", "size_mult", "alloc_mult", "combined_size_mult"],
            ""
        )

        reason = first_present(
            combined_row,
            ["reason", "combined_reason", "why"],
            ""
        )

        values = [
            market,
            alloc_gate,
            alloc_mult,
            alloc_reason,
            risk_gate,
            risk_mult,
            health_gate,
            health_mult,
            final_gate,
            final_mult,
            reason,
        ]

        print(" ".join(pad(v, w) for v, (_, w) in zip(values, headers)))

    print("")
    print("SOURCES")
    print(f"alloc_source={ALLOC_PATH}")
    print(f"risk_source={RISK_PATH}")
    print(f"health_source={HEALTH_PATH}")
    print(f"combined_source={COMBINED_PATH}")
    print(f"alloc_error={alloc_err}")
    print(f"risk_error={risk_err}")
    print(f"health_error={health_err}")
    print(f"combined_error={combined_err}")
    print("")


if __name__ == "__main__":
    main()