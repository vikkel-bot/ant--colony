import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")
EDGE4_COLONY_STATUS = OUT_DIR / "edge4_colony_status.json"
OUT_JSON = OUT_DIR / "edge4_signal_lite.json"
OUT_TSV = OUT_DIR / "edge4_signal_lite.tsv"


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def safe_int(v, default=0):
    try:
        if v is None or str(v).strip() == "":
            return default
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return default


def derive_bias(long_signals: int, short_signals: int) -> str:
    total = long_signals + short_signals
    if total <= 0:
        return "NEUTRAL"
    if long_signals > short_signals:
        return "LONG"
    if short_signals > long_signals:
        return "SHORT"
    return "MIXED"


def derive_strength(long_signals: int, short_signals: int, state: str, ok: bool) -> float:
    total = long_signals + short_signals
    if total <= 0:
        return 0.0
    if str(state).lower() != "ok" or not bool(ok):
        return 0.0
    return round(abs(long_signals - short_signals) / total, 6)


def write_tsv(ts_utc: str, rows: list[dict]):
    headers = [
        "ts_utc",
        "market",
        "edge4_state",
        "ok",
        "long_signals",
        "short_signals",
        "total_signals",
        "signal_bias",
        "signal_strength",
        "edge4_enabled",
    ]

    lines = ["\t".join(headers)]

    for row in rows:
        values = [
            ts_utc,
            str(row.get("market", "")),
            str(row.get("edge4_state", "")),
            str(row.get("ok", "")),
            str(row.get("long_signals", "")),
            str(row.get("short_signals", "")),
            str(row.get("total_signals", "")),
            str(row.get("signal_bias", "")),
            str(row.get("signal_strength", "")),
            str(row.get("edge4_enabled", "")),
        ]
        lines.append("\t".join(values))

    OUT_TSV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    edge4_doc, edge4_err = load_json(EDGE4_COLONY_STATUS)
    if edge4_doc is None:
        out = {
            "ts_utc": utc_now(),
            "component": "edge4_signal_lite",
            "source": str(EDGE4_COLONY_STATUS),
            "source_error": edge4_err,
            "market_count": 0,
            "markets": {},
        }
        OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
        write_tsv(out["ts_utc"], [])
        print(f"WROTE {OUT_JSON}")
        print(f"WROTE {OUT_TSV}")
        return 0

    src_markets = edge4_doc.get("markets", {}) or {}
    ts_utc = utc_now()

    out_markets = {}
    out_rows = []

    for market in sorted(src_markets.keys()):
        row = src_markets.get(market, {}) or {}

        state = row.get("state", "")
        ok = bool(row.get("ok", False))
        long_signals = safe_int(row.get("long_signals", 0), 0)
        short_signals = safe_int(row.get("short_signals", 0), 0)
        total_signals = long_signals + short_signals

        signal_bias = derive_bias(long_signals, short_signals)
        signal_strength = derive_strength(long_signals, short_signals, state, ok)
        edge4_enabled = bool(ok) and str(state).lower() == "ok"

        market_obj = {
            "market": market,
            "edge4_state": state,
            "ok": ok,
            "long_signals": long_signals,
            "short_signals": short_signals,
            "total_signals": total_signals,
            "signal_bias": signal_bias,
            "signal_strength": signal_strength,
            "edge4_enabled": edge4_enabled,
        }

        out_markets[market] = market_obj
        out_rows.append(market_obj)

    out = {
        "ts_utc": ts_utc,
        "component": "edge4_signal_lite",
        "source": str(EDGE4_COLONY_STATUS),
        "source_error": edge4_err,
        "market_count": len(out_markets),
        "markets": out_markets,
    }

    OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    write_tsv(ts_utc, out_rows)

    print(f"WROTE {OUT_JSON}")
    print(f"WROTE {OUT_TSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())