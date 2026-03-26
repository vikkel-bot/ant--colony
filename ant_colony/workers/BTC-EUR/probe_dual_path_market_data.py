from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.worker_io import get_worker_market_data


ANT_OUT = Path(r"C:\Trading\ANT_OUT")
CACHE_CANDIDATES = [
    Path(r"C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\BTC-EUR\data_cache\BTC-EUR_4h_candles.json"),
    Path(r"C:\Trading\EDGE3\ant_colony\workers\BTC-EUR\data_cache\BTC-EUR_4h_candles.json"),
    Path(r"C:\Trading\EDGE3\var\data_cache\BTC-EUR_4h_candles.json"),
]

MARKET = "BTC-EUR"
INTERVAL = "4h"
LIMIT = 50


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_cache_path():
    for p in CACHE_CANDIDATES:
        if p.exists():
            return p
    return None


def load_legacy_candles(path: Path):
    if path is None or not path.exists():
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    candles = raw.get("candles") if isinstance(raw, dict) else raw
    if not isinstance(candles, list):
        return []
    return candles


def legacy_row_to_obj(row):
    if not isinstance(row, list) or len(row) < 6:
        return None

    ts_ms = int(row[0])
    ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts_ms": ts_ms,
        "ts_utc": ts_utc,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
    }


def diff_pct(a, b):
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return None
        return ((a - b) / b) * 100.0
    except Exception:
        return None


def main() -> int:
    ANT_OUT.mkdir(parents=True, exist_ok=True)

    cache_path = resolve_cache_path()
    legacy_rows = load_legacy_candles(cache_path)

    legacy_last_live = legacy_row_to_obj(legacy_rows[-1]) if len(legacy_rows) >= 1 else None
    legacy_last_closed = legacy_row_to_obj(legacy_rows[-2]) if len(legacy_rows) >= 2 else None

    adapter_result = get_worker_market_data(
        market=MARKET,
        interval=INTERVAL,
        limit=LIMIT,
    )
    adapter_rows = adapter_result.get("rows") or []

    adapter_last_live = adapter_rows[-1] if len(adapter_rows) >= 1 else None
    adapter_last_closed = adapter_rows[-2] if len(adapter_rows) >= 2 else None

    probe = {
        "ts_utc": utc_now_iso(),
        "market": MARKET,
        "interval": INTERVAL,
        "cache_path_used": str(cache_path) if cache_path else None,
        "legacy_ok": len(legacy_rows) >= 2,
        "adapter_ok": bool(adapter_result.get("ok")) and len(adapter_rows) >= 2,
        "compare_mode": "last_closed_candle",
        "legacy_last_live": legacy_last_live,
        "adapter_last_live": adapter_last_live,
        "legacy_last_closed": legacy_last_closed,
        "adapter_last_closed": adapter_last_closed,
        "diff": None,
        "meta": {
            "adapter_count": adapter_result.get("count"),
            "adapter_meta": adapter_result.get("meta"),
        },
    }

    if legacy_last_closed and adapter_last_closed:
        probe["diff"] = {
            "same_ts": legacy_last_closed.get("ts_utc") == adapter_last_closed.get("ts_utc"),
            "open_diff_pct": diff_pct(adapter_last_closed.get("open"), legacy_last_closed.get("open")),
            "high_diff_pct": diff_pct(adapter_last_closed.get("high"), legacy_last_closed.get("high")),
            "low_diff_pct": diff_pct(adapter_last_closed.get("low"), legacy_last_closed.get("low")),
            "close_diff_pct": diff_pct(adapter_last_closed.get("close"), legacy_last_closed.get("close")),
            "volume_diff_pct": diff_pct(adapter_last_closed.get("volume"), legacy_last_closed.get("volume")),
        }

    print("=== BTC WORKER DUAL PATH PROBE (LAST CLOSED CANDLE) ===")
    print(json.dumps(probe, indent=2))

    out_path = ANT_OUT / "btc_worker_dual_path_probe.json"
    out_path.write_text(json.dumps(probe, indent=2), encoding="utf-8")
    print(f"WROTE {out_path}")

    return 0 if adapter_result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())