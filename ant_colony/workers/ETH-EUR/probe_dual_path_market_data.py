import json
import sys
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ant_colony.worker_io.market_data_interface import get_worker_market_data

ANT_OUT = Path(r"C:\Trading\ANT_OUT")


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> int:
    ANT_OUT.mkdir(parents=True, exist_ok=True)

    result = get_worker_market_data(
        market="BTC-EUR",
        interval="4h",
        limit=50,
    )

    rows = result.get("rows") or []
    meta = result.get("meta") or {}

    adapter_ok = len(rows) >= 2
    last_row = rows[-1] if rows else None
    last_closed = rows[-2] if len(rows) >= 2 else None

    probe = {
        "ts_utc": utc_now(),
        "market": "BTC-EUR",
        "interval": "4h",
        "probe_mode": "adapter_source_of_truth",
        "adapter_ok": adapter_ok,
        "rows_count": len(rows),
        "last_row": last_row,
        "last_closed_row": last_closed,
        "meta": meta,
        "error": None if adapter_ok else "INSUFFICIENT_ADAPTER_ROWS",
    }

    print("=== BTC ADAPTER SOURCE PROBE ===")
    print(json.dumps(probe, indent=2))

    out_path = ANT_OUT / "btc_worker_dual_path_probe.json"
    out_path.write_text(json.dumps(probe, indent=2), encoding="utf-8")
    print(f"WROTE {out_path}")

    status = {
        "ts_utc": probe["ts_utc"],
        "last_probe_ts_utc": probe["ts_utc"],
        "probe_enabled": True,
        "adapter_ok": adapter_ok,
        "parity_ok": adapter_ok,
        "latency_ms": meta.get("latency_ms"),
        "adapter_source": "bitvavo_adapter",
        "market": "BTC-EUR",
        "interval": "4h",
        "error": probe["error"],
    }

    status_path = ANT_OUT / "btc_worker_adapter_probe_status.json"
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(f"WROTE {status_path}")

    return 0 if adapter_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
