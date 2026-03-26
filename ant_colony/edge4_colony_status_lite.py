from __future__ import annotations

import glob
import json
import os
from datetime import datetime, timezone


OUTDIR = r"C:\Trading\ANT_OUT"
OUT_JSON = os.path.join(OUTDIR, "edge4_colony_status.json")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def main() -> int:
    os.makedirs(OUTDIR, exist_ok=True)

    pattern = os.path.join(OUTDIR, "edge4_status_*.json")
    files = sorted(glob.glob(pattern))

    markets = {}
    ok_count = 0
    error_count = 0

    for path in files:
        j = read_json(path)
        if not j:
            continue

        market = str(j.get("market", "UNKNOWN"))
        state = str(j.get("state", "unknown"))
        ok = bool(j.get("ok", False))

        if ok:
            ok_count += 1
        else:
            error_count += 1

        markets[market] = {
            "component": j.get("component"),
            "state": state,
            "ok": ok,
            "long_signals": j.get("long_signals"),
            "short_signals": j.get("short_signals"),
            "worker_log": j.get("worker_log"),
            "worker_summary_json": j.get("worker_summary_json"),
            "edge4_heartbeat_json": j.get("edge4_heartbeat_json"),
        }

    out = {
        "ts_utc": utc_now_iso(),
        "component": "edge4_colony_status_lite",
        "markets": markets,
        "market_count": len(markets),
        "ok_count": ok_count,
        "error_count": error_count,
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())