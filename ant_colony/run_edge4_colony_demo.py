from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone


OUTDIR = r"C:\Trading\ANT_OUT"
DEFAULT_MARKETS = [
    "BTC-EUR",
    "ETH-EUR",
    "SOL-EUR",
    "XRP-EUR",
    "ADA-EUR",
    "BNB-EUR",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_market_name(market: str) -> str:
    return market.replace("/", "-").replace("\\", "-").replace(":", "-")


def parse_json_file(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def main() -> int:
    os.makedirs(OUTDIR, exist_ok=True)

    root = os.path.dirname(os.path.abspath(__file__))
    worker_py = os.path.join(root, "edge4_worker_lite.py")
    colony_status_py = os.path.join(root, "edge4_colony_status_lite.py")

    markets = []
    ok_count = 0
    error_count = 0

    for market in DEFAULT_MARKETS:
        safe_market = safe_market_name(market)
        worker_json = os.path.join(OUTDIR, f"edge4_{safe_market}_worker_summary.json")

        cmd = [
            sys.executable,
            worker_py,
            "--market",
            market,
        ]

        proc = subprocess.run(cmd, capture_output=True, text=True)
        worker_summary = parse_json_file(worker_json)

        item = {
            "market": market,
            "returncode": proc.returncode,
            "ok": proc.returncode == 0,
            "worker_summary_json": worker_json,
            "worker_summary": worker_summary,
        }
        markets.append(item)

        if proc.returncode == 0:
            ok_count += 1
        else:
            error_count += 1

    colony_proc = subprocess.run(
        [sys.executable, colony_status_py],
        capture_output=True,
        text=True,
    )

    out = {
        "ts_utc": utc_now_iso(),
        "component": "run_edge4_colony_demo",
        "market_count": len(DEFAULT_MARKETS),
        "ok_count": ok_count,
        "error_count": error_count,
        "markets": markets,
        "colony_status_returncode": colony_proc.returncode,
        "colony_status_stdout": colony_proc.stdout,
        "colony_status_stderr": colony_proc.stderr,
    }

    out_json = os.path.join(OUTDIR, "edge4_colony_demo_run.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out, indent=2))
    return 0 if (error_count == 0 and colony_proc.returncode == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())