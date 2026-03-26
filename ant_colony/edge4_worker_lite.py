from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone


OUTDIR = r"C:\Trading\ANT_OUT"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_market_name(market: str) -> str:
    return market.replace("/", "-").replace("\\", "-").replace(":", "-")


def parse_json_file(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def append_log(path: str, line: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="EDGE4 worker lite")
    parser.add_argument("--market", default="BTC-EUR")
    args = parser.parse_args()

    os.makedirs(OUTDIR, exist_ok=True)

    market = args.market
    safe_market = safe_market_name(market)

    root = os.path.dirname(os.path.abspath(__file__))
    runner_py = os.path.join(root, "run_edge4_breakout_demo.py")

    summary_json = os.path.join(OUTDIR, f"edge4_{safe_market}_summary.json")
    heartbeat_json = os.path.join(OUTDIR, f"edge4_{safe_market}_heartbeat.json")
    worker_json = os.path.join(OUTDIR, f"edge4_{safe_market}_worker_summary.json")
    worker_log = os.path.join(OUTDIR, f"edge4_worker_{safe_market}.log")
    status_json = os.path.join(OUTDIR, f"edge4_status_{safe_market}.json")

    cmd = [
        sys.executable,
        runner_py,
        "--market",
        market,
    ]

    append_log(worker_log, f"{utc_now_iso()} EDGE4_WORKER START market={market}")

    proc = subprocess.run(cmd, capture_output=True, text=True)

    edge4_summary = parse_json_file(summary_json)
    edge4_hb = parse_json_file(heartbeat_json)

    worker_summary = {
        "ts_utc": utc_now_iso(),
        "component": "edge4_worker_lite",
        "market": market,
        "cmd": cmd,
        "returncode": proc.returncode,
        "ok": proc.returncode == 0,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "edge4_summary_json": summary_json,
        "edge4_heartbeat_json": heartbeat_json,
        "edge4_summary": edge4_summary,
        "edge4_heartbeat": edge4_hb,
        "worker_log": worker_log,
        "status_json": status_json,
    }

    with open(worker_json, "w", encoding="utf-8") as f:
        json.dump(worker_summary, f, indent=2)

    if proc.returncode == 0:
        long_signals = None if not edge4_hb else edge4_hb.get("long_signals")
        short_signals = None if not edge4_hb else edge4_hb.get("short_signals")
        append_log(
            worker_log,
            f"{utc_now_iso()} EDGE4_WORKER OK market={market} long_signals={long_signals} short_signals={short_signals}",
        )
    else:
        long_signals = None
        short_signals = None
        append_log(
            worker_log,
            f"{utc_now_iso()} EDGE4_WORKER ERROR market={market} returncode={proc.returncode}",
        )

    status = {
        "ts_utc": utc_now_iso(),
        "component": "edge4_worker_lite",
        "market": market,
        "state": "ok" if proc.returncode == 0 else "error",
        "ok": proc.returncode == 0,
        "long_signals": long_signals,
        "short_signals": short_signals,
        "worker_log": worker_log,
        "worker_summary_json": worker_json,
        "edge4_heartbeat_json": heartbeat_json,
    }

    with open(status_json, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(json.dumps(worker_summary, indent=2))
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())