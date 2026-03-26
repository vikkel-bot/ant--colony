from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone


OUTDIR = r"C:\Trading\ANT_OUT"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_json_stdout(text: str):
    try:
        s = (text or "").strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run EDGE4 breakout demo")
    parser.add_argument("--market", default="BTC-EUR", help="Market label used in output filenames")
    args = parser.parse_args()

    os.makedirs(OUTDIR, exist_ok=True)

    root = os.path.dirname(os.path.abspath(__file__))
    edge4_py = os.path.join(root, "edge4_breakout.py")

    market = args.market
    safe_market = market.replace("/", "-").replace("\\", "-").replace(":", "-")

    out_csv = os.path.join(OUTDIR, f"edge4_{safe_market}_signals.csv")
    out_json = os.path.join(OUTDIR, f"edge4_{safe_market}_summary.json")
    hb_json = os.path.join(OUTDIR, f"edge4_{safe_market}_heartbeat.json")

    cmd = [
        sys.executable,
        edge4_py,
        "--demo",
        "1",
        "--out-csv",
        out_csv,
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    edge4_summary = parse_json_stdout(proc.stdout)

    summary = {
        "ts_utc": utc_now_iso(),
        "component": "edge4_breakout_demo",
        "market": market,
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "out_csv": out_csv,
        "ok": proc.returncode == 0,
        "edge4_summary": edge4_summary,
    }

    heartbeat = {
        "ts_utc": summary["ts_utc"],
        "component": "edge4_breakout_demo",
        "market": market,
        "state": "ok" if proc.returncode == 0 else "error",
        "ok": proc.returncode == 0,
        "long_signals": None if not edge4_summary else edge4_summary.get("long_signals"),
        "short_signals": None if not edge4_summary else edge4_summary.get("short_signals"),
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(hb_json, "w", encoding="utf-8") as f:
        json.dump(heartbeat, f, indent=2)

    print(json.dumps(summary, indent=2))

    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())