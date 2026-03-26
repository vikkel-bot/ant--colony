from __future__ import annotations

import json
import os
import sys

OUTDIR = r"C:\Trading\ANT_OUT"
ALLOC_FILE_COMBINED = os.path.join(OUTDIR, "alloc_targets_combined.json")
ALLOC_FILE_FALLBACK = os.path.join(OUTDIR, "alloc_targets.json")


def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def main() -> int:
    market = sys.argv[1] if len(sys.argv) > 1 else ""
    if not market:
        print("ALLOC_MULT=1.0")
        print("ALLOC_REASON=NO_MARKET")
        print("ALLOC_GATE=ALLOW")
        print("ALLOC_FILE_USED=NONE")
        return 0

    path = None
    if os.path.exists(ALLOC_FILE_COMBINED):
        path = ALLOC_FILE_COMBINED
    elif os.path.exists(ALLOC_FILE_FALLBACK):
        path = ALLOC_FILE_FALLBACK

    if path is None:
        print("ALLOC_MULT=1.0")
        print("ALLOC_REASON=NO_ALLOC_FILE")
        print("ALLOC_GATE=ALLOW")
        print("ALLOC_FILE_USED=NONE")
        return 0

    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception:
        print("ALLOC_MULT=1.0")
        print("ALLOC_REASON=ALLOC_PARSE_FAIL")
        print("ALLOC_GATE=ALLOW")
        print(f"ALLOC_FILE_USED={path}")
        return 0

    default_mult = safe_float(j.get("default_size_mult", 1.0), 1.0)
    markets = j.get("markets", {}) or {}
    obj = markets.get(market, {}) or {}

    mult = safe_float(obj.get("target_size_mult", default_mult), default_mult)
    reason = str(obj.get("reason", "DEFAULT")).replace("\r", " ").replace("\n", " ")
    gate = str(obj.get("gate", "ALLOW")).strip().upper() or "ALLOW"

    if gate == "BLOCK":
        mult = 0.0

    print(f"ALLOC_MULT={mult}")
    print(f"ALLOC_REASON={reason}")
    print(f"ALLOC_GATE={gate}")
    print(f"ALLOC_FILE_USED={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())