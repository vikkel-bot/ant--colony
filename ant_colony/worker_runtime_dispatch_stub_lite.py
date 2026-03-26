from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
INTENT_PATH = OUT_DIR / "worker_runtime_intent.json"

OUTPUT_JSON = OUT_DIR / "worker_runtime_dispatch.json"
OUTPUT_TSV = OUT_DIR / "worker_runtime_dispatch.tsv"


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")


def load_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def f(x, default=0.0):
    try:
        return float(x)
    except:
        return default


def build_dispatch(row):

    enabled = bool(row.get("intent_enabled", False))
    strategy = str(row.get("intent_strategy","NONE")).upper()
    bias = str(row.get("intent_bias","NEUTRAL")).upper()
    size = f(row.get("intent_size_mult",0))
    state = str(row.get("intent_state","INTENT_IDLE")).upper()
    reason = str(row.get("intent_reason",""))

    if not enabled:
        return {
            "dispatch_enabled": False,
            "dispatch_strategy": "NONE",
            "dispatch_bias": "NEUTRAL",
            "dispatch_size_mult": 0.0,
            "dispatch_action": "NO_DISPATCH",
            "dispatch_reason": f"INTENT_DISABLED; {reason}",
        }

    if state != "INTENT_READY":
        return {
            "dispatch_enabled": False,
            "dispatch_strategy": strategy,
            "dispatch_bias": bias,
            "dispatch_size_mult": 0.0,
            "dispatch_action": "WAIT_INTENT",
            "dispatch_reason": f"INTENT_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "dispatch_enabled": False,
            "dispatch_strategy": "NONE",
            "dispatch_bias": "NEUTRAL",
            "dispatch_size_mult": 0.0,
            "dispatch_action": "INVALID_STRATEGY",
            "dispatch_reason": f"NO_STRATEGY; {reason}",
        }

    if bias not in ("LONG","SHORT"):
        return {
            "dispatch_enabled": False,
            "dispatch_strategy": strategy,
            "dispatch_bias": bias,
            "dispatch_size_mult": 0.0,
            "dispatch_action": "INVALID_BIAS",
            "dispatch_reason": f"INVALID_BIAS; {reason}",
        }

    if size <= 0:
        return {
            "dispatch_enabled": False,
            "dispatch_strategy": strategy,
            "dispatch_bias": bias,
            "dispatch_size_mult": 0.0,
            "dispatch_action": "ZERO_SIZE",
            "dispatch_reason": f"ZERO_SIZE; {reason}",
        }

    return {
        "dispatch_enabled": True,
        "dispatch_strategy": strategy,
        "dispatch_bias": bias,
        "dispatch_size_mult": round(size,4),
        "dispatch_action": "DISPATCH_READY",
        "dispatch_reason": f"DISPATCH_READY; {reason}",
    }


def main():

    intent = load_json(INTENT_PATH)
    markets = intent.get("markets",{})

    result = {
        "version": "worker_runtime_dispatch_stub_v1",
        "ts_utc": now_utc(),
        "markets": {}
    }

    tsv = [
        "market\tdispatch_enabled\tdispatch_strategy\tdispatch_bias\tdispatch_size_mult\tdispatch_action\tdispatch_reason"
    ]

    for m in sorted(markets.keys()):

        row = markets.get(m,{})
        d = build_dispatch(row)

        result["markets"][m] = d

        tsv.append(
            f"{m}\t{d['dispatch_enabled']}\t{d['dispatch_strategy']}\t{d['dispatch_bias']}\t{d['dispatch_size_mult']}\t{d['dispatch_action']}\t{d['dispatch_reason']}"
        )

    OUTPUT_JSON.write_text(json.dumps(result,indent=2),encoding="utf-8")
    OUTPUT_TSV.write_text("\n".join(tsv)+"\n",encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "output_json": str(OUTPUT_JSON),
        "output_tsv": str(OUTPUT_TSV),
        "markets": len(markets)
    },indent=2))


if __name__ == "__main__":
    main()