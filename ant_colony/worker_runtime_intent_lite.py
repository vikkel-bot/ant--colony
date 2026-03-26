from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
CONSUMER_PATH = OUT_DIR / "worker_consumer_stub.json"
OUTPUT_JSON = OUT_DIR / "worker_runtime_intent.json"
OUTPUT_TSV = OUT_DIR / "worker_runtime_intent.tsv"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def normalize_markets_block(raw: dict) -> dict:
    markets = raw.get("markets", {})
    return markets if isinstance(markets, dict) else {}


def build_intent(row: dict) -> dict:
    enabled = bool(row.get("consumer_enabled", False))
    strategy = str(row.get("consumer_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("consumer_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("consumer_size_mult", 0.0), 0.0)
    action = str(row.get("consumer_action", "NO_CONSUME") or "NO_CONSUME").upper()
    reason = str(row.get("consumer_reason", "NO_CONSUMER_REASON") or "NO_CONSUMER_REASON")

    if not enabled:
        return {
            "intent_enabled": False,
            "intent_strategy": "NONE",
            "intent_bias": "NEUTRAL",
            "intent_size_mult": 0.0,
            "intent_state": "INTENT_IDLE",
            "intent_reason": f"NO_RUNTIME_INTENT; {reason}",
        }

    if action != "CONSUME_CONTEXT":
        return {
            "intent_enabled": False,
            "intent_strategy": strategy,
            "intent_bias": bias,
            "intent_size_mult": 0.0,
            "intent_state": "INTENT_WAIT",
            "intent_reason": f"CONSUMER_ACTION_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "intent_enabled": False,
            "intent_strategy": "NONE",
            "intent_bias": "NEUTRAL",
            "intent_size_mult": 0.0,
            "intent_state": "INTENT_INVALID",
            "intent_reason": f"NO_INTENT_STRATEGY; {reason}",
        }

    if bias not in ("LONG", "SHORT"):
        return {
            "intent_enabled": False,
            "intent_strategy": strategy,
            "intent_bias": bias,
            "intent_size_mult": 0.0,
            "intent_state": "INTENT_INVALID",
            "intent_reason": f"INVALID_INTENT_BIAS; {reason}",
        }

    if size_mult <= 0:
        return {
            "intent_enabled": False,
            "intent_strategy": strategy,
            "intent_bias": bias,
            "intent_size_mult": 0.0,
            "intent_state": "INTENT_INVALID",
            "intent_reason": f"ZERO_INTENT_SIZE; {reason}",
        }

    return {
        "intent_enabled": True,
        "intent_strategy": strategy,
        "intent_bias": bias,
        "intent_size_mult": round(size_mult, 4),
        "intent_state": "INTENT_READY",
        "intent_reason": f"RUNTIME_INTENT_READY; {reason}",
    }


def main() -> int:
    consumer = load_json(CONSUMER_PATH)
    consumer_markets = normalize_markets_block(consumer)
    all_markets = sorted(consumer_markets.keys())

    result = {
        "version": "worker_runtime_intent_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_consumer_stub": str(CONSUMER_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tintent_enabled\tintent_strategy\tintent_bias\tintent_size_mult\tintent_state\tintent_reason"
    ]

    for market in all_markets:
        row = consumer_markets.get(market, {}) or {}
        intent = build_intent(row)
        result["markets"][market] = intent

        tsv_lines.append(
            f'{market}\t{intent["intent_enabled"]}\t{intent["intent_strategy"]}\t{intent["intent_bias"]}\t{intent["intent_size_mult"]}\t{intent["intent_state"]}\t{intent["intent_reason"]}'
        )

    OUTPUT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    OUTPUT_TSV.write_text("\n".join(tsv_lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "output_json": str(OUTPUT_JSON),
        "output_tsv": str(OUTPUT_TSV),
        "markets": len(all_markets),
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())