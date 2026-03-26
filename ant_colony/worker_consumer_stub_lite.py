from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
CONTEXT_PATH = OUT_DIR / "worker_context.json"
OUTPUT_JSON = OUT_DIR / "worker_consumer_stub.json"
OUTPUT_TSV = OUT_DIR / "worker_consumer_stub.tsv"


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


def build_consumer(row: dict) -> dict:
    enabled = bool(row.get("context_enabled", False))
    strategy = str(row.get("context_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("context_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("context_size_mult", 0.0), 0.0)
    state = str(row.get("context_state", "IDLE_CONTEXT") or "IDLE_CONTEXT").upper()
    reason = str(row.get("context_reason", "NO_CONTEXT_REASON") or "NO_CONTEXT_REASON")

    if not enabled:
        return {
            "consumer_enabled": False,
            "consumer_strategy": "NONE",
            "consumer_bias": "NEUTRAL",
            "consumer_size_mult": 0.0,
            "consumer_action": "NO_CONSUME",
            "consumer_reason": f"CONSUMER_IDLE; {reason}",
        }

    if state != "CONTEXT_READY":
        return {
            "consumer_enabled": False,
            "consumer_strategy": strategy,
            "consumer_bias": bias,
            "consumer_size_mult": 0.0,
            "consumer_action": "WAIT_CONTEXT",
            "consumer_reason": f"CONTEXT_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "consumer_enabled": False,
            "consumer_strategy": "NONE",
            "consumer_bias": "NEUTRAL",
            "consumer_size_mult": 0.0,
            "consumer_action": "NO_CONSUME",
            "consumer_reason": f"NO_CONSUMER_STRATEGY; {reason}",
        }

    if bias not in ("LONG", "SHORT"):
        return {
            "consumer_enabled": False,
            "consumer_strategy": strategy,
            "consumer_bias": bias,
            "consumer_size_mult": 0.0,
            "consumer_action": "NO_CONSUME",
            "consumer_reason": f"INVALID_CONSUMER_BIAS; {reason}",
        }

    if size_mult <= 0:
        return {
            "consumer_enabled": False,
            "consumer_strategy": strategy,
            "consumer_bias": bias,
            "consumer_size_mult": 0.0,
            "consumer_action": "NO_CONSUME",
            "consumer_reason": f"ZERO_CONSUMER_SIZE; {reason}",
        }

    return {
        "consumer_enabled": True,
        "consumer_strategy": strategy,
        "consumer_bias": bias,
        "consumer_size_mult": round(size_mult, 4),
        "consumer_action": "CONSUME_CONTEXT",
        "consumer_reason": f"CONSUMER_READY; {reason}",
    }


def main() -> int:
    context = load_json(CONTEXT_PATH)
    context_markets = normalize_markets_block(context)
    all_markets = sorted(context_markets.keys())

    result = {
        "version": "worker_consumer_stub_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_context": str(CONTEXT_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tconsumer_enabled\tconsumer_strategy\tconsumer_bias\tconsumer_size_mult\tconsumer_action\tconsumer_reason"
    ]

    for market in all_markets:
        row = context_markets.get(market, {}) or {}
        consumer = build_consumer(row)
        result["markets"][market] = consumer

        tsv_lines.append(
            f'{market}\t{consumer["consumer_enabled"]}\t{consumer["consumer_strategy"]}\t{consumer["consumer_bias"]}\t{consumer["consumer_size_mult"]}\t{consumer["consumer_action"]}\t{consumer["consumer_reason"]}'
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