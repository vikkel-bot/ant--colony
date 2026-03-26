from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
ORCH_PATH = OUT_DIR / "worker_orchestration_stub.json"
OUTPUT_JSON = OUT_DIR / "worker_context.json"
OUTPUT_TSV = OUT_DIR / "worker_context.tsv"


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


def build_context(row: dict) -> dict:
    orch_enabled = bool(row.get("orchestration_enabled", False))
    strategy = str(row.get("orchestration_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("orchestration_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("orchestration_size_mult", 0.0), 0.0)
    action = str(row.get("orchestration_action", "NOOP") or "NOOP").upper()
    reason = str(row.get("orchestration_reason", "NO_ORCH_REASON") or "NO_ORCH_REASON")

    if not orch_enabled:
        return {
            "context_enabled": False,
            "context_strategy": "NONE",
            "context_bias": "NEUTRAL",
            "context_size_mult": 0.0,
            "context_state": "IDLE_CONTEXT",
            "context_reason": f"NO_CONTEXT; {reason}",
        }

    if action != "PREPARE_WORKER_CONTEXT":
        return {
            "context_enabled": False,
            "context_strategy": strategy,
            "context_bias": bias,
            "context_size_mult": 0.0,
            "context_state": "WAIT_CONTEXT",
            "context_reason": f"ORCH_ACTION_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "context_enabled": False,
            "context_strategy": "NONE",
            "context_bias": "NEUTRAL",
            "context_size_mult": 0.0,
            "context_state": "INVALID_CONTEXT",
            "context_reason": f"NO_CONTEXT_STRATEGY; {reason}",
        }

    if bias not in ("LONG", "SHORT"):
        return {
            "context_enabled": False,
            "context_strategy": strategy,
            "context_bias": bias,
            "context_size_mult": 0.0,
            "context_state": "INVALID_CONTEXT",
            "context_reason": f"INVALID_CONTEXT_BIAS; {reason}",
        }

    if size_mult <= 0:
        return {
            "context_enabled": False,
            "context_strategy": strategy,
            "context_bias": bias,
            "context_size_mult": 0.0,
            "context_state": "INVALID_CONTEXT",
            "context_reason": f"ZERO_CONTEXT_SIZE; {reason}",
        }

    return {
        "context_enabled": True,
        "context_strategy": strategy,
        "context_bias": bias,
        "context_size_mult": round(size_mult, 4),
        "context_state": "CONTEXT_READY",
        "context_reason": f"WORKER_CONTEXT_READY; {reason}",
    }


def main() -> int:
    orch = load_json(ORCH_PATH)
    orch_markets = normalize_markets_block(orch)
    all_markets = sorted(orch_markets.keys())

    result = {
        "version": "worker_context_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_orchestration_stub": str(ORCH_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tcontext_enabled\tcontext_strategy\tcontext_bias\tcontext_size_mult\tcontext_state\tcontext_reason"
    ]

    for market in all_markets:
        row = orch_markets.get(market, {}) or {}
        ctx = build_context(row)
        result["markets"][market] = ctx

        tsv_lines.append(
            f'{market}\t{ctx["context_enabled"]}\t{ctx["context_strategy"]}\t{ctx["context_bias"]}\t{ctx["context_size_mult"]}\t{ctx["context_state"]}\t{ctx["context_reason"]}'
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