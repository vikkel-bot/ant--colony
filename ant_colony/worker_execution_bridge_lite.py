from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
EXEC_PLAN_PATH = OUT_DIR / "worker_execution_plan.json"
OUTPUT_JSON = OUT_DIR / "worker_execution_bridge.json"
OUTPUT_TSV = OUT_DIR / "worker_execution_bridge.tsv"


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


def build_bridge(row: dict) -> dict:
    enabled = bool(row.get("enabled", False))
    strategy = str(row.get("selected_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("execution_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("execution_size_mult", 0.0), 0.0)
    exec_mode = str(row.get("execution_mode", "IDLE") or "IDLE").upper()
    reason = str(row.get("plan_reason", "NO_PLAN_REASON") or "NO_PLAN_REASON")

    if not enabled:
        return {
            "worker_enabled": False,
            "worker_target_strategy": "NONE",
            "worker_target_bias": "NEUTRAL",
            "worker_target_size_mult": 0.0,
            "worker_execution_state": "STANDBY",
            "bridge_reason": f"WORKER_STANDBY; {reason}",
        }

    if strategy == "NONE":
        return {
            "worker_enabled": False,
            "worker_target_strategy": "NONE",
            "worker_target_bias": "NEUTRAL",
            "worker_target_size_mult": 0.0,
            "worker_execution_state": "STANDBY",
            "bridge_reason": f"NO_TARGET_STRATEGY; {reason}",
        }

    if size_mult <= 0:
        return {
            "worker_enabled": False,
            "worker_target_strategy": strategy,
            "worker_target_bias": bias,
            "worker_target_size_mult": 0.0,
            "worker_execution_state": "STANDBY",
            "bridge_reason": f"ZERO_TARGET_SIZE; {reason}",
        }

    if exec_mode != "READ_ONLY_READY":
        return {
            "worker_enabled": False,
            "worker_target_strategy": strategy,
            "worker_target_bias": bias,
            "worker_target_size_mult": 0.0,
            "worker_execution_state": "WAITING",
            "bridge_reason": f"EXEC_MODE_NOT_READY; {reason}",
        }

    return {
        "worker_enabled": True,
        "worker_target_strategy": strategy,
        "worker_target_bias": bias,
        "worker_target_size_mult": round(size_mult, 4),
        "worker_execution_state": "READY_FOR_WORKER",
        "bridge_reason": f"WORKER_BRIDGE_READY; {reason}",
    }


def main() -> int:
    exec_plan = load_json(EXEC_PLAN_PATH)
    exec_markets = normalize_markets_block(exec_plan)
    all_markets = sorted(exec_markets.keys())

    result = {
        "version": "worker_execution_bridge_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_execution_plan": str(EXEC_PLAN_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tworker_enabled\tworker_target_strategy\tworker_target_bias\tworker_target_size_mult\tworker_execution_state\tbridge_reason"
    ]

    for market in all_markets:
        row = exec_markets.get(market, {}) or {}
        bridge = build_bridge(row)
        result["markets"][market] = bridge

        tsv_lines.append(
            f'{market}\t{bridge["worker_enabled"]}\t{bridge["worker_target_strategy"]}\t{bridge["worker_target_bias"]}\t{bridge["worker_target_size_mult"]}\t{bridge["worker_execution_state"]}\t{bridge["bridge_reason"]}'
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