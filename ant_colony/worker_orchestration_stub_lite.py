from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
BRIDGE_PATH = OUT_DIR / "worker_execution_bridge.json"
OUTPUT_JSON = OUT_DIR / "worker_orchestration_stub.json"
OUTPUT_TSV = OUT_DIR / "worker_orchestration_stub.tsv"


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


def build_orchestration(row: dict) -> dict:
    worker_enabled = bool(row.get("worker_enabled", False))
    strategy = str(row.get("worker_target_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("worker_target_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("worker_target_size_mult", 0.0), 0.0)
    worker_state = str(row.get("worker_execution_state", "STANDBY") or "STANDBY").upper()
    reason = str(row.get("bridge_reason", "NO_BRIDGE_REASON") or "NO_BRIDGE_REASON")

    if not worker_enabled:
        return {
            "orchestration_enabled": False,
            "orchestration_strategy": "NONE",
            "orchestration_bias": "NEUTRAL",
            "orchestration_size_mult": 0.0,
            "orchestration_action": "NOOP",
            "orchestration_reason": f"ORCH_IDLE; {reason}",
        }

    if worker_state != "READY_FOR_WORKER":
        return {
            "orchestration_enabled": False,
            "orchestration_strategy": strategy,
            "orchestration_bias": bias,
            "orchestration_size_mult": 0.0,
            "orchestration_action": "WAIT",
            "orchestration_reason": f"WORKER_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "orchestration_enabled": False,
            "orchestration_strategy": "NONE",
            "orchestration_bias": "NEUTRAL",
            "orchestration_size_mult": 0.0,
            "orchestration_action": "NOOP",
            "orchestration_reason": f"NO_ORCH_STRATEGY; {reason}",
        }

    if bias not in ("LONG", "SHORT"):
        return {
            "orchestration_enabled": False,
            "orchestration_strategy": strategy,
            "orchestration_bias": bias,
            "orchestration_size_mult": 0.0,
            "orchestration_action": "NOOP",
            "orchestration_reason": f"INVALID_ORCH_BIAS; {reason}",
        }

    if size_mult <= 0:
        return {
            "orchestration_enabled": False,
            "orchestration_strategy": strategy,
            "orchestration_bias": bias,
            "orchestration_size_mult": 0.0,
            "orchestration_action": "NOOP",
            "orchestration_reason": f"ZERO_ORCH_SIZE; {reason}",
        }

    return {
        "orchestration_enabled": True,
        "orchestration_strategy": strategy,
        "orchestration_bias": bias,
        "orchestration_size_mult": round(size_mult, 4),
        "orchestration_action": "PREPARE_WORKER_CONTEXT",
        "orchestration_reason": f"ORCH_READY; {reason}",
    }


def main() -> int:
    bridge = load_json(BRIDGE_PATH)
    bridge_markets = normalize_markets_block(bridge)
    all_markets = sorted(bridge_markets.keys())

    result = {
        "version": "worker_orchestration_stub_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_execution_bridge": str(BRIDGE_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\torchestration_enabled\torchestration_strategy\torchestration_bias\torchestration_size_mult\torchestration_action\torchestration_reason"
    ]

    for market in all_markets:
        row = bridge_markets.get(market, {}) or {}
        orch = build_orchestration(row)
        result["markets"][market] = orch

        tsv_lines.append(
            f'{market}\t{orch["orchestration_enabled"]}\t{orch["orchestration_strategy"]}\t{orch["orchestration_bias"]}\t{orch["orchestration_size_mult"]}\t{orch["orchestration_action"]}\t{orch["orchestration_reason"]}'
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