from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
DISPATCH_PATH = OUT_DIR / "worker_runtime_dispatch.json"
OUTPUT_JSON = OUT_DIR / "worker_execution_simulator.json"
OUTPUT_TSV = OUT_DIR / "worker_execution_simulator.tsv"


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


def build_sim(row: dict) -> dict:
    enabled = bool(row.get("dispatch_enabled", False))
    strategy = str(row.get("dispatch_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("dispatch_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("dispatch_size_mult", 0.0), 0.0)
    action = str(row.get("dispatch_action", "NO_DISPATCH") or "NO_DISPATCH").upper()
    reason = str(row.get("dispatch_reason", "NO_DISPATCH_REASON") or "NO_DISPATCH_REASON")

    if not enabled:
        return {
            "sim_enabled": False,
            "sim_strategy": "NONE",
            "sim_bias": "NEUTRAL",
            "sim_size_mult": 0.0,
            "sim_action": "NO_ACTION",
            "sim_position_state": "FLAT",
            "sim_reason": f"SIM_IDLE; {reason}",
        }

    if action != "DISPATCH_READY":
        return {
            "sim_enabled": False,
            "sim_strategy": strategy,
            "sim_bias": bias,
            "sim_size_mult": 0.0,
            "sim_action": "WAIT_DISPATCH",
            "sim_position_state": "FLAT",
            "sim_reason": f"DISPATCH_NOT_READY; {reason}",
        }

    if strategy == "NONE":
        return {
            "sim_enabled": False,
            "sim_strategy": "NONE",
            "sim_bias": "NEUTRAL",
            "sim_size_mult": 0.0,
            "sim_action": "INVALID_STRATEGY",
            "sim_position_state": "FLAT",
            "sim_reason": f"NO_SIM_STRATEGY; {reason}",
        }

    if bias == "LONG" and size_mult > 0:
        return {
            "sim_enabled": True,
            "sim_strategy": strategy,
            "sim_bias": "LONG",
            "sim_size_mult": round(size_mult, 4),
            "sim_action": "OPEN_LONG_SIM",
            "sim_position_state": "SIM_LONG",
            "sim_reason": f"SIM_EXECUTION_READY; {reason}",
        }

    if bias == "SHORT" and size_mult > 0:
        return {
            "sim_enabled": True,
            "sim_strategy": strategy,
            "sim_bias": "SHORT",
            "sim_size_mult": round(size_mult, 4),
            "sim_action": "OPEN_SHORT_SIM",
            "sim_position_state": "SIM_SHORT",
            "sim_reason": f"SIM_EXECUTION_READY; {reason}",
        }

    return {
        "sim_enabled": False,
        "sim_strategy": strategy,
        "sim_bias": bias,
        "sim_size_mult": 0.0,
        "sim_action": "NO_ACTION",
        "sim_position_state": "FLAT",
        "sim_reason": f"INVALID_SIM_INPUT; {reason}",
    }


def main() -> int:
    dispatch = load_json(DISPATCH_PATH)
    dispatch_markets = normalize_markets_block(dispatch)
    all_markets = sorted(dispatch_markets.keys())

    result = {
        "version": "worker_execution_simulator_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_runtime_dispatch": str(DISPATCH_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tsim_enabled\tsim_strategy\tsim_bias\tsim_size_mult\tsim_action\tsim_position_state\tsim_reason"
    ]

    for market in all_markets:
        row = dispatch_markets.get(market, {}) or {}
        sim = build_sim(row)
        result["markets"][market] = sim

        tsv_lines.append(
            f'{market}\t{sim["sim_enabled"]}\t{sim["sim_strategy"]}\t{sim["sim_bias"]}\t{sim["sim_size_mult"]}\t{sim["sim_action"]}\t{sim["sim_position_state"]}\t{sim["sim_reason"]}'
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