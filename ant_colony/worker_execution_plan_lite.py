from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
SELECTION_PATH = OUT_DIR / "worker_strategy_selection.json"
PORTFOLIO_PATH = OUT_DIR / "worker_portfolio_state.json"
OUTPUT_JSON = OUT_DIR / "worker_execution_plan.json"
OUTPUT_TSV = OUT_DIR / "worker_execution_plan.tsv"


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


def markets_from_list_doc(raw: dict) -> dict:
    markets = raw.get("markets", [])
    out = {}
    if isinstance(markets, list):
        for row in markets:
            if isinstance(row, dict):
                market = str(row.get("market", "") or "").strip()
                if market:
                    out[market] = row
    return out


def build_plan(row: dict, portfolio_row: dict) -> dict:
    strategy = str(row.get("selected_strategy", "NONE") or "NONE").upper()
    bias = str(row.get("selected_bias", "NEUTRAL") or "NEUTRAL").upper()
    size_mult = to_float(row.get("selected_size_mult", 0.0), 0.0)
    reason = str(row.get("selection_reason", "NO_SELECTION_REASON") or "NO_SELECTION_REASON")

    portfolio_position = str(portfolio_row.get("position", "FLAT") or "FLAT").upper()
    recently_closed = bool(portfolio_row.get("recently_closed", False))

    if recently_closed and portfolio_position == "FLAT":
        return {
            "enabled": False,
            "selected_strategy": strategy if strategy else "NONE",
            "strategy_family": str(row.get("strategy_family", "NONE") or "NONE"),
            "strategy_class": str(row.get("strategy_class", "INACTIVE") or "INACTIVE"),
            "strategy_priority": int(row.get("strategy_priority", 0) or 0),
            "execution_bias": bias if bias else "NEUTRAL",
            "execution_size_mult": 0.0,
            "execution_mode": "COOLDOWN_BLOCKED",
            "plan_reason": f"RECENTLY_CLOSED_COOLDOWN; {reason}",
        }

    if strategy == "NONE":
        return {
            "enabled": False,
            "selected_strategy": "NONE",
            "strategy_family": str(row.get("strategy_family", "NONE") or "NONE"),
            "strategy_class": str(row.get("strategy_class", "INACTIVE") or "INACTIVE"),
            "strategy_priority": int(row.get("strategy_priority", 0) or 0),
            "execution_bias": "NEUTRAL",
            "execution_size_mult": 0.0,
            "execution_mode": "IDLE",
            "plan_reason": f"NO_EXECUTION_PLAN; {reason}",
        }

    if size_mult <= 0:
        return {
            "enabled": False,
            "selected_strategy": strategy,
            "strategy_family": str(row.get("strategy_family", "NONE") or "NONE"),
            "strategy_class": str(row.get("strategy_class", "INACTIVE") or "INACTIVE"),
            "strategy_priority": int(row.get("strategy_priority", 0) or 0),
            "execution_bias": bias,
            "execution_size_mult": 0.0,
            "execution_mode": "IDLE",
            "plan_reason": f"ZERO_SIZE_PLAN; {reason}",
        }

    if bias not in ("LONG", "SHORT"):
        return {
            "enabled": False,
            "selected_strategy": strategy,
            "strategy_family": str(row.get("strategy_family", "NONE") or "NONE"),
            "strategy_class": str(row.get("strategy_class", "INACTIVE") or "INACTIVE"),
            "strategy_priority": int(row.get("strategy_priority", 0) or 0),
            "execution_bias": bias,
            "execution_size_mult": 0.0,
            "execution_mode": "IDLE",
            "plan_reason": f"INVALID_BIAS_FOR_EXECUTION; {reason}",
        }

    return {
        "enabled": True,
        "selected_strategy": strategy,
            "strategy_family": str(row.get("strategy_family", "NONE") or "NONE"),
            "strategy_class": str(row.get("strategy_class", "INACTIVE") or "INACTIVE"),
            "strategy_priority": int(row.get("strategy_priority", 0) or 0),
        "execution_bias": bias,
        "execution_size_mult": round(size_mult, 4),
        "execution_mode": "READ_ONLY_READY",
        "plan_reason": f"EXECUTION_PLAN_READY; {reason}",
    }


def main() -> int:
    selection = load_json(SELECTION_PATH)
    portfolio = load_json(PORTFOLIO_PATH)

    selection_markets = normalize_markets_block(selection)
    portfolio_markets = markets_from_list_doc(portfolio)
    all_markets = sorted(set(selection_markets.keys()) | set(portfolio_markets.keys()))

    result = {
        "version": "worker_execution_plan_lite_v2",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "worker_strategy_selection": str(SELECTION_PATH),
            "worker_portfolio_state": str(PORTFOLIO_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tenabled\tselected_strategy\texecution_bias\texecution_size_mult\texecution_mode\tplan_reason"
    ]

    for market in all_markets:
        row = selection_markets.get(market, {}) or {}
        portfolio_row = portfolio_markets.get(market, {}) or {}
        plan = build_plan(row, portfolio_row)
        result["markets"][market] = plan

        tsv_lines.append(
            f'{market}\t{plan["enabled"]}\t{plan["selected_strategy"]}\t{plan["execution_bias"]}\t{plan["execution_size_mult"]}\t{plan["execution_mode"]}\t{plan["plan_reason"]}'
        )

    OUTPUT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    OUTPUT_TSV.write_text("\n".join(tsv_lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "ok": True,
        "output_json": str(OUTPUT_JSON),
        "output_tsv": str(OUTPUT_TSV),
        "markets": len(all_markets),
        "version": result["version"],
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
