from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

COMBINED_PATH = OUT_DIR / "alloc_targets_combined.json"
SIM_PATH = OUT_DIR / "worker_execution_simulator.json"
PORTFOLIO_PATH = OUT_DIR / "worker_portfolio_state.json"
PRICE_PATH = OUT_DIR / "worker_market_price_feed.json"

OUTPUT_JSON = OUT_DIR / "worker_position_sizing.json"
OUTPUT_TSV = OUT_DIR / "worker_position_sizing.tsv"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
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


def normalize_markets_dict(raw: dict) -> dict:
    markets = raw.get("markets", {})
    return markets if isinstance(markets, dict) else {}


def normalize_markets_list(raw: dict) -> dict:
    rows = raw.get("markets", [])
    out = {}
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                market = row.get("market")
                if market:
                    out[str(market)] = row
    return out


def build_sizing_row(market: str, combined_row: dict, sim_row: dict, portfolio_row: dict, price_row: dict) -> dict:
    gate = str(combined_row.get("gate", "ALLOW") or "ALLOW").upper()
    sim_enabled = bool(sim_row.get("sim_enabled", False))

    strategy = str(sim_row.get("sim_strategy", "NONE") or "NONE").upper()
    bias = str(sim_row.get("sim_bias", "NEUTRAL") or "NEUTRAL").upper()
    input_size_mult = to_float(sim_row.get("sim_size_mult", 0.0), 0.0)

    asset_class = str(combined_row.get("asset_class", "unknown") or "unknown")
    profile_base_pf = to_float(combined_row.get("profile_base_pf", 0.0), 0.0)
    profile_max_pf = to_float(combined_row.get("profile_max_pf", 0.0), 0.0)

    equity_eur = to_float(portfolio_row.get("equity", 0.0), 0.0)
    market_price = to_float(price_row.get("market_price", 0.0), 0.0)

    combined_reason = str(combined_row.get("reason", "NO_COMBINED_REASON") or "NO_COMBINED_REASON")
    sim_reason = str(sim_row.get("sim_reason", "NO_SIM_REASON") or "NO_SIM_REASON")

    if not sim_enabled:
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": 0.0,
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": f"SIM_DISABLED; {sim_reason}",
        }

    if gate != "ALLOW":
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": round(input_size_mult, 6),
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": f"GATE_BLOCKED; {combined_reason}",
        }

    if input_size_mult <= 0:
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": round(input_size_mult, 6),
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": f"NON_POSITIVE_SIZE_MULT; {sim_reason}",
        }

    if equity_eur <= 0:
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": round(input_size_mult, 6),
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": "NON_POSITIVE_EQUITY",
        }

    if market_price <= 0:
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": round(input_size_mult, 6),
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": "NON_POSITIVE_MARKET_PRICE",
        }

    if profile_base_pf <= 0 or profile_max_pf <= 0:
        return {
            "sizing_enabled": False,
            "sizing_strategy": strategy,
            "sizing_bias": bias,
            "input_size_mult": round(input_size_mult, 6),
            "asset_class": asset_class,
            "profile_base_pf": round(profile_base_pf, 6),
            "profile_max_pf": round(profile_max_pf, 6),
            "equity_eur": round(equity_eur, 4),
            "market_price": round(market_price, 8),
            "target_notional_eur": 0.0,
            "max_notional_eur": 0.0,
            "target_qty": 0.0,
            "sizing_state": "SIZING_DISABLED",
            "sizing_reason": "INVALID_PROFILE_POSITION_FRACTIONS",
        }

    base_target_notional = equity_eur * profile_base_pf * input_size_mult
    max_notional_eur = equity_eur * profile_max_pf
    target_notional_eur = min(base_target_notional, max_notional_eur)
    target_qty = target_notional_eur / market_price

    return {
        "sizing_enabled": True,
        "sizing_strategy": strategy,
        "sizing_bias": bias,
        "input_size_mult": round(input_size_mult, 6),
        "asset_class": asset_class,
        "profile_base_pf": round(profile_base_pf, 6),
        "profile_max_pf": round(profile_max_pf, 6),
        "equity_eur": round(equity_eur, 4),
        "market_price": round(market_price, 8),
        "target_notional_eur": round(target_notional_eur, 4),
        "max_notional_eur": round(max_notional_eur, 4),
        "target_qty": round(target_qty, 8),
        "sizing_state": "SIZING_READY",
        "sizing_reason": f"SIZING_READY; {sim_reason}",
    }


def main() -> int:
    combined = load_json(COMBINED_PATH)
    sim = load_json(SIM_PATH)
    portfolio = load_json(PORTFOLIO_PATH)
    price = load_json(PRICE_PATH)

    combined_markets = normalize_markets_dict(combined)
    sim_markets = normalize_markets_dict(sim)
    portfolio_markets = normalize_markets_list(portfolio)
    price_markets = normalize_markets_list(price)

    all_markets = sorted(
        set(combined_markets.keys())
        | set(sim_markets.keys())
        | set(portfolio_markets.keys())
        | set(price_markets.keys())
    )

    result = {
        "version": "worker_position_sizing_lite_v1",
        "ts_utc": now_utc_iso(),
        "source_files": {
            "alloc_targets_combined": str(COMBINED_PATH),
            "worker_execution_simulator": str(SIM_PATH),
            "worker_portfolio_state": str(PORTFOLIO_PATH),
            "worker_market_price_feed": str(PRICE_PATH),
        },
        "markets": {},
    }

    tsv_lines = [
        "market\tsizing_enabled\tsizing_strategy\tsizing_bias\tinput_size_mult\tasset_class\tprofile_base_pf\tprofile_max_pf\tequity_eur\tmarket_price\ttarget_notional_eur\tmax_notional_eur\ttarget_qty\tsizing_state\tsizing_reason"
    ]

    for market in all_markets:
        row = build_sizing_row(
            market=market,
            combined_row=combined_markets.get(market, {}) or {},
            sim_row=sim_markets.get(market, {}) or {},
            portfolio_row=portfolio_markets.get(market, {}) or {},
            price_row=price_markets.get(market, {}) or {},
        )
        result["markets"][market] = row

        tsv_lines.append(
            f'{market}\t{row["sizing_enabled"]}\t{row["sizing_strategy"]}\t{row["sizing_bias"]}\t{row["input_size_mult"]}\t{row["asset_class"]}\t{row["profile_base_pf"]}\t{row["profile_max_pf"]}\t{row["equity_eur"]}\t{row["market_price"]}\t{row["target_notional_eur"]}\t{row["max_notional_eur"]}\t{row["target_qty"]}\t{row["sizing_state"]}\t{row["sizing_reason"]}'
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