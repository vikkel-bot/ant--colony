"""
AC-82: Queen Capital Allocator (Simulation-Only)

Converts AC-81 strategy weight splits into simulated EUR capital allocations
per market and per strategy. Purely observability — no cash movement, no
position changes, no execution.

Full simulation chain:
  market_specs + equity
    → build_allocation_splits()         [AC-81: weight splits per market]
    → build_capital_allocation()        [AC-82: capital in EUR per market/strategy]

Design principles:
  - simulation_only=True and non_binding=True always.
  - Fail-closed: invalid splits or equity → zero-allocation baseline result.
  - Deterministic: same input → same output every call.
  - Capital consistency: allocated_capital_total + unallocated_capital == total_equity_eur.
  - Per-market capital fractions: equal split if not provided; normalized if sum ≠ 1.
  - Pure core function (build_capital_allocation) — no I/O, no side effects.

Allocation modes:
  MULTI_STRATEGY  — valid splits across ≥1 markets, capital distributed
  BASELINE_HOLD   — all markets baseline; no capital distributed
  MIXED           — some markets valid, some baseline
  EMPTY           — no markets at all

Usage (importable):
    from build_queen_capital_allocator_lite import build_capital_allocation
    result = build_capital_allocation(
        splits_result,
        total_equity_eur=10_000.0,
        market_capital_fractions={"BTC-EUR": 0.5, "ETH-EUR": 0.5},
    )

    # Convenience: run AC-81 + AC-82 in one call
    from build_queen_capital_allocator_lite import build_capital_allocation_from_specs
    result = build_capital_allocation_from_specs(
        market_specs, total_equity_eur=10_000.0
    )

Output fields:
    allocation_summary          — see below
    allocated_capital_by_market — {market: EUR float}
    allocated_capital_by_strategy — {strategy_id: EUR float} (summed across markets)
    market_allocations          — per-market detail list
    simulation_only             — always True
    non_binding                 — always True

allocation_summary fields:
    allocated_capital_total     — float EUR
    unallocated_capital         — float EUR
    total_equity_eur            — float EUR
    allocation_mode             — "MULTI_STRATEGY"|"BASELINE_HOLD"|"MIXED"|"EMPTY"
    allocation_reason           — human-readable
    allocation_reason_code      — machine-stable
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "queen_capital_allocator_v1"

# Allocation modes
ALLOC_MODE_MULTI     = "MULTI_STRATEGY"
ALLOC_MODE_BASELINE  = "BASELINE_HOLD"
ALLOC_MODE_MIXED     = "MIXED"
ALLOC_MODE_EMPTY     = "EMPTY"

# Machine-stable reason codes
ALLOCATION_REASON_CODES: dict = {
    ALLOC_MODE_MULTI:    "CAPITAL_ALLOCATED_MULTI_STRATEGY",
    ALLOC_MODE_BASELINE: "CAPITAL_BASELINE_HOLD",
    ALLOC_MODE_MIXED:    "CAPITAL_ALLOCATED_MIXED",
    ALLOC_MODE_EMPTY:    "CAPITAL_EMPTY_NO_MARKETS",
    "INVALID_SPLITS":    "CAPITAL_INVALID_SPLITS_INPUT",
    "INVALID_EQUITY":    "CAPITAL_INVALID_EQUITY",
}

_TOL = 1e-9   # float tolerance
_BASELINE_SPLIT_MODE = "BASELINE"


# ---------------------------------------------------------------------------
# Core capital allocation function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_capital_allocation(
    splits_result: object,
    total_equity_eur: float,
    market_capital_fractions: object = None,
) -> dict:
    """
    Convert AC-81 splits result into simulated EUR capital allocations.

    Args:
        splits_result:            dict returned by build_allocation_splits() (AC-81).
        total_equity_eur:         total portfolio equity in EUR.
        market_capital_fractions: optional dict {market: fraction_of_equity}.
                                  If None → equal split across valid markets.
                                  If provided but sum ≠ 1.0 → normalized.

    Returns:
        Capital allocation dict (simulation-only, non-binding).
    """
    # Validate equity
    equity = _safe_float(total_equity_eur, None)
    if equity is None or equity < 0.0:
        return _zero_result(
            code="INVALID_EQUITY",
            reason=f"invalid total_equity_eur: {total_equity_eur!r}",
            equity=0.0,
        )

    # Validate splits_result
    if not isinstance(splits_result, dict) or "market_splits" not in splits_result:
        return _zero_result(
            code="INVALID_SPLITS",
            reason="splits_result is not a valid dict or missing market_splits",
            equity=equity,
        )

    market_splits: list = splits_result.get("market_splits") or []

    if not market_splits:
        return _empty_result(equity)

    # Separate valid vs baseline markets
    valid_markets  = [ms for ms in market_splits if ms.get("market_split_valid")]
    all_markets    = [ms.get("market", "UNKNOWN") for ms in market_splits]

    # Compute per-market capital budget
    mcf = _resolve_market_fractions(
        market_capital_fractions,
        all_markets,
        valid_markets,
    )

    # Build per-market allocations
    market_allocations:        list = []
    allocated_by_market:       dict = {}
    allocated_by_strategy:     dict = {}

    for ms in market_splits:
        market = ms.get("market", "UNKNOWN")
        frac   = mcf.get(market, 0.0)
        market_capital = round(frac * equity, 4)

        strategy_entries = []
        if ms.get("market_split_valid") and market_capital > _TOL:
            for split in (ms.get("splits") or []):
                sid     = split.get("strategy_id", "UNKNOWN")
                weight  = _safe_float(split.get("simulated_weight"), 0.0)
                capital = round(weight * market_capital, 4)
                strategy_entries.append({
                    "strategy_id":          sid,
                    "strategy_family":      split.get("strategy_family", "UNKNOWN"),
                    "capital_eur":          capital,
                    "simulated_weight":     split.get("simulated_weight", 0.0),
                    "split_reason_code":    split.get("split_reason_code", ""),
                })
                allocated_by_strategy[sid] = round(
                    allocated_by_strategy.get(sid, 0.0) + capital, 4
                )
            market_total_capital = sum(e["capital_eur"] for e in strategy_entries)
        else:
            # Baseline or zero-budget market
            market_total_capital = 0.0

        market_allocations.append({
            "market":                market,
            "market_capital_eur":    market_total_capital,
            "market_capital_fraction": frac,
            "market_split_mode":     ms.get("market_split_mode", _BASELINE_SPLIT_MODE),
            "market_split_valid":    ms.get("market_split_valid", False),
            "strategy_allocations":  strategy_entries,
        })
        allocated_by_market[market] = market_total_capital

    allocated_total  = round(sum(allocated_by_market.values()), 4)
    unallocated      = round(equity - allocated_total, 4)

    # Derive allocation mode
    split_mode = (splits_result.get("split_summary") or {}).get("split_mode", ALLOC_MODE_BASELINE)
    if split_mode == "MULTI_STRATEGY":
        mode = ALLOC_MODE_MULTI
    elif split_mode == "MIXED":
        mode = ALLOC_MODE_MIXED
    else:
        mode = ALLOC_MODE_BASELINE

    reason_code = ALLOCATION_REASON_CODES.get(mode, "CAPITAL_UNKNOWN")

    return {
        "allocation_summary": {
            "allocated_capital_total": allocated_total,
            "unallocated_capital":     unallocated,
            "total_equity_eur":        equity,
            "allocation_mode":         mode,
            "allocation_reason":       (
                f"{mode}"
                f"|allocated={allocated_total:.2f}"
                f"|unallocated={unallocated:.2f}"
                f"|equity={equity:.2f}"
            ),
            "allocation_reason_code":  reason_code,
        },
        "allocated_capital_by_market":    allocated_by_market,
        "allocated_capital_by_strategy":  allocated_by_strategy,
        "market_allocations":             market_allocations,
        "simulation_only":               True,
        "non_binding":                   True,
    }


# ---------------------------------------------------------------------------
# Convenience: run AC-81 + AC-82 in one call
# ---------------------------------------------------------------------------

def build_capital_allocation_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → AC-81 splits → AC-82 capital allocation.

    Returns a dict with keys: splits_result, capital_allocation.
    Both layers are simulation-only and non-binding.
    """
    _splits_mod = _load_splits_module()
    equity = _safe_float(total_equity_eur, 0.0)

    # AC-81: weight splits (without notionals — we compute capital here)
    splits_result = _splits_mod.build_allocation_splits(market_specs, total_equity_eur=0.0)

    # AC-82: capital allocation
    capital_allocation = build_capital_allocation(
        splits_result,
        equity,
        market_capital_fractions,
    )
    return {
        "splits_result":      splits_result,
        "capital_allocation": capital_allocation,
    }


# ---------------------------------------------------------------------------
# Market fraction resolution
# ---------------------------------------------------------------------------

def _resolve_market_fractions(
    user_fractions: object,
    all_markets: list,
    valid_markets: list,
) -> dict:
    """
    Resolve per-market capital fractions.

    Rules:
    - If user_fractions provided and valid: normalize to 1.0.
    - Otherwise: equal share for valid markets; 0.0 for baseline markets.
    """
    valid_set = {ms.get("market", "UNKNOWN") for ms in valid_markets}

    if isinstance(user_fractions, dict) and user_fractions:
        # Keep only markets in all_markets; normalize
        raw = {m: _safe_float(user_fractions.get(m), 0.0) for m in all_markets}
        total = sum(raw.values())
        if total > _TOL:
            return {m: round(v / total, 9) for m, v in raw.items()}
        # All zero → fall through to equal split

    # Equal split: only valid markets get capital
    n = len(valid_set)
    if n == 0:
        return {m: 0.0 for m in all_markets}
    share = round(1.0 / n, 9)
    return {m: (share if m in valid_set else 0.0) for m in all_markets}


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _zero_result(code: str, reason: str, equity: float) -> dict:
    return {
        "allocation_summary": {
            "allocated_capital_total": 0.0,
            "unallocated_capital":     equity,
            "total_equity_eur":        equity,
            "allocation_mode":         ALLOC_MODE_BASELINE,
            "allocation_reason":       reason,
            "allocation_reason_code":  ALLOCATION_REASON_CODES.get(code, "CAPITAL_UNKNOWN"),
        },
        "allocated_capital_by_market":   {},
        "allocated_capital_by_strategy": {},
        "market_allocations":            [],
        "simulation_only":               True,
        "non_binding":                   True,
    }


def _empty_result(equity: float) -> dict:
    return {
        "allocation_summary": {
            "allocated_capital_total": 0.0,
            "unallocated_capital":     equity,
            "total_equity_eur":        equity,
            "allocation_mode":         ALLOC_MODE_EMPTY,
            "allocation_reason":       "no market splits provided",
            "allocation_reason_code":  ALLOCATION_REASON_CODES[ALLOC_MODE_EMPTY],
        },
        "allocated_capital_by_market":   {},
        "allocated_capital_by_strategy": {},
        "market_allocations":            [],
        "simulation_only":               True,
        "non_binding":                   True,
    }


def _safe_float(value: object, default):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_splits_module():
    path = Path(__file__).parent / "build_allocation_split_simulation_lite.py"
    spec = importlib.util.spec_from_file_location("_splits", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Optional main (CLI demo)
# ---------------------------------------------------------------------------

def main() -> None:
    import json

    specs = [
        {
            "market": "BTC-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.6},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.4},
            ],
        },
        {
            "market": "ETH-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION"},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT"},
            ],
        },
        {
            "market": "SOL-EUR",
            "strategies": [],
        },
    ]

    result = build_capital_allocation_from_specs(
        specs,
        total_equity_eur=10_000.0,
        market_capital_fractions={"BTC-EUR": 0.5, "ETH-EUR": 0.4, "SOL-EUR": 0.1},
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
