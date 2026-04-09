"""
AC-84: Cross-Market / Cross-Asset Allocation Envelope

Builds one top-level allocation envelope over the AC-82 capital allocation
output. Makes visible how capital and weight are distributed across markets
and asset classes — purely observational.

Design principles:
  - envelope_non_binding=True and envelope_simulation_only=True always.
  - Fail-closed: invalid or missing input → ENVELOPE_REJECTED.
  - Deterministic: same capital_allocation → same envelope every call.
  - Pure core function (build_allocation_envelope) — no I/O, no side effects.
  - Asset-class classification via MARKET_TO_ASSET_CLASS; unknown → "crypto".

Envelope status values:
  ENVELOPE_ACTIVE    — ≥1 market has allocated capital (MULTI_STRATEGY or MIXED)
  ENVELOPE_BASELINE  — all markets at baseline / no capital allocated
  ENVELOPE_REJECTED  — invalid input

Usage (importable):
    from build_allocation_envelope_lite import build_allocation_envelope
    envelope = build_allocation_envelope(capital_allocation_result)

    # Full chain from specs:
    from build_allocation_envelope_lite import build_envelope_from_specs
    result = build_envelope_from_specs(market_specs, total_equity_eur=10_000.0)

Output fields:
    allocation_envelope_status — "ENVELOPE_ACTIVE"|"ENVELOPE_BASELINE"|"ENVELOPE_REJECTED"
    allocation_mode            — mirrors AC-82 allocation_mode
    asset_class_allocations    — {asset_class: {capital_eur, weight_fraction, markets}}
    market_allocations         — list of per-market envelope entries
    total_allocated_weight     — float sum of all market weight fractions
    total_allocated_capital    — float EUR total capital across all markets
    unallocated_capital        — float EUR (from AC-82)
    total_equity_eur           — float EUR
    allocation_reason          — human-readable
    allocation_reason_code     — machine-stable
    envelope_non_binding       — always True
    envelope_simulation_only   — always True
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "allocation_envelope_v1"

# Envelope status values
ENVELOPE_ACTIVE   = "ENVELOPE_ACTIVE"
ENVELOPE_BASELINE = "ENVELOPE_BASELINE"
ENVELOPE_REJECTED = "ENVELOPE_REJECTED"

# Reason codes
REASON_CODES: dict = {
    ENVELOPE_ACTIVE:   "ENVELOPE_CAPITAL_ALLOCATED",
    ENVELOPE_BASELINE: "ENVELOPE_BASELINE_HOLD",
    ENVELOPE_REJECTED: "ENVELOPE_INVALID_INPUT",
}

_ACTIVE_MODES  = {"MULTI_STRATEGY", "MIXED"}
_DEFAULT_ASSET_CLASS = "crypto"
_TOL = 1e-9

# Inline copy of MARKET_TO_ASSET_CLASS from ant_colony/core/asset_profiles.py.
# Avoids loading a module that uses @dataclass (Python 3.14 importlib compat issue).
_MARKET_TO_ASSET_CLASS: dict = {
    "BTC-EUR": "crypto",
    "ETH-EUR": "crypto",
    "SOL-EUR": "crypto",
    "XRP-EUR": "crypto",
    "ADA-EUR": "crypto",
    "BNB-EUR": "crypto",
}


# ---------------------------------------------------------------------------
# Core envelope function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_allocation_envelope(capital_allocation: object) -> dict:
    """
    Build a cross-market / cross-asset allocation envelope from AC-82 output.

    envelope_non_binding=True and envelope_simulation_only=True always.
    No execution, no cash movement, no state mutation.

    Args:
        capital_allocation: dict returned by build_capital_allocation() (AC-82).

    Returns:
        Allocation envelope dict.
    """
    if not isinstance(capital_allocation, dict):
        return _rejected_envelope("capital_allocation is not a dict")

    if "allocation_summary" not in capital_allocation or \
       "market_allocations" not in capital_allocation:
        return _rejected_envelope("capital_allocation missing required keys")

    summary = capital_allocation.get("allocation_summary") or {}
    alloc_mode      = summary.get("allocation_mode", "BASELINE_HOLD")
    total_equity    = _safe_float(summary.get("total_equity_eur"), 0.0)
    unallocated     = _safe_float(summary.get("unallocated_capital"), total_equity)
    allocated_total = _safe_float(summary.get("allocated_capital_total"), 0.0)
    alloc_reason    = summary.get("allocation_reason", "")

    market_alloc_list = capital_allocation.get("market_allocations") or []

    # Build per-market envelope entries
    market_entries: list = []
    asset_class_buckets: dict = {}  # asset_class → {capital_eur, weight_sum, markets}

    for ma in market_alloc_list:
        if not isinstance(ma, dict):
            continue
        market       = str(ma.get("market") or "UNKNOWN")
        market_cap   = _safe_float(ma.get("market_capital_eur"), 0.0)
        mkt_frac     = _safe_float(ma.get("market_capital_fraction"), 0.0)
        split_mode   = str(ma.get("market_split_mode") or "BASELINE")
        split_valid  = bool(ma.get("market_split_valid", False))
        strat_allocs = ma.get("strategy_allocations") or []

        asset_class = _classify_market(market)

        # Collect strategy summaries
        strategy_summaries = [
            {
                "strategy_id":      str(sa.get("strategy_id") or "UNKNOWN"),
                "strategy_family":  str(sa.get("strategy_family") or "UNKNOWN"),
                "capital_eur":      _safe_float(sa.get("capital_eur"), 0.0),
                "simulated_weight": _safe_float(sa.get("simulated_weight"), 0.0),
            }
            for sa in strat_allocs
            if isinstance(sa, dict)
        ]

        entry = {
            "market":                  market,
            "asset_class":             asset_class,
            "market_capital_eur":      round(market_cap, 4),
            "market_capital_fraction": round(mkt_frac, 9),
            "market_split_mode":       split_mode,
            "market_split_valid":      split_valid,
            "strategy_count":          len(strategy_summaries),
            "strategy_allocations":    strategy_summaries,
        }
        market_entries.append(entry)

        # Accumulate into asset class bucket
        if asset_class not in asset_class_buckets:
            asset_class_buckets[asset_class] = {
                "capital_eur": 0.0,
                "weight_sum":  0.0,
                "markets":     [],
            }
        asset_class_buckets[asset_class]["capital_eur"] = round(
            asset_class_buckets[asset_class]["capital_eur"] + market_cap, 4
        )
        asset_class_buckets[asset_class]["weight_sum"] = round(
            asset_class_buckets[asset_class]["weight_sum"] + mkt_frac, 9
        )
        asset_class_buckets[asset_class]["markets"].append(market)

    # Build asset_class_allocations output
    asset_class_allocations = {
        ac: {
            "capital_eur":      round(bucket["capital_eur"], 4),
            "weight_fraction":  round(bucket["weight_sum"], 9),
            "markets":          sorted(bucket["markets"]),
        }
        for ac, bucket in sorted(asset_class_buckets.items())
    }

    # Derive total weight from market fractions
    total_weight = round(
        sum(e["market_capital_fraction"] for e in market_entries), 9
    )

    # Derive envelope status
    if alloc_mode in _ACTIVE_MODES and allocated_total > _TOL:
        env_status = ENVELOPE_ACTIVE
    else:
        env_status = ENVELOPE_BASELINE

    reason_code = REASON_CODES[env_status]

    return {
        "allocation_envelope_status": env_status,
        "allocation_mode":            alloc_mode,
        "asset_class_allocations":    asset_class_allocations,
        "market_allocations":         market_entries,
        "total_allocated_weight":     total_weight,
        "total_allocated_capital":    round(allocated_total, 4),
        "unallocated_capital":        round(unallocated, 4),
        "total_equity_eur":           round(total_equity, 4),
        "allocation_reason":          alloc_reason,
        "allocation_reason_code":     reason_code,
        "envelope_non_binding":       True,
        "envelope_simulation_only":   True,
    }


# ---------------------------------------------------------------------------
# Asset class classification
# ---------------------------------------------------------------------------

def _classify_market(market: str) -> str:
    """Map a market symbol to its asset class. Unknown markets default to 'crypto'."""
    return _MARKET_TO_ASSET_CLASS.get(market, _DEFAULT_ASSET_CLASS)


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_envelope(reason: str) -> dict:
    return {
        "allocation_envelope_status": ENVELOPE_REJECTED,
        "allocation_mode":            "UNKNOWN",
        "asset_class_allocations":    {},
        "market_allocations":         [],
        "total_allocated_weight":     0.0,
        "total_allocated_capital":    0.0,
        "unallocated_capital":        0.0,
        "total_equity_eur":           0.0,
        "allocation_reason":          reason,
        "allocation_reason_code":     REASON_CODES[ENVELOPE_REJECTED],
        "envelope_non_binding":       True,
        "envelope_simulation_only":   True,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Module loader helpers
# ---------------------------------------------------------------------------

def _load_allocator_module():
    path = Path(__file__).parent / "build_queen_capital_allocator_lite.py"
    spec = importlib.util.spec_from_file_location("_alloc", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81 + AC-82 + AC-84
# ---------------------------------------------------------------------------

def build_envelope_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → allocation envelope (AC-84).

    Returns dict with keys: splits_result, capital_allocation, allocation_envelope.
    envelope_non_binding=True and envelope_simulation_only=True always.
    """
    _alloc_mod = _load_allocator_module()
    pipeline   = _alloc_mod.build_capital_allocation_from_specs(
        market_specs, total_equity_eur, market_capital_fractions
    )
    envelope = build_allocation_envelope(pipeline["capital_allocation"])
    return {
        "splits_result":       pipeline["splits_result"],
        "capital_allocation":  pipeline["capital_allocation"],
        "allocation_envelope": envelope,
    }


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
    result = build_envelope_from_specs(specs, total_equity_eur=10_000.0)
    env = result["allocation_envelope"]
    print(json.dumps({
        "allocation_envelope_status": env["allocation_envelope_status"],
        "allocation_mode":            env["allocation_mode"],
        "total_allocated_capital":    env["total_allocated_capital"],
        "unallocated_capital":        env["unallocated_capital"],
        "total_equity_eur":           env["total_equity_eur"],
        "total_allocated_weight":     env["total_allocated_weight"],
        "allocation_reason_code":     env["allocation_reason_code"],
        "envelope_non_binding":       env["envelope_non_binding"],
        "envelope_simulation_only":   env["envelope_simulation_only"],
        "asset_class_allocations":    env["asset_class_allocations"],
        "market_allocations": [
            {
                "market":             m["market"],
                "asset_class":        m["asset_class"],
                "market_capital_eur": m["market_capital_eur"],
                "strategy_count":     m["strategy_count"],
            }
            for m in env["market_allocations"]
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
