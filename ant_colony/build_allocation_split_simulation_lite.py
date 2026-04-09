"""
AC-81: Multi-Strategy Allocation Splits (Simulation-Only)

Simulates how a total market allocation weight is split across multiple
strategies per market. Purely observability — no execution, no orders,
no portfolio state mutation.

Design principles:
  - simulation_only=True and non_binding=True always.
  - Fail-closed: invalid/missing input → BASELINE_HOLD split.
  - Deterministic: same input → same split output every call.
  - Pure core function (build_allocation_splits) — no I/O, no side effects.
  - Weights normalized if sum > 1.0; equal-split if not provided.
  - Unknown/empty strategy list falls back to BASELINE_HOLD.

Split modes (per market):
  EXPLICIT    — weights provided explicitly and valid (sum in (0, 1])
  NORMALIZED  — weights provided but sum > 1.0; normalized to 1.0
  EQUAL       — weights not provided; equal split across all strategies
  BASELINE    — no valid strategies; single baseline placeholder entry

Input (market_specs list):
  Each entry: {
    "market":     str,
    "strategies": [
      {
        "strategy_id":     str,
        "strategy_family": str  (optional, default "UNKNOWN"),
        "weight_fraction": float (optional; if absent → EQUAL mode)
      },
      ...
    ]
  }

Usage (importable):
    from build_allocation_split_simulation_lite import build_allocation_splits
    result = build_allocation_splits(market_specs, total_equity_eur=10000.0)

Usage (CLI):
    python build_allocation_split_simulation_lite.py

Output top-level fields:
    split_summary     — {total_markets_split, total_strategies_active,
                          total_weight_assigned, split_mode,
                          simulation_only, non_binding}
    market_splits     — list of per-market split results
    simulation_only   — always True
    non_binding       — always True
"""
from __future__ import annotations

VERSION = "allocation_split_simulation_v1"

# Split mode values
SPLIT_MODE_EXPLICIT   = "EXPLICIT"
SPLIT_MODE_NORMALIZED = "NORMALIZED"
SPLIT_MODE_EQUAL      = "EQUAL"
SPLIT_MODE_BASELINE   = "BASELINE"

# Machine-stable split reason codes
SPLIT_REASON_CODES: dict = {
    SPLIT_MODE_EXPLICIT:   "SPLIT_EXPLICIT_WEIGHT",
    SPLIT_MODE_NORMALIZED: "SPLIT_NORMALIZED_WEIGHT",
    SPLIT_MODE_EQUAL:      "SPLIT_EQUAL_WEIGHT",
    SPLIT_MODE_BASELINE:   "SPLIT_BASELINE_HOLD",
}

# Top-level split_mode summary labels
TOP_SPLIT_MODE_MULTI    = "MULTI_STRATEGY"
TOP_SPLIT_MODE_BASELINE = "BASELINE_HOLD"
TOP_SPLIT_MODE_MIXED    = "MIXED"

# Confidence by split mode
_SPLIT_CONFIDENCE: dict = {
    SPLIT_MODE_EXPLICIT:   1.0,
    SPLIT_MODE_NORMALIZED: 0.8,
    SPLIT_MODE_EQUAL:      0.6,
    SPLIT_MODE_BASELINE:   0.0,
}

_TOLERANCE = 1e-9   # float comparison tolerance


# ---------------------------------------------------------------------------
# Core split function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_allocation_splits(
    market_specs: object,
    total_equity_eur: float = 0.0,
) -> dict:
    """
    Simulate multi-strategy allocation splits for a list of markets.

    Args:
        market_specs: list of market spec dicts (see module docstring).
        total_equity_eur: total portfolio equity in EUR for notional calculation.
                          If 0.0, simulated_notional will be 0.0.

    Returns:
        Dict with split_summary, market_splits, simulation_only, non_binding.
    """
    if not isinstance(market_specs, list):
        return _baseline_result(reason="market_specs is not a list")

    equity = _safe_float(total_equity_eur, 0.0)
    market_splits: list = []
    total_strategies_active = 0
    total_weight_assigned   = 0.0
    split_modes_seen: set   = set()

    for spec in market_specs:
        if not isinstance(spec, dict):
            market_splits.append(_baseline_market_split(
                market="UNKNOWN",
                reason="market_spec is not a dict",
                equity=equity,
            ))
            split_modes_seen.add(SPLIT_MODE_BASELINE)
            continue

        market = str(spec.get("market") or "UNKNOWN")
        strategies = spec.get("strategies")

        if not isinstance(strategies, list) or len(strategies) == 0:
            market_splits.append(_baseline_market_split(
                market=market,
                reason="no valid strategies provided",
                equity=equity,
            ))
            split_modes_seen.add(SPLIT_MODE_BASELINE)
            continue

        ms = _split_market(market, strategies, equity)
        market_splits.append(ms)
        split_modes_seen.add(ms["market_split_mode"])

        if ms["market_split_valid"]:
            total_strategies_active += len(ms["splits"])
            total_weight_assigned   += ms["market_total_weight"]

    # Top-level split_mode summary
    non_baseline = split_modes_seen - {SPLIT_MODE_BASELINE}
    if not split_modes_seen or split_modes_seen == {SPLIT_MODE_BASELINE}:
        top_split_mode = TOP_SPLIT_MODE_BASELINE
    elif SPLIT_MODE_BASELINE in split_modes_seen and non_baseline:
        top_split_mode = TOP_SPLIT_MODE_MIXED
    else:
        top_split_mode = TOP_SPLIT_MODE_MULTI

    n_markets = len(market_splits)
    n_markets_split = sum(
        1 for ms in market_splits
        if ms.get("market_split_valid") and ms.get("market_split_mode") != SPLIT_MODE_BASELINE
    )

    return {
        "split_summary": {
            "total_markets":          n_markets,
            "total_markets_split":    n_markets_split,
            "total_strategies_active": total_strategies_active,
            "total_weight_assigned":   round(total_weight_assigned, 6),
            "split_mode":             top_split_mode,
        },
        "market_splits":  market_splits,
        "simulation_only": True,
        "non_binding":     True,
    }


# ---------------------------------------------------------------------------
# Per-market split logic
# ---------------------------------------------------------------------------

def _split_market(market: str, strategies: list, equity: float) -> dict:
    """
    Compute per-strategy splits for one market.
    """
    valid_strategies = [s for s in strategies if isinstance(s, dict) and s.get("strategy_id")]
    if not valid_strategies:
        return _baseline_market_split(market, "no valid strategy entries", equity)

    # Determine split mode
    has_weights = all("weight_fraction" in s for s in valid_strategies)
    raw_weights = [_safe_float(s.get("weight_fraction"), 0.0) for s in valid_strategies]
    total_raw   = sum(raw_weights)

    if has_weights and total_raw > _TOLERANCE:
        if total_raw <= 1.0 + _TOLERANCE:
            mode    = SPLIT_MODE_EXPLICIT
            weights = raw_weights
        else:
            mode    = SPLIT_MODE_NORMALIZED
            weights = [w / total_raw for w in raw_weights]
    else:
        # No weights or all zero → equal split
        mode    = SPLIT_MODE_EQUAL
        n       = len(valid_strategies)
        weights = [1.0 / n] * n

    # Filter out zero-weight entries after normalization
    active = [(s, w) for s, w in zip(valid_strategies, weights) if w > _TOLERANCE]
    if not active:
        return _baseline_market_split(market, "all weights resolved to zero", equity)

    total_weight = sum(w for _, w in active)
    reason_code  = SPLIT_REASON_CODES[mode]
    confidence   = _SPLIT_CONFIDENCE[mode]

    splits = []
    for strat, weight in active:
        sid     = str(strat.get("strategy_id", "UNKNOWN"))
        family  = str(strat.get("strategy_family") or "UNKNOWN")
        notional = round(weight * equity, 4) if equity > 0 else 0.0
        splits.append({
            "strategy_id":        sid,
            "strategy_family":    family,
            "simulated_weight":   round(weight, 6),
            "simulated_notional": notional,
            "split_reason":       f"{mode}|weight={round(weight, 4)}|market={market}",
            "split_reason_code":  reason_code,
            "split_confidence":   confidence,
        })

    return {
        "market":             market,
        "splits":             splits,
        "market_total_weight": round(total_weight, 6),
        "market_split_mode":  mode,
        "market_split_valid": True,
    }


# ---------------------------------------------------------------------------
# Fail-closed baseline helpers
# ---------------------------------------------------------------------------

def _baseline_market_split(market: str, reason: str, equity: float) -> dict:
    """Return a single BASELINE placeholder split for a market."""
    notional = round(0.0 * equity, 4)
    return {
        "market": market,
        "splits": [
            {
                "strategy_id":        "BASELINE",
                "strategy_family":    "BASELINE",
                "simulated_weight":   0.0,
                "simulated_notional": notional,
                "split_reason":       f"BASELINE_HOLD|reason={reason}",
                "split_reason_code":  SPLIT_REASON_CODES[SPLIT_MODE_BASELINE],
                "split_confidence":   0.0,
            }
        ],
        "market_total_weight": 0.0,
        "market_split_mode":   SPLIT_MODE_BASELINE,
        "market_split_valid":  False,
    }


def _baseline_result(reason: str) -> dict:
    """Return a top-level fail-closed result with no market splits."""
    return {
        "split_summary": {
            "total_markets":           0,
            "total_markets_split":     0,
            "total_strategies_active": 0,
            "total_weight_assigned":   0.0,
            "split_mode":              TOP_SPLIT_MODE_BASELINE,
        },
        "market_splits":  [],
        "simulation_only": True,
        "non_binding":     True,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Optional main (CLI demo — no file dependencies)
# ---------------------------------------------------------------------------

def main() -> None:
    import json

    demo_specs = [
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
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 1.5},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.8},
            ],
        },
        {
            "market": "XRP-EUR",
            "strategies": [],
        },
    ]

    result = build_allocation_splits(demo_specs, total_equity_eur=10_000.0)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
