"""
AC-83: Execution Bridge (Paper-Only, Live-Ready Boundary)

Translates AC-82 capital allocation output into paper execution intents.
Provides a formal, auditable boundary between allocation simulation and
paper execution intent generation.

paper_only=True and live_activation_allowed=False are set on EVERY output.
No broker coupling, no live order path, no real portfolio mutation.

Design principles:
  - paper_only=True always — never removed, never overridden.
  - live_activation_allowed=False always — explicit contract boundary.
  - Fail-closed: invalid allocation input → BRIDGE_BLOCKED, all intents blocked.
  - Deterministic: same allocation → same bridge output every call.
  - Pure core function (build_execution_bridge) — no I/O, no side effects.
  - Clear separation: allocation simulation | intent preview | live execution.

Bridge status values:
  BRIDGE_ACTIVE    — ≥1 ALLOWED intents produced
  BRIDGE_BLOCKED   — all intents blocked (no capital, baseline, or invalid input)
  BRIDGE_BASELINE  — allocation mode is baseline hold; no intents generated

Blocking rules (per strategy allocation):
  ZERO_CAPITAL         — capital_eur == 0.0
  BASELINE_PLACEHOLDER — strategy_id is "BASELINE"
  BASELINE_SPLIT       — split_reason_code is "SPLIT_BASELINE_HOLD"
  MARKET_INVALID       — market_split_valid is False

Intent action values (ALLOWED intents only):
  PAPER_ALLOCATE       — positive capital assigned to this strategy
  PAPER_HOLD           — intent produced but no capital change needed

Usage (importable):
    from build_execution_bridge_paper_lite import build_execution_bridge
    bridge = build_execution_bridge(capital_allocation_result)

    # Full chain from specs:
    from build_execution_bridge_paper_lite import build_bridge_from_specs
    result = build_bridge_from_specs(market_specs, total_equity_eur=10_000.0)

Output fields:
    execution_bridge_status  — "BRIDGE_ACTIVE"|"BRIDGE_BLOCKED"|"BRIDGE_BASELINE"
    bridge_mode              — "PAPER_MULTI_STRATEGY"|"PAPER_BASELINE_HOLD"|"PAPER_REJECTED"
    intent_count             — int: total intents (allowed + blocked)
    allowed_count            — int: intents with status ALLOWED
    blocked_count            — int: intents with status BLOCKED
    blocked_reasons          — list of distinct block reason strings
    intents_by_market        — {market: [intent, ...]}
    intents_by_strategy      — {strategy_id: [intent, ...]}
    paper_only               — always True
    live_activation_allowed  — always False
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "execution_bridge_paper_v1"

# Bridge status values
BRIDGE_ACTIVE   = "BRIDGE_ACTIVE"
BRIDGE_BLOCKED  = "BRIDGE_BLOCKED"
BRIDGE_BASELINE = "BRIDGE_BASELINE"

# Bridge mode values
MODE_PAPER_MULTI    = "PAPER_MULTI_STRATEGY"
MODE_PAPER_BASELINE = "PAPER_BASELINE_HOLD"
MODE_PAPER_REJECTED = "PAPER_REJECTED"

# Intent status values
INTENT_ALLOWED = "ALLOWED"
INTENT_BLOCKED = "BLOCKED"

# Intent action values
ACTION_PAPER_ALLOCATE = "PAPER_ALLOCATE"
ACTION_PAPER_HOLD     = "PAPER_HOLD"
ACTION_PAPER_BLOCKED  = "PAPER_BLOCKED"

# Block reason codes
BLOCK_ZERO_CAPITAL    = "ZERO_CAPITAL"
BLOCK_BASELINE_STRAT  = "BASELINE_PLACEHOLDER"
BLOCK_BASELINE_SPLIT  = "BASELINE_SPLIT"
BLOCK_MARKET_INVALID  = "MARKET_INVALID"

_TOL = 1e-9


# ---------------------------------------------------------------------------
# Core bridge function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_execution_bridge(capital_allocation: object) -> dict:
    """
    Translate an AC-82 capital allocation result into paper execution intents.

    paper_only=True and live_activation_allowed=False are ALWAYS set.
    No broker coupling, no live execution, no state mutation.

    Args:
        capital_allocation: dict returned by build_capital_allocation() (AC-82).

    Returns:
        Bridge output dict.
    """
    # Validate input
    if not isinstance(capital_allocation, dict):
        return _rejected_bridge("capital_allocation is not a dict")

    if "allocation_summary" not in capital_allocation or \
       "market_allocations" not in capital_allocation:
        return _rejected_bridge("capital_allocation missing required keys")

    alloc_mode = (capital_allocation.get("allocation_summary") or {}).get(
        "allocation_mode", "BASELINE_HOLD"
    )
    market_allocations = capital_allocation.get("market_allocations") or []

    # Baseline-hold shortcut — no intents needed
    if alloc_mode in ("BASELINE_HOLD", "EMPTY") or not market_allocations:
        return _baseline_bridge(alloc_mode)

    # Build intents from market allocations
    all_intents:    list = []
    by_market:      dict = {}
    by_strategy:    dict = {}
    blocked_reasons: set = set()

    for ma in market_allocations:
        if not isinstance(ma, dict):
            continue
        market        = str(ma.get("market") or "UNKNOWN")
        split_valid   = bool(ma.get("market_split_valid", False))
        strategy_allocs = ma.get("strategy_allocations") or []

        market_intents = []
        for sa in strategy_allocs:
            if not isinstance(sa, dict):
                continue
            intent = _build_intent(market, sa, split_valid)
            all_intents.append(intent)
            market_intents.append(intent)
            if intent["intent_status"] == INTENT_BLOCKED:
                blocked_reasons.add(intent["block_reason"])

            sid = intent["strategy_id"]
            by_strategy.setdefault(sid, []).append(intent)

        if market_intents:
            by_market[market] = market_intents

    allowed = [i for i in all_intents if i["intent_status"] == INTENT_ALLOWED]
    blocked = [i for i in all_intents if i["intent_status"] == INTENT_BLOCKED]

    if not all_intents:
        return _baseline_bridge(alloc_mode)

    if allowed:
        bridge_status = BRIDGE_ACTIVE
        bridge_mode   = MODE_PAPER_MULTI
    else:
        bridge_status = BRIDGE_BLOCKED
        bridge_mode   = MODE_PAPER_BASELINE

    return {
        "execution_bridge_status": bridge_status,
        "bridge_mode":             bridge_mode,
        "intent_count":            len(all_intents),
        "allowed_count":           len(allowed),
        "blocked_count":           len(blocked),
        "blocked_reasons":         sorted(blocked_reasons),
        "intents_by_market":       by_market,
        "intents_by_strategy":     by_strategy,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Per-strategy intent builder
# ---------------------------------------------------------------------------

def _build_intent(market: str, sa: dict, split_valid: bool) -> dict:
    """Build a single execution intent from a strategy allocation entry."""
    sid         = str(sa.get("strategy_id") or "UNKNOWN")
    family      = str(sa.get("strategy_family") or "UNKNOWN")
    capital     = _safe_float(sa.get("capital_eur"), 0.0)
    weight      = _safe_float(sa.get("simulated_weight"), 0.0)
    reason_code = str(sa.get("split_reason_code") or "")

    # Apply blocking rules
    if sid == "BASELINE":
        return _blocked_intent(market, sid, family, capital, weight, BLOCK_BASELINE_STRAT)
    if reason_code == "SPLIT_BASELINE_HOLD":
        return _blocked_intent(market, sid, family, capital, weight, BLOCK_BASELINE_SPLIT)
    if not split_valid:
        return _blocked_intent(market, sid, family, capital, weight, BLOCK_MARKET_INVALID)
    if capital <= _TOL:
        return _blocked_intent(market, sid, family, capital, weight, BLOCK_ZERO_CAPITAL)

    # Allowed intent
    return {
        "market":                market,
        "strategy_id":           sid,
        "strategy_family":       family,
        "intent_action":         ACTION_PAPER_ALLOCATE,
        "intent_notional_eur":   round(capital, 4),
        "intent_weight":         round(weight, 6),
        "intent_status":         INTENT_ALLOWED,
        "block_reason":          "",
        "paper_only":            True,
        "live_activation_allowed": False,
    }


def _blocked_intent(
    market: str, sid: str, family: str,
    capital: float, weight: float, block_reason: str,
) -> dict:
    return {
        "market":                market,
        "strategy_id":           sid,
        "strategy_family":       family,
        "intent_action":         ACTION_PAPER_BLOCKED,
        "intent_notional_eur":   0.0,
        "intent_weight":         round(weight, 6),
        "intent_status":         INTENT_BLOCKED,
        "block_reason":          block_reason,
        "paper_only":            True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_bridge(reason: str) -> dict:
    return {
        "execution_bridge_status": BRIDGE_BLOCKED,
        "bridge_mode":             MODE_PAPER_REJECTED,
        "intent_count":            0,
        "allowed_count":           0,
        "blocked_count":           0,
        "blocked_reasons":         [reason],
        "intents_by_market":       {},
        "intents_by_strategy":     {},
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _baseline_bridge(alloc_mode: str) -> dict:
    return {
        "execution_bridge_status": BRIDGE_BASELINE,
        "bridge_mode":             MODE_PAPER_BASELINE,
        "intent_count":            0,
        "allowed_count":           0,
        "blocked_count":           0,
        "blocked_reasons":         [],
        "intents_by_market":       {},
        "intents_by_strategy":     {},
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81 + AC-82 + AC-83
# ---------------------------------------------------------------------------

def build_bridge_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → execution bridge (AC-83).

    Returns dict with keys: splits_result, capital_allocation, execution_bridge.
    paper_only=True and live_activation_allowed=False always.
    """
    _alloc_mod = _load_allocator_module()
    pipeline   = _alloc_mod.build_capital_allocation_from_specs(
        market_specs, total_equity_eur, market_capital_fractions
    )
    bridge = build_execution_bridge(pipeline["capital_allocation"])
    return {
        "splits_result":      pipeline["splits_result"],
        "capital_allocation": pipeline["capital_allocation"],
        "execution_bridge":   bridge,
    }


def _load_allocator_module():
    path = Path(__file__).parent / "build_queen_capital_allocator_lite.py"
    spec = importlib.util.spec_from_file_location("_alloc", path)
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
            ],
        },
        {
            "market": "SOL-EUR",
            "strategies": [],
        },
    ]
    result = build_bridge_from_specs(specs, total_equity_eur=10_000.0)
    # Print bridge only (compact output)
    bridge = result["execution_bridge"]
    print(json.dumps({
        "execution_bridge_status": bridge["execution_bridge_status"],
        "bridge_mode":             bridge["bridge_mode"],
        "intent_count":            bridge["intent_count"],
        "allowed_count":           bridge["allowed_count"],
        "blocked_count":           bridge["blocked_count"],
        "blocked_reasons":         bridge["blocked_reasons"],
        "paper_only":              bridge["paper_only"],
        "live_activation_allowed": bridge["live_activation_allowed"],
        "intents_by_market": {
            m: [{"strategy_id": i["strategy_id"], "intent_action": i["intent_action"],
                 "intent_notional_eur": i["intent_notional_eur"],
                 "intent_status": i["intent_status"]}
                for i in intents]
            for m, intents in bridge["intents_by_market"].items()
        },
    }, indent=2))


if __name__ == "__main__":
    main()
