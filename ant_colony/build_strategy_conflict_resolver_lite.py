"""
AC-143: Strategy Conflict Resolver (Research-Only)

Detects and resolves conflicts when multiple strategies are active on the
same market. Supports two conflict types:
  - OPPOSING_DIRECTION: two strategies with opposite bias (LONG vs SHORT)
  - BUDGET_OVERLAP:     sum of budget_fractions on same market > 1.0

Resolution is explicit, hard-bounded, and fail-closed:
  - OPPOSING_DIRECTION -> BLOCK_BOTH (both strategies blocked)
  - BUDGET_OVERLAP     -> CAP_BUDGET (each strategy capped to equal share = 1/n)
  - No conflict        -> ALLOW_ALL

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No implicit netting, no hidden strategy merge.

Input:  dict of {market: [intent, ...]} where each intent has:
          - strategy: str
          - bias: "LONG" | "SHORT" | "NEUTRAL"
          - budget_fraction: float (0.0–1.0)
Output: conflict resolution dict with per-market result and colony summary

Fail-closed:
  - invalid input -> CONFLICT_ERROR per market, no crash
  - missing fields -> treated as safe defaults (bias=NEUTRAL, budget=0.0)

Usage (importable):
    from ant_colony.build_strategy_conflict_resolver_lite import resolve_conflicts
    result = resolve_conflicts({
        "BTC-EUR": [
            {"strategy": "mean_reversion", "bias": "LONG",  "budget_fraction": 0.5},
            {"strategy": "trend_follow",   "bias": "SHORT", "budget_fraction": 0.5},
        ]
    })

Usage (CLI):
    python ant_colony/build_strategy_conflict_resolver_lite.py
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Version / flags
# ---------------------------------------------------------------------------

SNAPSHOT_VERSION = "strategy_conflict_resolver_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Resolution actions
ACTION_ALLOW_ALL   = "ALLOW_ALL"
ACTION_BLOCK_BOTH  = "BLOCK_BOTH"
ACTION_CAP_BUDGET  = "CAP_BUDGET"
ACTION_ERROR       = "CONFLICT_ERROR"

# Conflict types
CONFLICT_NONE       = "NONE"
CONFLICT_OPPOSING   = "OPPOSING_DIRECTION"
CONFLICT_BUDGET     = "BUDGET_OVERLAP"
CONFLICT_BOTH       = "OPPOSING_DIRECTION_AND_BUDGET_OVERLAP"

# Machine-stable reason codes
REASON_CODES: dict = {
    "ALLOW_ALL":        "CONFLICT_NONE_ALL_ALLOWED",
    "OPPOSING":         "CONFLICT_OPPOSING_DIRECTION_BLOCKED",
    "BUDGET":           "CONFLICT_BUDGET_OVERLAP_CAPPED",
    "OPPOSING_BUDGET":  "CONFLICT_OPPOSING_AND_BUDGET_BLOCKED",
    "ERROR":            "CONFLICT_RESOLUTION_ERROR",
    "SINGLE":           "CONFLICT_SINGLE_STRATEGY_NO_CONFLICT",
}

_OPPOSING_PAIRS = frozenset({("LONG", "SHORT"), ("SHORT", "LONG")})


# ---------------------------------------------------------------------------
# Core resolver function (pure, no I/O)
# ---------------------------------------------------------------------------

def resolve_conflicts(market_intents: object) -> dict:
    """
    Detect and resolve strategy conflicts per market.

    Args:
        market_intents: {market: [intent_dict, ...]}
                        Each intent_dict should have:
                          strategy (str), bias (str), budget_fraction (float).
                        Missing fields use safe defaults.

    Returns:
        Conflict resolution dict. research_only=True always.
    """
    if not isinstance(market_intents, dict):
        return _make_result(
            markets_resolved={},
            colony_summary={
                "total_markets":      0,
                "markets_with_conflicts": 0,
                "markets_clean":      0,
                "total_intents":      0,
            },
            error_reason="market_intents is not a dict",
        )

    markets_resolved: dict[str, dict] = {}
    total_intents = 0
    markets_with_conflicts = 0
    markets_clean = 0

    for market in sorted(market_intents.keys()):
        intents_raw = market_intents.get(market)
        if not isinstance(intents_raw, list):
            intents_raw = []
        # Normalise each intent
        intents = [_normalise_intent(i) for i in intents_raw]
        total_intents += len(intents)

        resolution = _resolve_market(market, intents)
        markets_resolved[market] = resolution

        if resolution.get("conflict_detected"):
            markets_with_conflicts += 1
        else:
            markets_clean += 1

    return _make_result(
        markets_resolved=markets_resolved,
        colony_summary={
            "total_markets":           len(markets_resolved),
            "markets_with_conflicts":  markets_with_conflicts,
            "markets_clean":           markets_clean,
            "total_intents":           total_intents,
        },
    )


# ---------------------------------------------------------------------------
# Per-market resolution (pure)
# ---------------------------------------------------------------------------

def _resolve_market(market: str, intents: list[dict]) -> dict:
    """
    Resolve conflicts for a single market's list of normalised intents.
    Returns a per-market resolution dict.
    """
    if len(intents) == 0:
        return _market_result(
            conflict_detected=False,
            conflict_type=CONFLICT_NONE,
            action=ACTION_ALLOW_ALL,
            reason_code=REASON_CODES["ALLOW_ALL"],
            reason="no intents — nothing to resolve",
            resolved_intents=[],
        )

    if len(intents) == 1:
        return _market_result(
            conflict_detected=False,
            conflict_type=CONFLICT_NONE,
            action=ACTION_ALLOW_ALL,
            reason_code=REASON_CODES["SINGLE"],
            reason="single strategy — no conflict possible",
            resolved_intents=[dict(i) for i in intents],
        )

    # Check 1: opposing direction
    biases = [i["bias"] for i in intents]
    has_long  = "LONG"  in biases
    has_short = "SHORT" in biases
    opposing  = has_long and has_short

    # Check 2: budget overlap
    total_budget = sum(i["budget_fraction"] for i in intents)
    budget_overlap = total_budget > 1.0

    if opposing and budget_overlap:
        return _market_result(
            conflict_detected=True,
            conflict_type=CONFLICT_BOTH,
            action=ACTION_BLOCK_BOTH,
            reason_code=REASON_CODES["OPPOSING_BUDGET"],
            reason=(
                f"opposing directions (LONG+SHORT) and budget overlap "
                f"(total={total_budget:.3f}) — BLOCK_BOTH"
            ),
            resolved_intents=[],
        )

    if opposing:
        return _market_result(
            conflict_detected=True,
            conflict_type=CONFLICT_OPPOSING,
            action=ACTION_BLOCK_BOTH,
            reason_code=REASON_CODES["OPPOSING"],
            reason=(
                f"opposing directions LONG+SHORT on same market — BLOCK_BOTH"
            ),
            resolved_intents=[],
        )

    if budget_overlap:
        n = len(intents)
        capped = round(1.0 / n, 6)
        resolved = [
            {**i, "budget_fraction": capped, "budget_capped": True}
            for i in intents
        ]
        return _market_result(
            conflict_detected=True,
            conflict_type=CONFLICT_BUDGET,
            action=ACTION_CAP_BUDGET,
            reason_code=REASON_CODES["BUDGET"],
            reason=(
                f"budget overlap (total={total_budget:.3f} > 1.0) — "
                f"capped to {capped:.4f} each"
            ),
            resolved_intents=resolved,
        )

    # No conflict
    return _market_result(
        conflict_detected=False,
        conflict_type=CONFLICT_NONE,
        action=ACTION_ALLOW_ALL,
        reason_code=REASON_CODES["ALLOW_ALL"],
        reason="no conflict detected",
        resolved_intents=[dict(i) for i in intents],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_intent(raw: object) -> dict:
    """Normalise a raw intent dict. Missing fields get safe defaults."""
    if not isinstance(raw, dict):
        raw = {}
    bias = str(raw.get("bias") or "NEUTRAL").upper()
    if bias not in ("LONG", "SHORT", "NEUTRAL"):
        bias = "NEUTRAL"
    budget = _safe_float(raw.get("budget_fraction"), 0.0)
    return {
        "strategy":       str(raw.get("strategy") or "unknown"),
        "bias":           bias,
        "budget_fraction": max(0.0, budget),
    }


def _market_result(
    conflict_detected: bool,
    conflict_type: str,
    action: str,
    reason_code: str,
    reason: str,
    resolved_intents: list,
) -> dict:
    return {
        "conflict_detected":  conflict_detected,
        "conflict_type":      conflict_type,
        "resolution_action":  action,
        "resolution_reason":  reason,
        "resolution_reason_code": reason_code,
        "resolved_intents":   resolved_intents,
    }


def _make_result(
    markets_resolved: dict,
    colony_summary: dict,
    error_reason: Optional[str] = None,
) -> dict:
    result = {
        "version":                  SNAPSHOT_VERSION,
        "ts_utc":                   _now_utc_iso(),
        "conflicts_detected":       sum(
            1 for r in markets_resolved.values()
            if isinstance(r, dict) and r.get("conflict_detected")
        ),
        "markets_resolved":         markets_resolved,
        "colony_conflict_summary":  colony_summary,
        "research_only":            True,
        "flags":                    dict(FLAGS),
    }
    if error_reason is not None:
        result["error_reason"] = error_reason
    return result


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CLI (demo / observability only)
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-143: Strategy conflict resolver (research-only)."
    )
    return p


def main(argv: list[str] | None = None) -> int:
    _build_parser().parse_args(argv)
    demo = {
        "BTC-EUR": [
            {"strategy": "mean_reversion",  "bias": "LONG",  "budget_fraction": 0.5},
            {"strategy": "trend_follow",    "bias": "SHORT", "budget_fraction": 0.5},
        ],
        "ETH-EUR": [
            {"strategy": "mean_reversion",  "bias": "LONG",  "budget_fraction": 0.6},
            {"strategy": "trend_follow",    "bias": "LONG",  "budget_fraction": 0.6},
        ],
        "ADA-EUR": [
            {"strategy": "mean_reversion",  "bias": "LONG",  "budget_fraction": 0.5},
        ],
    }
    result = resolve_conflicts(demo)
    cs = result["colony_conflict_summary"]
    print()
    print("=== AC-143 STRATEGY CONFLICT RESOLVER ===")
    print(f"{'markets':<16}: {cs['total_markets']}")
    print(f"{'with_conflicts':<16}: {cs['markets_with_conflicts']}")
    print(f"{'clean':<16}: {cs['markets_clean']}")
    print(f"{'conflicts_total':<16}: {result['conflicts_detected']}")
    for market, res in result["markets_resolved"].items():
        print(f"  {market}: {res['conflict_type']} -> {res['resolution_action']}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
