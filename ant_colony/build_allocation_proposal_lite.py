"""
AC-86: Regime-Adjusted Allocation Proposal

Builds a regime-adjusted allocation proposal on top of the AC-85 regime
overlay and AC-84 allocation envelope. Shows which allocations *would* be
raised, lowered, or held under regime influence — purely observational.

Design principles:
  - proposal_non_binding=True and proposal_simulation_only=True always.
  - Fail-closed: invalid overlay or envelope → PROPOSAL_REJECTED.
  - Deterministic: same overlay + envelope → same proposal every call.
  - Pure core function (build_allocation_proposal) — no I/O, no side effects.
  - Direction logic: bias_scalar > 0 → UPWEIGHT, < 0 → DOWNWEIGHT, = 0 → HOLD.
  - Proposed capital = current_capital × (1 + bias_scalar), clamped ≥ 0.
    This is the *observational* proposal only; it never mutates allocation.

Proposal status values:
  PROPOSAL_ACTIVE    — overlay is OVERLAY_ACTIVE; at least one market processed
  PROPOSAL_BASELINE  — overlay is OVERLAY_BASELINE or envelope inactive
  PROPOSAL_REJECTED  — invalid/missing input

Direction values (per market / per asset class):
  UPWEIGHT    — regime bias > 0; allocation would be raised
  DOWNWEIGHT  — regime bias < 0; allocation would be lowered
  HOLD        — regime bias = 0 or no data; allocation unchanged

Usage (importable):
    from build_allocation_proposal_lite import build_allocation_proposal
    proposal = build_allocation_proposal(regime_overlay, allocation_envelope)

    # Full chain from specs:
    from build_allocation_proposal_lite import build_proposal_from_specs
    result = build_proposal_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Output fields:
    proposal_status             — "PROPOSAL_ACTIVE"|"PROPOSAL_BASELINE"|"PROPOSAL_REJECTED"
    proposal_mode               — "REGIME_ADJUSTED"|"PROPOSAL_BASELINE"|"PROPOSAL_REJECTED"
    asset_class_proposals       — {asset_class: proposal entry}
    market_proposals            — {market: proposal entry}
    total_proposed_capital      — float EUR sum of per-market proposed_capital_eur
    proposed_upweight_count     — int: markets with UPWEIGHT direction
    proposed_downweight_count   — int: markets with DOWNWEIGHT direction
    proposed_hold_count         — int: markets with HOLD direction
    proposal_reason             — human-readable
    proposal_reason_code        — machine-stable
    proposal_non_binding        — always True
    proposal_simulation_only    — always True

Per-market proposal entry fields:
    market                  — str
    asset_class             — str
    current_capital_eur     — float: from allocation envelope
    bias_scalar             — float: from regime overlay
    bias_reason_code        — str: from regime overlay
    proposed_direction      — "UPWEIGHT"|"DOWNWEIGHT"|"HOLD"
    proposed_capital_eur    — float: current × (1 + bias), clamped ≥ 0
    proposed_delta_eur      — float: proposed − current (signed)
    proposal_reason_code    — str: machine-stable direction reason

Per-asset-class proposal entry fields:
    asset_class             — str
    current_capital_eur     — float: sum of market capitals
    bias_scalar             — float: mean of market biases
    proposed_direction      — "UPWEIGHT"|"DOWNWEIGHT"|"HOLD"
    proposed_capital_eur    — float: sum of market proposed_capital_eur
    proposed_delta_eur      — float: sum of market deltas
    market_count            — int
    proposal_reason_code    — str
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "allocation_proposal_v1"

# Proposal status values
PROPOSAL_ACTIVE   = "PROPOSAL_ACTIVE"
PROPOSAL_BASELINE = "PROPOSAL_BASELINE"
PROPOSAL_REJECTED = "PROPOSAL_REJECTED"

# Proposal mode values
MODE_REGIME_ADJUSTED   = "REGIME_ADJUSTED"
MODE_PROPOSAL_BASELINE = "PROPOSAL_BASELINE"
MODE_PROPOSAL_REJECTED = "PROPOSAL_REJECTED"

# Direction values
DIR_UPWEIGHT   = "UPWEIGHT"
DIR_DOWNWEIGHT = "DOWNWEIGHT"
DIR_HOLD       = "HOLD"

# Proposal reason codes (machine-stable)
PROP_UPWEIGHT_BIAS    = "UPWEIGHT_REGIME_BIAS"
PROP_DOWNWEIGHT_BIAS  = "DOWNWEIGHT_REGIME_BIAS"
PROP_HOLD_NEUTRAL     = "HOLD_NEUTRAL_BIAS"
PROP_HOLD_NO_REGIME   = "HOLD_NO_REGIME_DATA"
PROP_HOLD_NO_CAPITAL  = "HOLD_NO_CAPITAL"
PROP_HOLD_GATE_BLOCK  = "HOLD_GATE_BLOCKED"

# Overlay status values (mirrored from AC-85)
_OV_ACTIVE   = "OVERLAY_ACTIVE"
_OV_BASELINE = "OVERLAY_BASELINE"
_OV_REJECTED = "OVERLAY_REJECTED"

_TOL = 1e-9


# ---------------------------------------------------------------------------
# Core proposal function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_allocation_proposal(
    regime_overlay:      object,
    allocation_envelope: object,
) -> dict:
    """
    Build a regime-adjusted allocation proposal from AC-85 overlay + AC-84 envelope.

    proposal_non_binding=True and proposal_simulation_only=True always.
    No execution, no cash movement, no state mutation.

    Args:
        regime_overlay:      dict returned by build_regime_overlay() (AC-85).
        allocation_envelope: dict returned by build_allocation_envelope() (AC-84).

    Returns:
        Allocation proposal dict.
    """
    # Validate inputs
    if not isinstance(regime_overlay, dict):
        return _rejected_proposal("regime_overlay is not a dict")
    if not isinstance(allocation_envelope, dict):
        return _rejected_proposal("allocation_envelope is not a dict")

    if "regime_overlay_status" not in regime_overlay:
        return _rejected_proposal("regime_overlay missing regime_overlay_status")
    if "allocation_envelope_status" not in allocation_envelope:
        return _rejected_proposal("allocation_envelope missing allocation_envelope_status")

    overlay_status = regime_overlay.get("regime_overlay_status", "")

    # Baseline / rejected overlay → baseline proposal
    if overlay_status in (_OV_BASELINE, _OV_REJECTED, ""):
        return _baseline_proposal(overlay_status)

    if overlay_status != _OV_ACTIVE:
        return _baseline_proposal(overlay_status)

    # Gather bias data from overlay
    bias_by_market = regime_overlay.get("allocation_bias_by_market") or {}
    bias_by_ac     = regime_overlay.get("allocation_bias_by_asset_class") or {}

    # Gather capital data from envelope
    env_market_list = allocation_envelope.get("market_allocations") or []

    # Build per-market proposals
    market_proposals:    dict = {}
    ac_accumulator:      dict = {}  # asset_class → {current, proposed, deltas, biases, count, has_regime}

    for ma in env_market_list:
        if not isinstance(ma, dict):
            continue
        market      = str(ma.get("market") or "UNKNOWN")
        asset_class = str(ma.get("asset_class") or "crypto")
        current_cap = _safe_float(ma.get("market_capital_eur"), 0.0)

        # Pull bias for this market
        mkt_bias_entry  = bias_by_market.get(market) or {}
        bias_scalar     = _safe_float(mkt_bias_entry.get("bias_scalar"), 0.0)
        bias_code       = str(mkt_bias_entry.get("bias_reason_code") or "NO_REGIME_DATA")

        direction, prop_code = _compute_direction(bias_scalar, bias_code, current_cap)
        proposed_cap   = _apply_bias(current_cap, bias_scalar)
        delta_eur      = round(proposed_cap - current_cap, 4)

        market_proposals[market] = {
            "market":               market,
            "asset_class":          asset_class,
            "current_capital_eur":  round(current_cap, 4),
            "bias_scalar":          round(bias_scalar, 4),
            "bias_reason_code":     bias_code,
            "proposed_direction":   direction,
            "proposed_capital_eur": proposed_cap,
            "proposed_delta_eur":   delta_eur,
            "proposal_reason_code": prop_code,
        }

        # Accumulate into asset class bucket
        if asset_class not in ac_accumulator:
            ac_accumulator[asset_class] = {
                "current":    0.0,
                "proposed":   0.0,
                "deltas":     0.0,
                "biases":     [],
                "count":      0,
                "has_regime": False,
            }
        acc = ac_accumulator[asset_class]
        acc["current"]  = round(acc["current"]  + current_cap, 4)
        acc["proposed"] = round(acc["proposed"]  + proposed_cap, 4)
        acc["deltas"]   = round(acc["deltas"]    + delta_eur, 4)
        acc["biases"].append(bias_scalar)
        acc["count"]   += 1
        if bias_code not in ("NO_REGIME_DATA", "GATE_BLOCKED"):
            acc["has_regime"] = True

    # Build per-asset-class proposals
    asset_class_proposals: dict = {}
    for ac in sorted(ac_accumulator):
        acc      = ac_accumulator[ac]
        avg_bias = round(sum(acc["biases"]) / max(len(acc["biases"]), 1), 4)

        # Derive direction from avg_bias + whether any market had regime data.
        # Don't rely on AC-85 ac bias code, which may be absent from test fixtures.
        if not acc["has_regime"]:
            ac_bias_code = "NO_REGIME_DATA"
        elif acc["current"] <= _TOL:
            ac_bias_code = "GATE_BLOCKED"  # No capital to adjust
        else:
            ac_bias_code = "REGIME_AVG"  # avg_bias drives direction below

        ac_dir, ac_prop_code = _compute_direction(avg_bias, ac_bias_code, acc["current"])

        asset_class_proposals[ac] = {
            "asset_class":          ac,
            "current_capital_eur":  acc["current"],
            "bias_scalar":          avg_bias,
            "proposed_direction":   ac_dir,
            "proposed_capital_eur": acc["proposed"],
            "proposed_delta_eur":   acc["deltas"],
            "market_count":         acc["count"],
            "proposal_reason_code": ac_prop_code,
        }

    # Totals
    up_count   = sum(1 for p in market_proposals.values() if p["proposed_direction"] == DIR_UPWEIGHT)
    down_count = sum(1 for p in market_proposals.values() if p["proposed_direction"] == DIR_DOWNWEIGHT)
    hold_count = sum(1 for p in market_proposals.values() if p["proposed_direction"] == DIR_HOLD)
    total_proposed = round(sum(p["proposed_capital_eur"] for p in market_proposals.values()), 4)

    proposal_reason, proposal_reason_code = _overall_proposal_summary(up_count, down_count, hold_count)

    return {
        "proposal_status":            PROPOSAL_ACTIVE,
        "proposal_mode":              MODE_REGIME_ADJUSTED,
        "asset_class_proposals":      asset_class_proposals,
        "market_proposals":           market_proposals,
        "total_proposed_capital":     total_proposed,
        "proposed_upweight_count":    up_count,
        "proposed_downweight_count":  down_count,
        "proposed_hold_count":        hold_count,
        "proposal_reason":            proposal_reason,
        "proposal_reason_code":       proposal_reason_code,
        "proposal_non_binding":       True,
        "proposal_simulation_only":   True,
    }


# ---------------------------------------------------------------------------
# Direction + proposed capital helpers
# ---------------------------------------------------------------------------

def _compute_direction(
    bias_scalar: float, bias_code: str, current_cap: float
) -> tuple:
    """Return (direction, proposal_reason_code) for a market or asset class."""
    if bias_code == "GATE_BLOCKED":
        return (DIR_HOLD, PROP_HOLD_GATE_BLOCK)
    if bias_code == "NO_REGIME_DATA":
        return (DIR_HOLD, PROP_HOLD_NO_REGIME)
    if current_cap <= _TOL:
        return (DIR_HOLD, PROP_HOLD_NO_CAPITAL)
    if bias_scalar > _TOL:
        return (DIR_UPWEIGHT, PROP_UPWEIGHT_BIAS)
    if bias_scalar < -_TOL:
        return (DIR_DOWNWEIGHT, PROP_DOWNWEIGHT_BIAS)
    return (DIR_HOLD, PROP_HOLD_NEUTRAL)


def _apply_bias(current_cap: float, bias_scalar: float) -> float:
    """Proposed capital = current × (1 + bias), clamped ≥ 0. Rounded to 4 dp."""
    proposed = current_cap * (1.0 + bias_scalar)
    return round(max(proposed, 0.0), 4)


def _overall_proposal_summary(up: int, down: int, hold: int) -> tuple:
    total = up + down + hold
    if total == 0:
        return ("no markets in proposal", "PROPOSAL_EMPTY")
    parts = []
    if up:
        parts.append(f"{up} UPWEIGHT")
    if down:
        parts.append(f"{down} DOWNWEIGHT")
    if hold:
        parts.append(f"{hold} HOLD")
    reason = f"regime-adjusted proposal: {', '.join(parts)} of {total} market(s)"
    if up > 0 and down == 0:
        code = "ALL_UPWEIGHT_OR_HOLD"
    elif down > 0 and up == 0:
        code = "ALL_DOWNWEIGHT_OR_HOLD"
    elif up > 0 and down > 0:
        code = "MIXED_DIRECTIONS"
    else:
        code = "ALL_HOLD"
    return (reason, code)


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_proposal(reason: str) -> dict:
    return {
        "proposal_status":            PROPOSAL_REJECTED,
        "proposal_mode":              MODE_PROPOSAL_REJECTED,
        "asset_class_proposals":      {},
        "market_proposals":           {},
        "total_proposed_capital":     0.0,
        "proposed_upweight_count":    0,
        "proposed_downweight_count":  0,
        "proposed_hold_count":        0,
        "proposal_reason":            reason,
        "proposal_reason_code":       "PROPOSAL_INVALID_INPUT",
        "proposal_non_binding":       True,
        "proposal_simulation_only":   True,
    }


def _baseline_proposal(overlay_status: str) -> dict:
    return {
        "proposal_status":            PROPOSAL_BASELINE,
        "proposal_mode":              MODE_PROPOSAL_BASELINE,
        "asset_class_proposals":      {},
        "market_proposals":           {},
        "total_proposed_capital":     0.0,
        "proposed_upweight_count":    0,
        "proposed_downweight_count":  0,
        "proposed_hold_count":        0,
        "proposal_reason":            f"overlay is {overlay_status} — regime proposal not generated",
        "proposal_reason_code":       "PROPOSAL_BASELINE_HOLD",
        "proposal_non_binding":       True,
        "proposal_simulation_only":   True,
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

def _load_overlay_module():
    path = Path(__file__).parent / "build_regime_overlay_lite.py"
    spec = importlib.util.spec_from_file_location("_overlay", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81 + AC-82 + AC-84 + AC-85 + AC-86
# ---------------------------------------------------------------------------

def build_proposal_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_regimes: object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → envelope (AC-84) → regime overlay (AC-85)
                             → allocation proposal (AC-86).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal.
    proposal_non_binding=True and proposal_simulation_only=True always.
    """
    _ov_mod = _load_overlay_module()
    pipeline = _ov_mod.build_overlay_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    proposal = build_allocation_proposal(
        pipeline["regime_overlay"],
        pipeline["allocation_envelope"],
    )
    return {
        "splits_result":        pipeline["splits_result"],
        "capital_allocation":   pipeline["capital_allocation"],
        "allocation_envelope":  pipeline["allocation_envelope"],
        "regime_overlay":       pipeline["regime_overlay"],
        "allocation_proposal":  proposal,
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
    regimes = {
        "BTC-EUR": {"trend_regime": "BULL",     "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BEAR",     "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        # SOL-EUR missing → HOLD / NO_REGIME_DATA
    }
    result = build_proposal_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    prop = result["allocation_proposal"]
    print(json.dumps({
        "proposal_status":           prop["proposal_status"],
        "proposal_mode":             prop["proposal_mode"],
        "total_proposed_capital":    prop["total_proposed_capital"],
        "proposed_upweight_count":   prop["proposed_upweight_count"],
        "proposed_downweight_count": prop["proposed_downweight_count"],
        "proposed_hold_count":       prop["proposed_hold_count"],
        "proposal_reason_code":      prop["proposal_reason_code"],
        "proposal_non_binding":      prop["proposal_non_binding"],
        "proposal_simulation_only":  prop["proposal_simulation_only"],
        "market_proposals": {
            m: {
                "current_capital_eur":  p["current_capital_eur"],
                "bias_scalar":          p["bias_scalar"],
                "proposed_direction":   p["proposed_direction"],
                "proposed_capital_eur": p["proposed_capital_eur"],
                "proposed_delta_eur":   p["proposed_delta_eur"],
            }
            for m, p in prop["market_proposals"].items()
        },
        "asset_class_proposals": {
            ac: {
                "current_capital_eur":  p["current_capital_eur"],
                "proposed_direction":   p["proposed_direction"],
                "proposed_capital_eur": p["proposed_capital_eur"],
            }
            for ac, p in prop["asset_class_proposals"].items()
        },
    }, indent=2))


if __name__ == "__main__":
    main()
