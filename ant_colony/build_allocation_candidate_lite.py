"""
AC-88: Selected Allocation Candidate + Paper Transition Preview

Two layers on top of AC-87 conflict selection:

1. Selected Allocation Candidate
   Consolidates the AC-87 safe selection into one explicit candidate object
   that represents the single chosen allocation state.

2. Paper Transition Preview
   Shows what the paper portfolio *would* need to change — per market —
   to move from the current envelope allocation to the selected candidate.
   Purely observational: no execution, no cash movement, no state mutation.

Design principles:
  - candidate_non_binding=True and candidate_simulation_only=True always.
  - transition_non_binding=True and transition_simulation_only=True always.
  - Fail-closed: invalid selection → CANDIDATE_REJECTED / TRANSITION_REJECTED.
  - Deterministic: same inputs → same outputs every call.
  - Pure core functions — no I/O, no side effects.
  - Transition direction: delta > 0 → INCREASE, delta < 0 → DECREASE, else HOLD.

Candidate status values:
  CANDIDATE_ACTIVE    — selection is SELECTION_ACTIVE; candidate built
  CANDIDATE_BASELINE  — selection is SELECTION_BASELINE
  CANDIDATE_REJECTED  — invalid/missing input

Transition status values:
  TRANSITION_ACTIVE    — candidate is CANDIDATE_ACTIVE; steps computed
  TRANSITION_BASELINE  — candidate is CANDIDATE_BASELINE
  TRANSITION_REJECTED  — invalid/missing input

Transition directions (per market):
  INCREASE  — selected_capital > current_capital (paper allocation would rise)
  DECREASE  — selected_capital < current_capital (paper allocation would fall)
  HOLD      — no change required

Usage (importable):
    from build_allocation_candidate_lite import build_allocation_candidate
    candidate = build_allocation_candidate(conflict_selection)

    from build_allocation_candidate_lite import build_paper_transition_preview
    transition = build_paper_transition_preview(candidate, allocation_envelope)

    # Combined:
    from build_allocation_candidate_lite import build_candidate_and_transition
    result = build_candidate_and_transition(conflict_selection, allocation_envelope)

    # Full chain from specs:
    from build_allocation_candidate_lite import build_transition_from_specs
    result = build_transition_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Candidate output fields:
    candidate_status        — "CANDIDATE_ACTIVE"|"CANDIDATE_BASELINE"|"CANDIDATE_REJECTED"
    candidate_mode          — "CANDIDATE_SELECTED"|"CANDIDATE_BASELINE"|"CANDIDATE_REJECTED"
    candidate_market_count  — int: markets in selected_candidate
    candidate_total_capital — float EUR: sum of selected proposed_capital_eur
    candidate_reason        — human-readable
    candidate_reason_code   — machine-stable
    selected_candidate      — {market: candidate entry}
    candidate_non_binding   — always True
    candidate_simulation_only — always True

Per-market candidate entry:
    market                  — str
    asset_class             — str
    selected_direction      — str: from AC-87 (UPWEIGHT/DOWNWEIGHT/HOLD)
    proposed_capital_eur    — float: from AC-87
    proposed_delta_eur      — float: from AC-87
    conflict_adjusted       — bool: from AC-87

Transition output fields:
    transition_status               — "TRANSITION_ACTIVE"|...|"TRANSITION_REJECTED"
    transition_mode                 — "PAPER_TRANSITION_PREVIEW"|...
    transition_steps                — list of per-market transition entries
    transition_summary              — {total_increase_eur, total_decrease_eur, net_change_eur}
    estimated_reallocation_count    — int: markets with INCREASE or DECREASE
    estimated_hold_count            — int: markets with HOLD
    transition_reason               — human-readable
    transition_reason_code          — machine-stable
    transition_non_binding          — always True
    transition_simulation_only      — always True

Per-market transition step:
    market                  — str
    asset_class             — str
    current_capital_eur     — float: from allocation envelope
    selected_capital_eur    — float: from candidate
    delta_eur               — float: selected − current (signed)
    transition_direction    — "INCREASE"|"DECREASE"|"HOLD"
    transition_reason_code  — str: machine-stable
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "allocation_candidate_v1"

# Candidate status
CANDIDATE_ACTIVE   = "CANDIDATE_ACTIVE"
CANDIDATE_BASELINE = "CANDIDATE_BASELINE"
CANDIDATE_REJECTED = "CANDIDATE_REJECTED"

# Candidate mode
MODE_CAND_SELECTED = "CANDIDATE_SELECTED"
MODE_CAND_BASELINE = "CANDIDATE_BASELINE"
MODE_CAND_REJECTED = "CANDIDATE_REJECTED"

# Transition status
TRANSITION_ACTIVE   = "TRANSITION_ACTIVE"
TRANSITION_BASELINE = "TRANSITION_BASELINE"
TRANSITION_REJECTED = "TRANSITION_REJECTED"

# Transition mode
MODE_PAPER_PREVIEW   = "PAPER_TRANSITION_PREVIEW"
MODE_TRANS_BASELINE  = "TRANSITION_BASELINE"
MODE_TRANS_REJECTED  = "TRANSITION_REJECTED"

# Transition directions
TRANS_INCREASE = "INCREASE"
TRANS_DECREASE = "DECREASE"
TRANS_HOLD     = "HOLD"

# Transition reason codes
TRANS_CODE_INCREASE = "PAPER_INCREASE"
TRANS_CODE_DECREASE = "PAPER_DECREASE"
TRANS_CODE_HOLD     = "PAPER_HOLD"
TRANS_CODE_NO_DATA  = "NO_CURRENT_DATA"

# Selection status (mirrored from AC-87)
_SEL_ACTIVE   = "SELECTION_ACTIVE"
_SEL_BASELINE = "SELECTION_BASELINE"
_SEL_REJECTED = "SELECTION_REJECTED"

_TOL = 1e-9


# ---------------------------------------------------------------------------
# Core candidate function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_allocation_candidate(conflict_selection: object) -> dict:
    """
    Consolidate an AC-87 conflict selection into one selected allocation candidate.

    candidate_non_binding=True and candidate_simulation_only=True always.

    Args:
        conflict_selection: dict returned by resolve_proposal_conflicts() (AC-87).

    Returns:
        Selected allocation candidate dict.
    """
    if not isinstance(conflict_selection, dict):
        return _rejected_candidate("conflict_selection is not a dict")

    if "selection_status" not in conflict_selection:
        return _rejected_candidate("conflict_selection missing selection_status")

    sel_status = conflict_selection.get("selection_status", "")

    if sel_status in (_SEL_BASELINE, _SEL_REJECTED, ""):
        return _baseline_candidate(sel_status)

    if sel_status != _SEL_ACTIVE:
        return _baseline_candidate(sel_status)

    selected_proposals = conflict_selection.get("selected_proposals") or {}

    # Build selected_candidate from AC-87 selected_proposals
    selected_candidate: dict = {}
    for market, sp in selected_proposals.items():
        if not isinstance(sp, dict):
            continue
        selected_candidate[market] = {
            "market":               market,
            "asset_class":          str(sp.get("asset_class") or "crypto"),
            "selected_direction":   str(sp.get("selected_direction") or "HOLD"),
            "proposed_capital_eur": _safe_float(sp.get("proposed_capital_eur"), 0.0),
            "proposed_delta_eur":   _safe_float(sp.get("proposed_delta_eur"), 0.0),
            "conflict_adjusted":    bool(sp.get("conflict_adjusted", False)),
        }

    n_markets      = len(selected_candidate)
    total_capital  = round(
        sum(c["proposed_capital_eur"] for c in selected_candidate.values()), 4
    )

    if n_markets == 0:
        cand_reason      = "no valid markets in safe selection"
        cand_reason_code = "CANDIDATE_EMPTY_SELECTION"
    else:
        cand_reason = (
            f"candidate selected: {n_markets} market(s), "
            f"total proposed capital {total_capital:.2f} EUR"
        )
        cand_reason_code = "CANDIDATE_BUILT_FROM_SELECTION"

    return {
        "candidate_status":         CANDIDATE_ACTIVE,
        "candidate_mode":           MODE_CAND_SELECTED,
        "candidate_market_count":   n_markets,
        "candidate_total_capital":  total_capital,
        "candidate_reason":         cand_reason,
        "candidate_reason_code":    cand_reason_code,
        "selected_candidate":       selected_candidate,
        "candidate_non_binding":    True,
        "candidate_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# Core transition function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_paper_transition_preview(
    allocation_candidate: object,
    allocation_envelope:  object,
) -> dict:
    """
    Build a paper transition preview showing the per-market step from current
    envelope allocation to the selected candidate allocation.

    transition_non_binding=True and transition_simulation_only=True always.
    No execution, no cash movement, no state mutation.

    Args:
        allocation_candidate: dict returned by build_allocation_candidate() (AC-88).
        allocation_envelope:  dict returned by build_allocation_envelope() (AC-84).

    Returns:
        Paper transition preview dict.
    """
    if not isinstance(allocation_candidate, dict):
        return _rejected_transition("allocation_candidate is not a dict")
    if not isinstance(allocation_envelope, dict):
        return _rejected_transition("allocation_envelope is not a dict")

    if "candidate_status" not in allocation_candidate:
        return _rejected_transition("allocation_candidate missing candidate_status")
    if "allocation_envelope_status" not in allocation_envelope:
        return _rejected_transition("allocation_envelope missing allocation_envelope_status")

    cand_status = allocation_candidate.get("candidate_status", "")
    if cand_status in (CANDIDATE_BASELINE, CANDIDATE_REJECTED, ""):
        return _baseline_transition(cand_status)
    if cand_status != CANDIDATE_ACTIVE:
        return _baseline_transition(cand_status)

    selected_candidate  = allocation_candidate.get("selected_candidate") or {}

    # Build a current-capital lookup from the envelope
    current_by_market: dict = {}
    for ma in (allocation_envelope.get("market_allocations") or []):
        if not isinstance(ma, dict):
            continue
        market = str(ma.get("market") or "UNKNOWN")
        current_by_market[market] = _safe_float(ma.get("market_capital_eur"), 0.0)

    # Build per-market transition steps
    steps: list = []
    total_increase = 0.0
    total_decrease = 0.0

    for market, centry in selected_candidate.items():
        if not isinstance(centry, dict):
            continue
        ac          = str(centry.get("asset_class") or "crypto")
        sel_capital = _safe_float(centry.get("proposed_capital_eur"), 0.0)
        cur_capital = current_by_market.get(market, 0.0)
        delta       = round(sel_capital - cur_capital, 4)

        if delta > _TOL:
            direction   = TRANS_INCREASE
            trans_code  = TRANS_CODE_INCREASE
            total_increase = round(total_increase + delta, 4)
        elif delta < -_TOL:
            direction   = TRANS_DECREASE
            trans_code  = TRANS_CODE_DECREASE
            total_decrease = round(total_decrease + abs(delta), 4)
        else:
            direction  = TRANS_HOLD
            trans_code = TRANS_CODE_HOLD

        steps.append({
            "market":               market,
            "asset_class":          ac,
            "current_capital_eur":  round(cur_capital, 4),
            "selected_capital_eur": round(sel_capital, 4),
            "delta_eur":            delta,
            "transition_direction": direction,
            "transition_reason_code": trans_code,
        })

    realloc_count = sum(1 for s in steps if s["transition_direction"] != TRANS_HOLD)
    hold_count    = sum(1 for s in steps if s["transition_direction"] == TRANS_HOLD)
    net_change    = round(total_increase - total_decrease, 4)

    if not steps:
        trans_reason      = "no candidate markets available for transition preview"
        trans_reason_code = "TRANSITION_EMPTY"
    elif realloc_count == 0:
        trans_reason      = f"all {hold_count} market(s) in HOLD — no paper reallocation needed"
        trans_reason_code = "TRANSITION_ALL_HOLD"
    else:
        trans_reason = (
            f"paper transition preview: {realloc_count} market(s) to reallocate "
            f"(+{total_increase:.2f} EUR increase, -{total_decrease:.2f} EUR decrease), "
            f"{hold_count} market(s) on HOLD"
        )
        trans_reason_code = "TRANSITION_PREVIEW_ACTIVE"

    return {
        "transition_status":            TRANSITION_ACTIVE,
        "transition_mode":              MODE_PAPER_PREVIEW,
        "transition_steps":             steps,
        "transition_summary": {
            "total_increase_eur":  total_increase,
            "total_decrease_eur":  total_decrease,
            "net_change_eur":      net_change,
        },
        "estimated_reallocation_count": realloc_count,
        "estimated_hold_count":         hold_count,
        "transition_reason":            trans_reason,
        "transition_reason_code":       trans_reason_code,
        "transition_non_binding":       True,
        "transition_simulation_only":   True,
    }


# ---------------------------------------------------------------------------
# Combined builder
# ---------------------------------------------------------------------------

def build_candidate_and_transition(
    conflict_selection:  object,
    allocation_envelope: object,
) -> dict:
    """
    Build both the selected allocation candidate (AC-88a) and the paper
    transition preview (AC-88b) in one call.

    Returns dict with keys: allocation_candidate, paper_transition_preview.
    """
    candidate  = build_allocation_candidate(conflict_selection)
    transition = build_paper_transition_preview(candidate, allocation_envelope)
    return {
        "allocation_candidate":    candidate,
        "paper_transition_preview": transition,
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_candidate(reason: str) -> dict:
    return {
        "candidate_status":         CANDIDATE_REJECTED,
        "candidate_mode":           MODE_CAND_REJECTED,
        "candidate_market_count":   0,
        "candidate_total_capital":  0.0,
        "candidate_reason":         reason,
        "candidate_reason_code":    "CANDIDATE_INVALID_INPUT",
        "selected_candidate":       {},
        "candidate_non_binding":    True,
        "candidate_simulation_only": True,
    }


def _baseline_candidate(sel_status: str) -> dict:
    return {
        "candidate_status":         CANDIDATE_BASELINE,
        "candidate_mode":           MODE_CAND_BASELINE,
        "candidate_market_count":   0,
        "candidate_total_capital":  0.0,
        "candidate_reason":         f"selection is {sel_status} — no candidate built",
        "candidate_reason_code":    "CANDIDATE_BASELINE_HOLD",
        "selected_candidate":       {},
        "candidate_non_binding":    True,
        "candidate_simulation_only": True,
    }


def _rejected_transition(reason: str) -> dict:
    return {
        "transition_status":            TRANSITION_REJECTED,
        "transition_mode":              MODE_TRANS_REJECTED,
        "transition_steps":             [],
        "transition_summary": {
            "total_increase_eur": 0.0,
            "total_decrease_eur": 0.0,
            "net_change_eur":     0.0,
        },
        "estimated_reallocation_count": 0,
        "estimated_hold_count":         0,
        "transition_reason":            reason,
        "transition_reason_code":       "TRANSITION_INVALID_INPUT",
        "transition_non_binding":       True,
        "transition_simulation_only":   True,
    }


def _baseline_transition(cand_status: str) -> dict:
    return {
        "transition_status":            TRANSITION_BASELINE,
        "transition_mode":              MODE_TRANS_BASELINE,
        "transition_steps":             [],
        "transition_summary": {
            "total_increase_eur": 0.0,
            "total_decrease_eur": 0.0,
            "net_change_eur":     0.0,
        },
        "estimated_reallocation_count": 0,
        "estimated_hold_count":         0,
        "transition_reason":            f"candidate is {cand_status} — paper transition not generated",
        "transition_reason_code":       "TRANSITION_BASELINE_HOLD",
        "transition_non_binding":       True,
        "transition_simulation_only":   True,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_resolver_module():
    path = Path(__file__).parent / "build_proposal_conflict_resolver_lite.py"
    spec = importlib.util.spec_from_file_location("_resolver", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-87 + AC-88
# ---------------------------------------------------------------------------

def build_transition_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_regimes: object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → envelope (AC-84) → regime overlay (AC-85)
                             → proposal (AC-86) → conflict selection (AC-87)
                             → candidate + transition preview (AC-88).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview.
    All outputs are non-binding and simulation-only.
    """
    _resolver_mod = _load_resolver_module()
    pipeline = _resolver_mod.build_selection_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    cat = build_candidate_and_transition(
        pipeline["conflict_selection"],
        pipeline["allocation_envelope"],
    )
    return {
        "splits_result":           pipeline["splits_result"],
        "capital_allocation":      pipeline["capital_allocation"],
        "allocation_envelope":     pipeline["allocation_envelope"],
        "regime_overlay":          pipeline["regime_overlay"],
        "allocation_proposal":     pipeline["allocation_proposal"],
        "conflict_selection":      pipeline["conflict_selection"],
        "allocation_candidate":    cat["allocation_candidate"],
        "paper_transition_preview": cat["paper_transition_preview"],
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
            ],
        },
        {
            "market": "SOL-EUR",
            "strategies": [
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT"},
            ],
        },
    ]
    regimes = {
        "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "SOL-EUR": {"trend_regime": "BULL", "vol_regime": "HIGH", "gate": "ALLOW", "size_mult": 1.0},
    }
    result = build_transition_from_specs(specs, total_equity_eur=12_000.0, market_regimes=regimes)
    cand   = result["allocation_candidate"]
    trans  = result["paper_transition_preview"]

    print(json.dumps({
        "allocation_candidate": {
            "candidate_status":        cand["candidate_status"],
            "candidate_market_count":  cand["candidate_market_count"],
            "candidate_total_capital": cand["candidate_total_capital"],
            "candidate_reason_code":   cand["candidate_reason_code"],
            "candidate_non_binding":   cand["candidate_non_binding"],
            "selected_candidate": {
                m: {
                    "selected_direction":   e["selected_direction"],
                    "proposed_capital_eur": e["proposed_capital_eur"],
                }
                for m, e in cand["selected_candidate"].items()
            },
        },
        "paper_transition_preview": {
            "transition_status":            trans["transition_status"],
            "estimated_reallocation_count": trans["estimated_reallocation_count"],
            "estimated_hold_count":         trans["estimated_hold_count"],
            "transition_reason_code":       trans["transition_reason_code"],
            "transition_non_binding":       trans["transition_non_binding"],
            "transition_summary":           trans["transition_summary"],
            "transition_steps": [
                {
                    "market":               s["market"],
                    "current_capital_eur":  s["current_capital_eur"],
                    "selected_capital_eur": s["selected_capital_eur"],
                    "delta_eur":            s["delta_eur"],
                    "transition_direction": s["transition_direction"],
                }
                for s in trans["transition_steps"]
            ],
        },
    }, indent=2))


if __name__ == "__main__":
    main()
