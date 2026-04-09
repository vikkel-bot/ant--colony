"""
AC-87: Proposal Conflict Resolver + Safe Selection

Detects conflicts in the AC-86 allocation proposal and makes one safe,
explainable selection per market — purely observational. No live activation,
no portfolio mutation, no execution-impact.

Design principles:
  - selection_non_binding=True and selection_simulation_only=True always.
  - Fail-closed: invalid proposal → SELECTION_REJECTED.
  - Deterministic: same proposal → same selection every call.
  - Pure core function (resolve_proposal_conflicts) — no I/O, no side effects.
  - Prefer caution: conflicting markets are downgraded to HOLD, not removed.
  - Three conflict types detected (see below); independent, composable.

Conflict types:
  OPPOSITE_DIRECTIONS_IN_ASSET_CLASS
    — At least one UPWEIGHT and one DOWNWEIGHT market within the same asset
      class. Signals regime disagreement within the class.
      Resolution: downgrade all UPWEIGHT markets in that class to HOLD.
      (DOWNWEIGHT stays — caution preferred.)

  MARKET_AC_DIRECTION_MISMATCH
    — A market's proposed direction contradicts its asset-class direction.
      e.g. market=UPWEIGHT but asset class=DOWNWEIGHT.
      Resolution: downgrade the conflicting market to HOLD.

  INVALID_PROPOSAL_FIELDS
    — A market proposal entry is missing required fields or has invalid
      (non-string direction, non-numeric capital, etc.).
      Resolution: add to rejected_proposals; skip from selected_proposals.

Selection status values:
  SELECTION_ACTIVE    — proposal is PROPOSAL_ACTIVE; selection produced
  SELECTION_BASELINE  — proposal is PROPOSAL_BASELINE or inactive
  SELECTION_REJECTED  — invalid/missing input

Selection mode values:
  CLEAN_SELECTION     — no conflicts detected; proposals passed through as-is
  CONFLICT_RESOLVED   — ≥1 conflict detected and resolved
  SELECTION_BASELINE  — proposal was baseline
  SELECTION_REJECTED  — invalid input

Usage (importable):
    from build_proposal_conflict_resolver_lite import resolve_proposal_conflicts
    selection = resolve_proposal_conflicts(allocation_proposal)

    # Full chain from specs:
    from build_proposal_conflict_resolver_lite import build_selection_from_specs
    result = build_selection_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Output fields:
    selection_status        — "SELECTION_ACTIVE"|"SELECTION_BASELINE"|"SELECTION_REJECTED"
    selection_mode          — "CLEAN_SELECTION"|"CONFLICT_RESOLVED"|...
    conflict_count          — int: total conflicts detected
    conflicts               — list of conflict entries
    selected_proposals      — {market: selected proposal entry}
    rejected_proposals      — {market: rejection entry}
    selection_reason        — human-readable
    selection_reason_code   — machine-stable
    selection_non_binding   — always True
    selection_simulation_only — always True

Conflict entry fields:
    conflict_type   — str: one of the three types above
    asset_class     — str: relevant asset class
    markets         — list[str]: markets involved
    description     — str: human-readable explanation

Selected proposal entry fields (per market):
    market                  — str
    asset_class             — str
    original_direction      — str: from AC-86 proposal
    selected_direction      — str: after conflict resolution
    proposed_capital_eur    — float: from AC-86 (unchanged, observational)
    proposed_delta_eur      — float: from AC-86
    conflict_adjusted       — bool: True if direction was downgraded
    selection_reason_code   — str: machine-stable

Rejection entry fields:
    market          — str
    rejection_reason — str
    rejection_code  — str
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "proposal_conflict_resolver_v1"

# Selection status
SELECTION_ACTIVE   = "SELECTION_ACTIVE"
SELECTION_BASELINE = "SELECTION_BASELINE"
SELECTION_REJECTED = "SELECTION_REJECTED"

# Selection mode
MODE_CLEAN          = "CLEAN_SELECTION"
MODE_RESOLVED       = "CONFLICT_RESOLVED"
MODE_SEL_BASELINE   = "SELECTION_BASELINE"
MODE_SEL_REJECTED   = "SELECTION_REJECTED"

# Conflict types (machine-stable)
CONFLICT_OPPOSITE_DIRS   = "OPPOSITE_DIRECTIONS_IN_ASSET_CLASS"
CONFLICT_AC_MISMATCH     = "MARKET_AC_DIRECTION_MISMATCH"
CONFLICT_INVALID_FIELDS  = "INVALID_PROPOSAL_FIELDS"

# Direction values (mirrored from AC-86)
DIR_UPWEIGHT   = "UPWEIGHT"
DIR_DOWNWEIGHT = "DOWNWEIGHT"
DIR_HOLD       = "HOLD"

# Selection reason codes
SEL_CLEAN               = "SELECTION_CLEAN"
SEL_CONFLICT_RESOLVED   = "SELECTION_CONFLICT_RESOLVED"
SEL_ALL_HELD            = "SELECTION_ALL_HELD"
SEL_EMPTY               = "SELECTION_EMPTY"
ADJ_OPPOSITE_DOWNGRADE  = "CONFLICT_DOWNGRADED_OPPOSITE"
ADJ_MISMATCH_DOWNGRADE  = "CONFLICT_DOWNGRADED_MISMATCH"
ADJ_PASS_THROUGH        = "SELECTED_PASS_THROUGH"
ADJ_REJECTED_INVALID    = "REJECTED_INVALID_FIELDS"

# Proposal status (mirrored from AC-86)
_PROP_ACTIVE   = "PROPOSAL_ACTIVE"
_PROP_BASELINE = "PROPOSAL_BASELINE"
_PROP_REJECTED = "PROPOSAL_REJECTED"

_REQUIRED_MARKET_FIELDS = {
    "market", "asset_class", "proposed_direction",
    "proposed_capital_eur", "proposed_delta_eur",
}
_VALID_DIRECTIONS = {DIR_UPWEIGHT, DIR_DOWNWEIGHT, DIR_HOLD}

_TOL = 1e-9


# ---------------------------------------------------------------------------
# Core resolver function (pure, no I/O)
# ---------------------------------------------------------------------------

def resolve_proposal_conflicts(allocation_proposal: object) -> dict:
    """
    Detect conflicts in an AC-86 allocation proposal and produce a safe
    per-market selection.

    selection_non_binding=True and selection_simulation_only=True always.
    No execution, no cash movement, no state mutation.

    Args:
        allocation_proposal: dict returned by build_allocation_proposal() (AC-86).

    Returns:
        Conflict-resolved selection dict.
    """
    if not isinstance(allocation_proposal, dict):
        return _rejected_selection("allocation_proposal is not a dict")

    if "proposal_status" not in allocation_proposal:
        return _rejected_selection("allocation_proposal missing proposal_status")

    prop_status = allocation_proposal.get("proposal_status", "")

    if prop_status in (_PROP_BASELINE, _PROP_REJECTED, ""):
        return _baseline_selection(prop_status)

    if prop_status != _PROP_ACTIVE:
        return _baseline_selection(prop_status)

    market_proposals    = allocation_proposal.get("market_proposals") or {}
    ac_proposals        = allocation_proposal.get("asset_class_proposals") or {}

    # ------------------------------------------------------------------
    # Pass 1: validate all market proposal entries
    # ------------------------------------------------------------------
    valid_markets:    dict = {}   # market → proposal entry (after field validation)
    rejected_markets: dict = {}   # market → rejection entry

    for market, mp in market_proposals.items():
        if not isinstance(mp, dict):
            rejected_markets[market] = _rejection(
                market, "proposal entry is not a dict", ADJ_REJECTED_INVALID
            )
            continue

        missing = _REQUIRED_MARKET_FIELDS - set(mp.keys())
        if missing:
            rejected_markets[market] = _rejection(
                market,
                f"missing required fields: {', '.join(sorted(missing))}",
                ADJ_REJECTED_INVALID,
            )
            continue

        direction = mp.get("proposed_direction", "")
        if direction not in _VALID_DIRECTIONS:
            rejected_markets[market] = _rejection(
                market,
                f"invalid proposed_direction: {direction!r}",
                ADJ_REJECTED_INVALID,
            )
            continue

        cap = mp.get("proposed_capital_eur")
        if not _is_numeric(cap):
            rejected_markets[market] = _rejection(
                market,
                f"invalid proposed_capital_eur: {cap!r}",
                ADJ_REJECTED_INVALID,
            )
            continue

        valid_markets[market] = mp

    # Track INVALID_PROPOSAL_FIELDS conflict if any were rejected
    invalid_conflict = None
    if rejected_markets:
        invalid_conflict = {
            "conflict_type": CONFLICT_INVALID_FIELDS,
            "asset_class":   "",
            "markets":       sorted(rejected_markets.keys()),
            "description":   (
                f"{len(rejected_markets)} market proposal(s) had invalid or "
                f"missing fields and were rejected"
            ),
        }

    # ------------------------------------------------------------------
    # Pass 2: group valid markets by asset class and detect conflicts
    # ------------------------------------------------------------------
    by_ac: dict = {}  # asset_class → {directions: set, markets_by_dir: {dir: [market]}}
    for market, mp in valid_markets.items():
        ac = str(mp.get("asset_class") or "crypto")
        if ac not in by_ac:
            by_ac[ac] = {"directions": set(), "markets_by_dir": {}}
        direction = mp["proposed_direction"]
        by_ac[ac]["directions"].add(direction)
        by_ac[ac]["markets_by_dir"].setdefault(direction, []).append(market)

    # Detect OPPOSITE_DIRECTIONS_IN_ASSET_CLASS
    opposite_conflict_acs: set = set()
    opposite_conflicts: list   = []
    for ac, data in by_ac.items():
        dirs = data["directions"]
        if DIR_UPWEIGHT in dirs and DIR_DOWNWEIGHT in dirs:
            opposite_conflict_acs.add(ac)
            all_involved = (
                data["markets_by_dir"].get(DIR_UPWEIGHT, []) +
                data["markets_by_dir"].get(DIR_DOWNWEIGHT, [])
            )
            opposite_conflicts.append({
                "conflict_type": CONFLICT_OPPOSITE_DIRS,
                "asset_class":   ac,
                "markets":       sorted(all_involved),
                "description":   (
                    f"asset class '{ac}' has both UPWEIGHT and DOWNWEIGHT proposals: "
                    f"UPWEIGHT={sorted(data['markets_by_dir'].get(DIR_UPWEIGHT,[]))}, "
                    f"DOWNWEIGHT={sorted(data['markets_by_dir'].get(DIR_DOWNWEIGHT,[]))}"
                ),
            })

    # Detect MARKET_AC_DIRECTION_MISMATCH
    mismatch_conflicts: list  = []
    mismatch_markets:   set   = set()
    for market, mp in valid_markets.items():
        ac = str(mp.get("asset_class") or "crypto")
        market_dir = mp["proposed_direction"]
        ac_entry   = ac_proposals.get(ac) or {}
        ac_dir     = str(ac_entry.get("proposed_direction") or DIR_HOLD)

        # HOLD at asset class level never conflicts with any market direction
        if ac_dir == DIR_HOLD:
            continue

        # Mismatch: market and asset class directions are opposite
        if _directions_conflict(market_dir, ac_dir):
            mismatch_markets.add(market)
            mismatch_conflicts.append({
                "conflict_type": CONFLICT_AC_MISMATCH,
                "asset_class":   ac,
                "markets":       [market],
                "description":   (
                    f"market '{market}' direction {market_dir!r} conflicts with "
                    f"asset class '{ac}' direction {ac_dir!r}"
                ),
            })

    # ------------------------------------------------------------------
    # Pass 3: build selected_proposals with conflict resolution
    # ------------------------------------------------------------------
    all_conflicts = opposite_conflicts + mismatch_conflicts
    if invalid_conflict:
        all_conflicts = [invalid_conflict] + all_conflicts

    selected: dict = {}

    for market, mp in valid_markets.items():
        ac            = str(mp.get("asset_class") or "crypto")
        orig_dir      = mp["proposed_direction"]
        selected_dir  = orig_dir
        adjusted      = False
        sel_code      = ADJ_PASS_THROUGH

        # Rule 1: opposite directions in asset class → downgrade UPWEIGHT to HOLD
        if ac in opposite_conflict_acs and orig_dir == DIR_UPWEIGHT:
            selected_dir = DIR_HOLD
            adjusted     = True
            sel_code     = ADJ_OPPOSITE_DOWNGRADE

        # Rule 2: market/AC mismatch → downgrade to HOLD
        # Apply only if not already adjusted by Rule 1
        if not adjusted and market in mismatch_markets:
            selected_dir = DIR_HOLD
            adjusted     = True
            sel_code     = ADJ_MISMATCH_DOWNGRADE

        selected[market] = {
            "market":               market,
            "asset_class":          ac,
            "original_direction":   orig_dir,
            "selected_direction":   selected_dir,
            "proposed_capital_eur": float(mp.get("proposed_capital_eur", 0.0)),
            "proposed_delta_eur":   float(mp.get("proposed_delta_eur", 0.0)),
            "conflict_adjusted":    adjusted,
            "selection_reason_code": sel_code,
        }

    # ------------------------------------------------------------------
    # Derive overall selection reason + mode
    # ------------------------------------------------------------------
    n_conflicts = len(all_conflicts)
    n_selected  = len(selected)

    if n_selected == 0:
        sel_reason      = "no valid market proposals to select from"
        sel_reason_code = SEL_EMPTY
    elif n_conflicts == 0:
        sel_reason      = f"clean selection: {n_selected} market proposal(s) accepted without conflicts"
        sel_reason_code = SEL_CLEAN
    else:
        adjusted_count = sum(1 for s in selected.values() if s["conflict_adjusted"])
        sel_reason = (
            f"{n_conflicts} conflict(s) detected; "
            f"{adjusted_count} market(s) downgraded to HOLD; "
            f"{n_selected} market(s) in final selection"
        )
        sel_reason_code = SEL_CONFLICT_RESOLVED

    mode = MODE_CLEAN if n_conflicts == 0 else MODE_RESOLVED

    return {
        "selection_status":         SELECTION_ACTIVE,
        "selection_mode":           mode,
        "conflict_count":           n_conflicts,
        "conflicts":                all_conflicts,
        "selected_proposals":       selected,
        "rejected_proposals":       rejected_markets,
        "selection_reason":         sel_reason,
        "selection_reason_code":    sel_reason_code,
        "selection_non_binding":    True,
        "selection_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _directions_conflict(dir_a: str, dir_b: str) -> bool:
    """True when directions are opposite (UPWEIGHT vs DOWNWEIGHT)."""
    return (
        (dir_a == DIR_UPWEIGHT   and dir_b == DIR_DOWNWEIGHT) or
        (dir_a == DIR_DOWNWEIGHT and dir_b == DIR_UPWEIGHT)
    )


def _is_numeric(value: object) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _rejection(market: str, reason: str, code: str) -> dict:
    return {"market": market, "rejection_reason": reason, "rejection_code": code}


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_selection(reason: str) -> dict:
    return {
        "selection_status":          SELECTION_REJECTED,
        "selection_mode":            MODE_SEL_REJECTED,
        "conflict_count":            0,
        "conflicts":                 [],
        "selected_proposals":        {},
        "rejected_proposals":        {},
        "selection_reason":          reason,
        "selection_reason_code":     "SELECTION_INVALID_INPUT",
        "selection_non_binding":     True,
        "selection_simulation_only": True,
    }


def _baseline_selection(prop_status: str) -> dict:
    return {
        "selection_status":          SELECTION_BASELINE,
        "selection_mode":            MODE_SEL_BASELINE,
        "conflict_count":            0,
        "conflicts":                 [],
        "selected_proposals":        {},
        "rejected_proposals":        {},
        "selection_reason":          f"proposal is {prop_status} — conflict resolution not applied",
        "selection_reason_code":     "SELECTION_BASELINE_HOLD",
        "selection_non_binding":     True,
        "selection_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_proposal_module():
    path = Path(__file__).parent / "build_allocation_proposal_lite.py"
    spec = importlib.util.spec_from_file_location("_proposal", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-86 + AC-87
# ---------------------------------------------------------------------------

def build_selection_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_regimes: object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → envelope (AC-84) → regime overlay (AC-85)
                             → proposal (AC-86) → conflict selection (AC-87).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection.
    selection_non_binding=True and selection_simulation_only=True always.
    """
    _prop_mod = _load_proposal_module()
    pipeline  = _prop_mod.build_proposal_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    selection = resolve_proposal_conflicts(pipeline["allocation_proposal"])
    return {
        "splits_result":       pipeline["splits_result"],
        "capital_allocation":  pipeline["capital_allocation"],
        "allocation_envelope": pipeline["allocation_envelope"],
        "regime_overlay":      pipeline["regime_overlay"],
        "allocation_proposal": pipeline["allocation_proposal"],
        "conflict_selection":  selection,
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
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.5},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.5},
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
    # Deliberately conflicting: BTC-EUR BULL, ETH-EUR BEAR in same asset class
    regimes = {
        "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "SOL-EUR": {"trend_regime": "BULL", "vol_regime": "HIGH", "gate": "ALLOW", "size_mult": 1.0},
    }
    result = build_selection_from_specs(specs, total_equity_eur=12_000.0, market_regimes=regimes)
    sel = result["conflict_selection"]
    print(json.dumps({
        "selection_status":         sel["selection_status"],
        "selection_mode":           sel["selection_mode"],
        "conflict_count":           sel["conflict_count"],
        "selection_reason_code":    sel["selection_reason_code"],
        "selection_non_binding":    sel["selection_non_binding"],
        "selection_simulation_only": sel["selection_simulation_only"],
        "conflicts": [
            {"conflict_type": c["conflict_type"], "markets": c["markets"]}
            for c in sel["conflicts"]
        ],
        "selected_proposals": {
            m: {
                "original_direction":    p["original_direction"],
                "selected_direction":    p["selected_direction"],
                "conflict_adjusted":     p["conflict_adjusted"],
                "selection_reason_code": p["selection_reason_code"],
            }
            for m, p in sel["selected_proposals"].items()
        },
    }, indent=2))


if __name__ == "__main__":
    main()
