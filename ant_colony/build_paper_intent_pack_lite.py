"""
AC-89: Paper Intent Pack + Transition Audit Summary

Two layers on top of AC-88 paper transition preview:

1. Paper Intent Pack
   Translates each AC-88 transition step into a formal paper intent.
   Produces a flat, auditable list of intents — one per market.

2. Transition Audit Summary
   Compact, deterministic audit of the intent pack output.
   Summarises totals, counts, and provides a machine-stable audit record.

Design principles:
  - intent_pack_non_binding=True, intent_pack_simulation_only=True, paper_only=True always.
  - audit_non_binding=True, audit_simulation_only=True always.
  - Fail-closed: invalid transition → PACK_REJECTED / AUDIT_REJECTED.
  - Deterministic: same transition → same pack and audit every call.
  - Pure core functions — no I/O, no side effects.
  - No broker coupling; no live execution path; no portfolio mutation.

Intent action values:
  PAPER_INCREASE_INTENT  — transition direction is INCREASE
  PAPER_DECREASE_INTENT  — transition direction is DECREASE
  PAPER_HOLD_INTENT      — transition direction is HOLD
  PAPER_BLOCKED_INTENT   — invalid/missing step (fail-closed)

Intent status values:
  ALLOWED  — intent is valid and passes all checks
  BLOCKED  — intent blocked due to invalid input

Pack status values:
  PACK_ACTIVE    — transition is TRANSITION_ACTIVE; ≥0 intents produced
  PACK_BASELINE  — transition is TRANSITION_BASELINE
  PACK_REJECTED  — invalid/missing input

Audit status values:
  AUDIT_COMPLETE — intent pack is PACK_ACTIVE; audit summary computed
  AUDIT_BASELINE — intent pack is PACK_BASELINE
  AUDIT_REJECTED — invalid/missing input

Usage (importable):
    from build_paper_intent_pack_lite import build_paper_intent_pack
    pack = build_paper_intent_pack(paper_transition_preview)

    from build_paper_intent_pack_lite import build_transition_audit_summary
    audit = build_transition_audit_summary(intent_pack)

    # Combined:
    from build_paper_intent_pack_lite import build_intent_pack_and_audit
    result = build_intent_pack_and_audit(paper_transition_preview)

    # Full chain from specs:
    from build_paper_intent_pack_lite import build_intent_pack_from_specs
    result = build_intent_pack_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Intent pack output fields:
    intent_pack_status          — "PACK_ACTIVE"|"PACK_BASELINE"|"PACK_REJECTED"
    intent_pack_mode            — "PAPER_INTENT_PACK"|"PACK_BASELINE"|"PACK_REJECTED"
    intents                     — list of per-market intent entries
    intent_count                — int: total intents
    allowed_count               — int: intents with status ALLOWED
    blocked_count               — int: intents with status BLOCKED
    blocked_reasons             — list of distinct block reason strings
    intent_pack_non_binding     — always True
    intent_pack_simulation_only — always True
    paper_only                  — always True

Per-market intent entry fields:
    market                  — str
    asset_class             — str
    intent_action           — str: PAPER_INCREASE_INTENT / _DECREASE / _HOLD / _BLOCKED
    intent_status           — "ALLOWED" | "BLOCKED"
    current_capital_eur     — float
    selected_capital_eur    — float
    delta_eur               — float
    transition_direction    — str: from AC-88
    block_reason            — str: empty if ALLOWED, reason code if BLOCKED
    paper_only              — always True

Audit output fields:
    audit_status            — "AUDIT_COMPLETE"|"AUDIT_BASELINE"|"AUDIT_REJECTED"
    audit_reason            — human-readable
    audit_reason_code       — machine-stable
    total_markets_reviewed  — int: total intents (allowed + blocked)
    total_increase_eur      — float: sum of deltas for INCREASE intents
    total_decrease_eur      — float: sum of abs(delta) for DECREASE intents
    total_hold_count        — int: markets with HOLD intent
    net_change_eur          — float: total_increase − total_decrease
    audit_non_binding       — always True
    audit_simulation_only   — always True
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "paper_intent_pack_v1"

# Pack status
PACK_ACTIVE   = "PACK_ACTIVE"
PACK_BASELINE = "PACK_BASELINE"
PACK_REJECTED = "PACK_REJECTED"

# Pack mode
MODE_PACK_ACTIVE   = "PAPER_INTENT_PACK"
MODE_PACK_BASELINE = "PACK_BASELINE"
MODE_PACK_REJECTED = "PACK_REJECTED"

# Audit status
AUDIT_COMPLETE = "AUDIT_COMPLETE"
AUDIT_BASELINE = "AUDIT_BASELINE"
AUDIT_REJECTED = "AUDIT_REJECTED"

# Intent action values
ACTION_INCREASE = "PAPER_INCREASE_INTENT"
ACTION_DECREASE = "PAPER_DECREASE_INTENT"
ACTION_HOLD     = "PAPER_HOLD_INTENT"
ACTION_BLOCKED  = "PAPER_BLOCKED_INTENT"

# Intent status values
INTENT_ALLOWED = "ALLOWED"
INTENT_BLOCKED = "BLOCKED"

# Block reason codes
BLOCK_INVALID_STEP = "INVALID_STEP"
BLOCK_MISSING_MKTFIELD = "MISSING_MARKET_FIELD"

# Transition direction values (mirrored from AC-88)
_DIR_INCREASE = "INCREASE"
_DIR_DECREASE = "DECREASE"
_DIR_HOLD     = "HOLD"

# Transition status values (mirrored from AC-88)
_TRANS_ACTIVE   = "TRANSITION_ACTIVE"
_TRANS_BASELINE = "TRANSITION_BASELINE"
_TRANS_REJECTED = "TRANSITION_REJECTED"

_REQUIRED_STEP_FIELDS = {
    "market", "current_capital_eur", "selected_capital_eur",
    "delta_eur", "transition_direction",
}

_DIRECTION_TO_ACTION = {
    _DIR_INCREASE: ACTION_INCREASE,
    _DIR_DECREASE: ACTION_DECREASE,
    _DIR_HOLD:     ACTION_HOLD,
}

_TOL = 1e-9


# ---------------------------------------------------------------------------
# Core intent pack function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_paper_intent_pack(paper_transition_preview: object) -> dict:
    """
    Translate an AC-88 paper transition preview into a formal paper intent pack.

    intent_pack_non_binding=True, intent_pack_simulation_only=True,
    paper_only=True always. No broker coupling, no live execution.

    Args:
        paper_transition_preview: dict from build_paper_transition_preview() (AC-88).

    Returns:
        Paper intent pack dict.
    """
    if not isinstance(paper_transition_preview, dict):
        return _rejected_pack("paper_transition_preview is not a dict")

    if "transition_status" not in paper_transition_preview:
        return _rejected_pack("paper_transition_preview missing transition_status")

    trans_status = paper_transition_preview.get("transition_status", "")

    if trans_status in (_TRANS_BASELINE, _TRANS_REJECTED, ""):
        return _baseline_pack(trans_status)

    if trans_status != _TRANS_ACTIVE:
        return _baseline_pack(trans_status)

    steps = paper_transition_preview.get("transition_steps") or []

    intents:         list = []
    blocked_reasons: set  = set()

    for step in steps:
        intent = _build_intent_from_step(step)
        intents.append(intent)
        if intent["intent_status"] == INTENT_BLOCKED:
            blocked_reasons.add(intent["block_reason"])

    allowed_count = sum(1 for i in intents if i["intent_status"] == INTENT_ALLOWED)
    blocked_count = sum(1 for i in intents if i["intent_status"] == INTENT_BLOCKED)

    return {
        "intent_pack_status":          PACK_ACTIVE,
        "intent_pack_mode":            MODE_PACK_ACTIVE,
        "intents":                     intents,
        "intent_count":                len(intents),
        "allowed_count":               allowed_count,
        "blocked_count":               blocked_count,
        "blocked_reasons":             sorted(blocked_reasons),
        "intent_pack_non_binding":     True,
        "intent_pack_simulation_only": True,
        "paper_only":                  True,
    }


# ---------------------------------------------------------------------------
# Per-step intent builder
# ---------------------------------------------------------------------------

def _build_intent_from_step(step: object) -> dict:
    """Build one paper intent from a single AC-88 transition step."""
    if not isinstance(step, dict):
        return _blocked_intent(
            market="UNKNOWN", asset_class="UNKNOWN",
            current=0.0, selected=0.0, delta=0.0,
            direction="UNKNOWN", reason=BLOCK_INVALID_STEP,
        )

    missing = _REQUIRED_STEP_FIELDS - set(step.keys())
    if missing:
        return _blocked_intent(
            market=str(step.get("market", "UNKNOWN")),
            asset_class=str(step.get("asset_class", "UNKNOWN")),
            current=0.0, selected=0.0, delta=0.0,
            direction="UNKNOWN",
            reason=BLOCK_MISSING_MKTFIELD,
        )

    market    = str(step.get("market")    or "UNKNOWN")
    ac        = str(step.get("asset_class") or "crypto")
    current   = _safe_float(step.get("current_capital_eur"),  0.0)
    selected  = _safe_float(step.get("selected_capital_eur"), 0.0)
    delta     = _safe_float(step.get("delta_eur"),            0.0)
    direction = str(step.get("transition_direction") or "")

    action = _DIRECTION_TO_ACTION.get(direction)
    if action is None:
        return _blocked_intent(market, ac, current, selected, delta, direction, BLOCK_INVALID_STEP)

    return {
        "market":               market,
        "asset_class":          ac,
        "intent_action":        action,
        "intent_status":        INTENT_ALLOWED,
        "current_capital_eur":  round(current,  4),
        "selected_capital_eur": round(selected, 4),
        "delta_eur":            round(delta,    4),
        "transition_direction": direction,
        "block_reason":         "",
        "paper_only":           True,
    }


def _blocked_intent(
    market: str, asset_class: str,
    current: float, selected: float, delta: float,
    direction: str, reason: str,
) -> dict:
    return {
        "market":               market,
        "asset_class":          asset_class,
        "intent_action":        ACTION_BLOCKED,
        "intent_status":        INTENT_BLOCKED,
        "current_capital_eur":  round(current,  4),
        "selected_capital_eur": round(selected, 4),
        "delta_eur":            round(delta,    4),
        "transition_direction": direction,
        "block_reason":         reason,
        "paper_only":           True,
    }


# ---------------------------------------------------------------------------
# Core audit function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_transition_audit_summary(intent_pack: object) -> dict:
    """
    Build a compact, deterministic audit summary from an AC-89 intent pack.

    audit_non_binding=True and audit_simulation_only=True always.

    Args:
        intent_pack: dict returned by build_paper_intent_pack() (AC-89).

    Returns:
        Transition audit summary dict.
    """
    if not isinstance(intent_pack, dict):
        return _rejected_audit("intent_pack is not a dict")

    if "intent_pack_status" not in intent_pack:
        return _rejected_audit("intent_pack missing intent_pack_status")

    pack_status = intent_pack.get("intent_pack_status", "")

    if pack_status in (PACK_BASELINE, PACK_REJECTED, ""):
        return _baseline_audit(pack_status)

    if pack_status != PACK_ACTIVE:
        return _baseline_audit(pack_status)

    intents = intent_pack.get("intents") or []

    total_increase = 0.0
    total_decrease = 0.0
    hold_count     = 0
    reviewed       = 0

    for intent in intents:
        if not isinstance(intent, dict):
            continue
        reviewed += 1
        action = str(intent.get("intent_action") or "")
        delta  = _safe_float(intent.get("delta_eur"), 0.0)

        if action == ACTION_INCREASE:
            total_increase = round(total_increase + max(delta, 0.0), 4)
        elif action == ACTION_DECREASE:
            total_decrease = round(total_decrease + abs(delta), 4)
        elif action == ACTION_HOLD:
            hold_count += 1

    net_change = round(total_increase - total_decrease, 4)

    allowed_count = intent_pack.get("allowed_count", 0)
    blocked_count = intent_pack.get("blocked_count", 0)

    if reviewed == 0:
        audit_reason      = "no intents to audit"
        audit_reason_code = "AUDIT_EMPTY"
    elif blocked_count > 0:
        audit_reason = (
            f"audit complete: {reviewed} market(s) reviewed, "
            f"{blocked_count} blocked intent(s), "
            f"net change {net_change:+.2f} EUR"
        )
        audit_reason_code = "AUDIT_WITH_BLOCKED"
    else:
        audit_reason = (
            f"audit complete: {reviewed} market(s) reviewed, "
            f"all allowed, net change {net_change:+.2f} EUR"
        )
        audit_reason_code = "AUDIT_ALL_ALLOWED"

    return {
        "audit_status":           AUDIT_COMPLETE,
        "audit_reason":           audit_reason,
        "audit_reason_code":      audit_reason_code,
        "total_markets_reviewed": reviewed,
        "total_increase_eur":     total_increase,
        "total_decrease_eur":     total_decrease,
        "total_hold_count":       hold_count,
        "net_change_eur":         net_change,
        "audit_non_binding":      True,
        "audit_simulation_only":  True,
    }


# ---------------------------------------------------------------------------
# Combined builder
# ---------------------------------------------------------------------------

def build_intent_pack_and_audit(paper_transition_preview: object) -> dict:
    """
    Build both the paper intent pack and the transition audit summary in one call.

    Returns dict with keys: intent_pack, transition_audit.
    """
    pack  = build_paper_intent_pack(paper_transition_preview)
    audit = build_transition_audit_summary(pack)
    return {
        "intent_pack":       pack,
        "transition_audit":  audit,
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_pack(reason: str) -> dict:
    return {
        "intent_pack_status":          PACK_REJECTED,
        "intent_pack_mode":            MODE_PACK_REJECTED,
        "intents":                     [],
        "intent_count":                0,
        "allowed_count":               0,
        "blocked_count":               0,
        "blocked_reasons":             [reason],
        "intent_pack_non_binding":     True,
        "intent_pack_simulation_only": True,
        "paper_only":                  True,
    }


def _baseline_pack(trans_status: str) -> dict:
    return {
        "intent_pack_status":          PACK_BASELINE,
        "intent_pack_mode":            MODE_PACK_BASELINE,
        "intents":                     [],
        "intent_count":                0,
        "allowed_count":               0,
        "blocked_count":               0,
        "blocked_reasons":             [],
        "intent_pack_non_binding":     True,
        "intent_pack_simulation_only": True,
        "paper_only":                  True,
    }


def _rejected_audit(reason: str) -> dict:
    return {
        "audit_status":           AUDIT_REJECTED,
        "audit_reason":           reason,
        "audit_reason_code":      "AUDIT_INVALID_INPUT",
        "total_markets_reviewed": 0,
        "total_increase_eur":     0.0,
        "total_decrease_eur":     0.0,
        "total_hold_count":       0,
        "net_change_eur":         0.0,
        "audit_non_binding":      True,
        "audit_simulation_only":  True,
    }


def _baseline_audit(pack_status: str) -> dict:
    return {
        "audit_status":           AUDIT_BASELINE,
        "audit_reason":           f"intent pack is {pack_status} — audit not performed",
        "audit_reason_code":      "AUDIT_BASELINE_HOLD",
        "total_markets_reviewed": 0,
        "total_increase_eur":     0.0,
        "total_decrease_eur":     0.0,
        "total_hold_count":       0,
        "net_change_eur":         0.0,
        "audit_non_binding":      True,
        "audit_simulation_only":  True,
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

def _load_candidate_module():
    path = Path(__file__).parent / "build_allocation_candidate_lite.py"
    spec = importlib.util.spec_from_file_location("_candidate", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-88 + AC-89
# ---------------------------------------------------------------------------

def build_intent_pack_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_regimes: object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → envelope (AC-84) → regime overlay (AC-85)
                             → proposal (AC-86) → conflict selection (AC-87)
                             → candidate + transition (AC-88)
                             → intent pack + audit (AC-89).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit.
    All outputs are paper-only, non-binding, simulation-only.
    """
    _cand_mod = _load_candidate_module()
    pipeline  = _cand_mod.build_transition_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    ia = build_intent_pack_and_audit(pipeline["paper_transition_preview"])
    return {
        "splits_result":            pipeline["splits_result"],
        "capital_allocation":       pipeline["capital_allocation"],
        "allocation_envelope":      pipeline["allocation_envelope"],
        "regime_overlay":           pipeline["regime_overlay"],
        "allocation_proposal":      pipeline["allocation_proposal"],
        "conflict_selection":       pipeline["conflict_selection"],
        "allocation_candidate":     pipeline["allocation_candidate"],
        "paper_transition_preview": pipeline["paper_transition_preview"],
        "intent_pack":              ia["intent_pack"],
        "transition_audit":         ia["transition_audit"],
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
    ]
    regimes = {
        "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
    }
    result = build_intent_pack_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    pack  = result["intent_pack"]
    audit = result["transition_audit"]

    print(json.dumps({
        "intent_pack": {
            "intent_pack_status":          pack["intent_pack_status"],
            "intent_count":                pack["intent_count"],
            "allowed_count":               pack["allowed_count"],
            "blocked_count":               pack["blocked_count"],
            "paper_only":                  pack["paper_only"],
            "intent_pack_non_binding":     pack["intent_pack_non_binding"],
            "intent_pack_simulation_only": pack["intent_pack_simulation_only"],
            "intents": [
                {
                    "market":               i["market"],
                    "intent_action":        i["intent_action"],
                    "intent_status":        i["intent_status"],
                    "delta_eur":            i["delta_eur"],
                    "transition_direction": i["transition_direction"],
                }
                for i in pack["intents"]
            ],
        },
        "transition_audit": {
            "audit_status":           audit["audit_status"],
            "audit_reason_code":      audit["audit_reason_code"],
            "total_markets_reviewed": audit["total_markets_reviewed"],
            "total_increase_eur":     audit["total_increase_eur"],
            "total_decrease_eur":     audit["total_decrease_eur"],
            "total_hold_count":       audit["total_hold_count"],
            "net_change_eur":         audit["net_change_eur"],
            "audit_non_binding":      audit["audit_non_binding"],
            "audit_simulation_only":  audit["audit_simulation_only"],
        },
    }, indent=2))


if __name__ == "__main__":
    main()
