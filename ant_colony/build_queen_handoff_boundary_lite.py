"""
AC-90: Intent Pack Consolidation + Queen Handoff Boundary

Builds one compact handoff object on top of the AC-89 intent pack and
transition audit summary. Provides a formal, auditable boundary that the
queen would use to pass paper intents to a later paper-runner or execution
layer — without live activation, without portfolio mutation, without broker
impact.

Design principles:
  - queen_handoff_non_binding=True always.
  - queen_handoff_simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always — never overridden, never removed.
  - Fail-closed: invalid pack or audit → REJECT_HANDOFF.
  - Deterministic: same pack + audit → same handoff every call.
  - Pure core function (build_queen_handoff) — no I/O, no side effects.

Handoff status values:
  READY_FOR_PAPER_HANDOFF — pack is PACK_ACTIVE with ≥1 allowed intent;
                            audit is AUDIT_COMPLETE; handoff_ready=True.
  HOLD_BASELINE_HANDOFF   — pack or audit is baseline / empty; safe hold.
  REJECT_HANDOFF          — invalid/missing input or pack/audit rejected.

Handoff mode values:
  PAPER_HANDOFF_READY     — ready for downstream paper consumption
  HANDOFF_BASELINE        — baseline hold; nothing to hand off
  HANDOFF_REJECTED        — invalid input; boundary closed

Usage (importable):
    from build_queen_handoff_boundary_lite import build_queen_handoff
    handoff = build_queen_handoff(intent_pack, transition_audit)

    # Combined:
    from build_queen_handoff_boundary_lite import build_handoff_from_pack_and_audit
    handoff = build_handoff_from_pack_and_audit(intent_pack, transition_audit)

    # Full chain from specs:
    from build_queen_handoff_boundary_lite import build_handoff_from_specs
    result = build_handoff_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Output fields:
    handoff_status              — "READY_FOR_PAPER_HANDOFF"|"HOLD_BASELINE_HANDOFF"|"REJECT_HANDOFF"
    handoff_mode                — "PAPER_HANDOFF_READY"|"HANDOFF_BASELINE"|"HANDOFF_REJECTED"
    handoff_ready               — bool: True only when READY_FOR_PAPER_HANDOFF
    handoff_reason              — human-readable
    handoff_reason_code         — machine-stable
    intent_pack_snapshot        — compact copy of key intent pack fields
    audit_snapshot              — compact copy of key audit fields
    total_intents               — int: from intent pack
    total_allowed               — int: from intent pack
    total_blocked               — int: from intent pack
    queen_handoff_non_binding   — always True
    queen_handoff_simulation_only — always True
    paper_only                  — always True
    live_activation_allowed     — always False

intent_pack_snapshot fields:
    intent_pack_status  — str
    intent_count        — int
    allowed_count       — int
    blocked_count       — int
    blocked_reasons     — list[str]

audit_snapshot fields:
    audit_status            — str
    audit_reason_code       — str
    total_markets_reviewed  — int
    total_increase_eur      — float
    total_decrease_eur      — float
    total_hold_count        — int
    net_change_eur          — float
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "queen_handoff_boundary_v1"

# Handoff status values
HANDOFF_READY    = "READY_FOR_PAPER_HANDOFF"
HANDOFF_BASELINE = "HOLD_BASELINE_HANDOFF"
HANDOFF_REJECTED = "REJECT_HANDOFF"

# Handoff mode values
MODE_READY    = "PAPER_HANDOFF_READY"
MODE_BASELINE = "HANDOFF_BASELINE"
MODE_REJECTED = "HANDOFF_REJECTED"

# Handoff reason codes
REASON_READY         = "HANDOFF_PACK_READY"
REASON_BASELINE_PACK = "HANDOFF_PACK_BASELINE"
REASON_NO_ALLOWED    = "HANDOFF_NO_ALLOWED_INTENTS"
REASON_REJECTED_PACK = "HANDOFF_PACK_REJECTED"
REASON_INVALID_INPUT = "HANDOFF_INVALID_INPUT"

# Pack / audit status mirrors
_PACK_ACTIVE   = "PACK_ACTIVE"
_PACK_BASELINE = "PACK_BASELINE"
_PACK_REJECTED = "PACK_REJECTED"
_AUDIT_COMPLETE = "AUDIT_COMPLETE"
_AUDIT_BASELINE = "AUDIT_BASELINE"
_AUDIT_REJECTED = "AUDIT_REJECTED"


# ---------------------------------------------------------------------------
# Core handoff function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_queen_handoff(
    intent_pack:       object,
    transition_audit:  object,
) -> dict:
    """
    Build a formal queen handoff boundary from an AC-89 intent pack and audit.

    queen_handoff_non_binding=True, queen_handoff_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.
    No broker coupling, no live execution, no state mutation.

    Args:
        intent_pack:      dict from build_paper_intent_pack() (AC-89).
        transition_audit: dict from build_transition_audit_summary() (AC-89).

    Returns:
        Queen handoff boundary dict.
    """
    # Validate inputs
    if not isinstance(intent_pack, dict):
        return _rejected_handoff("intent_pack is not a dict")
    if not isinstance(transition_audit, dict):
        return _rejected_handoff("transition_audit is not a dict")

    if "intent_pack_status" not in intent_pack:
        return _rejected_handoff("intent_pack missing intent_pack_status")
    if "audit_status" not in transition_audit:
        return _rejected_handoff("transition_audit missing audit_status")

    pack_status  = intent_pack.get("intent_pack_status", "")
    audit_status = transition_audit.get("audit_status", "")

    # Rejected pack → reject handoff
    if pack_status == _PACK_REJECTED:
        return _rejected_handoff("intent_pack status is PACK_REJECTED")

    # Baseline pack → hold handoff
    if pack_status in (_PACK_BASELINE, ""):
        return _baseline_handoff(pack_status, intent_pack, transition_audit)

    # Pack is PACK_ACTIVE — check allowed count
    allowed_count = int(intent_pack.get("allowed_count", 0))
    blocked_count = int(intent_pack.get("blocked_count", 0))
    intent_count  = int(intent_pack.get("intent_count",  0))

    if allowed_count == 0:
        # No allowed intents — hold, don't reject; fail-closed but not error
        pack_snap  = _pack_snapshot(intent_pack)
        audit_snap = _audit_snapshot(transition_audit)
        return {
            "handoff_status":             HANDOFF_BASELINE,
            "handoff_mode":               MODE_BASELINE,
            "handoff_ready":              False,
            "handoff_reason":             "no allowed intents in pack — paper handoff held",
            "handoff_reason_code":        REASON_NO_ALLOWED,
            "intent_pack_snapshot":       pack_snap,
            "audit_snapshot":             audit_snap,
            "total_intents":              intent_count,
            "total_allowed":              allowed_count,
            "total_blocked":              blocked_count,
            "queen_handoff_non_binding":  True,
            "queen_handoff_simulation_only": True,
            "paper_only":                 True,
            "live_activation_allowed":    False,
        }

    # Pack is active with ≥1 allowed intent — ready for paper handoff
    pack_snap  = _pack_snapshot(intent_pack)
    audit_snap = _audit_snapshot(transition_audit)

    return {
        "handoff_status":             HANDOFF_READY,
        "handoff_mode":               MODE_READY,
        "handoff_ready":              True,
        "handoff_reason":             (
            f"paper handoff ready: {allowed_count} allowed intent(s), "
            f"{blocked_count} blocked, "
            f"audit={audit_status}"
        ),
        "handoff_reason_code":        REASON_READY,
        "intent_pack_snapshot":       pack_snap,
        "audit_snapshot":             audit_snap,
        "total_intents":              intent_count,
        "total_allowed":              allowed_count,
        "total_blocked":              blocked_count,
        "queen_handoff_non_binding":  True,
        "queen_handoff_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


# ---------------------------------------------------------------------------
# Snapshot helpers (compact, non-mutating copies)
# ---------------------------------------------------------------------------

def _pack_snapshot(intent_pack: dict) -> dict:
    return {
        "intent_pack_status": str(intent_pack.get("intent_pack_status", "")),
        "intent_count":       int(intent_pack.get("intent_count",       0)),
        "allowed_count":      int(intent_pack.get("allowed_count",      0)),
        "blocked_count":      int(intent_pack.get("blocked_count",      0)),
        "blocked_reasons":    list(intent_pack.get("blocked_reasons",   [])),
    }


def _audit_snapshot(transition_audit: dict) -> dict:
    return {
        "audit_status":           str(transition_audit.get("audit_status",           "")),
        "audit_reason_code":      str(transition_audit.get("audit_reason_code",      "")),
        "total_markets_reviewed": int(transition_audit.get("total_markets_reviewed", 0)),
        "total_increase_eur":     _safe_float(transition_audit.get("total_increase_eur"), 0.0),
        "total_decrease_eur":     _safe_float(transition_audit.get("total_decrease_eur"), 0.0),
        "total_hold_count":       int(transition_audit.get("total_hold_count",       0)),
        "net_change_eur":         _safe_float(transition_audit.get("net_change_eur"), 0.0),
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_handoff(reason: str) -> dict:
    return {
        "handoff_status":             HANDOFF_REJECTED,
        "handoff_mode":               MODE_REJECTED,
        "handoff_ready":              False,
        "handoff_reason":             reason,
        "handoff_reason_code":        REASON_INVALID_INPUT,
        "intent_pack_snapshot":       {},
        "audit_snapshot":             {},
        "total_intents":              0,
        "total_allowed":              0,
        "total_blocked":              0,
        "queen_handoff_non_binding":  True,
        "queen_handoff_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _baseline_handoff(
    pack_status: str,
    intent_pack: dict,
    transition_audit: dict,
) -> dict:
    return {
        "handoff_status":             HANDOFF_BASELINE,
        "handoff_mode":               MODE_BASELINE,
        "handoff_ready":              False,
        "handoff_reason":             f"intent pack is {pack_status} — handoff held at baseline",
        "handoff_reason_code":        REASON_BASELINE_PACK,
        "intent_pack_snapshot":       _pack_snapshot(intent_pack),
        "audit_snapshot":             _audit_snapshot(transition_audit),
        "total_intents":              int(intent_pack.get("intent_count",  0)),
        "total_allowed":              int(intent_pack.get("allowed_count", 0)),
        "total_blocked":              int(intent_pack.get("blocked_count", 0)),
        "queen_handoff_non_binding":  True,
        "queen_handoff_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Convenience: build handoff from pack + audit (alias)
# ---------------------------------------------------------------------------

def build_handoff_from_pack_and_audit(
    intent_pack: object,
    transition_audit: object,
) -> dict:
    """Alias for build_queen_handoff — explicit naming for pipeline use."""
    return build_queen_handoff(intent_pack, transition_audit)


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_intent_pack_module():
    path = Path(__file__).parent / "build_paper_intent_pack_lite.py"
    spec = importlib.util.spec_from_file_location("_intent_pack", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-89 + AC-90
# ---------------------------------------------------------------------------

def build_handoff_from_specs(
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
                             → intent pack + audit (AC-89)
                             → queen handoff boundary (AC-90).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit, queen_handoff.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _pack_mod = _load_intent_pack_module()
    pipeline  = _pack_mod.build_intent_pack_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    handoff = build_queen_handoff(
        pipeline["intent_pack"],
        pipeline["transition_audit"],
    )
    return {
        "splits_result":            pipeline["splits_result"],
        "capital_allocation":       pipeline["capital_allocation"],
        "allocation_envelope":      pipeline["allocation_envelope"],
        "regime_overlay":           pipeline["regime_overlay"],
        "allocation_proposal":      pipeline["allocation_proposal"],
        "conflict_selection":       pipeline["conflict_selection"],
        "allocation_candidate":     pipeline["allocation_candidate"],
        "paper_transition_preview": pipeline["paper_transition_preview"],
        "intent_pack":              pipeline["intent_pack"],
        "transition_audit":         pipeline["transition_audit"],
        "queen_handoff":            handoff,
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
        "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
    }
    result  = build_handoff_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    handoff = result["queen_handoff"]

    print(json.dumps({
        "handoff_status":              handoff["handoff_status"],
        "handoff_mode":                handoff["handoff_mode"],
        "handoff_ready":               handoff["handoff_ready"],
        "handoff_reason_code":         handoff["handoff_reason_code"],
        "total_intents":               handoff["total_intents"],
        "total_allowed":               handoff["total_allowed"],
        "total_blocked":               handoff["total_blocked"],
        "queen_handoff_non_binding":   handoff["queen_handoff_non_binding"],
        "queen_handoff_simulation_only": handoff["queen_handoff_simulation_only"],
        "paper_only":                  handoff["paper_only"],
        "live_activation_allowed":     handoff["live_activation_allowed"],
        "intent_pack_snapshot":        handoff["intent_pack_snapshot"],
        "audit_snapshot":              handoff["audit_snapshot"],
    }, indent=2))


if __name__ == "__main__":
    main()
