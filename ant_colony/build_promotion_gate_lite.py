"""
AC-94: Promotion Gate for Paper-Ready Candidate

Single layer on top of AC-93 (replay validation + handoff consistency).
Aggregates all upstream verdicts into one formal promotion decision:
PAPER_READY, PAPER_HOLD, or PAPER_REJECTED.

Design principles:
  - promotion_non_binding=True always.
  - promotion_simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always — never overridden.
  - Fail-closed: any upstream reject → PAPER_REJECTED.
  - Fail-closed: invalid/missing input → PAPER_REJECTED.
  - Deterministic: same upstream verdicts → same promotion every call.
  - Pure core function — no I/O, no side effects.
  - No broker coupling, no live execution path, no portfolio mutation.
  - PAPER_READY does NOT trigger execution; it is a paper-only signal.

Promotion decision logic (evaluated in priority order):
  1. Invalid input (non-dict or missing required keys) → PAPER_REJECTED
  2. Any upstream status is *_REJECTED               → PAPER_REJECTED
  3. validation_passed=False or replay_consistent=False → PAPER_REJECTED
  4. handoff_consistency_passed=False                 → PAPER_REJECTED
  5. Any upstream status is *_HOLD or *_BASELINE      → PAPER_HOLD
  6. handoff_ready=False or runner_contract_valid=False → PAPER_HOLD
  7. All checks pass                                  → PAPER_READY

Promotion status values:
  PAPER_READY    — all upstream checks pass; candidate is paper-ready
  PAPER_HOLD     — upstream is in safe hold/baseline state
  PAPER_REJECTED — upstream rejected, check failed, or invalid input

Promotion mode values:
  PROMOTION_READY    — candidate promoted to paper-ready
  PROMOTION_HOLD     — candidate held at baseline
  PROMOTION_REJECTED — candidate rejected

Promotion reason codes:
  PROMOTION_ALL_CLEAR          — all checks pass → PAPER_READY
  PROMOTION_UPSTREAM_HOLD      — upstream layers in hold/baseline
  PROMOTION_HANDOFF_NOT_READY  — handoff_ready=False or runner_contract_valid=False
  PROMOTION_VALIDATION_FAILED  — validation_passed=False or replay_consistent=False
  PROMOTION_CONSISTENCY_FAILED — handoff_consistency_passed=False
  PROMOTION_UPSTREAM_REJECTED  — upstream status is rejected
  PROMOTION_INVALID_INPUT      — input is not a valid dict or missing keys

upstream_snapshot fields:
  handoff_status          — str
  handoff_ready           — bool
  runner_intake_status    — str
  runner_contract_valid   — bool
  validation_status       — str
  validation_passed       — bool
  replay_consistent       — bool
  consistency_status      — str
  consistency_passed      — bool

Usage (importable):
    from build_promotion_gate_lite import build_promotion_gate
    gate = build_promotion_gate(
        queen_handoff, runner_intake, replay_validation, handoff_consistency
    )

    # Full chain from specs:
    from build_promotion_gate_lite import build_promotion_from_specs
    result = build_promotion_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "promotion_gate_v1"

# ---------------------------------------------------------------------------
# Promotion status / mode / reason codes
# ---------------------------------------------------------------------------

PAPER_READY    = "PAPER_READY"
PAPER_HOLD     = "PAPER_HOLD"
PAPER_REJECTED = "PAPER_REJECTED"

MODE_READY    = "PROMOTION_READY"
MODE_HOLD     = "PROMOTION_HOLD"
MODE_REJECTED = "PROMOTION_REJECTED"

REASON_ALL_CLEAR         = "PROMOTION_ALL_CLEAR"
REASON_UPSTREAM_HOLD     = "PROMOTION_UPSTREAM_HOLD"
REASON_HANDOFF_NOT_READY = "PROMOTION_HANDOFF_NOT_READY"
REASON_VALIDATION_FAILED = "PROMOTION_VALIDATION_FAILED"
REASON_CONSISTENCY_FAILED = "PROMOTION_CONSISTENCY_FAILED"
REASON_UPSTREAM_REJECTED = "PROMOTION_UPSTREAM_REJECTED"
REASON_INVALID_INPUT     = "PROMOTION_INVALID_INPUT"

# Upstream hold / rejected status mirrors
_REJECTED_STATUSES = {
    "REJECT_HANDOFF", "HANDOFF_REJECTED",
    "INTAKE_REJECTED",
    "VALIDATION_REJECTED", "LEDGER_REJECTED", "TRACE_REJECTED",
    "CONSISTENCY_REJECTED",
}
_HOLD_STATUSES = {
    "HOLD_BASELINE_HANDOFF", "HANDOFF_BASELINE",
    "INTAKE_HOLD",
    "VALIDATION_HOLD", "LEDGER_HOLD", "TRACE_HOLD",
    "CONSISTENCY_HOLD",
}


# ---------------------------------------------------------------------------
# Core promotion gate function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_promotion_gate(
    queen_handoff:         object,
    runner_intake:         object,
    replay_validation:     object,
    handoff_consistency:   object,
) -> dict:
    """
    Aggregate upstream verdicts into one formal promotion decision.

    promotion_non_binding=True, promotion_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.
    No broker coupling, no live execution, no state mutation.
    PAPER_READY is a paper-only signal — it does NOT trigger execution.

    Args:
        queen_handoff:       dict from build_queen_handoff() (AC-90).
        runner_intake:       dict from build_runner_intake() (AC-91).
        replay_validation:   dict from build_replay_validator() (AC-93).
        handoff_consistency: dict from build_handoff_consistency_check() (AC-93).

    Returns:
        Promotion gate dict.
    """
    # Step 1 — validate inputs
    for name, obj in [
        ("queen_handoff", queen_handoff),
        ("runner_intake", runner_intake),
        ("replay_validation", replay_validation),
        ("handoff_consistency", handoff_consistency),
    ]:
        if not isinstance(obj, dict):
            return _rejected_gate(f"{name} is not a dict")

    if "handoff_status" not in queen_handoff:
        return _rejected_gate("queen_handoff missing handoff_status")
    if "runner_intake_status" not in runner_intake:
        return _rejected_gate("runner_intake missing runner_intake_status")
    if "validation_status" not in replay_validation:
        return _rejected_gate("replay_validation missing validation_status")
    if "handoff_consistency_status" not in handoff_consistency:
        return _rejected_gate("handoff_consistency missing handoff_consistency_status")

    # Extract upstream status values
    ho_status  = str(queen_handoff.get("handoff_status", ""))
    ri_status  = str(runner_intake.get("runner_intake_status", ""))
    val_status = str(replay_validation.get("validation_status", ""))
    con_status = str(handoff_consistency.get("handoff_consistency_status", ""))

    ho_ready      = bool(queen_handoff.get("handoff_ready", False))
    ri_valid      = bool(runner_intake.get("runner_contract_valid", False))
    val_passed    = bool(replay_validation.get("validation_passed", False))
    replay_ok     = bool(replay_validation.get("replay_consistent", False))
    con_passed    = bool(handoff_consistency.get("handoff_consistency_passed", False))

    snap = _upstream_snapshot(
        queen_handoff, runner_intake, replay_validation, handoff_consistency
    )

    all_statuses = {ho_status, ri_status, val_status, con_status}

    # Step 2 — any upstream rejected → PAPER_REJECTED
    if all_statuses & _REJECTED_STATUSES:
        return _gate_result(
            PAPER_REJECTED, MODE_REJECTED, False, False, snap,
            "upstream layer is rejected — promotion closed",
            REASON_UPSTREAM_REJECTED,
        )

    # Step 3 — any upstream hold/baseline → PAPER_HOLD
    # (hold = no data / safe pause; checked before boolean flags so a
    #  hold-state with validation_passed=False yields HOLD, not REJECT)
    if all_statuses & _HOLD_STATUSES:
        return _gate_result(
            PAPER_HOLD, MODE_HOLD, False, False, snap,
            "upstream layer is in hold/baseline state — promotion held",
            REASON_UPSTREAM_HOLD,
        )

    # Step 4 — validation or replay failed → PAPER_REJECTED
    if not val_passed or not replay_ok:
        return _gate_result(
            PAPER_REJECTED, MODE_REJECTED, False, False, snap,
            f"validation_passed={val_passed}, replay_consistent={replay_ok} — promotion rejected",
            REASON_VALIDATION_FAILED,
        )

    # Step 5 — consistency failed → PAPER_REJECTED
    if not con_passed:
        return _gate_result(
            PAPER_REJECTED, MODE_REJECTED, False, False, snap,
            "handoff_consistency_passed=False — promotion rejected",
            REASON_CONSISTENCY_FAILED,
        )

    # Step 6 — handoff not ready or runner contract not valid → PAPER_HOLD
    if not ho_ready or not ri_valid:
        return _gate_result(
            PAPER_HOLD, MODE_HOLD, False, False, snap,
            f"handoff_ready={ho_ready}, runner_contract_valid={ri_valid} — promotion held",
            REASON_HANDOFF_NOT_READY,
        )

    # Step 7 — all clear → PAPER_READY
    return _gate_result(
        PAPER_READY, MODE_READY, True, True, snap,
        "all upstream checks pass — candidate is paper-ready",
        REASON_ALL_CLEAR,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gate_result(
    status:       str,
    mode:         str,
    ready:        bool,
    paper_ready:  bool,
    snap:         dict,
    reason:       str,
    reason_code:  str,
) -> dict:
    return {
        "promotion_status":          status,
        "promotion_mode":            mode,
        "promotion_ready":           ready,
        "promotion_reason":          reason,
        "promotion_reason_code":     reason_code,
        "promotion_decision":        f"{status}: {reason_code}",
        "upstream_snapshot":         snap,
        "paper_ready_candidate":     paper_ready,
        "promotion_non_binding":     True,
        "promotion_simulation_only": True,
        "paper_only":                True,
        "live_activation_allowed":   False,
    }


def _rejected_gate(reason: str) -> dict:
    return _gate_result(
        PAPER_REJECTED, MODE_REJECTED, False, False, {},
        reason, REASON_INVALID_INPUT,
    )


def _upstream_snapshot(
    queen_handoff:       dict,
    runner_intake:       dict,
    replay_validation:   dict,
    handoff_consistency: dict,
) -> dict:
    return {
        "handoff_status":        str(queen_handoff.get("handoff_status", "")),
        "handoff_ready":         bool(queen_handoff.get("handoff_ready", False)),
        "runner_intake_status":  str(runner_intake.get("runner_intake_status", "")),
        "runner_contract_valid": bool(runner_intake.get("runner_contract_valid", False)),
        "validation_status":     str(replay_validation.get("validation_status", "")),
        "validation_passed":     bool(replay_validation.get("validation_passed", False)),
        "replay_consistent":     bool(replay_validation.get("replay_consistent", False)),
        "consistency_status":    str(handoff_consistency.get("handoff_consistency_status", "")),
        "consistency_passed":    bool(handoff_consistency.get("handoff_consistency_passed", False)),
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_validator_module():
    path = Path(__file__).parent / "build_replay_validator_lite.py"
    spec = importlib.util.spec_from_file_location("_validator", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-93 + AC-94
# ---------------------------------------------------------------------------

def build_promotion_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → … → validation + consistency (AC-93)
                             → promotion gate (AC-94).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit, queen_handoff,
        runner_intake, dry_run_consumption,
        execution_ledger, audit_trace,
        replay_validation, handoff_consistency,
        promotion_gate.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _val_mod = _load_validator_module()
    pipeline = _val_mod.build_validation_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    gate = build_promotion_gate(
        pipeline["queen_handoff"],
        pipeline["runner_intake"],
        pipeline["replay_validation"],
        pipeline["handoff_consistency"],
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
        "queen_handoff":            pipeline["queen_handoff"],
        "runner_intake":            pipeline["runner_intake"],
        "dry_run_consumption":      pipeline["dry_run_consumption"],
        "execution_ledger":         pipeline["execution_ledger"],
        "audit_trace":              pipeline["audit_trace"],
        "replay_validation":        pipeline["replay_validation"],
        "handoff_consistency":      pipeline["handoff_consistency"],
        "promotion_gate":           gate,
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
        "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
    }

    result = build_promotion_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    g = result["promotion_gate"]

    print(json.dumps({
        "promotion_status":          g["promotion_status"],
        "promotion_mode":            g["promotion_mode"],
        "promotion_ready":           g["promotion_ready"],
        "promotion_reason_code":     g["promotion_reason_code"],
        "promotion_decision":        g["promotion_decision"],
        "paper_ready_candidate":     g["paper_ready_candidate"],
        "promotion_non_binding":     g["promotion_non_binding"],
        "promotion_simulation_only": g["promotion_simulation_only"],
        "paper_only":                g["paper_only"],
        "live_activation_allowed":   g["live_activation_allowed"],
        "upstream_snapshot":         g["upstream_snapshot"],
    }, indent=2))


if __name__ == "__main__":
    main()
