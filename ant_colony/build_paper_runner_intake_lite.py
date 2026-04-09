"""
AC-91: Paper Runner Intake Contract + Dry-Run Consumption

Two layers on top of the AC-90 queen handoff boundary:

1. Paper Runner Intake Contract
   Validates the queen handoff boundary object and produces a compact,
   auditable runner intake record — showing whether a paper runner may
   accept the handoff for further processing.

2. Dry-Run Consumption
   Shows explicitly how a paper runner *would* consume the intake — which
   intents would be processed, which would be skipped (HOLD), and which
   are blocked — without any real execution, state mutation, portfolio
   change, or broker impact.

Design principles:
  - runner_intake_non_binding=True always.
  - runner_intake_simulation_only=True always.
  - dry_run_non_binding=True always.
  - dry_run_simulation_only=True always.
  - paper_only=True always on all outputs.
  - live_activation_allowed=False always — never overridden, never removed.
  - Fail-closed: invalid/missing input → INTAKE_REJECTED / DRY_RUN_REJECTED.
  - Deterministic: same inputs → same outputs every call.
  - Pure core functions — no I/O, no side effects.
  - No broker coupling, no live execution path, no portfolio mutation.

Runner intake status values:
  INTAKE_ACCEPTED  — handoff_ready=True and handoff is READY_FOR_PAPER_HANDOFF;
                     runner_contract_valid=True.
  INTAKE_HOLD      — handoff is baseline / no allowed intents; safe hold.
  INTAKE_REJECTED  — invalid/missing handoff or handoff was REJECT_HANDOFF.

Runner intake mode values:
  INTAKE_READY     — intake accepted; ready for dry-run consumption
  INTAKE_BASELINE  — safe baseline hold; nothing to consume
  INTAKE_REJECTED  — intake rejected; boundary closed

Runner contract reason codes:
  INTAKE_CONTRACT_VALID         — handoff accepted, contract valid
  INTAKE_CONTRACT_HOLD_BASELINE — handoff is baseline / hold
  INTAKE_CONTRACT_NO_ALLOWED    — active pack but zero allowed intents
  INTAKE_CONTRACT_HANDOFF_REJECTED — source handoff was REJECT_HANDOFF
  INTAKE_CONTRACT_INVALID_INPUT    — handoff object is not a valid dict

Dry-run status values:
  DRY_RUN_COMPLETE — intake accepted; dry-run ran to completion
  DRY_RUN_HOLD     — intake was hold; dry-run held at baseline
  DRY_RUN_REJECTED — intake rejected; dry-run closed

Dry-run mode values:
  DRY_RUN_READY    — dry-run completed (even if all intents skipped/blocked)
  DRY_RUN_BASELINE — dry-run held at baseline
  DRY_RUN_REJECTED — dry-run rejected

Dry-run reason codes:
  DRY_RUN_OK                  — dry-run completed with ≥1 consumed intent
  DRY_RUN_ALL_SKIPPED_OR_BLOCKED — completed but zero consumed (all hold/blocked)
  DRY_RUN_HOLD_BASELINE       — intake was hold
  DRY_RUN_INVALID_INTAKE      — intake object invalid

Usage (importable):
    from build_paper_runner_intake_lite import build_runner_intake
    intake = build_runner_intake(queen_handoff)

    from build_paper_runner_intake_lite import build_dry_run_consumption
    dry_run = build_dry_run_consumption(runner_intake, intent_pack=None)

    # Combined:
    from build_paper_runner_intake_lite import build_intake_and_dry_run
    result = build_intake_and_dry_run(queen_handoff, intent_pack=None)

    # Full chain from specs:
    from build_paper_runner_intake_lite import build_dry_run_from_specs
    result = build_dry_run_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Runner intake output fields:
    runner_intake_status         — "INTAKE_ACCEPTED"|"INTAKE_HOLD"|"INTAKE_REJECTED"
    runner_intake_mode           — "INTAKE_READY"|"INTAKE_BASELINE"|"INTAKE_REJECTED"
    runner_contract_valid        — bool: True only when INTAKE_ACCEPTED
    runner_contract_reason       — human-readable
    runner_contract_reason_code  — machine-stable
    handoff_snapshot             — compact copy of key handoff fields
    consumed_intent_count        — int: total intents from handoff
    consumed_allowed_count       — int: allowed intents from handoff
    consumed_blocked_count       — int: blocked intents from handoff
    runner_intake_non_binding    — always True
    runner_intake_simulation_only — always True
    paper_only                   — always True
    live_activation_allowed      — always False

handoff_snapshot fields:
    handoff_status     — str
    handoff_mode       — str
    handoff_ready      — bool
    total_intents      — int
    total_allowed      — int
    total_blocked      — int
    live_activation_allowed — always False

Dry-run output fields:
    dry_run_status           — "DRY_RUN_COMPLETE"|"DRY_RUN_HOLD"|"DRY_RUN_REJECTED"
    dry_run_mode             — "DRY_RUN_READY"|"DRY_RUN_BASELINE"|"DRY_RUN_REJECTED"
    dry_run_reason           — human-readable
    dry_run_reason_code      — machine-stable
    dry_run_consumed_intents — list: intents the runner would process (INCREASE/DECREASE)
    dry_run_skipped_intents  — list: intents the runner would skip (HOLD)
    dry_run_blocked_intents  — list: intents that cannot be processed (BLOCKED)
    dry_run_non_binding      — always True
    dry_run_simulation_only  — always True
    paper_only               — always True
    live_activation_allowed  — always False

Per dry-run intent entry fields (when intent_pack provided):
    market              — str
    intent_action       — str
    intent_status       — str
    delta_eur           — float
    paper_only          — always True

Per dry-run intent entry fields (snapshot-derived, no intent_pack):
    intent_action       — str (synthetic from snapshot counts)
    intent_status       — str
    paper_only          — always True
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "paper_runner_intake_v1"

# ---------------------------------------------------------------------------
# Runner intake status / mode / reason codes
# ---------------------------------------------------------------------------

INTAKE_ACCEPTED = "INTAKE_ACCEPTED"
INTAKE_HOLD     = "INTAKE_HOLD"
INTAKE_REJECTED = "INTAKE_REJECTED"

INTAKE_MODE_READY    = "INTAKE_READY"
INTAKE_MODE_BASELINE = "INTAKE_BASELINE"
INTAKE_MODE_REJECTED = "INTAKE_REJECTED"

REASON_CONTRACT_VALID        = "INTAKE_CONTRACT_VALID"
REASON_CONTRACT_HOLD_BASELINE = "INTAKE_CONTRACT_HOLD_BASELINE"
REASON_CONTRACT_NO_ALLOWED   = "INTAKE_CONTRACT_NO_ALLOWED"
REASON_CONTRACT_HO_REJECTED  = "INTAKE_CONTRACT_HANDOFF_REJECTED"
REASON_CONTRACT_INVALID      = "INTAKE_CONTRACT_INVALID_INPUT"

# ---------------------------------------------------------------------------
# Dry-run status / mode / reason codes
# ---------------------------------------------------------------------------

DRY_RUN_COMPLETE = "DRY_RUN_COMPLETE"
DRY_RUN_HOLD     = "DRY_RUN_HOLD"
DRY_RUN_REJECTED = "DRY_RUN_REJECTED"

DRY_RUN_MODE_READY    = "DRY_RUN_READY"
DRY_RUN_MODE_BASELINE = "DRY_RUN_BASELINE"
DRY_RUN_MODE_REJECTED = "DRY_RUN_REJECTED"

REASON_DRY_RUN_OK            = "DRY_RUN_OK"
REASON_DRY_RUN_ALL_SKIPPED   = "DRY_RUN_ALL_SKIPPED_OR_BLOCKED"
REASON_DRY_RUN_HOLD_BASELINE = "DRY_RUN_HOLD_BASELINE"
REASON_DRY_RUN_INVALID       = "DRY_RUN_INVALID_INTAKE"

# Handoff status mirrors (AC-90)
_HO_READY    = "READY_FOR_PAPER_HANDOFF"
_HO_BASELINE = "HOLD_BASELINE_HANDOFF"
_HO_REJECTED = "REJECT_HANDOFF"

# Intent action / status mirrors (AC-89)
_ACTION_INCREASE = "PAPER_INCREASE_INTENT"
_ACTION_DECREASE = "PAPER_DECREASE_INTENT"
_ACTION_HOLD     = "PAPER_HOLD_INTENT"
_ACTION_BLOCKED  = "PAPER_BLOCKED_INTENT"
_STATUS_ALLOWED  = "ALLOWED"
_STATUS_BLOCKED  = "BLOCKED"

# Actions a runner would consume (not skip, not blocked)
_CONSUME_ACTIONS = {_ACTION_INCREASE, _ACTION_DECREASE}


# ---------------------------------------------------------------------------
# Core intake function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_runner_intake(queen_handoff: object) -> dict:
    """
    Build a paper runner intake contract from an AC-90 queen handoff boundary.

    runner_intake_non_binding=True, runner_intake_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.
    No broker coupling, no live execution, no state mutation.

    Args:
        queen_handoff: dict from build_queen_handoff() (AC-90).

    Returns:
        Runner intake contract dict.
    """
    if not isinstance(queen_handoff, dict):
        return _rejected_intake("queen_handoff is not a dict")

    if "handoff_status" not in queen_handoff:
        return _rejected_intake("queen_handoff missing handoff_status")

    if "handoff_ready" not in queen_handoff:
        return _rejected_intake("queen_handoff missing handoff_ready")

    ho_status     = queen_handoff.get("handoff_status", "")
    ho_ready      = queen_handoff.get("handoff_ready", False)
    total_intents = int(queen_handoff.get("total_intents", 0))
    total_allowed = int(queen_handoff.get("total_allowed", 0))
    total_blocked = int(queen_handoff.get("total_blocked", 0))
    snap          = _handoff_snapshot(queen_handoff)

    # Rejected handoff → reject intake
    if ho_status == _HO_REJECTED:
        return {
            "runner_intake_status":        INTAKE_REJECTED,
            "runner_intake_mode":          INTAKE_MODE_REJECTED,
            "runner_contract_valid":       False,
            "runner_contract_reason":      "source handoff was REJECT_HANDOFF — intake closed",
            "runner_contract_reason_code": REASON_CONTRACT_HO_REJECTED,
            "handoff_snapshot":            snap,
            "consumed_intent_count":       total_intents,
            "consumed_allowed_count":      total_allowed,
            "consumed_blocked_count":      total_blocked,
            "runner_intake_non_binding":   True,
            "runner_intake_simulation_only": True,
            "paper_only":                  True,
            "live_activation_allowed":     False,
        }

    # Baseline handoff → hold intake
    if ho_status == _HO_BASELINE or not ho_ready:
        return {
            "runner_intake_status":        INTAKE_HOLD,
            "runner_intake_mode":          INTAKE_MODE_BASELINE,
            "runner_contract_valid":       False,
            "runner_contract_reason":      f"handoff is {ho_status} — runner intake held at baseline",
            "runner_contract_reason_code": REASON_CONTRACT_HOLD_BASELINE,
            "handoff_snapshot":            snap,
            "consumed_intent_count":       total_intents,
            "consumed_allowed_count":      total_allowed,
            "consumed_blocked_count":      total_blocked,
            "runner_intake_non_binding":   True,
            "runner_intake_simulation_only": True,
            "paper_only":                  True,
            "live_activation_allowed":     False,
        }

    # Handoff is READY — check for zero allowed intents
    if total_allowed == 0:
        return {
            "runner_intake_status":        INTAKE_HOLD,
            "runner_intake_mode":          INTAKE_MODE_BASELINE,
            "runner_contract_valid":       False,
            "runner_contract_reason":      "handoff ready but zero allowed intents — runner intake held",
            "runner_contract_reason_code": REASON_CONTRACT_NO_ALLOWED,
            "handoff_snapshot":            snap,
            "consumed_intent_count":       total_intents,
            "consumed_allowed_count":      total_allowed,
            "consumed_blocked_count":      total_blocked,
            "runner_intake_non_binding":   True,
            "runner_intake_simulation_only": True,
            "paper_only":                  True,
            "live_activation_allowed":     False,
        }

    # Handoff ready with ≥1 allowed intent — accept intake
    return {
        "runner_intake_status":        INTAKE_ACCEPTED,
        "runner_intake_mode":          INTAKE_MODE_READY,
        "runner_contract_valid":       True,
        "runner_contract_reason":      (
            f"runner intake accepted: {total_allowed} allowed intent(s), "
            f"{total_blocked} blocked"
        ),
        "runner_contract_reason_code": REASON_CONTRACT_VALID,
        "handoff_snapshot":            snap,
        "consumed_intent_count":       total_intents,
        "consumed_allowed_count":      total_allowed,
        "consumed_blocked_count":      total_blocked,
        "runner_intake_non_binding":   True,
        "runner_intake_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


# ---------------------------------------------------------------------------
# Core dry-run function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_dry_run_consumption(
    runner_intake: object,
    intent_pack:   object = None,
) -> dict:
    """
    Build a dry-run consumption record showing how a paper runner would
    consume the accepted intake — without any real execution.

    Classifies each intent as:
      - consumed  : ALLOWED + action is INCREASE or DECREASE
      - skipped   : ALLOWED + action is HOLD (no capital change needed)
      - blocked   : BLOCKED intents (invalid, fail-closed)

    If intent_pack is None or missing intents list, synthetic count-based
    summary entries are generated from the intake snapshot.

    dry_run_non_binding=True, dry_run_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.
    No execution, no state mutation, no broker impact.

    Args:
        runner_intake: dict from build_runner_intake() (AC-91).
        intent_pack:   optional dict from build_paper_intent_pack() (AC-89).
                       When provided, per-intent detail is extracted.

    Returns:
        Dry-run consumption dict.
    """
    if not isinstance(runner_intake, dict):
        return _rejected_dry_run("runner_intake is not a dict")

    if "runner_intake_status" not in runner_intake:
        return _rejected_dry_run("runner_intake missing runner_intake_status")

    intake_status = runner_intake.get("runner_intake_status", "")

    # Non-accepted intake → hold or reject dry-run
    if intake_status == INTAKE_REJECTED:
        return _hold_dry_run(
            REASON_DRY_RUN_INVALID,
            "runner intake was INTAKE_REJECTED — dry-run closed",
            DRY_RUN_REJECTED,
            DRY_RUN_MODE_REJECTED,
        )

    if intake_status == INTAKE_HOLD:
        return _hold_dry_run(
            REASON_DRY_RUN_HOLD_BASELINE,
            "runner intake is INTAKE_HOLD — dry-run held at baseline",
            DRY_RUN_HOLD,
            DRY_RUN_MODE_BASELINE,
        )

    # Intake accepted — perform dry-run classification
    consumed, skipped, blocked = _classify_intents(runner_intake, intent_pack)

    consumed_count = len(consumed)
    reason_code = REASON_DRY_RUN_OK if consumed_count > 0 else REASON_DRY_RUN_ALL_SKIPPED
    reason = (
        f"dry-run complete: {consumed_count} consumed, "
        f"{len(skipped)} skipped, {len(blocked)} blocked"
    )

    return {
        "dry_run_status":           DRY_RUN_COMPLETE,
        "dry_run_mode":             DRY_RUN_MODE_READY,
        "dry_run_reason":           reason,
        "dry_run_reason_code":      reason_code,
        "dry_run_consumed_intents": consumed,
        "dry_run_skipped_intents":  skipped,
        "dry_run_blocked_intents":  blocked,
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


# ---------------------------------------------------------------------------
# Intent classification helper
# ---------------------------------------------------------------------------

def _classify_intents(
    runner_intake: dict,
    intent_pack:   object,
) -> tuple:
    """
    Classify intents into (consumed, skipped, blocked) lists.
    Returns three lists of compact intent descriptors.
    Uses intent_pack.intents when available; falls back to snapshot counts.
    """
    # Try to use full intent list from intent_pack
    if (isinstance(intent_pack, dict)
            and isinstance(intent_pack.get("intents"), list)):
        return _classify_from_intent_list(intent_pack["intents"])

    # Fall back: synthetic entries from snapshot counts
    return _classify_from_snapshot(runner_intake)


def _classify_from_intent_list(intents: list) -> tuple:
    consumed = []
    skipped  = []
    blocked  = []

    for entry in intents:
        if not isinstance(entry, dict):
            blocked.append({"intent_action": "UNKNOWN", "intent_status": "BLOCKED", "paper_only": True})
            continue

        action  = str(entry.get("intent_action", ""))
        status  = str(entry.get("intent_status", ""))
        market  = str(entry.get("market", ""))
        delta   = _safe_float(entry.get("delta_eur"), 0.0)
        record  = {
            "market":        market,
            "intent_action": action,
            "intent_status": status,
            "delta_eur":     delta,
            "paper_only":    True,
        }

        if status == _STATUS_BLOCKED or action == _ACTION_BLOCKED:
            blocked.append(record)
        elif action == _ACTION_HOLD:
            skipped.append(record)
        elif action in _CONSUME_ACTIONS:
            consumed.append(record)
        else:
            # Unknown action — treat as blocked (fail-closed)
            record["intent_status"] = _STATUS_BLOCKED
            blocked.append(record)

    return consumed, skipped, blocked


def _classify_from_snapshot(runner_intake: dict) -> tuple:
    """
    Generate synthetic count-based entries when no intent_pack is available.
    Consumed = allowed_count placeholder entries (action unknown, assume active).
    Blocked  = blocked_count placeholder entries.
    Skipped  = [] (cannot determine HOLD vs INCREASE/DECREASE from counts alone).
    """
    snap          = runner_intake.get("handoff_snapshot", {})
    allowed_count = int(runner_intake.get("consumed_allowed_count", 0))
    blocked_count = int(runner_intake.get("consumed_blocked_count", 0))

    consumed = [
        {"intent_action": "PAPER_ACTIVE_INTENT_SNAPSHOT", "intent_status": _STATUS_ALLOWED, "paper_only": True}
        for _ in range(allowed_count)
    ]
    blocked = [
        {"intent_action": _ACTION_BLOCKED, "intent_status": _STATUS_BLOCKED, "paper_only": True}
        for _ in range(blocked_count)
    ]
    return consumed, [], blocked


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def _handoff_snapshot(queen_handoff: dict) -> dict:
    return {
        "handoff_status":        str(queen_handoff.get("handoff_status", "")),
        "handoff_mode":          str(queen_handoff.get("handoff_mode", "")),
        "handoff_ready":         bool(queen_handoff.get("handoff_ready", False)),
        "total_intents":         int(queen_handoff.get("total_intents", 0)),
        "total_allowed":         int(queen_handoff.get("total_allowed", 0)),
        "total_blocked":         int(queen_handoff.get("total_blocked", 0)),
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
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_intake(reason: str) -> dict:
    return {
        "runner_intake_status":        INTAKE_REJECTED,
        "runner_intake_mode":          INTAKE_MODE_REJECTED,
        "runner_contract_valid":       False,
        "runner_contract_reason":      reason,
        "runner_contract_reason_code": REASON_CONTRACT_INVALID,
        "handoff_snapshot":            {},
        "consumed_intent_count":       0,
        "consumed_allowed_count":      0,
        "consumed_blocked_count":      0,
        "runner_intake_non_binding":   True,
        "runner_intake_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _rejected_dry_run(reason: str) -> dict:
    return {
        "dry_run_status":           DRY_RUN_REJECTED,
        "dry_run_mode":             DRY_RUN_MODE_REJECTED,
        "dry_run_reason":           reason,
        "dry_run_reason_code":      REASON_DRY_RUN_INVALID,
        "dry_run_consumed_intents": [],
        "dry_run_skipped_intents":  [],
        "dry_run_blocked_intents":  [],
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


def _hold_dry_run(
    reason_code: str,
    reason:      str,
    status:      str,
    mode:        str,
) -> dict:
    return {
        "dry_run_status":           status,
        "dry_run_mode":             mode,
        "dry_run_reason":           reason,
        "dry_run_reason_code":      reason_code,
        "dry_run_consumed_intents": [],
        "dry_run_skipped_intents":  [],
        "dry_run_blocked_intents":  [],
        "dry_run_non_binding":      True,
        "dry_run_simulation_only":  True,
        "paper_only":               True,
        "live_activation_allowed":  False,
    }


# ---------------------------------------------------------------------------
# Convenience: intake + dry-run combined
# ---------------------------------------------------------------------------

def build_intake_and_dry_run(
    queen_handoff: object,
    intent_pack:   object = None,
) -> dict:
    """
    Build runner intake contract and dry-run consumption in one call.

    Args:
        queen_handoff: dict from build_queen_handoff() (AC-90).
        intent_pack:   optional dict from build_paper_intent_pack() (AC-89).
                       When provided, per-intent dry-run detail is available.

    Returns:
        Dict with keys: runner_intake, dry_run_consumption.
    """
    intake  = build_runner_intake(queen_handoff)
    dry_run = build_dry_run_consumption(intake, intent_pack)
    return {
        "runner_intake":       intake,
        "dry_run_consumption": dry_run,
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_handoff_module():
    path = Path(__file__).parent / "build_queen_handoff_boundary_lite.py"
    spec = importlib.util.spec_from_file_location("_handoff", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-90 + AC-91
# ---------------------------------------------------------------------------

def build_dry_run_from_specs(
    market_specs:              object,
    total_equity_eur:          float,
    market_regimes:            object = None,
    market_capital_fractions:  object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → envelope (AC-84) → regime overlay (AC-85)
                             → proposal (AC-86) → conflict selection (AC-87)
                             → candidate + transition (AC-88)
                             → intent pack + audit (AC-89)
                             → queen handoff boundary (AC-90)
                             → runner intake contract + dry-run (AC-91).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit, queen_handoff,
        runner_intake, dry_run_consumption.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _ho_mod   = _load_handoff_module()
    pipeline  = _ho_mod.build_handoff_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    intake  = build_runner_intake(pipeline["queen_handoff"])
    dry_run = build_dry_run_consumption(intake, pipeline["intent_pack"])
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
        "runner_intake":            intake,
        "dry_run_consumption":      dry_run,
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

    result  = build_dry_run_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    intake  = result["runner_intake"]
    dry_run = result["dry_run_consumption"]

    print(json.dumps({
        "runner_intake_status":          intake["runner_intake_status"],
        "runner_intake_mode":            intake["runner_intake_mode"],
        "runner_contract_valid":         intake["runner_contract_valid"],
        "runner_contract_reason_code":   intake["runner_contract_reason_code"],
        "consumed_intent_count":         intake["consumed_intent_count"],
        "consumed_allowed_count":        intake["consumed_allowed_count"],
        "consumed_blocked_count":        intake["consumed_blocked_count"],
        "runner_intake_non_binding":     intake["runner_intake_non_binding"],
        "runner_intake_simulation_only": intake["runner_intake_simulation_only"],
        "paper_only":                    intake["paper_only"],
        "live_activation_allowed":       intake["live_activation_allowed"],
        "dry_run_status":                dry_run["dry_run_status"],
        "dry_run_mode":                  dry_run["dry_run_mode"],
        "dry_run_reason_code":           dry_run["dry_run_reason_code"],
        "dry_run_consumed_count":        len(dry_run["dry_run_consumed_intents"]),
        "dry_run_skipped_count":         len(dry_run["dry_run_skipped_intents"]),
        "dry_run_blocked_count":         len(dry_run["dry_run_blocked_intents"]),
        "dry_run_non_binding":           dry_run["dry_run_non_binding"],
        "dry_run_simulation_only":       dry_run["dry_run_simulation_only"],
    }, indent=2))


if __name__ == "__main__":
    main()
