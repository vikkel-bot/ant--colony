"""
AC-93: Replay Validator + Handoff Consistency Check

Two layers on top of the AC-92 dry-run execution ledger and audit trace:

1. Replay Validator
   Checks that the execution ledger and audit trace are internally
   consistent: counts match, indices are sequential, required fields
   are present, and the replay trace faithfully mirrors the ledger.

2. Handoff Consistency Check
   Cross-layer count reconciliation: queen handoff (AC-90) →
   runner intake (AC-91) → ledger (AC-92). Detects intent count
   mismatches across layers without touching live state.

Design principles:
  - validation_non_binding=True always.
  - validation_simulation_only=True always.
  - consistency_non_binding=True always.
  - consistency_simulation_only=True always.
  - paper_only=True always on all outputs.
  - live_activation_allowed=False always — never overridden.
  - Fail-closed: invalid/missing input → VALIDATION_REJECTED / CONSISTENCY_REJECTED.
  - Deterministic: same inputs → same validation result every call.
  - Pure core functions — no I/O, no side effects.
  - No broker coupling, no live execution path, no portfolio mutation.

Replay validation checks (all must pass for validation_passed=True):
  CHECK_COUNTS        — ledger_entry_count == trace_step_count
  CHECK_INDICES       — entry_index / step_index are 0-based sequential, matching
  CHECK_CATEGORY_SUM  — consumed+skipped+blocked == ledger_entry_count
  CHECK_FIELDS        — all entries/steps have required fields

Handoff consistency checks:
  CROSS_TOTAL_INTENTS — handoff.total_intents == intake.consumed_intent_count
  CROSS_ALLOWED       — handoff.total_allowed == intake.consumed_allowed_count
  CROSS_BLOCKED       — handoff.total_blocked == intake.consumed_blocked_count
  CROSS_ALLOWED_LEDGER — intake.consumed_allowed_count == ledger.consumed_count + ledger.skipped_count
  CROSS_BLOCKED_LEDGER — intake.consumed_blocked_count == ledger.blocked_count
  CROSS_TRACE         — ledger.ledger_entry_count == trace.trace_step_count (if both present)

Validation status values:
  VALIDATION_PASSED   — all internal checks pass
  VALIDATION_FAILED   — one or more checks fail
  VALIDATION_HOLD     — ledger/trace is hold/baseline
  VALIDATION_REJECTED — invalid/missing input

Validation mode values:
  VALIDATION_OK       — passed
  VALIDATION_ERROR    — failed
  VALIDATION_BASELINE — hold
  VALIDATION_REJECTED — rejected

Validation reason codes:
  VALIDATION_ALL_CHECKS_PASSED    — all checks pass
  VALIDATION_COUNT_MISMATCH       — ledger_entry_count != trace_step_count
  VALIDATION_INDEX_MISMATCH       — indices not sequential or not matching
  VALIDATION_CATEGORY_SUM_MISMATCH — category counts don't sum to total
  VALIDATION_MISSING_FIELDS       — required fields absent in entries/steps
  VALIDATION_HOLD_BASELINE        — ledger or trace is hold
  VALIDATION_INVALID_INPUT        — input is not a valid dict

Consistency status values:
  CONSISTENCY_PASSED   — all cross-layer counts match
  CONSISTENCY_FAILED   — one or more mismatches detected
  CONSISTENCY_HOLD     — upstream layers are hold
  CONSISTENCY_REJECTED — invalid/missing input

Consistency reason codes:
  CONSISTENCY_ALL_MATCHED          — all cross-layer checks pass
  CONSISTENCY_TOTAL_MISMATCH       — total_intents mismatch
  CONSISTENCY_ALLOWED_MISMATCH     — allowed count mismatch
  CONSISTENCY_BLOCKED_MISMATCH     — blocked count mismatch
  CONSISTENCY_ALLOWED_LEDGER_MISMATCH — allowed vs ledger(consumed+skipped) mismatch
  CONSISTENCY_BLOCKED_LEDGER_MISMATCH — blocked vs ledger.blocked_count mismatch
  CONSISTENCY_TRACE_MISMATCH       — ledger.entry_count != trace.step_count
  CONSISTENCY_HOLD_BASELINE        — upstream layers in hold state
  CONSISTENCY_INVALID_INPUT        — input not valid

Usage (importable):
    from build_replay_validator_lite import build_replay_validator
    validation = build_replay_validator(ledger, trace)

    from build_replay_validator_lite import build_handoff_consistency_check
    consistency = build_handoff_consistency_check(queen_handoff, runner_intake, ledger, trace)

    # Combined:
    from build_replay_validator_lite import build_validation_and_consistency
    result = build_validation_and_consistency(ledger, trace, queen_handoff, runner_intake)

    # Full chain from specs:
    from build_replay_validator_lite import build_validation_from_specs
    result = build_validation_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "replay_validator_v1"

# ---------------------------------------------------------------------------
# Validation status / mode / reason codes
# ---------------------------------------------------------------------------

VALIDATION_PASSED   = "VALIDATION_PASSED"
VALIDATION_FAILED   = "VALIDATION_FAILED"
VALIDATION_HOLD     = "VALIDATION_HOLD"
VALIDATION_REJECTED = "VALIDATION_REJECTED"

VAL_MODE_OK       = "VALIDATION_OK"
VAL_MODE_ERROR    = "VALIDATION_ERROR"
VAL_MODE_BASELINE = "VALIDATION_BASELINE"
VAL_MODE_REJECTED = "VALIDATION_REJECTED"

REASON_VAL_PASSED          = "VALIDATION_ALL_CHECKS_PASSED"
REASON_VAL_COUNT_MISMATCH  = "VALIDATION_COUNT_MISMATCH"
REASON_VAL_INDEX_MISMATCH  = "VALIDATION_INDEX_MISMATCH"
REASON_VAL_CAT_MISMATCH    = "VALIDATION_CATEGORY_SUM_MISMATCH"
REASON_VAL_FIELDS          = "VALIDATION_MISSING_FIELDS"
REASON_VAL_HOLD            = "VALIDATION_HOLD_BASELINE"
REASON_VAL_INVALID         = "VALIDATION_INVALID_INPUT"

# ---------------------------------------------------------------------------
# Consistency status / reason codes
# ---------------------------------------------------------------------------

CONSISTENCY_PASSED   = "CONSISTENCY_PASSED"
CONSISTENCY_FAILED   = "CONSISTENCY_FAILED"
CONSISTENCY_HOLD     = "CONSISTENCY_HOLD"
CONSISTENCY_REJECTED = "CONSISTENCY_REJECTED"

CON_MODE_OK       = "CONSISTENCY_OK"
CON_MODE_ERROR    = "CONSISTENCY_ERROR"
CON_MODE_BASELINE = "CONSISTENCY_BASELINE"
CON_MODE_REJECTED = "CONSISTENCY_REJECTED"

REASON_CON_MATCHED          = "CONSISTENCY_ALL_MATCHED"
REASON_CON_TOTAL_MISMATCH   = "CONSISTENCY_TOTAL_MISMATCH"
REASON_CON_ALLOWED_MISMATCH = "CONSISTENCY_ALLOWED_MISMATCH"
REASON_CON_BLOCKED_MISMATCH = "CONSISTENCY_BLOCKED_MISMATCH"
REASON_CON_ALLOWED_LEDGER   = "CONSISTENCY_ALLOWED_LEDGER_MISMATCH"
REASON_CON_BLOCKED_LEDGER   = "CONSISTENCY_BLOCKED_LEDGER_MISMATCH"
REASON_CON_TRACE_MISMATCH   = "CONSISTENCY_TRACE_MISMATCH"
REASON_CON_HOLD             = "CONSISTENCY_HOLD_BASELINE"
REASON_CON_INVALID          = "CONSISTENCY_INVALID_INPUT"

# Ledger / trace hold mirrors
_LEDGER_HOLD     = "LEDGER_HOLD"
_LEDGER_REJECTED = "LEDGER_REJECTED"
_TRACE_HOLD      = "TRACE_HOLD"
_TRACE_REJECTED  = "TRACE_REJECTED"

# Required fields for ledger entries and trace steps
_REQUIRED_ENTRY_FIELDS = {"entry_index", "ledger_category", "market",
                          "intent_action", "intent_status", "delta_eur", "paper_only"}
_REQUIRED_STEP_FIELDS  = {"step_index", "step_category", "step_market",
                          "step_action", "step_delta_eur", "replay_safe", "paper_only"}


# ---------------------------------------------------------------------------
# Core replay validator (pure, no I/O)
# ---------------------------------------------------------------------------

def build_replay_validator(ledger: object, trace: object) -> dict:
    """
    Validate internal consistency of a dry-run execution ledger and trace.

    Checks:
      - ledger_entry_count == trace_step_count
      - entry_index / step_index are sequential (0, 1, 2, ...) and matching
      - consumed + skipped + blocked == ledger_entry_count
      - all entries/steps have required fields

    validation_non_binding=True, validation_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        ledger: dict from build_dry_run_ledger() (AC-92).
        trace:  dict from build_audit_trace() (AC-92).

    Returns:
        Replay validation dict.
    """
    if not isinstance(ledger, dict):
        return _rejected_validation("ledger is not a dict")
    if not isinstance(trace, dict):
        return _rejected_validation("trace is not a dict")
    if "ledger_status" not in ledger:
        return _rejected_validation("ledger missing ledger_status")
    if "trace_status" not in trace:
        return _rejected_validation("trace missing trace_status")

    l_status = ledger.get("ledger_status", "")
    t_status = trace.get("trace_status", "")

    # Hold states → safe hold
    if l_status in (_LEDGER_HOLD, _LEDGER_REJECTED) or t_status in (_TRACE_HOLD, _TRACE_REJECTED):
        return _hold_validation(
            ledger.get("ledger_entry_count", 0),
            trace.get("trace_step_count", 0),
        )

    entry_count = int(ledger.get("ledger_entry_count", 0))
    step_count  = int(trace.get("trace_step_count", 0))
    entries     = ledger.get("ledger_entries", [])
    steps       = trace.get("trace_steps", [])

    if not isinstance(entries, list):
        entries = []
    if not isinstance(steps, list):
        steps = []

    failures = []

    # CHECK_COUNTS
    if entry_count != step_count:
        failures.append(
            f"count mismatch: ledger_entry_count={entry_count} != trace_step_count={step_count}"
        )

    # CHECK_INDICES
    index_ok = True
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or entry.get("entry_index") != i:
            index_ok = False
            break
    for i, step in enumerate(steps):
        if not isinstance(step, dict) or step.get("step_index") != i:
            index_ok = False
            break
    # Check entry_index == step_index alignment (up to min length)
    for entry, step in zip(entries, steps):
        if (isinstance(entry, dict) and isinstance(step, dict)
                and entry.get("entry_index") != step.get("step_index")):
            index_ok = False
            break
    if not index_ok:
        failures.append("index mismatch: entry_index / step_index not sequential or not aligned")

    # CHECK_CATEGORY_SUM
    consumed_count = int(ledger.get("consumed_count", 0))
    skipped_count  = int(ledger.get("skipped_count", 0))
    blocked_count  = int(ledger.get("blocked_count", 0))
    cat_sum = consumed_count + skipped_count + blocked_count
    if cat_sum != entry_count:
        failures.append(
            f"category sum mismatch: consumed({consumed_count})+skipped({skipped_count})"
            f"+blocked({blocked_count})={cat_sum} != entry_count={entry_count}"
        )

    # CHECK_FIELDS
    missing_fields = False
    for entry in entries:
        if not isinstance(entry, dict) or not _REQUIRED_ENTRY_FIELDS.issubset(entry.keys()):
            missing_fields = True
            break
    for step in steps:
        if not isinstance(step, dict) or not _REQUIRED_STEP_FIELDS.issubset(step.keys()):
            missing_fields = True
            break
    if missing_fields:
        failures.append("missing required fields in ledger entries or trace steps")

    # Determine result
    passed       = len(failures) == 0
    replay_ok    = passed and (entry_count == step_count)
    reason_code  = REASON_VAL_PASSED if passed else _first_failure_code(failures)
    reason       = (
        f"validation passed: {entry_count} entries/{step_count} steps consistent"
        if passed
        else "; ".join(failures)
    )

    return {
        "validation_status":          VALIDATION_PASSED if passed else VALIDATION_FAILED,
        "validation_mode":            VAL_MODE_OK if passed else VAL_MODE_ERROR,
        "validation_passed":          passed,
        "validation_reason":          reason,
        "validation_reason_code":     reason_code,
        "validated_ledger_count":     entry_count,
        "validated_trace_count":      step_count,
        "replay_consistent":          replay_ok,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _first_failure_code(failures: list) -> str:
    if not failures:
        return REASON_VAL_PASSED
    f = failures[0]
    if "count mismatch" in f:
        return REASON_VAL_COUNT_MISMATCH
    if "index mismatch" in f:
        return REASON_VAL_INDEX_MISMATCH
    if "category sum" in f:
        return REASON_VAL_CAT_MISMATCH
    if "missing required" in f:
        return REASON_VAL_FIELDS
    return REASON_VAL_COUNT_MISMATCH


# ---------------------------------------------------------------------------
# Core handoff consistency check (pure, no I/O)
# ---------------------------------------------------------------------------

def build_handoff_consistency_check(
    queen_handoff:  object,
    runner_intake:  object,
    ledger:         object,
    trace:          object = None,
) -> dict:
    """
    Cross-layer count reconciliation: queen handoff → runner intake → ledger.

    Checks:
      - handoff.total_intents == intake.consumed_intent_count
      - handoff.total_allowed == intake.consumed_allowed_count
      - handoff.total_blocked == intake.consumed_blocked_count
      - intake.consumed_allowed_count == ledger.consumed_count + ledger.skipped_count
      - intake.consumed_blocked_count == ledger.blocked_count
      - ledger.ledger_entry_count == trace.trace_step_count (if trace provided)

    consistency_non_binding=True, consistency_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        queen_handoff: dict from build_queen_handoff() (AC-90).
        runner_intake: dict from build_runner_intake() (AC-91).
        ledger:        dict from build_dry_run_ledger() (AC-92).
        trace:         optional dict from build_audit_trace() (AC-92).

    Returns:
        Handoff consistency check dict.
    """
    # Input validation
    if not isinstance(queen_handoff, dict):
        return _rejected_consistency("queen_handoff is not a dict")
    if not isinstance(runner_intake, dict):
        return _rejected_consistency("runner_intake is not a dict")
    if not isinstance(ledger, dict):
        return _rejected_consistency("ledger is not a dict")

    # Hold state detection
    ho_status  = queen_handoff.get("handoff_status", "")
    int_status = runner_intake.get("runner_intake_status", "")
    led_status = ledger.get("ledger_status", "")

    _hold_statuses = {"HOLD_BASELINE_HANDOFF", "INTAKE_HOLD", "LEDGER_HOLD"}
    if ho_status in _hold_statuses or int_status in _hold_statuses or led_status in _hold_statuses:
        return _hold_consistency()

    # Extract counts
    ho_total   = int(queen_handoff.get("total_intents", 0))
    ho_allowed = int(queen_handoff.get("total_allowed", 0))
    ho_blocked = int(queen_handoff.get("total_blocked", 0))

    in_total   = int(runner_intake.get("consumed_intent_count", 0))
    in_allowed = int(runner_intake.get("consumed_allowed_count", 0))
    in_blocked = int(runner_intake.get("consumed_blocked_count", 0))

    led_entries  = int(ledger.get("ledger_entry_count", 0))
    led_consumed = int(ledger.get("consumed_count", 0))
    led_skipped  = int(ledger.get("skipped_count", 0))
    led_blocked  = int(ledger.get("blocked_count", 0))

    failures  = []
    matched   = 0
    total_checks = 5 + (1 if trace is not None else 0)

    # CROSS_TOTAL_INTENTS
    if ho_total == in_total:
        matched += 1
    else:
        failures.append(f"total_intents: handoff={ho_total} != intake={in_total}")

    # CROSS_ALLOWED
    if ho_allowed == in_allowed:
        matched += 1
    else:
        failures.append(f"total_allowed: handoff={ho_allowed} != intake={in_allowed}")

    # CROSS_BLOCKED
    if ho_blocked == in_blocked:
        matched += 1
    else:
        failures.append(f"total_blocked: handoff={ho_blocked} != intake={in_blocked}")

    # CROSS_ALLOWED_LEDGER
    in_allowed_vs_ledger = led_consumed + led_skipped
    if in_allowed == in_allowed_vs_ledger:
        matched += 1
    else:
        failures.append(
            f"allowed vs ledger: intake_allowed={in_allowed} != "
            f"ledger(consumed+skipped)={in_allowed_vs_ledger}"
        )

    # CROSS_BLOCKED_LEDGER
    if in_blocked == led_blocked:
        matched += 1
    else:
        failures.append(
            f"blocked vs ledger: intake_blocked={in_blocked} != ledger_blocked={led_blocked}"
        )

    # CROSS_TRACE (optional)
    if trace is not None:
        if isinstance(trace, dict):
            tr_steps = int(trace.get("trace_step_count", 0))
            if led_entries == tr_steps:
                matched += 1
            else:
                failures.append(
                    f"ledger_entry_count={led_entries} != trace_step_count={tr_steps}"
                )

    # Counts
    missing_in_handoff = max(0, in_total - ho_total)
    missing_in_ledger  = max(0, in_total - led_entries)
    missing_in_trace   = 0
    if trace is not None and isinstance(trace, dict):
        tr_steps = int(trace.get("trace_step_count", 0))
        missing_in_trace = max(0, led_entries - tr_steps)

    passed      = len(failures) == 0
    reason_code = REASON_CON_MATCHED if passed else _first_con_code(failures)
    reason      = (
        f"consistency passed: {matched}/{total_checks} checks matched"
        if passed
        else "; ".join(failures)
    )

    return {
        "handoff_consistency_status":  CONSISTENCY_PASSED if passed else CONSISTENCY_FAILED,
        "handoff_consistency_mode":    CON_MODE_OK if passed else CON_MODE_ERROR,
        "handoff_consistency_passed":  passed,
        "consistency_reason":          reason,
        "consistency_reason_code":     reason_code,
        "matched_intent_count":        matched,
        "missing_in_handoff_count":    missing_in_handoff,
        "missing_in_ledger_count":     missing_in_ledger,
        "missing_in_trace_count":      missing_in_trace,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _first_con_code(failures: list) -> str:
    if not failures:
        return REASON_CON_MATCHED
    f = failures[0]
    if "total_intents" in f:
        return REASON_CON_TOTAL_MISMATCH
    if "total_allowed" in f:
        return REASON_CON_ALLOWED_MISMATCH
    if "total_blocked" in f:
        return REASON_CON_BLOCKED_MISMATCH
    if "allowed vs ledger" in f:
        return REASON_CON_ALLOWED_LEDGER
    if "blocked vs ledger" in f:
        return REASON_CON_BLOCKED_LEDGER
    if "trace_step_count" in f:
        return REASON_CON_TRACE_MISMATCH
    return REASON_CON_TOTAL_MISMATCH


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_validation(reason: str) -> dict:
    return {
        "validation_status":          VALIDATION_REJECTED,
        "validation_mode":            VAL_MODE_REJECTED,
        "validation_passed":          False,
        "validation_reason":          reason,
        "validation_reason_code":     REASON_VAL_INVALID,
        "validated_ledger_count":     0,
        "validated_trace_count":      0,
        "replay_consistent":          False,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _hold_validation(ledger_count: int = 0, trace_count: int = 0) -> dict:
    return {
        "validation_status":          VALIDATION_HOLD,
        "validation_mode":            VAL_MODE_BASELINE,
        "validation_passed":          False,
        "validation_reason":          "ledger or trace is hold — validation held at baseline",
        "validation_reason_code":     REASON_VAL_HOLD,
        "validated_ledger_count":     ledger_count,
        "validated_trace_count":      trace_count,
        "replay_consistent":          False,
        "validation_non_binding":     True,
        "validation_simulation_only": True,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _rejected_consistency(reason: str) -> dict:
    return {
        "handoff_consistency_status":  CONSISTENCY_REJECTED,
        "handoff_consistency_mode":    CON_MODE_REJECTED,
        "handoff_consistency_passed":  False,
        "consistency_reason":          reason,
        "consistency_reason_code":     REASON_CON_INVALID,
        "matched_intent_count":        0,
        "missing_in_handoff_count":    0,
        "missing_in_ledger_count":     0,
        "missing_in_trace_count":      0,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


def _hold_consistency() -> dict:
    return {
        "handoff_consistency_status":  CONSISTENCY_HOLD,
        "handoff_consistency_mode":    CON_MODE_BASELINE,
        "handoff_consistency_passed":  False,
        "consistency_reason":          "upstream layers in hold state — consistency held at baseline",
        "consistency_reason_code":     REASON_CON_HOLD,
        "matched_intent_count":        0,
        "missing_in_handoff_count":    0,
        "missing_in_ledger_count":     0,
        "missing_in_trace_count":      0,
        "consistency_non_binding":     True,
        "consistency_simulation_only": True,
        "paper_only":                  True,
        "live_activation_allowed":     False,
    }


# ---------------------------------------------------------------------------
# Convenience: validation + consistency combined
# ---------------------------------------------------------------------------

def build_validation_and_consistency(
    ledger:        object,
    trace:         object,
    queen_handoff: object = None,
    runner_intake: object = None,
) -> dict:
    """
    Build replay validation and handoff consistency check in one call.

    Args:
        ledger:        dict from build_dry_run_ledger() (AC-92).
        trace:         dict from build_audit_trace() (AC-92).
        queen_handoff: optional dict from build_queen_handoff() (AC-90).
        runner_intake: optional dict from build_runner_intake() (AC-91).

    Returns:
        Dict with keys: replay_validation, handoff_consistency.
    """
    validation = build_replay_validator(ledger, trace)

    if queen_handoff is not None and runner_intake is not None:
        consistency = build_handoff_consistency_check(
            queen_handoff, runner_intake, ledger, trace
        )
    else:
        consistency = _hold_consistency()

    return {
        "replay_validation":    validation,
        "handoff_consistency":  consistency,
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_ledger_module():
    path = Path(__file__).parent / "build_dry_run_ledger_lite.py"
    spec = importlib.util.spec_from_file_location("_ledger", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-92 + AC-93
# ---------------------------------------------------------------------------

def build_validation_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → … → ledger + trace (AC-92)
                             → replay validation + consistency (AC-93).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit, queen_handoff,
        runner_intake, dry_run_consumption,
        execution_ledger, audit_trace,
        replay_validation, handoff_consistency.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _ledger_mod = _load_ledger_module()
    pipeline    = _ledger_mod.build_ledger_trace_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    validation  = build_replay_validator(
        pipeline["execution_ledger"],
        pipeline["audit_trace"],
    )
    consistency = build_handoff_consistency_check(
        pipeline["queen_handoff"],
        pipeline["runner_intake"],
        pipeline["execution_ledger"],
        pipeline["audit_trace"],
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
        "replay_validation":        validation,
        "handoff_consistency":      consistency,
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

    result = build_validation_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    v = result["replay_validation"]
    c = result["handoff_consistency"]

    print(json.dumps({
        "validation_status":          v["validation_status"],
        "validation_mode":            v["validation_mode"],
        "validation_passed":          v["validation_passed"],
        "validation_reason_code":     v["validation_reason_code"],
        "validated_ledger_count":     v["validated_ledger_count"],
        "validated_trace_count":      v["validated_trace_count"],
        "replay_consistent":          v["replay_consistent"],
        "validation_non_binding":     v["validation_non_binding"],
        "validation_simulation_only": v["validation_simulation_only"],
        "paper_only":                 v["paper_only"],
        "live_activation_allowed":    v["live_activation_allowed"],
        "handoff_consistency_status": c["handoff_consistency_status"],
        "handoff_consistency_passed": c["handoff_consistency_passed"],
        "consistency_reason_code":    c["consistency_reason_code"],
        "matched_intent_count":       c["matched_intent_count"],
        "missing_in_handoff_count":   c["missing_in_handoff_count"],
        "missing_in_ledger_count":    c["missing_in_ledger_count"],
        "missing_in_trace_count":     c["missing_in_trace_count"],
        "consistency_non_binding":    c["consistency_non_binding"],
        "consistency_simulation_only": c["consistency_simulation_only"],
    }, indent=2))


if __name__ == "__main__":
    main()
