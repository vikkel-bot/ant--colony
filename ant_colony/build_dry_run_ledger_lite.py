"""
AC-92: Dry-Run Execution Ledger + Replayable Audit Trace

Two layers on top of the AC-91 dry-run consumption:

1. Dry-Run Execution Ledger
   Builds a compact, ordered record of what the dry-run consumed,
   skipped, and blocked — one ledger entry per intent, with a stable
   index and category tag. No execution, no state mutation.

2. Replayable Audit Trace
   Lifts the ledger into a replayable trace: same ordered entries
   with explicit replay metadata (step_index, replay_safe=True).
   Supports future audit/replay without any live coupling.

Design principles:
  - ledger_non_binding=True always.
  - ledger_simulation_only=True always.
  - trace_non_binding=True always.
  - trace_simulation_only=True always.
  - paper_only=True always on all outputs.
  - live_activation_allowed=False always — never overridden.
  - replayable=True always on trace outputs.
  - Fail-closed: invalid/missing input → LEDGER_REJECTED / TRACE_REJECTED.
  - Deterministic: same dry-run consumption → same ledger and trace.
  - Pure core functions — no I/O, no side effects.
  - No broker coupling, no live execution path, no portfolio mutation.

Ledger ordering convention (stable, deterministic):
  1. CONSUMED entries (INCREASE / DECREASE) — actions the runner would take
  2. SKIPPED entries (HOLD) — no-ops the runner would pass
  3. BLOCKED entries — intents that could not be processed

Ledger status values:
  LEDGER_COMPLETE  — dry-run was DRY_RUN_COMPLETE; ledger built
  LEDGER_HOLD      — dry-run was DRY_RUN_HOLD; ledger held at baseline
  LEDGER_REJECTED  — invalid/missing input or DRY_RUN_REJECTED

Ledger mode values:
  LEDGER_READY     — ledger built; ready for trace
  LEDGER_BASELINE  — baseline hold; empty ledger
  LEDGER_REJECTED  — rejected; ledger closed

Ledger reason codes:
  LEDGER_OK                — ledger built with ≥1 entry
  LEDGER_EMPTY_DRY_RUN     — ledger built but zero entries (all intents absent)
  LEDGER_HOLD_BASELINE     — dry-run was hold
  LEDGER_INVALID_INPUT     — input not a valid dry-run consumption dict

Per-ledger-entry fields:
  entry_index      — int: 0-based position in ledger
  ledger_category  — "CONSUMED" | "SKIPPED" | "BLOCKED"
  market           — str (empty if not available)
  intent_action    — str
  intent_status    — str
  delta_eur        — float
  paper_only       — always True

Trace status values:
  TRACE_COMPLETE   — ledger was LEDGER_COMPLETE; trace built
  TRACE_HOLD       — ledger was hold; trace held at baseline
  TRACE_REJECTED   — invalid/missing input or LEDGER_REJECTED

Trace mode values:
  TRACE_READY      — trace built; ready for audit/replay
  TRACE_BASELINE   — baseline hold; empty trace
  TRACE_REJECTED   — rejected; trace closed

Trace reason codes:
  TRACE_OK               — trace built with ≥1 step
  TRACE_EMPTY_LEDGER     — trace built but zero steps
  TRACE_HOLD_BASELINE    — ledger was hold
  TRACE_INVALID_INPUT    — input not a valid ledger dict

Per-trace-step fields:
  step_index       — int: 0-based; matches entry_index
  step_category    — str: CONSUMED / SKIPPED / BLOCKED
  step_market      — str (empty if not available)
  step_action      — str: intent_action of the ledger entry
  step_delta_eur   — float
  replay_safe      — always True (paper-only simulation)
  paper_only       — always True

Usage (importable):
    from build_dry_run_ledger_lite import build_dry_run_ledger
    ledger = build_dry_run_ledger(dry_run_consumption)

    from build_dry_run_ledger_lite import build_audit_trace
    trace = build_audit_trace(ledger)

    # Combined:
    from build_dry_run_ledger_lite import build_ledger_and_trace
    result = build_ledger_and_trace(dry_run_consumption)

    # Full chain from specs:
    from build_dry_run_ledger_lite import build_ledger_trace_from_specs
    result = build_ledger_trace_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "dry_run_ledger_v1"

# ---------------------------------------------------------------------------
# Ledger status / mode / reason codes
# ---------------------------------------------------------------------------

LEDGER_COMPLETE = "LEDGER_COMPLETE"
LEDGER_HOLD     = "LEDGER_HOLD"
LEDGER_REJECTED = "LEDGER_REJECTED"

LEDGER_MODE_READY    = "LEDGER_READY"
LEDGER_MODE_BASELINE = "LEDGER_BASELINE"
LEDGER_MODE_REJECTED = "LEDGER_REJECTED"

REASON_LEDGER_OK      = "LEDGER_OK"
REASON_LEDGER_EMPTY   = "LEDGER_EMPTY_DRY_RUN"
REASON_LEDGER_HOLD    = "LEDGER_HOLD_BASELINE"
REASON_LEDGER_INVALID = "LEDGER_INVALID_INPUT"

# ---------------------------------------------------------------------------
# Trace status / mode / reason codes
# ---------------------------------------------------------------------------

TRACE_COMPLETE = "TRACE_COMPLETE"
TRACE_HOLD     = "TRACE_HOLD"
TRACE_REJECTED = "TRACE_REJECTED"

TRACE_MODE_READY    = "TRACE_READY"
TRACE_MODE_BASELINE = "TRACE_BASELINE"
TRACE_MODE_REJECTED = "TRACE_REJECTED"

REASON_TRACE_OK      = "TRACE_OK"
REASON_TRACE_EMPTY   = "TRACE_EMPTY_LEDGER"
REASON_TRACE_HOLD    = "TRACE_HOLD_BASELINE"
REASON_TRACE_INVALID = "TRACE_INVALID_INPUT"

# Ledger entry categories
CAT_CONSUMED = "CONSUMED"
CAT_SKIPPED  = "SKIPPED"
CAT_BLOCKED  = "BLOCKED"

# Dry-run status mirrors (AC-91)
_DR_COMPLETE = "DRY_RUN_COMPLETE"
_DR_HOLD     = "DRY_RUN_HOLD"
_DR_REJECTED = "DRY_RUN_REJECTED"


# ---------------------------------------------------------------------------
# Core ledger function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_dry_run_ledger(dry_run_consumption: object) -> dict:
    """
    Build a dry-run execution ledger from an AC-91 dry-run consumption.

    Produces one ledger entry per intent in stable order:
    consumed (INCREASE/DECREASE) → skipped (HOLD) → blocked.

    ledger_non_binding=True, ledger_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        dry_run_consumption: dict from build_dry_run_consumption() (AC-91).

    Returns:
        Dry-run execution ledger dict.
    """
    if not isinstance(dry_run_consumption, dict):
        return _rejected_ledger("dry_run_consumption is not a dict")

    if "dry_run_status" not in dry_run_consumption:
        return _rejected_ledger("dry_run_consumption missing dry_run_status")

    dr_status = dry_run_consumption.get("dry_run_status", "")

    if dr_status == _DR_REJECTED:
        return _rejected_ledger("dry_run_consumption status is DRY_RUN_REJECTED")

    if dr_status == _DR_HOLD:
        return _hold_ledger()

    # DRY_RUN_COMPLETE — build ledger entries
    consumed_list = dry_run_consumption.get("dry_run_consumed_intents", [])
    skipped_list  = dry_run_consumption.get("dry_run_skipped_intents",  [])
    blocked_list  = dry_run_consumption.get("dry_run_blocked_intents",  [])

    if not isinstance(consumed_list, list):
        consumed_list = []
    if not isinstance(skipped_list, list):
        skipped_list = []
    if not isinstance(blocked_list, list):
        blocked_list = []

    entries = []
    idx = 0

    for item in consumed_list:
        entries.append(_ledger_entry(idx, CAT_CONSUMED, item))
        idx += 1
    for item in skipped_list:
        entries.append(_ledger_entry(idx, CAT_SKIPPED, item))
        idx += 1
    for item in blocked_list:
        entries.append(_ledger_entry(idx, CAT_BLOCKED, item))
        idx += 1

    entry_count    = len(entries)
    consumed_count = len(consumed_list)
    skipped_count  = len(skipped_list)
    blocked_count  = len(blocked_list)

    reason_code = REASON_LEDGER_OK if entry_count > 0 else REASON_LEDGER_EMPTY
    reason = (
        f"ledger built: {consumed_count} consumed, "
        f"{skipped_count} skipped, {blocked_count} blocked"
    )

    return {
        "ledger_status":          LEDGER_COMPLETE,
        "ledger_mode":            LEDGER_MODE_READY,
        "ledger_entries":         entries,
        "ledger_entry_count":     entry_count,
        "ledger_reason":          reason,
        "ledger_reason_code":     reason_code,
        "consumed_count":         consumed_count,
        "skipped_count":          skipped_count,
        "blocked_count":          blocked_count,
        "ledger_non_binding":     True,
        "ledger_simulation_only": True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Core trace function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_audit_trace(ledger: object) -> dict:
    """
    Build a replayable audit trace from a dry-run execution ledger.

    Each trace step mirrors its ledger entry with explicit replay
    metadata. replayable=True always. Order is identical to ledger.

    trace_non_binding=True, trace_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        ledger: dict from build_dry_run_ledger() (AC-92).

    Returns:
        Replayable audit trace dict.
    """
    if not isinstance(ledger, dict):
        return _rejected_trace("ledger is not a dict")

    if "ledger_status" not in ledger:
        return _rejected_trace("ledger missing ledger_status")

    l_status = ledger.get("ledger_status", "")

    if l_status == LEDGER_REJECTED:
        return _rejected_trace("ledger status is LEDGER_REJECTED")

    if l_status == LEDGER_HOLD:
        return _hold_trace()

    # LEDGER_COMPLETE — build trace steps
    entries = ledger.get("ledger_entries", [])
    if not isinstance(entries, list):
        entries = []

    steps = [_trace_step(entry) for entry in entries if isinstance(entry, dict)]

    step_count  = len(steps)
    reason_code = REASON_TRACE_OK if step_count > 0 else REASON_TRACE_EMPTY
    reason = (
        f"trace built: {step_count} step(s) — "
        f"{ledger.get('consumed_count', 0)} consumed, "
        f"{ledger.get('skipped_count', 0)} skipped, "
        f"{ledger.get('blocked_count', 0)} blocked"
    )

    return {
        "trace_status":           TRACE_COMPLETE,
        "trace_mode":             TRACE_MODE_READY,
        "trace_steps":            steps,
        "trace_step_count":       step_count,
        "trace_reason":           reason,
        "trace_reason_code":      reason_code,
        "replayable":             True,
        "trace_non_binding":      True,
        "trace_simulation_only":  True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Entry / step builders
# ---------------------------------------------------------------------------

def _ledger_entry(index: int, category: str, intent: object) -> dict:
    if not isinstance(intent, dict):
        return {
            "entry_index":    index,
            "ledger_category": category,
            "market":         "",
            "intent_action":  "",
            "intent_status":  "BLOCKED",
            "delta_eur":      0.0,
            "paper_only":     True,
        }
    return {
        "entry_index":    index,
        "ledger_category": category,
        "market":         str(intent.get("market", "")),
        "intent_action":  str(intent.get("intent_action", "")),
        "intent_status":  str(intent.get("intent_status", "")),
        "delta_eur":      _safe_float(intent.get("delta_eur"), 0.0),
        "paper_only":     True,
    }


def _trace_step(entry: dict) -> dict:
    return {
        "step_index":    int(entry.get("entry_index", 0)),
        "step_category": str(entry.get("ledger_category", "")),
        "step_market":   str(entry.get("market", "")),
        "step_action":   str(entry.get("intent_action", "")),
        "step_delta_eur": _safe_float(entry.get("delta_eur"), 0.0),
        "replay_safe":   True,
        "paper_only":    True,
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_ledger(reason: str) -> dict:
    return {
        "ledger_status":          LEDGER_REJECTED,
        "ledger_mode":            LEDGER_MODE_REJECTED,
        "ledger_entries":         [],
        "ledger_entry_count":     0,
        "ledger_reason":          reason,
        "ledger_reason_code":     REASON_LEDGER_INVALID,
        "consumed_count":         0,
        "skipped_count":          0,
        "blocked_count":          0,
        "ledger_non_binding":     True,
        "ledger_simulation_only": True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _hold_ledger() -> dict:
    return {
        "ledger_status":          LEDGER_HOLD,
        "ledger_mode":            LEDGER_MODE_BASELINE,
        "ledger_entries":         [],
        "ledger_entry_count":     0,
        "ledger_reason":          "dry-run was held — ledger held at baseline",
        "ledger_reason_code":     REASON_LEDGER_HOLD,
        "consumed_count":         0,
        "skipped_count":          0,
        "blocked_count":          0,
        "ledger_non_binding":     True,
        "ledger_simulation_only": True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _rejected_trace(reason: str) -> dict:
    return {
        "trace_status":           TRACE_REJECTED,
        "trace_mode":             TRACE_MODE_REJECTED,
        "trace_steps":            [],
        "trace_step_count":       0,
        "trace_reason":           reason,
        "trace_reason_code":      REASON_TRACE_INVALID,
        "replayable":             True,
        "trace_non_binding":      True,
        "trace_simulation_only":  True,
        "paper_only":             True,
        "live_activation_allowed": False,
    }


def _hold_trace() -> dict:
    return {
        "trace_status":           TRACE_HOLD,
        "trace_mode":             TRACE_MODE_BASELINE,
        "trace_steps":            [],
        "trace_step_count":       0,
        "trace_reason":           "ledger was held — trace held at baseline",
        "trace_reason_code":      REASON_TRACE_HOLD,
        "replayable":             True,
        "trace_non_binding":      True,
        "trace_simulation_only":  True,
        "paper_only":             True,
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
# Convenience: ledger + trace combined
# ---------------------------------------------------------------------------

def build_ledger_and_trace(dry_run_consumption: object) -> dict:
    """
    Build execution ledger and audit trace in one call.

    Args:
        dry_run_consumption: dict from build_dry_run_consumption() (AC-91).

    Returns:
        Dict with keys: execution_ledger, audit_trace.
    """
    ledger = build_dry_run_ledger(dry_run_consumption)
    trace  = build_audit_trace(ledger)
    return {
        "execution_ledger": ledger,
        "audit_trace":      trace,
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_intake_module():
    path = Path(__file__).parent / "build_paper_runner_intake_lite.py"
    spec = importlib.util.spec_from_file_location("_intake", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-91 + AC-92
# ---------------------------------------------------------------------------

def build_ledger_trace_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → … → queen handoff (AC-90)
                             → runner intake + dry-run (AC-91)
                             → execution ledger + audit trace (AC-92).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope,
        regime_overlay, allocation_proposal, conflict_selection,
        allocation_candidate, paper_transition_preview,
        intent_pack, transition_audit, queen_handoff,
        runner_intake, dry_run_consumption,
        execution_ledger, audit_trace.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _intake_mod = _load_intake_module()
    pipeline    = _intake_mod.build_dry_run_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    ledger = build_dry_run_ledger(pipeline["dry_run_consumption"])
    trace  = build_audit_trace(ledger)
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
        "execution_ledger":         ledger,
        "audit_trace":              trace,
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

    result = build_ledger_trace_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    ledger = result["execution_ledger"]
    trace  = result["audit_trace"]

    print(json.dumps({
        "ledger_status":          ledger["ledger_status"],
        "ledger_mode":            ledger["ledger_mode"],
        "ledger_entry_count":     ledger["ledger_entry_count"],
        "ledger_reason_code":     ledger["ledger_reason_code"],
        "consumed_count":         ledger["consumed_count"],
        "skipped_count":          ledger["skipped_count"],
        "blocked_count":          ledger["blocked_count"],
        "ledger_non_binding":     ledger["ledger_non_binding"],
        "ledger_simulation_only": ledger["ledger_simulation_only"],
        "paper_only":             ledger["paper_only"],
        "live_activation_allowed": ledger["live_activation_allowed"],
        "trace_status":           trace["trace_status"],
        "trace_mode":             trace["trace_mode"],
        "trace_step_count":       trace["trace_step_count"],
        "trace_reason_code":      trace["trace_reason_code"],
        "replayable":             trace["replayable"],
        "trace_non_binding":      trace["trace_non_binding"],
        "trace_simulation_only":  trace["trace_simulation_only"],
        "trace_steps":            trace["trace_steps"],
    }, indent=2))


if __name__ == "__main__":
    main()
