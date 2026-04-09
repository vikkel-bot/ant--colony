"""
AC-76: Queen Advisory Intake (Passive)

Provides a formal, passive intake layer for the queen to consume the advisory
produced by build_allocation_memory_policy_simulation_lite.py (AC-74/AC-75).

Design principles:
  - Strictly passive and non-binding: no allocation or execution changes.
  - intake_simulation_only=True is always set on every output.
  - Fail-closed: any validation error -> INTAKE_INVALID -> passive hold.
  - Contract version mismatch -> INTAKE_INVALID -> passive hold.
  - queen_intake_ready=False -> INTAKE_INVALID.
  - Pure core function (consume_advisory) — no I/O, no side effects.
  - Optional main() for file-based intake observability.

Usage (importable):
    from queen_advisory_intake_lite import consume_advisory
    result = consume_advisory(advisory_dict)

Usage (CLI):
    python queen_advisory_intake_lite.py --sim-out <sim_output.json>

Intake output fields:
    intake_status                      — "INTAKE_ACTIVE" | "INTAKE_HOLD" | "INTAKE_INVALID"
    intake_valid                       — bool
    intake_reason                      — human-readable explanation
    intake_reason_code                 — machine-stable code
    consumed_advisory_scenario_id      — advisory_scenario_id (or "baseline")
    would_consider_variant             — bool (True only when INTAKE_ACTIVE)
    intake_simulation_only             — always True
    consumed_advisory_status           — pass-through from advisory
    consumed_advisory_action           — pass-through from advisory
    consumed_advisory_confidence       — pass-through from advisory (or None if invalid)
    consumed_advisory_reason_code      — pass-through from advisory
    consumed_queen_intake_contract_version — pass-through from advisory
"""
import json
from pathlib import Path

VERSION = "queen_advisory_intake_v1"

# The contract version this intake layer understands.
# Must match QUEEN_INTAKE_CONTRACT_VERSION from build_allocation_memory_policy_simulation_lite.
EXPECTED_CONTRACT_VERSION: str = "v1"

# Valid advisory field values
_VALID_ADVISORY_STATUSES: frozenset = frozenset({"ADVISORY_ACTIVE", "BASELINE_HOLD"})
_VALID_ADVISORY_ACTIONS: frozenset  = frozenset({"CONSIDER_VARIANT", "KEEP_CURRENT_POLICY"})

# All 7 fields required by the AC-75 queen intake contract
_REQUIRED_FIELDS: tuple = (
    "advisory_status",
    "advisory_action",
    "advisory_scenario_id",
    "advisory_confidence",
    "advisory_reason_code",
    "queen_intake_ready",
    "queen_intake_contract_version",
)

# Machine-stable intake reason codes
INTAKE_REASON_CODES: dict = {
    # Valid paths
    "ACTIVE_OK":                 "ACTIVE_INTAKE_OK",
    "HOLD_OK":                   "HOLD_INTAKE_OK",
    # Validation failures (all map to passive hold)
    "INVALID_INPUT":             "HOLD_INVALID_INPUT",
    "MISSING_FIELD":             "HOLD_MISSING_FIELD",
    "CONTRACT_VERSION_MISMATCH": "HOLD_CONTRACT_VERSION_MISMATCH",
    "INVALID_STATUS":            "HOLD_INVALID_STATUS",
    "INVALID_ACTION":            "HOLD_INVALID_ACTION",
    "INVALID_CONFIDENCE":        "HOLD_INVALID_CONFIDENCE",
    "NOT_INTAKE_READY":          "HOLD_NOT_INTAKE_READY",
    "ACTIVE_BUT_NOT_READY":      "HOLD_ACTIVE_BUT_NOT_READY",
}


# ---------------------------------------------------------------------------
# Core intake function (pure, no I/O)
# ---------------------------------------------------------------------------

def consume_advisory(advisory: object) -> dict:
    """
    Passively consume and validate an advisory dict.

    Returns an intake result dict with observability fields.
    This function has NO side effects and NO execution impact.
    intake_simulation_only=True is always set.

    Fail-closed: any validation problem produces INTAKE_INVALID with
    would_consider_variant=False.
    """
    # Step 1: input must be a non-None dict
    if not isinstance(advisory, dict):
        return _invalid_intake(
            code_key="INVALID_INPUT",
            reason="advisory is not a dict",
            advisory=None,
        )

    # Step 2: all required fields must be present
    for field in _REQUIRED_FIELDS:
        if field not in advisory:
            return _invalid_intake(
                code_key="MISSING_FIELD",
                reason=f"missing required field: {field}",
                advisory=advisory,
            )

    # Step 3: contract version must match
    contract_version = advisory["queen_intake_contract_version"]
    if contract_version != EXPECTED_CONTRACT_VERSION:
        return _invalid_intake(
            code_key="CONTRACT_VERSION_MISMATCH",
            reason=(
                f"contract version mismatch:"
                f" expected={EXPECTED_CONTRACT_VERSION}"
                f" got={contract_version}"
            ),
            advisory=advisory,
        )

    # Step 4: advisory_status must be a known value
    status = advisory["advisory_status"]
    if status not in _VALID_ADVISORY_STATUSES:
        return _invalid_intake(
            code_key="INVALID_STATUS",
            reason=f"unknown advisory_status: {status}",
            advisory=advisory,
        )

    # Step 5: advisory_action must be a known value
    action = advisory["advisory_action"]
    if action not in _VALID_ADVISORY_ACTIONS:
        return _invalid_intake(
            code_key="INVALID_ACTION",
            reason=f"unknown advisory_action: {action}",
            advisory=advisory,
        )

    # Step 6: advisory_confidence must be a numeric float in [0.0, 1.0]
    confidence = advisory["advisory_confidence"]
    try:
        confidence_f = float(confidence)
    except (TypeError, ValueError):
        return _invalid_intake(
            code_key="INVALID_CONFIDENCE",
            reason=f"advisory_confidence is not numeric: {confidence!r}",
            advisory=advisory,
        )
    if not (0.0 <= confidence_f <= 1.0):
        return _invalid_intake(
            code_key="INVALID_CONFIDENCE",
            reason=f"advisory_confidence out of range [0,1]: {confidence_f}",
            advisory=advisory,
        )

    # Step 7: queen_intake_ready must be True for ADVISORY_ACTIVE
    intake_ready = advisory["queen_intake_ready"]
    if status == "ADVISORY_ACTIVE" and intake_ready is not True:
        return _invalid_intake(
            code_key="ACTIVE_BUT_NOT_READY",
            reason="advisory_status=ADVISORY_ACTIVE but queen_intake_ready is not True",
            advisory=advisory,
        )

    # Step 8: queen_intake_ready=False on a non-ACTIVE advisory is unusual but not invalid;
    # we accept BASELINE_HOLD regardless of intake_ready value — it's already a hold.
    if intake_ready is False and status != "ADVISORY_ACTIVE":
        # BASELINE_HOLD with intake_ready=False — valid hold, pass through
        pass

    # ---------------------------------------------------------------------------
    # All validations passed — determine intake outcome
    # ---------------------------------------------------------------------------
    scenario_id = str(advisory.get("advisory_scenario_id") or "baseline")
    reason_code_str = str(advisory.get("advisory_reason_code") or "")

    if status == "ADVISORY_ACTIVE":
        return {
            "intake_status":                       "INTAKE_ACTIVE",
            "intake_valid":                        True,
            "intake_reason":                       (
                f"ADVISORY_ACTIVE|scenario={scenario_id}"
                f"|confidence={confidence_f:.2f}"
                f"|code={reason_code_str}"
            ),
            "intake_reason_code":                  INTAKE_REASON_CODES["ACTIVE_OK"],
            "consumed_advisory_scenario_id":       scenario_id,
            "would_consider_variant":              True,
            "intake_simulation_only":              True,
            "consumed_advisory_status":            status,
            "consumed_advisory_action":            action,
            "consumed_advisory_confidence":        confidence_f,
            "consumed_advisory_reason_code":       reason_code_str,
            "consumed_queen_intake_contract_version": contract_version,
        }

    # BASELINE_HOLD (valid)
    return {
        "intake_status":                       "INTAKE_HOLD",
        "intake_valid":                        True,
        "intake_reason":                       (
            f"BASELINE_HOLD|scenario={scenario_id}"
            f"|code={reason_code_str}"
        ),
        "intake_reason_code":                  INTAKE_REASON_CODES["HOLD_OK"],
        "consumed_advisory_scenario_id":       scenario_id,
        "would_consider_variant":              False,
        "intake_simulation_only":              True,
        "consumed_advisory_status":            status,
        "consumed_advisory_action":            action,
        "consumed_advisory_confidence":        confidence_f,
        "consumed_advisory_reason_code":       reason_code_str,
        "consumed_queen_intake_contract_version": contract_version,
    }


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _invalid_intake(code_key: str, reason: str, advisory: object) -> dict:
    """Build a fail-closed INTAKE_INVALID result. No execution impact."""
    # Extract pass-through fields safely (advisory may be None or non-dict)
    def _safe_get(key, default=""):
        if isinstance(advisory, dict):
            return advisory.get(key, default)
        return default

    return {
        "intake_status":                       "INTAKE_INVALID",
        "intake_valid":                        False,
        "intake_reason":                       reason,
        "intake_reason_code":                  INTAKE_REASON_CODES.get(code_key, "HOLD_UNKNOWN"),
        "consumed_advisory_scenario_id":       str(_safe_get("advisory_scenario_id") or "baseline"),
        "would_consider_variant":              False,
        "intake_simulation_only":              True,
        "consumed_advisory_status":            _safe_get("advisory_status"),
        "consumed_advisory_action":            _safe_get("advisory_action"),
        "consumed_advisory_confidence":        None,
        "consumed_advisory_reason_code":       _safe_get("advisory_reason_code"),
        "consumed_queen_intake_contract_version": _safe_get("queen_intake_contract_version"),
    }


# ---------------------------------------------------------------------------
# File-based intake (optional main — purely observational)
# ---------------------------------------------------------------------------

def _load_advisory_from_sim_output(sim_path: Path) -> tuple:
    """
    Load advisory dict from simulation output JSON.
    Returns (advisory: dict | None, error: str | None).
    """
    if not sim_path.exists():
        return None, f"SIM_OUTPUT_NOT_FOUND:{sim_path}"
    try:
        data = json.loads(sim_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return None, f"PARSE_ERROR:{exc}"
    advisory = data.get("allocation_advisory")
    if advisory is None:
        return None, "MISSING_allocation_advisory_KEY"
    if not isinstance(advisory, dict):
        return None, "allocation_advisory_NOT_DICT"
    return advisory, None


def main() -> None:
    import argparse
    from datetime import datetime, timezone

    ap = argparse.ArgumentParser(
        description="Queen advisory intake (passive, simulation-only)"
    )
    ap.add_argument(
        "--sim-out",
        type=Path,
        default=Path(r"C:\Trading\ANT_OUT\allocation_memory_policy_simulation.json"),
        help="Path to simulation output JSON containing allocation_advisory",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional: write intake result JSON to this path",
    )
    args = ap.parse_args()

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    advisory, load_err = _load_advisory_from_sim_output(args.sim_out)

    if load_err:
        intake = _invalid_intake(
            code_key="INVALID_INPUT",
            reason=f"could not load advisory: {load_err}",
            advisory=None,
        )
    else:
        intake = consume_advisory(advisory)

    out = {
        "component":   "queen_advisory_intake_lite",
        "version":     VERSION,
        "ts_utc":      ts_utc,
        "source":      str(args.sim_out),
        "intake":      intake,
    }

    print(json.dumps(out, indent=2))

    if args.out:
        tmp = str(args.out) + ".tmp"
        try:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            import os
            os.replace(tmp, str(args.out))
            print(f"[OK] intake result written to {args.out}")
        except Exception as e:
            print(f"[WARN] could not write intake result: {e}")


if __name__ == "__main__":
    main()
