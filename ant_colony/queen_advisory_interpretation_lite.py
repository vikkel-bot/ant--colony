"""
AC-77: Queen Advisory Interpretation + Non-Binding Allocation Preview

Builds a non-binding allocation preview on top of the AC-76 intake result.
The queen uses this to show *what it would consider doing* — strictly for
observability; no allocation, execution, or policy changes are made.

Design principles:
  - preview_non_binding=True always.
  - preview_simulation_only=True always.
  - Fail-closed: invalid/missing intake → REJECT_INTAKE preview.
  - Deterministic: same intake → same preview every call.
  - Pure core function (interpret_intake) — no I/O, no side effects.
  - Optional main() for file-based preview observability.

Three interpretation paths:
  INTAKE_ACTIVE  → interpretation_status=CONSIDER_VARIANT
  INTAKE_HOLD    → interpretation_status=HOLD_BASELINE
  INTAKE_INVALID → interpretation_status=REJECT_INTAKE

Usage (importable):
    from queen_advisory_interpretation_lite import interpret_intake
    preview = interpret_intake(intake_result)

    # Or run the full pipeline in one step:
    from queen_advisory_intake_lite import consume_advisory
    intake = consume_advisory(advisory_dict)
    preview = interpret_intake(intake)

Usage (CLI):
    python queen_advisory_interpretation_lite.py --sim-out <sim_output.json>

Preview output fields:
    interpretation_status      — "CONSIDER_VARIANT" | "HOLD_BASELINE" | "REJECT_INTAKE"
    interpretation_reason      — human-readable explanation
    interpretation_reason_code — machine-stable code
    interpreted_scenario_id    — scenario_id from intake (or "baseline")
    interpreted_advisory_action— advisory_action from intake (or empty string)
    preview_action             — same as interpretation_status
    preview_variant_id         — scenario_id when CONSIDER_VARIANT, else "baseline"
    preview_confidence         — float from intake (or 0.0 if unavailable)
    preview_non_binding        — always True
    preview_simulation_only    — always True
"""
import json
from pathlib import Path

VERSION = "queen_advisory_interpretation_v1"

# Stable interpretation status values
INTERPRETATION_CONSIDER_VARIANT = "CONSIDER_VARIANT"
INTERPRETATION_HOLD_BASELINE     = "HOLD_BASELINE"
INTERPRETATION_REJECT_INTAKE     = "REJECT_INTAKE"

# Machine-stable reason codes
INTERPRETATION_REASON_CODES: dict = {
    # Valid forward paths
    "CONSIDER_VARIANT_OK":     "CONSIDER_VARIANT_INTAKE_ACTIVE",
    "HOLD_BASELINE_OK":        "HOLD_BASELINE_INTAKE_HOLD",
    # Reject paths
    "REJECT_INTAKE_INVALID":   "REJECT_INTAKE_INVALID",
    "REJECT_INTAKE_NOT_READY": "REJECT_INTAKE_NOT_READY",
    "REJECT_BAD_INPUT":        "REJECT_BAD_INTAKE_INPUT",
    "REJECT_UNKNOWN_STATUS":   "REJECT_UNKNOWN_INTAKE_STATUS",
}

# Required fields in the intake result (AC-76 output)
_INTAKE_REQUIRED_FIELDS: tuple = (
    "intake_status",
    "intake_valid",
    "would_consider_variant",
    "consumed_advisory_scenario_id",
    "consumed_advisory_action",
    "consumed_advisory_confidence",
)


# ---------------------------------------------------------------------------
# Core interpretation function (pure, no I/O)
# ---------------------------------------------------------------------------

def interpret_intake(intake: object) -> dict:
    """
    Interpret an AC-76 intake result and produce a non-binding allocation preview.

    This function has NO side effects and NO execution impact.
    preview_non_binding=True and preview_simulation_only=True are always set.

    Args:
        intake: dict returned by queen_advisory_intake_lite.consume_advisory()

    Returns:
        Preview dict with interpretation_status and all preview fields.
    """
    # Step 1: intake must be a non-None dict
    if not isinstance(intake, dict):
        return _reject_preview(
            code_key="REJECT_BAD_INPUT",
            reason="intake is not a dict",
            intake=None,
        )

    # Step 2: required fields must be present
    for field in _INTAKE_REQUIRED_FIELDS:
        if field not in intake:
            return _reject_preview(
                code_key="REJECT_BAD_INPUT",
                reason=f"intake missing required field: {field}",
                intake=intake,
            )

    intake_status    = intake["intake_status"]
    intake_valid     = intake["intake_valid"]
    would_consider   = intake["would_consider_variant"]
    scenario_id      = str(intake.get("consumed_advisory_scenario_id") or "baseline")
    advisory_action  = str(intake.get("consumed_advisory_action") or "")
    raw_conf         = intake.get("consumed_advisory_confidence")
    confidence       = _safe_float(raw_conf, 0.0)

    # Step 3: route by intake_status
    if intake_status == "INTAKE_ACTIVE":
        if not would_consider:
            # Defensive: INTAKE_ACTIVE should always have would_consider=True; treat mismatch as reject
            return _reject_preview(
                code_key="REJECT_INTAKE_NOT_READY",
                reason="INTAKE_ACTIVE but would_consider_variant is not True",
                intake=intake,
            )
        return {
            "interpretation_status":      INTERPRETATION_CONSIDER_VARIANT,
            "interpretation_reason":      (
                f"INTAKE_ACTIVE|scenario={scenario_id}"
                f"|confidence={confidence:.2f}"
                f"|action={advisory_action}"
            ),
            "interpretation_reason_code": INTERPRETATION_REASON_CODES["CONSIDER_VARIANT_OK"],
            "interpreted_scenario_id":    scenario_id,
            "interpreted_advisory_action": advisory_action,
            "preview_action":             INTERPRETATION_CONSIDER_VARIANT,
            "preview_variant_id":         scenario_id,
            "preview_confidence":         confidence,
            "preview_non_binding":        True,
            "preview_simulation_only":    True,
        }

    if intake_status == "INTAKE_HOLD":
        return {
            "interpretation_status":      INTERPRETATION_HOLD_BASELINE,
            "interpretation_reason":      (
                f"INTAKE_HOLD|scenario={scenario_id}"
                f"|action={advisory_action}"
            ),
            "interpretation_reason_code": INTERPRETATION_REASON_CODES["HOLD_BASELINE_OK"],
            "interpreted_scenario_id":    scenario_id,
            "interpreted_advisory_action": advisory_action,
            "preview_action":             INTERPRETATION_HOLD_BASELINE,
            "preview_variant_id":         "baseline",
            "preview_confidence":         confidence,
            "preview_non_binding":        True,
            "preview_simulation_only":    True,
        }

    if intake_status == "INTAKE_INVALID":
        return _reject_preview(
            code_key="REJECT_INTAKE_INVALID",
            reason=f"intake invalid: {intake.get('intake_reason', 'unknown')}",
            intake=intake,
        )

    # Unknown intake_status — fail-closed
    return _reject_preview(
        code_key="REJECT_UNKNOWN_STATUS",
        reason=f"unknown intake_status: {intake_status!r}",
        intake=intake,
    )


# ---------------------------------------------------------------------------
# Convenience pipeline: advisory dict → intake → preview
# ---------------------------------------------------------------------------

def interpret_advisory(advisory: object) -> dict:
    """
    Full pipeline: advisory dict → intake → interpretation preview.

    Convenience wrapper; calls consume_advisory then interpret_intake.
    Returns a dict with both 'intake' and 'preview' keys.
    """
    import importlib.util as _ilu
    _intake_path = Path(__file__).parent / "queen_advisory_intake_lite.py"
    _spec = _ilu.spec_from_file_location("_intake", _intake_path)
    _mod  = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)

    intake  = _mod.consume_advisory(advisory)
    preview = interpret_intake(intake)
    return {"intake": intake, "preview": preview}


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _reject_preview(code_key: str, reason: str, intake: object) -> dict:
    """Build a fail-closed REJECT_INTAKE preview. No execution impact."""
    def _safe_get(key, default=""):
        if isinstance(intake, dict):
            return intake.get(key, default)
        return default

    scenario_id = str(_safe_get("consumed_advisory_scenario_id") or "baseline")
    advisory_action = str(_safe_get("consumed_advisory_action") or "")

    return {
        "interpretation_status":      INTERPRETATION_REJECT_INTAKE,
        "interpretation_reason":      reason,
        "interpretation_reason_code": INTERPRETATION_REASON_CODES.get(code_key, "REJECT_UNKNOWN"),
        "interpreted_scenario_id":    scenario_id,
        "interpreted_advisory_action": advisory_action,
        "preview_action":             INTERPRETATION_REJECT_INTAKE,
        "preview_variant_id":         "baseline",
        "preview_confidence":         0.0,
        "preview_non_binding":        True,
        "preview_simulation_only":    True,
    }


# ---------------------------------------------------------------------------
# File-based interpretation (optional main — purely observational)
# ---------------------------------------------------------------------------

def _load_advisory_from_sim_output(sim_path: Path) -> tuple:
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
        description="Queen advisory interpretation + non-binding preview (simulation-only)"
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
        help="Optional: write interpretation result JSON to this path",
    )
    args = ap.parse_args()

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    advisory, load_err = _load_advisory_from_sim_output(args.sim_out)

    if load_err:
        import importlib.util as _ilu
        _intake_path = Path(__file__).parent / "queen_advisory_intake_lite.py"
        _spec = _ilu.spec_from_file_location("_intake", _intake_path)
        _mod  = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        intake = _mod._invalid_intake(
            code_key="INVALID_INPUT",
            reason=f"could not load advisory: {load_err}",
            advisory=None,
        )
    else:
        pipeline = interpret_advisory(advisory)
        intake   = pipeline["intake"]

    preview = interpret_intake(intake)

    out = {
        "component":       "queen_advisory_interpretation_lite",
        "version":         VERSION,
        "ts_utc":          ts_utc,
        "source":          str(args.sim_out),
        "intake":          intake,
        "preview":         preview,
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
            print(f"[OK] preview written to {args.out}")
        except Exception as e:
            print(f"[WARN] could not write preview: {e}")


if __name__ == "__main__":
    main()
