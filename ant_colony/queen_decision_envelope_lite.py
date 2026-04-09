"""
AC-78: Queen Advisory Preview Aggregation + Decision Envelope

Builds a formal, compact decision envelope from the AC-77 preview result.
This is the final observability layer in the advisory pipeline — the queen
shows what it *would* decide, without any allocation, execution, or policy
changes.

Decision pipeline (end-to-end):
  advisory dict
    → consume_advisory()            [AC-76: intake + contract validation]
    → interpret_intake()            [AC-77: interpretation + preview]
    → build_decision_envelope()     [AC-78: aggregation + decision envelope]

Design principles:
  - envelope_non_binding=True always.
  - envelope_simulation_only=True always.
  - Fail-closed: invalid/missing preview → ENVELOPE_REJECTED.
  - Deterministic: same preview → same envelope every call.
  - Pure core function (build_decision_envelope) — no I/O, no side effects.
  - Optional main() for file-based decision envelope observability.

Three envelope paths:
  CONSIDER_VARIANT preview  → decision_envelope_status=VARIANT_CONSIDERATION
  HOLD_BASELINE preview     → decision_envelope_status=BASELINE_HOLD
  REJECT_INTAKE preview     → decision_envelope_status=ENVELOPE_REJECTED

Usage (importable):
    from queen_decision_envelope_lite import build_decision_envelope
    envelope = build_decision_envelope(preview)

    # Full pipeline in one step:
    from queen_decision_envelope_lite import build_envelope_from_advisory
    result = build_envelope_from_advisory(advisory_dict)
    # result keys: intake, preview, envelope

Usage (CLI):
    python queen_decision_envelope_lite.py --sim-out <sim_output.json>

Envelope output fields:
    decision_envelope_status  — "VARIANT_CONSIDERATION" | "BASELINE_HOLD" | "ENVELOPE_REJECTED"
    decision_mode             — "ADVISORY_VARIANT" | "ADVISORY_HOLD" | "ADVISORY_REJECTED"
    decision_reason           — human-readable explanation
    decision_reason_code      — machine-stable code
    selected_variant_id       — scenario_id for VARIANT_CONSIDERATION, "baseline" otherwise
    selected_action           — "CONSIDER_VARIANT" | "HOLD_BASELINE" | "REJECT"
    decision_confidence       — float [0.0, 1.0] from preview
    envelope_non_binding      — always True
    envelope_simulation_only  — always True
"""
import json
from pathlib import Path

VERSION = "queen_decision_envelope_v1"

# Stable envelope status values
ENVELOPE_VARIANT_CONSIDERATION = "VARIANT_CONSIDERATION"
ENVELOPE_BASELINE_HOLD         = "BASELINE_HOLD"
ENVELOPE_REJECTED               = "ENVELOPE_REJECTED"

# Stable decision_mode values
MODE_ADVISORY_VARIANT  = "ADVISORY_VARIANT"
MODE_ADVISORY_HOLD     = "ADVISORY_HOLD"
MODE_ADVISORY_REJECTED = "ADVISORY_REJECTED"

# Selected action values
ACTION_CONSIDER_VARIANT = "CONSIDER_VARIANT"
ACTION_HOLD_BASELINE    = "HOLD_BASELINE"
ACTION_REJECT           = "REJECT"

# Machine-stable decision reason codes
DECISION_REASON_CODES: dict = {
    # Forward paths
    "VARIANT_OK":           "ENVELOPE_VARIANT_CONSIDERATION_OK",
    "HOLD_OK":              "ENVELOPE_BASELINE_HOLD_OK",
    # Reject paths
    "REJECT_PREVIEW":       "ENVELOPE_REJECTED_PREVIEW_REJECT",
    "REJECT_BAD_INPUT":     "ENVELOPE_REJECTED_BAD_INPUT",
    "REJECT_MISSING_FIELD": "ENVELOPE_REJECTED_MISSING_FIELD",
    "REJECT_UNKNOWN_STATUS":"ENVELOPE_REJECTED_UNKNOWN_STATUS",
}

# Required fields in the preview result (AC-77 output)
_PREVIEW_REQUIRED_FIELDS: tuple = (
    "interpretation_status",
    "preview_action",
    "preview_variant_id",
    "preview_confidence",
    "preview_non_binding",
    "preview_simulation_only",
)

# Preview interpretation status values from AC-77
_PREVIEW_CONSIDER_VARIANT = "CONSIDER_VARIANT"
_PREVIEW_HOLD_BASELINE    = "HOLD_BASELINE"
_PREVIEW_REJECT_INTAKE    = "REJECT_INTAKE"


# ---------------------------------------------------------------------------
# Core envelope function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_decision_envelope(preview: object) -> dict:
    """
    Build a decision envelope from an AC-77 preview result.

    This function has NO side effects and NO execution impact.
    envelope_non_binding=True and envelope_simulation_only=True always set.

    Args:
        preview: dict returned by queen_advisory_interpretation_lite.interpret_intake()

    Returns:
        Decision envelope dict.
    """
    # Step 1: preview must be a non-None dict
    if not isinstance(preview, dict):
        return _rejected_envelope(
            code_key="REJECT_BAD_INPUT",
            reason="preview is not a dict",
            preview=None,
        )

    # Step 2: required fields must be present
    for field in _PREVIEW_REQUIRED_FIELDS:
        if field not in preview:
            return _rejected_envelope(
                code_key="REJECT_MISSING_FIELD",
                reason=f"preview missing required field: {field}",
                preview=preview,
            )

    interp_status = preview["interpretation_status"]
    variant_id    = str(preview.get("preview_variant_id") or "baseline")
    confidence    = _safe_float(preview.get("preview_confidence"), 0.0)

    # Step 3: route by interpretation_status
    if interp_status == _PREVIEW_CONSIDER_VARIANT:
        return {
            "decision_envelope_status": ENVELOPE_VARIANT_CONSIDERATION,
            "decision_mode":            MODE_ADVISORY_VARIANT,
            "decision_reason":          (
                f"CONSIDER_VARIANT|variant={variant_id}"
                f"|confidence={confidence:.2f}"
            ),
            "decision_reason_code":     DECISION_REASON_CODES["VARIANT_OK"],
            "selected_variant_id":      variant_id,
            "selected_action":          ACTION_CONSIDER_VARIANT,
            "decision_confidence":      confidence,
            "envelope_non_binding":     True,
            "envelope_simulation_only": True,
        }

    if interp_status == _PREVIEW_HOLD_BASELINE:
        return {
            "decision_envelope_status": ENVELOPE_BASELINE_HOLD,
            "decision_mode":            MODE_ADVISORY_HOLD,
            "decision_reason":          (
                f"HOLD_BASELINE|variant={variant_id}"
                f"|confidence={confidence:.2f}"
            ),
            "decision_reason_code":     DECISION_REASON_CODES["HOLD_OK"],
            "selected_variant_id":      "baseline",
            "selected_action":          ACTION_HOLD_BASELINE,
            "decision_confidence":      confidence,
            "envelope_non_binding":     True,
            "envelope_simulation_only": True,
        }

    if interp_status == _PREVIEW_REJECT_INTAKE:
        return _rejected_envelope(
            code_key="REJECT_PREVIEW",
            reason=f"preview rejected: {preview.get('interpretation_reason', 'unknown')}",
            preview=preview,
        )

    # Unknown interpretation_status — fail-closed
    return _rejected_envelope(
        code_key="REJECT_UNKNOWN_STATUS",
        reason=f"unknown interpretation_status: {interp_status!r}",
        preview=preview,
    )


# ---------------------------------------------------------------------------
# Convenience full-pipeline function
# ---------------------------------------------------------------------------

def build_envelope_from_advisory(advisory: object) -> dict:
    """
    Full pipeline: advisory → intake → preview → decision envelope.

    Returns a dict with keys: intake, preview, envelope.
    All three layers are simulation-only and non-binding.
    """
    import importlib.util as _ilu

    _base = Path(__file__).parent

    # Load AC-77 interpretation module (which in turn loads AC-76 intake)
    _interp_path = _base / "queen_advisory_interpretation_lite.py"
    _spec = _ilu.spec_from_file_location("_interp", _interp_path)
    _mod  = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)

    pipeline = _mod.interpret_advisory(advisory)   # returns {intake, preview}
    intake   = pipeline["intake"]
    preview  = pipeline["preview"]
    envelope = build_decision_envelope(preview)

    return {"intake": intake, "preview": preview, "envelope": envelope}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _rejected_envelope(code_key: str, reason: str, preview: object) -> dict:
    """Build a fail-closed ENVELOPE_REJECTED dict. No execution impact."""
    def _safe_get(key, default=""):
        if isinstance(preview, dict):
            return preview.get(key, default)
        return default

    variant_id = str(_safe_get("preview_variant_id") or "baseline")

    return {
        "decision_envelope_status": ENVELOPE_REJECTED,
        "decision_mode":            MODE_ADVISORY_REJECTED,
        "decision_reason":          reason,
        "decision_reason_code":     DECISION_REASON_CODES.get(code_key, "ENVELOPE_REJECTED_UNKNOWN"),
        "selected_variant_id":      "baseline",
        "selected_action":          ACTION_REJECT,
        "decision_confidence":      0.0,
        "envelope_non_binding":     True,
        "envelope_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# File-based envelope (optional main — purely observational)
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
        description="Queen decision envelope (passive, simulation-only)"
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
        help="Optional: write decision envelope JSON to this path",
    )
    args = ap.parse_args()

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    advisory, load_err = _load_advisory_from_sim_output(args.sim_out)

    if load_err:
        # Build a rejected envelope directly from load failure
        fake_preview = {
            "interpretation_status":      "REJECT_INTAKE",
            "interpretation_reason":      f"could not load advisory: {load_err}",
            "interpretation_reason_code": "REJECT_BAD_INTAKE_INPUT",
            "interpreted_scenario_id":    "baseline",
            "interpreted_advisory_action": "",
            "preview_action":             "REJECT_INTAKE",
            "preview_variant_id":         "baseline",
            "preview_confidence":         0.0,
            "preview_non_binding":        True,
            "preview_simulation_only":    True,
        }
        envelope = build_decision_envelope(fake_preview)
        intake   = None
        preview  = fake_preview
    else:
        result   = build_envelope_from_advisory(advisory)
        intake   = result["intake"]
        preview  = result["preview"]
        envelope = result["envelope"]

    out = {
        "component":   "queen_decision_envelope_lite",
        "version":     VERSION,
        "ts_utc":      ts_utc,
        "source":      str(args.sim_out),
        "intake":      intake,
        "preview":     preview,
        "envelope":    envelope,
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
            print(f"[OK] decision envelope written to {args.out}")
        except Exception as e:
            print(f"[WARN] could not write envelope: {e}")


if __name__ == "__main__":
    main()
