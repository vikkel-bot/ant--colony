"""
AC-79: Queen Decision Envelope Observability + Portfolio Preview Hook

Provides a non-binding portfolio preview hook that makes the AC-78 decision
envelope visible at portfolio level. Shows what the portfolio *would* do
under a given envelope — purely for observability and auditability.

Full advisory pipeline (end-to-end):
  advisory dict
    → consume_advisory()              [AC-76: intake + contract validation]
    → interpret_intake()              [AC-77: interpretation + preview]
    → build_decision_envelope()       [AC-78: aggregation + decision envelope]
    → build_portfolio_preview()       [AC-79: envelope observability + portfolio hook]

Design principles:
  - portfolio_preview_non_binding=True always.
  - portfolio_preview_simulation_only=True always.
  - Fail-closed: invalid/missing envelope → REJECT_ENVELOPE.
  - Deterministic: same envelope → same portfolio preview every call.
  - Pure core function (build_portfolio_preview) — no I/O, no side effects.
  - No actual portfolio state is read, modified, or executed.
  - Optional main() for file-based observability output.

Three portfolio preview paths:
  VARIANT_CONSIDERATION envelope → preview_portfolio_action=CONSIDER_VARIANT_ALLOCATION
  BASELINE_HOLD envelope         → preview_portfolio_action=HOLD_BASELINE_ALLOCATION
  ENVELOPE_REJECTED / invalid    → preview_portfolio_action=REJECT_ENVELOPE

Usage (importable):
    from queen_portfolio_preview_lite import build_portfolio_preview
    portfolio_preview = build_portfolio_preview(envelope)

    # Full pipeline in one step:
    from queen_portfolio_preview_lite import build_portfolio_preview_from_advisory
    result = build_portfolio_preview_from_advisory(advisory_dict)
    # result keys: intake, preview, envelope, portfolio_preview

Usage (CLI):
    python queen_portfolio_preview_lite.py --sim-out <sim_output.json>

Portfolio preview output fields:
    envelope_observed                 — decision_envelope_status from envelope
    envelope_status                   — same (stable alias)
    preview_portfolio_action          — "CONSIDER_VARIANT_ALLOCATION" |
                                        "HOLD_BASELINE_ALLOCATION" |
                                        "REJECT_ENVELOPE"
    preview_portfolio_target          — selected_variant_id from envelope, or "baseline"
    preview_portfolio_reason          — human-readable explanation
    preview_portfolio_reason_code     — machine-stable code
    preview_portfolio_confidence      — float [0.0, 1.0] from envelope
    portfolio_preview_non_binding     — always True
    portfolio_preview_simulation_only — always True
"""
import json
from pathlib import Path

VERSION = "queen_portfolio_preview_v1"

# Stable portfolio preview action values
PORTFOLIO_ACTION_CONSIDER_VARIANT = "CONSIDER_VARIANT_ALLOCATION"
PORTFOLIO_ACTION_HOLD_BASELINE    = "HOLD_BASELINE_ALLOCATION"
PORTFOLIO_ACTION_REJECT_ENVELOPE  = "REJECT_ENVELOPE"

# Machine-stable portfolio preview reason codes
PORTFOLIO_PREVIEW_REASON_CODES: dict = {
    # Forward paths
    "CONSIDER_VARIANT_OK": "PORTFOLIO_PREVIEW_VARIANT_CONSIDERATION_OK",
    "HOLD_BASELINE_OK":    "PORTFOLIO_PREVIEW_BASELINE_HOLD_OK",
    # Reject paths
    "REJECT_ENVELOPE":     "PORTFOLIO_PREVIEW_REJECTED_ENVELOPE",
    "REJECT_BAD_INPUT":    "PORTFOLIO_PREVIEW_REJECTED_BAD_INPUT",
    "REJECT_MISSING_FIELD":"PORTFOLIO_PREVIEW_REJECTED_MISSING_FIELD",
    "REJECT_UNKNOWN":      "PORTFOLIO_PREVIEW_REJECTED_UNKNOWN_STATUS",
}

# Required fields in the envelope result (AC-78 output)
_ENVELOPE_REQUIRED_FIELDS: tuple = (
    "decision_envelope_status",
    "selected_variant_id",
    "decision_confidence",
    "envelope_non_binding",
    "envelope_simulation_only",
)

# Envelope status values from AC-78
_ENV_VARIANT_CONSIDERATION = "VARIANT_CONSIDERATION"
_ENV_BASELINE_HOLD         = "BASELINE_HOLD"
_ENV_REJECTED              = "ENVELOPE_REJECTED"


# ---------------------------------------------------------------------------
# Core portfolio preview function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_portfolio_preview(envelope: object) -> dict:
    """
    Build a non-binding portfolio preview from an AC-78 decision envelope.

    This function has NO side effects and NO execution impact.
    portfolio_preview_non_binding=True and portfolio_preview_simulation_only=True
    are always set.

    Args:
        envelope: dict returned by queen_decision_envelope_lite.build_decision_envelope()

    Returns:
        Portfolio preview dict.
    """
    # Step 1: envelope must be a non-None dict
    if not isinstance(envelope, dict):
        return _reject_portfolio_preview(
            code_key="REJECT_BAD_INPUT",
            reason="envelope is not a dict",
            envelope=None,
        )

    # Step 2: required fields must be present
    for field in _ENVELOPE_REQUIRED_FIELDS:
        if field not in envelope:
            return _reject_portfolio_preview(
                code_key="REJECT_MISSING_FIELD",
                reason=f"envelope missing required field: {field}",
                envelope=envelope,
            )

    env_status  = envelope["decision_envelope_status"]
    variant_id  = str(envelope.get("selected_variant_id") or "baseline")
    confidence  = _safe_float(envelope.get("decision_confidence"), 0.0)

    # Step 3: route by envelope status
    if env_status == _ENV_VARIANT_CONSIDERATION:
        return {
            "envelope_observed":                 env_status,
            "envelope_status":                   env_status,
            "preview_portfolio_action":          PORTFOLIO_ACTION_CONSIDER_VARIANT,
            "preview_portfolio_target":          variant_id,
            "preview_portfolio_reason":          (
                f"VARIANT_CONSIDERATION|target={variant_id}"
                f"|confidence={confidence:.2f}"
            ),
            "preview_portfolio_reason_code":     PORTFOLIO_PREVIEW_REASON_CODES["CONSIDER_VARIANT_OK"],
            "preview_portfolio_confidence":      confidence,
            "portfolio_preview_non_binding":     True,
            "portfolio_preview_simulation_only": True,
        }

    if env_status == _ENV_BASELINE_HOLD:
        return {
            "envelope_observed":                 env_status,
            "envelope_status":                   env_status,
            "preview_portfolio_action":          PORTFOLIO_ACTION_HOLD_BASELINE,
            "preview_portfolio_target":          "baseline",
            "preview_portfolio_reason":          (
                f"BASELINE_HOLD|target=baseline"
                f"|confidence={confidence:.2f}"
            ),
            "preview_portfolio_reason_code":     PORTFOLIO_PREVIEW_REASON_CODES["HOLD_BASELINE_OK"],
            "preview_portfolio_confidence":      confidence,
            "portfolio_preview_non_binding":     True,
            "portfolio_preview_simulation_only": True,
        }

    if env_status == _ENV_REJECTED:
        return _reject_portfolio_preview(
            code_key="REJECT_ENVELOPE",
            reason=f"envelope rejected: {envelope.get('decision_reason', 'unknown')}",
            envelope=envelope,
        )

    # Unknown envelope status — fail-closed
    return _reject_portfolio_preview(
        code_key="REJECT_UNKNOWN",
        reason=f"unknown decision_envelope_status: {env_status!r}",
        envelope=envelope,
    )


# ---------------------------------------------------------------------------
# Convenience full-pipeline function
# ---------------------------------------------------------------------------

def build_portfolio_preview_from_advisory(advisory: object) -> dict:
    """
    Full pipeline: advisory → intake → preview → envelope → portfolio preview.

    Returns a dict with keys: intake, preview, envelope, portfolio_preview.
    All layers are simulation-only and non-binding.
    """
    import importlib.util as _ilu

    _base = Path(__file__).parent
    _env_path = _base / "queen_decision_envelope_lite.py"
    _spec = _ilu.spec_from_file_location("_env", _env_path)
    _mod  = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)

    result   = _mod.build_envelope_from_advisory(advisory)
    portfolio_preview = build_portfolio_preview(result["envelope"])

    return {
        "intake":            result["intake"],
        "preview":           result["preview"],
        "envelope":          result["envelope"],
        "portfolio_preview": portfolio_preview,
    }


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


def _reject_portfolio_preview(code_key: str, reason: str, envelope: object) -> dict:
    """Build a fail-closed REJECT_ENVELOPE portfolio preview. No execution impact."""
    def _safe_get(key, default=""):
        if isinstance(envelope, dict):
            return envelope.get(key, default)
        return default

    env_status = str(_safe_get("decision_envelope_status") or "")

    return {
        "envelope_observed":                 env_status,
        "envelope_status":                   env_status,
        "preview_portfolio_action":          PORTFOLIO_ACTION_REJECT_ENVELOPE,
        "preview_portfolio_target":          "baseline",
        "preview_portfolio_reason":          reason,
        "preview_portfolio_reason_code":     PORTFOLIO_PREVIEW_REASON_CODES.get(
                                                 code_key, "PORTFOLIO_PREVIEW_REJECTED_UNKNOWN"
                                             ),
        "preview_portfolio_confidence":      0.0,
        "portfolio_preview_non_binding":     True,
        "portfolio_preview_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# File-based portfolio preview (optional main — purely observational)
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
        description="Queen portfolio preview hook (passive, simulation-only)"
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
        help="Optional: write portfolio preview JSON to this path",
    )
    args = ap.parse_args()

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    advisory, load_err = _load_advisory_from_sim_output(args.sim_out)

    if load_err:
        # Build a rejected portfolio preview directly from load failure
        portfolio_preview = _reject_portfolio_preview(
            code_key="REJECT_BAD_INPUT",
            reason=f"could not load advisory: {load_err}",
            envelope=None,
        )
        result = {
            "intake": None, "preview": None, "envelope": None,
            "portfolio_preview": portfolio_preview,
        }
    else:
        result = build_portfolio_preview_from_advisory(advisory)

    out = {
        "component":        "queen_portfolio_preview_lite",
        "version":          VERSION,
        "ts_utc":           ts_utc,
        "source":           str(args.sim_out),
        "intake":           result["intake"],
        "preview":          result["preview"],
        "envelope":         result["envelope"],
        "portfolio_preview": result["portfolio_preview"],
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
            print(f"[OK] portfolio preview written to {args.out}")
        except Exception as e:
            print(f"[WARN] could not write portfolio preview: {e}")


if __name__ == "__main__":
    main()
