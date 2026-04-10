"""
AC-98: Anomaly Action Queue (Human Action Triage)

Translates AC-97 anomaly escalation into concrete human triage actions.
No approval workflow. No execution. No state machine. No operator writeback.

Primary driver: anomaly_escalation (AC-97).
Context fields drawn from review_packet (AC-96) and underlying layers.

Design principles:
  - non_binding=True always.
  - simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always.
  - Fail-closed: invalid/missing input → CRITICAL action entry.
  - Deterministic: same inputs → same queue every call.
  - Pure core function — no I/O, no side effects.
  - No new trading logic. No execution coupling. No broker layer.
  - Existing modules (AC-94..97) are read-only — no modification.

Action status values:
  NONE     — no action needed
  INFO     — informational; no human action required
  REVIEW   — human review recommended
  URGENT   — human review required soon
  CRITICAL — immediate human attention required

Action class values:
  NO_ACTION                — nothing to do
  REVIEW_STATUS            — review overall status
  REVIEW_CONFLICT          — layer status conflict detected
  REVIEW_BLOCKING_FINDINGS — blocking findings need human review
  REVIEW_MISSING_INPUT     — required input missing or invalid
  REVIEW_CRITICAL_STATE    — critical system state (rejected/validation/consistency)

Operator action values:
  NONE                       — no operator action needed
  CHECK_SUMMARY              — read human_review_summary
  INSPECT_REVIEW_PACKET      — open review_packet and check findings
  INSPECT_UPSTREAM_SNAPSHOTS — drill into upstream layer snapshots
  VERIFY_MISSING_INPUTS      — identify and supply missing inputs
  ESCALATE_TO_HUMAN_NOW      — escalate immediately to responsible human

Urgency values:
  NONE     → action_status=NONE
  LOW      → action_status=INFO
  MEDIUM   → action_status=REVIEW
  HIGH     → action_status=URGENT
  CRITICAL → action_status=CRITICAL

Recommended window values:
  NONE         — no action window
  NEXT_REVIEW  — handle at next scheduled review
  SOON         — handle before next cycle
  NOW          — handle immediately

Action reason codes:
  ACTION_NONE                     — no anomaly; no action
  ACTION_INFO_LOW                 — informational deviation only
  ACTION_REVIEW_HOLD              — hold/cautious state; review recommended
  ACTION_URGENT_CONFLICT          — layer conflict; urgent inspection
  ACTION_URGENT_BLOCKING_FINDINGS — blocking findings; urgent resolution
  ACTION_URGENT_HIGH_PRIORITY     — high-priority review signal
  ACTION_CRITICAL_MISSING_INPUT   — missing or invalid required input
  ACTION_CRITICAL_STATE           — critical system state (rejected/failed)

Mapping from anomaly_escalation (primary driver):
  NONE     → action_status=NONE,     action_class=NO_ACTION,                 operator_action=NONE
  LOW      → action_status=INFO,     action_class=REVIEW_STATUS,             operator_action=CHECK_SUMMARY
  MEDIUM   → action_status=REVIEW,   action_class=REVIEW_STATUS,             operator_action=INSPECT_REVIEW_PACKET
  HIGH (+ESCALATION_HIGH_LAYER_CONFLICT)      → action_class=REVIEW_CONFLICT
  HIGH (+ESCALATION_HIGH_BLOCKING_FINDINGS)   → action_class=REVIEW_BLOCKING_FINDINGS
  HIGH (+ESCALATION_HIGH_REVIEW_PRIORITY)     → action_class=REVIEW_BLOCKING_FINDINGS
  CRITICAL (+ESCALATION_CRITICAL_INVALID_INPUT / _SNAPSHOT_MISSING) → action_class=REVIEW_MISSING_INPUT
  CRITICAL (other)                            → action_class=REVIEW_CRITICAL_STATE

Output file: C:\\Trading\\ANT_OUT\\anomaly_action_queue.json
"""
from __future__ import annotations
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION   = "anomaly_action_queue_v1"
COMPONENT = "build_anomaly_action_queue_lite"
OUT_DIR   = Path(r"C:\Trading\ANT_OUT")
ACTION_QUEUE_PATH = OUT_DIR / "anomaly_action_queue.json"

# Action status
STATUS_NONE     = "NONE"
STATUS_INFO     = "INFO"
STATUS_REVIEW   = "REVIEW"
STATUS_URGENT   = "URGENT"
STATUS_CRITICAL = "CRITICAL"

# Action class
CLASS_NO_ACTION          = "NO_ACTION"
CLASS_REVIEW_STATUS      = "REVIEW_STATUS"
CLASS_REVIEW_CONFLICT    = "REVIEW_CONFLICT"
CLASS_REVIEW_BLOCKING    = "REVIEW_BLOCKING_FINDINGS"
CLASS_MISSING_INPUT      = "REVIEW_MISSING_INPUT"
CLASS_CRITICAL_STATE     = "REVIEW_CRITICAL_STATE"

# Operator action
OP_NONE             = "NONE"
OP_CHECK_SUMMARY    = "CHECK_SUMMARY"
OP_INSPECT_PACKET   = "INSPECT_REVIEW_PACKET"
OP_INSPECT_UPSTREAM = "INSPECT_UPSTREAM_SNAPSHOTS"
OP_VERIFY_MISSING   = "VERIFY_MISSING_INPUTS"
OP_ESCALATE_NOW     = "ESCALATE_TO_HUMAN_NOW"

# Urgency
URGENCY_NONE     = "NONE"
URGENCY_LOW      = "LOW"
URGENCY_MEDIUM   = "MEDIUM"
URGENCY_HIGH     = "HIGH"
URGENCY_CRITICAL = "CRITICAL"

# Recommended window
WINDOW_NONE        = "NONE"
WINDOW_NEXT_REVIEW = "NEXT_REVIEW"
WINDOW_SOON        = "SOON"
WINDOW_NOW         = "NOW"

# Action reason codes
REASON_NONE                     = "ACTION_NONE"
REASON_INFO_LOW                 = "ACTION_INFO_LOW"
REASON_REVIEW_HOLD              = "ACTION_REVIEW_HOLD"
REASON_URGENT_CONFLICT          = "ACTION_URGENT_CONFLICT"
REASON_URGENT_BLOCKING          = "ACTION_URGENT_BLOCKING_FINDINGS"
REASON_URGENT_HIGH_PRIORITY     = "ACTION_URGENT_HIGH_PRIORITY"
REASON_CRITICAL_MISSING_INPUT   = "ACTION_CRITICAL_MISSING_INPUT"
REASON_CRITICAL_STATE           = "ACTION_CRITICAL_STATE"

# Anomaly level mirrors (AC-97)
_LEVEL_NONE     = "NONE"
_LEVEL_LOW      = "LOW"
_LEVEL_MEDIUM   = "MEDIUM"
_LEVEL_HIGH     = "HIGH"
_LEVEL_CRITICAL = "CRITICAL"

# Escalation reason code mirrors (AC-97)
_ESC_INVALID_INPUT   = "ESCALATION_CRITICAL_INVALID_INPUT"
_ESC_SNAPSHOT        = "ESCALATION_CRITICAL_SNAPSHOT_MISSING"
_ESC_LAYER_CONFLICT  = "ESCALATION_HIGH_LAYER_CONFLICT"
_ESC_BLOCKING        = "ESCALATION_HIGH_BLOCKING_FINDINGS"
_ESC_HIGH_PRIORITY   = "ESCALATION_HIGH_REVIEW_PRIORITY"

# Missing-input escalation codes (all map to REVIEW_MISSING_INPUT)
_MISSING_INPUT_CODES = {_ESC_INVALID_INPUT, _ESC_SNAPSHOT}


# ---------------------------------------------------------------------------
# Core action queue function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_anomaly_action_queue(
    anomaly_escalation:      object,
    review_packet:           object = None,
    promotion_gate:          object = None,
    paper_readiness_dossier: object = None,
    human_review_summary:    object = None,
) -> dict:
    """
    Translate anomaly_escalation (AC-97) into a concrete human action queue.

    anomaly_escalation is the primary driver.
    Other inputs are read-only context for source_context fields.

    No new decision logic. No execution. No state mutation.

    Returns anomaly_action_queue dict.
    """
    ts = _utc_ts()

    # Validate primary input
    if not isinstance(anomaly_escalation, dict):
        return _queue_result(
            ts,
            action_required=True,
            action_status=STATUS_CRITICAL,
            action_class=CLASS_MISSING_INPUT,
            operator_action=OP_VERIFY_MISSING,
            urgency=URGENCY_CRITICAL,
            window=WINDOW_NOW,
            reason="anomaly_escalation is not a dict — cannot build action queue",
            reason_code=REASON_CRITICAL_MISSING_INPUT,
            findings=["anomaly_escalation is not a dict — required input missing"],
            promotion_status="",
            dossier_status="",
            review_status="",
            anomaly_level="",
            human_attention_required=True,
        )

    if "anomaly_level" not in anomaly_escalation:
        return _queue_result(
            ts,
            action_required=True,
            action_status=STATUS_CRITICAL,
            action_class=CLASS_MISSING_INPUT,
            operator_action=OP_VERIFY_MISSING,
            urgency=URGENCY_CRITICAL,
            window=WINDOW_NOW,
            reason="anomaly_escalation missing required field 'anomaly_level'",
            reason_code=REASON_CRITICAL_MISSING_INPUT,
            findings=["anomaly_escalation missing 'anomaly_level' — cannot classify action"],
            promotion_status="",
            dossier_status="",
            review_status="",
            anomaly_level="",
            human_attention_required=True,
        )

    # Extract escalation signals
    level           = str(anomaly_escalation.get("anomaly_level", ""))
    esc_reason_code = str(anomaly_escalation.get("escalation_reason_code", ""))
    esc_findings    = list(anomaly_escalation.get("escalation_findings", []))
    human_attn      = bool(anomaly_escalation.get("human_attention_required", False))

    # Extract context snapshot fields
    promo_status   = str(anomaly_escalation.get("promotion_status", ""))
    dossier_status = str(anomaly_escalation.get("dossier_status", ""))
    review_status  = str(anomaly_escalation.get("review_status", ""))

    # --- NONE ---
    if level == _LEVEL_NONE:
        return _queue_result(
            ts,
            action_required=False,
            action_status=STATUS_NONE,
            action_class=CLASS_NO_ACTION,
            operator_action=OP_NONE,
            urgency=URGENCY_NONE,
            window=WINDOW_NONE,
            reason="no anomaly detected — no operator action required",
            reason_code=REASON_NONE,
            findings=["all layers consistent — no action needed"],
            promotion_status=promo_status,
            dossier_status=dossier_status,
            review_status=review_status,
            anomaly_level=level,
            human_attention_required=False,
        )

    # --- LOW ---
    if level == _LEVEL_LOW:
        action_findings = list(esc_findings) or ["informational deviation — no action needed"]
        return _queue_result(
            ts,
            action_required=False,
            action_status=STATUS_INFO,
            action_class=CLASS_REVIEW_STATUS,
            operator_action=OP_CHECK_SUMMARY,
            urgency=URGENCY_LOW,
            window=WINDOW_NEXT_REVIEW,
            reason="informational anomaly — review summary at next opportunity",
            reason_code=REASON_INFO_LOW,
            findings=action_findings,
            promotion_status=promo_status,
            dossier_status=dossier_status,
            review_status=review_status,
            anomaly_level=level,
            human_attention_required=False,
        )

    # --- MEDIUM ---
    if level == _LEVEL_MEDIUM:
        action_findings = list(esc_findings) or ["hold/cautious state — review recommended"]
        return _queue_result(
            ts,
            action_required=True,
            action_status=STATUS_REVIEW,
            action_class=CLASS_REVIEW_STATUS,
            operator_action=OP_INSPECT_PACKET,
            urgency=URGENCY_MEDIUM,
            window=WINDOW_SOON,
            reason="hold/cautious state — inspect review packet before next cycle",
            reason_code=REASON_REVIEW_HOLD,
            findings=action_findings,
            promotion_status=promo_status,
            dossier_status=dossier_status,
            review_status=review_status,
            anomaly_level=level,
            human_attention_required=True,
        )

    # --- HIGH ---
    if level == _LEVEL_HIGH:
        action_class, reason, reason_code = _classify_high(esc_reason_code, esc_findings)
        action_findings = list(esc_findings) or [f"high-priority anomaly: {esc_reason_code}"]
        return _queue_result(
            ts,
            action_required=True,
            action_status=STATUS_URGENT,
            action_class=action_class,
            operator_action=OP_INSPECT_PACKET,
            urgency=URGENCY_HIGH,
            window=WINDOW_NOW,
            reason=reason,
            reason_code=reason_code,
            findings=action_findings,
            promotion_status=promo_status,
            dossier_status=dossier_status,
            review_status=review_status,
            anomaly_level=level,
            human_attention_required=True,
        )

    # --- CRITICAL ---
    if level == _LEVEL_CRITICAL:
        action_class, operator_action, reason, reason_code = _classify_critical(
            esc_reason_code, esc_findings
        )
        action_findings = list(esc_findings) or [f"critical anomaly: {esc_reason_code}"]
        return _queue_result(
            ts,
            action_required=True,
            action_status=STATUS_CRITICAL,
            action_class=action_class,
            operator_action=operator_action,
            urgency=URGENCY_CRITICAL,
            window=WINDOW_NOW,
            reason=reason,
            reason_code=reason_code,
            findings=action_findings,
            promotion_status=promo_status,
            dossier_status=dossier_status,
            review_status=review_status,
            anomaly_level=level,
            human_attention_required=True,
        )

    # --- Unknown level — fail-closed → CRITICAL ---
    return _queue_result(
        ts,
        action_required=True,
        action_status=STATUS_CRITICAL,
        action_class=CLASS_MISSING_INPUT,
        operator_action=OP_VERIFY_MISSING,
        urgency=URGENCY_CRITICAL,
        window=WINDOW_NOW,
        reason=f"unrecognised anomaly_level={level!r} — fail-closed to CRITICAL",
        reason_code=REASON_CRITICAL_MISSING_INPUT,
        findings=[f"unrecognised anomaly_level={level!r} — cannot classify action"],
        promotion_status=promo_status,
        dossier_status=dossier_status,
        review_status=review_status,
        anomaly_level=level,
        human_attention_required=True,
    )


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

def _classify_high(
    esc_reason_code: str,
    esc_findings:    list,
) -> tuple[str, str, str]:
    """Return (action_class, reason, reason_code) for HIGH anomaly."""
    if esc_reason_code == _ESC_LAYER_CONFLICT:
        return (
            CLASS_REVIEW_CONFLICT,
            "layer status conflict detected — inspect review packet and upstream snapshots",
            REASON_URGENT_CONFLICT,
        )
    if esc_reason_code in (_ESC_BLOCKING, _ESC_HIGH_PRIORITY):
        return (
            CLASS_REVIEW_BLOCKING,
            "blocking findings present — inspect review packet and resolve before proceeding",
            REASON_URGENT_BLOCKING,
        )
    # fallback for unknown HIGH reason code (fail-closed to conflict)
    return (
        CLASS_REVIEW_CONFLICT,
        f"high-priority anomaly ({esc_reason_code}) — inspect review packet",
        REASON_URGENT_HIGH_PRIORITY,
    )


def _classify_critical(
    esc_reason_code: str,
    esc_findings:    list,
) -> tuple[str, str, str, str]:
    """Return (action_class, operator_action, reason, reason_code) for CRITICAL anomaly."""
    if esc_reason_code in _MISSING_INPUT_CODES:
        return (
            CLASS_MISSING_INPUT,
            OP_VERIFY_MISSING,
            "required inputs missing or invalid — verify all pipeline inputs",
            REASON_CRITICAL_MISSING_INPUT,
        )
    # All other CRITICAL codes: validation/consistency/promotion/dossier failures
    if esc_reason_code in (
        "ESCALATION_CRITICAL_VALIDATION_FAILED",
        "ESCALATION_CRITICAL_CONSISTENCY_FAILED",
    ):
        return (
            CLASS_CRITICAL_STATE,
            OP_INSPECT_UPSTREAM,
            f"critical validation/consistency failure ({esc_reason_code}) — inspect upstream snapshots",
            REASON_CRITICAL_STATE,
        )
    # Promotion or dossier rejected → escalate
    return (
        CLASS_CRITICAL_STATE,
        OP_ESCALATE_NOW,
        f"critical system state ({esc_reason_code}) — escalate to human immediately",
        REASON_CRITICAL_STATE,
    )


# ---------------------------------------------------------------------------
# Result builder
# ---------------------------------------------------------------------------

def _queue_result(
    ts:                       str,
    action_required:          bool,
    action_status:            str,
    action_class:             str,
    operator_action:          str,
    urgency:                  str,
    window:                   str,
    reason:                   str,
    reason_code:              str,
    findings:                 list,
    promotion_status:         str,
    dossier_status:           str,
    review_status:            str,
    anomaly_level:            str,
    human_attention_required: bool,
) -> dict:
    return {
        "version":              VERSION,
        "component":            COMPONENT,
        "ts_utc":               ts,
        "action_required":      action_required,
        "action_status":        action_status,
        "action_class":         action_class,
        "operator_action":      operator_action,
        "urgency":              urgency,
        "recommended_window":   window,
        "reason":               reason,
        "reason_code":          reason_code,
        "source_context": {
            "promotion_status":         promotion_status,
            "dossier_status":           dossier_status,
            "review_status":            review_status,
            "anomaly_level":            anomaly_level,
            "human_attention_required": human_attention_required,
        },
        "findings": {
            "action_findings": list(findings),
        },
        "flags": {
            "non_binding":             True,
            "simulation_only":         True,
            "paper_only":              True,
            "live_activation_allowed": False,
        },
    }


def _utc_ts() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# File I/O (separated from pure core)
# ---------------------------------------------------------------------------

def write_anomaly_action_queue(
    queue: dict,
    path:  Path = ACTION_QUEUE_PATH,
) -> None:
    """Write action queue to JSON file. Creates parent dirs if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(queue, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_escalation_module():
    path = Path(__file__).parent / "build_anomaly_escalation_lite.py"
    spec = importlib.util.spec_from_file_location("_escalation", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-97 + AC-98
# ---------------------------------------------------------------------------

def build_action_queue_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
    write_output:             bool   = False,
) -> dict:
    """
    Full chain: market_specs → … → anomaly escalation (AC-97)
                             → anomaly action queue (AC-98).

    Returns dict with all pipeline keys plus anomaly_action_queue.
    Optionally writes anomaly_action_queue.json when write_output=True.
    live_activation_allowed=False always.
    """
    _esc_mod = _load_escalation_module()
    pipeline = _esc_mod.build_escalation_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    queue = build_anomaly_action_queue(
        pipeline["anomaly_escalation"],
        pipeline["review_packet"],
        pipeline["promotion_gate"],
        pipeline["paper_readiness_dossier"],
        pipeline["human_review_summary"],
    )
    if write_output:
        write_anomaly_action_queue(queue)

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
        "promotion_gate":           pipeline["promotion_gate"],
        "paper_readiness_dossier":  pipeline["paper_readiness_dossier"],
        "human_review_summary":     pipeline["human_review_summary"],
        "review_packet":            pipeline["review_packet"],
        "anomaly_escalation":       pipeline["anomaly_escalation"],
        "anomaly_action_queue":     queue,
    }


# ---------------------------------------------------------------------------
# Optional main (CLI demo + file write)
# ---------------------------------------------------------------------------

def main() -> None:
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

    result = build_action_queue_from_specs(
        specs, total_equity_eur=10_000.0, market_regimes=regimes, write_output=True
    )
    print(json.dumps(result["anomaly_action_queue"], indent=2))


if __name__ == "__main__":
    main()
