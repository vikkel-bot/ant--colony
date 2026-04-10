"""
AC-97: Anomaly Escalation Layer (Human Attention Filter)

Filter between machine and human: only escalate deviations.
No approval logic. No execution. No new trading logic.
Triage layer for human attention — reads existing layer outputs only.

Inputs (read-only):
  promotion_gate          (AC-94)
  paper_readiness_dossier (AC-95)
  human_review_summary    (AC-95)
  review_packet           (AC-96)

Output: anomaly_escalation dict. Pure function, deterministic.

Design principles:
  - non_binding=True always.
  - simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always.
  - Fail-closed: unclear/missing input → at least MEDIUM; invalid → CRITICAL.
  - Deterministic: same inputs → same escalation every call.
  - Pure core function — no I/O, no side effects.
  - No new decision logic; only interprets existing layer signals.

Anomaly level values:
  NONE     — all layers consistent; no anomaly; human attention not required
  LOW      — informational deviation only; no human attention required
  MEDIUM   — hold/cautious state; human attention required
  HIGH     — conflicting signals or blocking findings; human attention required
  CRITICAL — promotion not ready / validation failures / missing data; human attention required

human_attention_required:
  CRITICAL → True
  HIGH     → True
  MEDIUM   → True
  LOW      → False
  NONE     → False

Escalation reason codes:
  ESCALATION_CRITICAL_INVALID_INPUT       — missing/invalid input object
  ESCALATION_CRITICAL_PROMOTION_NOT_READY — promotion_status != PAPER_READY
  ESCALATION_CRITICAL_DOSSIER_REJECTED    — dossier_status == DOSSIER_REJECTED
  ESCALATION_CRITICAL_VALIDATION_FAILED   — validation_passed=False or replay_consistent=False
  ESCALATION_CRITICAL_CONSISTENCY_FAILED  — consistency_passed=False
  ESCALATION_CRITICAL_SNAPSHOT_MISSING    — required promotion_snapshot absent in dossier
  ESCALATION_HIGH_REVIEW_PRIORITY         — review_priority == HIGH
  ESCALATION_HIGH_BLOCKING_FINDINGS       — blocking_findings non-empty
  ESCALATION_HIGH_LAYER_CONFLICT          — status disagreement between layers
  ESCALATION_MEDIUM_HOLD                  — hold/cautious state detected
  ESCALATION_LOW_INFO                     — informational deviation (e.g., total_intents=0)
  ESCALATION_NONE                         — all consistent, no anomaly

Output file: C:\\Trading\\ANT_OUT\\anomaly_escalation.json
"""
from __future__ import annotations
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION   = "anomaly_escalation_v1"
COMPONENT = "build_anomaly_escalation_lite"
OUT_DIR   = Path(r"C:\Trading\ANT_OUT")
ESCALATION_PATH = OUT_DIR / "anomaly_escalation.json"

# Anomaly levels
LEVEL_NONE     = "NONE"
LEVEL_LOW      = "LOW"
LEVEL_MEDIUM   = "MEDIUM"
LEVEL_HIGH     = "HIGH"
LEVEL_CRITICAL = "CRITICAL"

# Reason codes
CODE_INVALID_INPUT        = "ESCALATION_CRITICAL_INVALID_INPUT"
CODE_PROMOTION_NOT_READY  = "ESCALATION_CRITICAL_PROMOTION_NOT_READY"
CODE_DOSSIER_REJECTED     = "ESCALATION_CRITICAL_DOSSIER_REJECTED"
CODE_VALIDATION_FAILED    = "ESCALATION_CRITICAL_VALIDATION_FAILED"
CODE_CONSISTENCY_FAILED   = "ESCALATION_CRITICAL_CONSISTENCY_FAILED"
CODE_SNAPSHOT_MISSING     = "ESCALATION_CRITICAL_SNAPSHOT_MISSING"
CODE_HIGH_PRIORITY        = "ESCALATION_HIGH_REVIEW_PRIORITY"
CODE_HIGH_BLOCKING        = "ESCALATION_HIGH_BLOCKING_FINDINGS"
CODE_HIGH_CONFLICT        = "ESCALATION_HIGH_LAYER_CONFLICT"
CODE_MEDIUM_HOLD          = "ESCALATION_MEDIUM_HOLD"
CODE_LOW_INFO             = "ESCALATION_LOW_INFO"
CODE_NONE                 = "ESCALATION_NONE"

# Status mirrors (read-only references)
_PROMO_READY      = "PAPER_READY"
_DOSSIER_REJECTED = "DOSSIER_REJECTED"
_DOSSIER_HOLD     = "DOSSIER_HOLD"
_REVIEW_READY     = "REVIEW_READY"
_PKT_READY        = "READY"
_PRIORITY_HIGH    = "HIGH"
_PRIORITY_MEDIUM  = "MEDIUM"


# ---------------------------------------------------------------------------
# Core escalation function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_anomaly_escalation(
    promotion_gate:          object,
    paper_readiness_dossier: object,
    human_review_summary:    object,
    review_packet:           object,
) -> dict:
    """
    Inspect AC-94/95/96 outputs and classify the anomaly level.

    No new decision logic. No execution. No mutations.
    Reads and interprets existing signals only.

    Returns anomaly_escalation dict with:
      anomaly_detected, anomaly_level, human_attention_required,
      escalation_reason, escalation_reason_code, escalation_findings,
      promotion_status, dossier_status, review_status, flags.
    """
    ts = _utc_ts()

    # --- Step 1: validate input types ---
    for name, obj in [
        ("promotion_gate",          promotion_gate),
        ("paper_readiness_dossier", paper_readiness_dossier),
        ("human_review_summary",    human_review_summary),
        ("review_packet",           review_packet),
    ]:
        if not isinstance(obj, dict):
            return _escalation_result(
                ts, LEVEL_CRITICAL, CODE_INVALID_INPUT,
                f"{name} is not a dict",
                [f"{name} is not a dict — cannot assess anomaly"],
                promotion_status="", dossier_status="", review_status="",
            )

    for name, obj, key in [
        ("promotion_gate",          promotion_gate,          "promotion_status"),
        ("paper_readiness_dossier", paper_readiness_dossier, "dossier_status"),
        ("human_review_summary",    human_review_summary,    "review_status"),
        ("review_packet",           review_packet,           "review_packet_status"),
    ]:
        if key not in obj:
            return _escalation_result(
                ts, LEVEL_CRITICAL, CODE_INVALID_INPUT,
                f"{name} missing required field '{key}'",
                [f"{name} missing '{key}' — cannot assess anomaly"],
                promotion_status="", dossier_status="", review_status="",
            )

    # --- Extract key signals ---
    promotion_status    = str(promotion_gate.get("promotion_status", ""))
    dossier_status      = str(paper_readiness_dossier.get("dossier_status", ""))
    review_status       = str(human_review_summary.get("review_status", ""))
    review_priority     = str(human_review_summary.get("review_priority", ""))
    blocking_findings   = list(human_review_summary.get("blocking_findings", []))
    review_packet_status = str(review_packet.get("review_packet_status", ""))

    upstream_snap = promotion_gate.get("upstream_snapshot", {})
    if not isinstance(upstream_snap, dict):
        upstream_snap = {}

    validation_passed   = bool(upstream_snap.get("validation_passed", False))
    replay_consistent   = bool(upstream_snap.get("replay_consistent", False))
    consistency_passed  = bool(upstream_snap.get("consistency_passed", False))

    promo_snap_in_dossier = paper_readiness_dossier.get("promotion_snapshot", {})
    snapshot_missing = not isinstance(promo_snap_in_dossier, dict) or not promo_snap_in_dossier

    # --- Step 2: CRITICAL checks ---
    critical_findings = []

    if promotion_status != _PROMO_READY:
        critical_findings.append(
            f"promotion_status={promotion_status!r} (expected PAPER_READY)"
        )
    if dossier_status == _DOSSIER_REJECTED:
        critical_findings.append(
            f"dossier_status=DOSSIER_REJECTED"
        )
    if not validation_passed:
        critical_findings.append(
            "validation_passed=False (upstream validation check failed)"
        )
    if not replay_consistent:
        critical_findings.append(
            "replay_consistent=False (audit trace replay mismatch)"
        )
    if not consistency_passed:
        critical_findings.append(
            "consistency_passed=False (cross-layer count mismatch)"
        )
    if snapshot_missing:
        critical_findings.append(
            "promotion_snapshot absent in dossier (required snapshot missing)"
        )

    if critical_findings:
        code = _pick_critical_code(
            promotion_status, dossier_status,
            validation_passed, replay_consistent,
            consistency_passed, snapshot_missing,
        )
        reason = critical_findings[0]
        return _escalation_result(
            ts, LEVEL_CRITICAL, code, reason,
            critical_findings,
            promotion_status, dossier_status, review_status,
        )

    # --- Step 3: HIGH checks ---
    # At this point: promotion_status == PAPER_READY, all upstream booleans True.
    high_findings = []

    if review_priority == _PRIORITY_HIGH:
        high_findings.append(f"review_priority=HIGH")
    if blocking_findings:
        high_findings.append(
            f"blocking_findings present ({len(blocking_findings)} items)"
        )
        for bf in blocking_findings:
            high_findings.append(f"  blocking: {bf}")
    # Layer conflict: if promotion is READY but other layers disagree
    if review_status != _REVIEW_READY:
        high_findings.append(
            f"layer conflict: promotion=PAPER_READY but review_status={review_status!r}"
        )
    if review_packet_status != _PKT_READY:
        high_findings.append(
            f"layer conflict: promotion=PAPER_READY but review_packet_status={review_packet_status!r}"
        )

    if high_findings:
        code = _pick_high_code(
            review_priority, blocking_findings,
            review_status, review_packet_status,
        )
        reason = high_findings[0]
        return _escalation_result(
            ts, LEVEL_HIGH, code, reason,
            high_findings,
            promotion_status, dossier_status, review_status,
        )

    # --- Step 4: MEDIUM checks ---
    medium_findings = []

    if review_priority == _PRIORITY_MEDIUM:
        medium_findings.append(f"review_priority=MEDIUM (hold/cautious state)")
    if dossier_status == _DOSSIER_HOLD:
        medium_findings.append(f"dossier_status=DOSSIER_HOLD")

    if medium_findings:
        reason = medium_findings[0]
        return _escalation_result(
            ts, LEVEL_MEDIUM, CODE_MEDIUM_HOLD, reason,
            medium_findings,
            promotion_status, dossier_status, review_status,
        )

    # --- Step 5: LOW checks ---
    low_findings = []

    counts = paper_readiness_dossier.get("readiness_counts", {})
    if isinstance(counts, dict):
        total_intents = int(counts.get("total_intents", 0))
        if total_intents == 0:
            low_findings.append(
                "total_intents=0 — no intents recorded (informational)"
            )

    if low_findings:
        reason = low_findings[0]
        return _escalation_result(
            ts, LEVEL_LOW, CODE_LOW_INFO, reason,
            low_findings,
            promotion_status, dossier_status, review_status,
        )

    # --- Step 6: NONE — all consistent ---
    return _escalation_result(
        ts, LEVEL_NONE, CODE_NONE,
        "all layers consistent — no anomaly detected",
        ["all layers consistent — no anomaly detected"],
        promotion_status, dossier_status, review_status,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_critical_code(
    promotion_status: str,
    dossier_status:   str,
    validation_passed: bool,
    replay_consistent: bool,
    consistency_passed: bool,
    snapshot_missing:  bool,
) -> str:
    """Return the most specific CRITICAL reason code (first triggered)."""
    if promotion_status != _PROMO_READY:
        return CODE_PROMOTION_NOT_READY
    if dossier_status == _DOSSIER_REJECTED:
        return CODE_DOSSIER_REJECTED
    if not validation_passed or not replay_consistent:
        return CODE_VALIDATION_FAILED
    if not consistency_passed:
        return CODE_CONSISTENCY_FAILED
    if snapshot_missing:
        return CODE_SNAPSHOT_MISSING
    return CODE_INVALID_INPUT  # fallback (should not reach)


def _pick_high_code(
    review_priority:      str,
    blocking_findings:    list,
    review_status:        str,
    review_packet_status: str,
) -> str:
    """Return the most specific HIGH reason code (first triggered)."""
    if review_priority == _PRIORITY_HIGH:
        return CODE_HIGH_PRIORITY
    if blocking_findings:
        return CODE_HIGH_BLOCKING
    return CODE_HIGH_CONFLICT


def _escalation_result(
    ts:               str,
    level:            str,
    reason_code:      str,
    reason:           str,
    findings:         list,
    promotion_status: str,
    dossier_status:   str,
    review_status:    str,
) -> dict:
    anomaly_detected        = level != LEVEL_NONE
    human_attention_required = level in (LEVEL_CRITICAL, LEVEL_HIGH, LEVEL_MEDIUM)
    return {
        "version":                  VERSION,
        "component":                COMPONENT,
        "ts_utc":                   ts,
        "anomaly_detected":         anomaly_detected,
        "anomaly_level":            level,
        "human_attention_required": human_attention_required,
        "escalation_reason":        reason,
        "escalation_reason_code":   reason_code,
        "escalation_findings":      list(findings),
        "promotion_status":         promotion_status,
        "dossier_status":           dossier_status,
        "review_status":            review_status,
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

def write_anomaly_escalation(
    escalation: dict,
    path: Path = ESCALATION_PATH,
) -> None:
    """Write anomaly escalation to JSON file. Creates parent dirs if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(escalation, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_packet_module():
    path = Path(__file__).parent / "build_review_packet_lite.py"
    spec = importlib.util.spec_from_file_location("_packet", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-96 + AC-97
# ---------------------------------------------------------------------------

def build_escalation_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
    write_output:             bool   = False,
) -> dict:
    """
    Full chain: market_specs → … → review packet (AC-96)
                             → anomaly escalation (AC-97).

    Returns dict with all pipeline keys plus anomaly_escalation.
    Optionally writes anomaly_escalation.json when write_output=True.
    live_activation_allowed=False always.
    """
    _packet_mod = _load_packet_module()
    pipeline    = _packet_mod.build_review_packet_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    escalation = build_anomaly_escalation(
        pipeline["promotion_gate"],
        pipeline["paper_readiness_dossier"],
        pipeline["human_review_summary"],
        pipeline["review_packet"],
    )
    if write_output:
        write_anomaly_escalation(escalation)

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
        "anomaly_escalation":       escalation,
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

    result = build_escalation_from_specs(
        specs, total_equity_eur=10_000.0, market_regimes=regimes, write_output=True
    )
    print(json.dumps(result["anomaly_escalation"], indent=2))


if __name__ == "__main__":
    main()
