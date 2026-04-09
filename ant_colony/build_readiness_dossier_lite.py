"""
AC-95: Paper Readiness Dossier + Human Review Summary

Two layers on top of the AC-94 promotion gate:

1. Paper Readiness Dossier
   Assembles compact, auditable snapshots from all upstream layers
   (AC-90…AC-94) into one review object. One source of truth for
   what the promotion decision was based on.

2. Human Review Summary
   Translates the dossier into a human-readable verdict: what was
   found, what is blocking, what to look at, and how urgent it is.
   Explicit key_findings and blocking_findings lists.

Design principles:
  - dossier_non_binding=True always.
  - dossier_simulation_only=True always.
  - review_non_binding=True always.
  - review_simulation_only=True always.
  - paper_only=True always on all outputs.
  - live_activation_allowed=False always — never overridden.
  - Fail-closed: invalid/missing input → DOSSIER_REJECTED / REVIEW_REJECTED.
  - Deterministic: same inputs → same dossier and review every call.
  - Pure core functions — no I/O, no side effects.
  - No broker coupling, no live execution path, no portfolio mutation.

Dossier status values:
  DOSSIER_READY    — promotion gate present; dossier built for review
  DOSSIER_HOLD     — promotion is PAPER_HOLD; dossier held at baseline
  DOSSIER_REJECTED — promotion is PAPER_REJECTED or invalid input

Dossier mode values:
  DOSSIER_PAPER_READY  — dossier built for a PAPER_READY candidate
  DOSSIER_PAPER_HOLD   — dossier built for a PAPER_HOLD candidate
  DOSSIER_PAPER_REJECT — dossier built for a PAPER_REJECTED candidate
  DOSSIER_INVALID      — dossier rejected due to invalid input

Dossier reason codes:
  DOSSIER_PROMOTION_READY    — promotion gate is PAPER_READY
  DOSSIER_PROMOTION_HOLD     — promotion gate is PAPER_HOLD
  DOSSIER_PROMOTION_REJECTED — promotion gate is PAPER_REJECTED
  DOSSIER_INVALID_INPUT      — input is not valid

readiness_counts fields:
  total_intents      — int: from handoff
  total_allowed      — int: from handoff
  total_blocked      — int: from handoff
  ledger_entry_count — int: from ledger (if provided)
  trace_step_count   — int: from trace (if provided)
  matched_checks     — int: from consistency

Review status values:
  REVIEW_READY    — promotion is PAPER_READY; low urgency
  REVIEW_HOLD     — promotion is PAPER_HOLD; medium urgency
  REVIEW_REJECTED — promotion is PAPER_REJECTED; high urgency

Review mode values:
  REVIEW_PAPER_READY  — candidate is paper-ready
  REVIEW_PAPER_HOLD   — candidate is on hold
  REVIEW_PAPER_REJECT — candidate is rejected

Review priority values:
  HIGH   — PAPER_REJECTED: needs human attention
  MEDIUM — PAPER_HOLD: safe but may need review
  LOW    — PAPER_READY: all clear, optional review

Review reason codes:
  REVIEW_PAPER_READY_OK    — all clear
  REVIEW_PAPER_HOLD_OK     — safe baseline hold
  REVIEW_PAPER_REJECTED_OK — explicit rejection; findings listed
  REVIEW_INVALID_INPUT     — dossier input invalid

Usage (importable):
    from build_readiness_dossier_lite import build_readiness_dossier
    dossier = build_readiness_dossier(promotion_gate, queen_handoff, runner_intake,
                                      replay_validation, handoff_consistency,
                                      execution_ledger, audit_trace)

    from build_readiness_dossier_lite import build_human_review_summary
    review = build_human_review_summary(dossier)

    # Combined:
    from build_readiness_dossier_lite import build_dossier_and_review
    result = build_dossier_and_review(...)

    # Full chain from specs:
    from build_readiness_dossier_lite import build_dossier_from_specs
    result = build_dossier_from_specs(market_specs, total_equity_eur=10_000.0, ...)
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "readiness_dossier_v1"

# ---------------------------------------------------------------------------
# Dossier status / mode / reason codes
# ---------------------------------------------------------------------------

DOSSIER_READY    = "DOSSIER_READY"
DOSSIER_HOLD     = "DOSSIER_HOLD"
DOSSIER_REJECTED = "DOSSIER_REJECTED"

DOSSIER_MODE_PAPER_READY  = "DOSSIER_PAPER_READY"
DOSSIER_MODE_PAPER_HOLD   = "DOSSIER_PAPER_HOLD"
DOSSIER_MODE_PAPER_REJECT = "DOSSIER_PAPER_REJECT"
DOSSIER_MODE_INVALID      = "DOSSIER_INVALID"

REASON_DOSSIER_READY    = "DOSSIER_PROMOTION_READY"
REASON_DOSSIER_HOLD     = "DOSSIER_PROMOTION_HOLD"
REASON_DOSSIER_REJECTED = "DOSSIER_PROMOTION_REJECTED"
REASON_DOSSIER_INVALID  = "DOSSIER_INVALID_INPUT"

# ---------------------------------------------------------------------------
# Review status / mode / reason codes / priority
# ---------------------------------------------------------------------------

REVIEW_READY    = "REVIEW_READY"
REVIEW_HOLD     = "REVIEW_HOLD"
REVIEW_REJECTED = "REVIEW_REJECTED"

REVIEW_MODE_PAPER_READY  = "REVIEW_PAPER_READY"
REVIEW_MODE_PAPER_HOLD   = "REVIEW_PAPER_HOLD"
REVIEW_MODE_PAPER_REJECT = "REVIEW_PAPER_REJECT"

REASON_REVIEW_READY    = "REVIEW_PAPER_READY_OK"
REASON_REVIEW_HOLD     = "REVIEW_PAPER_HOLD_OK"
REASON_REVIEW_REJECTED = "REVIEW_PAPER_REJECTED_OK"
REASON_REVIEW_INVALID  = "REVIEW_INVALID_INPUT"

PRIORITY_HIGH   = "HIGH"
PRIORITY_MEDIUM = "MEDIUM"
PRIORITY_LOW    = "LOW"

# Promotion status mirrors (AC-94)
_PROMO_READY    = "PAPER_READY"
_PROMO_HOLD     = "PAPER_HOLD"
_PROMO_REJECTED = "PAPER_REJECTED"


# ---------------------------------------------------------------------------
# Core dossier function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_readiness_dossier(
    promotion_gate:      object,
    queen_handoff:       object = None,
    runner_intake:       object = None,
    replay_validation:   object = None,
    handoff_consistency: object = None,
    execution_ledger:    object = None,
    audit_trace:         object = None,
) -> dict:
    """
    Assemble a paper readiness dossier from the AC-94 promotion gate
    and optional upstream layer objects.

    dossier_non_binding=True, dossier_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        promotion_gate:      dict from build_promotion_gate() (AC-94).
        queen_handoff:       optional dict from build_queen_handoff() (AC-90).
        runner_intake:       optional dict from build_runner_intake() (AC-91).
        replay_validation:   optional dict from build_replay_validator() (AC-93).
        handoff_consistency: optional dict from build_handoff_consistency_check() (AC-93).
        execution_ledger:    optional dict from build_dry_run_ledger() (AC-92).
        audit_trace:         optional dict from build_audit_trace() (AC-92).

    Returns:
        Paper readiness dossier dict.
    """
    if not isinstance(promotion_gate, dict):
        return _rejected_dossier("promotion_gate is not a dict")
    if "promotion_status" not in promotion_gate:
        return _rejected_dossier("promotion_gate missing promotion_status")

    promo_status = str(promotion_gate.get("promotion_status", ""))

    # Build compact snapshots
    promo_snap   = _promotion_snapshot(promotion_gate)
    val_snap     = _validation_snapshot(replay_validation)
    con_snap     = _consistency_snapshot(handoff_consistency)
    ho_snap      = _handoff_snapshot(queen_handoff)
    runner_snap  = _runner_snapshot(runner_intake)
    counts       = _readiness_counts(
        queen_handoff, execution_ledger, audit_trace, handoff_consistency
    )

    if promo_status == _PROMO_READY:
        return {
            "dossier_status":          DOSSIER_READY,
            "dossier_mode":            DOSSIER_MODE_PAPER_READY,
            "dossier_ready_for_review": True,
            "dossier_reason":          "promotion is PAPER_READY — dossier ready for review",
            "dossier_reason_code":     REASON_DOSSIER_READY,
            "promotion_snapshot":      promo_snap,
            "validation_snapshot":     val_snap,
            "consistency_snapshot":    con_snap,
            "handoff_snapshot":        ho_snap,
            "runner_snapshot":         runner_snap,
            "readiness_counts":        counts,
            "dossier_non_binding":     True,
            "dossier_simulation_only": True,
            "paper_only":              True,
            "live_activation_allowed": False,
        }

    if promo_status == _PROMO_HOLD:
        return {
            "dossier_status":          DOSSIER_HOLD,
            "dossier_mode":            DOSSIER_MODE_PAPER_HOLD,
            "dossier_ready_for_review": True,  # reviewable even in hold
            "dossier_reason":          "promotion is PAPER_HOLD — dossier held at baseline",
            "dossier_reason_code":     REASON_DOSSIER_HOLD,
            "promotion_snapshot":      promo_snap,
            "validation_snapshot":     val_snap,
            "consistency_snapshot":    con_snap,
            "handoff_snapshot":        ho_snap,
            "runner_snapshot":         runner_snap,
            "readiness_counts":        counts,
            "dossier_non_binding":     True,
            "dossier_simulation_only": True,
            "paper_only":              True,
            "live_activation_allowed": False,
        }

    # PAPER_REJECTED or unknown
    return {
        "dossier_status":          DOSSIER_REJECTED,
        "dossier_mode":            DOSSIER_MODE_PAPER_REJECT,
        "dossier_ready_for_review": True,  # always reviewable
        "dossier_reason":          f"promotion is {promo_status} — dossier marks rejection",
        "dossier_reason_code":     REASON_DOSSIER_REJECTED,
        "promotion_snapshot":      promo_snap,
        "validation_snapshot":     val_snap,
        "consistency_snapshot":    con_snap,
        "handoff_snapshot":        ho_snap,
        "runner_snapshot":         runner_snap,
        "readiness_counts":        counts,
        "dossier_non_binding":     True,
        "dossier_simulation_only": True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Core human review summary function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_human_review_summary(dossier: object) -> dict:
    """
    Translate a paper readiness dossier into a human-readable review summary.

    Produces:
      - review_status / mode
      - review_decision_hint: one-sentence guidance for a human reviewer
      - key_findings: list of relevant observations (always ≥1)
      - blocking_findings: list of issues that prevent promotion (may be empty)
      - review_priority: HIGH / MEDIUM / LOW

    review_non_binding=True, review_simulation_only=True,
    paper_only=True, live_activation_allowed=False always.

    Args:
        dossier: dict from build_readiness_dossier() (AC-95).

    Returns:
        Human review summary dict.
    """
    if not isinstance(dossier, dict):
        return _rejected_review("dossier is not a dict")
    if "dossier_status" not in dossier:
        return _rejected_review("dossier missing dossier_status")

    d_status  = str(dossier.get("dossier_status", ""))
    promo_snap = dossier.get("promotion_snapshot", {})
    val_snap   = dossier.get("validation_snapshot", {})
    con_snap   = dossier.get("consistency_snapshot", {})
    ho_snap    = dossier.get("handoff_snapshot", {})
    runner_snap = dossier.get("runner_snapshot", {})
    counts     = dossier.get("readiness_counts", {})

    key_findings      = _build_key_findings(promo_snap, val_snap, con_snap, ho_snap, runner_snap, counts)
    blocking_findings = _build_blocking_findings(d_status, promo_snap, val_snap, con_snap)

    if d_status == DOSSIER_READY:
        return {
            "review_status":           REVIEW_READY,
            "review_mode":             REVIEW_MODE_PAPER_READY,
            "review_decision_hint":    (
                "Candidate is PAPER_READY. All checks passed. "
                "Human review optional before paper execution."
            ),
            "review_reason":           "promotion gate cleared — candidate paper-ready",
            "review_reason_code":      REASON_REVIEW_READY,
            "key_findings":            key_findings,
            "blocking_findings":       blocking_findings,
            "review_priority":         PRIORITY_LOW,
            "review_non_binding":      True,
            "review_simulation_only":  True,
            "paper_only":              True,
            "live_activation_allowed": False,
        }

    if d_status == DOSSIER_HOLD:
        return {
            "review_status":           REVIEW_HOLD,
            "review_mode":             REVIEW_MODE_PAPER_HOLD,
            "review_decision_hint":    (
                "Candidate is PAPER_HOLD. Upstream layers in baseline/hold state. "
                "Review upstream status before proceeding."
            ),
            "review_reason":           "promotion gate on hold — upstream baseline state",
            "review_reason_code":      REASON_REVIEW_HOLD,
            "key_findings":            key_findings,
            "blocking_findings":       blocking_findings,
            "review_priority":         PRIORITY_MEDIUM,
            "review_non_binding":      True,
            "review_simulation_only":  True,
            "paper_only":              True,
            "live_activation_allowed": False,
        }

    # DOSSIER_REJECTED
    return {
        "review_status":           REVIEW_REJECTED,
        "review_mode":             REVIEW_MODE_PAPER_REJECT,
        "review_decision_hint":    (
            "Candidate is PAPER_REJECTED. One or more checks failed. "
            "Review blocking_findings and resolve before re-submitting."
        ),
        "review_reason":           "promotion gate rejected — check blocking_findings",
        "review_reason_code":      REASON_REVIEW_REJECTED,
        "key_findings":            key_findings,
        "blocking_findings":       blocking_findings,
        "review_priority":         PRIORITY_HIGH,
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Findings builders
# ---------------------------------------------------------------------------

def _build_key_findings(
    promo_snap:  dict,
    val_snap:    dict,
    con_snap:    dict,
    ho_snap:     dict,
    runner_snap: dict,
    counts:      dict,
) -> list:
    findings = []

    # Promotion
    promo_status = str(promo_snap.get("promotion_status", ""))
    reason_code  = str(promo_snap.get("promotion_reason_code", ""))
    if promo_status:
        findings.append(f"promotion_status={promo_status} ({reason_code})")

    # Handoff
    ho_status = str(ho_snap.get("handoff_status", ""))
    ho_ready  = ho_snap.get("handoff_ready", False)
    if ho_status:
        findings.append(f"handoff_status={ho_status} handoff_ready={ho_ready}")

    # Runner
    ri_status = str(runner_snap.get("runner_intake_status", ""))
    ri_valid  = runner_snap.get("runner_contract_valid", False)
    if ri_status:
        findings.append(f"runner_intake_status={ri_status} contract_valid={ri_valid}")

    # Validation
    val_status  = str(val_snap.get("validation_status", ""))
    val_passed  = val_snap.get("validation_passed", False)
    replay_ok   = val_snap.get("replay_consistent", False)
    l_count     = val_snap.get("validated_ledger_count", 0)
    t_count     = val_snap.get("validated_trace_count", 0)
    if val_status:
        findings.append(
            f"validation_status={val_status} passed={val_passed} "
            f"replay_consistent={replay_ok} "
            f"ledger_entries={l_count} trace_steps={t_count}"
        )

    # Consistency
    con_status  = str(con_snap.get("consistency_status", ""))
    con_passed  = con_snap.get("consistency_passed", False)
    matched     = con_snap.get("matched_checks", 0)
    if con_status:
        findings.append(
            f"consistency_status={con_status} passed={con_passed} "
            f"matched_checks={matched}"
        )

    # Counts
    total_i  = counts.get("total_intents", 0)
    allowed  = counts.get("total_allowed", 0)
    blocked  = counts.get("total_blocked", 0)
    findings.append(
        f"intents: total={total_i} allowed={allowed} blocked={blocked}"
    )

    return findings if findings else ["no upstream data available"]


def _build_blocking_findings(
    d_status:   str,
    promo_snap: dict,
    val_snap:   dict,
    con_snap:   dict,
) -> list:
    if d_status == DOSSIER_READY:
        return []

    findings = []

    # Promotion reason
    reason_code = str(promo_snap.get("promotion_reason_code", ""))
    promo_reason = str(promo_snap.get("promotion_reason", ""))
    if reason_code and reason_code not in ("PROMOTION_ALL_CLEAR", "PROMOTION_UPSTREAM_HOLD"):
        findings.append(f"promotion: {reason_code} — {promo_reason}")

    # Validation issues
    val_passed = val_snap.get("validation_passed", True)
    replay_ok  = val_snap.get("replay_consistent", True)
    val_reason = str(val_snap.get("validation_reason", ""))
    if not val_passed:
        findings.append(f"validation failed: {val_reason}")
    if not replay_ok:
        findings.append("replay_consistent=False")

    # Consistency issues
    con_passed  = con_snap.get("consistency_passed", True)
    con_reason  = str(con_snap.get("consistency_reason", ""))
    if not con_passed and d_status != DOSSIER_HOLD:
        findings.append(f"consistency failed: {con_reason}")

    return findings


# ---------------------------------------------------------------------------
# Snapshot builders (compact, non-mutating)
# ---------------------------------------------------------------------------

def _promotion_snapshot(gate: dict) -> dict:
    if not isinstance(gate, dict):
        return {}
    snap = gate.get("upstream_snapshot", {})
    return {
        "promotion_status":      str(gate.get("promotion_status", "")),
        "promotion_ready":       bool(gate.get("promotion_ready", False)),
        "paper_ready_candidate": bool(gate.get("paper_ready_candidate", False)),
        "promotion_reason_code": str(gate.get("promotion_reason_code", "")),
        "promotion_reason":      str(gate.get("promotion_reason", "")),
        "promotion_decision":    str(gate.get("promotion_decision", "")),
        "upstream_snapshot":     dict(snap) if isinstance(snap, dict) else {},
    }


def _validation_snapshot(val: object) -> dict:
    if not isinstance(val, dict):
        return {}
    return {
        "validation_status":     str(val.get("validation_status", "")),
        "validation_passed":     bool(val.get("validation_passed", False)),
        "replay_consistent":     bool(val.get("replay_consistent", False)),
        "validation_reason":     str(val.get("validation_reason", "")),
        "validated_ledger_count": int(val.get("validated_ledger_count", 0)),
        "validated_trace_count": int(val.get("validated_trace_count", 0)),
    }


def _consistency_snapshot(con: object) -> dict:
    if not isinstance(con, dict):
        return {}
    return {
        "consistency_status":  str(con.get("handoff_consistency_status", "")),
        "consistency_passed":  bool(con.get("handoff_consistency_passed", False)),
        "consistency_reason":  str(con.get("consistency_reason", "")),
        "matched_checks":      int(con.get("matched_intent_count", 0)),
        "missing_in_handoff":  int(con.get("missing_in_handoff_count", 0)),
        "missing_in_ledger":   int(con.get("missing_in_ledger_count", 0)),
        "missing_in_trace":    int(con.get("missing_in_trace_count", 0)),
    }


def _handoff_snapshot(ho: object) -> dict:
    if not isinstance(ho, dict):
        return {}
    return {
        "handoff_status":  str(ho.get("handoff_status", "")),
        "handoff_ready":   bool(ho.get("handoff_ready", False)),
        "total_intents":   int(ho.get("total_intents", 0)),
        "total_allowed":   int(ho.get("total_allowed", 0)),
        "total_blocked":   int(ho.get("total_blocked", 0)),
    }


def _runner_snapshot(ri: object) -> dict:
    if not isinstance(ri, dict):
        return {}
    return {
        "runner_intake_status":  str(ri.get("runner_intake_status", "")),
        "runner_contract_valid": bool(ri.get("runner_contract_valid", False)),
        "consumed_intent_count": int(ri.get("consumed_intent_count", 0)),
        "consumed_allowed_count": int(ri.get("consumed_allowed_count", 0)),
        "consumed_blocked_count": int(ri.get("consumed_blocked_count", 0)),
    }


def _readiness_counts(
    ho:      object,
    ledger:  object,
    trace:   object,
    con:     object,
) -> dict:
    total_i  = int(ho.get("total_intents", 0))    if isinstance(ho, dict)     else 0
    allowed  = int(ho.get("total_allowed", 0))    if isinstance(ho, dict)     else 0
    blocked  = int(ho.get("total_blocked", 0))    if isinstance(ho, dict)     else 0
    l_count  = int(ledger.get("ledger_entry_count", 0)) if isinstance(ledger, dict) else 0
    t_count  = int(trace.get("trace_step_count", 0))   if isinstance(trace, dict)  else 0
    matched  = int(con.get("matched_intent_count", 0)) if isinstance(con, dict)    else 0
    return {
        "total_intents":      total_i,
        "total_allowed":      allowed,
        "total_blocked":      blocked,
        "ledger_entry_count": l_count,
        "trace_step_count":   t_count,
        "matched_checks":     matched,
    }


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_dossier(reason: str) -> dict:
    return {
        "dossier_status":          DOSSIER_REJECTED,
        "dossier_mode":            DOSSIER_MODE_INVALID,
        "dossier_ready_for_review": False,
        "dossier_reason":          reason,
        "dossier_reason_code":     REASON_DOSSIER_INVALID,
        "promotion_snapshot":      {},
        "validation_snapshot":     {},
        "consistency_snapshot":    {},
        "handoff_snapshot":        {},
        "runner_snapshot":         {},
        "readiness_counts":        {},
        "dossier_non_binding":     True,
        "dossier_simulation_only": True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _rejected_review(reason: str) -> dict:
    return {
        "review_status":           REVIEW_REJECTED,
        "review_mode":             REVIEW_MODE_PAPER_REJECT,
        "review_decision_hint":    "Invalid dossier input — cannot produce review.",
        "review_reason":           reason,
        "review_reason_code":      REASON_REVIEW_INVALID,
        "key_findings":            [reason],
        "blocking_findings":       [reason],
        "review_priority":         PRIORITY_HIGH,
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


# ---------------------------------------------------------------------------
# Convenience: dossier + review combined
# ---------------------------------------------------------------------------

def build_dossier_and_review(
    promotion_gate:      object,
    queen_handoff:       object = None,
    runner_intake:       object = None,
    replay_validation:   object = None,
    handoff_consistency: object = None,
    execution_ledger:    object = None,
    audit_trace:         object = None,
) -> dict:
    """
    Build paper readiness dossier and human review summary in one call.

    Returns:
        Dict with keys: paper_readiness_dossier, human_review_summary.
    """
    dossier = build_readiness_dossier(
        promotion_gate, queen_handoff, runner_intake,
        replay_validation, handoff_consistency,
        execution_ledger, audit_trace,
    )
    review = build_human_review_summary(dossier)
    return {
        "paper_readiness_dossier": dossier,
        "human_review_summary":    review,
    }


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_promotion_module():
    path = Path(__file__).parent / "build_promotion_gate_lite.py"
    spec = importlib.util.spec_from_file_location("_promo", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81…AC-94 + AC-95
# ---------------------------------------------------------------------------

def build_dossier_from_specs(
    market_specs:             object,
    total_equity_eur:         float,
    market_regimes:           object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → … → promotion gate (AC-94)
                             → readiness dossier + review summary (AC-95).

    Returns dict with keys:
        …all AC-94 keys…, paper_readiness_dossier, human_review_summary.
    All outputs are paper-only, non-binding, simulation-only.
    live_activation_allowed=False always.
    """
    _promo_mod = _load_promotion_module()
    pipeline   = _promo_mod.build_promotion_from_specs(
        market_specs, total_equity_eur,
        market_regimes or {},
        market_capital_fractions,
    )
    dossier = build_readiness_dossier(
        pipeline["promotion_gate"],
        pipeline["queen_handoff"],
        pipeline["runner_intake"],
        pipeline["replay_validation"],
        pipeline["handoff_consistency"],
        pipeline["execution_ledger"],
        pipeline["audit_trace"],
    )
    review = build_human_review_summary(dossier)
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
        "paper_readiness_dossier":  dossier,
        "human_review_summary":     review,
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

    result  = build_dossier_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    dossier = result["paper_readiness_dossier"]
    review  = result["human_review_summary"]

    print(json.dumps({
        "dossier_status":          dossier["dossier_status"],
        "dossier_mode":            dossier["dossier_mode"],
        "dossier_ready_for_review": dossier["dossier_ready_for_review"],
        "dossier_reason_code":     dossier["dossier_reason_code"],
        "readiness_counts":        dossier["readiness_counts"],
        "dossier_non_binding":     dossier["dossier_non_binding"],
        "paper_only":              dossier["paper_only"],
        "live_activation_allowed": dossier["live_activation_allowed"],
        "review_status":           review["review_status"],
        "review_mode":             review["review_mode"],
        "review_priority":         review["review_priority"],
        "review_decision_hint":    review["review_decision_hint"],
        "key_findings":            review["key_findings"],
        "blocking_findings":       review["blocking_findings"],
        "review_non_binding":      review["review_non_binding"],
        "review_simulation_only":  review["review_simulation_only"],
    }, indent=2))


if __name__ == "__main__":
    main()
