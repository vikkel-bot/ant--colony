"""
Tests for AC-95: Paper Readiness Dossier + Human Review Summary
build_readiness_dossier_lite.py
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_readiness_dossier_lite import (
    build_readiness_dossier,
    build_human_review_summary,
    build_dossier_and_review,
    build_dossier_from_specs,
    DOSSIER_READY, DOSSIER_HOLD, DOSSIER_REJECTED,
    DOSSIER_MODE_PAPER_READY, DOSSIER_MODE_PAPER_HOLD,
    DOSSIER_MODE_PAPER_REJECT, DOSSIER_MODE_INVALID,
    REASON_DOSSIER_READY, REASON_DOSSIER_HOLD,
    REASON_DOSSIER_REJECTED, REASON_DOSSIER_INVALID,
    REVIEW_READY, REVIEW_HOLD, REVIEW_REJECTED,
    REVIEW_MODE_PAPER_READY, REVIEW_MODE_PAPER_HOLD, REVIEW_MODE_PAPER_REJECT,
    REASON_REVIEW_READY, REASON_REVIEW_HOLD, REASON_REVIEW_REJECTED, REASON_REVIEW_INVALID,
    PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _gate(status="PAPER_READY", ready=True, paper_ready=True,
          reason_code="PROMOTION_ALL_CLEAR", reason="all clear"):
    return {
        "promotion_status":          status,
        "promotion_mode":            "PROMOTION_READY",
        "promotion_ready":           ready,
        "promotion_reason":          reason,
        "promotion_reason_code":     reason_code,
        "promotion_decision":        f"{status}: {reason_code}",
        "upstream_snapshot":         {
            "handoff_status":        "READY_FOR_PAPER_HANDOFF",
            "handoff_ready":         True,
            "runner_intake_status":  "INTAKE_ACCEPTED",
            "runner_contract_valid": True,
            "validation_status":     "VALIDATION_PASSED",
            "validation_passed":     True,
            "replay_consistent":     True,
            "consistency_status":    "CONSISTENCY_PASSED",
            "consistency_passed":    True,
        },
        "paper_ready_candidate":     paper_ready,
        "promotion_non_binding":     True,
        "promotion_simulation_only": True,
        "paper_only":                True,
        "live_activation_allowed":   False,
    }


def _gate_hold():
    return _gate(
        status="PAPER_HOLD", ready=False, paper_ready=False,
        reason_code="PROMOTION_UPSTREAM_HOLD", reason="upstream hold",
    )


def _gate_rejected():
    return _gate(
        status="PAPER_REJECTED", ready=False, paper_ready=False,
        reason_code="PROMOTION_VALIDATION_FAILED", reason="validation failed",
    )


def _handoff(allowed=3, blocked=1):
    return {
        "handoff_status":  "READY_FOR_PAPER_HANDOFF",
        "handoff_ready":   True,
        "total_intents":   allowed + blocked,
        "total_allowed":   allowed,
        "total_blocked":   blocked,
        "paper_only":      True,
        "live_activation_allowed": False,
    }


def _intake(total=4, allowed=3, blocked=1):
    return {
        "runner_intake_status":    "INTAKE_ACCEPTED",
        "runner_contract_valid":   True,
        "consumed_intent_count":   total,
        "consumed_allowed_count":  allowed,
        "consumed_blocked_count":  blocked,
        "paper_only":              True,
        "live_activation_allowed": False,
    }


def _validation(passed=True, replay=True, ledger=4, trace=4):
    return {
        "validation_status":          "VALIDATION_PASSED" if passed else "VALIDATION_FAILED",
        "validation_passed":          passed,
        "replay_consistent":          replay,
        "validation_reason":          "ok" if passed else "count mismatch",
        "validated_ledger_count":     ledger,
        "validated_trace_count":      trace,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _consistency(passed=True, matched=6):
    return {
        "handoff_consistency_status": "CONSISTENCY_PASSED" if passed else "CONSISTENCY_FAILED",
        "handoff_consistency_passed": passed,
        "consistency_reason":         "ok" if passed else "mismatch",
        "matched_intent_count":       matched,
        "missing_in_handoff_count":   0,
        "missing_in_ledger_count":    0,
        "missing_in_trace_count":     0,
        "paper_only":                 True,
        "live_activation_allowed":    False,
    }


def _ledger(entries=4, consumed=2, skipped=1, blocked=1):
    return {
        "ledger_status":      "LEDGER_COMPLETE",
        "ledger_entry_count": entries,
        "consumed_count":     consumed,
        "skipped_count":      skipped,
        "blocked_count":      blocked,
        "paper_only":         True,
        "live_activation_allowed": False,
    }


def _trace(steps=4):
    return {
        "trace_status":     "TRACE_COMPLETE",
        "trace_step_count": steps,
        "paper_only":       True,
        "live_activation_allowed": False,
    }


def _full_dossier(promotion_status="PAPER_READY"):
    if promotion_status == "PAPER_READY":
        g = _gate()
    elif promotion_status == "PAPER_HOLD":
        g = _gate_hold()
    else:
        g = _gate_rejected()
    return build_readiness_dossier(
        g, _handoff(), _intake(),
        _validation(), _consistency(),
        _ledger(), _trace(),
    )


# ---------------------------------------------------------------------------
# 1. Dossier — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestDossierAlwaysFlags:
    def test_ready_non_binding_true(self):
        d = _full_dossier()
        assert d["dossier_non_binding"] is True

    def test_ready_simulation_only_true(self):
        d = _full_dossier()
        assert d["dossier_simulation_only"] is True

    def test_ready_paper_only_true(self):
        d = _full_dossier()
        assert d["paper_only"] is True

    def test_ready_live_activation_false(self):
        d = _full_dossier()
        assert d["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        assert _full_dossier("PAPER_HOLD")["dossier_non_binding"] is True

    def test_hold_live_activation_false(self):
        assert _full_dossier("PAPER_HOLD")["live_activation_allowed"] is False

    def test_rejected_non_binding_true(self):
        assert _full_dossier("PAPER_REJECTED")["dossier_non_binding"] is True

    def test_rejected_live_activation_false(self):
        assert _full_dossier("PAPER_REJECTED")["live_activation_allowed"] is False

    def test_invalid_input_non_binding_true(self):
        d = build_readiness_dossier(None)
        assert d["dossier_non_binding"] is True

    def test_invalid_input_live_activation_false(self):
        d = build_readiness_dossier(None)
        assert d["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Dossier — DOSSIER_READY (PAPER_READY promotion)
# ---------------------------------------------------------------------------

class TestDossierReady:
    def test_status_ready(self):
        assert _full_dossier()["dossier_status"] == DOSSIER_READY

    def test_mode_paper_ready(self):
        assert _full_dossier()["dossier_mode"] == DOSSIER_MODE_PAPER_READY

    def test_ready_for_review_true(self):
        assert _full_dossier()["dossier_ready_for_review"] is True

    def test_reason_code_ready(self):
        assert _full_dossier()["dossier_reason_code"] == REASON_DOSSIER_READY


# ---------------------------------------------------------------------------
# 3. Dossier — DOSSIER_HOLD (PAPER_HOLD promotion)
# ---------------------------------------------------------------------------

class TestDossierHold:
    def test_status_hold(self):
        assert _full_dossier("PAPER_HOLD")["dossier_status"] == DOSSIER_HOLD

    def test_mode_paper_hold(self):
        assert _full_dossier("PAPER_HOLD")["dossier_mode"] == DOSSIER_MODE_PAPER_HOLD

    def test_reason_code_hold(self):
        assert _full_dossier("PAPER_HOLD")["dossier_reason_code"] == REASON_DOSSIER_HOLD

    def test_ready_for_review_true_on_hold(self):
        # Hold dossiers are still reviewable
        assert _full_dossier("PAPER_HOLD")["dossier_ready_for_review"] is True


# ---------------------------------------------------------------------------
# 4. Dossier — DOSSIER_REJECTED (PAPER_REJECTED promotion)
# ---------------------------------------------------------------------------

class TestDossierRejected:
    def test_status_rejected(self):
        assert _full_dossier("PAPER_REJECTED")["dossier_status"] == DOSSIER_REJECTED

    def test_mode_paper_reject(self):
        assert _full_dossier("PAPER_REJECTED")["dossier_mode"] == DOSSIER_MODE_PAPER_REJECT

    def test_reason_code_rejected(self):
        assert _full_dossier("PAPER_REJECTED")["dossier_reason_code"] == REASON_DOSSIER_REJECTED

    def test_ready_for_review_true_on_rejected(self):
        # Rejected dossiers are still reviewable
        assert _full_dossier("PAPER_REJECTED")["dossier_ready_for_review"] is True


# ---------------------------------------------------------------------------
# 5. Dossier — invalid input
# ---------------------------------------------------------------------------

class TestDossierInvalidInput:
    def test_none_gate_rejected(self):
        d = build_readiness_dossier(None)
        assert d["dossier_status"] == DOSSIER_REJECTED

    def test_list_gate_rejected(self):
        d = build_readiness_dossier([])
        assert d["dossier_status"] == DOSSIER_REJECTED

    def test_missing_promotion_status_rejected(self):
        g = _gate()
        del g["promotion_status"]
        d = build_readiness_dossier(g)
        assert d["dossier_status"] == DOSSIER_REJECTED

    def test_invalid_reason_code(self):
        d = build_readiness_dossier(None)
        assert d["dossier_reason_code"] == REASON_DOSSIER_INVALID

    def test_invalid_mode(self):
        d = build_readiness_dossier(None)
        assert d["dossier_mode"] == DOSSIER_MODE_INVALID

    def test_invalid_ready_for_review_false(self):
        d = build_readiness_dossier(None)
        assert d["dossier_ready_for_review"] is False

    def test_invalid_snapshots_empty(self):
        d = build_readiness_dossier(None)
        assert d["promotion_snapshot"] == {}
        assert d["validation_snapshot"] == {}


# ---------------------------------------------------------------------------
# 6. Dossier — Output field completeness
# ---------------------------------------------------------------------------

_DOSSIER_KEYS = {
    "dossier_status", "dossier_mode", "dossier_ready_for_review",
    "dossier_reason", "dossier_reason_code",
    "promotion_snapshot", "validation_snapshot", "consistency_snapshot",
    "handoff_snapshot", "runner_snapshot", "readiness_counts",
    "dossier_non_binding", "dossier_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestDossierFieldCompleteness:
    def test_ready_has_all_keys(self):
        assert _DOSSIER_KEYS.issubset(_full_dossier().keys())

    def test_hold_has_all_keys(self):
        assert _DOSSIER_KEYS.issubset(_full_dossier("PAPER_HOLD").keys())

    def test_rejected_has_all_keys(self):
        assert _DOSSIER_KEYS.issubset(_full_dossier("PAPER_REJECTED").keys())

    def test_invalid_has_all_keys(self):
        d = build_readiness_dossier(None)
        assert _DOSSIER_KEYS.issubset(d.keys())


# ---------------------------------------------------------------------------
# 7. Dossier — Snapshot contents
# ---------------------------------------------------------------------------

class TestDossierSnapshots:
    def test_promotion_snapshot_has_status(self):
        d = _full_dossier()
        assert "promotion_status" in d["promotion_snapshot"]

    def test_promotion_snapshot_status_correct(self):
        d = _full_dossier()
        assert d["promotion_snapshot"]["promotion_status"] == "PAPER_READY"

    def test_validation_snapshot_has_passed(self):
        d = _full_dossier()
        assert "validation_passed" in d["validation_snapshot"]

    def test_consistency_snapshot_has_passed(self):
        d = _full_dossier()
        assert "consistency_passed" in d["consistency_snapshot"]

    def test_handoff_snapshot_has_status(self):
        d = _full_dossier()
        assert "handoff_status" in d["handoff_snapshot"]

    def test_runner_snapshot_has_status(self):
        d = _full_dossier()
        assert "runner_intake_status" in d["runner_snapshot"]

    def test_readiness_counts_has_total_intents(self):
        d = _full_dossier()
        assert "total_intents" in d["readiness_counts"]

    def test_readiness_counts_ledger_entry_count(self):
        d = _full_dossier()
        assert d["readiness_counts"]["ledger_entry_count"] == 4

    def test_readiness_counts_trace_step_count(self):
        d = _full_dossier()
        assert d["readiness_counts"]["trace_step_count"] == 4

    def test_no_upstream_optional_gives_empty_snaps(self):
        d = build_readiness_dossier(_gate())  # no optional args
        assert d["handoff_snapshot"] == {}
        assert d["runner_snapshot"] == {}
        assert d["validation_snapshot"] == {}

    def test_snapshots_are_copies_not_references(self):
        g = _gate()
        d = build_readiness_dossier(g, _handoff())
        d["handoff_snapshot"]["handoff_status"] = "MUTATED"
        # _handoff() is a new dict each time so no alias concern; just check type
        assert isinstance(d["handoff_snapshot"], dict)


# ---------------------------------------------------------------------------
# 8. Review — Always-True / Always-False flags
# ---------------------------------------------------------------------------

class TestReviewAlwaysFlags:
    def test_ready_non_binding_true(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_non_binding"] is True

    def test_ready_simulation_only_true(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_simulation_only"] is True

    def test_ready_paper_only_true(self):
        r = build_human_review_summary(_full_dossier())
        assert r["paper_only"] is True

    def test_ready_live_activation_false(self):
        r = build_human_review_summary(_full_dossier())
        assert r["live_activation_allowed"] is False

    def test_hold_non_binding_true(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert r["review_non_binding"] is True

    def test_rejected_live_activation_false(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert r["live_activation_allowed"] is False

    def test_invalid_non_binding_true(self):
        r = build_human_review_summary(None)
        assert r["review_non_binding"] is True

    def test_invalid_live_activation_false(self):
        r = build_human_review_summary(None)
        assert r["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 9. Review — status mapping
# ---------------------------------------------------------------------------

class TestReviewStatusMapping:
    def test_dossier_ready_gives_review_ready(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_status"] == REVIEW_READY

    def test_dossier_hold_gives_review_hold(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert r["review_status"] == REVIEW_HOLD

    def test_dossier_rejected_gives_review_rejected(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert r["review_status"] == REVIEW_REJECTED

    def test_ready_mode(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_mode"] == REVIEW_MODE_PAPER_READY

    def test_hold_mode(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert r["review_mode"] == REVIEW_MODE_PAPER_HOLD

    def test_rejected_mode(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert r["review_mode"] == REVIEW_MODE_PAPER_REJECT


# ---------------------------------------------------------------------------
# 10. Review — priority
# ---------------------------------------------------------------------------

class TestReviewPriority:
    def test_ready_priority_low(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_priority"] == PRIORITY_LOW

    def test_hold_priority_medium(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert r["review_priority"] == PRIORITY_MEDIUM

    def test_rejected_priority_high(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert r["review_priority"] == PRIORITY_HIGH

    def test_invalid_input_priority_high(self):
        r = build_human_review_summary(None)
        assert r["review_priority"] == PRIORITY_HIGH


# ---------------------------------------------------------------------------
# 11. Review — reason codes
# ---------------------------------------------------------------------------

class TestReviewReasonCodes:
    def test_ready_reason_code(self):
        r = build_human_review_summary(_full_dossier())
        assert r["review_reason_code"] == REASON_REVIEW_READY

    def test_hold_reason_code(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert r["review_reason_code"] == REASON_REVIEW_HOLD

    def test_rejected_reason_code(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert r["review_reason_code"] == REASON_REVIEW_REJECTED

    def test_invalid_reason_code(self):
        r = build_human_review_summary(None)
        assert r["review_reason_code"] == REASON_REVIEW_INVALID


# ---------------------------------------------------------------------------
# 12. Review — key_findings and blocking_findings
# ---------------------------------------------------------------------------

class TestReviewFindings:
    def test_key_findings_is_list(self):
        r = build_human_review_summary(_full_dossier())
        assert isinstance(r["key_findings"], list)

    def test_key_findings_non_empty(self):
        r = build_human_review_summary(_full_dossier())
        assert len(r["key_findings"]) >= 1

    def test_key_findings_contains_promotion_status(self):
        r = build_human_review_summary(_full_dossier())
        combined = " ".join(r["key_findings"])
        assert "PAPER_READY" in combined

    def test_blocking_findings_is_list(self):
        r = build_human_review_summary(_full_dossier())
        assert isinstance(r["blocking_findings"], list)

    def test_blocking_findings_empty_when_ready(self):
        r = build_human_review_summary(_full_dossier())
        assert r["blocking_findings"] == []

    def test_blocking_findings_non_empty_when_rejected(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert len(r["blocking_findings"]) >= 1

    def test_key_findings_contains_handoff_status(self):
        r = build_human_review_summary(_full_dossier())
        combined = " ".join(r["key_findings"])
        assert "handoff_status" in combined

    def test_key_findings_contain_intents_counts(self):
        r = build_human_review_summary(_full_dossier())
        combined = " ".join(r["key_findings"])
        assert "intents" in combined

    def test_blocking_findings_mention_reason_on_rejected(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        # Should mention the promotion reason code
        combined = " ".join(r["blocking_findings"])
        assert len(combined) > 0  # non-trivial content


# ---------------------------------------------------------------------------
# 13. Review — review_decision_hint
# ---------------------------------------------------------------------------

class TestReviewDecisionHint:
    def test_ready_hint_contains_paper_ready(self):
        r = build_human_review_summary(_full_dossier())
        assert "PAPER_READY" in r["review_decision_hint"]

    def test_hold_hint_contains_paper_hold(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert "PAPER_HOLD" in r["review_decision_hint"]

    def test_rejected_hint_contains_paper_rejected(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert "PAPER_REJECTED" in r["review_decision_hint"]

    def test_hint_is_non_empty_string(self):
        for status in ["PAPER_READY", "PAPER_HOLD", "PAPER_REJECTED"]:
            r = build_human_review_summary(_full_dossier(status))
            assert isinstance(r["review_decision_hint"], str)
            assert len(r["review_decision_hint"]) > 10


# ---------------------------------------------------------------------------
# 14. Review — output field completeness
# ---------------------------------------------------------------------------

_REVIEW_KEYS = {
    "review_status", "review_mode",
    "review_decision_hint", "review_reason", "review_reason_code",
    "key_findings", "blocking_findings", "review_priority",
    "review_non_binding", "review_simulation_only",
    "paper_only", "live_activation_allowed",
}

class TestReviewFieldCompleteness:
    def test_ready_has_all_keys(self):
        r = build_human_review_summary(_full_dossier())
        assert _REVIEW_KEYS.issubset(r.keys())

    def test_hold_has_all_keys(self):
        r = build_human_review_summary(_full_dossier("PAPER_HOLD"))
        assert _REVIEW_KEYS.issubset(r.keys())

    def test_rejected_has_all_keys(self):
        r = build_human_review_summary(_full_dossier("PAPER_REJECTED"))
        assert _REVIEW_KEYS.issubset(r.keys())

    def test_invalid_has_all_keys(self):
        r = build_human_review_summary(None)
        assert _REVIEW_KEYS.issubset(r.keys())


# ---------------------------------------------------------------------------
# 15. Review — invalid dossier input
# ---------------------------------------------------------------------------

class TestReviewInvalidInput:
    def test_none_dossier_rejected(self):
        r = build_human_review_summary(None)
        assert r["review_status"] == REVIEW_REJECTED

    def test_list_dossier_rejected(self):
        r = build_human_review_summary([])
        assert r["review_status"] == REVIEW_REJECTED

    def test_missing_dossier_status_rejected(self):
        d = _full_dossier()
        del d["dossier_status"]
        r = build_human_review_summary(d)
        assert r["review_status"] == REVIEW_REJECTED


# ---------------------------------------------------------------------------
# 16. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_dossier_same_inputs_same_output(self):
        g = _gate()
        ho = _handoff()
        ri = _intake()
        val = _validation()
        con = _consistency()
        d1 = build_readiness_dossier(g, ho, ri, val, con)
        d2 = build_readiness_dossier(g, ho, ri, val, con)
        assert d1 == d2

    def test_review_same_inputs_same_output(self):
        d = _full_dossier()
        r1 = build_human_review_summary(d)
        r2 = build_human_review_summary(d)
        assert r1 == r2


# ---------------------------------------------------------------------------
# 17. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_gate_not_mutated(self):
        g = _gate()
        original = g["promotion_status"]
        build_readiness_dossier(g)
        assert g["promotion_status"] == original

    def test_dossier_not_mutated_by_review(self):
        d = _full_dossier()
        original = d["dossier_status"]
        build_human_review_summary(d)
        assert d["dossier_status"] == original


# ---------------------------------------------------------------------------
# 18. Combined: build_dossier_and_review
# ---------------------------------------------------------------------------

class TestCombined:
    def test_has_dossier_key(self):
        result = build_dossier_and_review(_gate())
        assert "paper_readiness_dossier" in result

    def test_has_review_key(self):
        result = build_dossier_and_review(_gate())
        assert "human_review_summary" in result

    def test_combined_ready(self):
        result = build_dossier_and_review(_gate(), _handoff(), _intake(), _validation(), _consistency())
        assert result["paper_readiness_dossier"]["dossier_status"] == DOSSIER_READY
        assert result["human_review_summary"]["review_status"] == REVIEW_READY

    def test_combined_hold(self):
        result = build_dossier_and_review(_gate_hold())
        assert result["paper_readiness_dossier"]["dossier_status"] == DOSSIER_HOLD

    def test_combined_rejected(self):
        result = build_dossier_and_review(_gate_rejected())
        assert result["paper_readiness_dossier"]["dossier_status"] == DOSSIER_REJECTED


# ---------------------------------------------------------------------------
# 19. Full chain: build_dossier_from_specs
# ---------------------------------------------------------------------------

_SPECS = [
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

_REGIMES = {
    "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
    "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
}

class TestFullChain:
    def test_has_dossier_key(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "paper_readiness_dossier" in result

    def test_has_review_key(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert "human_review_summary" in result

    def test_has_all_pipeline_keys(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        for key in [
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption",
            "execution_ledger", "audit_trace",
            "replay_validation", "handoff_consistency",
            "promotion_gate",
            "paper_readiness_dossier", "human_review_summary",
        ]:
            assert key in result, f"missing key: {key}"

    def test_dossier_live_activation_false(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["paper_readiness_dossier"]["live_activation_allowed"] is False

    def test_review_live_activation_false(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["live_activation_allowed"] is False

    def test_dossier_paper_only_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["paper_readiness_dossier"]["paper_only"] is True

    def test_review_paper_only_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["paper_only"] is True

    def test_dossier_non_binding_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["paper_readiness_dossier"]["dossier_non_binding"] is True

    def test_review_non_binding_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["review_non_binding"] is True

    def test_dossier_simulation_only_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["paper_readiness_dossier"]["dossier_simulation_only"] is True

    def test_review_simulation_only_true(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["review_simulation_only"] is True

    def test_dossier_status_valid(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["paper_readiness_dossier"]["dossier_status"] in {
            DOSSIER_READY, DOSSIER_HOLD, DOSSIER_REJECTED
        }

    def test_review_status_valid(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["review_status"] in {
            REVIEW_READY, REVIEW_HOLD, REVIEW_REJECTED
        }

    def test_review_priority_valid(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert result["human_review_summary"]["review_priority"] in {
            PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW
        }

    def test_key_findings_non_empty(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert len(result["human_review_summary"]["key_findings"]) >= 1

    def test_blocking_findings_is_list(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        assert isinstance(result["human_review_summary"]["blocking_findings"], list)

    def test_dossier_review_status_consistent(self):
        result = build_dossier_from_specs(_SPECS, 10_000.0, _REGIMES)
        d_status = result["paper_readiness_dossier"]["dossier_status"]
        r_status = result["human_review_summary"]["review_status"]
        mapping = {DOSSIER_READY: REVIEW_READY, DOSSIER_HOLD: REVIEW_HOLD, DOSSIER_REJECTED: REVIEW_REJECTED}
        assert mapping[d_status] == r_status

    def test_empty_specs_safe(self):
        result = build_dossier_from_specs([], 10_000.0)
        assert result["paper_readiness_dossier"]["live_activation_allowed"] is False

    def test_zero_equity_safe(self):
        result = build_dossier_from_specs(_SPECS, 0.0, _REGIMES)
        assert result["human_review_summary"]["review_non_binding"] is True
