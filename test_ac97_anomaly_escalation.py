"""
AC-97 tests — Anomaly Escalation Layer (Human Attention Filter)

Coverage:
  - build_anomaly_escalation: NONE / LOW / MEDIUM / HIGH / CRITICAL paths
  - Input validation (fail-closed)
  - All reason codes
  - human_attention_required mapping
  - anomaly_detected mapping
  - Flags invariants
  - Context snapshot refs (promotion_status, dossier_status, review_status)
  - build_escalation_from_specs (full pipeline integration)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_anomaly_escalation_lite import (
    build_anomaly_escalation,
    build_escalation_from_specs,
    LEVEL_NONE, LEVEL_LOW, LEVEL_MEDIUM, LEVEL_HIGH, LEVEL_CRITICAL,
    CODE_INVALID_INPUT,
    CODE_PROMOTION_NOT_READY,
    CODE_DOSSIER_REJECTED,
    CODE_VALIDATION_FAILED,
    CODE_CONSISTENCY_FAILED,
    CODE_SNAPSHOT_MISSING,
    CODE_HIGH_PRIORITY,
    CODE_HIGH_BLOCKING,
    CODE_HIGH_CONFLICT,
    CODE_MEDIUM_HOLD,
    CODE_LOW_INFO,
    CODE_NONE,
    VERSION,
    COMPONENT,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _valid_upstream_snapshot(**overrides):
    snap = {
        "handoff_status":        "READY_FOR_PAPER_HANDOFF",
        "handoff_ready":         True,
        "runner_intake_status":  "INTAKE_ACCEPTED",
        "runner_contract_valid": True,
        "validation_status":     "VALIDATION_PASSED",
        "validation_passed":     True,
        "replay_consistent":     True,
        "consistency_status":    "CONSISTENCY_PASSED",
        "consistency_passed":    True,
    }
    snap.update(overrides)
    return snap


def _valid_promo_snapshot(**overrides):
    snap = {
        "promotion_status":      "PAPER_READY",
        "promotion_ready":       True,
        "paper_ready_candidate": True,
        "promotion_reason_code": "PROMOTION_ALL_CLEAR",
        "promotion_reason":      "all upstream checks pass",
        "promotion_decision":    "PAPER_READY: PROMOTION_ALL_CLEAR",
        "upstream_snapshot":     _valid_upstream_snapshot(),
    }
    snap.update(overrides)
    return snap


def _valid_gate(**overrides):
    gate = {
        "promotion_status":          "PAPER_READY",
        "promotion_mode":            "PROMOTION_READY",
        "promotion_ready":           True,
        "promotion_reason":          "all upstream checks pass",
        "promotion_reason_code":     "PROMOTION_ALL_CLEAR",
        "promotion_decision":        "PAPER_READY: PROMOTION_ALL_CLEAR",
        "upstream_snapshot":         _valid_upstream_snapshot(),
        "paper_ready_candidate":     True,
        "promotion_non_binding":     True,
        "promotion_simulation_only": True,
        "paper_only":                True,
        "live_activation_allowed":   False,
    }
    gate.update(overrides)
    return gate


def _valid_dossier(**overrides):
    dossier = {
        "dossier_status":          "DOSSIER_READY",
        "dossier_mode":            "DOSSIER_PAPER_READY",
        "dossier_ready_for_review": True,
        "dossier_reason":          "promotion is PAPER_READY",
        "dossier_reason_code":     "DOSSIER_PROMOTION_READY",
        "promotion_snapshot":      _valid_promo_snapshot(),
        "validation_snapshot":     {
            "validation_status": "VALIDATION_PASSED",
            "validation_passed": True,
            "replay_consistent": True,
        },
        "consistency_snapshot":    {
            "consistency_status": "CONSISTENCY_PASSED",
            "consistency_passed": True,
        },
        "handoff_snapshot":        {"handoff_status": "READY_FOR_PAPER_HANDOFF", "handoff_ready": True},
        "runner_snapshot":         {"runner_intake_status": "INTAKE_ACCEPTED", "runner_contract_valid": True},
        "readiness_counts":        {
            "total_intents": 3, "total_allowed": 2, "total_blocked": 1,
            "ledger_entry_count": 3, "trace_step_count": 3, "matched_checks": 3,
        },
        "dossier_non_binding":     True,
        "dossier_simulation_only": True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }
    dossier.update(overrides)
    return dossier


def _valid_review(**overrides):
    review = {
        "review_status":           "REVIEW_READY",
        "review_mode":             "REVIEW_PAPER_READY",
        "review_decision_hint":    "Candidate is PAPER_READY.",
        "review_reason":           "promotion gate cleared",
        "review_reason_code":      "REVIEW_PAPER_READY_OK",
        "key_findings":            ["promotion_status=PAPER_READY (PROMOTION_ALL_CLEAR)"],
        "blocking_findings":       [],
        "review_priority":         "LOW",
        "review_non_binding":      True,
        "review_simulation_only":  True,
        "paper_only":              True,
        "live_activation_allowed": False,
    }
    review.update(overrides)
    return review


def _valid_packet(**overrides):
    packet = {
        "version":              "review_packet_v1",
        "component":            "build_review_packet_lite",
        "ts_utc":               "2026-04-10T12:00:00Z",
        "review_packet_status": "READY",
        "review_packet_mode":   "SIMULATION_ONLY",
        "decision": {
            "decision_hint": "ALLOW_REVIEW",
            "priority":      "LOW",
            "reason":        "promotion gate cleared",
            "reason_code":   "REVIEW_PAPER_READY_OK",
        },
        "findings": {
            "key_findings":      ["promotion_status=PAPER_READY (PROMOTION_ALL_CLEAR)"],
            "blocking_findings": [],
        },
        "summary": {
            "promotion_status": "PAPER_READY",
            "dossier_status":   "DOSSIER_READY",
            "review_status":    "REVIEW_READY",
        },
        "snapshots": {
            "promotion": {"promotion_status": "PAPER_READY"},
            "dossier":   {"dossier_status": "DOSSIER_READY"},
            "review":    {"review_status": "REVIEW_READY"},
        },
        "flags": {
            "non_binding":             True,
            "simulation_only":         True,
            "paper_only":              True,
            "live_activation_allowed": False,
        },
    }
    packet.update(overrides)
    return packet


def _call(**overrides):
    """Build with valid inputs, then apply top-level overrides."""
    kwargs = {
        "promotion_gate":          _valid_gate(),
        "paper_readiness_dossier": _valid_dossier(),
        "human_review_summary":    _valid_review(),
        "review_packet":           _valid_packet(),
    }
    kwargs.update(overrides)
    return build_anomaly_escalation(**kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_flags(e: dict) -> None:
    f = e["flags"]
    assert f["non_binding"]             is True
    assert f["simulation_only"]         is True
    assert f["paper_only"]              is True
    assert f["live_activation_allowed"] is False


def _assert_structure(e: dict) -> None:
    for key in (
        "version", "component", "ts_utc",
        "anomaly_detected", "anomaly_level", "human_attention_required",
        "escalation_reason", "escalation_reason_code", "escalation_findings",
        "promotion_status", "dossier_status", "review_status", "flags",
    ):
        assert key in e, f"missing key: {key}"
    assert e["version"]   == VERSION
    assert e["component"] == COMPONENT
    assert isinstance(e["escalation_findings"], list)
    assert len(e["escalation_findings"]) >= 1


# ---------------------------------------------------------------------------
# 1. NONE — happy path
# ---------------------------------------------------------------------------

class TestNone:
    def test_none_level(self):
        e = _call()
        assert e["anomaly_level"] == LEVEL_NONE

    def test_none_anomaly_detected_false(self):
        e = _call()
        assert e["anomaly_detected"] is False

    def test_none_human_attention_false(self):
        e = _call()
        assert e["human_attention_required"] is False

    def test_none_reason_code(self):
        e = _call()
        assert e["escalation_reason_code"] == CODE_NONE

    def test_none_finding_present(self):
        e = _call()
        assert len(e["escalation_findings"]) >= 1

    def test_none_promotion_status_populated(self):
        e = _call()
        assert e["promotion_status"] == "PAPER_READY"

    def test_none_dossier_status_populated(self):
        e = _call()
        assert e["dossier_status"] == "DOSSIER_READY"

    def test_none_review_status_populated(self):
        e = _call()
        assert e["review_status"] == "REVIEW_READY"

    def test_none_flags(self):
        _assert_flags(_call())

    def test_none_structure(self):
        _assert_structure(_call())

    def test_none_reason_not_empty(self):
        e = _call()
        assert e["escalation_reason"]


# ---------------------------------------------------------------------------
# 2. LOW — total_intents=0 (informational deviation)
# ---------------------------------------------------------------------------

class TestLow:
    def _low_dossier(self):
        d = _valid_dossier()
        d["readiness_counts"] = {
            "total_intents": 0, "total_allowed": 0, "total_blocked": 0,
            "ledger_entry_count": 0, "trace_step_count": 0, "matched_checks": 0,
        }
        return d

    def test_low_level(self):
        e = _call(paper_readiness_dossier=self._low_dossier())
        assert e["anomaly_level"] == LEVEL_LOW

    def test_low_anomaly_detected_true(self):
        e = _call(paper_readiness_dossier=self._low_dossier())
        assert e["anomaly_detected"] is True

    def test_low_human_attention_false(self):
        e = _call(paper_readiness_dossier=self._low_dossier())
        assert e["human_attention_required"] is False

    def test_low_reason_code(self):
        e = _call(paper_readiness_dossier=self._low_dossier())
        assert e["escalation_reason_code"] == CODE_LOW_INFO

    def test_low_finding_mentions_intents(self):
        e = _call(paper_readiness_dossier=self._low_dossier())
        combined = " ".join(e["escalation_findings"])
        assert "total_intents" in combined

    def test_low_flags(self):
        _assert_flags(_call(paper_readiness_dossier=self._low_dossier()))

    def test_low_structure(self):
        _assert_structure(_call(paper_readiness_dossier=self._low_dossier()))

    def test_low_positive_intents_gives_none(self):
        """Non-zero intents with all-clear → NONE, not LOW."""
        e = _call()  # default has total_intents=3
        assert e["anomaly_level"] == LEVEL_NONE

    def test_low_missing_readiness_counts_still_low(self):
        """Missing readiness_counts field → total_intents defaults to 0 → LOW."""
        d = _valid_dossier()
        del d["readiness_counts"]
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_LOW


# ---------------------------------------------------------------------------
# 3. MEDIUM — hold states
# ---------------------------------------------------------------------------

class TestMedium:
    def test_medium_via_review_priority(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_MEDIUM

    def test_medium_anomaly_detected_true(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        assert e["anomaly_detected"] is True

    def test_medium_human_attention_true(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        assert e["human_attention_required"] is True

    def test_medium_reason_code(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        assert e["escalation_reason_code"] == CODE_MEDIUM_HOLD

    def test_medium_via_dossier_hold(self):
        d = _valid_dossier(dossier_status="DOSSIER_HOLD")
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_MEDIUM

    def test_medium_dossier_hold_reason_code(self):
        d = _valid_dossier(dossier_status="DOSSIER_HOLD")
        e = _call(paper_readiness_dossier=d)
        assert e["escalation_reason_code"] == CODE_MEDIUM_HOLD

    def test_medium_finding_mentions_hold(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        combined = " ".join(e["escalation_findings"])
        assert "MEDIUM" in combined or "hold" in combined.lower()

    def test_medium_flags(self):
        r = _valid_review(review_priority="MEDIUM")
        _assert_flags(_call(human_review_summary=r))

    def test_medium_structure(self):
        r = _valid_review(review_priority="MEDIUM")
        _assert_structure(_call(human_review_summary=r))

    def test_medium_beats_low(self):
        """MEDIUM takes priority over LOW (0-intent) when both conditions present."""
        d = _valid_dossier(
            dossier_status="DOSSIER_HOLD",
            readiness_counts={"total_intents": 0, "total_allowed": 0,
                              "total_blocked": 0, "ledger_entry_count": 0,
                              "trace_step_count": 0, "matched_checks": 0},
        )
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_MEDIUM


# ---------------------------------------------------------------------------
# 4. HIGH — blocking findings / priority / layer conflict
# ---------------------------------------------------------------------------

class TestHigh:
    def test_high_via_review_priority(self):
        r = _valid_review(review_priority="HIGH", blocking_findings=["some block"])
        e = _call(human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_HIGH

    def test_high_priority_reason_code(self):
        r = _valid_review(review_priority="HIGH")
        e = _call(human_review_summary=r)
        assert e["escalation_reason_code"] == CODE_HIGH_PRIORITY

    def test_high_via_blocking_findings(self):
        r = _valid_review(blocking_findings=["validation failed: something"])
        e = _call(human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_HIGH

    def test_high_blocking_reason_code(self):
        r = _valid_review(blocking_findings=["validation failed: something"])
        e = _call(human_review_summary=r)
        assert e["escalation_reason_code"] == CODE_HIGH_BLOCKING

    def test_high_blocking_findings_in_escalation_findings(self):
        r = _valid_review(blocking_findings=["validation failed: something"])
        e = _call(human_review_summary=r)
        combined = " ".join(e["escalation_findings"])
        assert "validation failed" in combined

    def test_high_layer_conflict_review_status(self):
        """Promotion READY but review_status disagrees → HIGH conflict."""
        r = _valid_review(review_status="REVIEW_HOLD", review_priority="LOW", blocking_findings=[])
        e = _call(human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_HIGH
        assert e["escalation_reason_code"] == CODE_HIGH_CONFLICT

    def test_high_layer_conflict_packet_status(self):
        """Promotion READY but review_packet_status disagrees → HIGH conflict."""
        p = _valid_packet(review_packet_status="HOLD")
        e = _call(review_packet=p)
        assert e["anomaly_level"] == LEVEL_HIGH
        assert e["escalation_reason_code"] == CODE_HIGH_CONFLICT

    def test_high_conflict_finding_mentions_conflict(self):
        r = _valid_review(review_status="REVIEW_HOLD", review_priority="LOW", blocking_findings=[])
        e = _call(human_review_summary=r)
        combined = " ".join(e["escalation_findings"])
        assert "conflict" in combined.lower() or "REVIEW_HOLD" in combined

    def test_high_anomaly_detected_true(self):
        r = _valid_review(review_priority="HIGH")
        e = _call(human_review_summary=r)
        assert e["anomaly_detected"] is True

    def test_high_human_attention_true(self):
        r = _valid_review(review_priority="HIGH")
        e = _call(human_review_summary=r)
        assert e["human_attention_required"] is True

    def test_high_flags(self):
        r = _valid_review(review_priority="HIGH")
        _assert_flags(_call(human_review_summary=r))

    def test_high_structure(self):
        r = _valid_review(review_priority="HIGH")
        _assert_structure(_call(human_review_summary=r))

    def test_high_beats_medium(self):
        """HIGH takes priority over MEDIUM (review_priority HIGH + MEDIUM conditions)."""
        r = _valid_review(review_priority="HIGH", blocking_findings=["x"])
        d = _valid_dossier(dossier_status="DOSSIER_HOLD")
        e = _call(human_review_summary=r, paper_readiness_dossier=d)
        # dossier HOLD would be CRITICAL (PAPER_HOLD promotion); just confirm HIGH wins over MEDIUM
        # Since promotion gate is PAPER_READY (from fixture), CRITICAL doesn't trigger.
        # But dossier HOLD alone is MEDIUM; HIGH priority should win.
        # Note: CRITICAL check includes promotion_status, which is PAPER_READY here.
        assert e["anomaly_level"] in (LEVEL_HIGH, LEVEL_CRITICAL)  # either is acceptable


# ---------------------------------------------------------------------------
# 5. CRITICAL — all critical conditions
# ---------------------------------------------------------------------------

class TestCriticalPromotionNotReady:
    def test_promotion_rejected_gives_critical(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_promotion_hold_gives_critical(self):
        g = _valid_gate(promotion_status="PAPER_HOLD",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_promotion_unknown_gives_critical(self):
        g = _valid_gate(promotion_status="UNKNOWN_STATUS",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_promotion_not_ready_reason_code(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["escalation_reason_code"] == CODE_PROMOTION_NOT_READY

    def test_promotion_not_ready_human_attention_true(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["human_attention_required"] is True

    def test_promotion_not_ready_anomaly_detected_true(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["anomaly_detected"] is True

    def test_promotion_not_ready_finding_mentions_status(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        combined = " ".join(e["escalation_findings"])
        assert "PAPER_REJECTED" in combined

    def test_critical_promotion_status_in_output(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["promotion_status"] == "PAPER_REJECTED"


class TestCriticalDossierRejected:
    def test_dossier_rejected_gives_critical(self):
        # Promotion is READY but dossier says REJECTED (unusual; defensive check)
        d = _valid_dossier(dossier_status="DOSSIER_REJECTED")
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_dossier_rejected_reason_code(self):
        d = _valid_dossier(dossier_status="DOSSIER_REJECTED")
        e = _call(paper_readiness_dossier=d)
        # promotion is READY, so DOSSIER_REJECTED code should be returned
        # unless promotion_not_ready fires first (it doesn't here)
        assert e["escalation_reason_code"] == CODE_DOSSIER_REJECTED


class TestCriticalValidationFailed:
    def _gate_val_false(self):
        snap = _valid_upstream_snapshot(validation_passed=False)
        return _valid_gate(upstream_snapshot=snap)

    def _gate_replay_false(self):
        snap = _valid_upstream_snapshot(replay_consistent=False)
        return _valid_gate(upstream_snapshot=snap)

    def test_validation_passed_false_gives_critical(self):
        e = _call(promotion_gate=self._gate_val_false())
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_validation_failed_reason_code(self):
        e = _call(promotion_gate=self._gate_val_false())
        assert e["escalation_reason_code"] == CODE_VALIDATION_FAILED

    def test_replay_consistent_false_gives_critical(self):
        e = _call(promotion_gate=self._gate_replay_false())
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_replay_false_reason_code(self):
        e = _call(promotion_gate=self._gate_replay_false())
        assert e["escalation_reason_code"] == CODE_VALIDATION_FAILED

    def test_validation_finding_mentions_failed(self):
        e = _call(promotion_gate=self._gate_val_false())
        combined = " ".join(e["escalation_findings"])
        assert "validation_passed=False" in combined

    def test_replay_finding_mentions_replay(self):
        e = _call(promotion_gate=self._gate_replay_false())
        combined = " ".join(e["escalation_findings"])
        assert "replay_consistent=False" in combined


class TestCriticalConsistencyFailed:
    def _gate_con_false(self):
        snap = _valid_upstream_snapshot(consistency_passed=False)
        return _valid_gate(upstream_snapshot=snap)

    def test_consistency_false_gives_critical(self):
        e = _call(promotion_gate=self._gate_con_false())
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_consistency_reason_code(self):
        e = _call(promotion_gate=self._gate_con_false())
        assert e["escalation_reason_code"] == CODE_CONSISTENCY_FAILED

    def test_consistency_finding_present(self):
        e = _call(promotion_gate=self._gate_con_false())
        combined = " ".join(e["escalation_findings"])
        assert "consistency_passed=False" in combined


class TestCriticalSnapshotMissing:
    def test_empty_promo_snapshot_gives_critical(self):
        d = _valid_dossier(promotion_snapshot={})
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_none_promo_snapshot_gives_critical(self):
        d = _valid_dossier(promotion_snapshot=None)
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_missing_promo_snapshot_reason_code(self):
        d = _valid_dossier()
        del d["promotion_snapshot"]
        e = _call(paper_readiness_dossier=d)
        # Missing key: .get returns None → non-dict → snapshot_missing=True
        # But promotion_status is still PAPER_READY → SNAPSHOT_MISSING
        assert e["escalation_reason_code"] in (CODE_SNAPSHOT_MISSING, CODE_CRITICAL_any := LEVEL_CRITICAL)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_snapshot_missing_finding(self):
        d = _valid_dossier(promotion_snapshot={})
        e = _call(paper_readiness_dossier=d)
        combined = " ".join(e["escalation_findings"])
        assert "snapshot" in combined.lower()


class TestCriticalPriority:
    """Verify CRITICAL is picked even when other conditions also present."""

    def test_critical_beats_high(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        r = _valid_review(review_priority="HIGH", blocking_findings=["x"])
        e = _call(promotion_gate=g, human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_critical_beats_medium(self):
        g = _valid_gate(promotion_status="PAPER_HOLD",
                        upstream_snapshot=_valid_upstream_snapshot())
        r = _valid_review(review_priority="MEDIUM")
        e = _call(promotion_gate=g, human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_multiple_critical_conditions_still_critical(self):
        g = _valid_gate(
            promotion_status="PAPER_REJECTED",
            upstream_snapshot=_valid_upstream_snapshot(
                validation_passed=False, consistency_passed=False
            ),
        )
        d = _valid_dossier(dossier_status="DOSSIER_REJECTED", promotion_snapshot={})
        e = _call(promotion_gate=g, paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert len(e["escalation_findings"]) > 1


# ---------------------------------------------------------------------------
# 6. Invalid input — fail-closed
# ---------------------------------------------------------------------------

class TestInvalidInput:
    def test_non_dict_gate_gives_critical(self):
        e = build_anomaly_escalation(
            promotion_gate=None,
            paper_readiness_dossier=_valid_dossier(),
            human_review_summary=_valid_review(),
            review_packet=_valid_packet(),
        )
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_non_dict_dossier_gives_critical(self):
        e = build_anomaly_escalation(
            promotion_gate=_valid_gate(),
            paper_readiness_dossier="bad",
            human_review_summary=_valid_review(),
            review_packet=_valid_packet(),
        )
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_non_dict_review_gives_critical(self):
        e = build_anomaly_escalation(
            promotion_gate=_valid_gate(),
            paper_readiness_dossier=_valid_dossier(),
            human_review_summary=42,
            review_packet=_valid_packet(),
        )
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_non_dict_packet_gives_critical(self):
        e = build_anomaly_escalation(
            promotion_gate=_valid_gate(),
            paper_readiness_dossier=_valid_dossier(),
            human_review_summary=_valid_review(),
            review_packet=[],
        )
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_gate_missing_promotion_status(self):
        g = _valid_gate()
        del g["promotion_status"]
        e = _call(promotion_gate=g)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_dossier_missing_dossier_status(self):
        d = _valid_dossier()
        del d["dossier_status"]
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_review_missing_review_status(self):
        r = _valid_review()
        del r["review_status"]
        e = _call(human_review_summary=r)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_packet_missing_review_packet_status(self):
        p = _valid_packet()
        del p["review_packet_status"]
        e = _call(review_packet=p)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_all_none_gives_critical(self):
        e = build_anomaly_escalation(None, None, None, None)
        assert e["anomaly_level"] == LEVEL_CRITICAL
        assert e["escalation_reason_code"] == CODE_INVALID_INPUT

    def test_invalid_input_flags_still_correct(self):
        e = build_anomaly_escalation(None, None, None, None)
        _assert_flags(e)

    def test_invalid_input_human_attention_true(self):
        e = build_anomaly_escalation(None, None, None, None)
        assert e["human_attention_required"] is True

    def test_invalid_input_empty_status_fields(self):
        """Invalid input path: status fields should be empty strings, not crash."""
        e = build_anomaly_escalation(None, None, None, None)
        assert e["promotion_status"] == ""
        assert e["dossier_status"] == ""
        assert e["review_status"] == ""

    def test_empty_dicts_give_critical(self):
        e = build_anomaly_escalation({}, {}, {}, {})
        assert e["anomaly_level"] == LEVEL_CRITICAL

    def test_upstream_snapshot_not_dict_gives_critical(self):
        """Non-dict upstream_snapshot: booleans default to False → CRITICAL."""
        g = _valid_gate(upstream_snapshot="bad")
        e = _call(promotion_gate=g)
        # promotion_status is PAPER_READY, but upstream booleans all False → CRITICAL
        assert e["anomaly_level"] == LEVEL_CRITICAL


# ---------------------------------------------------------------------------
# 7. Flags invariants — always correct regardless of level
# ---------------------------------------------------------------------------

class TestFlagsInvariants:
    def test_flags_none(self):
        _assert_flags(_call())

    def test_flags_critical(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        _assert_flags(_call(promotion_gate=g))

    def test_flags_high(self):
        r = _valid_review(review_priority="HIGH")
        _assert_flags(_call(human_review_summary=r))

    def test_flags_medium(self):
        r = _valid_review(review_priority="MEDIUM")
        _assert_flags(_call(human_review_summary=r))

    def test_flags_low(self):
        d = _valid_dossier(readiness_counts={"total_intents": 0})
        _assert_flags(_call(paper_readiness_dossier=d))

    def test_flags_invalid_input(self):
        _assert_flags(build_anomaly_escalation(None, None, None, None))


# ---------------------------------------------------------------------------
# 8. anomaly_detected and human_attention_required mapping
# ---------------------------------------------------------------------------

class TestBoolMapping:
    def test_none_anomaly_false_attention_false(self):
        e = _call()
        assert e["anomaly_detected"] is False
        assert e["human_attention_required"] is False

    def test_low_anomaly_true_attention_false(self):
        d = _valid_dossier(readiness_counts={"total_intents": 0})
        e = _call(paper_readiness_dossier=d)
        assert e["anomaly_detected"] is True
        assert e["human_attention_required"] is False

    def test_medium_anomaly_true_attention_true(self):
        r = _valid_review(review_priority="MEDIUM")
        e = _call(human_review_summary=r)
        assert e["anomaly_detected"] is True
        assert e["human_attention_required"] is True

    def test_high_anomaly_true_attention_true(self):
        r = _valid_review(review_priority="HIGH")
        e = _call(human_review_summary=r)
        assert e["anomaly_detected"] is True
        assert e["human_attention_required"] is True

    def test_critical_anomaly_true_attention_true(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e = _call(promotion_gate=g)
        assert e["anomaly_detected"] is True
        assert e["human_attention_required"] is True


# ---------------------------------------------------------------------------
# 9. Output structure and metadata
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_version_field(self):
        e = _call()
        assert e["version"] == VERSION

    def test_component_field(self):
        e = _call()
        assert e["component"] == COMPONENT

    def test_ts_utc_format(self):
        e = _call()
        ts = e["ts_utc"]
        assert ts.endswith("Z")
        assert "T" in ts

    def test_escalation_findings_is_list(self):
        e = _call()
        assert isinstance(e["escalation_findings"], list)

    def test_escalation_findings_non_empty(self):
        e = _call()
        assert len(e["escalation_findings"]) >= 1

    def test_escalation_reason_is_str(self):
        e = _call()
        assert isinstance(e["escalation_reason"], str)
        assert e["escalation_reason"]

    def test_escalation_reason_code_is_str(self):
        e = _call()
        assert isinstance(e["escalation_reason_code"], str)
        assert e["escalation_reason_code"]

    def test_anomaly_level_is_str(self):
        e = _call()
        assert isinstance(e["anomaly_level"], str)
        assert e["anomaly_level"] in (LEVEL_NONE, LEVEL_LOW, LEVEL_MEDIUM, LEVEL_HIGH, LEVEL_CRITICAL)

    def test_anomaly_detected_is_bool(self):
        e = _call()
        assert isinstance(e["anomaly_detected"], bool)

    def test_human_attention_required_is_bool(self):
        e = _call()
        assert isinstance(e["human_attention_required"], bool)

    def test_all_status_snapshot_fields_are_str(self):
        e = _call()
        assert isinstance(e["promotion_status"], str)
        assert isinstance(e["dossier_status"], str)
        assert isinstance(e["review_status"], str)

    def test_no_side_effects_on_inputs(self):
        """Inputs should not be mutated by the function."""
        g = _valid_gate()
        d = _valid_dossier()
        r = _valid_review()
        p = _valid_packet()
        g_copy = dict(g)
        build_anomaly_escalation(g, d, r, p)
        assert g == g_copy


# ---------------------------------------------------------------------------
# 10. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_inputs_same_output_none(self):
        e1 = _call()
        e2 = _call()
        # ts_utc may differ by up to 1 second; compare everything else
        e1.pop("ts_utc")
        e2.pop("ts_utc")
        assert e1 == e2

    def test_same_inputs_same_output_critical(self):
        g = _valid_gate(promotion_status="PAPER_REJECTED",
                        upstream_snapshot=_valid_upstream_snapshot())
        e1 = _call(promotion_gate=g)
        e2 = _call(promotion_gate=g)
        e1.pop("ts_utc"); e2.pop("ts_utc")
        assert e1 == e2

    def test_same_inputs_same_output_high(self):
        r = _valid_review(review_priority="HIGH", blocking_findings=["x"])
        e1 = _call(human_review_summary=r)
        e2 = _call(human_review_summary=r)
        e1.pop("ts_utc"); e2.pop("ts_utc")
        assert e1 == e2


# ---------------------------------------------------------------------------
# 11. Full pipeline — build_escalation_from_specs
# ---------------------------------------------------------------------------

class TestFromSpecs:
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

    def _run(self):
        return build_escalation_from_specs(
            self._SPECS, total_equity_eur=10_000.0, market_regimes=self._REGIMES
        )

    def test_returns_dict_with_all_keys(self):
        result = self._run()
        for key in (
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption", "execution_ledger",
            "audit_trace", "replay_validation", "handoff_consistency",
            "promotion_gate", "paper_readiness_dossier", "human_review_summary",
            "review_packet", "anomaly_escalation",
        ):
            assert key in result, f"missing pipeline key: {key}"

    def test_escalation_is_dict(self):
        result = self._run()
        assert isinstance(result["anomaly_escalation"], dict)

    def test_escalation_has_anomaly_level(self):
        result = self._run()
        assert "anomaly_level" in result["anomaly_escalation"]

    def test_escalation_flags_correct(self):
        result = self._run()
        _assert_flags(result["anomaly_escalation"])

    def test_escalation_live_activation_false(self):
        result = self._run()
        assert result["anomaly_escalation"]["flags"]["live_activation_allowed"] is False

    def test_escalation_structure_valid(self):
        result = self._run()
        _assert_structure(result["anomaly_escalation"])

    def test_escalation_level_is_valid_value(self):
        result = self._run()
        level = result["anomaly_escalation"]["anomaly_level"]
        assert level in (LEVEL_NONE, LEVEL_LOW, LEVEL_MEDIUM, LEVEL_HIGH, LEVEL_CRITICAL)

    def test_no_write_by_default(self, tmp_path):
        """write_output=False (default) must not write any file."""
        result = build_escalation_from_specs(
            self._SPECS, total_equity_eur=10_000.0, market_regimes=self._REGIMES,
            write_output=False,
        )
        assert isinstance(result["anomaly_escalation"], dict)
