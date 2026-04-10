"""
AC-98 tests — Anomaly Action Queue (Human Action Triage)

Coverage:
  - build_anomaly_action_queue: all 5 level paths (NONE/LOW/MEDIUM/HIGH/CRITICAL)
  - HIGH sub-paths: REVIEW_CONFLICT, REVIEW_BLOCKING_FINDINGS
  - CRITICAL sub-paths: REVIEW_MISSING_INPUT, REVIEW_CRITICAL_STATE
  - Invalid / missing input (fail-closed)
  - Flags invariants
  - action_required / human_attention mapping
  - source_context fields populated from escalation
  - Determinism (same input → same output)
  - No input mutation
  - build_action_queue_from_specs (full pipeline integration)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_anomaly_action_queue_lite import (
    build_anomaly_action_queue,
    build_action_queue_from_specs,
    STATUS_NONE, STATUS_INFO, STATUS_REVIEW, STATUS_URGENT, STATUS_CRITICAL,
    CLASS_NO_ACTION, CLASS_REVIEW_STATUS, CLASS_REVIEW_CONFLICT,
    CLASS_REVIEW_BLOCKING, CLASS_MISSING_INPUT, CLASS_CRITICAL_STATE,
    OP_NONE, OP_CHECK_SUMMARY, OP_INSPECT_PACKET,
    OP_INSPECT_UPSTREAM, OP_VERIFY_MISSING, OP_ESCALATE_NOW,
    URGENCY_NONE, URGENCY_LOW, URGENCY_MEDIUM, URGENCY_HIGH, URGENCY_CRITICAL,
    WINDOW_NONE, WINDOW_NEXT_REVIEW, WINDOW_SOON, WINDOW_NOW,
    REASON_NONE, REASON_INFO_LOW, REASON_REVIEW_HOLD,
    REASON_URGENT_CONFLICT, REASON_URGENT_BLOCKING,
    REASON_CRITICAL_MISSING_INPUT, REASON_CRITICAL_STATE,
    VERSION, COMPONENT,
)


# ---------------------------------------------------------------------------
# Escalation fixture factory
# ---------------------------------------------------------------------------

def _esc(
    level:                   str  = "NONE",
    reason_code:             str  = "ESCALATION_NONE",
    human_attention_required: bool = False,
    findings:                list | None = None,
    promotion_status:        str  = "PAPER_READY",
    dossier_status:          str  = "DOSSIER_READY",
    review_status:           str  = "REVIEW_READY",
) -> dict:
    return {
        "version":                  "anomaly_escalation_v1",
        "component":                "build_anomaly_escalation_lite",
        "ts_utc":                   "2026-04-10T12:00:00Z",
        "anomaly_detected":         level != "NONE",
        "anomaly_level":            level,
        "human_attention_required": human_attention_required,
        "escalation_reason":        f"test reason for {level}",
        "escalation_reason_code":   reason_code,
        "escalation_findings":      findings or [f"test finding for {level}"],
        "promotion_status":         promotion_status,
        "dossier_status":           dossier_status,
        "review_status":            review_status,
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


def _call(escalation=None, **kwargs):
    if escalation is None:
        escalation = _esc()
    return build_anomaly_action_queue(escalation, **kwargs)


# ---------------------------------------------------------------------------
# Structural helpers
# ---------------------------------------------------------------------------

def _assert_flags(q: dict) -> None:
    f = q["flags"]
    assert f["non_binding"]             is True
    assert f["simulation_only"]         is True
    assert f["paper_only"]              is True
    assert f["live_activation_allowed"] is False


def _assert_structure(q: dict) -> None:
    for key in (
        "version", "component", "ts_utc",
        "action_required", "action_status", "action_class",
        "operator_action", "urgency", "recommended_window",
        "reason", "reason_code",
        "source_context", "findings", "flags",
    ):
        assert key in q, f"missing key: {key}"
    assert q["version"]   == VERSION
    assert q["component"] == COMPONENT
    sc = q["source_context"]
    for sc_key in ("promotion_status", "dossier_status", "review_status",
                   "anomaly_level", "human_attention_required"):
        assert sc_key in sc, f"missing source_context key: {sc_key}"
    assert "action_findings" in q["findings"]
    assert isinstance(q["findings"]["action_findings"], list)
    assert len(q["findings"]["action_findings"]) >= 1


# ---------------------------------------------------------------------------
# 1. NONE — no anomaly
# ---------------------------------------------------------------------------

class TestNone:
    def test_action_status_none(self):
        q = _call(_esc(level="NONE"))
        assert q["action_status"] == STATUS_NONE

    def test_action_required_false(self):
        q = _call(_esc(level="NONE"))
        assert q["action_required"] is False

    def test_action_class_no_action(self):
        q = _call(_esc(level="NONE"))
        assert q["action_class"] == CLASS_NO_ACTION

    def test_operator_action_none(self):
        q = _call(_esc(level="NONE"))
        assert q["operator_action"] == OP_NONE

    def test_urgency_none(self):
        q = _call(_esc(level="NONE"))
        assert q["urgency"] == URGENCY_NONE

    def test_recommended_window_none(self):
        q = _call(_esc(level="NONE"))
        assert q["recommended_window"] == WINDOW_NONE

    def test_reason_code_none(self):
        q = _call(_esc(level="NONE"))
        assert q["reason_code"] == REASON_NONE

    def test_human_attention_false_in_context(self):
        q = _call(_esc(level="NONE", human_attention_required=False))
        assert q["source_context"]["human_attention_required"] is False

    def test_flags(self):
        _assert_flags(_call(_esc(level="NONE")))

    def test_structure(self):
        _assert_structure(_call(_esc(level="NONE")))


# ---------------------------------------------------------------------------
# 2. LOW — informational
# ---------------------------------------------------------------------------

class TestLow:
    def _esc_low(self):
        return _esc(level="LOW", reason_code="ESCALATION_LOW_INFO",
                    findings=["total_intents=0 — informational"])

    def test_action_status_info(self):
        assert _call(self._esc_low())["action_status"] == STATUS_INFO

    def test_action_required_false(self):
        assert _call(self._esc_low())["action_required"] is False

    def test_action_class_review_status(self):
        assert _call(self._esc_low())["action_class"] == CLASS_REVIEW_STATUS

    def test_operator_action_check_summary(self):
        assert _call(self._esc_low())["operator_action"] == OP_CHECK_SUMMARY

    def test_urgency_low(self):
        assert _call(self._esc_low())["urgency"] == URGENCY_LOW

    def test_recommended_window_next_review(self):
        assert _call(self._esc_low())["recommended_window"] == WINDOW_NEXT_REVIEW

    def test_reason_code_info_low(self):
        assert _call(self._esc_low())["reason_code"] == REASON_INFO_LOW

    def test_human_attention_false_in_context(self):
        q = _call(self._esc_low())
        assert q["source_context"]["human_attention_required"] is False

    def test_findings_from_escalation(self):
        q = _call(self._esc_low())
        combined = " ".join(q["findings"]["action_findings"])
        assert "total_intents" in combined

    def test_flags(self):
        _assert_flags(_call(self._esc_low()))

    def test_structure(self):
        _assert_structure(_call(self._esc_low()))


# ---------------------------------------------------------------------------
# 3. MEDIUM — hold state
# ---------------------------------------------------------------------------

class TestMedium:
    def _esc_medium(self):
        return _esc(level="MEDIUM", reason_code="ESCALATION_MEDIUM_HOLD",
                    human_attention_required=True,
                    findings=["review_priority=MEDIUM (hold/cautious state)"])

    def test_action_status_review(self):
        assert _call(self._esc_medium())["action_status"] == STATUS_REVIEW

    def test_action_required_true(self):
        assert _call(self._esc_medium())["action_required"] is True

    def test_action_class_review_status(self):
        assert _call(self._esc_medium())["action_class"] == CLASS_REVIEW_STATUS

    def test_operator_action_inspect_packet(self):
        assert _call(self._esc_medium())["operator_action"] == OP_INSPECT_PACKET

    def test_urgency_medium(self):
        assert _call(self._esc_medium())["urgency"] == URGENCY_MEDIUM

    def test_recommended_window_soon(self):
        assert _call(self._esc_medium())["recommended_window"] == WINDOW_SOON

    def test_reason_code_review_hold(self):
        assert _call(self._esc_medium())["reason_code"] == REASON_REVIEW_HOLD

    def test_human_attention_true_in_context(self):
        q = _call(self._esc_medium())
        assert q["source_context"]["human_attention_required"] is True

    def test_flags(self):
        _assert_flags(_call(self._esc_medium()))

    def test_structure(self):
        _assert_structure(_call(self._esc_medium()))


# ---------------------------------------------------------------------------
# 4. HIGH — conflict
# ---------------------------------------------------------------------------

class TestHighConflict:
    def _esc_conflict(self):
        return _esc(
            level="HIGH", reason_code="ESCALATION_HIGH_LAYER_CONFLICT",
            human_attention_required=True,
            findings=["layer conflict: promotion=PAPER_READY but review_status='REVIEW_HOLD'"],
        )

    def test_action_status_urgent(self):
        assert _call(self._esc_conflict())["action_status"] == STATUS_URGENT

    def test_action_required_true(self):
        assert _call(self._esc_conflict())["action_required"] is True

    def test_action_class_review_conflict(self):
        assert _call(self._esc_conflict())["action_class"] == CLASS_REVIEW_CONFLICT

    def test_operator_action_inspect_packet(self):
        assert _call(self._esc_conflict())["operator_action"] == OP_INSPECT_PACKET

    def test_urgency_high(self):
        assert _call(self._esc_conflict())["urgency"] == URGENCY_HIGH

    def test_recommended_window_now(self):
        assert _call(self._esc_conflict())["recommended_window"] == WINDOW_NOW

    def test_reason_code_urgent_conflict(self):
        assert _call(self._esc_conflict())["reason_code"] == REASON_URGENT_CONFLICT

    def test_flags(self):
        _assert_flags(_call(self._esc_conflict()))

    def test_structure(self):
        _assert_structure(_call(self._esc_conflict()))

    def test_findings_from_escalation(self):
        q = _call(self._esc_conflict())
        combined = " ".join(q["findings"]["action_findings"])
        assert "conflict" in combined.lower() or "REVIEW_HOLD" in combined


# ---------------------------------------------------------------------------
# 5. HIGH — blocking findings
# ---------------------------------------------------------------------------

class TestHighBlocking:
    def _esc_blocking(self):
        return _esc(
            level="HIGH", reason_code="ESCALATION_HIGH_BLOCKING_FINDINGS",
            human_attention_required=True,
            findings=["blocking_findings present (2 items)", "  blocking: validation failed"],
        )

    def _esc_high_priority(self):
        return _esc(
            level="HIGH", reason_code="ESCALATION_HIGH_REVIEW_PRIORITY",
            human_attention_required=True,
            findings=["review_priority=HIGH"],
        )

    def test_blocking_action_class(self):
        assert _call(self._esc_blocking())["action_class"] == CLASS_REVIEW_BLOCKING

    def test_blocking_reason_code(self):
        assert _call(self._esc_blocking())["reason_code"] == REASON_URGENT_BLOCKING

    def test_high_priority_action_class(self):
        assert _call(self._esc_high_priority())["action_class"] == CLASS_REVIEW_BLOCKING

    def test_high_priority_reason_code(self):
        assert _call(self._esc_high_priority())["reason_code"] == REASON_URGENT_BLOCKING

    def test_urgency_high(self):
        assert _call(self._esc_blocking())["urgency"] == URGENCY_HIGH

    def test_recommended_window_now(self):
        assert _call(self._esc_blocking())["recommended_window"] == WINDOW_NOW

    def test_action_required_true(self):
        assert _call(self._esc_blocking())["action_required"] is True

    def test_flags(self):
        _assert_flags(_call(self._esc_blocking()))

    def test_structure(self):
        _assert_structure(_call(self._esc_blocking()))

    def test_findings_contain_blocking(self):
        q = _call(self._esc_blocking())
        combined = " ".join(q["findings"]["action_findings"])
        assert "blocking" in combined.lower()


# ---------------------------------------------------------------------------
# 6. CRITICAL — missing input
# ---------------------------------------------------------------------------

class TestCriticalMissingInput:
    def _esc_invalid(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_INVALID_INPUT",
            human_attention_required=True,
            findings=["promotion_gate is not a dict — cannot assess anomaly"],
        )

    def _esc_snapshot(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_SNAPSHOT_MISSING",
            human_attention_required=True,
            findings=["promotion_snapshot absent in dossier"],
        )

    def test_invalid_input_action_class(self):
        assert _call(self._esc_invalid())["action_class"] == CLASS_MISSING_INPUT

    def test_invalid_input_operator_action(self):
        assert _call(self._esc_invalid())["operator_action"] == OP_VERIFY_MISSING

    def test_snapshot_action_class(self):
        assert _call(self._esc_snapshot())["action_class"] == CLASS_MISSING_INPUT

    def test_snapshot_operator_action(self):
        assert _call(self._esc_snapshot())["operator_action"] == OP_VERIFY_MISSING

    def test_reason_code_missing_input(self):
        assert _call(self._esc_invalid())["reason_code"] == REASON_CRITICAL_MISSING_INPUT

    def test_action_status_critical(self):
        assert _call(self._esc_invalid())["action_status"] == STATUS_CRITICAL

    def test_urgency_critical(self):
        assert _call(self._esc_invalid())["urgency"] == URGENCY_CRITICAL

    def test_recommended_window_now(self):
        assert _call(self._esc_invalid())["recommended_window"] == WINDOW_NOW

    def test_action_required_true(self):
        assert _call(self._esc_invalid())["action_required"] is True

    def test_flags(self):
        _assert_flags(_call(self._esc_invalid()))

    def test_structure(self):
        _assert_structure(_call(self._esc_invalid()))


# ---------------------------------------------------------------------------
# 7. CRITICAL — critical state (validation/consistency/promotion/dossier)
# ---------------------------------------------------------------------------

class TestCriticalState:
    def _esc_validation(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_VALIDATION_FAILED",
            human_attention_required=True,
            promotion_status="PAPER_READY",
            findings=["validation_passed=False"],
        )

    def _esc_consistency(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_CONSISTENCY_FAILED",
            human_attention_required=True,
            findings=["consistency_passed=False"],
        )

    def _esc_promotion_not_ready(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_PROMOTION_NOT_READY",
            human_attention_required=True,
            promotion_status="PAPER_REJECTED",
            findings=["promotion_status='PAPER_REJECTED' (expected PAPER_READY)"],
        )

    def _esc_dossier_rejected(self):
        return _esc(
            level="CRITICAL", reason_code="ESCALATION_CRITICAL_DOSSIER_REJECTED",
            human_attention_required=True,
            dossier_status="DOSSIER_REJECTED",
            findings=["dossier_status=DOSSIER_REJECTED"],
        )

    def test_validation_action_class(self):
        assert _call(self._esc_validation())["action_class"] == CLASS_CRITICAL_STATE

    def test_validation_operator_action(self):
        assert _call(self._esc_validation())["operator_action"] == OP_INSPECT_UPSTREAM

    def test_consistency_action_class(self):
        assert _call(self._esc_consistency())["action_class"] == CLASS_CRITICAL_STATE

    def test_consistency_operator_action(self):
        assert _call(self._esc_consistency())["operator_action"] == OP_INSPECT_UPSTREAM

    def test_promotion_not_ready_action_class(self):
        assert _call(self._esc_promotion_not_ready())["action_class"] == CLASS_CRITICAL_STATE

    def test_promotion_not_ready_operator_action(self):
        assert _call(self._esc_promotion_not_ready())["operator_action"] == OP_ESCALATE_NOW

    def test_dossier_rejected_action_class(self):
        assert _call(self._esc_dossier_rejected())["action_class"] == CLASS_CRITICAL_STATE

    def test_dossier_rejected_operator_action(self):
        assert _call(self._esc_dossier_rejected())["operator_action"] == OP_ESCALATE_NOW

    def test_reason_code_critical_state(self):
        assert _call(self._esc_validation())["reason_code"] == REASON_CRITICAL_STATE

    def test_action_status_critical(self):
        assert _call(self._esc_validation())["action_status"] == STATUS_CRITICAL

    def test_recommended_window_now(self):
        assert _call(self._esc_validation())["recommended_window"] == WINDOW_NOW

    def test_urgency_critical(self):
        assert _call(self._esc_validation())["urgency"] == URGENCY_CRITICAL

    def test_action_required_true(self):
        assert _call(self._esc_validation())["action_required"] is True

    def test_flags_validation(self):
        _assert_flags(_call(self._esc_validation()))

    def test_structure_validation(self):
        _assert_structure(_call(self._esc_validation()))


# ---------------------------------------------------------------------------
# 8. Invalid primary input — fail-closed
# ---------------------------------------------------------------------------

class TestInvalidPrimaryInput:
    def test_none_escalation_gives_critical(self):
        q = build_anomaly_action_queue(None)
        assert q["action_status"] == STATUS_CRITICAL
        assert q["action_class"] == CLASS_MISSING_INPUT
        assert q["reason_code"] == REASON_CRITICAL_MISSING_INPUT

    def test_string_escalation_gives_critical(self):
        q = build_anomaly_action_queue("bad")
        assert q["action_status"] == STATUS_CRITICAL

    def test_list_escalation_gives_critical(self):
        q = build_anomaly_action_queue([])
        assert q["action_status"] == STATUS_CRITICAL

    def test_missing_anomaly_level_gives_critical(self):
        e = _esc(level="NONE")
        del e["anomaly_level"]
        q = build_anomaly_action_queue(e)
        assert q["action_status"] == STATUS_CRITICAL
        assert q["reason_code"] == REASON_CRITICAL_MISSING_INPUT

    def test_empty_dict_gives_critical(self):
        q = build_anomaly_action_queue({})
        assert q["action_status"] == STATUS_CRITICAL

    def test_invalid_input_action_required_true(self):
        q = build_anomaly_action_queue(None)
        assert q["action_required"] is True

    def test_invalid_input_urgency_critical(self):
        q = build_anomaly_action_queue(None)
        assert q["urgency"] == URGENCY_CRITICAL

    def test_invalid_input_operator_verify_missing(self):
        q = build_anomaly_action_queue(None)
        assert q["operator_action"] == OP_VERIFY_MISSING

    def test_invalid_input_flags_correct(self):
        _assert_flags(build_anomaly_action_queue(None))

    def test_invalid_input_findings_non_empty(self):
        q = build_anomaly_action_queue(None)
        assert len(q["findings"]["action_findings"]) >= 1

    def test_unknown_level_gives_critical(self):
        e = _esc(level="BOGUS_LEVEL")
        q = build_anomaly_action_queue(e)
        assert q["action_status"] == STATUS_CRITICAL
        assert q["reason_code"] == REASON_CRITICAL_MISSING_INPUT


# ---------------------------------------------------------------------------
# 9. Flags invariants
# ---------------------------------------------------------------------------

class TestFlagsInvariants:
    def test_flags_none(self):
        _assert_flags(_call(_esc(level="NONE")))

    def test_flags_low(self):
        _assert_flags(_call(_esc(level="LOW", reason_code="ESCALATION_LOW_INFO")))

    def test_flags_medium(self):
        _assert_flags(_call(_esc(level="MEDIUM", reason_code="ESCALATION_MEDIUM_HOLD",
                                  human_attention_required=True)))

    def test_flags_high_conflict(self):
        _assert_flags(_call(_esc(level="HIGH", reason_code="ESCALATION_HIGH_LAYER_CONFLICT",
                                  human_attention_required=True)))

    def test_flags_high_blocking(self):
        _assert_flags(_call(_esc(level="HIGH", reason_code="ESCALATION_HIGH_BLOCKING_FINDINGS",
                                  human_attention_required=True)))

    def test_flags_critical_invalid(self):
        _assert_flags(_call(_esc(level="CRITICAL", reason_code="ESCALATION_CRITICAL_INVALID_INPUT",
                                  human_attention_required=True)))

    def test_flags_critical_validation(self):
        _assert_flags(_call(_esc(level="CRITICAL", reason_code="ESCALATION_CRITICAL_VALIDATION_FAILED",
                                  human_attention_required=True)))

    def test_flags_invalid_primary_input(self):
        _assert_flags(build_anomaly_action_queue(None))


# ---------------------------------------------------------------------------
# 10. source_context population
# ---------------------------------------------------------------------------

class TestSourceContext:
    def test_promotion_status_passed_through(self):
        e = _esc(level="NONE", promotion_status="PAPER_READY")
        q = _call(e)
        assert q["source_context"]["promotion_status"] == "PAPER_READY"

    def test_dossier_status_passed_through(self):
        e = _esc(level="NONE", dossier_status="DOSSIER_READY")
        q = _call(e)
        assert q["source_context"]["dossier_status"] == "DOSSIER_READY"

    def test_review_status_passed_through(self):
        e = _esc(level="NONE", review_status="REVIEW_READY")
        q = _call(e)
        assert q["source_context"]["review_status"] == "REVIEW_READY"

    def test_anomaly_level_passed_through(self):
        e = _esc(level="NONE")
        q = _call(e)
        assert q["source_context"]["anomaly_level"] == "NONE"

    def test_human_attention_false_for_none(self):
        q = _call(_esc(level="NONE", human_attention_required=False))
        assert q["source_context"]["human_attention_required"] is False

    def test_human_attention_true_for_critical(self):
        e = _esc(level="CRITICAL", reason_code="ESCALATION_CRITICAL_VALIDATION_FAILED",
                 human_attention_required=True)
        q = _call(e)
        assert q["source_context"]["human_attention_required"] is True

    def test_context_with_rejected_promotion(self):
        e = _esc(level="CRITICAL", reason_code="ESCALATION_CRITICAL_PROMOTION_NOT_READY",
                 promotion_status="PAPER_REJECTED", human_attention_required=True)
        q = _call(e)
        assert q["source_context"]["promotion_status"] == "PAPER_REJECTED"


# ---------------------------------------------------------------------------
# 11. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_input_none(self):
        e = _esc(level="NONE")
        q1 = _call(e); q2 = _call(e)
        q1.pop("ts_utc"); q2.pop("ts_utc")
        assert q1 == q2

    def test_same_input_medium(self):
        e = _esc(level="MEDIUM", reason_code="ESCALATION_MEDIUM_HOLD",
                 human_attention_required=True)
        q1 = _call(e); q2 = _call(e)
        q1.pop("ts_utc"); q2.pop("ts_utc")
        assert q1 == q2

    def test_same_input_high_conflict(self):
        e = _esc(level="HIGH", reason_code="ESCALATION_HIGH_LAYER_CONFLICT",
                 human_attention_required=True)
        q1 = _call(e); q2 = _call(e)
        q1.pop("ts_utc"); q2.pop("ts_utc")
        assert q1 == q2

    def test_same_input_critical(self):
        e = _esc(level="CRITICAL", reason_code="ESCALATION_CRITICAL_VALIDATION_FAILED",
                 human_attention_required=True)
        q1 = _call(e); q2 = _call(e)
        q1.pop("ts_utc"); q2.pop("ts_utc")
        assert q1 == q2

    def test_no_mutation_of_input(self):
        e = _esc(level="NONE")
        original = dict(e)
        _call(e)
        assert e == original


# ---------------------------------------------------------------------------
# 12. Output structure / metadata
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def test_version_field(self):
        assert _call()["version"] == VERSION

    def test_component_field(self):
        assert _call()["component"] == COMPONENT

    def test_ts_utc_format(self):
        ts = _call()["ts_utc"]
        assert ts.endswith("Z")
        assert "T" in ts

    def test_action_required_is_bool(self):
        assert isinstance(_call()["action_required"], bool)

    def test_action_findings_is_list(self):
        q = _call()
        assert isinstance(q["findings"]["action_findings"], list)

    def test_action_findings_non_empty(self):
        q = _call()
        assert len(q["findings"]["action_findings"]) >= 1

    def test_reason_is_str(self):
        assert isinstance(_call()["reason"], str)
        assert _call()["reason"]

    def test_reason_code_is_str(self):
        assert isinstance(_call()["reason_code"], str)
        assert _call()["reason_code"]

    def test_action_status_is_valid(self):
        valid = {STATUS_NONE, STATUS_INFO, STATUS_REVIEW, STATUS_URGENT, STATUS_CRITICAL}
        assert _call()["action_status"] in valid

    def test_action_class_is_valid(self):
        valid = {CLASS_NO_ACTION, CLASS_REVIEW_STATUS, CLASS_REVIEW_CONFLICT,
                 CLASS_REVIEW_BLOCKING, CLASS_MISSING_INPUT, CLASS_CRITICAL_STATE}
        assert _call()["action_class"] in valid

    def test_operator_action_is_valid(self):
        valid = {OP_NONE, OP_CHECK_SUMMARY, OP_INSPECT_PACKET,
                 OP_INSPECT_UPSTREAM, OP_VERIFY_MISSING, OP_ESCALATE_NOW}
        assert _call()["operator_action"] in valid

    def test_urgency_is_valid(self):
        valid = {URGENCY_NONE, URGENCY_LOW, URGENCY_MEDIUM, URGENCY_HIGH, URGENCY_CRITICAL}
        assert _call()["urgency"] in valid

    def test_window_is_valid(self):
        valid = {WINDOW_NONE, WINDOW_NEXT_REVIEW, WINDOW_SOON, WINDOW_NOW}
        assert _call()["recommended_window"] in valid


# ---------------------------------------------------------------------------
# 13. Full pipeline — build_action_queue_from_specs
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
        return build_action_queue_from_specs(
            self._SPECS, total_equity_eur=10_000.0, market_regimes=self._REGIMES
        )

    def test_returns_all_keys(self):
        result = self._run()
        for key in (
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit", "queen_handoff",
            "runner_intake", "dry_run_consumption", "execution_ledger",
            "audit_trace", "replay_validation", "handoff_consistency",
            "promotion_gate", "paper_readiness_dossier", "human_review_summary",
            "review_packet", "anomaly_escalation", "anomaly_action_queue",
        ):
            assert key in result, f"missing pipeline key: {key}"

    def test_queue_is_dict(self):
        result = self._run()
        assert isinstance(result["anomaly_action_queue"], dict)

    def test_queue_flags(self):
        result = self._run()
        _assert_flags(result["anomaly_action_queue"])

    def test_queue_structure(self):
        result = self._run()
        _assert_structure(result["anomaly_action_queue"])

    def test_queue_action_status_valid(self):
        result = self._run()
        valid = {STATUS_NONE, STATUS_INFO, STATUS_REVIEW, STATUS_URGENT, STATUS_CRITICAL}
        assert result["anomaly_action_queue"]["action_status"] in valid

    def test_no_write_by_default(self):
        result = build_action_queue_from_specs(
            self._SPECS, total_equity_eur=10_000.0, market_regimes=self._REGIMES,
            write_output=False,
        )
        assert isinstance(result["anomaly_action_queue"], dict)

    def test_live_activation_always_false(self):
        result = self._run()
        assert result["anomaly_action_queue"]["flags"]["live_activation_allowed"] is False
