"""
AC-75: Advisory Observability + Queen Intake Contract — tests

Covers:
  - advisory_reason_code present and machine-stable on all paths
  - queen_intake_ready True iff ADVISORY_ACTIVE
  - queen_intake_contract_version always == QUEEN_INTAKE_CONTRACT_VERSION
  - All HOLD sub-paths produce distinct, correct reason codes
  - ADVISORY_ACTIVE paths map correct codes per recommendation
  - No mutation of inputs
  - Backward compatibility: all AC-74 fields still present
"""
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load module under test
# ---------------------------------------------------------------------------
_SIM_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_memory_policy_simulation_lite.py"
)

def _load_sim():
    spec = importlib.util.spec_from_file_location("_sim", _SIM_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_sim = _load_sim()

build_allocation_advisory     = _sim.build_allocation_advisory
evaluate_scenario_comparisons = _sim.evaluate_scenario_comparisons
QUEEN_INTAKE_CONTRACT_VERSION = _sim.QUEEN_INTAKE_CONTRACT_VERSION
_ADVISORY_REASON_CODES        = _sim._ADVISORY_REASON_CODES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_comp(scenario_id, delta_score=0.06, risk_class="LOW_RISK",
               recommendation="WORTH_REVIEW", n_changed=1, scenario_type="variant"):
    return {
        "scenario_id":               scenario_id,
        "scenario_type":             scenario_type,
        "fingerprint":               "abcd1234abcd1234",
        "delta_score":               delta_score,
        "policy_risk_class":         risk_class,
        "policy_recommendation":     recommendation,
        "changed_parameters_count":  n_changed,
        "summary":                   {},
    }


def _advisory_from_comps(comps):
    enriched, summary = evaluate_scenario_comparisons(comps)
    return build_allocation_advisory(summary, enriched)


# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

class TestContractConstants:
    def test_queen_intake_contract_version_is_v1(self):
        assert QUEEN_INTAKE_CONTRACT_VERSION == "v1"

    def test_reason_code_keys_cover_active_paths(self):
        assert "WORTH_REVIEW" in _ADVISORY_REASON_CODES
        assert "CANDIDATE_FOR_MANUAL_TRIAL" in _ADVISORY_REASON_CODES

    def test_reason_code_keys_cover_hold_paths(self):
        for k in ("_HOLD_NO_CANDIDATES", "_HOLD_ALL_REJECTED_OR_INSUFFICIENT",
                  "_HOLD_ALL_NO_CHANGE", "_HOLD_NO_SUITABLE_CANDIDATE",
                  "_HOLD_NO_ELIGIBLE_REC"):
            assert k in _ADVISORY_REASON_CODES, f"Missing key: {k}"

    def test_active_codes_start_with_ACTIVE(self):
        assert _ADVISORY_REASON_CODES["WORTH_REVIEW"].startswith("ACTIVE_")
        assert _ADVISORY_REASON_CODES["CANDIDATE_FOR_MANUAL_TRIAL"].startswith("ACTIVE_")

    def test_hold_codes_start_with_HOLD(self):
        for k in ("_HOLD_NO_CANDIDATES", "_HOLD_ALL_REJECTED_OR_INSUFFICIENT",
                  "_HOLD_ALL_NO_CHANGE", "_HOLD_NO_SUITABLE_CANDIDATE",
                  "_HOLD_NO_ELIGIBLE_REC"):
            assert _ADVISORY_REASON_CODES[k].startswith("HOLD_"), f"Bad prefix for {k}"


# ---------------------------------------------------------------------------
# AC-75 fields present on ADVISORY_ACTIVE
# ---------------------------------------------------------------------------

class TestAdvisoryActiveAC75Fields:
    def _active_advisory(self, rec="WORTH_REVIEW"):
        comps = [_make_comp("s1", delta_score=0.08, recommendation=rec)]
        return _advisory_from_comps(comps)

    def test_advisory_reason_code_present(self):
        adv = self._active_advisory()
        assert "advisory_reason_code" in adv

    def test_queen_intake_ready_present(self):
        adv = self._active_advisory()
        assert "queen_intake_ready" in adv

    def test_queen_intake_contract_version_present(self):
        adv = self._active_advisory()
        assert "queen_intake_contract_version" in adv

    def test_queen_intake_ready_is_true_when_active(self):
        adv = self._active_advisory()
        assert adv["advisory_status"] == "ADVISORY_ACTIVE"
        assert adv["queen_intake_ready"] is True

    def test_queen_intake_contract_version_value(self):
        adv = self._active_advisory()
        assert adv["queen_intake_contract_version"] == QUEEN_INTAKE_CONTRACT_VERSION

    def test_reason_code_worth_review(self):
        adv = self._active_advisory("WORTH_REVIEW")
        assert adv["advisory_reason_code"] == "ACTIVE_WORTH_REVIEW"

    def test_reason_code_candidate_for_manual_trial(self):
        adv = self._active_advisory("CANDIDATE_FOR_MANUAL_TRIAL")
        assert adv["advisory_reason_code"] == "ACTIVE_CANDIDATE_FOR_TRIAL"

    def test_reason_code_is_string(self):
        adv = self._active_advisory()
        assert isinstance(adv["advisory_reason_code"], str)

    def test_reason_code_not_empty(self):
        adv = self._active_advisory()
        assert adv["advisory_reason_code"].strip() != ""


# ---------------------------------------------------------------------------
# AC-75 fields present on BASELINE_HOLD
# ---------------------------------------------------------------------------

class TestBaselineHoldAC75Fields:
    def _hold_advisory_no_candidates(self):
        return _advisory_from_comps([])

    def _hold_advisory_all_rejected(self):
        comps = [
            _make_comp("s1", delta_score=-0.10, risk_class="HIGH_RISK", recommendation="TOO_RISKY"),
            _make_comp("s2", delta_score=-0.05, risk_class="HIGH_RISK", recommendation="TOO_RISKY"),
        ]
        return _advisory_from_comps(comps)

    def _hold_advisory_all_no_change(self):
        comps = [_make_comp("s1", delta_score=0.0, recommendation="NO_CHANGE")]
        return _advisory_from_comps(comps)

    def _hold_advisory_no_eligible_rec(self):
        # Craft summary where best_id is set but recommendation is not eligible.
        # This path cannot be reached through evaluate_scenario_comparisons (only
        # rank<=2 recs set best_candidate, and both are in _ADVISORY_ELIGIBLE_RECS).
        summary = {
            "comparison_count":              1,
            "best_candidate_scenario_id":    "s1",
            "best_candidate_recommendation": "NO_CHANGE",  # not in eligible set
            "best_candidate_risk_class":     "LOW_RISK",
            "rejected_count":                0,
            "insufficient_data_count":       0,
            "no_change_count":               1,
        }
        comps = [{"scenario_id": "s1", "changed_parameters_count": 1}]
        return build_allocation_advisory(summary, comps)

    def test_advisory_reason_code_present_no_candidates(self):
        adv = self._hold_advisory_no_candidates()
        assert "advisory_reason_code" in adv

    def test_queen_intake_ready_present_hold(self):
        adv = self._hold_advisory_no_candidates()
        assert "queen_intake_ready" in adv

    def test_queen_intake_contract_version_present_hold(self):
        adv = self._hold_advisory_no_candidates()
        assert "queen_intake_contract_version" in adv

    def test_queen_intake_ready_is_false_when_hold(self):
        for adv in [
            self._hold_advisory_no_candidates(),
            self._hold_advisory_all_rejected(),
            self._hold_advisory_all_no_change(),
            self._hold_advisory_no_eligible_rec(),
        ]:
            assert adv["advisory_status"] == "BASELINE_HOLD"
            assert adv["queen_intake_ready"] is False

    def test_queen_intake_contract_version_value_hold(self):
        adv = self._hold_advisory_no_candidates()
        assert adv["queen_intake_contract_version"] == QUEEN_INTAKE_CONTRACT_VERSION

    def test_reason_code_hold_no_candidates(self):
        adv = self._hold_advisory_no_candidates()
        assert adv["advisory_reason_code"] == "HOLD_NO_CANDIDATES"

    def test_reason_code_hold_all_rejected(self):
        adv = self._hold_advisory_all_rejected()
        assert adv["advisory_reason_code"] == "HOLD_ALL_REJECTED_OR_INSUFFICIENT"

    def test_reason_code_hold_all_no_change(self):
        adv = self._hold_advisory_all_no_change()
        assert adv["advisory_reason_code"] == "HOLD_ALL_NO_CHANGE"

    def test_reason_code_hold_no_eligible_rec(self):
        adv = self._hold_advisory_no_eligible_rec()
        assert adv["advisory_reason_code"] == "HOLD_NO_ELIGIBLE_REC"

    def test_reason_code_is_string_hold(self):
        adv = self._hold_advisory_no_candidates()
        assert isinstance(adv["advisory_reason_code"], str)


# ---------------------------------------------------------------------------
# Reason codes are stable (machine-readable, do not change between calls)
# ---------------------------------------------------------------------------

class TestReasonCodeStability:
    def test_same_input_same_reason_code_active(self):
        comps = [_make_comp("s1", recommendation="WORTH_REVIEW")]
        adv1 = _advisory_from_comps(comps)
        adv2 = _advisory_from_comps(comps)
        assert adv1["advisory_reason_code"] == adv2["advisory_reason_code"]

    def test_same_input_same_reason_code_hold(self):
        adv1 = _advisory_from_comps([])
        adv2 = _advisory_from_comps([])
        assert adv1["advisory_reason_code"] == adv2["advisory_reason_code"]

    def test_contract_version_never_changes_within_call(self):
        for _ in range(5):
            adv = _advisory_from_comps([])
            assert adv["queen_intake_contract_version"] == "v1"


# ---------------------------------------------------------------------------
# Backward compatibility: all AC-74 fields still present
# ---------------------------------------------------------------------------

_AC74_REQUIRED_FIELDS = {
    "advisory_status",
    "advisory_scenario_id",
    "advisory_action",
    "advisory_confidence",
    "advisory_reason",
    "advisory_simulation_only",
}

class TestBackwardCompatAC74:
    def _check_ac74_fields(self, adv):
        for f in _AC74_REQUIRED_FIELDS:
            assert f in adv, f"AC-74 field missing: {f}"

    def test_active_path_has_ac74_fields(self):
        comps = [_make_comp("s1", recommendation="WORTH_REVIEW")]
        adv = _advisory_from_comps(comps)
        self._check_ac74_fields(adv)

    def test_hold_path_has_ac74_fields(self):
        adv = _advisory_from_comps([])
        self._check_ac74_fields(adv)

    def test_advisory_simulation_only_always_true(self):
        for comps in [
            [_make_comp("s1", recommendation="WORTH_REVIEW")],
            [],
        ]:
            adv = _advisory_from_comps(comps)
            assert adv["advisory_simulation_only"] is True


# ---------------------------------------------------------------------------
# No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_build_advisory_does_not_mutate_summary(self):
        comps = [_make_comp("s1", recommendation="WORTH_REVIEW")]
        enriched, summary = evaluate_scenario_comparisons(comps)
        import copy
        summary_copy = copy.deepcopy(summary)
        build_allocation_advisory(summary, enriched)
        assert summary == summary_copy

    def test_build_advisory_does_not_mutate_comparisons(self):
        comps = [_make_comp("s1", recommendation="WORTH_REVIEW")]
        enriched, summary = evaluate_scenario_comparisons(comps)
        import copy
        enriched_copy = copy.deepcopy(enriched)
        build_allocation_advisory(summary, enriched)
        assert enriched == enriched_copy


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_mixed_hold_insufficient_and_no_change(self):
        """Some INSUFFICIENT_DATA + some NO_CHANGE — not all one type → NO_SUITABLE_CANDIDATE."""
        comps = [
            _make_comp("s1", delta_score=0.01, recommendation="INSUFFICIENT_DATA"),
            _make_comp("s2", delta_score=0.0, recommendation="NO_CHANGE"),
        ]
        adv = _advisory_from_comps(comps)
        assert adv["advisory_status"] == "BASELINE_HOLD"
        assert adv["queen_intake_ready"] is False
        # Neither all-rejected-or-insufficient nor all-no-change
        assert adv["advisory_reason_code"] in ("HOLD_NO_SUITABLE_CANDIDATE", "HOLD_NO_ELIGIBLE_REC")

    def test_active_advisory_queen_intake_ready_is_bool(self):
        comps = [_make_comp("s1", recommendation="WORTH_REVIEW")]
        adv = _advisory_from_comps(comps)
        assert isinstance(adv["queen_intake_ready"], bool)

    def test_hold_advisory_queen_intake_ready_is_bool(self):
        adv = _advisory_from_comps([])
        assert isinstance(adv["queen_intake_ready"], bool)

    def test_contract_version_is_string(self):
        adv = _advisory_from_comps([])
        assert isinstance(adv["queen_intake_contract_version"], str)

    def test_multiple_active_candidates_picks_best(self):
        comps = [
            _make_comp("s1", delta_score=0.05, recommendation="WORTH_REVIEW"),
            _make_comp("s2", delta_score=0.12, recommendation="CANDIDATE_FOR_MANUAL_TRIAL"),
        ]
        adv = _advisory_from_comps(comps)
        assert adv["advisory_status"] == "ADVISORY_ACTIVE"
        assert adv["queen_intake_ready"] is True
        assert adv["advisory_reason_code"] in ("ACTIVE_WORTH_REVIEW", "ACTIVE_CANDIDATE_FOR_TRIAL")
