"""
AC-74: Controlled Bridge from Policy Evaluation to Allocation Advisory — test suite

Covers:
  A. advisory_simulation_only always True
  B. Advisory structure — required fields, correct types
  C. ADVISORY_ACTIVE — eligible recommendations trigger advisory
  D. BASELINE_HOLD — rejected / no-suitable / empty triggers hold
  E. HIGH_RISK / TOO_RISKY never advisory-active
  F. INSUFFICIENT_DATA never advisory-active
  G. NO_CHANGE never advisory-active
  H. Advisory confidence values correct and bounded
  I. Advisory determinism — same input → same output
  J. build_simulation() return includes allocation_advisory
  K. Backward compat — existing keys unchanged
  L. _ADVISORY_ELIGIBLE_RECS exported and correct
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module import
# ---------------------------------------------------------------------------

_SIM_PATH = Path(__file__).parent / "ant_colony" / "build_allocation_memory_policy_simulation_lite.py"


def _load_mod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SIM = _load_mod(_SIM_PATH, "_ac74_sim")
BASELINE_POLICY = copy.deepcopy(SIM.DEFAULT_POLICY)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _eval_summary(
    best_id=None,
    best_rec=None,
    best_risk=None,
    comparison_count=5,
    rejected_count=0,
    insufficient_count=0,
    no_change_count=0,
):
    return {
        "comparison_count":              comparison_count,
        "best_candidate_scenario_id":    best_id,
        "best_candidate_recommendation": best_rec,
        "best_candidate_risk_class":     best_risk,
        "rejected_scenarios":            [],
        "rejected_count":                rejected_count,
        "insufficient_data_scenarios":   [],
        "insufficient_data_count":       insufficient_count,
        "worth_review_count":            0,
        "no_change_count":               no_change_count,
        "candidate_for_trial_count":     0,
        "ranked_scenario_ids":           [],
    }


def _comp(scenario_id="s1", policy_recommendation="WORTH_REVIEW",
          changed_parameters_count=1):
    return {
        "scenario_id":              scenario_id,
        "policy_name":              scenario_id,
        "policy_recommendation":    policy_recommendation,
        "changed_parameters_count": changed_parameters_count,
    }


def _make_obs_rec(memory_available=True, mem_conf=0.80, mem_modifier=0.95,
                  cycle_mod=1.00):
    return {
        "memory_available":         memory_available,
        "memory_confidence":        mem_conf,
        "memory_modifier":          mem_modifier,
        "memory_bias_class":        "NEGATIVE",
        "cycle_modifier":           cycle_mod,
        "cycle_bias_class":         "NEUTRAL",
        "cooldown_flag":            False,
        "memory_influence_gate":    "NEGATIVE_GATE_OPEN",
        "base_feedback_confidence": 0.60,
    }


# ---------------------------------------------------------------------------
# A. advisory_simulation_only always True
# ---------------------------------------------------------------------------

class TestSimulationOnlyFlag:
    def test_advisory_active_has_simulation_only_true(self):
        ev = _eval_summary("best", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("best")])
        assert adv["advisory_simulation_only"] is True

    def test_baseline_hold_has_simulation_only_true(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_simulation_only"] is True

    def test_simulation_only_cannot_be_false(self):
        """All code paths must set advisory_simulation_only=True."""
        for best_id, best_rec in [
            ("x", "WORTH_REVIEW"),
            ("y", "CANDIDATE_FOR_MANUAL_TRIAL"),
            (None, None),
        ]:
            ev = _eval_summary(best_id, best_rec)
            adv = SIM.build_allocation_advisory(ev, [_comp(best_id or "y")])
            assert adv["advisory_simulation_only"] is True, \
                f"simulation_only=False for best_id={best_id}, best_rec={best_rec}"


# ---------------------------------------------------------------------------
# B. Advisory structure
# ---------------------------------------------------------------------------

class TestAdvisoryStructure:
    def test_required_fields_present(self):
        ev = _eval_summary("x", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("x")])
        for key in ("advisory_status", "advisory_scenario_id", "advisory_action",
                    "advisory_confidence", "advisory_reason", "advisory_simulation_only"):
            assert key in adv, f"Missing key: {key}"

    def test_advisory_status_is_valid_string(self):
        ev = _eval_summary("x", "WORTH_REVIEW")
        adv = SIM.build_allocation_advisory(ev, [_comp("x")])
        assert adv["advisory_status"] in ("ADVISORY_ACTIVE", "BASELINE_HOLD")

    def test_advisory_confidence_is_float(self):
        ev = _eval_summary("x", "WORTH_REVIEW")
        adv = SIM.build_allocation_advisory(ev, [_comp("x")])
        assert isinstance(adv["advisory_confidence"], float)

    def test_advisory_reason_non_empty(self):
        ev = _eval_summary("x", "WORTH_REVIEW")
        adv = SIM.build_allocation_advisory(ev, [_comp("x")])
        assert adv["advisory_reason"]

    def test_exported_eligible_recs(self):
        assert hasattr(SIM, "_ADVISORY_ELIGIBLE_RECS")
        assert hasattr(SIM, "_ADVISORY_CONFIDENCE")


# ---------------------------------------------------------------------------
# C. ADVISORY_ACTIVE — eligible recommendations
# ---------------------------------------------------------------------------

class TestAdvisoryActive:
    def test_worth_review_triggers_advisory_active(self):
        ev = _eval_summary("good", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("good")])
        assert adv["advisory_status"] == "ADVISORY_ACTIVE"

    def test_candidate_for_trial_triggers_advisory_active(self):
        ev = _eval_summary("ok", "CANDIDATE_FOR_MANUAL_TRIAL", "MEDIUM_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("ok")])
        assert adv["advisory_status"] == "ADVISORY_ACTIVE"

    def test_advisory_scenario_id_matches_best(self):
        ev = _eval_summary("my_scenario", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("my_scenario")])
        assert adv["advisory_scenario_id"] == "my_scenario"

    def test_advisory_action_is_consider_variant(self):
        ev = _eval_summary("s", "WORTH_REVIEW")
        adv = SIM.build_allocation_advisory(ev, [_comp("s")])
        assert adv["advisory_action"] == "CONSIDER_VARIANT"

    def test_advisory_reason_includes_recommendation(self):
        ev = _eval_summary("s", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("s")])
        assert "WORTH_REVIEW" in adv["advisory_reason"]

    def test_advisory_reason_includes_changed_params(self):
        ev = _eval_summary("s", "WORTH_REVIEW", "LOW_RISK")
        adv = SIM.build_allocation_advisory(ev, [_comp("s", changed_parameters_count=3)])
        assert "changed_params=3" in adv["advisory_reason"]

    def test_advisory_reason_includes_basis(self):
        ev = _eval_summary("s", "WORTH_REVIEW", comparison_count=7)
        adv = SIM.build_allocation_advisory(ev, [_comp("s")])
        assert "basis=7_scenarios" in adv["advisory_reason"]


# ---------------------------------------------------------------------------
# D. BASELINE_HOLD — various hold conditions
# ---------------------------------------------------------------------------

class TestBaselineHold:
    def test_no_best_candidate_gives_baseline_hold(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_status"] == "BASELINE_HOLD"

    def test_baseline_hold_scenario_id_is_baseline(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_scenario_id"] == "baseline"

    def test_baseline_hold_action_is_keep_current(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_action"] == "KEEP_CURRENT_POLICY"

    def test_baseline_hold_confidence_is_1(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_confidence"] == 1.00

    def test_zero_candidates_gives_baseline_hold(self):
        ev = _eval_summary(comparison_count=0)
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_status"] == "BASELINE_HOLD"
        assert "NO_CANDIDATES" in adv["advisory_reason"]

    def test_all_rejected_gives_baseline_hold(self):
        ev = _eval_summary(comparison_count=3, rejected_count=3)
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_status"] == "BASELINE_HOLD"

    def test_all_insufficient_gives_baseline_hold(self):
        ev = _eval_summary(comparison_count=3, insufficient_count=3)
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_status"] == "BASELINE_HOLD"


# ---------------------------------------------------------------------------
# E. HIGH_RISK / TOO_RISKY never advisory-active
# ---------------------------------------------------------------------------

class TestHighRiskExcluded:
    def test_too_risky_not_eligible(self):
        assert "TOO_RISKY" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_high_risk_rec_gives_baseline_hold(self):
        # When best_candidate has TOO_RISKY recommendation → not eligible → BASELINE_HOLD
        ev = _eval_summary("bad", "TOO_RISKY", "HIGH_RISK",
                           rejected_count=1, comparison_count=1)
        adv = SIM.build_allocation_advisory(ev, [_comp("bad", "TOO_RISKY")])
        assert adv["advisory_status"] == "BASELINE_HOLD"

    def test_end_to_end_unsafe_extreme_gives_baseline_hold(self):
        """With only unsafe_extreme as candidate, advisory must be BASELINE_HOLD."""
        unsafe_spec = {
            "scenario_id": "unsafe_extreme",
            "policy_name": "unsafe_extreme",
            "overlay": {
                "memory_gate": {
                    "positive_correction_cap": 0.15,
                    "negative_blend_weight":   0.95,
                    "negative_correction_cap": 0.15,
                }
            },
        }
        recs = [_make_obs_rec() for _ in range(10)]
        result = SIM.build_simulation(recs, BASELINE_POLICY, [unsafe_spec])
        adv = result["allocation_advisory"]
        assert adv["advisory_status"] == "BASELINE_HOLD"
        assert adv["advisory_simulation_only"] is True


# ---------------------------------------------------------------------------
# F. INSUFFICIENT_DATA never advisory-active
# ---------------------------------------------------------------------------

class TestInsufficientDataExcluded:
    def test_insufficient_data_not_eligible(self):
        assert "INSUFFICIENT_DATA" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_insufficient_data_rec_gives_baseline_hold(self):
        ev = _eval_summary("nodata", "INSUFFICIENT_DATA", "INSUFFICIENT_DATA",
                           insufficient_count=1, comparison_count=1)
        adv = SIM.build_allocation_advisory(ev, [_comp("nodata", "INSUFFICIENT_DATA")])
        assert adv["advisory_status"] == "BASELINE_HOLD"

    def test_end_to_end_zero_records_gives_baseline_hold(self):
        """Zero observation records → INSUFFICIENT_DATA → BASELINE_HOLD."""
        spec = {"policy_name": "any", "overlay": {}}
        result = SIM.build_simulation([], BASELINE_POLICY, [spec])
        assert result["allocation_advisory"]["advisory_status"] == "BASELINE_HOLD"


# ---------------------------------------------------------------------------
# G. NO_CHANGE never advisory-active
# ---------------------------------------------------------------------------

class TestNoChangeExcluded:
    def test_no_change_not_eligible(self):
        assert "NO_CHANGE" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_no_change_rec_gives_baseline_hold(self):
        ev = _eval_summary("nc", "NO_CHANGE", "LOW_RISK",
                           no_change_count=1, comparison_count=1)
        adv = SIM.build_allocation_advisory(ev, [_comp("nc", "NO_CHANGE")])
        assert adv["advisory_status"] == "BASELINE_HOLD"


# ---------------------------------------------------------------------------
# H. Advisory confidence values
# ---------------------------------------------------------------------------

class TestAdvisoryConfidence:
    def test_worth_review_confidence(self):
        ev = _eval_summary("s", "WORTH_REVIEW")
        adv = SIM.build_allocation_advisory(ev, [_comp("s")])
        assert adv["advisory_confidence"] == SIM._ADVISORY_CONFIDENCE["WORTH_REVIEW"]

    def test_candidate_for_trial_confidence(self):
        ev = _eval_summary("s", "CANDIDATE_FOR_MANUAL_TRIAL")
        adv = SIM.build_allocation_advisory(ev, [_comp("s")])
        assert adv["advisory_confidence"] == SIM._ADVISORY_CONFIDENCE["CANDIDATE_FOR_MANUAL_TRIAL"]

    def test_candidate_for_trial_confidence_higher_than_worth_review(self):
        """CANDIDATE_FOR_MANUAL_TRIAL is more specific → higher confidence."""
        assert (SIM._ADVISORY_CONFIDENCE["CANDIDATE_FOR_MANUAL_TRIAL"] >
                SIM._ADVISORY_CONFIDENCE["WORTH_REVIEW"])

    def test_confidence_in_valid_range(self):
        for ev, comps in [
            (_eval_summary("s", "WORTH_REVIEW"), [_comp("s")]),
            (_eval_summary("s", "CANDIDATE_FOR_MANUAL_TRIAL"), [_comp("s")]),
            (_eval_summary(), []),
        ]:
            adv = SIM.build_allocation_advisory(ev, comps)
            assert 0.0 <= adv["advisory_confidence"] <= 1.0

    def test_baseline_hold_confidence_is_exactly_1(self):
        ev = _eval_summary()
        adv = SIM.build_allocation_advisory(ev, [])
        assert adv["advisory_confidence"] == 1.00


# ---------------------------------------------------------------------------
# I. Determinism
# ---------------------------------------------------------------------------

class TestAdvisoryDeterminism:
    def test_same_input_same_output(self):
        ev = _eval_summary("s", "WORTH_REVIEW", "LOW_RISK")
        comp = [_comp("s")]
        adv1 = SIM.build_allocation_advisory(ev, comp)
        adv2 = SIM.build_allocation_advisory(ev, comp)
        assert adv1 == adv2

    def test_same_input_same_output_baseline_hold(self):
        ev = _eval_summary()
        adv1 = SIM.build_allocation_advisory(ev, [])
        adv2 = SIM.build_allocation_advisory(ev, [])
        assert adv1 == adv2

    def test_does_not_mutate_input(self):
        ev = _eval_summary("s", "WORTH_REVIEW")
        original_keys = set(ev.keys())
        SIM.build_allocation_advisory(ev, [])
        assert set(ev.keys()) == original_keys


# ---------------------------------------------------------------------------
# J. build_simulation() includes allocation_advisory
# ---------------------------------------------------------------------------

class TestBuildSimulationAdvisoryKey:
    def test_allocation_advisory_in_return(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert "allocation_advisory" in result

    def test_allocation_advisory_is_dict(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert isinstance(result["allocation_advisory"], dict)

    def test_advisory_simulation_only_true_in_build_simulation(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert result["allocation_advisory"]["advisory_simulation_only"] is True

    def test_zero_candidates_advisory_present(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY, [])
        assert "allocation_advisory" in result
        assert result["allocation_advisory"]["advisory_status"] == "BASELINE_HOLD"


# ---------------------------------------------------------------------------
# K. Backward compat — existing keys unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    def _run(self):
        spec = {"policy_name": "t", "overlay": {}}
        return SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY, [spec])

    def test_existing_top_level_keys_still_present(self):
        result = self._run()
        for key in ("summary", "baseline_policy", "baseline_metrics",
                    "policy_comparisons", "scenario_evaluation_summary"):
            assert key in result, f"Missing key: {key}"

    def test_policy_comparisons_still_has_original_fields(self):
        result = self._run()
        comp = result["policy_comparisons"][0]
        for field in ("policy_name", "policy_recommendation", "policy_risk_class",
                      "delta_vs_baseline", "changed_parameters"):
            assert field in comp

    def test_allocation_advisory_is_additive(self):
        result = self._run()
        # New key added; existing keys not removed or renamed
        assert "allocation_advisory" in result
        assert result["allocation_advisory"] is not result["scenario_evaluation_summary"]


# ---------------------------------------------------------------------------
# L. _ADVISORY_ELIGIBLE_RECS exported and correct
# ---------------------------------------------------------------------------

class TestEligibleRecs:
    def test_exported(self):
        assert hasattr(SIM, "_ADVISORY_ELIGIBLE_RECS")

    def test_worth_review_eligible(self):
        assert "WORTH_REVIEW" in SIM._ADVISORY_ELIGIBLE_RECS

    def test_candidate_for_trial_eligible(self):
        assert "CANDIDATE_FOR_MANUAL_TRIAL" in SIM._ADVISORY_ELIGIBLE_RECS

    def test_too_risky_not_eligible(self):
        assert "TOO_RISKY" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_insufficient_data_not_eligible(self):
        assert "INSUFFICIENT_DATA" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_no_change_not_eligible(self):
        assert "NO_CHANGE" not in SIM._ADVISORY_ELIGIBLE_RECS

    def test_only_two_eligible_recs(self):
        """Eligible set is deliberately small and conservative."""
        assert len(SIM._ADVISORY_ELIGIBLE_RECS) == 2
