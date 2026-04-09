"""
AC-73: Scenario Evaluation Summary + Recommendation Layer — test suite

Covers:
  A. _RECOMMENDATION_RANK exported and correct
  B. evaluate_scenario_comparisons() — basic structure and fields
  C. Ranking determinism — same input → same output, tie-break by scenario_id
  D. best_candidate logic — CANDIDATE_FOR_MANUAL_TRIAL > WORTH_REVIEW > none
  E. HIGH_RISK / TOO_RISKY always rejected, never best_candidate
  F. INSUFFICIENT_DATA scenarios in separate list
  G. NO_CHANGE does not become best_candidate over WORTH_REVIEW
  H. Empty comparisons handled safely
  I. build_simulation() return includes scenario_evaluation_summary
  J. Backward compat — existing comparison fields unchanged
  K. evaluation_reason strings are non-empty and reflect recommendation
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


SIM = _load_mod(_SIM_PATH, "_ac73_sim")
BASELINE_POLICY = copy.deepcopy(SIM.DEFAULT_POLICY)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _comp(
    scenario_id="s1",
    policy_name=None,
    policy_recommendation="WORTH_REVIEW",
    policy_risk_class="LOW_RISK",
    policy_safe=True,
    changed_parameters_count=1,
    simulated_records_total=10,
    simulation_reasons=None,
    delta_vs_baseline=None,
):
    """Build a minimal comparison dict for testing evaluate_scenario_comparisons."""
    return {
        "scenario_id":              scenario_id,
        "policy_name":              policy_name or scenario_id,
        "policy_recommendation":    policy_recommendation,
        "policy_risk_class":        policy_risk_class,
        "policy_safe":              policy_safe,
        "changed_parameters_count": changed_parameters_count,
        "simulated_records_total":  simulated_records_total,
        "simulation_reasons":       simulation_reasons or ["ALL_METRICS_WITHIN_BOUNDS"],
        "delta_vs_baseline":        delta_vs_baseline or {
            "positive_applied_rate": 0.05,
            "negative_applied_rate": 0.0,
            "memory_applied_rate":   0.05,
            "avg_modifier_delta":    0.002,
        },
    }


def _make_obs_rec(memory_available=True, mem_conf=0.80, mem_modifier=0.95,
                  cycle_mod=1.00, cycle_bias="NEUTRAL"):
    return {
        "memory_available":       memory_available,
        "memory_confidence":      mem_conf,
        "memory_modifier":        mem_modifier,
        "memory_bias_class":      "NEGATIVE",
        "cycle_modifier":         cycle_mod,
        "cycle_bias_class":       cycle_bias,
        "cooldown_flag":          False,
        "memory_influence_gate":  "NEGATIVE_GATE_OPEN",
        "base_feedback_confidence": 0.60,
    }


# ---------------------------------------------------------------------------
# A. _RECOMMENDATION_RANK exported and correct
# ---------------------------------------------------------------------------

class TestRecommendationRank:
    def test_rank_exported(self):
        assert hasattr(SIM, "_RECOMMENDATION_RANK")
        assert isinstance(SIM._RECOMMENDATION_RANK, dict)

    def test_candidate_for_trial_is_rank_1(self):
        assert SIM._RECOMMENDATION_RANK["CANDIDATE_FOR_MANUAL_TRIAL"] == 1

    def test_worth_review_is_rank_2(self):
        assert SIM._RECOMMENDATION_RANK["WORTH_REVIEW"] == 2

    def test_no_change_is_rank_3(self):
        assert SIM._RECOMMENDATION_RANK["NO_CHANGE"] == 3

    def test_insufficient_data_is_rank_4(self):
        assert SIM._RECOMMENDATION_RANK["INSUFFICIENT_DATA"] == 4

    def test_too_risky_is_rank_5(self):
        assert SIM._RECOMMENDATION_RANK["TOO_RISKY"] == 5

    def test_ranks_are_ordered(self):
        r = SIM._RECOMMENDATION_RANK
        assert r["CANDIDATE_FOR_MANUAL_TRIAL"] < r["WORTH_REVIEW"]
        assert r["WORTH_REVIEW"] < r["NO_CHANGE"]
        assert r["NO_CHANGE"] < r["INSUFFICIENT_DATA"]
        assert r["INSUFFICIENT_DATA"] < r["TOO_RISKY"]


# ---------------------------------------------------------------------------
# B. evaluate_scenario_comparisons() — structure
# ---------------------------------------------------------------------------

class TestEvaluateBasicStructure:
    def test_returns_tuple(self):
        enriched, summary = SIM.evaluate_scenario_comparisons([_comp()])
        assert isinstance(enriched, list)
        assert isinstance(summary, dict)

    def test_enriched_has_risk_rank(self):
        enriched, _ = SIM.evaluate_scenario_comparisons([_comp("s1", policy_recommendation="WORTH_REVIEW")])
        assert "risk_rank" in enriched[0]
        assert enriched[0]["risk_rank"] == 2

    def test_enriched_has_evaluation_reason(self):
        enriched, _ = SIM.evaluate_scenario_comparisons([_comp()])
        assert "evaluation_reason" in enriched[0]
        assert isinstance(enriched[0]["evaluation_reason"], str)
        assert enriched[0]["evaluation_reason"]

    def test_summary_required_keys(self):
        _, summary = SIM.evaluate_scenario_comparisons([_comp()])
        for key in (
            "comparison_count", "best_candidate_scenario_id",
            "best_candidate_recommendation", "best_candidate_risk_class",
            "rejected_scenarios", "rejected_count",
            "insufficient_data_scenarios", "insufficient_data_count",
            "worth_review_count", "no_change_count",
            "candidate_for_trial_count", "ranked_scenario_ids",
        ):
            assert key in summary, f"Missing key: {key}"

    def test_comparison_count_matches_input(self):
        comps = [_comp("a"), _comp("b"), _comp("c")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["comparison_count"] == 3

    def test_does_not_mutate_input(self):
        comp = _comp("s1")
        original_keys = set(comp.keys())
        SIM.evaluate_scenario_comparisons([comp])
        assert set(comp.keys()) == original_keys, "Input comp was mutated"


# ---------------------------------------------------------------------------
# C. Ranking determinism
# ---------------------------------------------------------------------------

class TestRankingDeterminism:
    def test_same_input_same_output(self):
        comps = [_comp("a"), _comp("b"), _comp("c")]
        _, s1 = SIM.evaluate_scenario_comparisons(comps)
        _, s2 = SIM.evaluate_scenario_comparisons(comps)
        assert s1["ranked_scenario_ids"] == s2["ranked_scenario_ids"]

    def test_ranked_scenario_ids_sorted_by_rank_then_id(self):
        comps = [
            _comp("zebra", policy_recommendation="WORTH_REVIEW"),
            _comp("apple", policy_recommendation="WORTH_REVIEW"),
            _comp("mango", policy_recommendation="TOO_RISKY",    policy_risk_class="HIGH_RISK"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        ranked = summary["ranked_scenario_ids"]
        # Both WORTH_REVIEW should come before TOO_RISKY
        # Within WORTH_REVIEW, alphabetical
        assert ranked.index("apple") < ranked.index("zebra")
        assert ranked.index("zebra") < ranked.index("mango")

    def test_risk_rank_in_enriched_matches_rank_dict(self):
        for rec, expected_rank in SIM._RECOMMENDATION_RANK.items():
            comp = _comp("s", policy_recommendation=rec)
            enriched, _ = SIM.evaluate_scenario_comparisons([comp])
            assert enriched[0]["risk_rank"] == expected_rank, \
                f"rank mismatch for {rec}: got {enriched[0]['risk_rank']}, expected {expected_rank}"

    def test_unknown_recommendation_gets_fallback_rank(self):
        comp = _comp("s", policy_recommendation="SOME_UNKNOWN_REC")
        enriched, _ = SIM.evaluate_scenario_comparisons([comp])
        assert enriched[0]["risk_rank"] == 99


# ---------------------------------------------------------------------------
# D. best_candidate logic
# ---------------------------------------------------------------------------

class TestBestCandidate:
    def test_worth_review_becomes_best(self):
        comps = [
            _comp("good", policy_recommendation="WORTH_REVIEW"),
            _comp("bad",  policy_recommendation="TOO_RISKY", policy_risk_class="HIGH_RISK"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] == "good"

    def test_candidate_for_trial_beats_worth_review(self):
        comps = [
            _comp("a", policy_recommendation="WORTH_REVIEW"),
            _comp("b", policy_recommendation="CANDIDATE_FOR_MANUAL_TRIAL", policy_risk_class="MEDIUM_RISK"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] == "b"

    def test_no_best_when_all_rejected(self):
        comps = [
            _comp("a", policy_recommendation="TOO_RISKY", policy_risk_class="HIGH_RISK"),
            _comp("b", policy_recommendation="TOO_RISKY", policy_risk_class="HIGH_RISK"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] is None

    def test_no_best_when_only_no_change(self):
        comps = [_comp("a", policy_recommendation="NO_CHANGE")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] is None

    def test_best_candidate_recommendation_matches(self):
        comps = [_comp("x", policy_recommendation="WORTH_REVIEW")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_recommendation"] == "WORTH_REVIEW"

    def test_best_candidate_risk_class_matches(self):
        comps = [_comp("x", policy_recommendation="WORTH_REVIEW", policy_risk_class="LOW_RISK")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_risk_class"] == "LOW_RISK"

    def test_alphabetical_tiebreak_picks_first_id(self):
        comps = [
            _comp("zebra", policy_recommendation="WORTH_REVIEW"),
            _comp("alpha", policy_recommendation="WORTH_REVIEW"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] == "alpha"


# ---------------------------------------------------------------------------
# E. HIGH_RISK / TOO_RISKY always rejected, never best_candidate
# ---------------------------------------------------------------------------

class TestHighRiskRejected:
    def test_too_risky_in_rejected_list(self):
        comps = [
            _comp("safe",   policy_recommendation="WORTH_REVIEW"),
            _comp("unsafe", policy_recommendation="TOO_RISKY", policy_risk_class="HIGH_RISK"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert "unsafe" in summary["rejected_scenarios"]

    def test_too_risky_not_best_candidate(self):
        comps = [_comp("only", policy_recommendation="TOO_RISKY", policy_risk_class="HIGH_RISK")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] is None

    def test_rejected_count_correct(self):
        comps = [
            _comp("r1", policy_recommendation="TOO_RISKY"),
            _comp("r2", policy_recommendation="TOO_RISKY"),
            _comp("ok", policy_recommendation="WORTH_REVIEW"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["rejected_count"] == 2

    def test_rejected_scenarios_sorted(self):
        comps = [
            _comp("zzz", policy_recommendation="TOO_RISKY"),
            _comp("aaa", policy_recommendation="TOO_RISKY"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["rejected_scenarios"] == sorted(summary["rejected_scenarios"])

    def test_full_simulation_unsafe_scenario_rejected(self):
        """End-to-end: unsafe_extreme from builtin candidates → TOO_RISKY → rejected."""
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
        ev = result["scenario_evaluation_summary"]
        assert "unsafe_extreme" in ev["rejected_scenarios"]
        assert ev["best_candidate_scenario_id"] != "unsafe_extreme"


# ---------------------------------------------------------------------------
# F. INSUFFICIENT_DATA scenarios
# ---------------------------------------------------------------------------

class TestInsufficientData:
    def test_insufficient_data_in_separate_list(self):
        comps = [
            _comp("good", policy_recommendation="WORTH_REVIEW"),
            _comp("nodata", policy_recommendation="INSUFFICIENT_DATA",
                  policy_risk_class="INSUFFICIENT_DATA"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert "nodata" in summary["insufficient_data_scenarios"]
        assert "nodata" not in summary["rejected_scenarios"]

    def test_insufficient_not_best_candidate(self):
        comps = [_comp("x", policy_recommendation="INSUFFICIENT_DATA")]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] is None

    def test_insufficient_data_count_correct(self):
        comps = [
            _comp("a", policy_recommendation="INSUFFICIENT_DATA"),
            _comp("b", policy_recommendation="INSUFFICIENT_DATA"),
            _comp("c", policy_recommendation="WORTH_REVIEW"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["insufficient_data_count"] == 2

    def test_full_simulation_zero_records_gives_insufficient(self):
        """With 0 records, all candidates get INSUFFICIENT_DATA."""
        spec = {"policy_name": "any", "overlay": {}}
        result = SIM.build_simulation([], BASELINE_POLICY, [spec])
        ev = result["scenario_evaluation_summary"]
        assert ev["best_candidate_scenario_id"] is None
        assert ev["insufficient_data_count"] >= 1


# ---------------------------------------------------------------------------
# G. NO_CHANGE does not beat WORTH_REVIEW
# ---------------------------------------------------------------------------

class TestNoChangePriority:
    def test_worth_review_beats_no_change(self):
        comps = [
            _comp("nc", policy_recommendation="NO_CHANGE"),
            _comp("wr", policy_recommendation="WORTH_REVIEW"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["best_candidate_scenario_id"] == "wr"

    def test_no_change_count_correct(self):
        comps = [
            _comp("a", policy_recommendation="NO_CHANGE"),
            _comp("b", policy_recommendation="NO_CHANGE"),
            _comp("c", policy_recommendation="WORTH_REVIEW"),
        ]
        _, summary = SIM.evaluate_scenario_comparisons(comps)
        assert summary["no_change_count"] == 2


# ---------------------------------------------------------------------------
# H. Empty comparisons handled safely
# ---------------------------------------------------------------------------

class TestEmptyComparisons:
    def test_empty_list_no_crash(self):
        enriched, summary = SIM.evaluate_scenario_comparisons([])
        assert enriched == []
        assert summary["comparison_count"] == 0
        assert summary["best_candidate_scenario_id"] is None
        assert summary["rejected_scenarios"] == []
        assert summary["insufficient_data_scenarios"] == []
        assert summary["ranked_scenario_ids"] == []

    def test_build_simulation_zero_candidates(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY, [])
        ev = result["scenario_evaluation_summary"]
        assert ev["comparison_count"] == 0
        assert ev["best_candidate_scenario_id"] is None


# ---------------------------------------------------------------------------
# I. build_simulation() includes scenario_evaluation_summary
# ---------------------------------------------------------------------------

class TestBuildSimulationReturnShape:
    def test_scenario_evaluation_summary_present(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert "scenario_evaluation_summary" in result

    def test_scenario_evaluation_summary_is_dict(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert isinstance(result["scenario_evaluation_summary"], dict)

    def test_existing_return_keys_still_present(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY, [])
        for key in ("summary", "baseline_policy", "baseline_metrics", "policy_comparisons"):
            assert key in result, f"Missing key: {key}"

    def test_policy_comparisons_is_still_list(self):
        result = SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY,
                                      [{"policy_name": "t", "overlay": {}}])
        assert isinstance(result["policy_comparisons"], list)


# ---------------------------------------------------------------------------
# J. Backward compat — existing comparison fields unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    def _run(self):
        spec = {
            "policy_name": "compat_test",
            "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}},
        }
        return SIM.build_simulation([_make_obs_rec()], BASELINE_POLICY, [spec])

    def test_original_required_fields_present(self):
        result = self._run()
        comp = result["policy_comparisons"][0]
        required = [
            "policy_name", "policy_description", "changed_parameters",
            "policy_safe", "policy_safe_violations", "simulated_records_total",
            "simulated_memory_applied_rate", "simulated_positive_applied_rate",
            "simulated_negative_applied_rate", "simulated_caution_applied_rate",
            "simulated_conflict_block_rate", "simulated_neutral_fallback_rate",
            "simulated_avg_modifier_delta", "simulated_avg_confidence_delta",
            "simulated_safe_band_violation_count",
            "delta_vs_baseline", "policy_risk_class", "policy_recommendation",
            "simulation_reasons",
        ]
        for field in required:
            assert field in comp, f"Missing backward-compat field: {field}"

    def test_policy_recommendation_value_unchanged(self):
        result = self._run()
        comp = result["policy_comparisons"][0]
        assert comp["policy_recommendation"] in (
            "NO_CHANGE", "WORTH_REVIEW", "TOO_RISKY",
            "INSUFFICIENT_DATA", "CANDIDATE_FOR_MANUAL_TRIAL",
        )

    def test_new_fields_are_additive(self):
        result = self._run()
        comp = result["policy_comparisons"][0]
        # New fields from AC-73 are present
        assert "risk_rank" in comp
        assert "evaluation_reason" in comp
        # AC-72 fields still present
        assert "scenario_id" in comp
        assert "fingerprint" in comp


# ---------------------------------------------------------------------------
# K. evaluation_reason strings
# ---------------------------------------------------------------------------

class TestEvaluationReason:
    def test_too_risky_reason_contains_high_risk(self):
        comp = _comp("bad", policy_recommendation="TOO_RISKY",
                     policy_risk_class="HIGH_RISK",
                     simulation_reasons=["POLICY_PARAMETER_OUTSIDE_SAFE_LIMITS"])
        enriched, _ = SIM.evaluate_scenario_comparisons([comp])
        reason = enriched[0]["evaluation_reason"]
        assert "HIGH_RISK" in reason

    def test_worth_review_reason_contains_delta(self):
        comp = _comp("x", policy_recommendation="WORTH_REVIEW",
                     delta_vs_baseline={"positive_applied_rate": 0.10, "negative_applied_rate": -0.05})
        enriched, _ = SIM.evaluate_scenario_comparisons([comp])
        reason = enriched[0]["evaluation_reason"]
        assert "d_pos" in reason or "NOTABLE_DELTA" in reason

    def test_no_change_reason_mentions_delta(self):
        comp = _comp("x", policy_recommendation="NO_CHANGE")
        enriched, _ = SIM.evaluate_scenario_comparisons([comp])
        assert "NO_DELTA" in enriched[0]["evaluation_reason"] or "NO_CHANGE" in enriched[0]["evaluation_reason"]

    def test_insufficient_data_reason_mentions_records(self):
        comp = _comp("x", policy_recommendation="INSUFFICIENT_DATA", simulated_records_total=2)
        enriched, _ = SIM.evaluate_scenario_comparisons([comp])
        assert "INSUFFICIENT_DATA" in enriched[0]["evaluation_reason"]
        assert "records=2" in enriched[0]["evaluation_reason"]

    def test_evaluation_reason_always_non_empty(self):
        for rec in SIM._RECOMMENDATION_RANK:
            comp = _comp("s", policy_recommendation=rec)
            enriched, _ = SIM.evaluate_scenario_comparisons([comp])
            assert enriched[0]["evaluation_reason"], f"Empty reason for {rec}"
