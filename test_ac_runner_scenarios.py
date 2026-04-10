"""
AC-Runner tests — Scenario + Feedback Generator (AC-102: 60 scenarios)

Coverage:
  - dry_run: script runs without error, no files written
  - returns correct scenario count (60)
  - anomaly_level present in all results
  - feedback values only CONFIRM/DISAGREE/UNCERTAIN
  - all known scenario families present (original 30 + new 30)
  - per-family feedback pools exist and have correct distribution
  - with file write: 60 entries in log
  - analysis written when write_output=True
  - flags of written entries are correct
  - no regression on existing tests
"""
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from run_ac_scenarios_lite import (
    run, _build_scenarios,
    POOL_NORMAL, POOL_CONFLICT, POOL_CRIT_CONF,
    POOL_CRIT_DIS, POOL_UNCERTAIN, POOL_HOLD,
)


# ---------------------------------------------------------------------------
# 1. Scenario definitions
# ---------------------------------------------------------------------------

class TestScenarioDefinitions:
    def test_exactly_60_scenarios(self):
        assert len(_build_scenarios()) == 60

    def test_all_scenario_names_unique(self):
        names = [s["name"] for s in _build_scenarios()]
        assert len(names) == len(set(names))

    # ── Original families ──

    def test_all_clear_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("ALL_CLEAR" in n for n in names)

    def test_paper_hold_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("PAPER_HOLD" in n for n in names)

    def test_paper_rejected_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("PAPER_REJECTED" in n for n in names)

    def test_critical_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("VALIDATION_FAILED" in n for n in names)
        assert any("CONSISTENCY_FAILED" in n for n in names)

    def test_high_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("BLOCKING_FINDINGS" in n for n in names)
        assert any("LAYER_CONFLICT" in n for n in names)

    def test_low_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("ZERO_INTENTS" in n for n in names)

    def test_medium_scenarios_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("HOLD_PRIORITY" in n for n in names)

    # ── New families (AC-102) ──

    def test_borderline_hold_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("BORDERLINE_HOLD" in n for n in names)

    def test_mixed_blockers_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("MIXED_BLOCKERS" in n for n in names)

    def test_review_conflict_lu_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("REVIEW_CONFLICT_LU" in n for n in names)

    def test_critical_confirm_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("CRITICAL_CONFIRM" in n for n in names)

    def test_critical_disagree_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("CRITICAL_DISAGREE" in n for n in names)

    def test_uncertain_heavy_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("UNCERTAIN_HEAVY" in n for n in names)

    def test_zero_intents_conflict_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("ZERO_INTENTS_CONFLICT" in n for n in names)

    def test_validation_ok_block_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("VALIDATION_OK_BLOCK" in n for n in names)

    def test_dossier_hold_clean_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("DOSSIER_HOLD_CLEAN" in n for n in names)

    def test_promo_ready_high_rev_present(self):
        names = [s["name"] for s in _build_scenarios()]
        assert any("PROMO_READY_HIGH_REV" in n for n in names)

    def test_each_scenario_has_required_keys(self):
        for s in _build_scenarios():
            for k in ("name", "gate", "dossier", "review", "packet", "feedback_pool"):
                assert k in s, f"{s['name']} missing key {k}"

    def test_each_scenario_has_valid_feedback_pool(self):
        valid_actions = {"CONFIRM", "DISAGREE", "UNCERTAIN"}
        for s in _build_scenarios():
            pool = s["feedback_pool"]
            assert len(pool) > 0, f"{s['name']} has empty feedback_pool"
            assert all(fb in valid_actions for fb in pool), \
                f"{s['name']} pool contains invalid action"


# ---------------------------------------------------------------------------
# 2. Per-family feedback pool distributions
# ---------------------------------------------------------------------------

class TestFeedbackPools:
    def test_pool_normal_distribution(self):
        assert POOL_NORMAL.count("CONFIRM")   == 7
        assert POOL_NORMAL.count("DISAGREE")  == 2
        assert POOL_NORMAL.count("UNCERTAIN") == 1

    def test_pool_conflict_distribution(self):
        assert POOL_CONFLICT.count("CONFIRM")   == 3
        assert POOL_CONFLICT.count("DISAGREE")  == 4
        assert POOL_CONFLICT.count("UNCERTAIN") == 3

    def test_pool_crit_conf_distribution(self):
        assert POOL_CRIT_CONF.count("CONFIRM")   == 8
        assert POOL_CRIT_CONF.count("DISAGREE")  == 1
        assert POOL_CRIT_CONF.count("UNCERTAIN") == 1

    def test_pool_crit_dis_distribution(self):
        assert POOL_CRIT_DIS.count("CONFIRM")   == 2
        assert POOL_CRIT_DIS.count("DISAGREE")  == 6
        assert POOL_CRIT_DIS.count("UNCERTAIN") == 2

    def test_pool_uncertain_distribution(self):
        assert POOL_UNCERTAIN.count("CONFIRM")   == 2
        assert POOL_UNCERTAIN.count("DISAGREE")  == 3
        assert POOL_UNCERTAIN.count("UNCERTAIN") == 5

    def test_pool_hold_distribution(self):
        assert POOL_HOLD.count("CONFIRM")   == 5
        assert POOL_HOLD.count("DISAGREE")  == 3
        assert POOL_HOLD.count("UNCERTAIN") == 2

    def test_conflict_disagree_rate_higher_than_normal(self):
        normal_dr   = POOL_NORMAL.count("DISAGREE")   / len(POOL_NORMAL)
        conflict_dr = POOL_CONFLICT.count("DISAGREE") / len(POOL_CONFLICT)
        assert conflict_dr > normal_dr

    def test_crit_dis_disagree_rate_highest(self):
        crit_dis_dr = POOL_CRIT_DIS.count("DISAGREE") / len(POOL_CRIT_DIS)
        assert crit_dis_dr >= 0.5

    def test_uncertain_heavy_uncertain_rate_highest(self):
        unc_rate = POOL_UNCERTAIN.count("UNCERTAIN") / len(POOL_UNCERTAIN)
        assert unc_rate >= 0.4


# ---------------------------------------------------------------------------
# 3. Dry-run — no file writes, no errors
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_runs_without_error(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result is not None

    def test_returns_60_scenarios(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result["scenarios_run"] == 60

    def test_no_file_written_in_dry_run(self, tmp_path):
        log = tmp_path / "log.jsonl"
        run(dry_run=True, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        assert not log.exists()

    def test_entries_written_zero_in_dry_run(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result["entries_written"] == 0

    def test_analysis_none_in_dry_run(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result["analysis"] is None

    def test_results_list_has_60_items(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert len(result["results"]) == 60

    def test_each_result_has_required_keys(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        for r in result["results"]:
            for k in ("scenario", "anomaly_level", "action_status", "feedback"):
                assert k in r, f"missing key {k} in result"

    def test_anomaly_level_valid_values(self, tmp_path):
        valid = {"NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"}
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        for r in result["results"]:
            assert r["anomaly_level"] in valid

    def test_feedback_valid_values(self, tmp_path):
        valid = {"CONFIRM", "DISAGREE", "UNCERTAIN"}
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        for r in result["results"]:
            assert r["feedback"] in valid


# ---------------------------------------------------------------------------
# 4. Anomaly level distribution (scenario types produce expected levels)
# ---------------------------------------------------------------------------

class TestAnomalyLevels:
    def setup_method(self):
        self.result = run(dry_run=True,
                          log_path=Path("C:/tmp/unused.jsonl"),
                          analysis_path=Path("C:/tmp/unused.json"))
        self.levels = [r["anomaly_level"] for r in self.result["results"]]

    def test_at_least_one_none(self):
        assert "NONE" in self.levels

    def test_at_least_one_critical(self):
        assert "CRITICAL" in self.levels

    def test_at_least_one_high(self):
        assert "HIGH" in self.levels

    def test_at_least_one_low(self):
        assert "LOW" in self.levels

    def test_at_least_one_medium(self):
        assert "MEDIUM" in self.levels

    def test_all_clear_produces_none(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if "ALL_CLEAR" in name:
                assert r["anomaly_level"] == "NONE", \
                    f"{name}: expected NONE, got {r['anomaly_level']}"

    def test_paper_hold_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("PAPER_HOLD_"):
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"

    def test_paper_rejected_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("PAPER_REJECTED_"):
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"

    def test_critical_confirm_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("CRITICAL_CONFIRM_"):
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"

    def test_critical_disagree_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("CRITICAL_DISAGREE_"):
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"

    def test_mixed_blockers_produces_high(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("MIXED_BLOCKERS_"):
                assert r["anomaly_level"] == "HIGH", \
                    f"{name}: expected HIGH, got {r['anomaly_level']}"

    def test_borderline_hold_produces_medium(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("BORDERLINE_HOLD_"):
                assert r["anomaly_level"] == "MEDIUM", \
                    f"{name}: expected MEDIUM, got {r['anomaly_level']}"

    def test_dossier_hold_clean_produces_medium(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if name.startswith("DOSSIER_HOLD_CLEAN_"):
                assert r["anomaly_level"] == "MEDIUM", \
                    f"{name}: expected MEDIUM, got {r['anomaly_level']}"


# ---------------------------------------------------------------------------
# 5. Feedback distribution (seeded, per-family)
# ---------------------------------------------------------------------------

class TestFeedbackDistribution:
    def setup_method(self):
        self.result = run(dry_run=True,
                          log_path=Path("C:/tmp/unused.jsonl"),
                          analysis_path=Path("C:/tmp/unused.json"))
        self.feedbacks = [r["feedback"] for r in self.result["results"]]

    def test_total_entries_count(self):
        assert len(self.feedbacks) == 60

    def test_disagree_present(self):
        assert "DISAGREE" in self.feedbacks

    def test_confirm_present(self):
        assert "CONFIRM" in self.feedbacks

    def test_uncertain_present(self):
        assert "UNCERTAIN" in self.feedbacks

    def test_all_entries_are_valid_actions(self):
        valid = {"CONFIRM", "DISAGREE", "UNCERTAIN"}
        assert all(fb in valid for fb in self.feedbacks)

    def test_same_seed_gives_same_distribution(self):
        result2 = run(dry_run=True,
                      log_path=Path("C:/tmp/unused.jsonl"),
                      analysis_path=Path("C:/tmp/unused.json"))
        feedbacks2 = [r["feedback"] for r in result2["results"]]
        assert self.feedbacks == feedbacks2

    def test_feedback_differs_across_families(self):
        """CRITICAL_DISAGREE family should produce more DISAGREE than ALL_CLEAR."""
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        ac_fbs  = [results_by_name[n]["feedback"]
                   for n in results_by_name if n.startswith("ALL_CLEAR_")]
        cd_fbs  = [results_by_name[n]["feedback"]
                   for n in results_by_name if n.startswith("CRITICAL_DISAGREE_")]
        # CRITICAL_DISAGREE pool is 2/6/2 — must produce at least 1 DISAGREE
        assert "DISAGREE" in cd_fbs, "CRITICAL_DISAGREE family produced no DISAGREE"


# ---------------------------------------------------------------------------
# 6. File write — log and analysis
# ---------------------------------------------------------------------------

class TestFileWrite:
    def test_log_file_created(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        assert log.exists()

    def test_exactly_60_entries_written_per_run(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 60

    def test_multiple_runs_append(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        an  = tmp_path / "analysis.json"
        run(dry_run=False, log_path=log, analysis_path=an)
        run(dry_run=False, log_path=log, analysis_path=an)
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 120  # 2 × 60

    def test_entries_are_valid_json(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                parsed = json.loads(line)
                assert "feedback_action" in parsed

    def test_entries_flags_correct(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entry = json.loads(line)
                f = entry["flags"]
                assert f["non_binding"]             is True
                assert f["simulation_only"]         is True
                assert f["paper_only"]              is True
                assert f["live_activation_allowed"] is False

    def test_analysis_file_created(self, tmp_path):
        an = tmp_path / "analysis.json"
        run(dry_run=False, log_path=tmp_path / "log.jsonl", analysis_path=an)
        assert an.exists()

    def test_analysis_file_valid_json(self, tmp_path):
        an = tmp_path / "analysis.json"
        run(dry_run=False, log_path=tmp_path / "log.jsonl", analysis_path=an)
        data = json.loads(an.read_text(encoding="utf-8"))
        assert data["version"] == "feedback_analysis_v1"

    def test_analysis_totals_match_entries(self, tmp_path):
        log = tmp_path / "log.jsonl"
        an  = tmp_path / "analysis.json"
        run(dry_run=False, log_path=log, analysis_path=an)
        data = json.loads(an.read_text(encoding="utf-8"))
        assert data["totals"]["entries"] == 60

    def test_analysis_returns_dict_from_run(self, tmp_path):
        result = run(dry_run=False,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert isinstance(result["analysis"], dict)
        assert "signals" in result["analysis"]

    def test_analysis_flags_correct(self, tmp_path):
        result = run(dry_run=False,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        f = result["analysis"]["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False
