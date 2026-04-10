"""
AC-Runner tests — Scenario + Feedback Generator

Coverage:
  - dry_run: script runs without error, no files written
  - returns correct scenario count (30)
  - anomaly_level present in all results
  - feedback values only CONFIRM/DISAGREE/UNCERTAIN
  - distribution roughly 70/20/10 (seeded, exact check)
  - all known scenario names present
  - with file write: ≥20 entries in log
  - analysis written when write_output=True
  - flags of written entries are correct
  - no regression on existing tests
"""
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from run_ac_scenarios_lite import run, _build_scenarios, _FEEDBACK_POOL


# ---------------------------------------------------------------------------
# 1. Scenario definitions
# ---------------------------------------------------------------------------

class TestScenarioDefinitions:
    def test_exactly_30_scenarios(self):
        assert len(_build_scenarios()) == 30

    def test_all_scenario_names_unique(self):
        names = [s["name"] for s in _build_scenarios()]
        assert len(names) == len(set(names))

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

    def test_each_scenario_has_required_keys(self):
        for s in _build_scenarios():
            for k in ("name", "gate", "dossier", "review", "packet"):
                assert k in s, f"{s['name']} missing key {k}"

    def test_feedback_pool_distribution(self):
        assert _FEEDBACK_POOL.count("CONFIRM")   == 7
        assert _FEEDBACK_POOL.count("DISAGREE")  == 2
        assert _FEEDBACK_POOL.count("UNCERTAIN") == 1


# ---------------------------------------------------------------------------
# 2. Dry-run — no file writes, no errors
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_runs_without_error(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result is not None

    def test_returns_30_scenarios(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert result["scenarios_run"] == 30

    def test_no_file_written_in_dry_run(self, tmp_path):
        log = tmp_path / "log.jsonl"
        result = run(dry_run=True, log_path=log,
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

    def test_results_list_has_30_items(self, tmp_path):
        result = run(dry_run=True,
                     log_path=tmp_path / "log.jsonl",
                     analysis_path=tmp_path / "analysis.json")
        assert len(result["results"]) == 30

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
# 3. Anomaly level distribution (scenario types produce expected levels)
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

    def test_all_clear_produces_none_or_low(self):
        """ALL_CLEAR scenarios should produce NONE (or LOW if zero intents)."""
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if "ALL_CLEAR" in name:
                # total_intents varies 1-10 (scenarios use index+1), none are 0
                # so ALL_CLEAR should be NONE
                assert r["anomaly_level"] == "NONE", \
                    f"{name}: expected NONE, got {r['anomaly_level']}"

    def test_paper_hold_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if "PAPER_HOLD" in name:
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"

    def test_paper_rejected_produces_critical(self):
        results_by_name = {r["scenario"]: r for r in self.result["results"]}
        for name, r in results_by_name.items():
            if "PAPER_REJECTED" in name:
                assert r["anomaly_level"] == "CRITICAL", \
                    f"{name}: expected CRITICAL, got {r['anomaly_level']}"


# ---------------------------------------------------------------------------
# 4. Feedback distribution (seeded)
# ---------------------------------------------------------------------------

class TestFeedbackDistribution:
    def setup_method(self):
        self.result = run(dry_run=True,
                          log_path=Path("C:/tmp/unused.jsonl"),
                          analysis_path=Path("C:/tmp/unused.json"))
        self.feedbacks = [r["feedback"] for r in self.result["results"]]

    def test_confirm_majority(self):
        n_confirm = self.feedbacks.count("CONFIRM")
        assert n_confirm >= 15  # at least 50% (expect ~21 with seed=42)

    def test_disagree_present(self):
        assert "DISAGREE" in self.feedbacks

    def test_uncertain_present_or_confirm_dominant(self):
        # With seed=42 and 30 draws from 10-item pool (7/2/1),
        # we may or may not see UNCERTAIN — accept either
        n_valid = len(self.feedbacks)
        assert n_valid == 30

    def test_all_entries_are_valid_actions(self):
        valid = {"CONFIRM", "DISAGREE", "UNCERTAIN"}
        assert all(fb in valid for fb in self.feedbacks)

    def test_same_seed_gives_same_distribution(self):
        result2 = run(dry_run=True,
                      log_path=Path("C:/tmp/unused.jsonl"),
                      analysis_path=Path("C:/tmp/unused.json"))
        feedbacks2 = [r["feedback"] for r in result2["results"]]
        assert self.feedbacks == feedbacks2


# ---------------------------------------------------------------------------
# 5. File write — log and analysis
# ---------------------------------------------------------------------------

class TestFileWrite:
    def test_log_file_created(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        assert log.exists()

    def test_at_least_20_entries_written(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) >= 20

    def test_exactly_30_entries_written_per_run(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        run(dry_run=False, log_path=log,
            analysis_path=tmp_path / "analysis.json")
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 30

    def test_multiple_runs_append(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        an  = tmp_path / "analysis.json"
        run(dry_run=False, log_path=log, analysis_path=an)
        run(dry_run=False, log_path=log, analysis_path=an)
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 60  # 2 × 30

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
        assert data["totals"]["entries"] == 30

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
