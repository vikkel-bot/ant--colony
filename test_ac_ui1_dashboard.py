"""
AC-UI-1 tests — Feedback Dashboard (Read-Only CLI)

Coverage:
  - missing file → prints NO DATA, no crash
  - corrupted file → prints ERROR, no crash
  - valid analysis file → prints without error
  - key sections present in output
  - no file writes (read-only)
  - output contains expected fields
  - AC-106: source health section shown / missing / corrupt
  - AC-108: combined review snapshot in header shown / missing / corrupt
"""
import sys
import json
import io
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from show_feedback_dashboard import show


# ---------------------------------------------------------------------------
# Minimal valid analysis fixture
# ---------------------------------------------------------------------------

def _analysis() -> dict:
    zero_group = {"entries": 0, "confirm": 0, "disagree": 0, "uncertain": 0}
    return {
        "version":   "feedback_analysis_v1",
        "component": "build_feedback_analysis_lite",
        "ts_utc":    "2026-04-10T12:00:00Z",
        "totals": {
            "entries": 30, "confirm": 22, "disagree": 5,
            "uncertain": 3, "invalid": 0,
        },
        "rates": {
            "confirm_rate": 0.7333, "disagree_rate": 0.1667, "uncertain_rate": 0.1,
        },
        "by_action_class": {
            "NO_ACTION":                {**zero_group, "entries": 10, "confirm": 8, "disagree": 2, "uncertain": 0},
            "REVIEW_STATUS":            zero_group,
            "REVIEW_CONFLICT":          {**zero_group, "entries": 2,  "confirm": 1, "disagree": 1, "uncertain": 0},
            "REVIEW_BLOCKING_FINDINGS": zero_group,
            "REVIEW_MISSING_INPUT":     zero_group,
            "REVIEW_CRITICAL_STATE":    {**zero_group, "entries": 18, "confirm": 13, "disagree": 2, "uncertain": 3},
        },
        "by_urgency": {
            "NONE":     {**zero_group, "entries": 10, "confirm": 8, "disagree": 2, "uncertain": 0},
            "LOW":      zero_group,
            "MEDIUM":   zero_group,
            "HIGH":     {**zero_group, "entries": 4,  "confirm": 3, "disagree": 1, "uncertain": 0},
            "CRITICAL": {**zero_group, "entries": 16, "confirm": 11, "disagree": 2, "uncertain": 3},
        },
        "signals": {
            "system_human_alignment": "HIGH",
            "needs_attention":        True,
            "attention_reason_code":  "CRITICAL_DISAGREE",
        },
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


def _captured(path: Path, health_path: Path | None = None,
              snapshot_path: Path | None = None) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        # Pass nonexistent paths when not supplied so tests stay deterministic
        # regardless of what exists on disk.
        hp = health_path   if health_path   is not None else Path("C:/nonexistent/health.json")
        sp = snapshot_path if snapshot_path is not None else Path("C:/nonexistent/snapshot.json")
        show(path, source_health_path=hp, snapshot_path=sp)
    finally:
        sys.stdout = old
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 1. Missing file
# ---------------------------------------------------------------------------

class TestMissingFile:
    def test_no_crash(self, tmp_path):
        _captured(tmp_path / "nonexistent.json")  # must not raise

    def test_prints_no_data(self, tmp_path):
        output = _captured(tmp_path / "nonexistent.json")
        assert "NO DATA" in output

    def test_no_file_written(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        _captured(path)
        assert not path.exists()  # dashboard must not create anything


# ---------------------------------------------------------------------------
# 2. Corrupted file
# ---------------------------------------------------------------------------

class TestCorruptedFile:
    def test_no_crash(self, tmp_path):
        bad = tmp_path / "analysis.json"
        bad.write_text("{ not valid json {{", encoding="utf-8")
        _captured(bad)  # must not raise

    def test_prints_error(self, tmp_path):
        bad = tmp_path / "analysis.json"
        bad.write_text("garbage", encoding="utf-8")
        output = _captured(bad)
        assert "ERROR" in output


# ---------------------------------------------------------------------------
# 3. Valid file — output correctness
# ---------------------------------------------------------------------------

class TestValidFile:
    def setup_method(self):
        pass

    def _write_and_show(self, tmp_path) -> str:
        p = tmp_path / "analysis.json"
        p.write_text(json.dumps(_analysis()), encoding="utf-8")
        return _captured(p)

    def test_no_crash(self, tmp_path):
        self._write_and_show(tmp_path)

    def test_contains_entries_count(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "30" in output

    def test_contains_confirm(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "CONFIRM" in output

    def test_contains_disagree(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "DISAGREE" in output

    def test_contains_uncertain(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "UNCERTAIN" in output

    def test_contains_alignment(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "HIGH" in output

    def test_contains_needs_attention(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "needs_attention" in output

    def test_contains_attention_reason_code(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "CRITICAL_DISAGREE" in output

    def test_contains_action_class_section(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "Action Class" in output or "action_class" in output.lower() \
               or "REVIEW_CONFLICT" in output

    def test_contains_urgency_section(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "Urgency" in output or "CRITICAL" in output

    def test_contains_flags(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "non_binding" in output

    def test_no_file_written(self, tmp_path):
        p = tmp_path / "analysis.json"
        p.write_text(json.dumps(_analysis()), encoding="utf-8")
        before = list(tmp_path.iterdir())
        _captured(p)
        after = list(tmp_path.iterdir())
        assert set(str(f) for f in before) == set(str(f) for f in after)

    def test_rates_shown(self, tmp_path):
        output = self._write_and_show(tmp_path)
        # confirm_rate 0.7333 → 73.3%
        assert "%" in output

    def test_ts_shown(self, tmp_path):
        output = self._write_and_show(tmp_path)
        assert "2026-04-10" in output


# ---------------------------------------------------------------------------
# 4. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_all_zeros_no_crash(self, tmp_path):
        zero_group = {"entries": 0, "confirm": 0, "disagree": 0, "uncertain": 0}
        data = {
            "version": "feedback_analysis_v1", "component": "x",
            "ts_utc": "2026-04-10T00:00:00Z",
            "totals": {"entries": 0, "confirm": 0, "disagree": 0, "uncertain": 0, "invalid": 0},
            "rates": {"confirm_rate": 0.0, "disagree_rate": 0.0, "uncertain_rate": 0.0},
            "by_action_class": {k: dict(zero_group) for k in (
                "NO_ACTION", "REVIEW_STATUS", "REVIEW_CONFLICT",
                "REVIEW_BLOCKING_FINDINGS", "REVIEW_MISSING_INPUT", "REVIEW_CRITICAL_STATE",
            )},
            "by_urgency": {k: dict(zero_group) for k in ("NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL")},
            "signals": {"system_human_alignment": "HIGH", "needs_attention": False,
                        "attention_reason_code": "NONE"},
            "flags": {"non_binding": True, "simulation_only": True,
                      "paper_only": True, "live_activation_allowed": False},
        }
        p = tmp_path / "analysis.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        _captured(p)  # must not raise

    def test_missing_keys_no_crash(self, tmp_path):
        p = tmp_path / "analysis.json"
        p.write_text(json.dumps({"version": "x"}), encoding="utf-8")
        _captured(p)  # must not raise

    def test_empty_json_object_no_crash(self, tmp_path):
        p = tmp_path / "analysis.json"
        p.write_text("{}", encoding="utf-8")
        _captured(p)


# ---------------------------------------------------------------------------
# 5. Source Health section (AC-106)
# ---------------------------------------------------------------------------

def _health(status: str = "HEALTHY", blocking: bool = False,
            reason_code: str = "ALL_SOURCES_FRESH",
            fresh: int = 3, stale: int = 0, missing: int = 0,
            affected: list | None = None) -> dict:
    return {
        "version":                   "source_health_review_v1",
        "component":                 "build_source_health_review_lite",
        "ts_utc":                    "2026-04-10T12:00:00Z",
        "source_health_status":      status,
        "markets_total":             fresh + stale + missing,
        "markets_fresh":             fresh,
        "markets_stale":             stale,
        "markets_missing":           missing,
        "freshness_blocking_review": blocking,
        "primary_reason":            "test reason",
        "primary_reason_code":       reason_code,
        "affected_markets":          affected or [],
        "sources":                   {"adapter_loaded": True, "md_loaded": True,
                                      "md_refresh_ts": "2026-04-07T09:46:20Z"},
        "flags": {"non_binding": True, "simulation_only": True,
                  "paper_only": True, "live_activation_allowed": False},
    }


def _write_health(tmp_path, data: dict) -> Path:
    p = tmp_path / "health.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _analysis_path(tmp_path) -> Path:
    p = tmp_path / "analysis.json"
    p.write_text(json.dumps(_analysis()), encoding="utf-8")
    return p


class TestSourceHealth:
    def test_missing_health_file_no_crash(self, tmp_path):
        ap = _analysis_path(tmp_path)
        _captured(ap, health_path=tmp_path / "nonexistent.json")

    def test_missing_health_file_shows_no_data(self, tmp_path):
        ap = _analysis_path(tmp_path)
        output = _captured(ap, health_path=tmp_path / "nonexistent.json")
        assert "NO DATA" in output

    def test_corrupt_health_file_no_crash(self, tmp_path):
        ap = _analysis_path(tmp_path)
        bad = tmp_path / "health.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        _captured(ap, health_path=bad)

    def test_corrupt_health_file_shows_error(self, tmp_path):
        ap = _analysis_path(tmp_path)
        bad = tmp_path / "health.json"
        bad.write_text("garbage", encoding="utf-8")
        output = _captured(ap, health_path=bad)
        assert "ERROR" in output

    def test_healthy_status_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health("HEALTHY", False, "ALL_SOURCES_FRESH",
                                             fresh=3, stale=0, missing=0))
        output = _captured(ap, health_path=hp)
        assert "HEALTHY" in output

    def test_critical_status_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health(
            "CRITICAL", True, "ALL_SOURCES_STALE",
            fresh=0, stale=6, missing=0,
            affected=["ADA-EUR", "BTC-EUR", "ETH-EUR", "SOL-EUR", "BNB-EUR", "XRP-EUR"],
        ))
        output = _captured(ap, health_path=hp)
        assert "CRITICAL" in output
        assert "ALL_SOURCES_STALE" in output

    def test_affected_markets_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health(
            "DEGRADED", False, "MAJORITY_SOURCES_STALE",
            fresh=2, stale=1, missing=0,
            affected=["SOL-EUR"],
        ))
        output = _captured(ap, health_path=hp)
        assert "SOL-EUR" in output

    def test_blocking_review_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health("CRITICAL", True, "ALL_SOURCES_STALE",
                                             fresh=0, stale=3, missing=0))
        output = _captured(ap, health_path=hp)
        assert "blocking_review" in output or "blocking" in output.lower()

    def test_fresh_stale_miss_counts_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health("DEGRADED", False,
                                             "MAJORITY_SOURCES_STALE",
                                             fresh=2, stale=1, missing=0))
        output = _captured(ap, health_path=hp)
        # fresh/stale/miss line must contain the counts
        assert "2" in output and "1" in output

    def test_review_context_line_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health("CRITICAL", True))
        output = _captured(ap, health_path=hp)
        assert "review context" in output

    def test_review_context_contains_source_status(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health("CRITICAL", True))
        output = _captured(ap, health_path=hp)
        assert "SOURCE_CRITICAL" in output

    def test_no_extra_files_written(self, tmp_path):
        ap = _analysis_path(tmp_path)
        hp = _write_health(tmp_path, _health())
        before = set(f.name for f in tmp_path.iterdir())
        _captured(ap, health_path=hp)
        after  = set(f.name for f in tmp_path.iterdir())
        assert before == after


# ---------------------------------------------------------------------------
# 6. Combined review snapshot header (AC-108)
# ---------------------------------------------------------------------------

def _snapshot(status: str = "HEALTHY", top_risk: str = "No risk identified",
              human_ctx: str = "No attention trigger — review aligned") -> dict:
    return {
        "version":              "combined_review_snapshot_v1",
        "component":            "build_combined_review_snapshot_lite",
        "ts_utc":               "2026-04-10T12:00:00Z",
        "overview_status":      status,
        "overview_reason_code": "TEST_CODE",
        "source_health":        {"status": "HEALTHY", "blocking_review": False,
                                 "reason_code": "ALL_SOURCES_FRESH",
                                 "markets_total": 3, "markets_fresh": 3,
                                 "markets_stale": 0, "markets_missing": 0},
        "review_health":        {"alignment": "HIGH", "needs_attention": False,
                                 "attention_reason_code": "NONE",
                                 "entries": 30, "confirm_rate": 0.7,
                                 "disagree_rate": 0.1, "uncertain_rate": 0.2},
        "summary":              {"top_risk": top_risk, "human_context": human_ctx},
        "sources":              {"analysis_loaded": True, "health_loaded": True},
        "flags": {"non_binding": True, "simulation_only": True,
                  "paper_only": True, "live_activation_allowed": False},
    }


def _write_snapshot(tmp_path, data: dict) -> Path:
    p = tmp_path / "snapshot.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


class TestCombinedSnapshot:
    def test_missing_snapshot_no_crash(self, tmp_path):
        ap = _analysis_path(tmp_path)
        _captured(ap, snapshot_path=tmp_path / "nonexistent.json")

    def test_missing_snapshot_shows_no_data(self, tmp_path):
        ap = _analysis_path(tmp_path)
        output = _captured(ap, snapshot_path=tmp_path / "nonexistent.json")
        assert "NO DATA" in output

    def test_corrupt_snapshot_no_crash(self, tmp_path):
        ap  = _analysis_path(tmp_path)
        bad = tmp_path / "snap.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        _captured(ap, snapshot_path=bad)

    def test_corrupt_snapshot_shows_error(self, tmp_path):
        ap  = _analysis_path(tmp_path)
        bad = tmp_path / "snap.json"
        bad.write_text("garbage", encoding="utf-8")
        output = _captured(ap, snapshot_path=bad)
        assert "ERROR" in output

    def test_overview_status_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot("CRITICAL"))
        output = _captured(ap, snapshot_path=sp)
        assert "CRITICAL" in output

    def test_top_risk_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot(
            "CRITICAL", top_risk="Source freshness: all market data stale"))
        output = _captured(ap, snapshot_path=sp)
        assert "Source freshness" in output or "stale" in output.lower()

    def test_human_context_shown(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot(
            "ATTENTION", human_ctx="Disagreement on critical cases — review required"))
        output = _captured(ap, snapshot_path=sp)
        assert "Disagreement" in output or "review required" in output

    def test_healthy_status_shown_in_overview(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot("HEALTHY"))
        output = _captured(ap, snapshot_path=sp)
        assert "HEALTHY" in output

    def test_overview_section_header_present(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot())
        output = _captured(ap, snapshot_path=sp)
        assert "Overview" in output or "overview" in output.lower()

    def test_no_extra_files_written(self, tmp_path):
        ap = _analysis_path(tmp_path)
        sp = _write_snapshot(tmp_path, _snapshot())
        before = set(f.name for f in tmp_path.iterdir())
        _captured(ap, snapshot_path=sp)
        after  = set(f.name for f in tmp_path.iterdir())
        assert before == after
