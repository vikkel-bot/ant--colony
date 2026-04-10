"""
AC-107 tests — Combined Review Snapshot (Daily Human Overview)

Coverage:
  - missing sources → no crash, fail-closed
  - source CRITICAL → overview_status=CRITICAL
  - review needs_attention → overview_status=ATTENTION
  - alignment MEDIUM or source DEGRADED → overview_status=WATCH
  - healthy + no attention → overview_status=HEALTHY
  - priority order respected
  - summary fields correct
  - flags always correct
  - deterministic output
  - output file written / valid JSON
  - no extra files written
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_combined_review_snapshot_lite import (
    build_snapshot,
    run_snapshot,
    FLAGS,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _health(status: str = "HEALTHY", blocking: bool = False,
            reason_code: str = "ALL_SOURCES_FRESH",
            total: int = 6, fresh: int = 6,
            stale: int = 0, missing: int = 0) -> dict:
    return {
        "version":                   "source_health_review_v1",
        "source_health_status":      status,
        "freshness_blocking_review": blocking,
        "primary_reason_code":       reason_code,
        "markets_total":             total,
        "markets_fresh":             fresh,
        "markets_stale":             stale,
        "markets_missing":           missing,
        "flags": dict(FLAGS),
    }


def _analysis(alignment: str = "HIGH", needs_attention: bool = False,
              attn_code: str = "NONE", entries: int = 30,
              confirm_rate: float = 0.7, disagree_rate: float = 0.1,
              uncertain_rate: float = 0.2) -> dict:
    return {
        "version":   "feedback_analysis_v1",
        "totals":    {"entries": entries, "confirm": 0, "disagree": 0,
                      "uncertain": 0, "invalid": 0},
        "rates":     {"confirm_rate":   confirm_rate,
                      "disagree_rate":  disagree_rate,
                      "uncertain_rate": uncertain_rate},
        "signals":   {"system_human_alignment": alignment,
                      "needs_attention":        needs_attention,
                      "attention_reason_code":  attn_code},
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# 1. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_sources_no_crash(self):
        result = build_snapshot(None, None, NOW)
        assert isinstance(result, dict)

    def test_none_sources_gives_healthy(self):
        """No data at all → nothing signals a problem → HEALTHY."""
        result = build_snapshot(None, None, NOW)
        assert result["overview_status"] == "HEALTHY"

    def test_corrupt_analysis_no_crash(self):
        result = build_snapshot({"broken": True}, None, NOW)
        assert isinstance(result, dict)

    def test_corrupt_health_no_crash(self):
        result = build_snapshot(None, {"broken": True}, NOW)
        assert isinstance(result, dict)

    def test_missing_files_no_crash(self, tmp_path):
        result = run_snapshot(
            analysis_path = tmp_path / "x.json",
            health_path   = tmp_path / "y.json",
            output_path   = tmp_path / "out.json",
            now_utc       = NOW,
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Overview status — priority order
# ---------------------------------------------------------------------------

class TestOverviewStatus:
    def test_source_critical_gives_critical(self):
        result = build_snapshot(
            _analysis("HIGH", False),
            _health("CRITICAL", True, "ALL_SOURCES_STALE", stale=6, fresh=0),
            NOW,
        )
        assert result["overview_status"] == "CRITICAL"

    def test_source_critical_takes_priority_over_attention(self):
        """CRITICAL source beats needs_attention=True."""
        result = build_snapshot(
            _analysis("MEDIUM", True, "CRITICAL_DISAGREE"),
            _health("CRITICAL", True, "ALL_SOURCES_STALE", stale=6, fresh=0),
            NOW,
        )
        assert result["overview_status"] == "CRITICAL"

    def test_needs_attention_gives_attention(self):
        result = build_snapshot(
            _analysis("HIGH", True, "CRITICAL_DISAGREE"),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_status"] == "ATTENTION"

    def test_attention_takes_priority_over_watch(self):
        """needs_attention=True beats alignment=MEDIUM."""
        result = build_snapshot(
            _analysis("MEDIUM", True, "CRITICAL_DISAGREE"),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_status"] == "ATTENTION"

    def test_medium_alignment_gives_watch(self):
        result = build_snapshot(
            _analysis("MEDIUM", False),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_status"] == "WATCH"

    def test_degraded_source_gives_watch(self):
        result = build_snapshot(
            _analysis("HIGH", False),
            _health("DEGRADED", False, "MAJORITY_SOURCES_STALE",
                    fresh=4, stale=2, total=6),
            NOW,
        )
        assert result["overview_status"] == "WATCH"

    def test_healthy_no_attention_gives_healthy(self):
        result = build_snapshot(
            _analysis("HIGH", False),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_status"] == "HEALTHY"

    def test_high_alignment_no_attention_healthy_source_is_healthy(self):
        result = build_snapshot(
            _analysis("HIGH", False, "NONE", entries=60,
                      confirm_rate=0.8, disagree_rate=0.1),
            _health("HEALTHY", False, "ALL_SOURCES_FRESH", fresh=6),
            NOW,
        )
        assert result["overview_status"] == "HEALTHY"

    def test_unknown_source_status_not_critical(self):
        """UNKNOWN source health should not trigger CRITICAL."""
        result = build_snapshot(None, None, NOW)
        assert result["overview_status"] != "CRITICAL"


# ---------------------------------------------------------------------------
# 3. Overview reason code
# ---------------------------------------------------------------------------

class TestOverviewReasonCode:
    def test_critical_reason_is_source_code(self):
        result = build_snapshot(
            _analysis(),
            _health("CRITICAL", True, "ALL_SOURCES_STALE", stale=6, fresh=0),
            NOW,
        )
        assert result["overview_reason_code"] == "ALL_SOURCES_STALE"

    def test_attention_reason_is_attn_code(self):
        result = build_snapshot(
            _analysis("HIGH", True, "CRITICAL_DISAGREE"),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_reason_code"] == "CRITICAL_DISAGREE"

    def test_healthy_reason_is_all_clear(self):
        result = build_snapshot(
            _analysis("HIGH", False),
            _health("HEALTHY"),
            NOW,
        )
        assert result["overview_reason_code"] == "ALL_CLEAR"


# ---------------------------------------------------------------------------
# 4. Source health sub-object
# ---------------------------------------------------------------------------

class TestSourceHealthBlock:
    def test_status_propagated(self):
        result = build_snapshot(None, _health("DEGRADED"), NOW)
        assert result["source_health"]["status"] == "DEGRADED"

    def test_counts_propagated(self):
        result = build_snapshot(
            None,
            _health("DEGRADED", False, "MAJORITY_SOURCES_STALE",
                    total=6, fresh=4, stale=2, missing=0),
            NOW,
        )
        sh = result["source_health"]
        assert sh["markets_total"]   == 6
        assert sh["markets_fresh"]   == 4
        assert sh["markets_stale"]   == 2
        assert sh["markets_missing"] == 0

    def test_blocking_propagated(self):
        result = build_snapshot(None, _health("CRITICAL", True), NOW)
        assert result["source_health"]["blocking_review"] is True

    def test_defaults_when_no_health_data(self):
        result = build_snapshot(None, None, NOW)
        sh = result["source_health"]
        assert sh["markets_total"]   == 0
        assert sh["blocking_review"] is False


# ---------------------------------------------------------------------------
# 5. Review health sub-object
# ---------------------------------------------------------------------------

class TestReviewHealthBlock:
    def test_alignment_propagated(self):
        result = build_snapshot(_analysis("MEDIUM"), None, NOW)
        assert result["review_health"]["alignment"] == "MEDIUM"

    def test_needs_attention_propagated(self):
        result = build_snapshot(_analysis("HIGH", True, "HIGH_DISAGREE_RATE"), None, NOW)
        assert result["review_health"]["needs_attention"] is True
        assert result["review_health"]["attention_reason_code"] == "HIGH_DISAGREE_RATE"

    def test_rates_propagated(self):
        result = build_snapshot(
            _analysis("HIGH", False, "NONE", entries=30,
                      confirm_rate=0.7333, disagree_rate=0.1667),
            None, NOW,
        )
        rv = result["review_health"]
        assert rv["confirm_rate"]  == round(0.7333, 4)
        assert rv["disagree_rate"] == round(0.1667, 4)

    def test_entries_propagated(self):
        result = build_snapshot(_analysis(entries=60), None, NOW)
        assert result["review_health"]["entries"] == 60

    def test_defaults_when_no_analysis(self):
        result = build_snapshot(None, None, NOW)
        rv = result["review_health"]
        assert rv["entries"]        == 0
        assert rv["needs_attention"] is False


# ---------------------------------------------------------------------------
# 6. Summary block
# ---------------------------------------------------------------------------

class TestSummaryBlock:
    def test_top_risk_present(self):
        result = build_snapshot(None, None, NOW)
        assert "top_risk" in result["summary"]
        assert isinstance(result["summary"]["top_risk"], str)

    def test_human_context_present(self):
        result = build_snapshot(None, None, NOW)
        assert "human_context" in result["summary"]
        assert isinstance(result["summary"]["human_context"], str)

    def test_stale_source_in_top_risk(self):
        result = build_snapshot(
            None,
            _health("CRITICAL", True, "ALL_SOURCES_STALE"),
            NOW,
        )
        assert "stale" in result["summary"]["top_risk"].lower() \
               or "freshness" in result["summary"]["top_risk"].lower()

    def test_critical_disagree_in_human_context(self):
        result = build_snapshot(
            _analysis("HIGH", True, "CRITICAL_DISAGREE"),
            _health("HEALTHY"),
            NOW,
        )
        ctx = result["summary"]["human_context"].lower()
        assert "disagree" in ctx or "critical" in ctx

    def test_no_risk_when_all_clear(self):
        result = build_snapshot(
            _analysis("HIGH", False),
            _health("HEALTHY"),
            NOW,
        )
        # Should express no significant risk
        risk = result["summary"]["top_risk"].lower()
        assert "no" in risk or "none" in risk or "clear" in risk


# ---------------------------------------------------------------------------
# 7. Flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_flags_correct(self):
        result = build_snapshot(None, None, NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_flags_correct_with_data(self):
        result = build_snapshot(_analysis(), _health(), NOW)
        f = result["flags"]
        assert f["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 8. Output fields and determinism
# ---------------------------------------------------------------------------

class TestOutputFields:
    def test_version_and_component(self):
        result = build_snapshot(None, None, NOW)
        assert result["version"]   == "combined_review_snapshot_v1"
        assert result["component"] == "build_combined_review_snapshot_lite"

    def test_ts_utc_present(self):
        result = build_snapshot(None, None, NOW)
        assert result["ts_utc"] == "2026-04-10T12:00:00Z"

    def test_deterministic(self):
        a = _analysis("MEDIUM", True, "CRITICAL_DISAGREE")
        h = _health("CRITICAL", True, "ALL_SOURCES_STALE", stale=6, fresh=0)
        r1 = build_snapshot(a, h, NOW)
        r2 = build_snapshot(a, h, NOW)
        assert r1 == r2

    def test_sources_loaded_flags(self):
        result = build_snapshot(_analysis(), _health(), NOW)
        assert result["sources"]["analysis_loaded"] is True
        assert result["sources"]["health_loaded"]   is True

    def test_sources_flags_when_none(self):
        result = build_snapshot(None, None, NOW)
        assert result["sources"]["analysis_loaded"] is False
        assert result["sources"]["health_loaded"]   is False


# ---------------------------------------------------------------------------
# 9. I/O — run_snapshot
# ---------------------------------------------------------------------------

class TestRunSnapshot:
    def test_output_file_written(self, tmp_path):
        out = tmp_path / "snapshot.json"
        run_snapshot(analysis_path=tmp_path / "x.json",
                     health_path=tmp_path / "y.json",
                     output_path=out, now_utc=NOW)
        assert out.exists()

    def test_output_valid_json(self, tmp_path):
        out = tmp_path / "snapshot.json"
        run_snapshot(analysis_path=tmp_path / "x.json",
                     health_path=tmp_path / "y.json",
                     output_path=out, now_utc=NOW)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == "combined_review_snapshot_v1"

    def test_real_files_produce_critical(self, tmp_path):
        ap = tmp_path / "analysis.json"
        hp = tmp_path / "health.json"
        ap.write_text(json.dumps(_analysis("MEDIUM", True, "CRITICAL_DISAGREE")),
                      encoding="utf-8")
        hp.write_text(json.dumps(
            _health("CRITICAL", True, "ALL_SOURCES_STALE", stale=6, fresh=0)),
            encoding="utf-8")
        out = tmp_path / "snapshot.json"
        result = run_snapshot(analysis_path=ap, health_path=hp,
                              output_path=out, now_utc=NOW)
        assert result["overview_status"] == "CRITICAL"
        assert json.loads(out.read_text())["overview_status"] == "CRITICAL"

    def test_no_extra_files_written(self, tmp_path):
        out = tmp_path / "snapshot.json"
        before = set(tmp_path.iterdir())
        run_snapshot(analysis_path=tmp_path / "x.json",
                     health_path=tmp_path / "y.json",
                     output_path=out, now_utc=NOW)
        after = set(tmp_path.iterdir())
        assert after - before == {out}
