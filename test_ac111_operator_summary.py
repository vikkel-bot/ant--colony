"""
AC-111 tests — Operator Summary Mini-Script

Coverage:
  - missing all files → no crash
  - corrupt files → no crash, NO DATA shown
  - valid snapshot → overview status shown
  - CRITICAL overview correctly visible
  - source health counts visible
  - recovery top priorities visible
  - URGENT recovery status visible
  - no file writes
  - deterministic output
"""
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from show_operator_summary import build_summary, show

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _snapshot(status: str = "CRITICAL",
              top_risk: str = "Source freshness: all market data stale",
              human_ctx: str = "Disagreement on critical cases — review required") -> dict:
    return {
        "version":         "combined_review_snapshot_v1",
        "overview_status": status,
        "summary":         {"top_risk": top_risk, "human_context": human_ctx},
    }


def _health(status: str = "CRITICAL", fresh: int = 0,
            stale: int = 6, missing: int = 0) -> dict:
    return {
        "version":              "source_health_review_v1",
        "source_health_status": status,
        "markets_fresh":        fresh,
        "markets_stale":        stale,
        "markets_missing":      missing,
    }


def _recovery(status: str = "URGENT", requiring: int = 6,
              markets: list[str] | None = None) -> dict:
    mkts = markets or ["ADA-EUR", "BNB-EUR", "BTC-EUR"]
    return {
        "version":              "source_freshness_recovery_plan_v1",
        "recovery_status":      status,
        "summary":              {"markets_requiring_recovery": requiring},
        "priority_order":       [{"market": m, "recovery_class": "REFRESH_STALE",
                                  "priority": "MEDIUM", "reason_code": "DATA_STALE"}
                                 for m in mkts],
    }


def _captured(snapshot=None, health=None, recovery=None) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        for line in build_summary(snapshot, health, recovery):
            print(line)
    finally:
        sys.stdout = old
    return buf.getvalue()


def _captured_show(tmp_path, snap_file=None, health_file=None, rec_file=None) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        show(
            snapshot_path = snap_file  or tmp_path / "nonexistent_snap.json",
            health_path   = health_file or tmp_path / "nonexistent_health.json",
            recovery_path = rec_file   or tmp_path / "nonexistent_rec.json",
        )
    finally:
        sys.stdout = old
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 1. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_all_none_no_crash(self):
        build_summary(None, None, None)

    def test_all_none_shows_no_data(self):
        output = _captured(None, None, None)
        assert "NO DATA" in output

    def test_corrupt_snapshot_no_crash(self):
        build_summary({"broken": True}, None, None)

    def test_corrupt_health_no_crash(self):
        build_summary(None, {"broken": True}, None)

    def test_corrupt_recovery_no_crash(self):
        build_summary(None, None, {"broken": True})

    def test_missing_files_no_crash(self, tmp_path):
        _captured_show(tmp_path)

    def test_missing_files_shows_no_data(self, tmp_path):
        output = _captured_show(tmp_path)
        assert "NO DATA" in output

    def test_corrupt_file_no_crash(self, tmp_path):
        bad = tmp_path / "snap.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        output = _captured_show(tmp_path, snap_file=bad)
        assert output  # something was printed


# ---------------------------------------------------------------------------
# 2. Header
# ---------------------------------------------------------------------------

class TestHeader:
    def test_header_present(self):
        output = _captured(None, None, None)
        assert "ANT OPERATOR SUMMARY" in output

    def test_header_always_shown(self):
        output = _captured(_snapshot(), _health(), _recovery())
        assert "ANT OPERATOR SUMMARY" in output


# ---------------------------------------------------------------------------
# 3. Overview section
# ---------------------------------------------------------------------------

class TestOverview:
    def test_critical_status_shown(self):
        output = _captured(_snapshot("CRITICAL"), None, None)
        assert "CRITICAL" in output

    def test_healthy_status_shown(self):
        output = _captured(_snapshot("HEALTHY", "No risk", "No attention"), None, None)
        assert "HEALTHY" in output

    def test_top_risk_shown(self):
        output = _captured(_snapshot(top_risk="Source freshness: all market data stale"),
                           None, None)
        assert "Source freshness" in output or "stale" in output.lower()

    def test_human_context_shown(self):
        output = _captured(_snapshot(human_ctx="Disagreement on critical cases"), None, None)
        assert "Disagreement" in output or "critical" in output.lower()

    def test_overview_label_present(self):
        output = _captured(_snapshot(), None, None)
        assert "overview" in output.lower()


# ---------------------------------------------------------------------------
# 4. Source health section
# ---------------------------------------------------------------------------

class TestSourceHealth:
    def test_source_status_shown(self):
        output = _captured(None, _health("CRITICAL"), None)
        assert "CRITICAL" in output

    def test_stale_count_shown(self):
        output = _captured(None, _health(stale=6), None)
        assert "6" in output

    def test_fresh_count_shown(self):
        output = _captured(None, _health(fresh=4, stale=2), None)
        assert "4" in output

    def test_missing_count_shown(self):
        output = _captured(None, _health(missing=1, stale=2, fresh=3), None)
        assert "1" in output

    def test_source_label_present(self):
        output = _captured(None, _health(), None)
        assert "source" in output.lower()


# ---------------------------------------------------------------------------
# 5. Recovery section
# ---------------------------------------------------------------------------

class TestRecovery:
    def test_urgent_status_shown(self):
        output = _captured(None, None, _recovery("URGENT"))
        assert "URGENT" in output

    def test_plan_ready_status_shown(self):
        output = _captured(None, None, _recovery("PLAN_READY"))
        assert "PLAN_READY" in output

    def test_requiring_count_shown(self):
        output = _captured(None, None, _recovery(requiring=6))
        assert "6" in output

    def test_top_markets_shown(self):
        output = _captured(None, None,
                           _recovery(markets=["ADA-EUR", "BTC-EUR", "ETH-EUR"]))
        assert "ADA-EUR" in output or "BTC-EUR" in output

    def test_recovery_label_present(self):
        output = _captured(None, None, _recovery())
        assert "recovery" in output.lower()

    def test_empty_priority_order_no_crash(self):
        rec = _recovery()
        rec["priority_order"] = []
        rec["summary"]["markets_requiring_recovery"] = 0
        output = _captured(None, None, rec)
        assert "recovery" in output.lower()


# ---------------------------------------------------------------------------
# 6. Full output / no file writes
# ---------------------------------------------------------------------------

class TestFullOutput:
    def test_full_output_no_crash(self):
        output = _captured(_snapshot(), _health(), _recovery())
        assert len(output) > 0

    def test_all_three_sections_present(self):
        output = _captured(_snapshot(), _health(), _recovery())
        assert "overview" in output.lower()
        assert "source"   in output.lower()
        assert "recovery" in output.lower()

    def test_deterministic(self):
        snap = _snapshot()
        hlth = _health()
        rec  = _recovery()
        lines1 = build_summary(snap, hlth, rec)
        lines2 = build_summary(snap, hlth, rec)
        assert lines1 == lines2

    def test_show_no_file_writes(self, tmp_path):
        # write valid files
        sp = tmp_path / "snap.json"
        hp = tmp_path / "health.json"
        rp = tmp_path / "rec.json"
        sp.write_text(json.dumps(_snapshot()), encoding="utf-8")
        hp.write_text(json.dumps(_health()),   encoding="utf-8")
        rp.write_text(json.dumps(_recovery()), encoding="utf-8")
        before = set(tmp_path.iterdir())
        _captured_show(tmp_path, snap_file=sp, health_file=hp, rec_file=rp)
        after = set(tmp_path.iterdir())
        assert before == after
