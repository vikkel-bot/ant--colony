"""
AC-119 tests — Daily Status Writer (Compact Operator File)

Coverage:
  - valid inputs → exact format (header, ts, all 9 content keys)
  - missing files → NO DATA on affected fields
  - corrupt files → ERROR on affected fields
  - max 10 non-blank lines
  - no colour codes
  - no JSON in output
  - output file written + UTF-8
  - no extra files
  - deterministic
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_daily_status_file_lite import build_daily_status, run_daily_status

_NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)
_TS  = "2026-04-10T12:00:00Z"

# ---------------------------------------------------------------------------
# Source tuple helpers
# ---------------------------------------------------------------------------

def _ok(d: dict):   return (d, None)
def _missing():     return (None, "NO DATA")
def _corrupt():     return (None, "ERROR")


def _snapshot(status="CRITICAL", top_risk="Source freshness: all market data stale",
              human_ctx="Disagreement on critical cases"):
    return _ok({
        "version":        "combined_review_snapshot_v1",
        "overview_status": status,
        "summary": {"top_risk": top_risk, "human_context": human_ctx},
    })


def _readiness(status="NOT_READY", score=0):
    return _ok({
        "version":          "system_readiness_score_v1",
        "readiness_status": status,
        "readiness_score":  score,
    })


def _trigger(status="URGENT", action="RUN_MANUAL_REFRESH_CHECK_NOW", window="NOW"):
    return _ok({
        "version":        "refresh_trigger_v1",
        "trigger_status": status,
        "operator_guidance": {"recommended_action": action, "recommended_window": window},
    })


def _health(status="CRITICAL", fresh=0, stale=6, missing=0):
    return _ok({
        "version":              "source_health_review_v1",
        "source_health_status": status,
        "markets_fresh":        fresh,
        "markets_stale":        stale,
        "markets_missing":      missing,
    })


def _recovery(status="URGENT", req=6, markets=None):
    mkts = markets or ["ADA-EUR", "BNB-EUR", "BTC-EUR"]
    return _ok({
        "version":         "source_freshness_recovery_plan_v1",
        "recovery_status": status,
        "summary":         {"markets_requiring_recovery": req},
        "priority_order":  [{"market": m} for m in mkts],
    })


def _build(snap=None, rd=None, tr=None, hl=None, rc=None):
    """Build with defaults — pass None to use missing() sentinel."""
    return build_daily_status(
        snap or _missing(),
        rd   or _missing(),
        tr   or _missing(),
        hl   or _missing(),
        rc   or _missing(),
        _NOW,
    )


# ---------------------------------------------------------------------------
# 1. Format structure
# ---------------------------------------------------------------------------

class TestFormatStructure:
    def _full(self):
        return build_daily_status(
            _snapshot(), _readiness(), _trigger(), _health(), _recovery(), _NOW
        )

    def test_header_first_line(self):
        lines = self._full().splitlines()
        assert lines[0] == "=== ANT DAILY STATUS ==="

    def test_ts_line_present(self):
        assert "ts        :" in self._full()

    def test_ts_value_correct(self):
        assert _TS in self._full()

    def test_overview_line_present(self):
        assert "overview  :" in self._full()

    def test_readiness_line_present(self):
        assert "readiness :" in self._full()

    def test_trigger_line_present(self):
        assert "trigger   :" in self._full()

    def test_source_line_present(self):
        assert "source    :" in self._full()

    def test_recovery_line_present(self):
        assert "recovery  :" in self._full()

    def test_risk_line_present(self):
        assert "risk      :" in self._full()

    def test_human_line_present(self):
        assert "human     :" in self._full()

    def test_max_10_non_blank_lines(self):
        output = self._full()
        non_blank = [l for l in output.splitlines() if l.strip()]
        assert len(non_blank) <= 10

    def test_no_ansi_escape_codes(self):
        assert "\033[" not in self._full()

    def test_no_json_braces(self):
        # Should be plain text, not JSON
        output = self._full()
        assert output.count("{") == 0 and output.count("}") == 0


# ---------------------------------------------------------------------------
# 2. Valid inputs — field values
# ---------------------------------------------------------------------------

class TestValidInputs:
    def test_overview_value(self):
        output = _build(snap=_snapshot("CRITICAL"))
        assert "overview  : CRITICAL" in output

    def test_readiness_value(self):
        output = _build(rd=_readiness("NOT_READY", 0))
        assert "NOT_READY (0/100)" in output

    def test_trigger_value(self):
        output = _build(tr=_trigger("URGENT", "RUN_MANUAL_REFRESH_CHECK_NOW", "NOW"))
        assert "URGENT (RUN_MANUAL_REFRESH_CHECK_NOW/NOW)" in output

    def test_source_value(self):
        output = _build(hl=_health("CRITICAL", fresh=0, stale=6, missing=0))
        assert "CRITICAL (fresh=0 stale=6 missing=0)" in output

    def test_recovery_value(self):
        output = _build(rc=_recovery("URGENT", req=6, markets=["ADA-EUR", "BNB-EUR", "BTC-EUR"]))
        assert "URGENT (req=6 top=ADA-EUR, BNB-EUR, BTC-EUR)" in output

    def test_risk_value(self):
        output = _build(snap=_snapshot(top_risk="Source freshness: all market data stale"))
        assert "Source freshness: all market data stale" in output

    def test_human_value(self):
        output = _build(snap=_snapshot(human_ctx="Disagreement on critical cases"))
        assert "Disagreement on critical cases" in output

    def test_healthy_overview(self):
        output = _build(snap=_snapshot("HEALTHY"))
        assert "overview  : HEALTHY" in output

    def test_ready_readiness(self):
        output = _build(rd=_readiness("READY", 100))
        assert "READY (100/100)" in output

    def test_none_trigger_watch(self):
        output = _build(tr=_trigger("NONE", "NONE", "NONE"))
        assert "NONE" in output

    def test_recovery_empty_priority_order(self):
        rd = _ok({
            "version": "source_freshness_recovery_plan_v1",
            "recovery_status": "NONE",
            "summary": {"markets_requiring_recovery": 0},
            "priority_order": [],
        })
        output = _build(rc=rd)
        assert "NONE" in output
        assert "req=0" in output
        assert "top=—" in output


# ---------------------------------------------------------------------------
# 3. Missing sources → NO DATA
# ---------------------------------------------------------------------------

class TestMissingSources:
    def test_all_missing_no_crash(self):
        _build()  # all _missing() defaults

    def test_snapshot_missing_overview_no_data(self):
        output = _build(snap=_missing())
        assert "overview  : NO DATA" in output

    def test_snapshot_missing_risk_no_data(self):
        output = _build(snap=_missing())
        assert "risk      : NO DATA" in output

    def test_snapshot_missing_human_no_data(self):
        output = _build(snap=_missing())
        assert "human     : NO DATA" in output

    def test_readiness_missing_no_data(self):
        output = _build(rd=_missing())
        assert "readiness : NO DATA" in output

    def test_trigger_missing_no_data(self):
        output = _build(tr=_missing())
        assert "trigger   : NO DATA" in output

    def test_health_missing_no_data(self):
        output = _build(hl=_missing())
        assert "source    : NO DATA" in output

    def test_recovery_missing_no_data(self):
        output = _build(rc=_missing())
        assert "recovery  : NO DATA" in output

    def test_all_missing_max_lines(self):
        output = _build()
        non_blank = [l for l in output.splitlines() if l.strip()]
        assert len(non_blank) <= 10


# ---------------------------------------------------------------------------
# 4. Corrupt sources → ERROR
# ---------------------------------------------------------------------------

class TestCorruptSources:
    def test_all_corrupt_no_crash(self):
        build_daily_status(
            _corrupt(), _corrupt(), _corrupt(), _corrupt(), _corrupt(), _NOW
        )

    def test_snapshot_corrupt_overview_error(self):
        output = build_daily_status(
            _corrupt(), _missing(), _missing(), _missing(), _missing(), _NOW
        )
        assert "overview  : ERROR" in output

    def test_snapshot_corrupt_risk_error(self):
        output = build_daily_status(
            _corrupt(), _missing(), _missing(), _missing(), _missing(), _NOW
        )
        assert "risk      : ERROR" in output

    def test_readiness_corrupt_error(self):
        output = build_daily_status(
            _missing(), _corrupt(), _missing(), _missing(), _missing(), _NOW
        )
        assert "readiness : ERROR" in output

    def test_trigger_corrupt_error(self):
        output = build_daily_status(
            _missing(), _missing(), _corrupt(), _missing(), _missing(), _NOW
        )
        assert "trigger   : ERROR" in output

    def test_health_corrupt_error(self):
        output = build_daily_status(
            _missing(), _missing(), _missing(), _corrupt(), _missing(), _NOW
        )
        assert "source    : ERROR" in output

    def test_recovery_corrupt_error(self):
        output = build_daily_status(
            _missing(), _missing(), _missing(), _missing(), _corrupt(), _NOW
        )
        assert "recovery  : ERROR" in output


# ---------------------------------------------------------------------------
# 5. I/O — run_daily_status
# ---------------------------------------------------------------------------

class TestRunDailyStatus:
    def test_missing_sources_no_crash(self, tmp_path):
        run_daily_status(
            snapshot_path  = tmp_path / "no_snap.json",
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = tmp_path / "status.txt",
            now_utc        = _NOW,
        )

    def test_output_file_written(self, tmp_path):
        out = tmp_path / "status.txt"
        run_daily_status(
            snapshot_path  = tmp_path / "no_snap.json",
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert out.exists()

    def test_output_is_text(self, tmp_path):
        out = tmp_path / "status.txt"
        run_daily_status(
            snapshot_path  = tmp_path / "no_snap.json",
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        content = out.read_text(encoding="utf-8")
        assert "=== ANT DAILY STATUS ===" in content

    def test_output_with_real_data(self, tmp_path):
        # Write source files
        def _w(name, data):
            p = tmp_path / name
            p.write_text(json.dumps(data), encoding="utf-8")
            return p
        sp  = _w("snap.json",     {"version": "v1", "overview_status": "HEALTHY",
                                   "summary": {"top_risk": "None", "human_context": "OK"}})
        rdp = _w("rd.json",       {"version": "v1", "readiness_status": "READY",
                                   "readiness_score": 100})
        tp  = _w("tr.json",       {"version": "v1", "trigger_status": "NONE",
                                   "operator_guidance": {"recommended_action": "NONE",
                                                         "recommended_window": "NONE"}})
        hlp = _w("hl.json",       {"version": "v1", "source_health_status": "HEALTHY",
                                   "markets_fresh": 6, "markets_stale": 0,
                                   "markets_missing": 0})
        rcp = _w("rc.json",       {"version": "v1", "recovery_status": "NONE",
                                   "summary": {"markets_requiring_recovery": 0},
                                   "priority_order": []})
        out = tmp_path / "status.txt"
        content = run_daily_status(
            snapshot_path=sp, readiness_path=rdp, trigger_path=tp,
            health_path=hlp, recovery_path=rcp, output_path=out,
            now_utc=_NOW,
        )
        assert "HEALTHY" in content
        assert "READY (100/100)" in content
        assert "fresh=6" in content

    def test_no_extra_files(self, tmp_path):
        out = tmp_path / "status.txt"
        run_daily_status(
            snapshot_path  = tmp_path / "no_snap.json",
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert {f.name for f in tmp_path.iterdir()} == {"status.txt"}

    def test_corrupt_source_loads_error(self, tmp_path):
        bad = tmp_path / "snap.json"
        bad.write_text("{ garbage {{{", encoding="utf-8")
        out = tmp_path / "status.txt"
        run_daily_status(
            snapshot_path  = bad,
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        content = out.read_text(encoding="utf-8")
        assert "ERROR" in content

    def test_missing_source_loads_no_data(self, tmp_path):
        out = tmp_path / "status.txt"
        run_daily_status(
            snapshot_path  = tmp_path / "no_snap.json",
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            recovery_path  = tmp_path / "no_rc.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        content = out.read_text(encoding="utf-8")
        assert "NO DATA" in content


# ---------------------------------------------------------------------------
# 6. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_deterministic_full(self):
        args = (_snapshot(), _readiness(), _trigger(), _health(), _recovery(), _NOW)
        assert build_daily_status(*args) == build_daily_status(*args)

    def test_deterministic_all_missing(self):
        args = (_missing(), _missing(), _missing(), _missing(), _missing(), _NOW)
        assert build_daily_status(*args) == build_daily_status(*args)

    def test_deterministic_all_corrupt(self):
        args = (_corrupt(), _corrupt(), _corrupt(), _corrupt(), _corrupt(), _NOW)
        assert build_daily_status(*args) == build_daily_status(*args)
