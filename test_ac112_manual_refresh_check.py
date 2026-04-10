"""
AC-112 tests — Manual Refresh Check Runner

Coverage:
  - runner executes without crash
  - missing source files → handled gracefully, NO DATA shown
  - output contains adapter / health / recovery lines
  - top priorities correctly shown
  - output files written by underlying builders
  - no extra files written beyond expected outputs
  - deterministic with same inputs
"""
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from run_manual_refresh_check_lite import run, _print_summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _captured_summary(result: dict) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        _print_summary(result)
    finally:
        sys.stdout = old
    return buf.getvalue()


def _md_data(markets: dict) -> dict:
    return {
        "component":    "worker_market_data_refresh_lite",
        "ts_utc":       "2026-04-10T11:00:00Z",
        "market_count": len(markets),
        "markets":      markets,
    }


def _price_entry(ts: str, price: float) -> dict:
    return {"last_price": price, "ts_utc": ts, "state": "OK"}


def _intents_data(markets: list) -> dict:
    return {"component": "build_rebalance_intents_lite",
            "ts_utc":    "2026-04-10T11:00:00Z",
            "intents":   [{"market": m} for m in markets]}


# ---------------------------------------------------------------------------
# 1. Runner — basic execution
# ---------------------------------------------------------------------------

class TestRunnerExecution:
    def test_runs_without_crash_missing_files(self, tmp_path):
        result = run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
        )
        assert isinstance(result, dict)

    def test_result_has_expected_keys(self, tmp_path):
        result = run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
        )
        assert "adapter"  in result
        assert "health"   in result
        assert "recovery" in result

    def test_runs_with_real_md_data(self, tmp_path):
        md = tmp_path / "md.json"
        it = tmp_path / "intents.json"
        FRESH = "2026-04-10T11:00:00Z"
        md.write_text(json.dumps(_md_data({
            "BTC-EUR": _price_entry(FRESH, 59823.0),
            "ETH-EUR": _price_entry(FRESH, 1840.0),
        })), encoding="utf-8")
        it.write_text(json.dumps(_intents_data(["BTC-EUR"])), encoding="utf-8")
        result = run(
            md_path       = md,
            intents_path  = it,
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
        )
        assert result["adapter"] is not None
        assert result["health"]  is not None
        assert result["recovery"] is not None

    def test_adapter_output_written(self, tmp_path):
        ap = tmp_path / "adapter.json"
        run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = ap,
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
        )
        assert ap.exists()

    def test_health_output_written(self, tmp_path):
        hp = tmp_path / "health.json"
        run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = tmp_path / "adapter.json",
            health_path   = hp,
            recovery_path = tmp_path / "recovery.json",
        )
        assert hp.exists()

    def test_recovery_output_written(self, tmp_path):
        rp = tmp_path / "recovery.json"
        run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = rp,
        )
        assert rp.exists()

    def test_no_extra_files_beyond_three_outputs(self, tmp_path):
        ap = tmp_path / "adapter.json"
        hp = tmp_path / "health.json"
        rp = tmp_path / "recovery.json"
        run(
            md_path       = tmp_path / "no_md.json",
            intents_path  = tmp_path / "no_intents.json",
            adapter_path  = ap,
            health_path   = hp,
            recovery_path = rp,
        )
        created = {f.name for f in tmp_path.iterdir()}
        expected = {"adapter.json", "health.json", "recovery.json"}
        assert created == expected


# ---------------------------------------------------------------------------
# 2. Pipeline outputs — content checks
# ---------------------------------------------------------------------------

class TestPipelineContent:
    def _run_fresh(self, tmp_path):
        """Run with two fresh markets and one intent."""
        FRESH = "2026-04-10T11:00:00Z"
        md = tmp_path / "md.json"
        it = tmp_path / "intents.json"
        md.write_text(json.dumps(_md_data({
            "BTC-EUR": _price_entry(FRESH, 59823.0),
            "ETH-EUR": _price_entry(FRESH, 1840.0),
        })), encoding="utf-8")
        it.write_text(json.dumps(_intents_data(["BTC-EUR"])), encoding="utf-8")
        return run(
            md_path       = md,
            intents_path  = it,
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
        )

    def test_adapter_has_markets(self, tmp_path):
        result = self._run_fresh(tmp_path)
        assert len(result["adapter"]["markets"]) == 2

    def test_health_has_status(self, tmp_path):
        result = self._run_fresh(tmp_path)
        assert "source_health_status" in result["health"]

    def test_recovery_has_status(self, tmp_path):
        result = self._run_fresh(tmp_path)
        assert "recovery_status" in result["recovery"]

    def test_fresh_markets_give_healthy_source(self, tmp_path):
        import datetime
        FRESH = "2026-04-10T11:00:00Z"
        md = tmp_path / "md.json"
        it = tmp_path / "intents.json"
        md.write_text(json.dumps(_md_data({
            "BTC-EUR": _price_entry(FRESH, 59823.0),
        })), encoding="utf-8")
        it.write_text(json.dumps(_intents_data(["BTC-EUR"])), encoding="utf-8")
        now = datetime.datetime(2026, 4, 10, 12, 0, 0,
                                tzinfo=datetime.timezone.utc)
        result = run(
            md_path       = md,
            intents_path  = it,
            adapter_path  = tmp_path / "adapter.json",
            health_path   = tmp_path / "health.json",
            recovery_path = tmp_path / "recovery.json",
            now_utc       = now,
        )
        assert result["health"]["source_health_status"] == "HEALTHY"
        assert result["recovery"]["recovery_status"] == "NONE"


# ---------------------------------------------------------------------------
# 3. _print_summary output
# ---------------------------------------------------------------------------

class TestPrintSummary:
    def test_header_present(self):
        output = _captured_summary({"adapter": None, "health": None, "recovery": None})
        assert "ANT MANUAL REFRESH CHECK" in output

    def test_adapter_no_data(self):
        output = _captured_summary({"adapter": None, "health": None, "recovery": None})
        assert "adapter" in output.lower()
        assert "NO DATA" in output

    def test_adapter_count_shown(self):
        ar = {"markets": [{"market": "BTC-EUR"}, {"market": "ETH-EUR"}]}
        output = _captured_summary({"adapter": ar, "health": None, "recovery": None})
        assert "2" in output

    def test_health_status_shown(self):
        hr = {"source_health_status": "CRITICAL",
              "markets_fresh": 0, "markets_stale": 6, "markets_missing": 0}
        output = _captured_summary({"adapter": None, "health": hr, "recovery": None})
        assert "CRITICAL" in output
        assert "stale=6" in output

    def test_health_no_data(self):
        output = _captured_summary({"adapter": None, "health": None, "recovery": None})
        assert "health" in output.lower()

    def test_recovery_status_shown(self):
        rr = {"recovery_status": "URGENT",
              "summary": {"markets_requiring_recovery": 6},
              "priority_order": [{"market": "ADA-EUR"}, {"market": "BTC-EUR"}]}
        output = _captured_summary({"adapter": None, "health": None, "recovery": rr})
        assert "URGENT" in output
        assert "requiring=6" in output

    def test_top_markets_shown(self):
        rr = {"recovery_status": "URGENT",
              "summary": {"markets_requiring_recovery": 3},
              "priority_order": [
                  {"market": "ADA-EUR"}, {"market": "BTC-EUR"}, {"market": "ETH-EUR"}
              ]}
        output = _captured_summary({"adapter": None, "health": None, "recovery": rr})
        assert "ADA-EUR" in output or "BTC-EUR" in output

    def test_recovery_no_data(self):
        output = _captured_summary({"adapter": None, "health": None, "recovery": None})
        assert "recovery" in output.lower()

    def test_all_sections_present(self):
        ar = {"markets": [{"market": "BTC-EUR"}]}
        hr = {"source_health_status": "HEALTHY",
              "markets_fresh": 1, "markets_stale": 0, "markets_missing": 0}
        rr = {"recovery_status": "NONE",
              "summary": {"markets_requiring_recovery": 0},
              "priority_order": []}
        output = _captured_summary({"adapter": ar, "health": hr, "recovery": rr})
        assert "adapter"  in output.lower()
        assert "health"   in output.lower()
        assert "recovery" in output.lower()

    def test_deterministic(self):
        ar = {"markets": [{"market": "BTC-EUR"}]}
        hr = {"source_health_status": "CRITICAL",
              "markets_fresh": 0, "markets_stale": 1, "markets_missing": 0}
        rr = {"recovery_status": "URGENT",
              "summary": {"markets_requiring_recovery": 1},
              "priority_order": [{"market": "BTC-EUR"}]}
        result = {"adapter": ar, "health": hr, "recovery": rr}
        assert _captured_summary(result) == _captured_summary(result)
