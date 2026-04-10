"""
AC-105 tests — Data Freshness / Source Health Review Layer

Coverage:
  - missing sources → no crash, fail-closed (HEALTHY with 0 markets)
  - corrupt sources → no crash
  - all fresh → HEALTHY
  - some stale (< half) → DEGRADED
  - half or more stale → CRITICAL
  - any missing → CRITICAL
  - mixed stale + missing → CRITICAL
  - freshness_blocking_review correct in each case
  - affected_markets correct
  - flags always correct
  - deterministic output
  - output file written
  - no extra files written
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_source_health_review_lite import (
    build_source_health,
    run_health_review,
    FLAGS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _adapter(markets: list[dict]) -> dict:
    return {
        "version":   "marketdata_scenario_adapter_v1",
        "component": "build_marketdata_scenario_adapter_lite",
        "ts_utc":    "2026-04-10T12:00:00Z",
        "markets":   markets,
        "sources":   {"market_data_loaded": True, "intents_data_loaded": True},
        "flags":     {"non_binding": True, "simulation_only": True,
                      "paper_only": True, "live_activation_allowed": False},
    }


def _mkt(name: str, seed_class: str) -> dict:
    return {"market": name, "review_seed_class": seed_class,
            "data_state": "OK", "price_present": True,
            "price_fresh": seed_class in ("ALL_CLEAR", "ZERO_INTENTS"),
            "intents_present": seed_class == "ALL_CLEAR"}


def _fresh(name: str) -> dict:
    return _mkt(name, "ALL_CLEAR")


def _stale(name: str) -> dict:
    return _mkt(name, "DATA_STALE")


def _missing(name: str) -> dict:
    return _mkt(name, "DATA_MISSING")


# ---------------------------------------------------------------------------
# 1. Fail-closed — missing / corrupt sources
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_adapter_no_crash(self):
        result = build_source_health(None, None, NOW)
        assert isinstance(result, dict)

    def test_none_adapter_healthy_zero_markets(self):
        result = build_source_health(None, None, NOW)
        assert result["source_health_status"] == "HEALTHY"
        assert result["markets_total"]        == 0

    def test_corrupt_adapter_no_crash(self):
        result = build_source_health({"not": "expected"}, None, NOW)
        assert isinstance(result, dict)

    def test_empty_markets_list_healthy(self):
        result = build_source_health(_adapter([]), None, NOW)
        assert result["source_health_status"] == "HEALTHY"
        assert result["markets_total"]        == 0

    def test_missing_file_no_crash(self, tmp_path):
        result = run_health_review(
            adapter_path = tmp_path / "nonexistent.json",
            md_path      = tmp_path / "nonexistent.json",
            output_path  = tmp_path / "out.json",
            now_utc      = NOW,
        )
        assert isinstance(result, dict)

    def test_corrupt_file_no_crash(self, tmp_path):
        bad = tmp_path / "adapter.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        result = run_health_review(
            adapter_path = bad,
            md_path      = tmp_path / "x.json",
            output_path  = tmp_path / "out.json",
            now_utc      = NOW,
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Health classification — pure function
# ---------------------------------------------------------------------------

class TestHealthClassification:
    def test_all_fresh_is_healthy(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"), _fresh("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "HEALTHY"

    def test_healthy_no_blocking(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["freshness_blocking_review"] is False

    def test_one_stale_out_of_four_is_degraded(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"),
                _fresh("SOL-EUR"), _stale("XRP-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "DEGRADED"

    def test_degraded_no_blocking(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"),
                _fresh("SOL-EUR"), _stale("XRP-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["freshness_blocking_review"] is False

    def test_two_stale_out_of_four_is_critical(self):
        """Exactly half stale → CRITICAL (stale * 2 >= total)."""
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"),
                _stale("SOL-EUR"), _stale("XRP-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"

    def test_majority_stale_is_critical(self):
        mkts = [_fresh("BTC-EUR"),
                _stale("ETH-EUR"), _stale("SOL-EUR"), _stale("XRP-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"

    def test_all_stale_is_critical(self):
        mkts = [_stale("BTC-EUR"), _stale("ETH-EUR"), _stale("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"

    def test_all_stale_blocking_true(self):
        mkts = [_stale("BTC-EUR"), _stale("ETH-EUR"), _stale("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["freshness_blocking_review"] is True

    def test_any_missing_is_critical(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"), _missing("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"

    def test_missing_blocking_true(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"), _missing("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["freshness_blocking_review"] is True

    def test_mixed_stale_and_missing_is_critical(self):
        mkts = [_fresh("BTC-EUR"), _stale("ETH-EUR"), _missing("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"

    def test_single_stale_out_of_six_is_degraded(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"), _fresh("SOL-EUR"),
                _fresh("ADA-EUR"), _fresh("BNB-EUR"), _stale("XRP-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "DEGRADED"

    def test_six_stale_of_six_is_critical(self):
        """Live scenario: all 6 markets DATA_STALE."""
        mkts = [_stale(f"MKT{i}-EUR") for i in range(6)]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["source_health_status"] == "CRITICAL"
        assert result["freshness_blocking_review"] is True


# ---------------------------------------------------------------------------
# 3. Counts and affected_markets
# ---------------------------------------------------------------------------

class TestCounts:
    def test_fresh_stale_missing_counts(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR"),
                _stale("SOL-EUR"), _missing("ADA-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["markets_total"]   == 4
        assert result["markets_fresh"]   == 2
        assert result["markets_stale"]   == 1
        assert result["markets_missing"] == 1

    def test_affected_markets_contains_stale(self):
        mkts = [_fresh("BTC-EUR"), _stale("ETH-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert "ETH-EUR" in result["affected_markets"]
        assert "BTC-EUR" not in result["affected_markets"]

    def test_affected_markets_contains_missing(self):
        mkts = [_fresh("BTC-EUR"), _missing("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert "SOL-EUR" in result["affected_markets"]

    def test_affected_markets_sorted(self):
        mkts = [_stale("XRP-EUR"), _stale("ADA-EUR"), _stale("BTC-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["affected_markets"] == sorted(result["affected_markets"])

    def test_affected_markets_empty_when_healthy(self):
        mkts = [_fresh("BTC-EUR"), _fresh("ETH-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["affected_markets"] == []

    def test_zero_intents_counts_as_fresh(self):
        mkts = [_mkt("BTC-EUR", "ZERO_INTENTS")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["markets_fresh"] == 1
        assert result["markets_stale"] == 0

    def test_hold_review_counts_as_fresh(self):
        mkts = [_mkt("BTC-EUR", "HOLD_REVIEW")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["markets_fresh"] == 1


# ---------------------------------------------------------------------------
# 4. Reason codes
# ---------------------------------------------------------------------------

class TestReasonCodes:
    def test_healthy_reason_code(self):
        result = build_source_health(_adapter([_fresh("BTC-EUR")]), None, NOW)
        assert result["primary_reason_code"] == "ALL_SOURCES_FRESH"

    def test_all_stale_reason_code(self):
        mkts = [_stale("BTC-EUR"), _stale("ETH-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["primary_reason_code"] == "ALL_SOURCES_STALE"

    def test_majority_stale_reason_code(self):
        mkts = [_fresh("BTC-EUR"), _stale("ETH-EUR"), _stale("SOL-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["primary_reason_code"] == "MAJORITY_SOURCES_STALE"

    def test_missing_reason_code(self):
        mkts = [_fresh("BTC-EUR"), _missing("ETH-EUR")]
        result = build_source_health(_adapter(mkts), None, NOW)
        assert result["primary_reason_code"] in (
            "SOURCES_MISSING", "SOURCES_MISSING_AND_STALE"
        )


# ---------------------------------------------------------------------------
# 5. Flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_flags_correct_healthy(self):
        result = build_source_health(_adapter([_fresh("BTC-EUR")]), None, NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_flags_correct_critical(self):
        result = build_source_health(_adapter([_stale("BTC-EUR")]), None, NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_flags_correct_none_sources(self):
        result = build_source_health(None, None, NOW)
        f = result["flags"]
        assert f["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 6. Output fields
# ---------------------------------------------------------------------------

class TestOutputFields:
    def test_version_and_component(self):
        result = build_source_health(None, None, NOW)
        assert result["version"]   == "source_health_review_v1"
        assert result["component"] == "build_source_health_review_lite"

    def test_ts_utc_present(self):
        result = build_source_health(None, None, NOW)
        assert result["ts_utc"] == "2026-04-10T12:00:00Z"

    def test_deterministic_same_input(self):
        mkts = [_stale("BTC-EUR"), _fresh("ETH-EUR")]
        r1 = build_source_health(_adapter(mkts), None, NOW)
        r2 = build_source_health(_adapter(mkts), None, NOW)
        assert r1 == r2

    def test_sources_loaded_flags(self):
        mkts = [_fresh("BTC-EUR")]
        md   = {"component": "worker_market_data_refresh_lite",
                "ts_utc": "2026-04-07T09:46:20Z", "markets": {}}
        result = build_source_health(_adapter(mkts), md, NOW)
        assert result["sources"]["adapter_loaded"] is True
        assert result["sources"]["md_loaded"]      is True
        assert result["sources"]["md_refresh_ts"]  == "2026-04-07T09:46:20Z"

    def test_sources_flags_when_none(self):
        result = build_source_health(None, None, NOW)
        assert result["sources"]["adapter_loaded"] is False
        assert result["sources"]["md_loaded"]      is False


# ---------------------------------------------------------------------------
# 7. I/O — run_health_review
# ---------------------------------------------------------------------------

class TestRunHealthReview:
    def test_output_file_written(self, tmp_path):
        out = tmp_path / "health.json"
        run_health_review(
            adapter_path = tmp_path / "x.json",
            md_path      = tmp_path / "x.json",
            output_path  = out,
            now_utc      = NOW,
        )
        assert out.exists()

    def test_output_valid_json(self, tmp_path):
        out = tmp_path / "health.json"
        run_health_review(
            adapter_path = tmp_path / "x.json",
            md_path      = tmp_path / "x.json",
            output_path  = out,
            now_utc      = NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == "source_health_review_v1"

    def test_real_adapter_written_and_parsed(self, tmp_path):
        # 1 stale out of 4 (25% < 50%) → DEGRADED
        mkts = [_stale("BTC-EUR"),
                _fresh("ETH-EUR"), _fresh("SOL-EUR"), _fresh("ADA-EUR")]
        ap = tmp_path / "adapter.json"
        ap.write_text(json.dumps(_adapter(mkts)), encoding="utf-8")
        out = tmp_path / "health.json"
        result = run_health_review(
            adapter_path = ap,
            md_path      = tmp_path / "x.json",
            output_path  = out,
            now_utc      = NOW,
        )
        assert result["source_health_status"] == "DEGRADED"
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["source_health_status"] == "DEGRADED"

    def test_no_extra_files_written(self, tmp_path):
        out = tmp_path / "health.json"
        before = set(tmp_path.iterdir())
        run_health_review(
            adapter_path = tmp_path / "x.json",
            md_path      = tmp_path / "x.json",
            output_path  = out,
            now_utc      = NOW,
        )
        after   = set(tmp_path.iterdir())
        new_files = after - before
        assert new_files == {out}
