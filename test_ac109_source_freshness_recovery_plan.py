"""
AC-109 tests — Source Freshness Recovery Plan

Coverage:
  - missing sources → no crash, NONE status
  - corrupt sources → no crash
  - no stale/missing → recovery_status=NONE
  - stale only → PLAN_READY
  - missing only → URGENT
  - mixed stale + missing → URGENT
  - source_health CRITICAL (no adapter data) → URGENT
  - priority order: missing before stale
  - within same class: alphabetical by market
  - recovery_class and priority fields correct
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

from build_source_freshness_recovery_plan_lite import (
    build_recovery_plan,
    run_recovery_plan,
    FLAGS,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _health(status: str = "HEALTHY") -> dict:
    return {
        "version":                   "source_health_review_v1",
        "source_health_status":      status,
        "freshness_blocking_review": status == "CRITICAL",
        "primary_reason_code":       "TEST",
        "markets_total":             3,
        "flags": dict(FLAGS),
    }


def _adapter(markets: list[dict]) -> dict:
    return {
        "version":   "marketdata_scenario_adapter_v1",
        "component": "build_marketdata_scenario_adapter_lite",
        "ts_utc":    "2026-04-10T12:00:00Z",
        "markets":   markets,
        "flags":     dict(FLAGS),
    }


def _mkt(name: str, seed_class: str) -> dict:
    return {"market": name, "review_seed_class": seed_class,
            "data_state": "STALE", "price_present": True}


def _fresh(name: str) -> dict:  return _mkt(name, "ALL_CLEAR")
def _stale(name: str) -> dict:  return _mkt(name, "DATA_STALE")
def _missing(name: str) -> dict: return _mkt(name, "DATA_MISSING")


# ---------------------------------------------------------------------------
# 1. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_sources_no_crash(self):
        result = build_recovery_plan(None, None, NOW)
        assert isinstance(result, dict)

    def test_none_sources_gives_none_status(self):
        result = build_recovery_plan(None, None, NOW)
        assert result["recovery_status"] == "NONE"

    def test_corrupt_health_no_crash(self):
        result = build_recovery_plan({"broken": True}, None, NOW)
        assert isinstance(result, dict)

    def test_corrupt_adapter_no_crash(self):
        result = build_recovery_plan(None, {"broken": True}, NOW)
        assert isinstance(result, dict)

    def test_empty_markets_list_none_status(self):
        result = build_recovery_plan(None, _adapter([]), NOW)
        assert result["recovery_status"] == "NONE"

    def test_missing_files_no_crash(self, tmp_path):
        result = run_recovery_plan(
            health_path  = tmp_path / "x.json",
            adapter_path = tmp_path / "y.json",
            output_path  = tmp_path / "out.json",
            now_utc      = NOW,
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 2. Recovery status
# ---------------------------------------------------------------------------

class TestRecoveryStatus:
    def test_all_fresh_gives_none(self):
        result = build_recovery_plan(
            _health("HEALTHY"),
            _adapter([_fresh("BTC-EUR"), _fresh("ETH-EUR")]),
            NOW,
        )
        assert result["recovery_status"] == "NONE"

    def test_stale_only_gives_plan_ready(self):
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_stale("BTC-EUR"), _fresh("ETH-EUR")]),
            NOW,
        )
        assert result["recovery_status"] == "PLAN_READY"

    def test_all_stale_gives_plan_ready(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_stale("BTC-EUR"), _stale("ETH-EUR"), _stale("SOL-EUR")]),
            NOW,
        )
        # Has stale but no missing — URGENT because source_health is CRITICAL
        assert result["recovery_status"] == "URGENT"

    def test_missing_only_gives_urgent(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("BTC-EUR"), _fresh("ETH-EUR")]),
            NOW,
        )
        assert result["recovery_status"] == "URGENT"

    def test_mixed_stale_and_missing_gives_urgent(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("BTC-EUR"), _stale("ETH-EUR")]),
            NOW,
        )
        assert result["recovery_status"] == "URGENT"

    def test_source_health_critical_no_adapter_gives_urgent(self):
        """CRITICAL source health without adapter data → URGENT."""
        result = build_recovery_plan(_health("CRITICAL"), None, NOW)
        assert result["recovery_status"] == "URGENT"

    def test_source_health_degraded_no_stale_gives_none(self):
        """DEGRADED health but adapter shows all fresh → NONE (adapter wins)."""
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_fresh("BTC-EUR"), _fresh("ETH-EUR")]),
            NOW,
        )
        assert result["recovery_status"] == "NONE"


# ---------------------------------------------------------------------------
# 3. Recovery reason code
# ---------------------------------------------------------------------------

class TestReasonCode:
    def test_none_reason_code(self):
        result = build_recovery_plan(None, _adapter([_fresh("BTC-EUR")]), NOW)
        assert result["recovery_reason_code"] == "ALL_SOURCES_FRESH"

    def test_stale_reason_code(self):
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_stale("BTC-EUR")]),
            NOW,
        )
        assert result["recovery_reason_code"] == "SOURCES_STALE"

    def test_missing_reason_code(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("BTC-EUR")]),
            NOW,
        )
        assert result["recovery_reason_code"] == "SOURCES_MISSING"

    def test_critical_health_no_adapter_reason_code(self):
        result = build_recovery_plan(_health("CRITICAL"), None, NOW)
        assert result["recovery_reason_code"] == "SOURCE_HEALTH_CRITICAL"


# ---------------------------------------------------------------------------
# 4. Priority order
# ---------------------------------------------------------------------------

class TestPriorityOrder:
    def test_missing_before_stale(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_stale("ADA-EUR"), _missing("BTC-EUR")]),
            NOW,
        )
        po = result["priority_order"]
        assert len(po) == 2
        assert po[0]["market"] == "BTC-EUR"
        assert po[0]["recovery_class"] == "RESTORE_MISSING"
        assert po[1]["market"] == "ADA-EUR"
        assert po[1]["recovery_class"] == "REFRESH_STALE"

    def test_multiple_missing_alphabetical(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("XRP-EUR"), _missing("ADA-EUR"), _missing("BTC-EUR")]),
            NOW,
        )
        markets = [e["market"] for e in result["priority_order"]]
        assert markets == ["ADA-EUR", "BTC-EUR", "XRP-EUR"]

    def test_multiple_stale_alphabetical(self):
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_stale("XRP-EUR"), _stale("ADA-EUR"), _stale("BTC-EUR")]),
            NOW,
        )
        markets = [e["market"] for e in result["priority_order"]]
        assert markets == ["ADA-EUR", "BTC-EUR", "XRP-EUR"]

    def test_missing_high_priority(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("BTC-EUR")]),
            NOW,
        )
        assert result["priority_order"][0]["priority"] == "HIGH"

    def test_stale_medium_priority(self):
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_stale("BTC-EUR")]),
            NOW,
        )
        assert result["priority_order"][0]["priority"] == "MEDIUM"

    def test_restore_missing_reason_code(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_missing("BTC-EUR")]),
            NOW,
        )
        assert result["priority_order"][0]["reason_code"] == "DATA_MISSING"

    def test_refresh_stale_reason_code(self):
        result = build_recovery_plan(
            _health("DEGRADED"),
            _adapter([_stale("BTC-EUR")]),
            NOW,
        )
        assert result["priority_order"][0]["reason_code"] == "DATA_STALE"

    def test_fresh_markets_not_in_priority_order(self):
        result = build_recovery_plan(
            _health("HEALTHY"),
            _adapter([_fresh("BTC-EUR"), _fresh("ETH-EUR")]),
            NOW,
        )
        assert result["priority_order"] == []

    def test_mixed_order_missing_stale_fresh(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([
                _fresh("ADA-EUR"), _stale("BTC-EUR"),
                _missing("ETH-EUR"), _stale("SOL-EUR"),
            ]),
            NOW,
        )
        po = result["priority_order"]
        classes = [e["recovery_class"] for e in po]
        # All RESTORE_MISSING before REFRESH_STALE
        restore_indices = [i for i, c in enumerate(classes) if c == "RESTORE_MISSING"]
        refresh_indices = [i for i, c in enumerate(classes) if c == "REFRESH_STALE"]
        assert all(r < s for r in restore_indices for s in refresh_indices)
        # Fresh market not present
        markets = [e["market"] for e in po]
        assert "ADA-EUR" not in markets


# ---------------------------------------------------------------------------
# 5. Summary counts
# ---------------------------------------------------------------------------

class TestSummaryCounts:
    def test_counts_correct(self):
        result = build_recovery_plan(
            _health("CRITICAL"),
            _adapter([_stale("BTC-EUR"), _missing("ETH-EUR"), _fresh("SOL-EUR")]),
            NOW,
        )
        sm = result["summary"]
        assert sm["markets_stale"]   == 1
        assert sm["markets_missing"] == 1
        assert sm["markets_total"]   == 2
        assert sm["markets_requiring_recovery"] == 2

    def test_zero_counts_when_none(self):
        result = build_recovery_plan(None, None, NOW)
        sm = result["summary"]
        assert sm["markets_total"]   == 0
        assert sm["markets_stale"]   == 0
        assert sm["markets_missing"] == 0


# ---------------------------------------------------------------------------
# 6. Flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_flags_correct(self):
        result = build_recovery_plan(None, None, NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_flags_correct_with_data(self):
        result = build_recovery_plan(_health(), _adapter([_stale("BTC-EUR")]), NOW)
        assert result["flags"]["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 7. Output fields and determinism
# ---------------------------------------------------------------------------

class TestOutputFields:
    def test_version_and_component(self):
        result = build_recovery_plan(None, None, NOW)
        assert result["version"]   == "source_freshness_recovery_plan_v1"
        assert result["component"] == "build_source_freshness_recovery_plan_lite"

    def test_ts_utc_present(self):
        result = build_recovery_plan(None, None, NOW)
        assert result["ts_utc"] == "2026-04-10T12:00:00Z"

    def test_deterministic(self):
        h = _health("CRITICAL")
        a = _adapter([_stale("BTC-EUR"), _missing("ETH-EUR")])
        r1 = build_recovery_plan(h, a, NOW)
        r2 = build_recovery_plan(h, a, NOW)
        assert r1 == r2

    def test_sources_loaded_flags(self):
        result = build_recovery_plan(_health(), _adapter([]), NOW)
        assert result["sources"]["health_loaded"]  is True
        assert result["sources"]["adapter_loaded"] is True

    def test_sources_flags_when_none(self):
        result = build_recovery_plan(None, None, NOW)
        assert result["sources"]["health_loaded"]  is False
        assert result["sources"]["adapter_loaded"] is False


# ---------------------------------------------------------------------------
# 8. I/O — run_recovery_plan
# ---------------------------------------------------------------------------

class TestRunRecoveryPlan:
    def test_output_file_written(self, tmp_path):
        out = tmp_path / "plan.json"
        run_recovery_plan(health_path=tmp_path / "x.json",
                          adapter_path=tmp_path / "y.json",
                          output_path=out, now_utc=NOW)
        assert out.exists()

    def test_output_valid_json(self, tmp_path):
        out = tmp_path / "plan.json"
        run_recovery_plan(health_path=tmp_path / "x.json",
                          adapter_path=tmp_path / "y.json",
                          output_path=out, now_utc=NOW)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == "source_freshness_recovery_plan_v1"

    def test_real_files_produce_expected_result(self, tmp_path):
        hp = tmp_path / "health.json"
        ap = tmp_path / "adapter.json"
        hp.write_text(json.dumps(_health("CRITICAL")), encoding="utf-8")
        ap.write_text(json.dumps(_adapter([
            _missing("BTC-EUR"), _stale("ETH-EUR"), _fresh("SOL-EUR")
        ])), encoding="utf-8")
        out = tmp_path / "plan.json"
        result = run_recovery_plan(health_path=hp, adapter_path=ap,
                                   output_path=out, now_utc=NOW)
        assert result["recovery_status"] == "URGENT"
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["recovery_status"] == "URGENT"
        assert data["priority_order"][0]["market"] == "BTC-EUR"
        assert data["priority_order"][1]["market"] == "ETH-EUR"

    def test_no_extra_files_written(self, tmp_path):
        out = tmp_path / "plan.json"
        before = set(tmp_path.iterdir())
        run_recovery_plan(health_path=tmp_path / "x.json",
                          adapter_path=tmp_path / "y.json",
                          output_path=out, now_utc=NOW)
        after = set(tmp_path.iterdir())
        assert after - before == {out}
