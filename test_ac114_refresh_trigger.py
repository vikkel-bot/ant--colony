"""
AC-114 tests — Semi-Automatic Refresh Trigger (No Execution)

Coverage:
  - missing sources → no crash, fail-closed WATCH
  - corrupt sources → no crash, fail-closed WATCH
  - healthy + none → NONE, refresh_check_required=False
  - degraded only → WATCH
  - plan_ready only → WATCH
  - degraded + plan_ready → DUE
  - critical health → URGENT
  - urgent recovery → URGENT
  - guidance mapping correct for each status
  - flags correct
  - summary fields present
  - output file written
  - deterministic output
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_refresh_trigger_lite import build_trigger, run_trigger

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _health(status: str = "HEALTHY", fresh: int = 2,
            stale: int = 0, missing: int = 0) -> dict:
    return {
        "version":              "source_health_review_v1",
        "source_health_status": status,
        "markets_fresh":        fresh,
        "markets_stale":        stale,
        "markets_missing":      missing,
    }


def _recovery(status: str = "NONE", requiring: int = 0) -> dict:
    return {
        "version":         "source_freshness_recovery_plan_v1",
        "recovery_status": status,
        "summary":         {"markets_requiring_recovery": requiring},
        "priority_order":  [],
    }


# ---------------------------------------------------------------------------
# 1. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_both_none_no_crash(self):
        build_trigger(None, None, _NOW)

    def test_both_none_returns_dict(self):
        result = build_trigger(None, None, _NOW)
        assert isinstance(result, dict)

    def test_both_none_watch_status(self):
        result = build_trigger(None, None, _NOW)
        assert result["trigger_status"] == "WATCH"

    def test_both_none_refresh_required(self):
        result = build_trigger(None, None, _NOW)
        assert result["refresh_check_required"] is True

    def test_corrupt_health_no_crash(self):
        build_trigger({"broken": True}, None, _NOW)

    def test_corrupt_recovery_no_crash(self):
        build_trigger(None, {"broken": True}, _NOW)

    def test_corrupt_both_no_crash(self):
        build_trigger({"x": 1}, {"y": 2}, _NOW)

    def test_no_data_reason_code(self):
        result = build_trigger(None, None, _NOW)
        assert result["trigger_reason_code"] == "NO_SOURCE_DATA"


# ---------------------------------------------------------------------------
# 2. NONE — healthy + none
# ---------------------------------------------------------------------------

class TestNoneStatus:
    def test_healthy_none_gives_none_status(self):
        result = build_trigger(_health("HEALTHY"), _recovery("NONE"), _NOW)
        assert result["trigger_status"] == "NONE"

    def test_healthy_none_not_required(self):
        result = build_trigger(_health("HEALTHY"), _recovery("NONE"), _NOW)
        assert result["refresh_check_required"] is False

    def test_healthy_none_action_none(self):
        result = build_trigger(_health("HEALTHY"), _recovery("NONE"), _NOW)
        assert result["operator_guidance"]["recommended_action"] == "NONE"

    def test_healthy_none_window_none(self):
        result = build_trigger(_health("HEALTHY"), _recovery("NONE"), _NOW)
        assert result["operator_guidance"]["recommended_window"] == "NONE"

    def test_healthy_none_reason_code(self):
        result = build_trigger(_health("HEALTHY"), _recovery("NONE"), _NOW)
        assert result["trigger_reason_code"] == "SOURCE_HEALTHY_RECOVERY_NONE"


# ---------------------------------------------------------------------------
# 3. WATCH — single signal
# ---------------------------------------------------------------------------

class TestWatchStatus:
    def test_degraded_health_gives_watch(self):
        result = build_trigger(_health("DEGRADED", stale=1), _recovery("NONE"), _NOW)
        assert result["trigger_status"] == "WATCH"

    def test_plan_ready_gives_watch(self):
        result = build_trigger(_health("HEALTHY"), _recovery("PLAN_READY", requiring=1), _NOW)
        assert result["trigger_status"] == "WATCH"

    def test_watch_refresh_required(self):
        result = build_trigger(_health("DEGRADED", stale=1), _recovery("NONE"), _NOW)
        assert result["refresh_check_required"] is True

    def test_watch_action_monitor(self):
        result = build_trigger(_health("DEGRADED", stale=1), _recovery("NONE"), _NOW)
        assert result["operator_guidance"]["recommended_action"] == "MONITOR"

    def test_watch_window_later(self):
        result = build_trigger(_health("DEGRADED", stale=1), _recovery("NONE"), _NOW)
        assert result["operator_guidance"]["recommended_window"] == "LATER"

    def test_degraded_reason_code(self):
        result = build_trigger(_health("DEGRADED", stale=1), _recovery("NONE"), _NOW)
        assert result["trigger_reason_code"] == "SOURCE_DEGRADED"

    def test_plan_ready_reason_code(self):
        result = build_trigger(_health("HEALTHY"), _recovery("PLAN_READY", requiring=1), _NOW)
        assert result["trigger_reason_code"] == "RECOVERY_PLAN_READY"


# ---------------------------------------------------------------------------
# 4. DUE — both degraded + plan_ready
# ---------------------------------------------------------------------------

class TestDueStatus:
    def test_degraded_and_plan_ready_gives_due(self):
        result = build_trigger(
            _health("DEGRADED", stale=1),
            _recovery("PLAN_READY", requiring=1),
            _NOW,
        )
        assert result["trigger_status"] == "DUE"

    def test_due_refresh_required(self):
        result = build_trigger(
            _health("DEGRADED", stale=1),
            _recovery("PLAN_READY", requiring=1),
            _NOW,
        )
        assert result["refresh_check_required"] is True

    def test_due_action_monitor(self):
        result = build_trigger(
            _health("DEGRADED", stale=1),
            _recovery("PLAN_READY", requiring=1),
            _NOW,
        )
        assert result["operator_guidance"]["recommended_action"] == "MONITOR"

    def test_due_window_soon(self):
        result = build_trigger(
            _health("DEGRADED", stale=1),
            _recovery("PLAN_READY", requiring=1),
            _NOW,
        )
        assert result["operator_guidance"]["recommended_window"] == "SOON"

    def test_due_reason_code(self):
        result = build_trigger(
            _health("DEGRADED", stale=1),
            _recovery("PLAN_READY", requiring=1),
            _NOW,
        )
        assert result["trigger_reason_code"] == "SOURCE_DEGRADED_AND_RECOVERY_PLAN_READY"


# ---------------------------------------------------------------------------
# 5. URGENT — critical or urgent
# ---------------------------------------------------------------------------

class TestUrgentStatus:
    def test_critical_health_gives_urgent(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("URGENT", requiring=4), _NOW)
        assert result["trigger_status"] == "URGENT"

    def test_urgent_recovery_gives_urgent(self):
        result = build_trigger(_health("HEALTHY"), _recovery("URGENT", requiring=3), _NOW)
        assert result["trigger_status"] == "URGENT"

    def test_critical_health_alone_gives_urgent(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("NONE"), _NOW)
        assert result["trigger_status"] == "URGENT"

    def test_urgent_refresh_required(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("URGENT", requiring=4), _NOW)
        assert result["refresh_check_required"] is True

    def test_urgent_action(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("URGENT", requiring=4), _NOW)
        assert result["operator_guidance"]["recommended_action"] == "RUN_MANUAL_REFRESH_CHECK_NOW"

    def test_urgent_window_now(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("URGENT", requiring=4), _NOW)
        assert result["operator_guidance"]["recommended_window"] == "NOW"

    def test_critical_reason_code(self):
        result = build_trigger(_health("CRITICAL", stale=4), _recovery("NONE"), _NOW)
        assert result["trigger_reason_code"] == "SOURCE_CRITICAL"

    def test_urgent_recovery_reason_code(self):
        result = build_trigger(_health("HEALTHY"), _recovery("URGENT", requiring=3), _NOW)
        assert result["trigger_reason_code"] == "RECOVERY_URGENT"


# ---------------------------------------------------------------------------
# 6. Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def _result(self):
        return build_trigger(_health("DEGRADED", stale=2), _recovery("PLAN_READY", requiring=2), _NOW)

    def test_version_present(self):
        assert self._result()["version"] == "refresh_trigger_v1"

    def test_component_present(self):
        assert self._result()["component"] == "build_refresh_trigger_lite"

    def test_ts_utc_present(self):
        result = self._result()
        assert "ts_utc" in result
        assert result["ts_utc"] == "2026-04-10T12:00:00Z"

    def test_summary_keys(self):
        sm = self._result()["summary"]
        assert "source_health_status" in sm
        assert "recovery_status" in sm
        assert "markets_requiring_recovery" in sm

    def test_summary_market_count(self):
        sm = self._result()["summary"]
        assert sm["markets_requiring_recovery"] == 2

    def test_guidance_keys(self):
        og = self._result()["operator_guidance"]
        assert "recommended_action" in og
        assert "recommended_window" in og

    def test_flags_present(self):
        flags = self._result()["flags"]
        assert flags["non_binding"]             is True
        assert flags["simulation_only"]         is True
        assert flags["paper_only"]              is True
        assert flags["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# 7. I/O — run_trigger
# ---------------------------------------------------------------------------

class TestRunTrigger:
    def test_missing_sources_no_crash(self, tmp_path):
        run_trigger(
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_recovery.json",
            output_path   = tmp_path / "trigger.json",
            now_utc       = _NOW,
        )

    def test_output_file_written(self, tmp_path):
        out = tmp_path / "trigger.json"
        run_trigger(
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_recovery.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        assert out.exists()

    def test_output_file_is_valid_json(self, tmp_path):
        out = tmp_path / "trigger.json"
        run_trigger(
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_recovery.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert "trigger_status" in data

    def test_output_file_with_real_data(self, tmp_path):
        hp = tmp_path / "health.json"
        rp = tmp_path / "recovery.json"
        out = tmp_path / "trigger.json"
        hp.write_text(json.dumps(_health("DEGRADED", stale=1)), encoding="utf-8")
        rp.write_text(json.dumps(_recovery("PLAN_READY", requiring=1)), encoding="utf-8")
        result = run_trigger(
            health_path   = hp,
            recovery_path = rp,
            output_path   = out,
            now_utc       = _NOW,
        )
        assert result["trigger_status"] == "DUE"
        assert out.exists()

    def test_no_extra_files(self, tmp_path):
        out = tmp_path / "trigger.json"
        run_trigger(
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_recovery.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        created = {f.name for f in tmp_path.iterdir()}
        assert created == {"trigger.json"}


# ---------------------------------------------------------------------------
# 8. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_deterministic_none(self):
        h = _health("HEALTHY")
        r = _recovery("NONE")
        assert build_trigger(h, r, _NOW) == build_trigger(h, r, _NOW)

    def test_deterministic_urgent(self):
        h = _health("CRITICAL", stale=4)
        r = _recovery("URGENT", requiring=4)
        assert build_trigger(h, r, _NOW) == build_trigger(h, r, _NOW)
