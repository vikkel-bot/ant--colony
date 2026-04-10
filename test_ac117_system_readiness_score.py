"""
AC-117 tests — System Readiness Score (No Execution)

Coverage:
  - missing inputs → no crash, score=0
  - CRITICAL path → NOT_READY + blocking=True
  - HEALTHY + NONE → score=100, READY, blocking=False
  - mixed inputs → correct computed score
  - boundaries: score<40 → NOT_READY, 40≤score<70 → LIMITED, ≥70 → READY
  - reason_code priority
  - blocking: only CRITICAL health or URGENT recovery
  - components populated
  - flags correct
  - output file written + valid JSON
  - deterministic
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_system_readiness_score_lite import build_readiness_score, run_readiness_score

_NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Minimal source-dict helpers
# ---------------------------------------------------------------------------

def _health(status: str = "HEALTHY") -> dict:
    return {"version": "source_health_review_v1",
            "source_health_status": status}


def _recovery(status: str = "NONE") -> dict:
    return {"version": "source_freshness_recovery_plan_v1",
            "recovery_status": status}


def _trigger(status: str = "NONE") -> dict:
    return {"version": "refresh_trigger_v1",
            "trigger_status": status}


def _snapshot(alignment: str = "HIGH") -> dict:
    return {"version": "combined_review_snapshot_v1",
            "review_health": {"alignment": alignment}}


def _score(snapshot=None, health=None, recovery=None, trigger=None):
    return build_readiness_score(snapshot, health, recovery, trigger, _NOW)


# ---------------------------------------------------------------------------
# 1. Fail-closed / missing inputs
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_all_none_no_crash(self):
        _score()

    def test_all_none_returns_dict(self):
        assert isinstance(_score(), dict)

    def test_all_none_score_zero(self):
        # All None → worst-case for each: CRITICAL(-50)+URGENT(-30)+URGENT(-30)+LOW(-25)=135 → 0
        assert _score()["readiness_score"] == 0

    def test_all_none_not_ready(self):
        assert _score()["readiness_status"] == "NOT_READY"

    def test_all_none_blocking_false(self):
        # Blocking only triggers on actual (non-None) CRITICAL or URGENT values
        assert _score()["blocking"] is False

    def test_all_none_reason_no_source_data(self):
        assert _score()["reason_code"] == "NO_SOURCE_DATA"

    def test_corrupt_health_no_crash(self):
        _score(health={"broken": True})

    def test_corrupt_recovery_no_crash(self):
        _score(recovery={"broken": True})

    def test_corrupt_snapshot_no_crash(self):
        _score(snapshot={"broken": True})

    def test_corrupt_trigger_no_crash(self):
        _score(trigger={"broken": True})

    def test_partial_none_no_crash(self):
        _score(health=_health("HEALTHY"), recovery=None, trigger=None, snapshot=None)


# ---------------------------------------------------------------------------
# 2. CRITICAL path
# ---------------------------------------------------------------------------

class TestCriticalPath:
    def _critical(self):
        return _score(
            snapshot = _snapshot("LOW"),
            health   = _health("CRITICAL"),
            recovery = _recovery("URGENT"),
            trigger  = _trigger("URGENT"),
        )

    def test_critical_is_not_ready(self):
        assert self._critical()["readiness_status"] == "NOT_READY"

    def test_critical_blocking_true(self):
        assert self._critical()["blocking"] is True

    def test_critical_source_health_reason(self):
        # SOURCE_HEALTH_CRITICAL has highest priority
        assert self._critical()["reason_code"] == "SOURCE_HEALTH_CRITICAL"

    def test_critical_score_low(self):
        # CRITICAL(-50)+URGENT(-30)+URGENT(-30)+LOW(-25) = 135 → 0
        assert self._critical()["readiness_score"] == 0

    def test_recovery_urgent_alone_blocks(self):
        result = _score(
            health   = _health("HEALTHY"),
            recovery = _recovery("URGENT"),
            trigger  = _trigger("NONE"),
            snapshot = _snapshot("HIGH"),
        )
        assert result["blocking"] is True

    def test_critical_health_alone_blocks(self):
        result = _score(
            health   = _health("CRITICAL"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
            snapshot = _snapshot("HIGH"),
        )
        assert result["blocking"] is True


# ---------------------------------------------------------------------------
# 3. HEALTHY / clean path
# ---------------------------------------------------------------------------

class TestHealthyPath:
    def _healthy(self):
        return _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("HEALTHY"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
        )

    def test_healthy_score_100(self):
        assert self._healthy()["readiness_score"] == 100

    def test_healthy_status_ready(self):
        assert self._healthy()["readiness_status"] == "READY"

    def test_healthy_blocking_false(self):
        assert self._healthy()["blocking"] is False

    def test_healthy_reason_system_ready(self):
        assert self._healthy()["reason_code"] == "SYSTEM_READY"


# ---------------------------------------------------------------------------
# 4. Score computation — individual deductions
# ---------------------------------------------------------------------------

class TestScoreDeductions:
    def _only(self, health="HEALTHY", recovery="NONE", trigger="NONE", alignment="HIGH"):
        return _score(
            snapshot = _snapshot(alignment),
            health   = _health(health),
            recovery = _recovery(recovery),
            trigger  = _trigger(trigger),
        )["readiness_score"]

    def test_critical_deduction_50(self):
        # HEALTHY base except CRITICAL health: 100-50=50
        assert self._only(health="CRITICAL") == 50

    def test_degraded_deduction_20(self):
        assert self._only(health="DEGRADED") == 80

    def test_recovery_urgent_deduction_30(self):
        assert self._only(recovery="URGENT") == 70

    def test_recovery_plan_ready_deduction_10(self):
        assert self._only(recovery="PLAN_READY") == 90

    def test_trigger_urgent_deduction_30(self):
        assert self._only(trigger="URGENT") == 70

    def test_trigger_due_deduction_15(self):
        assert self._only(trigger="DUE") == 85

    def test_alignment_low_deduction_25(self):
        assert self._only(alignment="LOW") == 75

    def test_alignment_medium_deduction_10(self):
        assert self._only(alignment="MEDIUM") == 90

    def test_combined_deductions(self):
        # DEGRADED(-20) + PLAN_READY(-10) + DUE(-15) + MEDIUM(-10) = 55 → 45
        assert self._only(health="DEGRADED", recovery="PLAN_READY",
                          trigger="DUE", alignment="MEDIUM") == 45

    def test_clamp_min_zero(self):
        # Max possible deductions: CRITICAL(-50)+URGENT(-30)+URGENT(-30)+LOW(-25)=135 → 0
        result = _score(
            snapshot = _snapshot("LOW"),
            health   = _health("CRITICAL"),
            recovery = _recovery("URGENT"),
            trigger  = _trigger("URGENT"),
        )
        assert result["readiness_score"] == 0


# ---------------------------------------------------------------------------
# 5. Status boundaries
# ---------------------------------------------------------------------------

class TestStatusBoundaries:
    def test_score_40_is_limited(self):
        # CRITICAL(-50) + PLAN_READY(-10) = 60 → score=40 → LIMITED
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("CRITICAL"),
            recovery = _recovery("PLAN_READY"),
            trigger  = _trigger("NONE"),
        )
        assert result["readiness_score"] == 40
        assert result["readiness_status"] == "LIMITED"

    def test_score_35_is_not_ready(self):
        # CRITICAL(-50) + DUE(-15) = 65 → score=35 → NOT_READY
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("CRITICAL"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("DUE"),
        )
        assert result["readiness_score"] == 35
        assert result["readiness_status"] == "NOT_READY"

    def test_score_70_is_ready(self):
        # DEGRADED(-20) + PLAN_READY(-10) = 30 → score=70 → READY
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("DEGRADED"),
            recovery = _recovery("PLAN_READY"),
            trigger  = _trigger("NONE"),
        )
        assert result["readiness_score"] == 70
        assert result["readiness_status"] == "READY"

    def test_score_69_is_limited(self):
        # DEGRADED(-20) + PLAN_READY(-10) + MEDIUM(-10) = 40 → score=60 → LIMITED
        result = _score(
            snapshot = _snapshot("MEDIUM"),
            health   = _health("DEGRADED"),
            recovery = _recovery("PLAN_READY"),
            trigger  = _trigger("NONE"),
        )
        assert result["readiness_score"] == 60
        assert result["readiness_status"] == "LIMITED"

    def test_score_100_is_ready(self):
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("HEALTHY"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
        )
        assert result["readiness_score"] == 100
        assert result["readiness_status"] == "READY"


# ---------------------------------------------------------------------------
# 6. Reason code priority
# ---------------------------------------------------------------------------

class TestReasonCode:
    def test_no_source_data(self):
        assert _score()["reason_code"] == "NO_SOURCE_DATA"

    def test_source_critical_beats_others(self):
        result = _score(
            snapshot = _snapshot("LOW"),
            health   = _health("CRITICAL"),
            recovery = _recovery("URGENT"),
            trigger  = _trigger("URGENT"),
        )
        assert result["reason_code"] == "SOURCE_HEALTH_CRITICAL"

    def test_recovery_urgent(self):
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("HEALTHY"),
            recovery = _recovery("URGENT"),
            trigger  = _trigger("NONE"),
        )
        assert result["reason_code"] == "RECOVERY_URGENT"

    def test_trigger_urgent(self):
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("HEALTHY"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("URGENT"),
        )
        assert result["reason_code"] == "TRIGGER_URGENT"

    def test_alignment_low(self):
        result = _score(
            snapshot = _snapshot("LOW"),
            health   = _health("HEALTHY"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
        )
        assert result["reason_code"] == "REVIEW_ALIGNMENT_LOW"

    def test_system_ready(self):
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("HEALTHY"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
        )
        assert result["reason_code"] == "SYSTEM_READY"

    def test_source_degraded(self):
        result = _score(
            snapshot = _snapshot("HIGH"),
            health   = _health("DEGRADED"),
            recovery = _recovery("NONE"),
            trigger  = _trigger("NONE"),
        )
        assert result["reason_code"] == "SOURCE_HEALTH_DEGRADED"


# ---------------------------------------------------------------------------
# 7. Blocking edge cases
# ---------------------------------------------------------------------------

class TestBlocking:
    def test_degraded_not_blocking(self):
        result = _score(health=_health("DEGRADED"), recovery=_recovery("NONE"),
                        trigger=_trigger("NONE"), snapshot=_snapshot("HIGH"))
        assert result["blocking"] is False

    def test_plan_ready_not_blocking(self):
        result = _score(health=_health("HEALTHY"), recovery=_recovery("PLAN_READY"),
                        trigger=_trigger("NONE"), snapshot=_snapshot("HIGH"))
        assert result["blocking"] is False

    def test_trigger_urgent_not_blocking(self):
        result = _score(health=_health("HEALTHY"), recovery=_recovery("NONE"),
                        trigger=_trigger("URGENT"), snapshot=_snapshot("HIGH"))
        assert result["blocking"] is False

    def test_all_none_not_blocking(self):
        assert _score()["blocking"] is False


# ---------------------------------------------------------------------------
# 8. Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:
    def _result(self):
        return _score(snapshot=_snapshot("HIGH"), health=_health("DEGRADED"),
                      recovery=_recovery("PLAN_READY"), trigger=_trigger("NONE"))

    def test_version(self):
        assert self._result()["version"] == "system_readiness_score_v1"

    def test_component(self):
        assert self._result()["component"] == "build_system_readiness_score_lite"

    def test_ts_utc(self):
        assert self._result()["ts_utc"] == "2026-04-10T12:00:00Z"

    def test_components_keys(self):
        comps = self._result()["components"]
        assert "source_health"    in comps
        assert "review_alignment" in comps
        assert "recovery_status"  in comps
        assert "trigger_status"   in comps

    def test_components_values(self):
        comps = self._result()["components"]
        assert comps["source_health"]    == "DEGRADED"
        assert comps["review_alignment"] == "HIGH"
        assert comps["recovery_status"]  == "PLAN_READY"
        assert comps["trigger_status"]   == "NONE"

    def test_components_unknown_on_none(self):
        comps = _score()["components"]
        assert comps["source_health"]    == "UNKNOWN"
        assert comps["review_alignment"] == "UNKNOWN"

    def test_flags(self):
        flags = self._result()["flags"]
        assert flags["non_execution"] is True
        assert flags["read_only"]     is True

    def test_blocking_is_bool(self):
        assert isinstance(self._result()["blocking"], bool)

    def test_score_is_int(self):
        assert isinstance(self._result()["readiness_score"], int)


# ---------------------------------------------------------------------------
# 9. I/O — run_readiness_score
# ---------------------------------------------------------------------------

class TestRunReadinessScore:
    def test_missing_sources_no_crash(self, tmp_path):
        run_readiness_score(
            snapshot_path = tmp_path / "no_snap.json",
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_rec.json",
            trigger_path  = tmp_path / "no_trig.json",
            output_path   = tmp_path / "out.json",
            now_utc       = _NOW,
        )

    def test_output_written(self, tmp_path):
        out = tmp_path / "out.json"
        run_readiness_score(
            snapshot_path = tmp_path / "no_snap.json",
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_rec.json",
            trigger_path  = tmp_path / "no_trig.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        assert out.exists()

    def test_output_valid_json(self, tmp_path):
        out = tmp_path / "out.json"
        run_readiness_score(
            snapshot_path = tmp_path / "no_snap.json",
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_rec.json",
            trigger_path  = tmp_path / "no_trig.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert "readiness_score" in data

    def test_output_with_real_data(self, tmp_path):
        hp  = tmp_path / "health.json"
        rp  = tmp_path / "recovery.json"
        tp  = tmp_path / "trigger.json"
        sp  = tmp_path / "snapshot.json"
        out = tmp_path / "out.json"
        hp.write_text(json.dumps(_health("HEALTHY")),     encoding="utf-8")
        rp.write_text(json.dumps(_recovery("NONE")),      encoding="utf-8")
        tp.write_text(json.dumps(_trigger("NONE")),       encoding="utf-8")
        sp.write_text(json.dumps(_snapshot("HIGH")),      encoding="utf-8")
        result = run_readiness_score(
            snapshot_path=sp, health_path=hp, recovery_path=rp,
            trigger_path=tp,  output_path=out, now_utc=_NOW,
        )
        assert result["readiness_score"]  == 100
        assert result["readiness_status"] == "READY"

    def test_no_extra_files(self, tmp_path):
        out = tmp_path / "out.json"
        run_readiness_score(
            snapshot_path = tmp_path / "no_snap.json",
            health_path   = tmp_path / "no_health.json",
            recovery_path = tmp_path / "no_rec.json",
            trigger_path  = tmp_path / "no_trig.json",
            output_path   = out,
            now_utc       = _NOW,
        )
        assert {f.name for f in tmp_path.iterdir()} == {"out.json"}


# ---------------------------------------------------------------------------
# 10. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_deterministic_healthy(self):
        h = _health("HEALTHY")
        r = _recovery("NONE")
        t = _trigger("NONE")
        s = _snapshot("HIGH")
        assert (build_readiness_score(s, h, r, t, _NOW) ==
                build_readiness_score(s, h, r, t, _NOW))

    def test_deterministic_critical(self):
        h = _health("CRITICAL")
        r = _recovery("URGENT")
        t = _trigger("URGENT")
        s = _snapshot("LOW")
        assert (build_readiness_score(s, h, r, t, _NOW) ==
                build_readiness_score(s, h, r, t, _NOW))

    def test_deterministic_all_none(self):
        assert (build_readiness_score(None, None, None, None, _NOW) ==
                build_readiness_score(None, None, None, None, _NOW))
