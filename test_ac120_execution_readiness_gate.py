"""
AC-120 tests — Execution Readiness Gate (Pre-Execution, No Execution)

Coverage:
  - execution_allowed is ALWAYS False
  - gate_status: BLOCKED / LIMITED / OPEN
  - BLOCKED conditions: CRITICAL health / URGENT trigger / NOT_READY readiness
  - LIMITED conditions: LIMITED readiness / DUE trigger / WATCH trigger
  - OPEN: READY + NONE + HEALTHY or DEGRADED
  - fail-closed: all-None → BLOCKED
  - fail-closed: unknown values → BLOCKED
  - reason_code priority chain
  - conditions dict present with all 3 keys
  - flags: paper_only=True, execution_disabled=True
  - output version key
  - ts_utc present
  - I/O: output file written, UTF-8 JSON, no extra files
  - I/O: missing sources → BLOCKED
  - I/O: corrupt sources → BLOCKED
  - deterministic
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_execution_readiness_gate_lite import build_execution_gate, run_execution_gate

_NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)
_TS  = "2026-04-10T12:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _readiness(status: str = "READY", score: int = 100) -> dict:
    return {
        "version":          "system_readiness_score_v1",
        "readiness_status": status,
        "readiness_score":  score,
    }


def _trigger(status: str = "NONE") -> dict:
    return {
        "version":        "refresh_trigger_v1",
        "trigger_status": status,
    }


def _health(status: str = "HEALTHY") -> dict:
    return {
        "version":              "source_health_review_v1",
        "source_health_status": status,
    }


def _gate(rd=None, tr=None, hl=None) -> dict:
    return build_execution_gate(rd, tr, hl, _NOW)


def _open_gate() -> dict:
    return _gate(_readiness("READY"), _trigger("NONE"), _health("HEALTHY"))


# ---------------------------------------------------------------------------
# 1. execution_allowed ALWAYS False
# ---------------------------------------------------------------------------

class TestExecutionAllowed:
    def test_always_false_open(self):
        assert _open_gate()["execution_allowed"] is False

    def test_always_false_blocked(self):
        assert _gate()["execution_allowed"] is False

    def test_always_false_limited(self):
        result = _gate(_readiness("LIMITED"), _trigger("NONE"), _health("HEALTHY"))
        assert result["execution_allowed"] is False

    def test_always_false_all_none(self):
        assert _gate()["execution_allowed"] is False


# ---------------------------------------------------------------------------
# 2. Gate BLOCKED conditions
# ---------------------------------------------------------------------------

class TestBlocked:
    def test_critical_health_blocked(self):
        result = _gate(_readiness("READY"), _trigger("NONE"), _health("CRITICAL"))
        assert result["gate_status"] == "BLOCKED"

    def test_urgent_trigger_blocked(self):
        result = _gate(_readiness("READY"), _trigger("URGENT"), _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_not_ready_blocked(self):
        result = _gate(_readiness("NOT_READY"), _trigger("NONE"), _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_all_worst_case_blocked(self):
        result = _gate(_readiness("NOT_READY"), _trigger("URGENT"), _health("CRITICAL"))
        assert result["gate_status"] == "BLOCKED"

    def test_all_none_blocked(self):
        assert _gate()["gate_status"] == "BLOCKED"

    def test_critical_overrides_ready(self):
        result = _gate(_readiness("READY"), _trigger("NONE"), _health("CRITICAL"))
        assert result["gate_status"] == "BLOCKED"

    def test_urgent_overrides_ready(self):
        result = _gate(_readiness("READY"), _trigger("URGENT"), _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# 3. Gate LIMITED conditions
# ---------------------------------------------------------------------------

class TestLimited:
    def test_limited_readiness_limited(self):
        result = _gate(_readiness("LIMITED"), _trigger("NONE"), _health("HEALTHY"))
        assert result["gate_status"] == "LIMITED"

    def test_due_trigger_limited(self):
        result = _gate(_readiness("READY"), _trigger("DUE"), _health("HEALTHY"))
        assert result["gate_status"] == "LIMITED"

    def test_watch_trigger_limited(self):
        result = _gate(_readiness("READY"), _trigger("WATCH"), _health("HEALTHY"))
        assert result["gate_status"] == "LIMITED"

    def test_limited_readiness_due_trigger_limited(self):
        result = _gate(_readiness("LIMITED"), _trigger("DUE"), _health("HEALTHY"))
        assert result["gate_status"] == "LIMITED"

    def test_degraded_health_watch_trigger_limited(self):
        result = _gate(_readiness("READY"), _trigger("WATCH"), _health("DEGRADED"))
        assert result["gate_status"] == "LIMITED"


# ---------------------------------------------------------------------------
# 4. Gate OPEN conditions
# ---------------------------------------------------------------------------

class TestOpen:
    def test_ready_none_healthy_open(self):
        result = _open_gate()
        assert result["gate_status"] == "OPEN"

    def test_ready_none_degraded_open(self):
        result = _gate(_readiness("READY"), _trigger("NONE"), _health("DEGRADED"))
        assert result["gate_status"] == "OPEN"


# ---------------------------------------------------------------------------
# 5. Reason code priority chain
# ---------------------------------------------------------------------------

class TestReasonCode:
    def test_critical_health_takes_priority(self):
        result = _gate(_readiness("NOT_READY"), _trigger("URGENT"), _health("CRITICAL"))
        assert result["reason_code"] == "SOURCE_HEALTH_CRITICAL"

    def test_urgent_trigger_beats_not_ready(self):
        result = _gate(_readiness("NOT_READY"), _trigger("URGENT"), _health("HEALTHY"))
        assert result["reason_code"] == "TRIGGER_URGENT"

    def test_not_ready_reason(self):
        result = _gate(_readiness("NOT_READY"), _trigger("NONE"), _health("HEALTHY"))
        assert result["reason_code"] == "NOT_READY"

    def test_limited_state_reason(self):
        result = _gate(_readiness("LIMITED"), _trigger("NONE"), _health("HEALTHY"))
        assert result["reason_code"] == "LIMITED_STATE"

    def test_due_trigger_limited_state_reason(self):
        result = _gate(_readiness("READY"), _trigger("DUE"), _health("HEALTHY"))
        assert result["reason_code"] == "LIMITED_STATE"

    def test_watch_trigger_limited_state_reason(self):
        result = _gate(_readiness("READY"), _trigger("WATCH"), _health("HEALTHY"))
        assert result["reason_code"] == "LIMITED_STATE"

    def test_open_gate_ready_but_locked(self):
        result = _open_gate()
        assert result["reason_code"] == "READY_BUT_LOCKED"

    def test_all_none_source_health_critical(self):
        result = _gate()
        assert result["reason_code"] == "SOURCE_HEALTH_CRITICAL"


# ---------------------------------------------------------------------------
# 6. Conditions dict
# ---------------------------------------------------------------------------

class TestConditions:
    def test_conditions_key_present(self):
        assert "conditions" in _open_gate()

    def test_conditions_readiness_status_present(self):
        assert "readiness_status" in _open_gate()["conditions"]

    def test_conditions_trigger_status_present(self):
        assert "trigger_status" in _open_gate()["conditions"]

    def test_conditions_source_health_status_present(self):
        assert "source_health_status" in _open_gate()["conditions"]

    def test_conditions_values_reflect_input(self):
        result = _gate(_readiness("READY"), _trigger("NONE"), _health("HEALTHY"))
        cond = result["conditions"]
        assert cond["readiness_status"] == "READY"
        assert cond["trigger_status"] == "NONE"
        assert cond["source_health_status"] == "HEALTHY"

    def test_conditions_fallback_on_none(self):
        result = _gate()
        cond = result["conditions"]
        assert cond["readiness_status"] == "NOT_READY"
        assert cond["trigger_status"] == "URGENT"
        assert cond["source_health_status"] == "CRITICAL"


# ---------------------------------------------------------------------------
# 7. Flags
# ---------------------------------------------------------------------------

class TestFlags:
    def test_flags_key_present(self):
        assert "flags" in _open_gate()

    def test_paper_only_true(self):
        assert _open_gate()["flags"]["paper_only"] is True

    def test_execution_disabled_true(self):
        assert _open_gate()["flags"]["execution_disabled"] is True

    def test_flags_on_blocked(self):
        result = _gate()
        assert result["flags"]["paper_only"] is True
        assert result["flags"]["execution_disabled"] is True


# ---------------------------------------------------------------------------
# 8. Version and timestamp
# ---------------------------------------------------------------------------

class TestMetadata:
    def test_version_key(self):
        assert _open_gate()["version"] == "execution_readiness_gate_v1"

    def test_ts_utc_present(self):
        assert "ts_utc" in _open_gate()

    def test_ts_utc_value(self):
        assert _open_gate()["ts_utc"] == _TS


# ---------------------------------------------------------------------------
# 9. Fail-closed: unknown / empty values
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_unknown_readiness_status_blocked(self):
        rd = {"readiness_status": "UNKNOWN"}
        result = _gate(rd, _trigger("NONE"), _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_unknown_trigger_status_blocked(self):
        tr = {"trigger_status": "UNKNOWN"}
        result = _gate(_readiness("READY"), tr, _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_unknown_health_status_blocked(self):
        hl = {"source_health_status": "UNKNOWN"}
        result = _gate(_readiness("READY"), _trigger("NONE"), hl)
        assert result["gate_status"] == "BLOCKED"

    def test_empty_dict_readiness_blocked(self):
        result = _gate({}, _trigger("NONE"), _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_empty_dict_trigger_blocked(self):
        result = _gate(_readiness("READY"), {}, _health("HEALTHY"))
        assert result["gate_status"] == "BLOCKED"

    def test_empty_dict_health_blocked(self):
        result = _gate(_readiness("READY"), _trigger("NONE"), {})
        assert result["gate_status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# 10. I/O — run_execution_gate
# ---------------------------------------------------------------------------

class TestRunExecutionGate:
    def test_missing_sources_no_crash(self, tmp_path):
        run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = tmp_path / "gate.json",
            now_utc        = _NOW,
        )

    def test_output_file_written(self, tmp_path):
        out = tmp_path / "gate.json"
        run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert out.exists()

    def test_output_is_valid_json(self, tmp_path):
        out = tmp_path / "gate.json"
        run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert "gate_status" in data

    def test_missing_sources_blocked(self, tmp_path):
        out = tmp_path / "gate.json"
        result = run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert result["gate_status"] == "BLOCKED"

    def test_corrupt_source_blocked(self, tmp_path):
        bad = tmp_path / "rd.json"
        bad.write_text("{ garbage {{{", encoding="utf-8")
        out = tmp_path / "gate.json"
        result = run_execution_gate(
            readiness_path = bad,
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert result["gate_status"] == "BLOCKED"

    def test_valid_sources_open(self, tmp_path):
        def _w(name, data):
            p = tmp_path / name
            p.write_text(json.dumps(data), encoding="utf-8")
            return p

        rdp = _w("rd.json",  {"readiness_status": "READY", "readiness_score": 100})
        trp = _w("tr.json",  {"trigger_status": "NONE"})
        hlp = _w("hl.json",  {"source_health_status": "HEALTHY"})
        out = tmp_path / "gate.json"

        result = run_execution_gate(
            readiness_path = rdp,
            trigger_path   = trp,
            health_path    = hlp,
            output_path    = out,
            now_utc        = _NOW,
        )
        assert result["gate_status"] == "OPEN"
        assert result["execution_allowed"] is False

    def test_no_extra_files(self, tmp_path):
        out = tmp_path / "gate.json"
        run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        assert {f.name for f in tmp_path.iterdir()} == {"gate.json"}

    def test_execution_allowed_false_in_output_file(self, tmp_path):
        out = tmp_path / "gate.json"
        run_execution_gate(
            readiness_path = tmp_path / "no_rd.json",
            trigger_path   = tmp_path / "no_tr.json",
            health_path    = tmp_path / "no_hl.json",
            output_path    = out,
            now_utc        = _NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["execution_allowed"] is False


# ---------------------------------------------------------------------------
# 11. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_deterministic_open(self):
        args = (_readiness("READY"), _trigger("NONE"), _health("HEALTHY"), _NOW)
        assert build_execution_gate(*args) == build_execution_gate(*args)

    def test_deterministic_blocked(self):
        args = (None, None, None, _NOW)
        assert build_execution_gate(*args) == build_execution_gate(*args)

    def test_deterministic_limited(self):
        args = (_readiness("LIMITED"), _trigger("WATCH"), _health("DEGRADED"), _NOW)
        assert build_execution_gate(*args) == build_execution_gate(*args)
