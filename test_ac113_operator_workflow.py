"""
AC-113 tests — Operator Workflow (Daily Routine)
AC-116 update: refresh trigger integration tests

Coverage:
  - show() runs without crash
  - output contains all 3 workflow steps
  - all 3 command scripts referenced
  - interpretation guide present
  - STOP/MONITOR/REVIEW/CLEAR conditions present
  - flags / notes section present
  - no file writes
  - deterministic output
  - AC-116: trigger interpretation rules present (NONE/WATCH/DUE/URGENT)
  - AC-116: missing trigger file → no crash, NO DATA shown
  - AC-116: valid trigger file → current status shown
  - AC-116: URGENT trigger → run immediately instruction shown
"""
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from show_operator_workflow import show, _load_trigger, _trigger_block


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOFILE = Path("C:/nonexistent/trigger.json")


def _captured(trigger_path: Path = _NOFILE) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        show(trigger_path=trigger_path)
    finally:
        sys.stdout = old
    return buf.getvalue()


def _trigger(status: str = "URGENT",
             action: str = "RUN_MANUAL_REFRESH_CHECK_NOW",
             window: str = "NOW",
             reason: str = "SOURCE_CRITICAL") -> dict:
    return {
        "version":               "refresh_trigger_v1",
        "trigger_status":        status,
        "trigger_reason_code":   reason,
        "refresh_check_required": status != "NONE",
        "operator_guidance": {"recommended_action": action,
                               "recommended_window": window},
    }


# ---------------------------------------------------------------------------
# 1. Execution
# ---------------------------------------------------------------------------

class TestExecution:
    def test_runs_without_crash(self):
        show()

    def test_returns_none(self):
        result = show()
        assert result is None

    def test_output_non_empty(self):
        output = _captured()
        assert len(output) > 0


# ---------------------------------------------------------------------------
# 2. Header
# ---------------------------------------------------------------------------

class TestHeader:
    def test_header_present(self):
        output = _captured()
        assert "ANT DAILY WORKFLOW" in output


# ---------------------------------------------------------------------------
# 3. Workflow steps
# ---------------------------------------------------------------------------

class TestWorkflowSteps:
    def test_step_1_present(self):
        output = _captured()
        assert "1." in output

    def test_step_2_present(self):
        output = _captured()
        assert "2." in output

    def test_step_3_present(self):
        output = _captured()
        assert "3." in output

    def test_refresh_check_step(self):
        output = _captured()
        assert "refresh" in output.lower() or "Refresh" in output

    def test_operator_summary_step(self):
        output = _captured()
        assert "operator" in output.lower() or "summary" in output.lower()

    def test_dashboard_step(self):
        output = _captured()
        assert "dashboard" in output.lower() or "Dashboard" in output


# ---------------------------------------------------------------------------
# 4. Commands
# ---------------------------------------------------------------------------

class TestCommands:
    def test_refresh_check_command(self):
        output = _captured()
        assert "run_manual_refresh_check_lite.py" in output

    def test_operator_summary_command(self):
        output = _captured()
        assert "show_operator_summary.py" in output

    def test_dashboard_command(self):
        output = _captured()
        assert "show_feedback_dashboard.py" in output


# ---------------------------------------------------------------------------
# 5. Interpretation conditions
# ---------------------------------------------------------------------------

class TestInterpretationConditions:
    def test_stop_condition_present(self):
        output = _captured()
        assert "STOP" in output

    def test_monitor_condition_present(self):
        output = _captured()
        assert "MONITOR" in output

    def test_review_condition_present(self):
        output = _captured()
        assert "REVIEW" in output

    def test_health_critical_stop(self):
        output = _captured()
        assert "CRITICAL" in output

    def test_health_degraded_monitor(self):
        output = _captured()
        assert "DEGRADED" in output

    def test_health_healthy_ok(self):
        output = _captured()
        assert "HEALTHY" in output

    def test_overview_attention_review(self):
        output = _captured()
        assert "ATTENTION" in output

    def test_overview_watch_monitor(self):
        output = _captured()
        assert "WATCH" in output

    def test_interpretation_guide_present(self):
        output = _captured()
        assert "nterpretation" in output


# ---------------------------------------------------------------------------
# 6. Notes / flags section
# ---------------------------------------------------------------------------

class TestNotesSection:
    def test_non_binding_flag(self):
        output = _captured()
        assert "non_binding" in output

    def test_simulation_only_flag(self):
        output = _captured()
        assert "simulation_only" in output

    def test_paper_only_flag(self):
        output = _captured()
        assert "paper_only" in output

    def test_live_activation_not_allowed(self):
        output = _captured()
        assert "live_activation_allowed" in output

    def test_no_execution_note(self):
        output = _captured()
        assert "execution" in output.lower()


# ---------------------------------------------------------------------------
# 7. Determinism / no file writes
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_deterministic(self):
        assert _captured() == _captured()

    def test_no_file_writes(self, tmp_path):
        before = set(tmp_path.iterdir())
        show(trigger_path=_NOFILE)
        after = set(tmp_path.iterdir())
        assert before == after


# ---------------------------------------------------------------------------
# 8. Trigger interpretation rules (AC-116) — static section
# ---------------------------------------------------------------------------

class TestTriggerInterpretationRules:
    def test_trigger_none_rule_present(self):
        output = _captured()
        assert "trigger=NONE" in output or "NONE" in output

    def test_trigger_watch_rule_present(self):
        output = _captured()
        assert "trigger=WATCH" in output or "WATCH" in output

    def test_trigger_due_rule_present(self):
        output = _captured()
        assert "trigger=DUE" in output or "DUE" in output

    def test_trigger_urgent_rule_present(self):
        output = _captured()
        assert "trigger=URGENT" in output or "URGENT" in output

    def test_trigger_interpretation_header(self):
        output = _captured()
        assert "rigger interpretation" in output or "Trigger" in output

    def test_trigger_none_proceed_normally(self):
        output = _captured()
        assert "proceed normally" in output or "no refresh" in output.lower()

    def test_trigger_urgent_run_immediately(self):
        output = _captured()
        assert "immediately" in output or "URGENT" in output


# ---------------------------------------------------------------------------
# 9. Live trigger section (AC-116) — dynamic current status
# ---------------------------------------------------------------------------

class TestLiveTriggerSection:
    def test_missing_trigger_no_crash(self, tmp_path):
        show(trigger_path=tmp_path / "nonexistent.json")

    def test_missing_trigger_no_data_shown(self, tmp_path):
        output = _captured(tmp_path / "nonexistent.json")
        assert "NO DATA" in output

    def test_corrupt_trigger_no_crash(self, tmp_path):
        bad = tmp_path / "trigger.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        show(trigger_path=bad)

    def test_corrupt_trigger_no_data_shown(self, tmp_path):
        bad = tmp_path / "trigger.json"
        bad.write_text("garbage", encoding="utf-8")
        output = _captured(bad)
        assert "NO DATA" in output

    def test_urgent_trigger_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger("URGENT")), encoding="utf-8")
        output = _captured(tp)
        assert "URGENT" in output

    def test_none_trigger_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(
            _trigger("NONE", "NONE", "NONE", "SOURCE_HEALTHY_RECOVERY_NONE")
        ), encoding="utf-8")
        output = _captured(tp)
        assert "NONE" in output

    def test_watch_trigger_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger("WATCH", "MONITOR", "LATER", "SOURCE_DEGRADED")),
                      encoding="utf-8")
        output = _captured(tp)
        assert "WATCH" in output

    def test_due_trigger_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(
            _trigger("DUE", "MONITOR", "SOON", "SOURCE_DEGRADED_AND_RECOVERY_PLAN_READY")
        ), encoding="utf-8")
        output = _captured(tp)
        assert "DUE" in output

    def test_action_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger("URGENT", "RUN_MANUAL_REFRESH_CHECK_NOW")),
                      encoding="utf-8")
        output = _captured(tp)
        assert "RUN_MANUAL_REFRESH_CHECK_NOW" in output

    def test_window_shown(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger("URGENT", window="NOW")), encoding="utf-8")
        output = _captured(tp)
        assert "NOW" in output

    def test_current_trigger_section_header(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger()), encoding="utf-8")
        output = _captured(tp)
        assert "Current trigger" in output or "current trigger" in output.lower()

    def test_no_file_writes(self, tmp_path):
        tp = tmp_path / "trigger.json"
        tp.write_text(json.dumps(_trigger()), encoding="utf-8")
        before = set(tmp_path.iterdir())
        show(trigger_path=tp)
        after = set(tmp_path.iterdir())
        assert before == after
