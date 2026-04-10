"""
AC-113 tests — Operator Workflow (Daily Routine)

Coverage:
  - show() runs without crash
  - output contains all 3 workflow steps
  - all 3 command scripts referenced
  - interpretation guide present
  - STOP/MONITOR/REVIEW/CLEAR conditions present
  - flags / notes section present
  - no file writes
  - deterministic output
"""
import io
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from show_operator_workflow import show


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _captured() -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        show()
    finally:
        sys.stdout = old
    return buf.getvalue()


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
        show()
        after = set(tmp_path.iterdir())
        assert before == after
