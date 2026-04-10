"""
AC-99 tests — Human Feedback Capture (Non-Binding)

Coverage:
  - build_feedback_entry: valid (CONFIRM/DISAGREE/UNCERTAIN)
  - build_feedback_entry: invalid feedback input (fail-closed)
  - action_context extraction from anomaly_action_queue
  - source_context extraction from anomaly_action_queue.source_context
  - Flags invariants (all entries)
  - append_feedback_entry: file created, JSONL written
  - append_feedback_entry: multiple entries append correctly (no overwrite)
  - read_feedback_log: reads back all entries
  - read_feedback_log: returns [] for absent file
  - capture_and_append: build + write in one call
  - Determinism (same valid input → same entry structure, excluding ts_utc)
  - No mutation of input objects
"""
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_human_feedback_capture_lite import (
    build_feedback_entry,
    append_feedback_entry,
    read_feedback_log,
    capture_and_append,
    FEEDBACK_CONFIRM, FEEDBACK_DISAGREE, FEEDBACK_UNCERTAIN, FEEDBACK_INVALID,
    VERSION, COMPONENT,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _queue(**overrides) -> dict:
    q = {
        "action_class":  "NO_ACTION",
        "urgency":       "NONE",
        "reason_code":   "ACTION_NONE",
        "source_context": {
            "anomaly_level":    "NONE",
            "promotion_status": "PAPER_READY",
            "dossier_status":   "DOSSIER_READY",
            "review_status":    "REVIEW_READY",
        },
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }
    q.update(overrides)
    return q


def _feedback(action="CONFIRM", note="all good", operator_id="op1") -> dict:
    return {"feedback_action": action, "feedback_note": note, "operator_id": operator_id}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_flags(entry: dict) -> None:
    f = entry["flags"]
    assert f["non_binding"]             is True
    assert f["simulation_only"]         is True
    assert f["paper_only"]              is True
    assert f["live_activation_allowed"] is False


def _assert_structure(entry: dict) -> None:
    for key in (
        "ts_utc", "feedback_action", "feedback_note", "operator_id",
        "entry_valid", "action_context", "source_context", "flags",
    ):
        assert key in entry, f"missing key: {key}"
    ac = entry["action_context"]
    for k in ("action_class", "urgency", "reason_code"):
        assert k in ac, f"missing action_context key: {k}"
    sc = entry["source_context"]
    for k in ("anomaly_level", "promotion_status", "dossier_status", "review_status"):
        assert k in sc, f"missing source_context key: {k}"


# ---------------------------------------------------------------------------
# 1. Valid feedback entries
# ---------------------------------------------------------------------------

class TestValidFeedback:
    def test_confirm_entry_valid(self):
        e = build_feedback_entry(_feedback("CONFIRM"), _queue())
        assert e["entry_valid"] is True
        assert e["feedback_action"] == FEEDBACK_CONFIRM

    def test_disagree_entry_valid(self):
        e = build_feedback_entry(_feedback("DISAGREE"), _queue())
        assert e["entry_valid"] is True
        assert e["feedback_action"] == FEEDBACK_DISAGREE

    def test_uncertain_entry_valid(self):
        e = build_feedback_entry(_feedback("UNCERTAIN"), _queue())
        assert e["entry_valid"] is True
        assert e["feedback_action"] == FEEDBACK_UNCERTAIN

    def test_feedback_note_preserved(self):
        e = build_feedback_entry(_feedback(note="looks fine"), _queue())
        assert e["feedback_note"] == "looks fine"

    def test_operator_id_preserved(self):
        e = build_feedback_entry(_feedback(operator_id="alice"), _queue())
        assert e["operator_id"] == "alice"

    def test_empty_note_allowed(self):
        e = build_feedback_entry({"feedback_action": "CONFIRM"}, _queue())
        assert e["entry_valid"] is True
        assert e["feedback_note"] == ""

    def test_empty_operator_id_allowed(self):
        e = build_feedback_entry({"feedback_action": "CONFIRM"}, _queue())
        assert e["operator_id"] == ""

    def test_ts_utc_format(self):
        e = build_feedback_entry(_feedback(), _queue())
        assert e["ts_utc"].endswith("Z")
        assert "T" in e["ts_utc"]

    def test_flags(self):
        _assert_flags(build_feedback_entry(_feedback(), _queue()))

    def test_structure(self):
        _assert_structure(build_feedback_entry(_feedback(), _queue()))


# ---------------------------------------------------------------------------
# 2. action_context extraction
# ---------------------------------------------------------------------------

class TestActionContext:
    def test_action_class_from_queue(self):
        q = _queue(action_class="REVIEW_CONFLICT")
        e = build_feedback_entry(_feedback(), q)
        assert e["action_context"]["action_class"] == "REVIEW_CONFLICT"

    def test_urgency_from_queue(self):
        q = _queue(urgency="HIGH")
        e = build_feedback_entry(_feedback(), q)
        assert e["action_context"]["urgency"] == "HIGH"

    def test_reason_code_from_queue(self):
        q = _queue(reason_code="ACTION_URGENT_CONFLICT")
        e = build_feedback_entry(_feedback(), q)
        assert e["action_context"]["reason_code"] == "ACTION_URGENT_CONFLICT"

    def test_missing_queue_gives_empty_action_context(self):
        e = build_feedback_entry(_feedback(), None)
        ac = e["action_context"]
        assert ac["action_class"] == ""
        assert ac["urgency"] == ""
        assert ac["reason_code"] == ""

    def test_non_dict_queue_gives_empty_action_context(self):
        e = build_feedback_entry(_feedback(), "bad")
        ac = e["action_context"]
        assert ac["action_class"] == ""

    def test_partial_queue_gives_defaults(self):
        e = build_feedback_entry(_feedback(), {"action_class": "NO_ACTION"})
        assert e["action_context"]["urgency"] == ""


# ---------------------------------------------------------------------------
# 3. source_context extraction
# ---------------------------------------------------------------------------

class TestSourceContext:
    def test_anomaly_level_from_queue(self):
        q = _queue()
        q["source_context"]["anomaly_level"] = "HIGH"
        e = build_feedback_entry(_feedback(), q)
        assert e["source_context"]["anomaly_level"] == "HIGH"

    def test_promotion_status_from_queue(self):
        q = _queue()
        q["source_context"]["promotion_status"] = "PAPER_REJECTED"
        e = build_feedback_entry(_feedback(), q)
        assert e["source_context"]["promotion_status"] == "PAPER_REJECTED"

    def test_dossier_status_from_queue(self):
        q = _queue()
        q["source_context"]["dossier_status"] = "DOSSIER_HOLD"
        e = build_feedback_entry(_feedback(), q)
        assert e["source_context"]["dossier_status"] == "DOSSIER_HOLD"

    def test_review_status_from_queue(self):
        q = _queue()
        q["source_context"]["review_status"] = "REVIEW_HOLD"
        e = build_feedback_entry(_feedback(), q)
        assert e["source_context"]["review_status"] == "REVIEW_HOLD"

    def test_missing_source_context_gives_empty_strings(self):
        q = {"action_class": "NO_ACTION", "urgency": "NONE", "reason_code": "ACTION_NONE"}
        e = build_feedback_entry(_feedback(), q)
        sc = e["source_context"]
        assert sc["anomaly_level"] == ""
        assert sc["promotion_status"] == ""

    def test_no_queue_gives_empty_source_context(self):
        e = build_feedback_entry(_feedback(), None)
        sc = e["source_context"]
        assert all(v == "" for v in sc.values())

    def test_non_dict_source_context_gives_empty(self):
        q = _queue()
        q["source_context"] = "bad"
        e = build_feedback_entry(_feedback(), q)
        sc = e["source_context"]
        assert all(v == "" for v in sc.values())


# ---------------------------------------------------------------------------
# 4. Invalid feedback — fail-closed
# ---------------------------------------------------------------------------

class TestInvalidFeedback:
    def test_non_dict_feedback_gives_invalid_entry(self):
        e = build_feedback_entry(None, _queue())
        assert e["entry_valid"] is False
        assert e["feedback_action"] == FEEDBACK_INVALID

    def test_string_feedback_gives_invalid_entry(self):
        e = build_feedback_entry("CONFIRM", _queue())
        assert e["entry_valid"] is False

    def test_list_feedback_gives_invalid_entry(self):
        e = build_feedback_entry([], _queue())
        assert e["entry_valid"] is False

    def test_missing_feedback_action_gives_invalid(self):
        e = build_feedback_entry({"feedback_note": "oops"}, _queue())
        assert e["entry_valid"] is False
        assert e["feedback_action"] == FEEDBACK_INVALID

    def test_unknown_feedback_action_gives_invalid(self):
        e = build_feedback_entry({"feedback_action": "APPROVE"}, _queue())
        assert e["entry_valid"] is False

    def test_empty_feedback_action_gives_invalid(self):
        e = build_feedback_entry({"feedback_action": ""}, _queue())
        assert e["entry_valid"] is False

    def test_invalid_entry_has_reason_in_note(self):
        e = build_feedback_entry(None, _queue())
        assert e["feedback_note"]  # not empty

    def test_invalid_entry_flags_correct(self):
        _assert_flags(build_feedback_entry(None, _queue()))

    def test_invalid_entry_structure_valid(self):
        _assert_structure(build_feedback_entry(None, _queue()))

    def test_invalid_no_crash_with_none_queue(self):
        e = build_feedback_entry(None, None)
        assert e["entry_valid"] is False
        assert e["feedback_action"] == FEEDBACK_INVALID

    def test_empty_dict_feedback_gives_invalid(self):
        e = build_feedback_entry({}, _queue())
        assert e["entry_valid"] is False


# ---------------------------------------------------------------------------
# 5. File I/O — append_feedback_entry
# ---------------------------------------------------------------------------

class TestAppendFeedbackEntry:
    def test_file_created(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(_feedback(), _queue())
        append_feedback_entry(e, log)
        assert log.exists()

    def test_single_entry_written(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(_feedback("CONFIRM", "ok"), _queue())
        append_feedback_entry(e, log)
        lines = log.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1

    def test_entry_is_valid_json(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(_feedback(), _queue())
        append_feedback_entry(e, log)
        line = log.read_text(encoding="utf-8").strip()
        parsed = json.loads(line)
        assert parsed["feedback_action"] == "CONFIRM"

    def test_multiple_entries_appended(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        for action in ("CONFIRM", "DISAGREE", "UNCERTAIN"):
            e = build_feedback_entry(_feedback(action), _queue())
            append_feedback_entry(e, log)
        lines = log.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3

    def test_entries_in_order(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        for action in ("CONFIRM", "DISAGREE"):
            e = build_feedback_entry(_feedback(action), _queue())
            append_feedback_entry(e, log)
        lines = log.read_text(encoding="utf-8").strip().splitlines()
        assert json.loads(lines[0])["feedback_action"] == "CONFIRM"
        assert json.loads(lines[1])["feedback_action"] == "DISAGREE"

    def test_no_overwrite_existing(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e1 = build_feedback_entry(_feedback("CONFIRM", "first"), _queue())
        e2 = build_feedback_entry(_feedback("DISAGREE", "second"), _queue())
        append_feedback_entry(e1, log)
        append_feedback_entry(e2, log)
        entries = read_feedback_log(log)
        assert entries[0]["feedback_note"] == "first"
        assert entries[1]["feedback_note"] == "second"

    def test_parent_dir_created(self, tmp_path):
        log = tmp_path / "nested" / "dir" / "feedback.jsonl"
        e = build_feedback_entry(_feedback(), _queue())
        append_feedback_entry(e, log)
        assert log.exists()

    def test_invalid_entry_also_appended(self, tmp_path):
        """fail-closed: invalid entries are also written to log."""
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(None, _queue())
        append_feedback_entry(e, log)
        lines = log.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["entry_valid"] is False


# ---------------------------------------------------------------------------
# 6. read_feedback_log
# ---------------------------------------------------------------------------

class TestReadFeedbackLog:
    def test_returns_empty_list_for_absent_file(self, tmp_path):
        log = tmp_path / "nonexistent.jsonl"
        assert read_feedback_log(log) == []

    def test_reads_all_entries(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        for action in ("CONFIRM", "DISAGREE", "UNCERTAIN"):
            e = build_feedback_entry(_feedback(action), _queue())
            append_feedback_entry(e, log)
        entries = read_feedback_log(log)
        assert len(entries) == 3

    def test_round_trip_structure(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(_feedback("CONFIRM", "round trip", "op99"), _queue())
        append_feedback_entry(e, log)
        loaded = read_feedback_log(log)[0]
        assert loaded["feedback_action"] == "CONFIRM"
        assert loaded["feedback_note"] == "round trip"
        assert loaded["operator_id"] == "op99"
        assert loaded["entry_valid"] is True

    def test_flags_preserved_after_round_trip(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = build_feedback_entry(_feedback(), _queue())
        append_feedback_entry(e, log)
        loaded = read_feedback_log(log)[0]
        _assert_flags(loaded)

    def test_source_context_preserved(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        q = _queue()
        q["source_context"]["anomaly_level"] = "HIGH"
        e = build_feedback_entry(_feedback(), q)
        append_feedback_entry(e, log)
        loaded = read_feedback_log(log)[0]
        assert loaded["source_context"]["anomaly_level"] == "HIGH"


# ---------------------------------------------------------------------------
# 7. capture_and_append convenience
# ---------------------------------------------------------------------------

class TestCaptureAndAppend:
    def test_returns_entry_dict(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        result = capture_and_append(_feedback(), _queue(), path=log)
        assert isinstance(result, dict)
        assert result["feedback_action"] == "CONFIRM"

    def test_writes_to_file(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        capture_and_append(_feedback(), _queue(), path=log)
        assert log.exists()
        entries = read_feedback_log(log)
        assert len(entries) == 1

    def test_multiple_calls_append(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        capture_and_append(_feedback("CONFIRM"), _queue(), path=log)
        capture_and_append(_feedback("DISAGREE"), _queue(), path=log)
        entries = read_feedback_log(log)
        assert len(entries) == 2

    def test_invalid_feedback_written_fail_closed(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        result = capture_and_append(None, _queue(), path=log)
        assert result["entry_valid"] is False
        entries = read_feedback_log(log)
        assert len(entries) == 1
        assert entries[0]["entry_valid"] is False

    def test_no_crash_all_none(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        result = capture_and_append(None, None, None, path=log)
        assert result["entry_valid"] is False


# ---------------------------------------------------------------------------
# 8. Flags invariants
# ---------------------------------------------------------------------------

class TestFlagsInvariants:
    def test_confirm(self):
        _assert_flags(build_feedback_entry(_feedback("CONFIRM"), _queue()))

    def test_disagree(self):
        _assert_flags(build_feedback_entry(_feedback("DISAGREE"), _queue()))

    def test_uncertain(self):
        _assert_flags(build_feedback_entry(_feedback("UNCERTAIN"), _queue()))

    def test_invalid_feedback(self):
        _assert_flags(build_feedback_entry(None, _queue()))

    def test_no_queue(self):
        _assert_flags(build_feedback_entry(_feedback(), None))

    def test_all_none(self):
        _assert_flags(build_feedback_entry(None, None))


# ---------------------------------------------------------------------------
# 9. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_valid_input_same_structure(self):
        q = _queue()
        e1 = build_feedback_entry(_feedback("CONFIRM", "note", "op1"), q)
        e2 = build_feedback_entry(_feedback("CONFIRM", "note", "op1"), q)
        e1.pop("ts_utc"); e2.pop("ts_utc")
        assert e1 == e2

    def test_same_invalid_input_same_structure(self):
        q = _queue()
        e1 = build_feedback_entry(None, q)
        e2 = build_feedback_entry(None, q)
        e1.pop("ts_utc"); e2.pop("ts_utc")
        # notes may vary slightly if they include dynamic content — check shape
        assert e1["entry_valid"] == e2["entry_valid"]
        assert e1["feedback_action"] == e2["feedback_action"]
        assert e1["flags"] == e2["flags"]

    def test_no_mutation_of_feedback(self):
        fb = _feedback()
        original = dict(fb)
        build_feedback_entry(fb, _queue())
        assert fb == original

    def test_no_mutation_of_queue(self):
        q = _queue()
        original = dict(q)
        build_feedback_entry(_feedback(), q)
        assert q == original


# ---------------------------------------------------------------------------
# 10. Entry structure completeness
# ---------------------------------------------------------------------------

class TestEntryStructure:
    def test_all_required_keys_confirm(self):
        _assert_structure(build_feedback_entry(_feedback("CONFIRM"), _queue()))

    def test_all_required_keys_disagree(self):
        _assert_structure(build_feedback_entry(_feedback("DISAGREE"), _queue()))

    def test_all_required_keys_uncertain(self):
        _assert_structure(build_feedback_entry(_feedback("UNCERTAIN"), _queue()))

    def test_all_required_keys_invalid(self):
        _assert_structure(build_feedback_entry(None, _queue()))

    def test_entry_valid_is_bool(self):
        e = build_feedback_entry(_feedback(), _queue())
        assert isinstance(e["entry_valid"], bool)

    def test_action_context_values_are_str(self):
        e = build_feedback_entry(_feedback(), _queue())
        for v in e["action_context"].values():
            assert isinstance(v, str)

    def test_source_context_values_are_str(self):
        e = build_feedback_entry(_feedback(), _queue())
        for v in e["source_context"].values():
            assert isinstance(v, str)
