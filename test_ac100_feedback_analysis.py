"""
AC-100 tests — Feedback Analysis Layer

Coverage:
  - analyse_feedback: empty list → all zeros
  - totals: correct counts for CONFIRM/DISAGREE/UNCERTAIN/INVALID
  - rates: correct calculation, zero-safe division
  - by_action_class: grouping correct, all known keys pre-seeded
  - by_urgency: grouping correct, all known keys pre-seeded
  - alignment mapping: HIGH/MEDIUM/LOW thresholds
  - needs_attention: all three trigger conditions + NONE
  - attention_reason_code values
  - flags invariants
  - corrupted entries skipped silently (no crash)
  - load_feedback_log: absent file → [], corrupted lines skipped
  - analyse_from_log integration
  - write_feedback_analysis creates file
  - Determinism (same entries → same output, excluding ts_utc)
  - No mutation of input list
"""
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_feedback_analysis_lite import (
    analyse_feedback,
    load_feedback_log,
    write_feedback_analysis,
    analyse_from_log,
    ALIGN_HIGH, ALIGN_MEDIUM, ALIGN_LOW,
    ATTN_NONE, ATTN_CRITICAL_DISAGREE,
    ATTN_HIGH_DISAGREE_RATE, ATTN_LOW_CONFIRM_RATE,
    VERSION, COMPONENT,
    _KNOWN_ACTION_CLASSES, _KNOWN_URGENCIES,
)


# ---------------------------------------------------------------------------
# Entry factories
# ---------------------------------------------------------------------------

def _entry(
    action:       str = "CONFIRM",
    action_class: str = "NO_ACTION",
    urgency:      str = "NONE",
) -> dict:
    return {
        "feedback_action": action,
        "feedback_note":   "",
        "operator_id":     "",
        "entry_valid":     action in ("CONFIRM", "DISAGREE", "UNCERTAIN"),
        "action_context": {
            "action_class": action_class,
            "urgency":      urgency,
            "reason_code":  "ACTION_NONE",
        },
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_flags(a: dict) -> None:
    f = a["flags"]
    assert f["non_binding"]             is True
    assert f["simulation_only"]         is True
    assert f["paper_only"]              is True
    assert f["live_activation_allowed"] is False


def _assert_structure(a: dict) -> None:
    for key in (
        "version", "component", "ts_utc",
        "totals", "rates", "by_action_class", "by_urgency",
        "signals", "flags",
    ):
        assert key in a, f"missing key: {key}"
    assert a["version"]   == VERSION
    assert a["component"] == COMPONENT
    for k in ("entries", "confirm", "disagree", "uncertain", "invalid"):
        assert k in a["totals"]
    for k in ("confirm_rate", "disagree_rate", "uncertain_rate"):
        assert k in a["rates"]
    for k in ("system_human_alignment", "needs_attention", "attention_reason_code"):
        assert k in a["signals"]


# ---------------------------------------------------------------------------
# 1. Empty list → all zeros
# ---------------------------------------------------------------------------

class TestEmpty:
    def test_entries_zero(self):
        a = analyse_feedback([])
        assert a["totals"]["entries"] == 0

    def test_confirm_zero(self):
        assert analyse_feedback([])["totals"]["confirm"] == 0

    def test_disagree_zero(self):
        assert analyse_feedback([])["totals"]["disagree"] == 0

    def test_uncertain_zero(self):
        assert analyse_feedback([])["totals"]["uncertain"] == 0

    def test_invalid_zero(self):
        assert analyse_feedback([])["totals"]["invalid"] == 0

    def test_confirm_rate_zero(self):
        assert analyse_feedback([])["rates"]["confirm_rate"] == 0.0

    def test_disagree_rate_zero(self):
        assert analyse_feedback([])["rates"]["disagree_rate"] == 0.0

    def test_uncertain_rate_zero(self):
        assert analyse_feedback([])["rates"]["uncertain_rate"] == 0.0

    def test_alignment_high_when_empty(self):
        # disagree_rate=0 < 0.2 → HIGH
        assert analyse_feedback([])["signals"]["system_human_alignment"] == ALIGN_HIGH

    def test_needs_attention_false_when_empty(self):
        assert analyse_feedback([])["signals"]["needs_attention"] is False

    def test_flags(self):
        _assert_flags(analyse_feedback([]))

    def test_structure(self):
        _assert_structure(analyse_feedback([]))

    def test_known_action_class_keys_present(self):
        a = analyse_feedback([])
        for k in _KNOWN_ACTION_CLASSES:
            assert k in a["by_action_class"], f"missing key: {k}"

    def test_known_urgency_keys_present(self):
        a = analyse_feedback([])
        for k in _KNOWN_URGENCIES:
            assert k in a["by_urgency"], f"missing key: {k}"


# ---------------------------------------------------------------------------
# 2. Totals
# ---------------------------------------------------------------------------

class TestTotals:
    def test_single_confirm(self):
        a = analyse_feedback([_entry("CONFIRM")])
        assert a["totals"]["confirm"] == 1
        assert a["totals"]["entries"] == 1

    def test_single_disagree(self):
        a = analyse_feedback([_entry("DISAGREE")])
        assert a["totals"]["disagree"] == 1

    def test_single_uncertain(self):
        a = analyse_feedback([_entry("UNCERTAIN")])
        assert a["totals"]["uncertain"] == 1

    def test_single_invalid(self):
        a = analyse_feedback([_entry("INVALID")])
        assert a["totals"]["invalid"] == 1

    def test_mixed_counts(self):
        entries = [
            _entry("CONFIRM"),
            _entry("CONFIRM"),
            _entry("DISAGREE"),
            _entry("UNCERTAIN"),
            _entry("INVALID"),
        ]
        a = analyse_feedback(entries)
        assert a["totals"]["confirm"]   == 2
        assert a["totals"]["disagree"]  == 1
        assert a["totals"]["uncertain"] == 1
        assert a["totals"]["invalid"]   == 1
        assert a["totals"]["entries"]   == 5

    def test_entries_is_sum_of_all(self):
        entries = [_entry("CONFIRM")] * 3 + [_entry("DISAGREE")] * 2
        a = analyse_feedback(entries)
        assert a["totals"]["entries"] == 5

    def test_non_dict_entries_skipped_in_totals(self):
        entries = [_entry("CONFIRM"), "bad", None, 42, _entry("DISAGREE")]
        a = analyse_feedback(entries)
        assert a["totals"]["confirm"]  == 1
        assert a["totals"]["disagree"] == 1
        assert a["totals"]["entries"]  == 2


# ---------------------------------------------------------------------------
# 3. Rates
# ---------------------------------------------------------------------------

class TestRates:
    def test_all_confirm_rate_one(self):
        a = analyse_feedback([_entry("CONFIRM")] * 4)
        assert a["rates"]["confirm_rate"] == 1.0
        assert a["rates"]["disagree_rate"] == 0.0

    def test_all_disagree_rate_one(self):
        a = analyse_feedback([_entry("DISAGREE")] * 3)
        assert a["rates"]["disagree_rate"] == 1.0
        assert a["rates"]["confirm_rate"]  == 0.0

    def test_half_confirm_half_disagree(self):
        entries = [_entry("CONFIRM")] * 2 + [_entry("DISAGREE")] * 2
        a = analyse_feedback(entries)
        assert a["rates"]["confirm_rate"]  == 0.5
        assert a["rates"]["disagree_rate"] == 0.5

    def test_rates_round_to_4_decimal_places(self):
        entries = [_entry("CONFIRM")] * 2 + [_entry("DISAGREE")]
        a = analyse_feedback(entries)
        # 1/3 ≈ 0.3333
        assert a["rates"]["disagree_rate"] == round(1/3, 4)

    def test_invalid_entries_excluded_from_denominator(self):
        entries = [_entry("CONFIRM")] * 2 + [_entry("INVALID")] * 8
        a = analyse_feedback(entries)
        # valid = 2, confirm_rate = 2/2 = 1.0
        assert a["rates"]["confirm_rate"] == 1.0

    def test_rates_sum_to_one(self):
        entries = [_entry("CONFIRM")] * 3 + [_entry("DISAGREE")] * 2 + [_entry("UNCERTAIN")]
        a = analyse_feedback(entries)
        total = (a["rates"]["confirm_rate"] +
                 a["rates"]["disagree_rate"] +
                 a["rates"]["uncertain_rate"])
        assert abs(total - 1.0) < 0.0001

    def test_zero_division_safe(self):
        a = analyse_feedback([])
        assert a["rates"]["confirm_rate"]  == 0.0
        assert a["rates"]["disagree_rate"] == 0.0


# ---------------------------------------------------------------------------
# 4. by_action_class grouping
# ---------------------------------------------------------------------------

class TestByActionClass:
    def test_review_status_counted(self):
        entries = [_entry("CONFIRM", action_class="REVIEW_STATUS")] * 2
        a = analyse_feedback(entries)
        assert a["by_action_class"]["REVIEW_STATUS"]["entries"]  == 2
        assert a["by_action_class"]["REVIEW_STATUS"]["confirm"]  == 2
        assert a["by_action_class"]["REVIEW_STATUS"]["disagree"] == 0

    def test_review_conflict_counted(self):
        entries = [
            _entry("CONFIRM",  action_class="REVIEW_CONFLICT"),
            _entry("DISAGREE", action_class="REVIEW_CONFLICT"),
        ]
        a = analyse_feedback(entries)
        g = a["by_action_class"]["REVIEW_CONFLICT"]
        assert g["entries"]  == 2
        assert g["confirm"]  == 1
        assert g["disagree"] == 1

    def test_review_blocking_findings_counted(self):
        entries = [_entry("DISAGREE", action_class="REVIEW_BLOCKING_FINDINGS")]
        a = analyse_feedback(entries)
        g = a["by_action_class"]["REVIEW_BLOCKING_FINDINGS"]
        assert g["disagree"] == 1

    def test_review_missing_input_counted(self):
        entries = [_entry("UNCERTAIN", action_class="REVIEW_MISSING_INPUT")]
        a = analyse_feedback(entries)
        assert a["by_action_class"]["REVIEW_MISSING_INPUT"]["uncertain"] == 1

    def test_review_critical_state_counted(self):
        entries = [_entry("DISAGREE", action_class="REVIEW_CRITICAL_STATE")]
        a = analyse_feedback(entries)
        assert a["by_action_class"]["REVIEW_CRITICAL_STATE"]["disagree"] == 1

    def test_empty_groups_zero(self):
        a = analyse_feedback([_entry("CONFIRM", action_class="NO_ACTION")])
        assert a["by_action_class"]["REVIEW_STATUS"]["entries"] == 0

    def test_unknown_action_class_creates_group(self):
        entries = [_entry("CONFIRM", action_class="SOME_UNKNOWN")]
        a = analyse_feedback(entries)
        assert "SOME_UNKNOWN" in a["by_action_class"]
        assert a["by_action_class"]["SOME_UNKNOWN"]["confirm"] == 1

    def test_group_entries_field(self):
        entries = [
            _entry("CONFIRM",  action_class="REVIEW_STATUS"),
            _entry("DISAGREE", action_class="REVIEW_STATUS"),
            _entry("CONFIRM",  action_class="NO_ACTION"),
        ]
        a = analyse_feedback(entries)
        assert a["by_action_class"]["REVIEW_STATUS"]["entries"] == 2
        assert a["by_action_class"]["NO_ACTION"]["entries"] == 1


# ---------------------------------------------------------------------------
# 5. by_urgency grouping
# ---------------------------------------------------------------------------

class TestByUrgency:
    def test_low_urgency_counted(self):
        entries = [_entry("CONFIRM", urgency="LOW")] * 3
        a = analyse_feedback(entries)
        assert a["by_urgency"]["LOW"]["entries"] == 3
        assert a["by_urgency"]["LOW"]["confirm"] == 3

    def test_medium_urgency_counted(self):
        entries = [_entry("DISAGREE", urgency="MEDIUM")]
        a = analyse_feedback(entries)
        assert a["by_urgency"]["MEDIUM"]["disagree"] == 1

    def test_high_urgency_counted(self):
        entries = [_entry("UNCERTAIN", urgency="HIGH")]
        a = analyse_feedback(entries)
        assert a["by_urgency"]["HIGH"]["uncertain"] == 1

    def test_critical_urgency_counted(self):
        entries = [_entry("DISAGREE", urgency="CRITICAL")]
        a = analyse_feedback(entries)
        assert a["by_urgency"]["CRITICAL"]["disagree"] == 1

    def test_empty_urgency_groups_zero(self):
        a = analyse_feedback([_entry("CONFIRM", urgency="LOW")])
        assert a["by_urgency"]["CRITICAL"]["entries"] == 0

    def test_none_urgency_counted(self):
        entries = [_entry("CONFIRM", urgency="NONE")]
        a = analyse_feedback(entries)
        assert a["by_urgency"]["NONE"]["confirm"] == 1

    def test_mixed_urgencies(self):
        entries = [
            _entry("CONFIRM",  urgency="LOW"),
            _entry("DISAGREE", urgency="HIGH"),
            _entry("CONFIRM",  urgency="CRITICAL"),
        ]
        a = analyse_feedback(entries)
        assert a["by_urgency"]["LOW"]["entries"]      == 1
        assert a["by_urgency"]["HIGH"]["entries"]     == 1
        assert a["by_urgency"]["CRITICAL"]["entries"] == 1


# ---------------------------------------------------------------------------
# 6. Alignment mapping
# ---------------------------------------------------------------------------

class TestAlignment:
    def test_zero_disagree_is_high(self):
        a = analyse_feedback([_entry("CONFIRM")] * 5)
        assert a["signals"]["system_human_alignment"] == ALIGN_HIGH

    def test_disagree_rate_below_0_2_is_high(self):
        # 1 disagree, 9 confirm → rate=0.1 < 0.2
        entries = [_entry("CONFIRM")] * 9 + [_entry("DISAGREE")]
        a = analyse_feedback(entries)
        assert a["signals"]["system_human_alignment"] == ALIGN_HIGH

    def test_disagree_rate_exactly_0_2_is_medium(self):
        # 2 disagree, 8 confirm → rate=0.2
        entries = [_entry("CONFIRM")] * 8 + [_entry("DISAGREE")] * 2
        a = analyse_feedback(entries)
        assert a["signals"]["system_human_alignment"] == ALIGN_MEDIUM

    def test_disagree_rate_0_35_is_medium(self):
        # approx 35% disagree
        entries = [_entry("CONFIRM")] * 13 + [_entry("DISAGREE")] * 7
        a = analyse_feedback(entries)
        # 7/20 = 0.35 → MEDIUM
        assert a["signals"]["system_human_alignment"] == ALIGN_MEDIUM

    def test_disagree_rate_exactly_0_5_is_medium(self):
        entries = [_entry("CONFIRM")] * 5 + [_entry("DISAGREE")] * 5
        a = analyse_feedback(entries)
        assert a["signals"]["system_human_alignment"] == ALIGN_MEDIUM

    def test_disagree_rate_above_0_5_is_low(self):
        entries = [_entry("CONFIRM")] * 3 + [_entry("DISAGREE")] * 7
        a = analyse_feedback(entries)
        # 7/10 = 0.7 → LOW
        assert a["signals"]["system_human_alignment"] == ALIGN_LOW

    def test_all_disagree_is_low(self):
        a = analyse_feedback([_entry("DISAGREE")] * 5)
        assert a["signals"]["system_human_alignment"] == ALIGN_LOW

    def test_empty_is_high(self):
        assert analyse_feedback([])["signals"]["system_human_alignment"] == ALIGN_HIGH


# ---------------------------------------------------------------------------
# 7. needs_attention + reason code
# ---------------------------------------------------------------------------

class TestNeedsAttention:
    def test_no_attention_when_all_confirm(self):
        a = analyse_feedback([_entry("CONFIRM")] * 5)
        assert a["signals"]["needs_attention"] is False
        assert a["signals"]["attention_reason_code"] == ATTN_NONE

    def test_no_attention_when_empty(self):
        a = analyse_feedback([])
        assert a["signals"]["needs_attention"] is False

    def test_critical_disagree_triggers_attention(self):
        entries = [
            _entry("CONFIRM",  urgency="LOW"),
            _entry("DISAGREE", urgency="CRITICAL"),
        ]
        a = analyse_feedback(entries)
        assert a["signals"]["needs_attention"] is True
        assert a["signals"]["attention_reason_code"] == ATTN_CRITICAL_DISAGREE

    def test_critical_disagree_wins_over_high_rate(self):
        """CRITICAL_DISAGREE takes priority over HIGH_DISAGREE_RATE."""
        # disagree_rate > 0.3 AND CRITICAL disagree present
        entries = (
            [_entry("DISAGREE", urgency="CRITICAL")]
            + [_entry("DISAGREE", urgency="HIGH")] * 3
            + [_entry("CONFIRM")] * 3
        )
        a = analyse_feedback(entries)
        assert a["signals"]["attention_reason_code"] == ATTN_CRITICAL_DISAGREE

    def test_high_disagree_rate_triggers_attention(self):
        # 4 disagree, 6 confirm → rate=0.4 > 0.3, no CRITICAL disagree
        entries = [_entry("CONFIRM")] * 6 + [_entry("DISAGREE")] * 4
        a = analyse_feedback(entries)
        assert a["signals"]["needs_attention"] is True
        assert a["signals"]["attention_reason_code"] == ATTN_HIGH_DISAGREE_RATE

    def test_disagree_rate_exactly_0_3_no_attention(self):
        # rate=0.3 is NOT > 0.3, so no HIGH_DISAGREE_RATE trigger
        entries = [_entry("CONFIRM")] * 7 + [_entry("DISAGREE")] * 3
        a = analyse_feedback(entries)
        # 3/10=0.3, not > 0.3
        assert a["signals"]["attention_reason_code"] != ATTN_HIGH_DISAGREE_RATE

    def test_low_confirm_rate_triggers_attention(self):
        # 1 confirm, 4 uncertain → confirm_rate=0.2, not < 0.2 → no
        # need < 0.2: 1 confirm, 5 uncertain → rate=1/6≈0.167
        entries = [_entry("CONFIRM")] + [_entry("UNCERTAIN")] * 5
        a = analyse_feedback(entries)
        # confirm_rate = 1/6 ≈ 0.1667 < 0.2 → LOW_CONFIRM_RATE
        assert a["signals"]["needs_attention"] is True
        assert a["signals"]["attention_reason_code"] == ATTN_LOW_CONFIRM_RATE

    def test_confirm_rate_exactly_0_2_no_low_confirm(self):
        # 2 confirm, 8 uncertain → rate=0.2, not < 0.2 → no LOW_CONFIRM_RATE
        entries = [_entry("CONFIRM")] * 2 + [_entry("UNCERTAIN")] * 8
        a = analyse_feedback(entries)
        if not (a["signals"]["attention_reason_code"] == ATTN_CRITICAL_DISAGREE
                or a["signals"]["attention_reason_code"] == ATTN_HIGH_DISAGREE_RATE):
            assert a["signals"]["attention_reason_code"] != ATTN_LOW_CONFIRM_RATE

    def test_no_attention_when_low_rate_but_no_valid_entries(self):
        # empty → valid_entries=0 → LOW_CONFIRM_RATE does not trigger
        a = analyse_feedback([])
        assert a["signals"]["attention_reason_code"] == ATTN_NONE


# ---------------------------------------------------------------------------
# 8. Corrupted / malformed entries
# ---------------------------------------------------------------------------

class TestCorrupted:
    def test_none_entries_skipped(self):
        entries = [None, _entry("CONFIRM"), None]
        a = analyse_feedback(entries)
        assert a["totals"]["confirm"] == 1
        assert a["totals"]["entries"] == 1

    def test_string_entries_skipped(self):
        entries = ["bad", _entry("DISAGREE"), "also bad"]
        a = analyse_feedback(entries)
        assert a["totals"]["disagree"] == 1

    def test_int_entries_skipped(self):
        entries = [42, _entry("UNCERTAIN")]
        a = analyse_feedback(entries)
        assert a["totals"]["uncertain"] == 1

    def test_empty_dict_treated_as_invalid(self):
        a = analyse_feedback([{}])
        # feedback_action="" → not CONFIRM/DISAGREE/UNCERTAIN → invalid count
        assert a["totals"]["invalid"] == 1
        assert a["totals"]["entries"] == 1

    def test_no_crash_all_corrupted(self):
        entries = [None, "bad", 42, [], True]
        a = analyse_feedback(entries)
        assert a["totals"]["entries"] == 0

    def test_missing_action_context_no_crash(self):
        entry = _entry("CONFIRM")
        del entry["action_context"]
        a = analyse_feedback([entry])
        assert a["totals"]["confirm"] == 1

    def test_non_dict_action_context_no_crash(self):
        entry = _entry("CONFIRM")
        entry["action_context"] = "bad"
        a = analyse_feedback([entry])
        assert a["totals"]["confirm"] == 1


# ---------------------------------------------------------------------------
# 9. load_feedback_log
# ---------------------------------------------------------------------------

class TestLoadFeedbackLog:
    def test_absent_file_returns_empty(self, tmp_path):
        assert load_feedback_log(tmp_path / "nonexistent.jsonl") == []

    def test_loads_valid_entries(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        e = _entry("CONFIRM")
        log.write_text(json.dumps(e) + "\n", encoding="utf-8")
        entries = load_feedback_log(log)
        assert len(entries) == 1
        assert entries[0]["feedback_action"] == "CONFIRM"

    def test_corrupted_lines_skipped(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        lines = [
            json.dumps(_entry("CONFIRM")),
            "this is not json {{",
            json.dumps(_entry("DISAGREE")),
        ]
        log.write_text("\n".join(lines) + "\n", encoding="utf-8")
        entries = load_feedback_log(log)
        assert len(entries) == 2

    def test_empty_lines_skipped(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        log.write_text(
            json.dumps(_entry("CONFIRM")) + "\n\n\n",
            encoding="utf-8"
        )
        entries = load_feedback_log(log)
        assert len(entries) == 1

    def test_multiple_entries_loaded(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        lines = [json.dumps(_entry(a)) for a in ("CONFIRM", "DISAGREE", "UNCERTAIN")]
        log.write_text("\n".join(lines) + "\n", encoding="utf-8")
        entries = load_feedback_log(log)
        assert len(entries) == 3


# ---------------------------------------------------------------------------
# 10. write_feedback_analysis
# ---------------------------------------------------------------------------

class TestWriteFeedbackAnalysis:
    def test_file_created(self, tmp_path):
        path = tmp_path / "analysis.json"
        a = analyse_feedback([])
        write_feedback_analysis(a, path)
        assert path.exists()

    def test_valid_json_written(self, tmp_path):
        path = tmp_path / "analysis.json"
        a = analyse_feedback([_entry("CONFIRM")])
        write_feedback_analysis(a, path)
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["version"] == VERSION

    def test_parent_dir_created(self, tmp_path):
        path = tmp_path / "nested" / "analysis.json"
        write_feedback_analysis(analyse_feedback([]), path)
        assert path.exists()


# ---------------------------------------------------------------------------
# 11. analyse_from_log integration
# ---------------------------------------------------------------------------

class TestAnalyseFromLog:
    def test_absent_log_returns_empty_analysis(self, tmp_path):
        a = analyse_from_log(log_path=tmp_path / "none.jsonl")
        assert a["totals"]["entries"] == 0
        assert a["version"] == VERSION

    def test_with_real_log(self, tmp_path):
        log = tmp_path / "feedback.jsonl"
        for action in ("CONFIRM", "CONFIRM", "DISAGREE"):
            log.write_text(
                log.read_text(encoding="utf-8") if log.exists() else "",
                encoding="utf-8"
            )
            with open(log, "a", encoding="utf-8") as f:
                f.write(json.dumps(_entry(action)) + "\n")
        a = analyse_from_log(log_path=log)
        assert a["totals"]["confirm"]  == 2
        assert a["totals"]["disagree"] == 1

    def test_write_output_false_no_file(self, tmp_path):
        out = tmp_path / "analysis.json"
        analyse_from_log(
            log_path=tmp_path / "none.jsonl",
            output_path=out,
            write_output=False,
        )
        assert not out.exists()

    def test_write_output_true_creates_file(self, tmp_path):
        out = tmp_path / "analysis.json"
        analyse_from_log(
            log_path=tmp_path / "none.jsonl",
            output_path=out,
            write_output=True,
        )
        assert out.exists()


# ---------------------------------------------------------------------------
# 12. Flags invariants
# ---------------------------------------------------------------------------

class TestFlagsInvariants:
    def test_flags_empty(self):
        _assert_flags(analyse_feedback([]))

    def test_flags_all_confirm(self):
        _assert_flags(analyse_feedback([_entry("CONFIRM")] * 5))

    def test_flags_all_disagree(self):
        _assert_flags(analyse_feedback([_entry("DISAGREE")] * 5))

    def test_flags_mixed(self):
        entries = [_entry("CONFIRM"), _entry("DISAGREE"), _entry("UNCERTAIN")]
        _assert_flags(analyse_feedback(entries))

    def test_flags_corrupted_entries(self):
        _assert_flags(analyse_feedback([None, "bad", {}]))


# ---------------------------------------------------------------------------
# 13. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_entries_same_output(self):
        entries = [_entry("CONFIRM")] * 3 + [_entry("DISAGREE")]
        a1 = analyse_feedback(entries)
        a2 = analyse_feedback(entries)
        a1.pop("ts_utc"); a2.pop("ts_utc")
        assert a1 == a2

    def test_empty_entries_deterministic(self):
        a1 = analyse_feedback([])
        a2 = analyse_feedback([])
        a1.pop("ts_utc"); a2.pop("ts_utc")
        assert a1 == a2

    def test_no_mutation_of_input_list(self):
        entries = [_entry("CONFIRM"), _entry("DISAGREE")]
        original_len = len(entries)
        original_0   = dict(entries[0])
        analyse_feedback(entries)
        assert len(entries) == original_len
        assert entries[0] == original_0
