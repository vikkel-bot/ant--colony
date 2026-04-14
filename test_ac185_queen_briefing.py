"""
AC-185: Queen Briefing — compact non-binding operator briefing.

Verifies:
  A. run() writes queen_briefing.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. attention_required_count taken from ops summary
  D. high/medium/low_priority_count taken from ops summary
  E. top_priority_summary is NONE when queue is empty
  F. top_priority_summary contains watch_status, market, signal_key, priority when set
  G. top_review_action is None when queue empty; set when top item present
  H. key_items_today contains up to MAX_KEY_ITEMS items from queue in order
  I. key_items_today capped at MAX_KEY_ITEMS (5) when queue has more
  J. each key_items_today entry has required compact fields
  K. operator_briefing_text is non-empty string with non-binding reminder
  L. operator_briefing_text mentions attention count when groups need attention
  M. operator_briefing_text says all normal when attention_count == 0
  N. operator_briefing_text says no data when total_groups == 0
  O. all required top-level fields present
  P. both inputs absent → ok=True, safe defaults
  Q. prior artifact files not modified
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_briefing import run, MAX_KEY_ITEMS

_NOW = "2026-04-14T10:00:00Z"

_REQUIRED_FIELDS = (
    "briefing_version", "briefing_type", "observational_only", "binding",
    "note", "generated_ts_utc", "source_lane",
    "attention_required_count", "high_priority_count",
    "medium_priority_count", "low_priority_count",
    "top_priority_summary", "top_review_action",
    "key_items_today", "operator_briefing_text",
)

_KEY_ITEM_FIELDS = (
    "market", "strategy_key", "signal_key",
    "priority", "watch_status", "review_action",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ops(tmp_path, lane, *,
              total_groups=2, attention=1, high=1, medium=0, low=0):
    ops = {
        "summary_version": "1", "summary_type": "queen_ops_summary",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_groups": total_groups,
        "attention_required_count": attention,
        "high_priority_count": high,
        "medium_priority_count": medium,
        "low_priority_count": low,
        "top_priority_item": None,
        "watched_groups": [],
        "operator_summary": "Test.",
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_ops_summary.json").write_text(json.dumps(ops), encoding="utf-8")
    return ops


def _make_queue_item(*, market="BNB-EUR", strategy_key="EDGE3",
                     signal_key="EDGE3_BREAKOUT_V2",
                     watch_status="WATCH_SIGNAL_DECAY",
                     priority="HIGH",
                     review_action="Review negative signal pattern and monitor next trades.",
                     operator_note="Note. Non-binding.") -> dict:
    return {
        "market": market, "strategy_key": strategy_key,
        "signal_key": signal_key, "watch_status": watch_status,
        "watch_flags": [watch_status], "priority": priority,
        "review_action": review_action, "operator_note": operator_note,
        "attention_required": True,
    }


def _make_queue(tmp_path, lane, items, *, high=0, medium=0, low=0):
    q = {
        "queue_version": "1", "queue_type": "queen_review_queue",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_items": len(items), "skipped_no_watch": 0,
        "high_count": high, "medium_count": medium, "low_count": low,
        "items": items,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_review_queue.json").write_text(json.dumps(q), encoding="utf-8")


def _load_briefing(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() writes file and returns ok=True
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    _make_queue(tmp_path, lane, [_make_queue_item()], high=1)
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    _make_queue(tmp_path, lane, [_make_queue_item()], high=1)
    result = run(str(tmp_path), lane)
    b = _load_briefing(result)
    assert b["observational_only"] is True
    assert b["binding"] is False


# ---------------------------------------------------------------------------
# C. attention_required_count from ops summary
# ---------------------------------------------------------------------------

def test_C_attention_count(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, attention=3)
    _make_queue(tmp_path, lane, [], high=0, medium=0, low=0)
    result = run(str(tmp_path), lane)
    assert _load_briefing(result)["attention_required_count"] == 3


# ---------------------------------------------------------------------------
# D. high/medium/low counts from ops summary
# ---------------------------------------------------------------------------

def test_D_priority_counts(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, high=2, medium=1, low=3)
    _make_queue(tmp_path, lane, [], high=2, medium=1, low=3)
    result = run(str(tmp_path), lane)
    b = _load_briefing(result)
    assert b["high_priority_count"]   == 2
    assert b["medium_priority_count"] == 1
    assert b["low_priority_count"]    == 3


# ---------------------------------------------------------------------------
# E. top_priority_summary = NONE when queue empty
# ---------------------------------------------------------------------------

def test_E_top_summary_none_when_empty(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, attention=0, high=0)
    _make_queue(tmp_path, lane, [])
    result = run(str(tmp_path), lane)
    assert _load_briefing(result)["top_priority_summary"] == "NONE"


# ---------------------------------------------------------------------------
# F. top_priority_summary contains watch_status, market, signal, priority
# ---------------------------------------------------------------------------

def test_F_top_summary_content(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    item = _make_queue_item(market="BNB-EUR", signal_key="SK1",
                            watch_status="WATCH_SIGNAL_DECAY", priority="HIGH")
    _make_queue(tmp_path, lane, [item], high=1)
    result = run(str(tmp_path), lane)
    s = _load_briefing(result)["top_priority_summary"]
    assert "WATCH_SIGNAL_DECAY" in s
    assert "BNB-EUR" in s
    assert "SK1" in s
    assert "HIGH" in s


# ---------------------------------------------------------------------------
# G. top_review_action
# ---------------------------------------------------------------------------

def test_G_top_review_action_none_when_empty(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, attention=0)
    _make_queue(tmp_path, lane, [])
    result = run(str(tmp_path), lane)
    assert _load_briefing(result)["top_review_action"] is None


def test_G_top_review_action_set(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    item = _make_queue_item(review_action="Review negative signal pattern and monitor next trades.")
    _make_queue(tmp_path, lane, [item], high=1)
    result = run(str(tmp_path), lane)
    action = _load_briefing(result)["top_review_action"]
    assert action is not None and len(action) > 0


# ---------------------------------------------------------------------------
# H. key_items_today contains queue items in order
# ---------------------------------------------------------------------------

def test_H_key_items_order(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, attention=2, high=1, low=1)
    items = [
        _make_queue_item(signal_key="SK_HIGH", priority="HIGH",
                         watch_status="WATCH_SIGNAL_DECAY",
                         review_action="Review signal."),
        _make_queue_item(signal_key="SK_LOW",  priority="LOW",
                         watch_status="WATCH_SAMPLE",
                         review_action="Collect sample."),
    ]
    _make_queue(tmp_path, lane, items, high=1, low=1)
    result = run(str(tmp_path), lane)
    ki = _load_briefing(result)["key_items_today"]
    assert len(ki) == 2
    assert ki[0]["signal_key"] == "SK_HIGH"
    assert ki[1]["signal_key"] == "SK_LOW"


# ---------------------------------------------------------------------------
# I. key_items_today capped at MAX_KEY_ITEMS
# ---------------------------------------------------------------------------

def test_I_key_items_capped(tmp_path):
    lane = "live_test"
    n = MAX_KEY_ITEMS + 3
    _make_ops(tmp_path, lane, attention=n, high=n)
    items = [
        _make_queue_item(signal_key=f"SK{i}", priority="HIGH",
                         review_action="Review signal.")
        for i in range(n)
    ]
    _make_queue(tmp_path, lane, items, high=n)
    result = run(str(tmp_path), lane)
    ki = _load_briefing(result)["key_items_today"]
    assert len(ki) == MAX_KEY_ITEMS


# ---------------------------------------------------------------------------
# J. key_items_today entries have required fields
# ---------------------------------------------------------------------------

def test_J_key_item_fields(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    _make_queue(tmp_path, lane, [_make_queue_item()], high=1)
    result = run(str(tmp_path), lane)
    item = _load_briefing(result)["key_items_today"][0]
    for field in _KEY_ITEM_FIELDS:
        assert field in item, f"missing key item field: {field}"


# ---------------------------------------------------------------------------
# K. operator_briefing_text non-empty with non-binding reminder
# ---------------------------------------------------------------------------

def test_K_briefing_text_non_binding(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    _make_queue(tmp_path, lane, [_make_queue_item()], high=1)
    result = run(str(tmp_path), lane)
    text = _load_briefing(result)["operator_briefing_text"]
    assert isinstance(text, str) and len(text) > 0
    assert "non-binding" in text.lower()


# ---------------------------------------------------------------------------
# L. operator_briefing_text mentions attention count when needed
# ---------------------------------------------------------------------------

def test_L_briefing_text_mentions_attention(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, total_groups=4, attention=2, high=2)
    _make_queue(tmp_path, lane, [
        _make_queue_item(signal_key="SK1"),
        _make_queue_item(signal_key="SK2"),
    ], high=2)
    result = run(str(tmp_path), lane)
    text = _load_briefing(result)["operator_briefing_text"]
    assert "2" in text


# ---------------------------------------------------------------------------
# M. operator_briefing_text says all normal when attention_count == 0
# ---------------------------------------------------------------------------

def test_M_briefing_text_all_normal(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, total_groups=3, attention=0, high=0)
    _make_queue(tmp_path, lane, [])
    result = run(str(tmp_path), lane)
    text = _load_briefing(result)["operator_briefing_text"]
    assert "normal" in text.lower() or "no action" in text.lower()


# ---------------------------------------------------------------------------
# N. operator_briefing_text says no data when total_groups == 0
# ---------------------------------------------------------------------------

def test_N_briefing_text_no_data(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane, total_groups=0, attention=0, high=0)
    _make_queue(tmp_path, lane, [])
    result = run(str(tmp_path), lane)
    text = _load_briefing(result)["operator_briefing_text"]
    assert "no" in text.lower()


# ---------------------------------------------------------------------------
# O. all required top-level fields present
# ---------------------------------------------------------------------------

def test_O_required_fields(tmp_path):
    lane = "live_test"
    _make_ops(tmp_path, lane)
    _make_queue(tmp_path, lane, [_make_queue_item()], high=1)
    result = run(str(tmp_path), lane)
    b = _load_briefing(result)
    for field in _REQUIRED_FIELDS:
        assert field in b, f"missing required field: {field}"


# ---------------------------------------------------------------------------
# P. both inputs absent → ok=True, safe defaults
# ---------------------------------------------------------------------------

def test_P_no_inputs(tmp_path):
    lane = "live_test"
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    b = _load_briefing(result)
    assert b["attention_required_count"] == 0
    assert b["top_priority_summary"] == "NONE"
    assert b["top_review_action"] is None
    assert b["key_items_today"] == []


# ---------------------------------------------------------------------------
# Q. prior artifact files not modified
# ---------------------------------------------------------------------------

def test_Q_prior_artifacts_unchanged(tmp_path):
    lane = "live_test"
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)

    ops_text = json.dumps({
        "summary_version": "1", "summary_type": "queen_ops_summary",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_groups": 1, "attention_required_count": 1,
        "high_priority_count": 1, "medium_priority_count": 0, "low_priority_count": 0,
        "top_priority_item": None, "watched_groups": [], "operator_summary": "x",
    })
    q_text = json.dumps({
        "queue_version": "1", "queue_type": "queen_review_queue",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_items": 1, "skipped_no_watch": 0,
        "high_count": 1, "medium_count": 0, "low_count": 0,
        "items": [_make_queue_item()],
    })
    (lane_dir / "queen_ops_summary.json").write_text(ops_text, encoding="utf-8")
    (lane_dir / "queen_review_queue.json").write_text(q_text, encoding="utf-8")

    run(str(tmp_path), lane)

    assert (lane_dir / "queen_ops_summary.json").read_text(encoding="utf-8") == ops_text
    assert (lane_dir / "queen_review_queue.json").read_text(encoding="utf-8") == q_text
