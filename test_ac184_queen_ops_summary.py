"""
AC-184: Queen Ops Summary — compact operator-facing overview of Queen state.

Verifies:
  A. run() writes queen_ops_summary.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. total_groups reflects watchlist group count
  D. attention_required_count reflects watchlist attention flags
  E. high/medium/low priority counts come from review queue
  F. top_priority_item is the first item from the review queue (highest priority)
  G. watched_groups lists only attention_required=True groups, compactly
  H. watched_groups entries contain market, strategy_key, signal_key, watch_status
  I. priority in watched_groups matches review queue priority for the group
  J. operator_summary is non-empty string with non-binding reminder
  K. operator_summary mentions attention count when groups need attention
  L. operator_summary says all normal when no groups need attention
  M. operator_summary says no data when watchlist is empty
  N. both inputs absent → ok=True, zero counts, valid artifact
  O. all required top-level fields present
  P. prior artifact files not modified
  Q. top_priority_item is None when review queue is empty
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_ops_summary import run

_NOW = "2026-04-14T10:00:00Z"

_REQUIRED_TOP_FIELDS = (
    "summary_version", "summary_type", "observational_only", "binding",
    "note", "generated_ts_utc", "source_lane",
    "total_groups", "attention_required_count",
    "high_priority_count", "medium_priority_count", "low_priority_count",
    "top_priority_item", "watched_groups", "operator_summary",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wl_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    watch_status="WATCH_SAMPLE",
    attention_required=True,
) -> dict:
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "watch_status": watch_status,
        "watch_flags": [watch_status] if attention_required else [],
        "watch_reasons": ["Reason."] if attention_required else [],
        "attention_required": attention_required,
    }


def _make_queue_item(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    watch_status="WATCH_SAMPLE",
    priority="LOW",
    review_action="Collect more sample before drawing conclusions.",
    operator_note="Test note. Non-binding.",
) -> dict:
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "watch_status": watch_status,
        "watch_flags": [watch_status],
        "priority": priority,
        "review_action": review_action,
        "operator_note": operator_note,
        "attention_required": True,
    }


def _write_watchlist(tmp_path, lane, groups):
    wl = {
        "watchlist_version": "1", "watchlist_type": "queen_watchlist",
        "observational_only": True, "binding": False, "note": "Non-binding.",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_groups": len(groups),
        "attention_required_count": sum(1 for g in groups if g.get("attention_required")),
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_watchlist.json").write_text(json.dumps(wl), encoding="utf-8")


def _write_queue(tmp_path, lane, items, *, high=0, medium=0, low=0):
    q = {
        "queue_version": "1", "queue_type": "queen_review_queue",
        "observational_only": True, "binding": False, "note": "Non-binding.",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_items": len(items),
        "skipped_no_watch": 0,
        "high_count": high, "medium_count": medium, "low_count": low,
        "items": items,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_review_queue.json").write_text(json.dumps(q), encoding="utf-8")


def _load_summary(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() writes file and returns ok=True
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [_make_queue_item()], low=1)
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding_flags(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [_make_queue_item()], low=1)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)
    assert s["observational_only"] is True
    assert s["binding"] is False


# ---------------------------------------------------------------------------
# C. total_groups from watchlist
# ---------------------------------------------------------------------------

def test_C_total_groups(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1"),
        _make_wl_group(signal_key="SK2"),
        _make_wl_group(signal_key="SK3", attention_required=False,
                       watch_status="NO_WATCH"),
    ])
    _write_queue(tmp_path, lane, [], high=0, medium=0, low=0)
    result = run(str(tmp_path), lane)
    assert _load_summary(result)["total_groups"] == 3


# ---------------------------------------------------------------------------
# D. attention_required_count from watchlist
# ---------------------------------------------------------------------------

def test_D_attention_count(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1", attention_required=True),
        _make_wl_group(signal_key="SK2", attention_required=False,
                       watch_status="NO_WATCH"),
    ])
    _write_queue(tmp_path, lane, [_make_queue_item(signal_key="SK1")], low=1)
    result = run(str(tmp_path), lane)
    assert _load_summary(result)["attention_required_count"] == 1


# ---------------------------------------------------------------------------
# E. priority counts from review queue
# ---------------------------------------------------------------------------

def test_E_priority_counts(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [
        _make_queue_item(signal_key="SK1",
                         watch_status="WATCH_SIGNAL_DECAY", priority="HIGH",
                         review_action="Review negative signal pattern."),
        _make_queue_item(signal_key="SK2",
                         watch_status="WATCH_LATENCY", priority="MEDIUM",
                         review_action="Review latency."),
        _make_queue_item(signal_key="SK3",
                         watch_status="WATCH_SAMPLE", priority="LOW",
                         review_action="Collect more sample."),
    ], high=1, medium=1, low=1)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)
    assert s["high_priority_count"]   == 1
    assert s["medium_priority_count"] == 1
    assert s["low_priority_count"]    == 1


# ---------------------------------------------------------------------------
# F. top_priority_item is first item from review queue
# ---------------------------------------------------------------------------

def test_F_top_priority_item(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    top = _make_queue_item(signal_key="TOP",
                           watch_status="WATCH_SIGNAL_DECAY", priority="HIGH",
                           review_action="Review negative signal pattern.")
    _write_queue(tmp_path, lane, [top,
        _make_queue_item(signal_key="LOWER", watch_status="WATCH_SAMPLE",
                         priority="LOW", review_action="Collect more sample.")
    ], high=1, low=1)
    result = run(str(tmp_path), lane)
    top_item = _load_summary(result)["top_priority_item"]
    assert top_item["signal_key"]   == "TOP"
    assert top_item["watch_status"] == "WATCH_SIGNAL_DECAY"


# ---------------------------------------------------------------------------
# G. watched_groups contains only attention_required=True groups
# ---------------------------------------------------------------------------

def test_G_watched_groups_only_attention(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1", attention_required=True),
        _make_wl_group(signal_key="SK2", attention_required=False,
                       watch_status="NO_WATCH"),
    ])
    _write_queue(tmp_path, lane, [_make_queue_item(signal_key="SK1")], low=1)
    result = run(str(tmp_path), lane)
    wg = _load_summary(result)["watched_groups"]
    assert len(wg) == 1
    assert wg[0]["signal_key"] == "SK1"


# ---------------------------------------------------------------------------
# H. watched_groups entries have required compact fields
# ---------------------------------------------------------------------------

def test_H_watched_groups_fields(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [_make_queue_item()], low=1)
    result = run(str(tmp_path), lane)
    entry = _load_summary(result)["watched_groups"][0]
    for field in ("market", "strategy_key", "signal_key", "watch_status"):
        assert field in entry, f"missing field: {field}"


# ---------------------------------------------------------------------------
# I. priority in watched_groups matches review queue priority
# ---------------------------------------------------------------------------

def test_I_watched_group_priority(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1", watch_status="WATCH_SIGNAL_DECAY"),
    ])
    _write_queue(tmp_path, lane, [
        _make_queue_item(signal_key="SK1", watch_status="WATCH_SIGNAL_DECAY",
                         priority="HIGH", review_action="Review signal."),
    ], high=1)
    result = run(str(tmp_path), lane)
    wg = _load_summary(result)["watched_groups"]
    assert wg[0]["priority"] == "HIGH"


# ---------------------------------------------------------------------------
# J. operator_summary non-empty with non-binding reminder
# ---------------------------------------------------------------------------

def test_J_operator_summary_non_binding(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [_make_queue_item()], low=1)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)["operator_summary"]
    assert isinstance(s, str) and len(s) > 0
    assert "non-binding" in s.lower()


# ---------------------------------------------------------------------------
# K. operator_summary mentions attention count
# ---------------------------------------------------------------------------

def test_K_operator_summary_mentions_attention(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1", attention_required=True),
        _make_wl_group(signal_key="SK2", attention_required=True),
    ])
    _write_queue(tmp_path, lane, [
        _make_queue_item(signal_key="SK1"),
        _make_queue_item(signal_key="SK2"),
    ], low=2)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)["operator_summary"]
    assert "2" in s   # attention count mentioned


# ---------------------------------------------------------------------------
# L. operator_summary says all normal when no attention needed
# ---------------------------------------------------------------------------

def test_L_operator_summary_all_normal(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_wl_group(signal_key="SK1", attention_required=False,
                       watch_status="NO_WATCH"),
    ])
    _write_queue(tmp_path, lane, [], high=0, medium=0, low=0)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)["operator_summary"]
    assert "normal" in s.lower() or "no operator" in s.lower()


# ---------------------------------------------------------------------------
# M. operator_summary says no data when watchlist empty
# ---------------------------------------------------------------------------

def test_M_operator_summary_no_data(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [])
    _write_queue(tmp_path, lane, [])
    result = run(str(tmp_path), lane)
    s = _load_summary(result)["operator_summary"]
    assert "no data" in s.lower() or "no groups" in s.lower()


# ---------------------------------------------------------------------------
# N. both inputs absent → ok=True, zero counts
# ---------------------------------------------------------------------------

def test_N_no_inputs(tmp_path):
    lane = "live_test"
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    s = _load_summary(result)
    assert s["total_groups"] == 0
    assert s["attention_required_count"] == 0


# ---------------------------------------------------------------------------
# O. all required top-level fields present
# ---------------------------------------------------------------------------

def test_O_required_fields(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [_make_queue_item()], low=1)
    result = run(str(tmp_path), lane)
    s = _load_summary(result)
    for field in _REQUIRED_TOP_FIELDS:
        assert field in s, f"missing top-level field: {field}"


# ---------------------------------------------------------------------------
# P. prior artifact files not modified
# ---------------------------------------------------------------------------

def test_P_prior_artifacts_unchanged(tmp_path):
    lane = "live_test"
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)

    wl_text = json.dumps({
        "watchlist_version": "1", "watchlist_type": "queen_watchlist",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_groups": 1, "attention_required_count": 1,
        "groups": [_make_wl_group()],
    })
    q_text = json.dumps({
        "queue_version": "1", "queue_type": "queen_review_queue",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_items": 1, "skipped_no_watch": 0,
        "high_count": 0, "medium_count": 0, "low_count": 1,
        "items": [_make_queue_item()],
    })
    (lane_dir / "queen_watchlist.json").write_text(wl_text, encoding="utf-8")
    (lane_dir / "queen_review_queue.json").write_text(q_text, encoding="utf-8")

    run(str(tmp_path), lane)

    assert (lane_dir / "queen_watchlist.json").read_text(encoding="utf-8") == wl_text
    assert (lane_dir / "queen_review_queue.json").read_text(encoding="utf-8") == q_text


# ---------------------------------------------------------------------------
# Q. top_priority_item is None when review queue is empty
# ---------------------------------------------------------------------------

def test_Q_top_item_none_when_queue_empty(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_wl_group()])
    _write_queue(tmp_path, lane, [], high=0, medium=0, low=0)
    result = run(str(tmp_path), lane)
    assert _load_summary(result)["top_priority_item"] is None
