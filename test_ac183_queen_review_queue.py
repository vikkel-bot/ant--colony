"""
AC-183: Queen Review Queue — non-binding operator review items from watchlist.

Verifies:
  A. run() writes queen_review_queue.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. only groups with attention_required=True generate review items
  D. groups with attention_required=False are excluded (counted in skipped_no_watch)
  E. WATCH_SIGNAL_DECAY maps to priority=HIGH
  F. WATCH_SLIPPAGE maps to priority=HIGH
  G. WATCH_LATENCY maps to priority=MEDIUM
  H. WATCH_REGIME_SHIFT maps to priority=MEDIUM
  I. WATCH_SAMPLE maps to priority=LOW
  J. each watch_status maps to the correct review_action sentence
  K. items are sorted HIGH → MEDIUM → LOW, then by (market, strategy_key, signal_key)
  L. operator_note is non-empty and contains non-binding reminder
  M. high_count / medium_count / low_count totals are correct
  N. total_items equals len(items)
  O. all required per-item fields present
  P. watchlist absent → ok=True, zero items
  Q. prior artifact files not modified
  R. items with multiple flags get all flags in watch_flags list
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_review_queue import build_review_item, run

_NOW = "2026-04-14T10:00:00Z"

_REQUIRED_ITEM_FIELDS = (
    "market", "strategy_key", "signal_key",
    "watch_status", "watch_flags", "priority",
    "review_action", "operator_note", "attention_required",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_watch_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    watch_status="WATCH_SAMPLE",
    watch_flags=None,
    watch_reasons=None,
    attention_required=True,
    trades_count=3,
) -> dict:
    if watch_flags is None:
        watch_flags = [watch_status] if attention_required else []
    if watch_reasons is None:
        watch_reasons = [f"Reason for {watch_status}."] if attention_required else []
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "watch_status": watch_status,
        "watch_flags": watch_flags,
        "watch_reasons": watch_reasons,
        "attention_required": attention_required,
        "trades_count": trades_count,
    }


def _write_watchlist(tmp_path, lane, groups):
    wl = {
        "watchlist_version": "1",
        "watchlist_type": "queen_watchlist",
        "observational_only": True,
        "binding": False,
        "note": "Non-binding.",
        "generated_ts_utc": _NOW,
        "source_lane": lane,
        "total_groups": len(groups),
        "attention_required_count": sum(1 for g in groups if g.get("attention_required")),
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_watchlist.json").write_text(json.dumps(wl), encoding="utf-8")


def _load_queue(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() writes file and returns ok=True
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group()])
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding_flags(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group()])
    result = run(str(tmp_path), lane)
    q = _load_queue(result)
    assert q["observational_only"] is True
    assert q["binding"] is False


# ---------------------------------------------------------------------------
# C. only attention_required=True groups become items
# ---------------------------------------------------------------------------

def test_C_only_watched_groups_included(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(signal_key="SK1", attention_required=True),
        _make_watch_group(signal_key="SK2", attention_required=False,
                          watch_status="NO_WATCH", watch_flags=[], watch_reasons=[]),
    ])
    result = run(str(tmp_path), lane)
    q = _load_queue(result)
    assert q["total_items"] == 1
    assert q["items"][0]["signal_key"] == "SK1"


# ---------------------------------------------------------------------------
# D. attention_required=False groups counted in skipped_no_watch
# ---------------------------------------------------------------------------

def test_D_skipped_count(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(signal_key="SK1", attention_required=True),
        _make_watch_group(signal_key="SK2", attention_required=False,
                          watch_status="NO_WATCH", watch_flags=[], watch_reasons=[]),
        _make_watch_group(signal_key="SK3", attention_required=False,
                          watch_status="NO_WATCH", watch_flags=[], watch_reasons=[]),
    ])
    result = run(str(tmp_path), lane)
    q = _load_queue(result)
    assert q["skipped_no_watch"] == 2


# ---------------------------------------------------------------------------
# E–I. Priority mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("watch_status,expected_priority", [
    ("WATCH_SIGNAL_DECAY", "HIGH"),
    ("WATCH_SLIPPAGE",     "HIGH"),
    ("WATCH_LATENCY",      "MEDIUM"),
    ("WATCH_REGIME_SHIFT", "MEDIUM"),
    ("WATCH_SAMPLE",       "LOW"),
])
def test_EI_priority_mapping(tmp_path, watch_status, expected_priority):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group(watch_status=watch_status)])
    result = run(str(tmp_path), lane)
    item = _load_queue(result)["items"][0]
    assert item["priority"] == expected_priority


# ---------------------------------------------------------------------------
# J. review_action sentences
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("watch_status,expected_fragment", [
    ("WATCH_SIGNAL_DECAY", "signal pattern"),
    ("WATCH_SLIPPAGE",     "execution cost"),
    ("WATCH_LATENCY",      "latency"),
    ("WATCH_REGIME_SHIFT", "market context"),
    ("WATCH_SAMPLE",       "more sample"),
])
def test_J_review_action_content(tmp_path, watch_status, expected_fragment):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group(watch_status=watch_status)])
    result = run(str(tmp_path), lane)
    action = _load_queue(result)["items"][0]["review_action"]
    assert expected_fragment.lower() in action.lower()


# ---------------------------------------------------------------------------
# K. items sorted HIGH → MEDIUM → LOW, then alphabetically within band
# ---------------------------------------------------------------------------

def test_K_sort_order(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(signal_key="LOW_1",    watch_status="WATCH_SAMPLE"),
        _make_watch_group(signal_key="HIGH_B",   watch_status="WATCH_SLIPPAGE"),
        _make_watch_group(signal_key="MEDIUM_1", watch_status="WATCH_LATENCY"),
        _make_watch_group(signal_key="HIGH_A",   watch_status="WATCH_SIGNAL_DECAY"),
    ])
    result = run(str(tmp_path), lane)
    items = _load_queue(result)["items"]
    priorities = [i["priority"] for i in items]
    # All HIGHs before MEDIUMs before LOWs
    seen_medium = False
    seen_low = False
    for p in priorities:
        if p == "MEDIUM":
            seen_medium = True
        if p == "LOW":
            seen_low = True
        if p == "HIGH":
            assert not seen_medium and not seen_low, "HIGH after MEDIUM or LOW"
        if p == "MEDIUM":
            assert not seen_low, "MEDIUM after LOW"


def test_K_alphabetical_within_band(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(market="ZZZ-EUR", signal_key="SK1", watch_status="WATCH_SAMPLE"),
        _make_watch_group(market="AAA-EUR", signal_key="SK1", watch_status="WATCH_SAMPLE"),
    ])
    result = run(str(tmp_path), lane)
    items = _load_queue(result)["items"]
    assert items[0]["market"] == "AAA-EUR"
    assert items[1]["market"] == "ZZZ-EUR"


# ---------------------------------------------------------------------------
# L. operator_note non-empty and contains non-binding reminder
# ---------------------------------------------------------------------------

def test_L_operator_note_non_binding(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group()])
    result = run(str(tmp_path), lane)
    note = _load_queue(result)["items"][0]["operator_note"]
    assert isinstance(note, str) and len(note) > 0
    assert "non-binding" in note.lower()


# ---------------------------------------------------------------------------
# M. high_count / medium_count / low_count correct
# ---------------------------------------------------------------------------

def test_M_priority_counts(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(signal_key="SK1", watch_status="WATCH_SIGNAL_DECAY"),
        _make_watch_group(signal_key="SK2", watch_status="WATCH_SLIPPAGE"),
        _make_watch_group(signal_key="SK3", watch_status="WATCH_LATENCY"),
        _make_watch_group(signal_key="SK4", watch_status="WATCH_SAMPLE"),
    ])
    result = run(str(tmp_path), lane)
    q = _load_queue(result)
    assert q["high_count"]   == 2
    assert q["medium_count"] == 1
    assert q["low_count"]    == 1


# ---------------------------------------------------------------------------
# N. total_items == len(items)
# ---------------------------------------------------------------------------

def test_N_total_items_consistent(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [
        _make_watch_group(signal_key="SK1"),
        _make_watch_group(signal_key="SK2"),
    ])
    result = run(str(tmp_path), lane)
    q = _load_queue(result)
    assert q["total_items"] == len(q["items"])


# ---------------------------------------------------------------------------
# O. required per-item fields present
# ---------------------------------------------------------------------------

def test_O_required_item_fields(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group()])
    result = run(str(tmp_path), lane)
    item = _load_queue(result)["items"][0]
    for field in _REQUIRED_ITEM_FIELDS:
        assert field in item, f"missing required field: {field}"


# ---------------------------------------------------------------------------
# P. watchlist absent → ok=True, zero items
# ---------------------------------------------------------------------------

def test_P_no_watchlist(tmp_path):
    lane = "live_test"
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    q = _load_queue(result)
    assert q["total_items"] == 0
    assert q["items"] == []


# ---------------------------------------------------------------------------
# Q. prior artifact files not modified
# ---------------------------------------------------------------------------

def test_Q_prior_artifacts_unchanged(tmp_path):
    lane = "live_test"
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)

    wl_text = json.dumps({
        "watchlist_version": "1", "watchlist_type": "queen_watchlist",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "total_groups": 1, "attention_required_count": 1,
        "groups": [_make_watch_group()],
    })
    (lane_dir / "queen_watchlist.json").write_text(wl_text, encoding="utf-8")
    # also plant earlier artifacts that must not change
    learning_text = '{"summary_version":"1","groups":[]}'
    (lane_dir / "queen_learning_summary.json").write_text(learning_text, encoding="utf-8")

    run(str(tmp_path), lane)

    assert (lane_dir / "queen_watchlist.json").read_text(encoding="utf-8") == wl_text
    assert (lane_dir / "queen_learning_summary.json").read_text(encoding="utf-8") == learning_text


# ---------------------------------------------------------------------------
# R. multiple flags preserved in item watch_flags
# ---------------------------------------------------------------------------

def test_R_multiple_flags_in_item(tmp_path):
    lane = "live_test"
    _write_watchlist(tmp_path, lane, [_make_watch_group(
        watch_status="WATCH_SIGNAL_DECAY",
        watch_flags=["WATCH_SIGNAL_DECAY", "WATCH_SLIPPAGE", "WATCH_SAMPLE"],
        watch_reasons=["Reason A.", "Reason B.", "Reason C."],
    )])
    result = run(str(tmp_path), lane)
    item = _load_queue(result)["items"][0]
    assert item["watch_flags"] == ["WATCH_SIGNAL_DECAY", "WATCH_SLIPPAGE", "WATCH_SAMPLE"]
