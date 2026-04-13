"""
AC-181: Queen Advisory Delta — non-binding trend comparison between snapshots.

Verifies:
  A. run() writes queen_advisory_delta.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. comparison_status=FIRST_SNAPSHOT when no previous advisory exists
  D. comparison_status=COMPARED when previous snapshot exists
  E. sample_size_trend=NEW for groups not in previous snapshot
  F. sample_size_trend=GROWING when trades_count increased
  G. sample_size_trend=SHRINKING when trades_count decreased
  H. sample_size_trend=UNCHANGED when trades_count identical
  I. slippage_trend=IMPROVING when slippage decreased beyond threshold
  J. slippage_trend=WORSENING when slippage increased beyond threshold
  K. slippage_trend=STABLE when slippage change is within threshold
  L. latency_trend=IMPROVING when latency decreased beyond threshold
  M. latency_trend=WORSENING when latency increased beyond threshold
  N. latency_trend=STABLE when latency change is within threshold
  O. signal_observation_trend=IMPROVED for NEGATIVE→POSITIVE movement
  P. signal_observation_trend=DEGRADED for POSITIVE→NEGATIVE movement
  Q. signal_observation_trend=UNCHANGED when identical
  R. advisory_change_note is non-empty string with non-binding reminder
  S. groups_new_count / groups_removed_count / groups_compared_count correct
  T. missing current advisory → ok=True, FIRST_SNAPSHOT, zero groups
  U. AC-179 and AC-180 output files are not modified by delta run
  V. current advisory saved as queen_advisory_prev.json after run
  W. all original advisory group fields preserved in delta groups
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_advisory_delta import (
    diff_group,
    build_delta,
    run,
    SLIPPAGE_THRESHOLD,
    LATENCY_THRESHOLD_MS,
)

_NOW = "2026-04-13T10:00:00Z"
_PREV_NOW = "2026-04-13T09:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_advisory_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    trades_count=5,
    win_count=3,
    loss_count=2,
    flat_count=0,
    queen_action_required_count=0,
    signal_observation="NEUTRAL_SIGNAL",
    sample_size_status="MINIMAL",
    execution_quality_status="CLEAN",
    avg_slippage_vs_expected_eur=0.12,
    avg_entry_latency_ms=95.0,
    avg_signal_strength=0.70,
    last_market_regime="BULL",
    last_volatility="MID",
    advisory_note="Test advisory note. This note is non-binding.",
) -> dict:
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "trades_count": trades_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "queen_action_required_count": queen_action_required_count,
        "signal_observation": signal_observation,
        "sample_size_status": sample_size_status,
        "execution_quality_status": execution_quality_status,
        "avg_slippage_vs_expected_eur": avg_slippage_vs_expected_eur,
        "avg_entry_latency_ms": avg_entry_latency_ms,
        "avg_signal_strength": avg_signal_strength,
        "last_market_regime": last_market_regime,
        "last_volatility": last_volatility,
        "advisory_note": advisory_note,
    }


def _write_advisory(tmp_path, lane, groups, filename="queen_advisory_summary.json", ts=_NOW):
    advisory = {
        "advisory_version": "1",
        "advisory_type": "queen_advisory_summary",
        "observational_only": True,
        "binding": False,
        "note": "Non-binding advisory summary.",
        "generated_ts_utc": ts,
        "source_lane": lane,
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / filename).write_text(json.dumps(advisory), encoding="utf-8")
    return advisory


def _load_delta(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() writes file and returns ok=True
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding_flags(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    result = run(str(tmp_path), lane)
    delta = _load_delta(result)
    assert delta["observational_only"] is True
    assert delta["binding"] is False


# ---------------------------------------------------------------------------
# C. FIRST_SNAPSHOT when no previous advisory exists
# ---------------------------------------------------------------------------

def test_C_first_snapshot_status(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    # no queen_advisory_prev.json
    result = run(str(tmp_path), lane)
    assert _load_delta(result)["comparison_status"] == "FIRST_SNAPSHOT"


# ---------------------------------------------------------------------------
# D. COMPARED when previous snapshot exists
# ---------------------------------------------------------------------------

def test_D_compared_status(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()], "queen_advisory_summary.json")
    _write_advisory(tmp_path, lane, [_make_advisory_group()], "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    assert _load_delta(result)["comparison_status"] == "COMPARED"


# ---------------------------------------------------------------------------
# E. sample_size_trend=NEW for groups not in previous
# ---------------------------------------------------------------------------

def test_E_new_group_trend(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(signal_key="SK_NEW")])
    _write_advisory(tmp_path, lane, [_make_advisory_group(signal_key="SK_OLD")],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["sample_size_trend"] == "NEW"


# ---------------------------------------------------------------------------
# F. sample_size_trend=GROWING
# ---------------------------------------------------------------------------

def test_F_growing_sample(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=8)])
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=5)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["sample_size_trend"] == "GROWING"
    assert g["sample_size_delta"] == 3


# ---------------------------------------------------------------------------
# G. sample_size_trend=SHRINKING
# ---------------------------------------------------------------------------

def test_G_shrinking_sample(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=3)])
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=5)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["sample_size_trend"] == "SHRINKING"
    assert g["sample_size_delta"] == -2


# ---------------------------------------------------------------------------
# H. sample_size_trend=UNCHANGED
# ---------------------------------------------------------------------------

def test_H_unchanged_sample(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=5)])
    _write_advisory(tmp_path, lane, [_make_advisory_group(trades_count=5)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["sample_size_trend"] == "UNCHANGED"
    assert g["sample_size_delta"] == 0


# ---------------------------------------------------------------------------
# I. slippage_trend=IMPROVING
# ---------------------------------------------------------------------------

def test_I_slippage_improving(tmp_path):
    lane = "live_test"
    # decreased by more than threshold
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.05)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.12)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["slippage_trend"] == "IMPROVING"


# ---------------------------------------------------------------------------
# J. slippage_trend=WORSENING
# ---------------------------------------------------------------------------

def test_J_slippage_worsening(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.20)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.12)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["slippage_trend"] == "WORSENING"


# ---------------------------------------------------------------------------
# K. slippage_trend=STABLE (within threshold)
# ---------------------------------------------------------------------------

def test_K_slippage_stable(tmp_path):
    lane = "live_test"
    # change = 0.002 < 0.005 threshold
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.122)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_slippage_vs_expected_eur=0.120)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["slippage_trend"] == "STABLE"


# ---------------------------------------------------------------------------
# L. latency_trend=IMPROVING
# ---------------------------------------------------------------------------

def test_L_latency_improving(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=80.0)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=95.0)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["latency_trend"] == "IMPROVING"


# ---------------------------------------------------------------------------
# M. latency_trend=WORSENING
# ---------------------------------------------------------------------------

def test_M_latency_worsening(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=120.0)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=95.0)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["latency_trend"] == "WORSENING"


# ---------------------------------------------------------------------------
# N. latency_trend=STABLE (within threshold)
# ---------------------------------------------------------------------------

def test_N_latency_stable(tmp_path):
    lane = "live_test"
    # change = 3 ms < 5 ms threshold
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=98.0)])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(avg_entry_latency_ms=95.0)],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["latency_trend"] == "STABLE"


# ---------------------------------------------------------------------------
# O. signal_observation_trend=IMPROVED
# ---------------------------------------------------------------------------

def test_O_signal_improved(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="POSITIVE_SIGNAL")])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEGATIVE_SIGNAL")],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["signal_observation_trend"] == "IMPROVED"


def test_O_signal_improved_neutral_to_positive(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="POSITIVE_SIGNAL")])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEUTRAL_SIGNAL")],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["signal_observation_trend"] == "IMPROVED"


# ---------------------------------------------------------------------------
# P. signal_observation_trend=DEGRADED
# ---------------------------------------------------------------------------

def test_P_signal_degraded(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEGATIVE_SIGNAL")])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="POSITIVE_SIGNAL")],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["signal_observation_trend"] == "DEGRADED"


# ---------------------------------------------------------------------------
# Q. signal_observation_trend=UNCHANGED
# ---------------------------------------------------------------------------

def test_Q_signal_unchanged(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEUTRAL_SIGNAL")])
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEUTRAL_SIGNAL")],
                    "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    g = _load_delta(result)["groups"][0]
    assert g["signal_observation_trend"] == "UNCHANGED"


# ---------------------------------------------------------------------------
# R. advisory_change_note non-empty with non-binding reminder
# ---------------------------------------------------------------------------

def test_R_change_note_non_binding(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    result = run(str(tmp_path), lane)
    note = _load_delta(result)["groups"][0]["advisory_change_note"]
    assert isinstance(note, str) and len(note) > 0
    assert "non-binding" in note.lower()


# ---------------------------------------------------------------------------
# S. groups counts correct
# ---------------------------------------------------------------------------

def test_S_group_counts(tmp_path):
    lane = "live_test"
    cur_groups = [
        _make_advisory_group(signal_key="SK_BOTH"),
        _make_advisory_group(signal_key="SK_NEW"),
    ]
    prev_groups = [
        _make_advisory_group(signal_key="SK_BOTH"),
        _make_advisory_group(signal_key="SK_GONE"),
    ]
    _write_advisory(tmp_path, lane, cur_groups)
    _write_advisory(tmp_path, lane, prev_groups, "queen_advisory_prev.json", ts=_PREV_NOW)
    result = run(str(tmp_path), lane)
    delta = _load_delta(result)
    assert delta["groups_new_count"] == 1
    assert delta["groups_removed_count"] == 1
    assert delta["groups_compared_count"] == 1


# ---------------------------------------------------------------------------
# T. missing current advisory → ok=True, FIRST_SNAPSHOT, zero groups
# ---------------------------------------------------------------------------

def test_T_missing_current_advisory(tmp_path):
    lane = "live_test"
    # no queen_advisory_summary.json
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    delta = _load_delta(result)
    assert delta["comparison_status"] == "FIRST_SNAPSHOT"
    assert delta["groups"] == []


# ---------------------------------------------------------------------------
# U. AC-179 and AC-180 output files not modified
# ---------------------------------------------------------------------------

def test_U_prior_artifacts_unchanged(tmp_path):
    lane = "live_test"
    # create dummy AC-179 and AC-180 files
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    learning_text = '{"summary_version":"1","groups":[]}'
    advisory_text = json.dumps({
        "advisory_version": "1",
        "advisory_type": "queen_advisory_summary",
        "observational_only": True,
        "binding": False,
        "note": "x",
        "generated_ts_utc": _NOW,
        "source_lane": lane,
        "groups": [_make_advisory_group()],
    })
    (lane_dir / "queen_learning_summary.json").write_text(learning_text, encoding="utf-8")
    (lane_dir / "queen_advisory_summary.json").write_text(advisory_text, encoding="utf-8")

    run(str(tmp_path), lane)

    assert (lane_dir / "queen_learning_summary.json").read_text(encoding="utf-8") == learning_text
    assert (lane_dir / "queen_advisory_summary.json").read_text(encoding="utf-8") == advisory_text


# ---------------------------------------------------------------------------
# V. current advisory saved as queen_advisory_prev.json after run
# ---------------------------------------------------------------------------

def test_V_prev_snapshot_rotated(tmp_path):
    lane = "live_test"
    groups = [_make_advisory_group(trades_count=7)]
    _write_advisory(tmp_path, lane, groups)
    run(str(tmp_path), lane)
    prev_path = Path(tmp_path) / lane / "queen_advisory_prev.json"
    assert prev_path.exists()
    prev = json.loads(prev_path.read_text(encoding="utf-8"))
    assert prev["groups"][0]["trades_count"] == 7


# ---------------------------------------------------------------------------
# W. original advisory group fields preserved in delta groups
# ---------------------------------------------------------------------------

def test_W_original_fields_preserved(tmp_path):
    lane = "live_test"
    group = _make_advisory_group(trades_count=6, win_count=4)
    _write_advisory(tmp_path, lane, [group])
    result = run(str(tmp_path), lane)
    dg = _load_delta(result)["groups"][0]
    for field, value in group.items():
        assert field in dg, f"missing original field: {field}"
        assert dg[field] == value, f"original field modified: {field}"
