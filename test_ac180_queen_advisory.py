"""
AC-180: Queen Advisory — non-binding interpretation layer over queen_learning_summary.

Verifies:
  A. run() writes queen_advisory_summary.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. each group contains the four required advisory fields
  D. sample_size_status=INSUFFICIENT when trades_count < 5
  E. sample_size_status=MINIMAL when 5 <= trades_count < 10
  F. sample_size_status=ADEQUATE when trades_count >= 10
  G. execution_quality_status=ATTENTION_NEEDED when queen_action_required_count > 0
  H. execution_quality_status=CLEAN when queen_action_required_count == 0
  I. signal_observation=POSITIVE_SIGNAL when win_rate > 0.60
  J. signal_observation=NEGATIVE_SIGNAL when win_rate < 0.40
  K. signal_observation=NEUTRAL_SIGNAL when 0.40 <= win_rate <= 0.60
  L. signal_observation=NO_DATA when trades_count == 0
  M. advisory_note is a non-empty string containing non-binding reminder
  N. multiple groups each receive independent advisory fields
  O. missing learning summary → ok=True, zero groups (not a failure)
  P. output contains required top-level metadata fields including thresholds
  Q. AC-179 queen_learning_summary.json is not modified by the advisory run
  R. advisory groups preserve all original learning-summary group fields
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_advisory import (
    advise_group,
    build_advisory,
    run,
    MIN_TRADES_FOR_SIGNAL,
    WIN_RATE_HIGH,
    WIN_RATE_LOW,
)

_NOW = "2026-04-13T10:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    trades_count=3,
    win_count=2,
    loss_count=1,
    flat_count=0,
    queen_action_required_count=0,
    last_market_regime="BULL",
    last_volatility="MID",
    avg_signal_strength=0.75,
    avg_slippage_vs_expected_eur=0.12,
    avg_entry_latency_ms=95.0,
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
        "last_market_regime": last_market_regime,
        "last_volatility": last_volatility,
        "avg_signal_strength": avg_signal_strength,
        "avg_slippage_vs_expected_eur": avg_slippage_vs_expected_eur,
        "avg_entry_latency_ms": avg_entry_latency_ms,
    }


def _make_learning_summary(tmp_path, lane, groups):
    """Write a queen_learning_summary.json with the given groups."""
    summary = {
        "summary_version": "1",
        "summary_type": "queen_learning_summary",
        "observational_only": True,
        "binding": False,
        "note": "Non-binding observational summary.",
        "generated_ts_utc": _NOW,
        "source_lane": lane,
        "source_dir": str(tmp_path / lane / "memory"),
        "total_records_read": sum(g["trades_count"] for g in groups),
        "skipped_records": 0,
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_learning_summary.json").write_text(
        json.dumps(summary), encoding="utf-8"
    )
    return summary


def _load_advisory(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() writes file and returns ok=True
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group()])
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding_flags(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group()])
    result = run(str(tmp_path), lane)
    adv = _load_advisory(result)
    assert adv["observational_only"] is True
    assert adv["binding"] is False


# ---------------------------------------------------------------------------
# C. each group has the four advisory fields
# ---------------------------------------------------------------------------

def test_C_advisory_fields_present(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group()])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    for field in ("sample_size_status", "execution_quality_status",
                  "signal_observation", "advisory_note"):
        assert field in g, f"missing advisory field: {field}"


# ---------------------------------------------------------------------------
# D. INSUFFICIENT when trades_count < 5
# ---------------------------------------------------------------------------

def test_D_insufficient_sample(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group(trades_count=4, win_count=3, loss_count=1)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["sample_size_status"] == "INSUFFICIENT"


def test_D_zero_trades_insufficient(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=0, win_count=0, loss_count=0, flat_count=0)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["sample_size_status"] == "INSUFFICIENT"


# ---------------------------------------------------------------------------
# E. MINIMAL when 5 <= trades_count < 10
# ---------------------------------------------------------------------------

def test_E_minimal_sample_at_boundary(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=5, win_count=3, loss_count=2)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["sample_size_status"] == "MINIMAL"


def test_E_minimal_sample_at_9(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=9, win_count=5, loss_count=4)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["sample_size_status"] == "MINIMAL"


# ---------------------------------------------------------------------------
# F. ADEQUATE when trades_count >= 10
# ---------------------------------------------------------------------------

def test_F_adequate_sample(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=6, loss_count=4)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["sample_size_status"] == "ADEQUATE"


# ---------------------------------------------------------------------------
# G. ATTENTION_NEEDED when queen_action_required_count > 0
# ---------------------------------------------------------------------------

def test_G_attention_needed(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(queen_action_required_count=2)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["execution_quality_status"] == "ATTENTION_NEEDED"


# ---------------------------------------------------------------------------
# H. CLEAN when queen_action_required_count == 0
# ---------------------------------------------------------------------------

def test_H_clean_execution(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(queen_action_required_count=0)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["execution_quality_status"] == "CLEAN"


# ---------------------------------------------------------------------------
# I. POSITIVE_SIGNAL when win_rate > 0.60
# ---------------------------------------------------------------------------

def test_I_positive_signal(tmp_path):
    lane = "live_test"
    # 7/10 = 0.70 > 0.60
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=7, loss_count=3)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "POSITIVE_SIGNAL"


# ---------------------------------------------------------------------------
# J. NEGATIVE_SIGNAL when win_rate < 0.40
# ---------------------------------------------------------------------------

def test_J_negative_signal(tmp_path):
    lane = "live_test"
    # 3/10 = 0.30 < 0.40
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=3, loss_count=7)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "NEGATIVE_SIGNAL"


# ---------------------------------------------------------------------------
# K. NEUTRAL_SIGNAL when 0.40 <= win_rate <= 0.60
# ---------------------------------------------------------------------------

def test_K_neutral_signal(tmp_path):
    lane = "live_test"
    # 5/10 = 0.50
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=5, loss_count=5)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "NEUTRAL_SIGNAL"


def test_K_neutral_signal_at_lower_bound(tmp_path):
    lane = "live_test"
    # exactly 4/10 = 0.40 → NEUTRAL
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=4, loss_count=6)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "NEUTRAL_SIGNAL"


def test_K_neutral_signal_at_upper_bound(tmp_path):
    lane = "live_test"
    # exactly 6/10 = 0.60 → NEUTRAL (not > 0.60)
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=10, win_count=6, loss_count=4)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "NEUTRAL_SIGNAL"


# ---------------------------------------------------------------------------
# L. NO_DATA when trades_count == 0
# ---------------------------------------------------------------------------

def test_L_no_data_signal(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane,
                           [_make_group(trades_count=0, win_count=0, loss_count=0, flat_count=0)])
    result = run(str(tmp_path), lane)
    g = _load_advisory(result)["groups"][0]
    assert g["signal_observation"] == "NO_DATA"


# ---------------------------------------------------------------------------
# M. advisory_note is non-empty and contains non-binding reminder
# ---------------------------------------------------------------------------

def test_M_advisory_note_non_binding(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group()])
    result = run(str(tmp_path), lane)
    note = _load_advisory(result)["groups"][0]["advisory_note"]
    assert isinstance(note, str) and len(note) > 0
    assert "non-binding" in note.lower()


# ---------------------------------------------------------------------------
# N. multiple groups each receive independent advisory
# ---------------------------------------------------------------------------

def test_N_multiple_groups_independent(tmp_path):
    lane = "live_test"
    groups = [
        _make_group(signal_key="SK1", trades_count=10, win_count=8, loss_count=2),  # POSITIVE
        _make_group(signal_key="SK2", trades_count=10, win_count=2, loss_count=8),  # NEGATIVE
    ]
    _make_learning_summary(tmp_path, lane, groups)
    result = run(str(tmp_path), lane)
    adv_groups = _load_advisory(result)["groups"]
    assert len(adv_groups) == 2
    obs = {g["signal_key"]: g["signal_observation"] for g in adv_groups}
    assert obs["SK1"] == "POSITIVE_SIGNAL"
    assert obs["SK2"] == "NEGATIVE_SIGNAL"


# ---------------------------------------------------------------------------
# O. missing learning summary → ok=True, zero groups
# ---------------------------------------------------------------------------

def test_O_missing_learning_summary(tmp_path):
    lane = "live_test"
    # no queen_learning_summary.json
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    adv = _load_advisory(result)
    assert adv["groups"] == []


# ---------------------------------------------------------------------------
# P. required top-level metadata fields and thresholds present
# ---------------------------------------------------------------------------

def test_P_metadata_fields(tmp_path):
    lane = "live_test"
    _make_learning_summary(tmp_path, lane, [_make_group()])
    result = run(str(tmp_path), lane)
    adv = _load_advisory(result)
    for field in (
        "advisory_version", "advisory_type", "observational_only", "binding",
        "note", "generated_ts_utc", "source_lane", "source_summary",
        "thresholds", "groups",
    ):
        assert field in adv, f"missing field: {field}"
    assert adv["thresholds"]["min_trades_for_signal"] == MIN_TRADES_FOR_SIGNAL
    assert adv["thresholds"]["win_rate_high"] == WIN_RATE_HIGH
    assert adv["thresholds"]["win_rate_low"] == WIN_RATE_LOW


# ---------------------------------------------------------------------------
# Q. AC-179 queen_learning_summary.json is not modified
# ---------------------------------------------------------------------------

def test_Q_learning_summary_unchanged(tmp_path):
    lane = "live_test"
    original = _make_learning_summary(tmp_path, lane, [_make_group()])
    original_text = (Path(tmp_path) / lane / "queen_learning_summary.json").read_text(
        encoding="utf-8"
    )
    run(str(tmp_path), lane)
    after_text = (Path(tmp_path) / lane / "queen_learning_summary.json").read_text(
        encoding="utf-8"
    )
    assert original_text == after_text


# ---------------------------------------------------------------------------
# R. advisory groups preserve all original learning-summary group fields
# ---------------------------------------------------------------------------

def test_R_original_fields_preserved(tmp_path):
    lane = "live_test"
    group = _make_group(trades_count=7, win_count=4, loss_count=3)
    _make_learning_summary(tmp_path, lane, [group])
    result = run(str(tmp_path), lane)
    adv_g = _load_advisory(result)["groups"][0]
    for field, value in group.items():
        assert field in adv_g, f"original field missing from advisory group: {field}"
        assert adv_g[field] == value, f"original field modified: {field}"
