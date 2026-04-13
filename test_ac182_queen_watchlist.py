"""
AC-182: Queen Watchlist — non-binding attention queue from advisory + delta.

Verifies:
  A. run() writes queen_watchlist.json and returns ok=True
  B. output is marked observational_only=True and binding=False
  C. WATCH_SIGNAL_DECAY triggered when signal_observation=NEGATIVE_SIGNAL
  D. WATCH_SIGNAL_DECAY triggered when signal_observation_trend=DEGRADED (delta)
  E. WATCH_SLIPPAGE triggered when slippage_trend=WORSENING (delta)
  F. WATCH_LATENCY triggered when latency_trend=WORSENING (delta)
  G. WATCH_REGIME_SHIFT triggered when last_market_regime=UNKNOWN
  H. WATCH_SAMPLE triggered when sample_size_status=INSUFFICIENT
  I. NO_WATCH when no flag triggers
  J. multiple flags can be set on one group (watch_flags list)
  K. watch_status is highest-priority flag
  L. attention_required=True when any flag present; False for NO_WATCH
  M. watch_reasons list has one entry per flag
  N. every reason string contains a human-readable explanation
  O. attention_required_count on top-level summary is correct
  P. delta absent → delta-based rules skipped, advisory rules still apply
  Q. both input files absent → ok=True, zero groups
  R. original advisory group fields preserved in watchlist groups
  S. AC-179/180/181 output files not modified
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_watchlist import build_watch_entry, run

_NOW = "2026-04-13T10:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_advisory_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    signal_observation="NEUTRAL_SIGNAL",
    sample_size_status="MINIMAL",
    execution_quality_status="CLEAN",
    last_market_regime="BULL",
    last_volatility="MID",
    trades_count=6,
    win_count=3,
    loss_count=3,
    flat_count=0,
    avg_slippage_vs_expected_eur=0.12,
    avg_entry_latency_ms=95.0,
    avg_signal_strength=0.70,
    queen_action_required_count=0,
    advisory_note="Test note. This note is non-binding.",
) -> dict:
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "signal_observation": signal_observation,
        "sample_size_status": sample_size_status,
        "execution_quality_status": execution_quality_status,
        "last_market_regime": last_market_regime,
        "last_volatility": last_volatility,
        "trades_count": trades_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "avg_slippage_vs_expected_eur": avg_slippage_vs_expected_eur,
        "avg_entry_latency_ms": avg_entry_latency_ms,
        "avg_signal_strength": avg_signal_strength,
        "queen_action_required_count": queen_action_required_count,
        "advisory_note": advisory_note,
    }


def _make_delta_group(
    *,
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    signal_observation_trend="UNCHANGED",
    slippage_trend="STABLE",
    latency_trend="STABLE",
    sample_size_trend="UNCHANGED",
    sample_size_delta=0,
    advisory_change_note="Test delta note. Non-binding.",
) -> dict:
    return {
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "signal_observation_trend": signal_observation_trend,
        "slippage_trend": slippage_trend,
        "latency_trend": latency_trend,
        "sample_size_trend": sample_size_trend,
        "sample_size_delta": sample_size_delta,
        "advisory_change_note": advisory_change_note,
    }


def _write_advisory(tmp_path, lane, groups, filename="queen_advisory_summary.json"):
    advisory = {
        "advisory_version": "1",
        "advisory_type": "queen_advisory_summary",
        "observational_only": True,
        "binding": False,
        "note": "Non-binding.",
        "generated_ts_utc": _NOW,
        "source_lane": lane,
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / filename).write_text(json.dumps(advisory), encoding="utf-8")


def _write_delta(tmp_path, lane, groups):
    delta = {
        "delta_version": "1",
        "delta_type": "queen_advisory_delta",
        "observational_only": True,
        "binding": False,
        "note": "Non-binding.",
        "generated_ts_utc": _NOW,
        "source_lane": lane,
        "comparison_status": "COMPARED",
        "groups": groups,
    }
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "queen_advisory_delta.json").write_text(json.dumps(delta), encoding="utf-8")


def _load_watchlist(result):
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
    wl = _load_watchlist(result)
    assert wl["observational_only"] is True
    assert wl["binding"] is False


# ---------------------------------------------------------------------------
# C. WATCH_SIGNAL_DECAY from NEGATIVE_SIGNAL in advisory
# ---------------------------------------------------------------------------

def test_C_signal_decay_from_negative_signal(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(signal_observation="NEGATIVE_SIGNAL")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SIGNAL_DECAY" in g["watch_flags"]


# ---------------------------------------------------------------------------
# D. WATCH_SIGNAL_DECAY from signal_observation_trend=DEGRADED in delta
# ---------------------------------------------------------------------------

def test_D_signal_decay_from_delta_degraded(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(signal_observation="NEUTRAL_SIGNAL")])
    _write_delta(tmp_path, lane,
                 [_make_delta_group(signal_observation_trend="DEGRADED")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SIGNAL_DECAY" in g["watch_flags"]


# ---------------------------------------------------------------------------
# E. WATCH_SLIPPAGE from delta
# ---------------------------------------------------------------------------

def test_E_watch_slippage(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    _write_delta(tmp_path, lane, [_make_delta_group(slippage_trend="WORSENING")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SLIPPAGE" in g["watch_flags"]


# ---------------------------------------------------------------------------
# F. WATCH_LATENCY from delta
# ---------------------------------------------------------------------------

def test_F_watch_latency(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group()])
    _write_delta(tmp_path, lane, [_make_delta_group(latency_trend="WORSENING")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_LATENCY" in g["watch_flags"]


# ---------------------------------------------------------------------------
# G. WATCH_REGIME_SHIFT when last_market_regime=UNKNOWN
# ---------------------------------------------------------------------------

def test_G_watch_regime_shift(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(last_market_regime="UNKNOWN")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_REGIME_SHIFT" in g["watch_flags"]


# ---------------------------------------------------------------------------
# H. WATCH_SAMPLE when sample_size_status=INSUFFICIENT
# ---------------------------------------------------------------------------

def test_H_watch_sample(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(sample_size_status="INSUFFICIENT")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SAMPLE" in g["watch_flags"]


# ---------------------------------------------------------------------------
# I. NO_WATCH when no flags trigger
# ---------------------------------------------------------------------------

def test_I_no_watch(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEUTRAL_SIGNAL",
        sample_size_status="ADEQUATE",
        last_market_regime="BULL",
    )])
    _write_delta(tmp_path, lane, [_make_delta_group(
        signal_observation_trend="UNCHANGED",
        slippage_trend="STABLE",
        latency_trend="STABLE",
    )])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert g["watch_status"] == "NO_WATCH"
    assert g["watch_flags"] == []
    assert g["attention_required"] is False


# ---------------------------------------------------------------------------
# J. multiple flags on one group
# ---------------------------------------------------------------------------

def test_J_multiple_flags(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEGATIVE_SIGNAL",
        sample_size_status="INSUFFICIENT",
    )])
    _write_delta(tmp_path, lane, [_make_delta_group(slippage_trend="WORSENING")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert len(g["watch_flags"]) >= 2
    assert "WATCH_SIGNAL_DECAY" in g["watch_flags"]
    assert "WATCH_SLIPPAGE" in g["watch_flags"]


# ---------------------------------------------------------------------------
# K. watch_status is highest-priority flag
# ---------------------------------------------------------------------------

def test_K_watch_status_highest_priority(tmp_path):
    lane = "live_test"
    # WATCH_SIGNAL_DECAY > WATCH_SAMPLE (highest priority wins)
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEGATIVE_SIGNAL",
        sample_size_status="INSUFFICIENT",
    )])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert g["watch_status"] == "WATCH_SIGNAL_DECAY"


def test_K_watch_slippage_before_sample(tmp_path):
    lane = "live_test"
    # WATCH_SLIPPAGE > WATCH_SAMPLE
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEUTRAL_SIGNAL",
        sample_size_status="INSUFFICIENT",
    )])
    _write_delta(tmp_path, lane, [_make_delta_group(slippage_trend="WORSENING")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert g["watch_status"] == "WATCH_SLIPPAGE"


# ---------------------------------------------------------------------------
# L. attention_required
# ---------------------------------------------------------------------------

def test_L_attention_required_true(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(sample_size_status="INSUFFICIENT")])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert g["attention_required"] is True


def test_L_attention_required_false(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        sample_size_status="ADEQUATE",
        last_market_regime="BULL",
        signal_observation="NEUTRAL_SIGNAL",
    )])
    _write_delta(tmp_path, lane, [_make_delta_group()])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert g["attention_required"] is False


# ---------------------------------------------------------------------------
# M. watch_reasons list has one entry per flag
# ---------------------------------------------------------------------------

def test_M_reasons_length_matches_flags(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEGATIVE_SIGNAL",
        sample_size_status="INSUFFICIENT",
        last_market_regime="UNKNOWN",
    )])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert len(g["watch_reasons"]) == len(g["watch_flags"])


# ---------------------------------------------------------------------------
# N. every reason string is human-readable (non-empty string)
# ---------------------------------------------------------------------------

def test_N_reasons_are_strings(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        signal_observation="NEGATIVE_SIGNAL",
        sample_size_status="INSUFFICIENT",
    )])
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    for reason in g["watch_reasons"]:
        assert isinstance(reason, str) and len(reason) > 0


# ---------------------------------------------------------------------------
# O. attention_required_count on top-level summary
# ---------------------------------------------------------------------------

def test_O_attention_count(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [
        _make_advisory_group(signal_key="SK1", sample_size_status="INSUFFICIENT"),
        _make_advisory_group(signal_key="SK2", sample_size_status="ADEQUATE",
                             last_market_regime="BULL",
                             signal_observation="NEUTRAL_SIGNAL"),
        _make_advisory_group(signal_key="SK3", last_market_regime="UNKNOWN"),
    ])
    _write_delta(tmp_path, lane, [
        _make_delta_group(signal_key="SK2"),
    ])
    result = run(str(tmp_path), lane)
    wl = _load_watchlist(result)
    assert wl["attention_required_count"] == 2  # SK1 and SK3


# ---------------------------------------------------------------------------
# P. delta absent → delta rules skipped, advisory rules still work
# ---------------------------------------------------------------------------

def test_P_no_delta_advisory_rules_apply(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane,
                    [_make_advisory_group(sample_size_status="INSUFFICIENT")])
    # no delta file
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SAMPLE" in g["watch_flags"]


def test_P_no_delta_slippage_not_flagged(tmp_path):
    lane = "live_test"
    _write_advisory(tmp_path, lane, [_make_advisory_group(
        sample_size_status="ADEQUATE",
        last_market_regime="BULL",
        signal_observation="NEUTRAL_SIGNAL",
    )])
    # no delta → slippage_trend unknown, WATCH_SLIPPAGE must not trigger
    result = run(str(tmp_path), lane)
    g = _load_watchlist(result)["groups"][0]
    assert "WATCH_SLIPPAGE" not in g["watch_flags"]


# ---------------------------------------------------------------------------
# Q. both input files absent → ok=True, zero groups
# ---------------------------------------------------------------------------

def test_Q_no_inputs(tmp_path):
    lane = "live_test"
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    wl = _load_watchlist(result)
    assert wl["groups"] == []
    assert wl["total_groups"] == 0


# ---------------------------------------------------------------------------
# R. original advisory group fields preserved
# ---------------------------------------------------------------------------

def test_R_original_fields_preserved(tmp_path):
    lane = "live_test"
    group = _make_advisory_group(trades_count=7, win_count=4)
    _write_advisory(tmp_path, lane, [group])
    result = run(str(tmp_path), lane)
    wg = _load_watchlist(result)["groups"][0]
    for field, value in group.items():
        assert field in wg, f"missing original field: {field}"
        assert wg[field] == value, f"original field modified: {field}"


# ---------------------------------------------------------------------------
# S. prior artifact files not modified
# ---------------------------------------------------------------------------

def test_S_prior_artifacts_unchanged(tmp_path):
    lane = "live_test"
    lane_dir = Path(tmp_path) / lane
    lane_dir.mkdir(parents=True, exist_ok=True)

    learning_text = '{"summary_version":"1","groups":[]}'
    advisory_text = json.dumps({
        "advisory_version": "1", "advisory_type": "queen_advisory_summary",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "groups": [_make_advisory_group()],
    })
    delta_text = json.dumps({
        "delta_version": "1", "delta_type": "queen_advisory_delta",
        "observational_only": True, "binding": False, "note": "x",
        "generated_ts_utc": _NOW, "source_lane": lane,
        "comparison_status": "COMPARED", "groups": [_make_delta_group()],
    })

    (lane_dir / "queen_learning_summary.json").write_text(learning_text, encoding="utf-8")
    (lane_dir / "queen_advisory_summary.json").write_text(advisory_text, encoding="utf-8")
    (lane_dir / "queen_advisory_delta.json").write_text(delta_text, encoding="utf-8")

    run(str(tmp_path), lane)

    assert (lane_dir / "queen_learning_summary.json").read_text(encoding="utf-8") == learning_text
    assert (lane_dir / "queen_advisory_summary.json").read_text(encoding="utf-8") == advisory_text
    assert (lane_dir / "queen_advisory_delta.json").read_text(encoding="utf-8") == delta_text
