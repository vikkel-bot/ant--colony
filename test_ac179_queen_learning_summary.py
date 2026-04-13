"""
AC-179: Queen Learning Summary — non-binding aggregation of live memory artifacts.

Verifies:
  A. run() succeeds and writes queen_learning_summary.json when memory artifacts exist
  B. output artifact is marked observational_only=True, binding=False
  C. trades_count is correct per group
  D. win_count / loss_count / flat_count are correct
  E. avg_signal_strength excludes sentinel (-1.0) values
  F. avg_slippage_vs_expected_eur is computed correctly
  G. avg_entry_latency_ms is computed correctly
  H. last_market_regime and last_volatility reflect most-recent record
  I. queen_action_required_count is correct
  J. multiple groups are separated by (market, strategy_key, signal_key)
  K. empty memory directory → summary with zero groups, ok=True
  L. missing memory directory → summary with zero groups, ok=True
  M. unreadable / corrupt memory files are skipped, not failures
  N. summary contains required top-level metadata fields
  O. avg_signal_strength is None when all values are sentinel
  P. groups are sorted by (market, strategy_key, signal_key)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_learning_summary import (
    read_memory_artifacts,
    aggregate_learning_summary,
    build_summary,
    run,
)

_NOW = "2026-04-13T10:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_memory_entry(
    tmp_path,
    lane,
    *,
    trade_id="T1",
    market="BNB-EUR",
    strategy_key="EDGE3",
    signal_key="EDGE3_BREAKOUT_V2",
    win_loss_label="WIN",
    signal_strength=0.75,
    slippage=0.12,
    latency_ms=95,
    market_regime="BULL",
    volatility="MID",
    queen_action_required=False,
    feedback_ts_utc=_NOW,
):
    entry = {
        "memory_version": "1",
        "record_type": "closed_trade_memory",
        "lane": lane,
        "market": market,
        "strategy_key": strategy_key,
        "signal_key": signal_key,
        "trade_id": trade_id,
        "win_loss_label": win_loss_label,
        "signal_strength": signal_strength,
        "slippage_vs_expected_eur": slippage,
        "entry_latency_ms": latency_ms,
        "market_regime_at_entry": market_regime,
        "volatility_at_entry": volatility,
        "queen_action_required": queen_action_required,
        "feedback_ts_utc": feedback_ts_utc,
        "memory_ts_utc": _NOW,
        "realized_pnl_eur": 1.0 if win_loss_label == "WIN" else (-1.0 if win_loss_label == "LOSS" else 0.0),
        "execution_quality_flag": "ANOMALY" if queen_action_required else "OK",
    }
    mem_dir = Path(tmp_path) / lane / "memory"
    mem_dir.mkdir(parents=True, exist_ok=True)
    (mem_dir / f"{trade_id}.json").write_text(json.dumps(entry), encoding="utf-8")
    return entry


def _load_summary(result):
    return json.loads(Path(result["output_path"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() succeeds and writes the file
# ---------------------------------------------------------------------------

def test_A_run_writes_file(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1")
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    assert Path(result["output_path"]).exists()


# ---------------------------------------------------------------------------
# B. observational_only=True and binding=False
# ---------------------------------------------------------------------------

def test_B_non_binding_flags(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1")
    result = run(str(tmp_path), lane)
    summary = _load_summary(result)
    assert summary["observational_only"] is True
    assert summary["binding"] is False


# ---------------------------------------------------------------------------
# C. trades_count
# ---------------------------------------------------------------------------

def test_C_trades_count(tmp_path):
    lane = "live_test"
    for i in range(3):
        _make_memory_entry(tmp_path, lane, trade_id=f"T{i}")
    result = run(str(tmp_path), lane)
    summary = _load_summary(result)
    assert len(summary["groups"]) == 1
    assert summary["groups"][0]["trades_count"] == 3


# ---------------------------------------------------------------------------
# D. win / loss / flat counts
# ---------------------------------------------------------------------------

def test_D_win_loss_flat_counts(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", win_loss_label="WIN")
    _make_memory_entry(tmp_path, lane, trade_id="T2", win_loss_label="WIN")
    _make_memory_entry(tmp_path, lane, trade_id="T3", win_loss_label="LOSS")
    _make_memory_entry(tmp_path, lane, trade_id="T4", win_loss_label="FLAT")
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert g["win_count"] == 2
    assert g["loss_count"] == 1
    assert g["flat_count"] == 1


# ---------------------------------------------------------------------------
# E. avg_signal_strength excludes sentinel -1.0
# ---------------------------------------------------------------------------

def test_E_avg_signal_strength_excludes_sentinel(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", signal_strength=0.80)
    _make_memory_entry(tmp_path, lane, trade_id="T2", signal_strength=-1.0)  # sentinel
    _make_memory_entry(tmp_path, lane, trade_id="T3", signal_strength=0.60)
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    # avg of 0.80 and 0.60 only
    assert abs(g["avg_signal_strength"] - 0.70) < 1e-6


# ---------------------------------------------------------------------------
# F. avg_slippage_vs_expected_eur
# ---------------------------------------------------------------------------

def test_F_avg_slippage(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", slippage=0.12)
    _make_memory_entry(tmp_path, lane, trade_id="T2", slippage=0.24)
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert abs(g["avg_slippage_vs_expected_eur"] - 0.18) < 1e-6


# ---------------------------------------------------------------------------
# G. avg_entry_latency_ms
# ---------------------------------------------------------------------------

def test_G_avg_latency(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", latency_ms=100)
    _make_memory_entry(tmp_path, lane, trade_id="T2", latency_ms=200)
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert abs(g["avg_entry_latency_ms"] - 150.0) < 1e-6


# ---------------------------------------------------------------------------
# H. last_market_regime and last_volatility from most-recent record
# ---------------------------------------------------------------------------

def test_H_last_regime_from_most_recent(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1",
                       market_regime="BULL", volatility="LOW",
                       feedback_ts_utc="2026-04-13T09:00:00Z")
    _make_memory_entry(tmp_path, lane, trade_id="T2",
                       market_regime="BEAR", volatility="HIGH",
                       feedback_ts_utc="2026-04-13T10:00:00Z")
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert g["last_market_regime"] == "BEAR"
    assert g["last_volatility"] == "HIGH"


# ---------------------------------------------------------------------------
# I. queen_action_required_count
# ---------------------------------------------------------------------------

def test_I_queen_action_required_count(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", queen_action_required=True)
    _make_memory_entry(tmp_path, lane, trade_id="T2", queen_action_required=False)
    _make_memory_entry(tmp_path, lane, trade_id="T3", queen_action_required=True)
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert g["queen_action_required_count"] == 2


# ---------------------------------------------------------------------------
# J. multiple groups separated by grouping key
# ---------------------------------------------------------------------------

def test_J_multiple_groups(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1",
                       market="BNB-EUR", strategy_key="EDGE3", signal_key="EDGE3_BREAKOUT_V2")
    _make_memory_entry(tmp_path, lane, trade_id="T2",
                       market="BNB-EUR", strategy_key="EDGE3", signal_key="EDGE3_PULLBACK_V1")
    _make_memory_entry(tmp_path, lane, trade_id="T3",
                       market="ETH-EUR", strategy_key="EDGE3", signal_key="EDGE3_BREAKOUT_V2")
    result = run(str(tmp_path), lane)
    groups = _load_summary(result)["groups"]
    assert len(groups) == 3
    keys = {(g["market"], g["signal_key"]) for g in groups}
    assert ("BNB-EUR", "EDGE3_BREAKOUT_V2") in keys
    assert ("BNB-EUR", "EDGE3_PULLBACK_V1") in keys
    assert ("ETH-EUR", "EDGE3_BREAKOUT_V2") in keys


# ---------------------------------------------------------------------------
# K. empty memory directory → zero groups, ok=True
# ---------------------------------------------------------------------------

def test_K_empty_memory_dir(tmp_path):
    lane = "live_test"
    (Path(tmp_path) / lane / "memory").mkdir(parents=True, exist_ok=True)
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    summary = _load_summary(result)
    assert summary["total_records_read"] == 0
    assert summary["groups"] == []


# ---------------------------------------------------------------------------
# L. missing memory directory → zero groups, ok=True
# ---------------------------------------------------------------------------

def test_L_missing_memory_dir(tmp_path):
    lane = "live_test"
    # no memory dir created
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    summary = _load_summary(result)
    assert summary["total_records_read"] == 0
    assert summary["groups"] == []


# ---------------------------------------------------------------------------
# M. corrupt files are skipped, valid files processed
# ---------------------------------------------------------------------------

def test_M_corrupt_files_skipped(tmp_path):
    lane = "live_test"
    mem_dir = Path(tmp_path) / lane / "memory"
    mem_dir.mkdir(parents=True, exist_ok=True)
    (mem_dir / "corrupt.json").write_text("not json {{{{", encoding="utf-8")
    _make_memory_entry(tmp_path, lane, trade_id="T1")
    result = run(str(tmp_path), lane)
    assert result["ok"] is True
    summary = _load_summary(result)
    assert summary["total_records_read"] == 1  # only the valid one
    assert len(summary["groups"]) == 1


# ---------------------------------------------------------------------------
# N. required top-level metadata fields present
# ---------------------------------------------------------------------------

def test_N_required_metadata_fields(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1")
    result = run(str(tmp_path), lane)
    summary = _load_summary(result)
    for field in (
        "summary_version", "summary_type", "observational_only", "binding",
        "note", "generated_ts_utc", "source_lane", "source_dir",
        "total_records_read", "skipped_records", "groups",
    ):
        assert field in summary, f"missing field: {field}"


# ---------------------------------------------------------------------------
# O. avg_signal_strength is None when all values are sentinel
# ---------------------------------------------------------------------------

def test_O_avg_signal_strength_none_when_all_sentinel(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T1", signal_strength=-1.0)
    _make_memory_entry(tmp_path, lane, trade_id="T2", signal_strength=-1.0)
    result = run(str(tmp_path), lane)
    g = _load_summary(result)["groups"][0]
    assert g["avg_signal_strength"] is None


# ---------------------------------------------------------------------------
# P. groups sorted by (market, strategy_key, signal_key)
# ---------------------------------------------------------------------------

def test_P_groups_sorted(tmp_path):
    lane = "live_test"
    _make_memory_entry(tmp_path, lane, trade_id="T3",
                       market="ETH-EUR", strategy_key="EDGE3", signal_key="EDGE3_BREAKOUT_V2")
    _make_memory_entry(tmp_path, lane, trade_id="T1",
                       market="BNB-EUR", strategy_key="EDGE3", signal_key="EDGE3_BREAKOUT_V2")
    _make_memory_entry(tmp_path, lane, trade_id="T2",
                       market="BNB-EUR", strategy_key="EDGE3", signal_key="EDGE3_PULLBACK_V1")
    result = run(str(tmp_path), lane)
    groups = _load_summary(result)["groups"]
    keys = [(g["market"], g["strategy_key"], g["signal_key"]) for g in groups]
    assert keys == sorted(keys)
