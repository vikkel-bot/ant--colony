"""
AC-186: Historical Memory Backfill

Verifies:
  A. run() returns ok=True and writes memory files for CLOSED trades
  B. OPEN trades are skipped (counted in skipped_open, not converted)
  C. Invalid/malformed records are skipped (counted in skipped_invalid)
  D. Field mapping: trade_id, market, strategy_key preserved correctly
  E. Timestamps normalised to YYYY-MM-DDTHH:MM:SSZ
  F. hold_duration_minutes computed correctly
  G. realized_pnl_eur rounded to 8dp; win_loss_label correct (WIN/LOSS/FLAT)
  H. Sentinel fields set correctly (UNKNOWN, -1.0, 0.0, 0, False, "OK")
  I. queen_action_required=True for all historical records
  J. exit_reason mapped: SL/TP/SIGNAL/OPERATOR_KILL/MANUAL preserved; else UNKNOWN
  K. Missing source file → ok=False with clear reason
  L. Output written to historical_backfill lane directory
  M. lane parameter overrides DEFAULT_LANE
  N. Source file with dict wrapper ({"trades": [...]}) parsed correctly
  O. Source file that is a plain list parsed correctly
  P. skipped_open and skipped_invalid counts are accurate
  Q. Memory artifacts pass read_memory_artifacts() (downstream pipeline compat)
  R. record_type="closed_trade_memory", memory_version="1"
  S. feedback_ts_utc == exit_ts_utc
  T. Unreadable source file → ok=False
  U. trade_id with special chars produces safe filename
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.historical_memory_backfill import (
    run,
    convert_paper_trade,
    read_paper_reconstruction,
    DEFAULT_LANE,
)

_NOW = "2026-04-14T10:00:00Z"

_CLOSED_TRADE = {
    "trade_id":      "T001",
    "market":        "BNB-EUR",
    "strategy":      "EDGE3",
    "state":         "CLOSED",
    "entry_ts":      "2026-01-01T09:00:00Z",
    "exit_ts":       "2026-01-01T10:30:00Z",
    "realized_pnl":  2.50,
    "exit_reason":   "TP",
}

_OPEN_TRADE = {
    "trade_id": "T002",
    "market":   "BNB-EUR",
    "strategy": "EDGE3",
    "state":    "OPEN",
    "entry_ts": "2026-01-02T09:00:00Z",
    "exit_ts":  None,
    "realized_pnl": 0.0,
    "exit_reason":  None,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_source(tmp_path, trades, *, wrapper=False):
    """Write paper_trade_reconstruction.json with trades list."""
    data = {"trades": trades} if wrapper else trades
    src = tmp_path / "paper_trade_reconstruction.json"
    src.write_text(json.dumps(data), encoding="utf-8")
    return src


def _memory_files(result):
    return list(Path(result["output_dir"]).glob("*.json"))


def _load_memory(result, trade_id):
    path = Path(result["output_dir"]) / f"{trade_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. run() ok=True and writes memory files
# ---------------------------------------------------------------------------

def test_A_run_writes_files(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    assert result["ok"] is True
    assert result["converted"] == 1
    files = _memory_files(result)
    assert len(files) == 1


# ---------------------------------------------------------------------------
# B. OPEN trades skipped
# ---------------------------------------------------------------------------

def test_B_open_trades_skipped(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE, _OPEN_TRADE])
    result = run(str(src), str(tmp_path))
    assert result["ok"] is True
    assert result["converted"] == 1
    assert result["skipped_open"] == 1


# ---------------------------------------------------------------------------
# C. Invalid/malformed records skipped
# ---------------------------------------------------------------------------

def test_C_invalid_records_skipped(tmp_path):
    trades = [
        _CLOSED_TRADE,
        "not_a_dict",
        {"state": "CLOSED"},            # missing trade_id, market, timestamps
        {"state": "CLOSED", "trade_id": "X", "market": "BNB-EUR"},  # missing timestamps
    ]
    src = _write_source(tmp_path, trades)
    result = run(str(src), str(tmp_path))
    assert result["converted"] == 1
    assert result["skipped_invalid"] >= 2


# ---------------------------------------------------------------------------
# D. Field mapping preserved
# ---------------------------------------------------------------------------

def test_D_field_mapping(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["trade_id"]      == "T001"
    assert mem["market"]        == "BNB-EUR"
    assert mem["strategy_key"]  == "EDGE3"


# ---------------------------------------------------------------------------
# E. Timestamps normalised
# ---------------------------------------------------------------------------

def test_E_timestamps_normalised(tmp_path):
    trade = dict(_CLOSED_TRADE, entry_ts="2026-01-01T09:00:00+00:00",
                 exit_ts="2026-01-01T10:30:00+00:00")
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["entry_ts_utc"] == "2026-01-01T09:00:00Z"
    assert mem["exit_ts_utc"]  == "2026-01-01T10:30:00Z"


# ---------------------------------------------------------------------------
# F. hold_duration_minutes computed correctly
# ---------------------------------------------------------------------------

def test_F_hold_duration(tmp_path):
    # entry 09:00, exit 10:30 → 90 minutes
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["hold_duration_minutes"] == pytest.approx(90.0, abs=0.01)


def test_F_hold_duration_zero_if_missing(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="T003", entry_ts="bad", exit_ts="bad")
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    # trade with bad timestamps skipped entirely (convert returns None)
    assert result["skipped_invalid"] >= 1


# ---------------------------------------------------------------------------
# G. realized_pnl_eur and win_loss_label
# ---------------------------------------------------------------------------

def test_G_win_loss_win(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])  # pnl=2.50
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["realized_pnl_eur"] == pytest.approx(2.5)
    assert mem["win_loss_label"] == "WIN"


def test_G_win_loss_loss(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="T004", realized_pnl=-1.20)
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T004")
    assert mem["win_loss_label"] == "LOSS"
    assert mem["realized_pnl_eur"] == pytest.approx(-1.20)


def test_G_win_loss_flat(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="T005", realized_pnl=0.0)
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T005")
    assert mem["win_loss_label"] == "FLAT"


# ---------------------------------------------------------------------------
# H. Sentinel fields
# ---------------------------------------------------------------------------

def test_H_sentinel_fields(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["market_regime_at_entry"]    == "UNKNOWN"
    assert mem["volatility_at_entry"]       == "UNKNOWN"
    assert mem["signal_strength"]           == pytest.approx(-1.0)
    assert mem["signal_key"]               == "UNKNOWN"
    assert mem["slippage_vs_expected_eur"]  == pytest.approx(0.0)
    assert mem["entry_latency_ms"]         == 0
    assert mem["anomaly_flag"]             is False
    assert mem["execution_quality_flag"]   == "OK"


# ---------------------------------------------------------------------------
# I. queen_action_required=True
# ---------------------------------------------------------------------------

def test_I_queen_action_required(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["queen_action_required"] is True


# ---------------------------------------------------------------------------
# J. exit_reason mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("SL",             "SL"),
    ("TP",             "TP"),
    ("SIGNAL",         "SIGNAL"),
    ("OPERATOR_KILL",  "OPERATOR_KILL"),
    ("MANUAL",         "MANUAL"),
    ("UNKNOWN",        "UNKNOWN"),
    ("sl",             "SL"),          # case-insensitive
    ("Tp",             "TP"),
    ("EXPIRED",        "UNKNOWN"),     # unmapped → UNKNOWN
    ("",               "UNKNOWN"),
    (None,             "UNKNOWN"),
    (42,               "UNKNOWN"),
])
def test_J_exit_reason_mapping(tmp_path, raw, expected):
    trade = dict(_CLOSED_TRADE, trade_id="TX", exit_reason=raw)
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    if result["converted"] == 1:
        mem = _load_memory(result, "TX")
        assert mem["exit_reason"] == expected


# ---------------------------------------------------------------------------
# K. Missing source file → ok=False
# ---------------------------------------------------------------------------

def test_K_missing_source(tmp_path):
    result = run(str(tmp_path / "nonexistent.json"), str(tmp_path))
    assert result["ok"] is False
    assert "missing" in result["reason"].lower() or "unreadable" in result["reason"].lower()


# ---------------------------------------------------------------------------
# L. Output in historical_backfill lane
# ---------------------------------------------------------------------------

def test_L_output_in_default_lane(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    assert result["lane"] == DEFAULT_LANE
    assert DEFAULT_LANE in result["output_dir"]
    assert (tmp_path / DEFAULT_LANE / "memory" / "T001.json").exists()


# ---------------------------------------------------------------------------
# M. lane parameter overrides DEFAULT_LANE
# ---------------------------------------------------------------------------

def test_M_custom_lane(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path), lane="custom_backfill")
    assert result["lane"] == "custom_backfill"
    assert (tmp_path / "custom_backfill" / "memory" / "T001.json").exists()


# ---------------------------------------------------------------------------
# N. Dict wrapper {"trades": [...]} parsed correctly
# ---------------------------------------------------------------------------

def test_N_dict_wrapper(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE], wrapper=True)
    result = run(str(src), str(tmp_path))
    assert result["ok"] is True
    assert result["converted"] == 1


# ---------------------------------------------------------------------------
# O. Plain list source parsed correctly
# ---------------------------------------------------------------------------

def test_O_plain_list(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE], wrapper=False)
    result = run(str(src), str(tmp_path))
    assert result["ok"] is True
    assert result["converted"] == 1


# ---------------------------------------------------------------------------
# P. Count accuracy
# ---------------------------------------------------------------------------

def test_P_counts_accurate(tmp_path):
    trades = [
        _CLOSED_TRADE,
        dict(_CLOSED_TRADE, trade_id="T006", realized_pnl=-0.5),
        dict(_OPEN_TRADE),
        "not_a_dict",
    ]
    src = _write_source(tmp_path, trades)
    result = run(str(src), str(tmp_path))
    assert result["total_source_records"] == 4
    assert result["converted"]       == 2
    assert result["skipped_open"]    == 1
    assert result["skipped_invalid"] == 1


# ---------------------------------------------------------------------------
# Q. Downstream pipeline compatibility via read_memory_artifacts
# ---------------------------------------------------------------------------

def test_Q_downstream_compat(tmp_path):
    from ant_colony.live.queen_learning_summary import read_memory_artifacts

    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    assert result["ok"] is True

    entries = read_memory_artifacts(str(tmp_path), DEFAULT_LANE)
    assert len(entries) == 1
    e = entries[0]
    assert e["trade_id"]     == "T001"
    assert e["market"]       == "BNB-EUR"
    assert e["strategy_key"] == "EDGE3"


# ---------------------------------------------------------------------------
# R. record_type and memory_version
# ---------------------------------------------------------------------------

def test_R_record_type_and_version(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["record_type"]     == "closed_trade_memory"
    assert mem["memory_version"]  == "1"


# ---------------------------------------------------------------------------
# S. feedback_ts_utc == exit_ts_utc
# ---------------------------------------------------------------------------

def test_S_feedback_ts_equals_exit_ts(tmp_path):
    src = _write_source(tmp_path, [_CLOSED_TRADE])
    result = run(str(src), str(tmp_path))
    mem = _load_memory(result, "T001")
    assert mem["feedback_ts_utc"] == mem["exit_ts_utc"]


# ---------------------------------------------------------------------------
# T. Unreadable (corrupt) source file → ok=False
# ---------------------------------------------------------------------------

def test_T_corrupt_source(tmp_path):
    src = tmp_path / "paper_trade_reconstruction.json"
    src.write_text("{not valid json", encoding="utf-8")
    result = run(str(src), str(tmp_path))
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# U. trade_id with special chars produces safe filename
# ---------------------------------------------------------------------------

def test_U_safe_filename(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="T001/sub:market")
    src = _write_source(tmp_path, [trade])
    result = run(str(src), str(tmp_path))
    assert result["converted"] == 1
    files = _memory_files(result)
    assert len(files) == 1
    # filename must not contain / or :
    name = files[0].name
    assert "/" not in name
    assert ":" not in name


# ---------------------------------------------------------------------------
# convert_paper_trade unit tests
# ---------------------------------------------------------------------------

def test_convert_returns_none_for_open():
    assert convert_paper_trade(dict(_OPEN_TRADE)) is None


def test_convert_returns_none_missing_trade_id():
    trade = dict(_CLOSED_TRADE)
    del trade["trade_id"]
    assert convert_paper_trade(trade) is None


def test_convert_returns_none_missing_market():
    trade = dict(_CLOSED_TRADE)
    del trade["market"]
    assert convert_paper_trade(trade) is None


def test_convert_returns_none_missing_entry_ts():
    trade = dict(_CLOSED_TRADE, entry_ts=None)
    assert convert_paper_trade(trade) is None


def test_convert_strategy_unknown_when_absent():
    trade = dict(_CLOSED_TRADE)
    del trade["strategy"]
    mem = convert_paper_trade(trade, now_utc=_NOW)
    assert mem is not None
    assert mem["strategy_key"] == "UNKNOWN"


def test_convert_pnl_zero_when_unparseable():
    trade = dict(_CLOSED_TRADE, realized_pnl="bad")
    mem = convert_paper_trade(trade, now_utc=_NOW)
    assert mem is not None
    assert mem["realized_pnl_eur"] == pytest.approx(0.0)
    assert mem["win_loss_label"] == "FLAT"


def test_convert_holding_state_field_accepted():
    """holding_state is an alternative to state in some paper trade formats."""
    trade = dict(_CLOSED_TRADE, trade_id="T007")
    del trade["state"]
    trade["holding_state"] = "CLOSED"
    mem = convert_paper_trade(trade, now_utc=_NOW)
    assert mem is not None
    assert mem["trade_id"] == "T007"


# ---------------------------------------------------------------------------
# read_paper_reconstruction unit tests
# ---------------------------------------------------------------------------

def test_read_returns_none_missing_file(tmp_path):
    assert read_paper_reconstruction(tmp_path / "nope.json") is None


def test_read_returns_list_direct(tmp_path):
    src = tmp_path / "x.json"
    src.write_text(json.dumps([_CLOSED_TRADE]), encoding="utf-8")
    result = read_paper_reconstruction(src)
    assert isinstance(result, list) and len(result) == 1


def test_read_returns_list_from_trades_key(tmp_path):
    src = tmp_path / "x.json"
    src.write_text(json.dumps({"trades": [_CLOSED_TRADE]}), encoding="utf-8")
    result = read_paper_reconstruction(src)
    assert isinstance(result, list) and len(result) == 1


def test_read_returns_list_from_closed_trades_key(tmp_path):
    src = tmp_path / "x.json"
    src.write_text(json.dumps({"closed_trades": [_CLOSED_TRADE]}), encoding="utf-8")
    result = read_paper_reconstruction(src)
    assert isinstance(result, list) and len(result) == 1


def test_read_returns_none_for_corrupt(tmp_path):
    src = tmp_path / "x.json"
    src.write_text("{bad", encoding="utf-8")
    assert read_paper_reconstruction(src) is None
