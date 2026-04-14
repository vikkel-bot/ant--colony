"""
AC-186 v2: Queen Memory Backfill (Corrected Architecture)

Verifies:
  A. Output files created for CLOSED trades
  B. data_source always present and correct ("historical_backfill")
  C. Schema matches live memory format (all required fields present)
  D. Missing fields fall back to sentinel values
  E. No mixing with live_test directory (strict lane separation)
  F. Corrupt records skipped safely
  G. Deterministic output (same input → same output)
  H. OPEN trades skipped and counted
  I. Invalid/non-dict records skipped and counted
  J. Field mapping: trade_id, market, strategy_key, pnl, timestamps
  K. hold_duration_minutes computed correctly
  L. win_loss_label correct (WIN/LOSS/FLAT)
  M. exit_reason mapping (valid enum preserved; else UNKNOWN)
  N. Sentinel fields: market_regime, volatility, signal_strength, etc.
  O. queen_action_required=True for all historical records
  P. Missing source_dir → ok=False
  Q. Count accuracy (total_source_records, converted, skipped_*)
  R. lane parameter overrides DEFAULT_LANE
  S. Directory source scans candidate filenames in priority order
  T. Downstream pipeline compat (queen_learning_summary reads output)
  U. record_type and memory_version fields
  V. feedback_ts_utc == exit_ts_utc
  W. Safe filenames (special chars replaced)
  X. read_source_records unit tests (file, dir, corrupt, missing)
  Y. map_to_memory_schema unit tests (open, missing fields, etc.)
  Z. write_memory_record unit test (atomic write, output readable)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_memory_backfill import (
    run,
    read_source_records,
    map_to_memory_schema,
    write_memory_record,
    DEFAULT_LANE,
    DATA_SOURCE,
)

_NOW = "2026-04-14T10:00:00Z"

# Required fields in every output record (superset of live memory schema + data_source)
_REQUIRED_FIELDS = (
    "memory_version", "record_type", "lane", "data_source",
    "market", "strategy_key", "trade_id",
    "entry_ts_utc", "exit_ts_utc", "hold_duration_minutes",
    "realized_pnl_eur", "win_loss_label", "exit_reason",
    "anomaly_flag", "execution_quality_flag",
    "market_regime_at_entry", "volatility_at_entry",
    "signal_strength", "signal_key",
    "slippage_vs_expected_eur", "entry_latency_ms",
    "feedback_ts_utc", "memory_ts_utc", "queen_action_required",
)

_CLOSED_TRADE = {
    "trade_id":     "T001",
    "market":       "BNB-EUR",
    "strategy":     "EDGE3",
    "state":        "CLOSED",
    "entry_ts":     "2026-01-01T09:00:00Z",
    "exit_ts":      "2026-01-01T10:30:00Z",
    "realized_pnl": 2.50,
    "exit_reason":  "TP",
}

_OPEN_TRADE = {
    "trade_id":     "T002",
    "market":       "BNB-EUR",
    "strategy":     "EDGE3",
    "state":        "OPEN",
    "entry_ts":     "2026-01-02T09:00:00Z",
    "exit_ts":      None,
    "realized_pnl": 0.0,
    "exit_reason":  None,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_reconstruction(tmp_path, trades, filename="paper_trade_reconstruction.json"):
    """Write a paper_trade_reconstruction.json with rows list."""
    src = tmp_path / filename
    src.write_text(json.dumps({"rows": trades}), encoding="utf-8")
    return src


def _memory_files(result):
    return sorted(Path(result["output_dir"]).glob("*.json"))


def _load_memory(result, trade_id):
    path = Path(result["output_dir"]) / f"{trade_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. Output files created
# ---------------------------------------------------------------------------

def test_A_output_files_created(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["ok"] is True
    assert result["converted"] == 1
    assert len(_memory_files(result)) == 1


# ---------------------------------------------------------------------------
# B. data_source always present and correct
# ---------------------------------------------------------------------------

def test_B_data_source_present(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert "data_source" in mem
    assert mem["data_source"] == "historical_backfill"


def test_B_data_source_equals_constant(tmp_path):
    assert DATA_SOURCE == "historical_backfill"


# ---------------------------------------------------------------------------
# C. Schema matches live memory format
# ---------------------------------------------------------------------------

def test_C_schema_complete(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    for field in _REQUIRED_FIELDS:
        assert field in mem, f"missing required field: {field}"


# ---------------------------------------------------------------------------
# D. Missing fields fall back to sentinel values
# ---------------------------------------------------------------------------

def test_D_missing_strategy_becomes_unknown(tmp_path):
    trade = {k: v for k, v in _CLOSED_TRADE.items() if k != "strategy"}
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["strategy_key"] == "UNKNOWN"


def test_D_missing_pnl_becomes_zero_flat(tmp_path):
    trade = dict(_CLOSED_TRADE, realized_pnl=None)
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["realized_pnl_eur"] == pytest.approx(0.0)
    assert mem["win_loss_label"] == "FLAT"


# ---------------------------------------------------------------------------
# E. No mixing with live_test directory
# ---------------------------------------------------------------------------

def test_E_no_live_test_mixing(tmp_path):
    live_test_dir = tmp_path / "out" / "live_test" / "memory"
    live_test_dir.mkdir(parents=True, exist_ok=True)
    sentinel = live_test_dir / "sentinel.json"
    sentinel.write_text("{}", encoding="utf-8")

    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))

    # historical output goes to historical_backfill, not live_test
    assert result["lane"] == DEFAULT_LANE
    out_path = Path(result["output_dir"])
    # output_dir must end with <lane>/memory, not live_test/memory
    assert out_path.parent.name == DEFAULT_LANE
    assert out_path.name == "memory"
    # sentinel in live_test directory untouched
    assert sentinel.read_text(encoding="utf-8") == "{}"
    # no files written into live_test
    assert list(live_test_dir.glob("*.json")) == [sentinel]


# ---------------------------------------------------------------------------
# F. Corrupt records skipped safely
# ---------------------------------------------------------------------------

def test_F_corrupt_record_skipped(tmp_path):
    trades = [_CLOSED_TRADE, "not_a_dict", 42, None]
    _write_reconstruction(tmp_path, trades)
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["ok"] is True
    assert result["converted"] == 1
    assert result["skipped_invalid"] >= 2


def test_F_corrupt_source_file(tmp_path):
    src = tmp_path / "paper_trade_reconstruction.json"
    src.write_text("{bad json", encoding="utf-8")
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# G. Deterministic output (same input → same output)
# ---------------------------------------------------------------------------

def test_G_deterministic_output(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result1 = run(str(tmp_path), str(tmp_path / "out1"), lane="lb1")
    result2 = run(str(tmp_path), str(tmp_path / "out2"), lane="lb2")

    mem1 = _load_memory(result1, "T001")
    mem2 = _load_memory(result2, "T001")

    # All fields except memory_ts_utc must be identical
    for field in _REQUIRED_FIELDS:
        if field in ("memory_ts_utc", "lane"):
            continue
        assert mem1[field] == mem2[field], f"field {field!r} not deterministic"


# ---------------------------------------------------------------------------
# H. OPEN trades skipped and counted
# ---------------------------------------------------------------------------

def test_H_open_trades_skipped(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE, _OPEN_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["converted"]    == 1
    assert result["skipped_open"] == 1


# ---------------------------------------------------------------------------
# I. Non-dict records skipped and counted
# ---------------------------------------------------------------------------

def test_I_non_dict_skipped(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE, "bad", 99])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["converted"]       == 1
    assert result["skipped_invalid"] == 2


# ---------------------------------------------------------------------------
# J. Field mapping
# ---------------------------------------------------------------------------

def test_J_field_mapping(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["trade_id"]     == "T001"
    assert mem["market"]       == "BNB-EUR"
    assert mem["strategy_key"] == "EDGE3"
    assert mem["entry_ts_utc"] == "2026-01-01T09:00:00Z"
    assert mem["exit_ts_utc"]  == "2026-01-01T10:30:00Z"
    assert mem["realized_pnl_eur"] == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# K. hold_duration_minutes computed correctly
# ---------------------------------------------------------------------------

def test_K_hold_duration(tmp_path):
    # entry 09:00, exit 10:30 → 90 minutes
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["hold_duration_minutes"] == pytest.approx(90.0, abs=0.01)


# ---------------------------------------------------------------------------
# L. win_loss_label
# ---------------------------------------------------------------------------

def test_L_win(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])  # pnl=2.50
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert _load_memory(result, "T001")["win_loss_label"] == "WIN"


def test_L_loss(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="TX", realized_pnl=-1.0)
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert _load_memory(result, "TX")["win_loss_label"] == "LOSS"


def test_L_flat(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="TF", realized_pnl=0.0)
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert _load_memory(result, "TF")["win_loss_label"] == "FLAT"


# ---------------------------------------------------------------------------
# M. exit_reason mapping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("SL",            "SL"),
    ("TP",            "TP"),
    ("SIGNAL",        "SIGNAL"),
    ("OPERATOR_KILL", "OPERATOR_KILL"),
    ("MANUAL",        "MANUAL"),
    ("UNKNOWN",       "UNKNOWN"),
    ("sl",            "SL"),
    ("EXPIRED",       "UNKNOWN"),
    ("",              "UNKNOWN"),
    (None,            "UNKNOWN"),
    (42,              "UNKNOWN"),
])
def test_M_exit_reason(tmp_path, raw, expected):
    trade = dict(_CLOSED_TRADE, trade_id="TR", exit_reason=raw)
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    if result["converted"] == 1:
        assert _load_memory(result, "TR")["exit_reason"] == expected


# ---------------------------------------------------------------------------
# N. Sentinel fields — no fake execution data
# ---------------------------------------------------------------------------

def test_N_sentinel_fields(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
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
# O. queen_action_required=True
# ---------------------------------------------------------------------------

def test_O_queen_action_required(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert _load_memory(result, "T001")["queen_action_required"] is True


# ---------------------------------------------------------------------------
# P. Missing source_dir → ok=False
# ---------------------------------------------------------------------------

def test_P_missing_source(tmp_path):
    result = run(str(tmp_path / "nonexistent"), str(tmp_path / "out"))
    assert result["ok"] is False
    assert "not found" in result["reason"].lower() or "unreadable" in result["reason"].lower()


# ---------------------------------------------------------------------------
# Q. Count accuracy
# ---------------------------------------------------------------------------

def test_Q_counts(tmp_path):
    trades = [
        _CLOSED_TRADE,
        dict(_CLOSED_TRADE, trade_id="T003", realized_pnl=-0.5),
        _OPEN_TRADE,
        "bad",
    ]
    _write_reconstruction(tmp_path, trades)
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["total_source_records"] == 4
    assert result["converted"]       == 2
    assert result["skipped_open"]    == 1
    assert result["skipped_invalid"] == 1


# ---------------------------------------------------------------------------
# R. lane parameter overrides DEFAULT_LANE
# ---------------------------------------------------------------------------

def test_R_custom_lane(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"), lane="custom_backfill")
    assert result["lane"] == "custom_backfill"
    assert (tmp_path / "out" / "custom_backfill" / "memory" / "T001.json").exists()


# ---------------------------------------------------------------------------
# S. Directory source: priority order (reconstruction before feedback)
# ---------------------------------------------------------------------------

def test_S_directory_priority_reconstruction_first(tmp_path):
    # Write both files; reconstruction has CLOSED T001, feedback has CLOSED T999
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "paper_trade_reconstruction.json").write_text(
        json.dumps({"rows": [_CLOSED_TRADE]}), encoding="utf-8"
    )
    (src_dir / "paper_trade_feedback.json").write_text(
        json.dumps({"rows": [dict(_CLOSED_TRADE, trade_id="T999")]}), encoding="utf-8"
    )
    result = run(str(src_dir), str(tmp_path / "out"))
    assert result["converted"] == 1
    assert (tmp_path / "out" / DEFAULT_LANE / "memory" / "T001.json").exists()


def test_S_directory_fallback_to_feedback(tmp_path):
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    # Only feedback file present
    (src_dir / "paper_trade_feedback.json").write_text(
        json.dumps({"rows": [_CLOSED_TRADE]}), encoding="utf-8"
    )
    result = run(str(src_dir), str(tmp_path / "out"))
    assert result["ok"] is True
    assert result["converted"] == 1


# ---------------------------------------------------------------------------
# T. Downstream pipeline compat
# ---------------------------------------------------------------------------

def test_T_downstream_compat(tmp_path):
    from ant_colony.live.queen_learning_summary import read_memory_artifacts

    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["ok"] is True

    entries = read_memory_artifacts(str(tmp_path / "out"), DEFAULT_LANE)
    assert len(entries) == 1
    e = entries[0]
    assert e["trade_id"]     == "T001"
    assert e["market"]       == "BNB-EUR"
    assert e["data_source"]  == "historical_backfill"


# ---------------------------------------------------------------------------
# U. record_type and memory_version
# ---------------------------------------------------------------------------

def test_U_record_type_and_version(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["record_type"]    == "closed_trade_memory"
    assert mem["memory_version"] == "1"


# ---------------------------------------------------------------------------
# V. feedback_ts_utc == exit_ts_utc
# ---------------------------------------------------------------------------

def test_V_feedback_ts_equals_exit_ts(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    result = run(str(tmp_path), str(tmp_path / "out"))
    mem = _load_memory(result, "T001")
    assert mem["feedback_ts_utc"] == mem["exit_ts_utc"]


# ---------------------------------------------------------------------------
# W. Safe filenames
# ---------------------------------------------------------------------------

def test_W_safe_filename(tmp_path):
    trade = dict(_CLOSED_TRADE, trade_id="T001/sub:market")
    _write_reconstruction(tmp_path, [trade])
    result = run(str(tmp_path), str(tmp_path / "out"))
    assert result["converted"] == 1
    files = _memory_files(result)
    assert len(files) == 1
    name = files[0].name
    assert "/" not in name
    assert ":" not in name


# ---------------------------------------------------------------------------
# X. read_source_records unit tests
# ---------------------------------------------------------------------------

def test_X_read_file_direct_list(tmp_path):
    src = tmp_path / "trades.json"
    src.write_text(json.dumps([_CLOSED_TRADE]), encoding="utf-8")
    records = read_source_records(src)
    assert isinstance(records, list) and len(records) == 1


def test_X_read_file_rows_wrapper(tmp_path):
    src = tmp_path / "trades.json"
    src.write_text(json.dumps({"rows": [_CLOSED_TRADE]}), encoding="utf-8")
    records = read_source_records(src)
    assert isinstance(records, list) and len(records) == 1


def test_X_read_file_trades_wrapper(tmp_path):
    src = tmp_path / "trades.json"
    src.write_text(json.dumps({"trades": [_CLOSED_TRADE]}), encoding="utf-8")
    records = read_source_records(src)
    assert isinstance(records, list) and len(records) == 1


def test_X_read_dir_reconstruction(tmp_path):
    _write_reconstruction(tmp_path, [_CLOSED_TRADE])
    records = read_source_records(tmp_path)
    assert isinstance(records, list) and len(records) == 1


def test_X_read_missing_returns_none(tmp_path):
    assert read_source_records(tmp_path / "nope") is None


def test_X_read_corrupt_file_returns_none(tmp_path):
    src = tmp_path / "paper_trade_reconstruction.json"
    src.write_text("{bad", encoding="utf-8")
    assert read_source_records(tmp_path) is None


# ---------------------------------------------------------------------------
# Y. map_to_memory_schema unit tests
# ---------------------------------------------------------------------------

def test_Y_open_returns_none():
    assert map_to_memory_schema(_OPEN_TRADE) is None


def test_Y_missing_trade_id_returns_none():
    trade = {k: v for k, v in _CLOSED_TRADE.items() if k != "trade_id"}
    assert map_to_memory_schema(trade) is None


def test_Y_missing_market_returns_none():
    trade = {k: v for k, v in _CLOSED_TRADE.items() if k != "market"}
    assert map_to_memory_schema(trade) is None


def test_Y_missing_timestamps_returns_none():
    trade = dict(_CLOSED_TRADE, entry_ts=None, exit_ts=None)
    assert map_to_memory_schema(trade) is None


def test_Y_data_source_present():
    mem = map_to_memory_schema(_CLOSED_TRADE, now_utc=_NOW)
    assert mem is not None
    assert mem["data_source"] == "historical_backfill"


def test_Y_strategy_key_field_also_accepted():
    """strategy_key field (live memory format) also accepted as source."""
    trade = dict(_CLOSED_TRADE)
    del trade["strategy"]
    trade["strategy_key"] = "EDGE3_ALT"
    mem = map_to_memory_schema(trade, now_utc=_NOW)
    assert mem is not None
    assert mem["strategy_key"] == "EDGE3_ALT"


def test_Y_holding_state_accepted():
    """holding_state is an alternative to state in some paper formats."""
    trade = dict(_CLOSED_TRADE, trade_id="TH")
    del trade["state"]
    trade["holding_state"] = "CLOSED"
    mem = map_to_memory_schema(trade, now_utc=_NOW)
    assert mem is not None


def test_Y_timestamp_normalisation():
    trade = dict(_CLOSED_TRADE, entry_ts="2026-01-01T09:00:00+00:00",
                 exit_ts="2026-01-01T10:30:00+00:00")
    mem = map_to_memory_schema(trade, now_utc=_NOW)
    assert mem["entry_ts_utc"] == "2026-01-01T09:00:00Z"
    assert mem["exit_ts_utc"]  == "2026-01-01T10:30:00Z"


# ---------------------------------------------------------------------------
# Z. write_memory_record unit test
# ---------------------------------------------------------------------------

def test_Z_write_memory_record(tmp_path):
    mem = map_to_memory_schema(_CLOSED_TRADE, now_utc=_NOW)
    assert mem is not None
    write_memory_record(mem, tmp_path)
    path = tmp_path / "T001.json"
    assert path.exists()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["trade_id"]   == "T001"
    assert loaded["data_source"] == "historical_backfill"
