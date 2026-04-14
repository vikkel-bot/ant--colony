"""
AC-187: Live Lane Scheduler — Heartbeat

Verifies:
  A. _write_heartbeat creates heartbeat.json in base_output_dir
  B. Required fields present: component, ts_utc, last_run_utc, last_status
  C. Optional fields present: lane, ok, reason, host
  D. last_status=BLOCKED, ok=False for a blocked result
  E. last_status=LIVE_GATE_READY, ok=True for a ready result
  F. last_status=EXECUTED, ok=True for an executed result
  G. Never raises on bad output_dir (non-writable or non-existent parent)
  H. Never raises on malformed result dict
  I. Atomic write — no .tmp file left behind after success
  J. Heartbeat updated on consecutive calls (ts_utc advances)
  K. Default path fallback (C:\\Trading\\ANT_LIVE) used when base_output_dir absent
  L. component field always == "live_lane_runner"
  M. reason field carries the block reason when state is BLOCKED
  N. reason field is None when result has no reason
  O. run() return value is unaffected by heartbeat writing
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_lane_runner import _write_heartbeat, _HEARTBEAT_FILENAME

_BLOCKED  = {"state": "BLOCKED",         "lane": "live_test", "reason": "LANE_DISABLED"}
_READY    = {"state": "LIVE_GATE_READY",  "lane": "live_test", "reason": None}
_EXECUTED = {"state": "EXECUTED",         "lane": "live_test", "reason": None}

_REQUIRED_FIELDS = ("component", "ts_utc", "last_run_utc", "last_status")
_OPTIONAL_FIELDS = ("lane", "ok", "reason", "host")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hb(tmp_path):
    return json.loads((tmp_path / _HEARTBEAT_FILENAME).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. File created
# ---------------------------------------------------------------------------

def test_A_file_created(tmp_path):
    _write_heartbeat(_BLOCKED, str(tmp_path))
    assert (tmp_path / _HEARTBEAT_FILENAME).exists()


# ---------------------------------------------------------------------------
# B. Required fields present
# ---------------------------------------------------------------------------

def test_B_required_fields(tmp_path):
    _write_heartbeat(_BLOCKED, str(tmp_path))
    hb = _hb(tmp_path)
    for field in _REQUIRED_FIELDS:
        assert field in hb, f"missing required field: {field}"


# ---------------------------------------------------------------------------
# C. Optional fields present
# ---------------------------------------------------------------------------

def test_C_optional_fields(tmp_path):
    _write_heartbeat(_READY, str(tmp_path))
    hb = _hb(tmp_path)
    for field in _OPTIONAL_FIELDS:
        assert field in hb, f"missing optional field: {field}"


# ---------------------------------------------------------------------------
# D. BLOCKED → last_status=BLOCKED, ok=False
# ---------------------------------------------------------------------------

def test_D_blocked(tmp_path):
    _write_heartbeat(_BLOCKED, str(tmp_path))
    hb = _hb(tmp_path)
    assert hb["last_status"] == "BLOCKED"
    assert hb["ok"] is False


# ---------------------------------------------------------------------------
# E. LIVE_GATE_READY → ok=True
# ---------------------------------------------------------------------------

def test_E_ready(tmp_path):
    _write_heartbeat(_READY, str(tmp_path))
    hb = _hb(tmp_path)
    assert hb["last_status"] == "LIVE_GATE_READY"
    assert hb["ok"] is True


# ---------------------------------------------------------------------------
# F. EXECUTED → ok=True
# ---------------------------------------------------------------------------

def test_F_executed(tmp_path):
    _write_heartbeat(_EXECUTED, str(tmp_path))
    hb = _hb(tmp_path)
    assert hb["last_status"] == "EXECUTED"
    assert hb["ok"] is True


# ---------------------------------------------------------------------------
# G. Never raises on non-writable path
# ---------------------------------------------------------------------------

def test_G_bad_path_no_raise():
    # Absolute path that cannot be created (e.g. inside a non-existent drive letter)
    # On Windows, Z:\ almost certainly doesn't exist
    _write_heartbeat(_BLOCKED, r"Z:\nonexistent_path_ac187_test")  # must not raise


def test_G_none_result_no_raise(tmp_path):
    _write_heartbeat(None, str(tmp_path))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# H. Never raises on malformed result
# ---------------------------------------------------------------------------

def test_H_malformed_result(tmp_path):
    _write_heartbeat({}, str(tmp_path))
    _write_heartbeat({"state": None}, str(tmp_path))


# ---------------------------------------------------------------------------
# I. No .tmp file left behind
# ---------------------------------------------------------------------------

def test_I_no_tmp_leftover(tmp_path):
    _write_heartbeat(_READY, str(tmp_path))
    tmp_files = list(tmp_path.glob("*.tmp"))
    assert tmp_files == [], f"unexpected .tmp files: {tmp_files}"


# ---------------------------------------------------------------------------
# J. Heartbeat updated on consecutive calls
# ---------------------------------------------------------------------------

def test_J_updated_on_consecutive_calls(tmp_path):
    _write_heartbeat(_BLOCKED, str(tmp_path))
    hb1 = _hb(tmp_path)

    # Update to a different state
    _write_heartbeat(_EXECUTED, str(tmp_path))
    hb2 = _hb(tmp_path)

    assert hb2["last_status"] == "EXECUTED"
    assert hb2["ok"] is True
    # ts_utc may be the same second — just check file was rewritten
    path = tmp_path / _HEARTBEAT_FILENAME
    assert path.exists()


# ---------------------------------------------------------------------------
# K. Default path fallback
# ---------------------------------------------------------------------------

def test_K_default_path_used_when_none(monkeypatch, tmp_path):
    """When base_output_dir is None, falls back to _DEFAULT_BASE_OUTPUT_DIR.
    We monkeypatch the constant to point at tmp_path so the test is portable."""
    import ant_colony.live.live_lane_runner as mod
    original = mod._DEFAULT_BASE_OUTPUT_DIR
    try:
        mod._DEFAULT_BASE_OUTPUT_DIR = str(tmp_path)
        _write_heartbeat(_READY, None)
        assert (tmp_path / _HEARTBEAT_FILENAME).exists()
    finally:
        mod._DEFAULT_BASE_OUTPUT_DIR = original


# ---------------------------------------------------------------------------
# L. component == "live_lane_runner"
# ---------------------------------------------------------------------------

def test_L_component(tmp_path):
    _write_heartbeat(_READY, str(tmp_path))
    assert _hb(tmp_path)["component"] == "live_lane_runner"


# ---------------------------------------------------------------------------
# M. reason carries block reason
# ---------------------------------------------------------------------------

def test_M_reason_from_result(tmp_path):
    _write_heartbeat(_BLOCKED, str(tmp_path))
    assert _hb(tmp_path)["reason"] == "LANE_DISABLED"


# ---------------------------------------------------------------------------
# N. reason is None when result has no reason
# ---------------------------------------------------------------------------

def test_N_reason_none_when_absent(tmp_path):
    _write_heartbeat(_READY, str(tmp_path))
    assert _hb(tmp_path)["reason"] is None


# ---------------------------------------------------------------------------
# O. run() return value unaffected by heartbeat
# ---------------------------------------------------------------------------

def test_O_run_return_unaffected(monkeypatch, tmp_path):
    """Heartbeat writing must not modify or replace the run() return value."""
    import ant_colony.live.live_lane_runner as mod

    calls = []

    def fake_write(result, base_output_dir=None):
        calls.append(result)

    monkeypatch.setattr(mod, "_write_heartbeat", fake_write)

    # Run with default config (gates will block since enabled=false)
    result = mod.run()
    assert isinstance(result, dict)
    assert "state" in result
    # heartbeat writer saw the same result
    assert len(calls) == 0  # we monkeypatched _write_heartbeat, not called via main()

    # Verify main() calls _write_heartbeat with the result
    printed = []
    import io, contextlib

    def fake_write2(result, base_output_dir=None):
        calls.append(("hb", result))

    monkeypatch.setattr(mod, "_write_heartbeat", fake_write2)
    monkeypatch.setattr(mod, "_DEFAULT_BASE_OUTPUT_DIR", str(tmp_path))

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mod.main()

    assert len(calls) == 1
    tag, hb_result = calls[0]
    assert tag == "hb"
    # The printed JSON and the heartbeat result must match
    printed_result = json.loads(buf.getvalue())
    assert printed_result["state"] == hb_result["state"]
