"""
AC-155: Tests for External Auto-Freeze Guard

Verifies:
  A. Quiet market data → allow (AUTO_FREEZE_CLEAR)
  B. Extreme negative move_pct → freeze
  C. Extreme positive move_pct → freeze
  D. Stale ts_utc → freeze
  E. Missing ts_utc → freeze
  F. market_data_ok=false → freeze
  G. market_snapshot missing + freeze_on_market_data_missing=true → freeze
  H. Invalid market in snapshot → freeze
  I. price_now <= 0 → freeze
  J. price_ref <= 0 → freeze
  K. config disabled → allow (AUTO_FREEZE_DISABLED)
  L. No exceptions leak out
  M. Output shape compatible with live gate (risk_state, freeze_new_entries)
  N. Abs move computed from prices triggers freeze independently
  O. Default config on disk is valid and produces allow on clean snapshot
  P. Stale threshold boundary conditions
  Q. move_pct non-numeric → freeze
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.external_auto_freeze_guard import (
    evaluate_external_auto_freeze,
    load_auto_freeze_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"
_FRESH_TS = _NOW
_STALE_TS = "2026-04-12T11:55:00Z"   # 5 min old (> 180s threshold)

_CFG = {
    "enabled": True,
    "market": "BNB-EUR",
    "max_single_move_pct": 8.0,
    "max_abs_move_pct": 12.0,
    "stale_market_data_seconds": 180,
    "freeze_on_market_data_missing": True,
}

_CLEAN_SNAPSHOT = {
    "market": "BNB-EUR",
    "price_now": 600.0,
    "price_ref": 600.0,
    "move_pct": 0.0,
    "ts_utc": _FRESH_TS,
    "market_data_ok": True,
    "_now_utc_override": _NOW,
}


def _snap(**overrides) -> dict:
    r = dict(_CLEAN_SNAPSHOT)
    r.update(overrides)
    return r


def _cfg(**overrides) -> dict:
    r = dict(_CFG)
    r.update(overrides)
    return r


def _eval(snap=None, cfg=None) -> dict:
    return evaluate_external_auto_freeze(
        snap if snap is not None else _snap(),
        cfg if cfg is not None else _CFG,
    )


# ---------------------------------------------------------------------------
# A. Quiet market data → allow
# ---------------------------------------------------------------------------

class TestQuietMarket:
    def test_clean_snapshot_allow(self):
        result = _eval()
        assert result["allow"] is True

    def test_clean_snapshot_reason(self):
        assert _eval()["reason"] == "AUTO_FREEZE_CLEAR"

    def test_clean_snapshot_risk_state_normal(self):
        assert _eval()["risk_state"] == "NORMAL"

    def test_clean_snapshot_freeze_new_entries_false(self):
        assert _eval()["freeze_new_entries"] is False

    def test_small_move_allowed(self):
        result = _eval(_snap(move_pct=3.0, price_now=618.0))
        assert result["allow"] is True


# ---------------------------------------------------------------------------
# B. Extreme negative move_pct → freeze
# ---------------------------------------------------------------------------

class TestExtremeNegativeMove:
    def test_extreme_negative_move_freeze(self):
        result = _eval(_snap(move_pct=-9.0))
        assert result["allow"] is False

    def test_extreme_negative_reason_mentions_move(self):
        result = _eval(_snap(move_pct=-9.0))
        assert "move" in result["reason"].lower()

    def test_exactly_at_threshold_freezes(self):
        # abs(8.0) >= 8.0 → freeze
        result = _eval(_snap(move_pct=-8.0))
        assert result["allow"] is False

    def test_just_below_threshold_allows(self):
        result = _eval(_snap(move_pct=-7.99, price_now=600.0))
        assert result["allow"] is True


# ---------------------------------------------------------------------------
# C. Extreme positive move_pct → freeze
# ---------------------------------------------------------------------------

class TestExtremePositiveMove:
    def test_extreme_positive_move_freeze(self):
        result = _eval(_snap(move_pct=10.0))
        assert result["allow"] is False

    def test_exactly_at_threshold_freezes(self):
        result = _eval(_snap(move_pct=8.0))
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# D. Stale ts_utc → freeze
# ---------------------------------------------------------------------------

class TestStaleData:
    def test_stale_ts_freeze(self):
        result = _eval(_snap(ts_utc=_STALE_TS))
        assert result["allow"] is False

    def test_stale_reason_mentions_stale(self):
        result = _eval(_snap(ts_utc=_STALE_TS))
        assert "stale" in result["reason"].lower()

    def test_exactly_at_threshold_allows(self):
        # exactly 180s old — not > threshold → allow
        now_dt = datetime(2026, 4, 12, 12, 0, 0, tzinfo=timezone.utc)
        exactly_at = (now_dt - timedelta(seconds=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _eval(_snap(ts_utc=exactly_at))
        assert result["allow"] is True

    def test_one_second_over_threshold_freezes(self):
        now_dt = datetime(2026, 4, 12, 12, 0, 0, tzinfo=timezone.utc)
        one_over = (now_dt - timedelta(seconds=181)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _eval(_snap(ts_utc=one_over))
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# E. Missing ts_utc → freeze
# ---------------------------------------------------------------------------

class TestMissingTs:
    def test_missing_ts_utc_freeze(self):
        snap = dict(_CLEAN_SNAPSHOT)
        del snap["ts_utc"]
        result = _eval(snap)
        assert result["allow"] is False

    def test_empty_ts_utc_freeze(self):
        result = _eval(_snap(ts_utc=""))
        assert result["allow"] is False

    def test_bad_ts_string_freeze(self):
        result = _eval(_snap(ts_utc="not-a-date"))
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# F. market_data_ok=false → freeze
# ---------------------------------------------------------------------------

class TestMarketDataNotOk:
    def test_market_data_false_freeze(self):
        result = _eval(_snap(market_data_ok=False))
        assert result["allow"] is False

    def test_market_data_none_freeze(self):
        result = _eval(_snap(market_data_ok=None))
        assert result["allow"] is False

    def test_market_data_missing_freeze(self):
        snap = dict(_CLEAN_SNAPSHOT)
        del snap["market_data_ok"]
        result = _eval(snap)
        assert result["allow"] is False

    def test_reason_mentions_market_data(self):
        result = _eval(_snap(market_data_ok=False))
        assert "market_data_ok" in result["reason"]


# ---------------------------------------------------------------------------
# G. Snapshot missing → freeze (freeze_on_market_data_missing=true)
# ---------------------------------------------------------------------------

class TestMissingSnapshot:
    def test_none_snapshot_freeze(self):
        result = evaluate_external_auto_freeze(None, _CFG)
        assert result["allow"] is False

    def test_empty_dict_snapshot_market_mismatch_freeze(self):
        result = evaluate_external_auto_freeze({}, _CFG)
        assert result["allow"] is False

    def test_non_dict_snapshot_freeze(self):
        result = evaluate_external_auto_freeze("bad", _CFG)
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# H. Invalid market in snapshot → freeze
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_freeze(self):
        result = _eval(_snap(market="BTC-EUR"))
        assert result["allow"] is False

    def test_market_reason_mentions_mismatch(self):
        result = _eval(_snap(market="BTC-EUR"))
        assert "market" in result["reason"].lower()

    def test_empty_market_freeze(self):
        result = _eval(_snap(market=""))
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# I. price_now <= 0 → freeze
# ---------------------------------------------------------------------------

class TestPriceNowBounds:
    def test_zero_price_now_freeze(self):
        result = _eval(_snap(price_now=0))
        assert result["allow"] is False

    def test_negative_price_now_freeze(self):
        result = _eval(_snap(price_now=-1.0))
        assert result["allow"] is False

    def test_reason_mentions_price_now(self):
        result = _eval(_snap(price_now=0))
        assert "price_now" in result["reason"]


# ---------------------------------------------------------------------------
# J. price_ref <= 0 → freeze
# ---------------------------------------------------------------------------

class TestPriceRefBounds:
    def test_zero_price_ref_freeze(self):
        result = _eval(_snap(price_ref=0))
        assert result["allow"] is False

    def test_negative_price_ref_freeze(self):
        result = _eval(_snap(price_ref=-100.0))
        assert result["allow"] is False

    def test_reason_mentions_price_ref(self):
        result = _eval(_snap(price_ref=0))
        assert "price_ref" in result["reason"]


# ---------------------------------------------------------------------------
# K. config disabled → allow (AUTO_FREEZE_DISABLED)
# ---------------------------------------------------------------------------

class TestConfigDisabled:
    def test_disabled_config_allow(self):
        result = _eval(cfg=_cfg(enabled=False))
        assert result["allow"] is True

    def test_disabled_config_reason(self):
        result = _eval(cfg=_cfg(enabled=False))
        assert "DISABLED" in result["reason"]

    def test_disabled_config_freeze_new_entries_false(self):
        result = _eval(cfg=_cfg(enabled=False))
        assert result["freeze_new_entries"] is False

    def test_disabled_with_extreme_move_still_allow(self):
        # if guard is disabled, it passes through regardless of snapshot
        result = evaluate_external_auto_freeze(
            _snap(move_pct=-50.0),
            _cfg(enabled=False),
        )
        assert result["allow"] is True


# ---------------------------------------------------------------------------
# L. No exceptions leak out
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_snap", [None, 42, "x", [], True])
    def test_no_exception_bad_snapshot(self, bad_snap):
        result = evaluate_external_auto_freeze(bad_snap, _CFG)
        assert isinstance(result, dict)
        assert "allow" in result

    @pytest.mark.parametrize("bad_cfg", [None, {}, 42, "x"])
    def test_no_exception_bad_config(self, bad_cfg):
        result = evaluate_external_auto_freeze(_snap(), bad_cfg)
        assert isinstance(result, dict)
        assert result["allow"] is False

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            result = evaluate_external_auto_freeze(v, _CFG)
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# M. Output shape compatible with live gate
# ---------------------------------------------------------------------------

class TestOutputShape:
    _REQUIRED_KEYS = ("allow", "reason", "risk_state", "freeze_new_entries")

    def test_allow_result_has_required_keys(self):
        result = _eval()
        for key in self._REQUIRED_KEYS:
            assert key in result, f"missing key: {key}"

    def test_freeze_result_has_required_keys(self):
        result = _eval(_snap(move_pct=-9.0))
        for key in self._REQUIRED_KEYS:
            assert key in result, f"missing key: {key}"

    def test_freeze_result_risk_state_freeze(self):
        result = _eval(_snap(move_pct=-9.0))
        assert result["risk_state"] == "FREEZE"

    def test_freeze_result_freeze_new_entries_true(self):
        result = _eval(_snap(move_pct=-9.0))
        assert result["freeze_new_entries"] is True

    def test_allow_result_risk_state_normal(self):
        assert _eval()["risk_state"] == "NORMAL"

    def test_allow_result_freeze_new_entries_false(self):
        assert _eval()["freeze_new_entries"] is False


# ---------------------------------------------------------------------------
# N. Abs move computed from prices triggers freeze
# ---------------------------------------------------------------------------

class TestAbsMoveFromPrices:
    def test_abs_move_above_threshold_freezes(self):
        # (540 - 600) / 600 * 100 = -10% → abs 10 < 12 (ok)
        # (528 - 600) / 600 * 100 = -12% → abs 12 >= 12 → freeze
        result = _eval(_snap(
            price_now=528.0,
            price_ref=600.0,
            move_pct=0.0,      # single-move ok, abs-move triggers
        ))
        assert result["allow"] is False

    def test_abs_move_exactly_at_threshold_freezes(self):
        # 12% exactly → freeze
        result = _eval(_snap(
            price_now=528.0,
            price_ref=600.0,
            move_pct=0.0,
        ))
        assert result["allow"] is False

    def test_abs_move_just_below_threshold_allows(self):
        # 11.9% < 12% → allow
        result = _eval(_snap(
            price_now=528.6,
            price_ref=600.0,
            move_pct=0.0,
        ))
        assert result["allow"] is True


# ---------------------------------------------------------------------------
# O. Default config on disk is valid and clean snapshot produces allow
# ---------------------------------------------------------------------------

class TestDefaultConfig:
    def test_default_config_loads(self):
        cfg = load_auto_freeze_config()
        assert isinstance(cfg, dict)
        assert cfg.get("enabled") is True

    def test_default_config_clean_snapshot_allow(self):
        cfg = load_auto_freeze_config()
        result = evaluate_external_auto_freeze(_snap(), cfg)
        assert result["allow"] is True

    def test_missing_file_returns_empty_dict(self, tmp_path):
        cfg = load_auto_freeze_config(tmp_path / "nonexistent.json")
        assert cfg == {}


# ---------------------------------------------------------------------------
# P. Threshold boundary: stale
# ---------------------------------------------------------------------------

class TestStaleBoundary:
    def test_fresh_data_exactly_at_boundary_allows(self):
        now_dt = datetime(2026, 4, 12, 12, 0, 0, tzinfo=timezone.utc)
        exactly = (now_dt - timedelta(seconds=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _eval(_snap(ts_utc=exactly))
        assert result["allow"] is True

    def test_fresh_data_one_second_under_boundary_allows(self):
        now_dt = datetime(2026, 4, 12, 12, 0, 0, tzinfo=timezone.utc)
        under = (now_dt - timedelta(seconds=179)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _eval(_snap(ts_utc=under))
        assert result["allow"] is True


# ---------------------------------------------------------------------------
# Q. move_pct non-numeric → freeze
# ---------------------------------------------------------------------------

class TestMoveNonNumeric:
    def test_string_move_pct_freeze(self):
        result = _eval(_snap(move_pct="big"))
        assert result["allow"] is False

    def test_none_move_pct_skipped(self):
        # None means field absent — guard skips single-move check, uses abs-move only
        snap = dict(_CLEAN_SNAPSHOT)
        del snap["move_pct"]
        result = _eval(snap)
        assert result["allow"] is True

    def test_bool_move_pct_freeze(self):
        result = _eval(_snap(move_pct=True))
        assert result["allow"] is False
