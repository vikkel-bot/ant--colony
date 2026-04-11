"""
AC-145: Tests for EDGE3 Watchdog (God Alert System)

Simulates all alert conditions:
  A. Snapshot missing          → SNAPSHOT_MISSING alert
  B. State != OK               → EDGE3_FATAL alert
  C. Timestamp stale           → STALE_DATA alert
  D. Gate = BLOCK              → GATE_BLOCK alert
  E. All OK                    → no alerts

Plus:
  F. Cooldown suppresses duplicate alerts
  G. Error in one market does not stop other markets
  H. run_once returns correct structure
  I. Telegram not called when telegram_enabled=False
  J. Non-dict / malformed snapshots handled safely
  K. Both field-name variants accepted (state/status, gate/edge3_combined_gate)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.supervisor.edge3_watchdog import (
    ALERT_SNAPSHOT_MISSING,
    ALERT_STALE_DATA,
    ALERT_EDGE3_FATAL,
    ALERT_GATE_BLOCK,
    check_market,
    run_once,
    send_telegram_alert,
    _parse_ts,
    _is_cooled_down,
    _record_sent,
    load_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_FRESH_TS = (_NOW - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
_STALE_TS = (_NOW - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _good_snapshot(ts=_FRESH_TS) -> dict:
    return {"ts_utc": ts, "status": "OK", "market": "BTC-EUR"}


def _good_cb21() -> dict:
    return {
        "ts_utc": _FRESH_TS,
        "edge3_combined_gate": "ALLOW",
        "edge3_health_gate": "ALLOW",
    }


def _make_worker(tmp_path: Path, market: str,
                 snapshot: dict | None = None,
                 cb21: dict | None = None) -> Path:
    """Create fake worker directory and optional report files."""
    reports = tmp_path / market / "reports"
    reports.mkdir(parents=True)
    if snapshot is not None:
        (reports / "edge3_snapshot.json").write_text(
            json.dumps(snapshot), encoding="utf-8"
        )
    if cb21 is not None:
        (reports / "edge3_cb21_meta.json").write_text(
            json.dumps(cb21), encoding="utf-8"
        )
    return tmp_path


def _config(tmp_path: Path, markets=("BTC-EUR",), stale_s=300) -> dict:
    return {
        "snapshot_base_path":    str(tmp_path),
        "stale_threshold_seconds": stale_s,
        "alert_cooldown_seconds": 600,
        "markets":               list(markets),
        "telegram_enabled":      False,
    }


def _events(alerts: list[dict]) -> list[str]:
    return [a["event"] for a in alerts]


# ---------------------------------------------------------------------------
# _parse_ts
# ---------------------------------------------------------------------------

class TestParseTs:
    def test_z_suffix(self):
        dt = _parse_ts("2025-06-01T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_offset_suffix(self):
        dt = _parse_ts("2026-03-17T11:11:07.018390+00:00")
        assert dt is not None

    def test_none_input(self):
        assert _parse_ts(None) is None

    def test_bad_string(self):
        assert _parse_ts("not-a-date") is None

    def test_result_utc(self):
        dt = _parse_ts("2025-01-01T00:00:00Z")
        assert dt.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# A. Snapshot missing → SNAPSHOT_MISSING
# ---------------------------------------------------------------------------

class TestSnapshotMissing:
    def test_missing_snapshot_alert(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR")  # no snapshot file
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_SNAPSHOT_MISSING in _events(alerts)

    def test_missing_snapshot_alert_content(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR")
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        snap_alerts = [a for a in alerts if a["event"] == ALERT_SNAPSHOT_MISSING]
        assert len(snap_alerts) == 1
        assert snap_alerts[0]["market"] == "BTC-EUR"
        assert snap_alerts[0]["component"] == "edge3_watchdog"

    def test_missing_snapshot_no_crash(self, tmp_path):
        # Directory doesn't even exist
        alerts = check_market("NONEXIST", _config(tmp_path), now=_NOW)
        assert any(a["event"] == ALERT_SNAPSHOT_MISSING for a in alerts)


# ---------------------------------------------------------------------------
# B. State != OK → EDGE3_FATAL
# ---------------------------------------------------------------------------

class TestEdge3Fatal:
    def test_fatal_status_field(self, tmp_path):
        snap = _good_snapshot()
        snap["status"] = "EDGE3_FATAL"
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL in _events(alerts)

    def test_fatal_state_field(self, tmp_path):
        snap = {"ts_utc": _FRESH_TS, "state": "EDGE3_FATAL"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL in _events(alerts)

    def test_non_ok_status(self, tmp_path):
        snap = _good_snapshot()
        snap["status"] = "ERROR"
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL in _events(alerts)

    def test_ok_status_no_fatal_alert(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL not in _events(alerts)


# ---------------------------------------------------------------------------
# C. Stale timestamp → STALE_DATA
# ---------------------------------------------------------------------------

class TestStaleData:
    def test_stale_snapshot_alert(self, tmp_path):
        snap = _good_snapshot(ts=_STALE_TS)
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path, stale_s=300), now=_NOW)
        assert ALERT_STALE_DATA in _events(alerts)

    def test_fresh_snapshot_no_stale_alert(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(ts=_FRESH_TS),
                     cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path, stale_s=300), now=_NOW)
        assert ALERT_STALE_DATA not in _events(alerts)

    def test_missing_ts_utc_stale_alert(self, tmp_path):
        snap = {"status": "OK"}  # no ts_utc
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_STALE_DATA in _events(alerts)

    def test_boundary_exactly_at_threshold_no_alert(self, tmp_path):
        # Exactly at threshold (not >) should NOT alert
        ts = (_NOW - timedelta(seconds=300)).strftime("%Y-%m-%dT%H:%M:%SZ")
        snap = _good_snapshot(ts=ts)
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path, stale_s=300), now=_NOW)
        assert ALERT_STALE_DATA not in _events(alerts)

    def test_one_second_over_threshold_alerts(self, tmp_path):
        ts = (_NOW - timedelta(seconds=301)).strftime("%Y-%m-%dT%H:%M:%SZ")
        snap = _good_snapshot(ts=ts)
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path, stale_s=300), now=_NOW)
        assert ALERT_STALE_DATA in _events(alerts)


# ---------------------------------------------------------------------------
# D. Gate = BLOCK → GATE_BLOCK
# ---------------------------------------------------------------------------

class TestGateBlock:
    def test_combined_gate_block_alert(self, tmp_path):
        cb21 = _good_cb21()
        cb21["edge3_combined_gate"] = "BLOCK"
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=cb21)
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK in _events(alerts)

    def test_health_gate_block_alert(self, tmp_path):
        cb21 = {"ts_utc": _FRESH_TS, "edge3_health_gate": "BLOCK"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=cb21)
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK in _events(alerts)

    def test_plain_gate_block_alert(self, tmp_path):
        cb21 = {"ts_utc": _FRESH_TS, "gate": "BLOCK"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=cb21)
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK in _events(alerts)

    def test_allow_gate_no_alert(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK not in _events(alerts)

    def test_missing_cb21_no_crash(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot())  # no cb21
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert isinstance(alerts, list)


# ---------------------------------------------------------------------------
# E. All OK → no alerts
# ---------------------------------------------------------------------------

class TestAllOK:
    def test_all_ok_no_alerts(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert alerts == []

    def test_all_ok_run_once_state(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: False)
        assert status["state"] == "OK"
        assert status["alerts_sent"] == 0


# ---------------------------------------------------------------------------
# F. Cooldown suppresses duplicates
# ---------------------------------------------------------------------------

class TestCooldown:
    def test_second_alert_suppressed_within_cooldown(self, tmp_path):
        snap = _good_snapshot()
        snap["status"] = "EDGE3_FATAL"
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        cfg["alert_cooldown_seconds"] = 600

        cooldown_map: dict = {}
        sent_calls = []

        def capture_send(msg, c):
            sent_calls.append(msg)
            return True

        # First run
        run_once(cfg, cooldown_map, now=_NOW, _send_fn=capture_send)
        count_after_first = len(sent_calls)

        # Second run immediately (within cooldown)
        run_once(cfg, cooldown_map, now=_NOW, _send_fn=capture_send)
        assert len(sent_calls) == count_after_first  # no new sends

    def test_alert_sent_after_cooldown_expires(self, tmp_path):
        snap = _good_snapshot()
        snap["status"] = "EDGE3_FATAL"
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        cfg["alert_cooldown_seconds"] = 60

        cooldown_map: dict = {}
        sent_calls = []

        def capture_send(msg, c):
            sent_calls.append(msg)
            return True

        run_once(cfg, cooldown_map, now=_NOW, _send_fn=capture_send)
        first_count = len(sent_calls)

        # Run again after cooldown period
        later = _NOW + timedelta(seconds=61)
        run_once(cfg, cooldown_map, now=later, _send_fn=capture_send)
        assert len(sent_calls) > first_count

    def test_cooldown_is_cooled_down_helper(self):
        cooldown_map: dict = {}
        market, event = "BTC-EUR", ALERT_EDGE3_FATAL
        # Nothing recorded yet → cooled down
        assert _is_cooled_down(cooldown_map, market, event, _NOW, 600) is True
        # Record and check immediately → NOT cooled down
        _record_sent(cooldown_map, market, event, _NOW)
        assert _is_cooled_down(cooldown_map, market, event, _NOW, 600) is False
        # After cooldown → cooled down again
        later = _NOW + timedelta(seconds=601)
        assert _is_cooled_down(cooldown_map, market, event, later, 600) is True


# ---------------------------------------------------------------------------
# G. Error in one market doesn't stop others
# ---------------------------------------------------------------------------

class TestMarketIsolation:
    def test_bad_market_doesnt_stop_good_market(self, tmp_path):
        # BTC-EUR: no snapshot (will produce SNAPSHOT_MISSING)
        # ETH-EUR: all OK
        _make_worker(tmp_path, "ETH-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        # BTC-EUR: no files at all

        cfg = _config(tmp_path, markets=("BTC-EUR", "ETH-EUR"))
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: True)

        # Both markets checked
        assert status["markets_checked"] == 2
        # ETH-EUR was OK (no alerts), BTC-EUR had SNAPSHOT_MISSING
        btc_alerts = [a for a in status["alerts"] if a["market"] == "BTC-EUR"]
        eth_alerts = [a for a in status["alerts"] if a["market"] == "ETH-EUR"]
        assert any(a["event"] == ALERT_SNAPSHOT_MISSING for a in btc_alerts)
        assert eth_alerts == []


# ---------------------------------------------------------------------------
# H. run_once returns correct structure
# ---------------------------------------------------------------------------

class TestRunOnceStructure:
    REQUIRED_KEYS = {
        "component", "ts_utc", "markets_checked", "alerts_sent", "state", "alerts"
    }

    def test_required_keys_present(self, tmp_path):
        cfg = _config(tmp_path, markets=())
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: False)
        assert self.REQUIRED_KEYS.issubset(status.keys())

    def test_component_correct(self, tmp_path):
        cfg = _config(tmp_path, markets=())
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: False)
        assert status["component"] == "edge3_watchdog"

    def test_state_ok_when_no_alerts(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=_good_cb21())
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: False)
        assert status["state"] == "OK"

    def test_state_alerting_when_alerts(self, tmp_path):
        cfg = _config(tmp_path, markets=("BTC-EUR",))  # no files → SNAPSHOT_MISSING
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: True)
        assert status["state"] == "ALERTING"

    def test_markets_checked_count(self, tmp_path):
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot())
        _make_worker(tmp_path, "ETH-EUR", snapshot=_good_snapshot())
        cfg = _config(tmp_path, markets=("BTC-EUR", "ETH-EUR"))
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: False)
        assert status["markets_checked"] == 2

    def test_alerts_list_contains_dicts(self, tmp_path):
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        status = run_once(cfg, {}, now=_NOW, _send_fn=lambda m, c: True)
        for a in status["alerts"]:
            assert isinstance(a, dict)
            assert "market" in a and "event" in a


# ---------------------------------------------------------------------------
# I. telegram_enabled=False → send function not called with real HTTP
# ---------------------------------------------------------------------------

class TestTelegramDisabled:
    def test_telegram_disabled_returns_false(self):
        cfg = {"telegram_enabled": False}
        result = send_telegram_alert("test", cfg)
        assert result is False

    def test_telegram_placeholder_token_returns_false(self):
        cfg = {
            "telegram_enabled": True,
            "telegram_bot_token": "PUT_YOUR_TOKEN_HERE",
            "telegram_chat_id": "123",
        }
        result = send_telegram_alert("test", cfg)
        assert result is False

    def test_no_real_http_in_tests(self, tmp_path):
        """Ensure tests never make real HTTP calls by using telegram_enabled=False."""
        cfg = _config(tmp_path, markets=("BTC-EUR",))
        cfg["telegram_enabled"] = False
        status = run_once(cfg, {}, now=_NOW)
        # Should complete without any HTTP error
        assert isinstance(status, dict)


# ---------------------------------------------------------------------------
# J. Malformed snapshots handled safely
# ---------------------------------------------------------------------------

class TestMalformedSnapshots:
    def test_bad_json_snapshot(self, tmp_path):
        reports = tmp_path / "BTC-EUR" / "reports"
        reports.mkdir(parents=True)
        (reports / "edge3_snapshot.json").write_text("not json", encoding="utf-8")
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        # Bad JSON → treated as missing
        assert any(a["event"] == ALERT_SNAPSHOT_MISSING for a in alerts)

    def test_non_object_json_snapshot(self, tmp_path):
        reports = tmp_path / "BTC-EUR" / "reports"
        reports.mkdir(parents=True)
        (reports / "edge3_snapshot.json").write_text("[1,2,3]", encoding="utf-8")
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert any(a["event"] == ALERT_SNAPSHOT_MISSING for a in alerts)

    def test_empty_market_name(self, tmp_path):
        alerts = check_market("", _config(tmp_path), now=_NOW)
        assert isinstance(alerts, list)


# ---------------------------------------------------------------------------
# K. Both field-name variants
# ---------------------------------------------------------------------------

class TestFieldNameVariants:
    def test_status_field_ok(self, tmp_path):
        snap = {"ts_utc": _FRESH_TS, "status": "OK"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL not in _events(alerts)

    def test_state_field_ok(self, tmp_path):
        snap = {"ts_utc": _FRESH_TS, "state": "OK"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=snap, cb21=_good_cb21())
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_EDGE3_FATAL not in _events(alerts)

    def test_edge3_combined_gate_block(self, tmp_path):
        cb21 = {"ts_utc": _FRESH_TS, "edge3_combined_gate": "BLOCK"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=cb21)
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK in _events(alerts)

    def test_plain_gate_allow_no_block(self, tmp_path):
        cb21 = {"ts_utc": _FRESH_TS, "gate": "ALLOW"}
        _make_worker(tmp_path, "BTC-EUR", snapshot=_good_snapshot(), cb21=cb21)
        alerts = check_market("BTC-EUR", _config(tmp_path), now=_NOW)
        assert ALERT_GATE_BLOCK not in _events(alerts)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_missing_config_returns_empty(self, tmp_path):
        cfg = load_config(tmp_path / "nonexistent.json")
        assert isinstance(cfg, dict)

    def test_valid_config_loaded(self, tmp_path):
        data = {"check_interval_seconds": 30, "markets": ["BTC-EUR"]}
        p = tmp_path / "cfg.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        cfg = load_config(p)
        assert cfg["check_interval_seconds"] == 30
