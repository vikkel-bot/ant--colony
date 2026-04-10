"""
AC-104 tests — Paper Market Data → Review Chain Bridge

Coverage:
  - missing adapter file → no crash, 0 markets processed
  - corrupt adapter file → no crash, 0 markets processed
  - single market runs full pipeline without error
  - mapping: DATA_STALE → CRITICAL anomaly
  - mapping: DATA_MISSING → CRITICAL anomaly
  - mapping: ALL_CLEAR → NONE anomaly
  - mapping: ZERO_INTENTS → LOW anomaly
  - mapping: HOLD_REVIEW → MEDIUM anomaly
  - unknown seed class → no crash (fail-closed)
  - feedback values only CONFIRM/DISAGREE/UNCERTAIN
  - feedback written to log (write mode)
  - analysis file created (write mode)
  - analysis flags correct
  - deterministic output (same adapter → same results)
  - dry_run → no file writes
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from run_marketdata_review_lite import run, inputs_for_class


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _adapter(markets: list[dict]) -> dict:
    return {
        "version":   "marketdata_scenario_adapter_v1",
        "component": "build_marketdata_scenario_adapter_lite",
        "ts_utc":    "2026-04-10T12:00:00Z",
        "markets":   markets,
        "sources":   {"market_data_loaded": True, "intents_data_loaded": True},
        "flags": {
            "non_binding": True, "simulation_only": True,
            "paper_only": True, "live_activation_allowed": False,
        },
    }


def _market(market: str, seed_class: str) -> dict:
    return {
        "market":            market,
        "price_present":     True,
        "price_fresh":       seed_class in ("ALL_CLEAR", "ZERO_INTENTS"),
        "data_state":        "OK" if seed_class in ("ALL_CLEAR", "ZERO_INTENTS") else "STALE",
        "intents_present":   seed_class == "ALL_CLEAR",
        "review_seed_class": seed_class,
    }


def _write_adapter(tmp_path: Path, markets: list[dict]) -> Path:
    p = tmp_path / "adapter.json"
    p.write_text(json.dumps(_adapter(markets)), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# 1. Fail-closed — missing / corrupt adapter
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_missing_adapter_no_crash(self, tmp_path):
        result = run(
            adapter_path  = tmp_path / "nonexistent.json",
            log_path      = tmp_path / "log.jsonl",
            analysis_path = tmp_path / "analysis.json",
            dry_run       = True,
        )
        assert result["markets_processed"] == 0

    def test_corrupt_adapter_no_crash(self, tmp_path):
        bad = tmp_path / "adapter.json"
        bad.write_text("{ bad json {{{", encoding="utf-8")
        result = run(
            adapter_path  = bad,
            log_path      = tmp_path / "log.jsonl",
            analysis_path = tmp_path / "analysis.json",
            dry_run       = True,
        )
        assert result["markets_processed"] == 0

    def test_empty_markets_list_no_crash(self, tmp_path):
        p = _write_adapter(tmp_path, [])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        assert result["markets_processed"] == 0

    def test_missing_adapter_no_file_written(self, tmp_path):
        log = tmp_path / "log.jsonl"
        run(adapter_path=tmp_path / "x.json", log_path=log,
            analysis_path=tmp_path / "a.json", dry_run=False)
        assert not log.exists()


# ---------------------------------------------------------------------------
# 2. inputs_for_class — mapping unit tests
# ---------------------------------------------------------------------------

class TestInputsForClass:
    def _anomaly_level(self, seed_class: str) -> str:
        from build_anomaly_escalation_lite import build_anomaly_escalation
        from build_anomaly_action_queue_lite import build_anomaly_action_queue
        gate, dossier, review, packet = inputs_for_class(seed_class)
        esc = build_anomaly_escalation(gate, dossier, review, packet)
        return esc["anomaly_level"]

    def test_all_clear_gives_none(self):
        assert self._anomaly_level("ALL_CLEAR") == "NONE"

    def test_zero_intents_gives_low(self):
        assert self._anomaly_level("ZERO_INTENTS") == "LOW"

    def test_data_stale_gives_critical(self):
        assert self._anomaly_level("DATA_STALE") == "CRITICAL"

    def test_data_missing_gives_critical(self):
        assert self._anomaly_level("DATA_MISSING") == "CRITICAL"

    def test_hold_review_gives_medium(self):
        assert self._anomaly_level("HOLD_REVIEW") == "MEDIUM"

    def test_unknown_class_no_crash(self):
        gate, dossier, review, packet = inputs_for_class("UNKNOWN_CLASS")
        assert gate is not None

    def test_returns_4_tuple(self):
        result = inputs_for_class("ALL_CLEAR")
        assert len(result) == 4


# ---------------------------------------------------------------------------
# 3. Single market pipeline
# ---------------------------------------------------------------------------

class TestSingleMarket:
    def test_runs_without_error(self, tmp_path):
        p = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        assert result["markets_processed"] == 1

    def test_result_has_required_keys(self, tmp_path):
        p = _write_adapter(tmp_path, [_market("BTC-EUR", "ALL_CLEAR")])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        r = result["results"][0]
        for k in ("market", "seed_class", "anomaly_level", "action_status", "feedback"):
            assert k in r, f"missing key {k}"

    def test_market_name_preserved(self, tmp_path):
        p = _write_adapter(tmp_path, [_market("SOL-EUR", "ALL_CLEAR")])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        assert result["results"][0]["market"] == "SOL-EUR"

    def test_seed_class_preserved(self, tmp_path):
        p = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        assert result["results"][0]["seed_class"] == "DATA_STALE"


# ---------------------------------------------------------------------------
# 4. Anomaly level mapping via full run
# ---------------------------------------------------------------------------

class TestAnomalyMapping:
    def _level(self, tmp_path, seed_class):
        p = _write_adapter(tmp_path, [_market("BTC-EUR", seed_class)])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        return result["results"][0]["anomaly_level"]

    def test_all_clear_is_none(self, tmp_path):
        assert self._level(tmp_path, "ALL_CLEAR") == "NONE"

    def test_zero_intents_is_low(self, tmp_path):
        assert self._level(tmp_path, "ZERO_INTENTS") == "LOW"

    def test_data_stale_is_critical(self, tmp_path):
        assert self._level(tmp_path, "DATA_STALE") == "CRITICAL"

    def test_data_missing_is_critical(self, tmp_path):
        assert self._level(tmp_path, "DATA_MISSING") == "CRITICAL"

    def test_hold_review_is_medium(self, tmp_path):
        assert self._level(tmp_path, "HOLD_REVIEW") == "MEDIUM"


# ---------------------------------------------------------------------------
# 5. Feedback values
# ---------------------------------------------------------------------------

class TestFeedback:
    def test_feedback_valid_values(self, tmp_path):
        markets = [_market(f"MKT{i}-EUR", c) for i, c in enumerate(
            ["ALL_CLEAR", "ZERO_INTENTS", "DATA_STALE", "DATA_MISSING", "HOLD_REVIEW"]
        )]
        p = _write_adapter(tmp_path, markets)
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        valid = {"CONFIRM", "DISAGREE", "UNCERTAIN"}
        for r in result["results"]:
            assert r["feedback"] in valid

    def test_deterministic_output(self, tmp_path):
        markets = [_market("BTC-EUR", "DATA_STALE"), _market("ETH-EUR", "ALL_CLEAR")]
        p = _write_adapter(tmp_path, markets)
        r1 = run(adapter_path=p, log_path=tmp_path / "l1.jsonl",
                 analysis_path=tmp_path / "a1.json", dry_run=True)
        r2 = run(adapter_path=p, log_path=tmp_path / "l2.jsonl",
                 analysis_path=tmp_path / "a2.json", dry_run=True)
        assert [r["feedback"] for r in r1["results"]] == \
               [r["feedback"] for r in r2["results"]]


# ---------------------------------------------------------------------------
# 6. File write
# ---------------------------------------------------------------------------

class TestFileWrite:
    def test_log_written(self, tmp_path):
        p = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        log = tmp_path / "log.jsonl"
        run(adapter_path=p, log_path=log,
            analysis_path=tmp_path / "a.json", dry_run=False)
        assert log.exists()

    def test_log_entry_count_matches_markets(self, tmp_path):
        markets = [_market("BTC-EUR", "DATA_STALE"),
                   _market("ETH-EUR", "ALL_CLEAR"),
                   _market("SOL-EUR", "ZERO_INTENTS")]
        p   = _write_adapter(tmp_path, markets)
        log = tmp_path / "log.jsonl"
        run(adapter_path=p, log_path=log,
            analysis_path=tmp_path / "a.json", dry_run=False)
        lines = [l for l in log.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 3

    def test_log_entries_valid_json(self, tmp_path):
        p   = _write_adapter(tmp_path, [_market("BTC-EUR", "ALL_CLEAR")])
        log = tmp_path / "log.jsonl"
        run(adapter_path=p, log_path=log,
            analysis_path=tmp_path / "a.json", dry_run=False)
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entry = json.loads(line)
                assert "feedback_action" in entry

    def test_log_flags_correct(self, tmp_path):
        p   = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        log = tmp_path / "log.jsonl"
        run(adapter_path=p, log_path=log,
            analysis_path=tmp_path / "a.json", dry_run=False)
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entry = json.loads(line)
                f = entry["flags"]
                assert f["non_binding"]             is True
                assert f["simulation_only"]         is True
                assert f["paper_only"]              is True
                assert f["live_activation_allowed"] is False

    def test_analysis_file_created(self, tmp_path):
        p  = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        an = tmp_path / "analysis.json"
        run(adapter_path=p, log_path=tmp_path / "l.jsonl",
            analysis_path=an, dry_run=False)
        assert an.exists()

    def test_analysis_valid_json(self, tmp_path):
        p  = _write_adapter(tmp_path, [_market("BTC-EUR", "ALL_CLEAR")])
        an = tmp_path / "analysis.json"
        run(adapter_path=p, log_path=tmp_path / "l.jsonl",
            analysis_path=an, dry_run=False)
        data = json.loads(an.read_text(encoding="utf-8"))
        assert data["version"] == "feedback_analysis_v1"

    def test_analysis_flags_correct(self, tmp_path):
        p  = _write_adapter(tmp_path, [_market("BTC-EUR", "DATA_STALE")])
        an = tmp_path / "analysis.json"
        run(adapter_path=p, log_path=tmp_path / "l.jsonl",
            analysis_path=an, dry_run=False)
        data = json.loads(an.read_text(encoding="utf-8"))
        f = data["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_dry_run_no_files_written(self, tmp_path):
        p   = _write_adapter(tmp_path, [_market("BTC-EUR", "ALL_CLEAR")])
        log = tmp_path / "log.jsonl"
        an  = tmp_path / "analysis.json"
        run(adapter_path=p, log_path=log, analysis_path=an, dry_run=True)
        assert not log.exists()
        assert not an.exists()

    def test_analysis_none_in_dry_run(self, tmp_path):
        p  = _write_adapter(tmp_path, [_market("BTC-EUR", "ALL_CLEAR")])
        result = run(adapter_path=p, log_path=tmp_path / "l.jsonl",
                     analysis_path=tmp_path / "a.json", dry_run=True)
        assert result["analysis"] is None
