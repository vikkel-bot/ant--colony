"""
AC-103 tests — Paper Market Data Adapter → Scenario Input

Coverage:
  - missing sources → fail-closed (no crash, DATA_MISSING result)
  - corrupt sources → fail-closed (no crash)
  - fresh price + intents → ALL_CLEAR
  - fresh price + no intents → ZERO_INTENTS
  - stale price → DATA_STALE
  - missing price → DATA_MISSING
  - flags always correct
  - deterministic output (same input → same output)
  - output file written correctly
  - version and component fields correct
  - no file writes outside own output path
"""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from build_marketdata_scenario_adapter_lite import (
    classify_market,
    build_adapter,
    run_adapter,
    FLAGS,
    STALE_THRESHOLD_H,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

NOW = datetime.datetime(2026, 4, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)
FRESH_TS  = "2026-04-10T11:00:00Z"   # 1 h ago — fresh
STALE_TS  = "2026-04-07T09:45:00Z"   # ~75 h ago — stale


def _md_data(markets: dict) -> dict:
    return {
        "component":   "worker_market_data_refresh_lite",
        "ts_utc":      FRESH_TS,
        "market_count": len(markets),
        "markets": markets,
    }


def _price_entry(ts: str = FRESH_TS, price: float = 59823.0) -> dict:
    return {"market": "BTC-EUR", "last_price": price, "ts_utc": ts, "state": "OK"}


def _intents_data(markets: list[str]) -> dict:
    return {
        "component": "build_rebalance_intents_lite",
        "ts_utc":    FRESH_TS,
        "intents":   [{"market": m} for m in markets],
    }


# ---------------------------------------------------------------------------
# 1. classify_market — unit tests (pure function)
# ---------------------------------------------------------------------------

class TestClassifyMarket:
    def test_fresh_price_with_intents_is_all_clear(self):
        r = classify_market("BTC-EUR", _price_entry(FRESH_TS), {"BTC-EUR"}, NOW)
        assert r["review_seed_class"] == "ALL_CLEAR"
        assert r["data_state"]        == "OK"
        assert r["price_present"]     is True
        assert r["price_fresh"]       is True
        assert r["intents_present"]   is True

    def test_fresh_price_no_intents_is_zero_intents(self):
        r = classify_market("ETH-EUR", _price_entry(FRESH_TS, 1840.0), set(), NOW)
        assert r["review_seed_class"] == "ZERO_INTENTS"
        assert r["data_state"]        == "OK"
        assert r["intents_present"]   is False

    def test_stale_price_is_data_stale(self):
        r = classify_market("BTC-EUR", _price_entry(STALE_TS), {"BTC-EUR"}, NOW)
        assert r["review_seed_class"] == "DATA_STALE"
        assert r["data_state"]        == "STALE"
        assert r["price_present"]     is True
        assert r["price_fresh"]       is False

    def test_missing_entry_is_data_missing(self):
        r = classify_market("BTC-EUR", None, {"BTC-EUR"}, NOW)
        assert r["review_seed_class"] == "DATA_MISSING"
        assert r["data_state"]        == "MISSING"
        assert r["price_present"]     is False
        assert r["price_fresh"]       is False

    def test_zero_price_is_data_missing(self):
        r = classify_market("BTC-EUR", _price_entry(FRESH_TS, 0.0), set(), NOW)
        assert r["review_seed_class"] == "DATA_MISSING"
        assert r["data_state"]        == "MISSING"
        assert r["price_present"]     is False

    def test_empty_dict_entry_is_data_missing(self):
        r = classify_market("BTC-EUR", {}, set(), NOW)
        assert r["review_seed_class"] == "DATA_MISSING"

    def test_market_field_preserved(self):
        r = classify_market("SOL-EUR", _price_entry(FRESH_TS, 69.0), set(), NOW)
        assert r["market"] == "SOL-EUR"

    def test_borderline_fresh_exactly_at_threshold(self):
        # exactly at threshold → fresh
        borderline_ts = (NOW - datetime.timedelta(hours=STALE_THRESHOLD_H)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        r = classify_market("BTC-EUR", _price_entry(borderline_ts), set(), NOW)
        assert r["price_fresh"] is True
        assert r["data_state"]  == "OK"

    def test_just_over_threshold_is_stale(self):
        stale_ts = (
            NOW - datetime.timedelta(hours=STALE_THRESHOLD_H, seconds=1)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        r = classify_market("BTC-EUR", _price_entry(stale_ts), set(), NOW)
        assert r["price_fresh"] is False
        assert r["data_state"]  == "STALE"

    def test_bad_ts_format_treated_as_not_fresh(self):
        entry = {"last_price": 100.0, "ts_utc": "not-a-date"}
        r = classify_market("BTC-EUR", entry, set(), NOW)
        assert r["price_present"] is True
        assert r["price_fresh"]   is False
        assert r["data_state"]    == "STALE"


# ---------------------------------------------------------------------------
# 2. build_adapter — pure function tests
# ---------------------------------------------------------------------------

class TestBuildAdapter:
    def test_no_crash_on_none_sources(self):
        result = build_adapter(None, None, NOW)
        assert isinstance(result, dict)

    def test_empty_sources_produce_no_markets(self):
        result = build_adapter(None, None, NOW)
        assert result["markets"] == []

    def test_markets_sorted_alphabetically(self):
        md = _md_data({
            "XRP-EUR": _price_entry(FRESH_TS, 1.14),
            "BTC-EUR": _price_entry(FRESH_TS, 59823.0),
            "ETH-EUR": _price_entry(FRESH_TS, 1840.0),
        })
        result = build_adapter(md, None, NOW)
        names = [m["market"] for m in result["markets"]]
        assert names == sorted(names)

    def test_version_and_component_correct(self):
        result = build_adapter(None, None, NOW)
        assert result["version"]   == "marketdata_scenario_adapter_v1"
        assert result["component"] == "build_marketdata_scenario_adapter_lite"

    def test_ts_utc_present(self):
        result = build_adapter(None, None, NOW)
        assert "ts_utc" in result
        assert result["ts_utc"].startswith("2026")

    def test_flags_always_correct(self):
        result = build_adapter(None, None, NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_flags_correct_with_real_data(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS)})
        result = build_adapter(md, _intents_data(["BTC-EUR"]), NOW)
        f = result["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False

    def test_sources_loaded_fields(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS)})
        result = build_adapter(md, None, NOW)
        assert result["sources"]["market_data_loaded"]  is True
        assert result["sources"]["intents_data_loaded"] is False

    def test_all_clear_classification(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS)})
        it = _intents_data(["BTC-EUR"])
        result = build_adapter(md, it, NOW)
        m = result["markets"][0]
        assert m["review_seed_class"] == "ALL_CLEAR"

    def test_zero_intents_classification(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS)})
        result = build_adapter(md, None, NOW)  # no intents source
        m = result["markets"][0]
        assert m["review_seed_class"] == "ZERO_INTENTS"

    def test_stale_classification(self):
        md = _md_data({"BTC-EUR": _price_entry(STALE_TS)})
        result = build_adapter(md, _intents_data(["BTC-EUR"]), NOW)
        m = result["markets"][0]
        assert m["review_seed_class"] == "DATA_STALE"

    def test_missing_classification(self):
        md = _md_data({"BTC-EUR": {"last_price": 0.0, "ts_utc": FRESH_TS}})
        result = build_adapter(md, _intents_data(["BTC-EUR"]), NOW)
        m = result["markets"][0]
        assert m["review_seed_class"] == "DATA_MISSING"

    def test_multiple_markets(self):
        md = _md_data({
            "BTC-EUR": _price_entry(FRESH_TS, 59823.0),
            "ETH-EUR": _price_entry(STALE_TS, 1840.0),
            "SOL-EUR": {"last_price": 0.0, "ts_utc": FRESH_TS},
        })
        it = _intents_data(["BTC-EUR", "ETH-EUR", "SOL-EUR"])
        result = build_adapter(md, it, NOW)
        by_mkt = {m["market"]: m for m in result["markets"]}
        assert by_mkt["BTC-EUR"]["review_seed_class"] == "ALL_CLEAR"
        assert by_mkt["ETH-EUR"]["review_seed_class"] == "DATA_STALE"
        assert by_mkt["SOL-EUR"]["review_seed_class"] == "DATA_MISSING"

    def test_intent_market_not_in_price_data_is_missing(self):
        """A market with an intent but no price → DATA_MISSING."""
        result = build_adapter(None, _intents_data(["UNKNOWN-EUR"]), NOW)
        assert len(result["markets"]) == 1
        assert result["markets"][0]["review_seed_class"] == "DATA_MISSING"

    def test_deterministic_same_input_same_output(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS), "ETH-EUR": _price_entry(FRESH_TS, 1840.0)})
        it = _intents_data(["BTC-EUR"])
        r1 = build_adapter(md, it, NOW)
        r2 = build_adapter(md, it, NOW)
        assert r1["markets"] == r2["markets"]

    def test_corrupt_md_data_no_crash(self):
        result = build_adapter({"not": "expected"}, None, NOW)
        assert isinstance(result, dict)

    def test_corrupt_intents_data_no_crash(self):
        md = _md_data({"BTC-EUR": _price_entry(FRESH_TS)})
        result = build_adapter(md, {"not": "expected"}, NOW)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 3. run_adapter — I/O integration (uses tmp_path)
# ---------------------------------------------------------------------------

class TestRunAdapter:
    def test_missing_source_files_no_crash(self, tmp_path):
        result = run_adapter(
            md_path      = tmp_path / "nonexistent_md.json",
            intents_path = tmp_path / "nonexistent_intents.json",
            output_path  = tmp_path / "out.json",
            now_utc      = NOW,
        )
        assert isinstance(result, dict)

    def test_output_file_written(self, tmp_path):
        out = tmp_path / "out.json"
        run_adapter(
            md_path      = tmp_path / "nonexistent.json",
            intents_path = tmp_path / "nonexistent.json",
            output_path  = out,
            now_utc      = NOW,
        )
        assert out.exists()

    def test_output_is_valid_json(self, tmp_path):
        out = tmp_path / "out.json"
        run_adapter(
            md_path      = tmp_path / "nonexistent.json",
            intents_path = tmp_path / "nonexistent.json",
            output_path  = out,
            now_utc      = NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["version"] == "marketdata_scenario_adapter_v1"

    def test_corrupt_md_file_no_crash(self, tmp_path):
        bad = tmp_path / "md.json"
        bad.write_text("{ not valid {{{", encoding="utf-8")
        out = tmp_path / "out.json"
        run_adapter(md_path=bad, intents_path=tmp_path / "x.json",
                    output_path=out, now_utc=NOW)
        assert out.exists()

    def test_real_sources_classify_all_markets(self, tmp_path):
        md = _md_data({
            "ADA-EUR": _price_entry(FRESH_TS, 0.21),
            "BTC-EUR": _price_entry(FRESH_TS, 59823.0),
            "ETH-EUR": _price_entry(FRESH_TS, 1840.0),
        })
        it = _intents_data(["BTC-EUR", "ADA-EUR"])
        md_f = tmp_path / "md.json"
        it_f = tmp_path / "intents.json"
        md_f.write_text(json.dumps(md), encoding="utf-8")
        it_f.write_text(json.dumps(it), encoding="utf-8")
        out = tmp_path / "out.json"
        result = run_adapter(md_path=md_f, intents_path=it_f,
                             output_path=out, now_utc=NOW)
        by_mkt = {m["market"]: m for m in result["markets"]}
        assert by_mkt["BTC-EUR"]["review_seed_class"] == "ALL_CLEAR"
        assert by_mkt["ADA-EUR"]["review_seed_class"] == "ALL_CLEAR"
        assert by_mkt["ETH-EUR"]["review_seed_class"] == "ZERO_INTENTS"

    def test_no_extra_files_written(self, tmp_path):
        out = tmp_path / "out.json"
        before = set(tmp_path.iterdir())
        run_adapter(
            md_path      = tmp_path / "nonexistent.json",
            intents_path = tmp_path / "nonexistent.json",
            output_path  = out,
            now_utc      = NOW,
        )
        after = set(tmp_path.iterdir())
        new_files = after - before
        assert new_files == {out}, f"unexpected extra files: {new_files - {out}}"

    def test_flags_in_written_output(self, tmp_path):
        out = tmp_path / "out.json"
        run_adapter(
            md_path=tmp_path / "x.json", intents_path=tmp_path / "x.json",
            output_path=out, now_utc=NOW,
        )
        data = json.loads(out.read_text(encoding="utf-8"))
        f = data["flags"]
        assert f["non_binding"]             is True
        assert f["simulation_only"]         is True
        assert f["paper_only"]              is True
        assert f["live_activation_allowed"] is False
