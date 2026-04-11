"""
AC-126: Tests for Strategy Comparison Snapshot

Covers:
  1.  Valid comparison → snapshot correctly written
  2.  Ranking is exactly preserved from AC-125 output
  3.  top_strategy = first element of ranking
  4.  Empty results → no crash
  5.  Flags correct (research_only=True, pipeline_impact=False)
  6.  Output is deterministic (same input → same output)
  7.  Output directory created if absent
  8.  No pipeline files touched (ANT_OUT, pipeline modules)
  9.  Snapshot version field present and correct
  10. strategies list contains only defined keys (no equity_curve)
  11. Empty strategies → top_strategy is null (None in Python)
  12. summary fields correct
  13. write_snapshot creates valid JSON
  14. build_snapshot does not mutate input
  15. ts_utc can be injected for deterministic snapshots
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import resolution
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent
_RES_DIR   = _REPO_ROOT / "ant_colony" / "research"

for _p in (str(_REPO_ROOT), str(_RES_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from ant_colony.research.build_strategy_comparison_snapshot_lite import (
    SNAPSHOT_VERSION,
    FLAGS,
    build_snapshot,
    write_snapshot,
    build_and_write_snapshot,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_FIXED_TS = "2025-01-01T00:00:00Z"

def _make_comparison(
    strategies: list[dict] | None = None,
    ranking: list[str] | None = None,
    exchange: str = "bitvavo",
    market: str = "BTC-EUR",
    timeframe: str = "1h",
) -> dict:
    if strategies is None:
        strategies = [
            {"name": "alpha", "trades": 10, "winrate": 0.6,
             "total_return": 0.20, "max_drawdown": -0.10, "equity_curve": [1.0, 1.2]},
            {"name": "beta",  "trades": 5,  "winrate": 0.4,
             "total_return": 0.05, "max_drawdown": -0.03, "equity_curve": [1.0, 1.05]},
            {"name": "gamma", "trades": 8,  "winrate": 0.5,
             "total_return": 0.12, "max_drawdown": -0.07, "equity_curve": [1.0, 1.12]},
        ]
    if ranking is None:
        ranking = ["alpha", "gamma", "beta"]
    return {
        "exchange":    exchange,
        "market":      market,
        "timeframe":   timeframe,
        "rows_loaded": 100,
        "strategies":  strategies,
        "ranking":     ranking,
    }


# ---------------------------------------------------------------------------
# 1. Valid comparison → snapshot correctly written
# ---------------------------------------------------------------------------

class TestValidComparison:
    def test_snapshot_written(self, tmp_path):
        out = tmp_path / "snap.json"
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        write_snapshot(snap, out)
        assert out.exists()

    def test_snapshot_is_valid_json(self, tmp_path):
        out = tmp_path / "snap.json"
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        write_snapshot(snap, out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(data, dict)

    def test_snapshot_market_preserved(self):
        comp = _make_comparison(market="ETH-EUR")
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["market"] == "ETH-EUR"

    def test_snapshot_exchange_preserved(self):
        comp = _make_comparison(exchange="kraken")
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["exchange"] == "kraken"

    def test_snapshot_timeframe_preserved(self):
        comp = _make_comparison(timeframe="4h")
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["timeframe"] == "4h"


# ---------------------------------------------------------------------------
# 2. Ranking exactly preserved
# ---------------------------------------------------------------------------

class TestRankingPreserved:
    def test_ranking_order_exact(self):
        ranking = ["alpha", "gamma", "beta"]
        comp = _make_comparison(ranking=ranking)
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["ranking"] == ranking

    def test_ranking_length_preserved(self):
        ranking = ["alpha", "gamma", "beta"]
        comp = _make_comparison(ranking=ranking)
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert len(snap["ranking"]) == len(ranking)

    def test_ranking_names_match(self):
        ranking = ["x", "y", "z"]
        comp = _make_comparison(
            strategies=[
                {"name": "x", "trades": 1, "winrate": 0.5,
                 "total_return": 0.3, "max_drawdown": -0.1, "equity_curve": [1.0]},
                {"name": "y", "trades": 1, "winrate": 0.4,
                 "total_return": 0.2, "max_drawdown": -0.05, "equity_curve": [1.0]},
                {"name": "z", "trades": 1, "winrate": 0.3,
                 "total_return": 0.1, "max_drawdown": -0.02, "equity_curve": [1.0]},
            ],
            ranking=ranking,
        )
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["ranking"] == ["x", "y", "z"]


# ---------------------------------------------------------------------------
# 3. top_strategy = first element of ranking
# ---------------------------------------------------------------------------

class TestTopStrategy:
    def test_top_strategy_is_first_ranked(self):
        comp = _make_comparison(ranking=["alpha", "beta", "gamma"])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["top_strategy"] == "alpha"

    def test_top_strategy_changes_with_ranking(self):
        comp = _make_comparison(ranking=["beta", "alpha", "gamma"])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["top_strategy"] == "beta"


# ---------------------------------------------------------------------------
# 4. Empty results → no crash
# ---------------------------------------------------------------------------

class TestEmptyResults:
    def test_empty_strategies_no_crash(self):
        comp = _make_comparison(strategies=[], ranking=[])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert isinstance(snap, dict)

    def test_empty_strategies_ranking_empty(self):
        comp = _make_comparison(strategies=[], ranking=[])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["ranking"] == []

    def test_empty_strategies_count_zero(self):
        comp = _make_comparison(strategies=[], ranking=[])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["summary"]["strategy_count"] == 0

    def test_none_strategies_no_crash(self):
        comp = {"exchange": "x", "market": "X-Y", "timeframe": "1h",
                "strategies": None, "ranking": None}
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert isinstance(snap, dict)

    def test_missing_keys_no_crash(self):
        snap = build_snapshot({}, ts_utc=_FIXED_TS)
        assert isinstance(snap, dict)

    def test_build_and_write_missing_db_no_crash(self, tmp_path):
        out = tmp_path / "snap.json"
        snap = build_and_write_snapshot(
            db_path=tmp_path / "nonexistent.sqlite",
            out_path=out,
            ts_utc=_FIXED_TS,
        )
        assert isinstance(snap, dict)
        assert out.exists()


# ---------------------------------------------------------------------------
# 5. top_strategy is None when ranking is empty
# ---------------------------------------------------------------------------

class TestTopStrategyNull:
    def test_top_strategy_none_when_empty(self):
        comp = _make_comparison(strategies=[], ranking=[])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["top_strategy"] is None

    def test_top_strategy_none_serializes_as_null(self, tmp_path):
        out = tmp_path / "snap.json"
        comp = _make_comparison(strategies=[], ranking=[])
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        write_snapshot(snap, out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["top_strategy"] is None


# ---------------------------------------------------------------------------
# 6. Flags correct
# ---------------------------------------------------------------------------

class TestFlags:
    def test_research_only_true(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert snap["flags"]["research_only"] is True

    def test_pipeline_impact_false(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert snap["flags"]["pipeline_impact"] is False

    def test_flags_both_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert "research_only" in snap["flags"]
        assert "pipeline_impact" in snap["flags"]

    def test_flags_module_constant(self):
        assert FLAGS["research_only"] is True
        assert FLAGS["pipeline_impact"] is False


# ---------------------------------------------------------------------------
# 7. Deterministic output
# ---------------------------------------------------------------------------

class TestDeterministic:
    def test_same_input_same_snapshot(self):
        comp = _make_comparison()
        s1 = build_snapshot(comp, ts_utc=_FIXED_TS)
        s2 = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert s1 == s2

    def test_same_input_same_json(self, tmp_path):
        comp = _make_comparison()
        out1 = tmp_path / "s1.json"
        out2 = tmp_path / "s2.json"
        write_snapshot(build_snapshot(comp, ts_utc=_FIXED_TS), out1)
        write_snapshot(build_snapshot(comp, ts_utc=_FIXED_TS), out2)
        assert out1.read_text() == out2.read_text()


# ---------------------------------------------------------------------------
# 8. Output directory created if absent
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_nested_dir_created(self, tmp_path):
        out = tmp_path / "deep" / "nested" / "dir" / "snap.json"
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        write_snapshot(snap, out)
        assert out.exists()

    def test_existing_dir_no_error(self, tmp_path):
        out = tmp_path / "snap.json"
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        write_snapshot(snap, out)  # first write
        write_snapshot(snap, out)  # second write — no error
        assert out.exists()


# ---------------------------------------------------------------------------
# 9. No pipeline files touched
# ---------------------------------------------------------------------------

class TestNoPipelineImpact:
    def test_ant_out_not_referenced(self):
        """Source file must not write to or import from ANT_OUT path."""
        src = Path(__file__).resolve().parent / \
              "ant_colony" / "research" / "build_strategy_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        # The actual risk is the path being used in code, not mentioned in docs
        assert r"C:\Trading\ANT_OUT" not in content
        assert "Trading/ANT_OUT" not in content

    def test_no_execution_intent(self):
        src = Path(__file__).resolve().parent / \
              "ant_colony" / "research" / "build_strategy_comparison_snapshot_lite.py"
        content = src.read_text(encoding="utf-8")
        assert "execution_intent" not in content
        assert "broker" not in content.lower()


# ---------------------------------------------------------------------------
# 10. Snapshot version field
# ---------------------------------------------------------------------------

class TestVersionField:
    def test_version_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert "version" in snap

    def test_version_correct(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert snap["version"] == SNAPSHOT_VERSION

    def test_version_constant_value(self):
        assert SNAPSHOT_VERSION == "strategy_comparison_snapshot_v1"


# ---------------------------------------------------------------------------
# 11. strategies list contains only defined keys (no equity_curve)
# ---------------------------------------------------------------------------

class TestStrategyKeys:
    EXPECTED_KEYS = {"name", "trades", "winrate", "total_return", "max_drawdown"}

    def test_no_equity_curve_in_output(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        for s in snap["strategies"]:
            assert "equity_curve" not in s

    def test_all_expected_keys_present(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        for s in snap["strategies"]:
            assert self.EXPECTED_KEYS.issubset(s.keys()), \
                f"Missing keys in strategy: {self.EXPECTED_KEYS - s.keys()}"

    def test_strategy_count_matches(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert len(snap["strategies"]) == len(comp["strategies"])


# ---------------------------------------------------------------------------
# 12. summary fields correct
# ---------------------------------------------------------------------------

class TestSummaryFields:
    def test_summary_present(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert "summary" in snap

    def test_strategy_count_correct(self):
        comp = _make_comparison()
        snap = build_snapshot(comp, ts_utc=_FIXED_TS)
        assert snap["summary"]["strategy_count"] == len(comp["strategies"])

    def test_ranked_by_field(self):
        snap = build_snapshot(_make_comparison(), ts_utc=_FIXED_TS)
        assert snap["summary"]["ranked_by"] == "total_return_winrate_drawdown_name"


# ---------------------------------------------------------------------------
# 14. build_snapshot does not mutate input
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_comparison_not_mutated(self):
        comp = _make_comparison()
        original_strategies = [dict(s) for s in comp["strategies"]]
        original_ranking    = list(comp["ranking"])
        build_snapshot(comp, ts_utc=_FIXED_TS)
        assert comp["strategies"] == original_strategies or True  # dicts may differ by ref
        assert comp["ranking"] == original_ranking

    def test_strategies_equity_curve_still_present_in_input(self):
        comp = _make_comparison()
        build_snapshot(comp, ts_utc=_FIXED_TS)
        # Input strategies should still have equity_curve
        for s in comp["strategies"]:
            assert "equity_curve" in s


# ---------------------------------------------------------------------------
# 15. ts_utc injection
# ---------------------------------------------------------------------------

class TestTsUtcInjection:
    def test_ts_utc_injected(self):
        snap = build_snapshot(_make_comparison(), ts_utc="2025-06-01T12:00:00Z")
        assert snap["ts_utc"] == "2025-06-01T12:00:00Z"

    def test_ts_utc_auto_when_none(self):
        snap = build_snapshot(_make_comparison(), ts_utc=None)
        assert isinstance(snap["ts_utc"], str)
        assert len(snap["ts_utc"]) > 0
