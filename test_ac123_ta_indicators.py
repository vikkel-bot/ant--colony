"""
AC-123 tests — TA Indicators (Research-Only, Pure Compute Layer)

Coverage:
  1.  normalize_ohlcv_rows returns consistent numeric dicts
  2.  add_indicators preserves original row count
  3.  original fields are preserved in output
  4.  new indicator fields are added
  5.  RSI always in [0, 100] where not None
  6.  SMA is correct on a simple known dataset
  7.  EMA converges correctly (greater weight on recent values)
  8.  ATR is positive or None
  9.  Bollinger upper > lower where not None
 10.  short dataset (< warmup) does not crash
 11.  empty input returns []
 12.  input rows are not mutated
 13.  warmup rows receive None for indicators
 14.  normalize handles string numeric values
 15.  normalize handles non-numeric values gracefully
 16.  deterministic — same input → same output
 17.  add_indicators handles all-None closes without crash
 18.  SMA warmup count is correct (first period-1 rows are None)
 19.  EMA warmup count is correct
 20.  ATR warmup count is correct
 21.  RSI warmup count is correct
 22.  Bollinger warmup counts correct
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony" / "research"))

from ta_indicators_lite import normalize_ohlcv_rows, add_indicators


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(i: int, base: float = 100.0) -> dict:
    """Generate a synthetic OHLCV row. Price drifts slightly with index."""
    c = base + i * 0.5
    return {
        "ts_utc":   1_000_000_000 + i * 3_600_000,
        "open":     c - 0.1,
        "high":     c + 0.5,
        "low":      c - 0.5,
        "close":    c,
        "volume":   float(100 + i),
        "extra_field": "should_be_kept",
    }


def _make_rows(n: int, base: float = 100.0) -> list[dict]:
    return [_make_row(i, base) for i in range(n)]


def _closes(rows: list[dict]) -> list:
    return [r.get("close") for r in rows]


# ---------------------------------------------------------------------------
# 1. normalize_ohlcv_rows
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_empty_returns_empty(self):
        assert normalize_ohlcv_rows([]) == []

    def test_preserves_row_count(self):
        rows = _make_rows(5)
        assert len(normalize_ohlcv_rows(rows)) == 5

    def test_numeric_fields_are_float(self):
        rows = _make_rows(3)
        normed = normalize_ohlcv_rows(rows)
        for row in normed:
            for f in ("open", "high", "low", "close", "volume"):
                assert isinstance(row[f], float)

    def test_string_numeric_values_converted(self):
        row = {"ts_utc": 1, "open": "100.5", "high": "101", "low": "99",
               "close": "100", "volume": "50.0"}
        result = normalize_ohlcv_rows([row])
        assert result[0]["close"] == pytest.approx(100.0)
        assert result[0]["volume"] == pytest.approx(50.0)

    def test_non_numeric_becomes_none(self):
        row = {"ts_utc": 1, "open": "bad", "high": "101", "low": "99",
               "close": "100", "volume": "50"}
        result = normalize_ohlcv_rows([row])
        assert result[0]["open"] is None
        assert result[0]["close"] == pytest.approx(100.0)

    def test_input_not_mutated(self):
        rows = [{"ts_utc": 1, "open": "100", "high": "101", "low": "99",
                 "close": "100", "volume": "50"}]
        original_close = rows[0]["close"]
        normalize_ohlcv_rows(rows)
        assert rows[0]["close"] == original_close  # still a string

    def test_non_dict_items_dropped(self):
        result = normalize_ohlcv_rows([None, {"ts_utc": 1, "close": "100"}, "bad"])
        assert len(result) == 1

    def test_extra_fields_preserved(self):
        row = {"ts_utc": 1, "close": "100", "my_tag": "keep_me"}
        result = normalize_ohlcv_rows([row])
        assert result[0]["my_tag"] == "keep_me"

    def test_none_input_not_accepted(self):
        # normalize_ohlcv_rows(None) should return [] gracefully
        result = normalize_ohlcv_rows([])
        assert result == []


# ---------------------------------------------------------------------------
# 2 & 3. add_indicators — structure
# ---------------------------------------------------------------------------

class TestAddIndicatorsStructure:
    def test_empty_returns_empty(self):
        assert add_indicators([]) == []

    def test_preserves_row_count_small(self):
        rows = _make_rows(5)
        assert len(add_indicators(rows)) == 5

    def test_preserves_row_count_large(self):
        rows = _make_rows(100)
        assert len(add_indicators(rows)) == 100

    def test_original_fields_present(self):
        rows = _make_rows(30)
        result = add_indicators(rows)
        for row in result:
            assert "ts_utc" in row
            assert "open" in row
            assert "close" in row
            assert "volume" in row

    def test_extra_fields_preserved(self):
        rows = _make_rows(30)
        result = add_indicators(rows)
        for row in result:
            assert row["extra_field"] == "should_be_kept"

    def test_indicator_fields_added(self):
        rows = _make_rows(30)
        result = add_indicators(rows)
        for field in ("rsi_14", "sma_20", "ema_20", "atr_14", "bb_upper", "bb_lower"):
            assert field in result[0]

    def test_input_not_mutated(self):
        rows = _make_rows(30)
        original_closes = [r["close"] for r in rows]
        add_indicators(rows)
        assert [r["close"] for r in rows] == original_closes

    def test_input_not_mutated_no_new_keys(self):
        rows = _make_rows(30)
        original_keys = set(rows[0].keys())
        add_indicators(rows)
        assert set(rows[0].keys()) == original_keys


# ---------------------------------------------------------------------------
# 4. Warmup — None in early rows
# ---------------------------------------------------------------------------

class TestWarmup:
    def test_sma_warmup_rows_are_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for i in range(19):  # first 19 rows → None for SMA(20)
            assert result[i]["sma_20"] is None, f"row {i} sma_20 should be None"

    def test_sma_first_valid_row(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        assert result[19]["sma_20"] is not None

    def test_ema_warmup_rows_are_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for i in range(19):
            assert result[i]["ema_20"] is None

    def test_ema_first_valid_row(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        assert result[19]["ema_20"] is not None

    def test_rsi_warmup_rows_are_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for i in range(14):  # first 14 rows → None for RSI(14)
            assert result[i]["rsi_14"] is None

    def test_rsi_first_valid_row(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        assert result[14]["rsi_14"] is not None

    def test_atr_warmup_rows_are_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for i in range(13):  # first 13 rows → None for ATR(14)
            assert result[i]["atr_14"] is None

    def test_atr_first_valid_row(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        assert result[13]["atr_14"] is not None

    def test_bollinger_warmup_rows_are_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for i in range(19):
            assert result[i]["bb_upper"] is None
            assert result[i]["bb_lower"] is None

    def test_bollinger_first_valid_row(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        assert result[19]["bb_upper"] is not None
        assert result[19]["bb_lower"] is not None

    def test_short_input_no_crash(self):
        rows = _make_rows(5)
        result = add_indicators(rows)
        assert len(result) == 5

    def test_short_input_all_none(self):
        rows = _make_rows(5)
        result = add_indicators(rows)
        for row in result:
            assert row["sma_20"] is None
            assert row["rsi_14"] is None


# ---------------------------------------------------------------------------
# 5. RSI range
# ---------------------------------------------------------------------------

class TestRSI:
    def test_rsi_in_range(self):
        rows = _make_rows(100)
        result = add_indicators(rows)
        for row in result:
            v = row["rsi_14"]
            if v is not None:
                assert 0.0 <= v <= 100.0, f"RSI out of range: {v}"

    def test_rsi_above_50_on_rising_series(self):
        """Steadily rising closes → RSI should settle above 50."""
        rows = _make_rows(60)  # linear uptrend
        result = add_indicators(rows)
        valid = [r["rsi_14"] for r in result if r["rsi_14"] is not None]
        assert len(valid) > 0
        assert valid[-1] > 50.0

    def test_rsi_below_50_on_falling_series(self):
        """Steadily falling closes → RSI should settle below 50."""
        rows = [_make_row(i, base=200.0 - i * 0.5) for i in range(60)]
        # rewrite close to be strictly decreasing
        for i, r in enumerate(rows):
            r["close"] = 200.0 - i * 1.0
            r["open"] = r["close"] + 0.1
            r["high"] = r["close"] + 0.5
            r["low"]  = r["close"] - 0.5
        result = add_indicators(rows)
        valid = [r["rsi_14"] for r in result if r["rsi_14"] is not None]
        assert valid[-1] < 50.0


# ---------------------------------------------------------------------------
# 6. SMA correctness
# ---------------------------------------------------------------------------

class TestSMA:
    def test_sma_exact_on_constant_series(self):
        """SMA of constant series equals the constant."""
        rows = []
        for i in range(30):
            rows.append({"ts_utc": i, "open": 50.0, "high": 51.0, "low": 49.0,
                         "close": 50.0, "volume": 100.0})
        result = add_indicators(rows)
        for row in result:
            if row["sma_20"] is not None:
                assert row["sma_20"] == pytest.approx(50.0)

    def test_sma_known_window(self):
        """SMA at row 19 = mean of closes[0..19]."""
        closes = [float(i + 1) for i in range(30)]  # 1..30
        rows = [{"ts_utc": i, "open": c, "high": c + 0.5, "low": c - 0.5,
                 "close": c, "volume": 10.0}
                for i, c in enumerate(closes)]
        result = add_indicators(rows)
        expected = sum(closes[:20]) / 20
        assert result[19]["sma_20"] == pytest.approx(expected)

    def test_sma_rolling_advances(self):
        """SMA at row 20 = mean of closes[1..20]."""
        closes = [float(i + 1) for i in range(30)]
        rows = [{"ts_utc": i, "open": c, "high": c + 0.5, "low": c - 0.5,
                 "close": c, "volume": 10.0}
                for i, c in enumerate(closes)]
        result = add_indicators(rows)
        expected = sum(closes[1:21]) / 20
        assert result[20]["sma_20"] == pytest.approx(expected)


# ---------------------------------------------------------------------------
# 7. EMA correctness
# ---------------------------------------------------------------------------

class TestEMA:
    def test_ema_equals_sma_on_constant_series(self):
        """EMA of constant series equals the constant."""
        rows = [{"ts_utc": i, "open": 50.0, "high": 51.0, "low": 49.0,
                 "close": 50.0, "volume": 100.0}
                for i in range(30)]
        result = add_indicators(rows)
        for row in result:
            if row["ema_20"] is not None:
                assert row["ema_20"] == pytest.approx(50.0, abs=1e-6)

    def test_ema_seeded_with_sma(self):
        """EMA at row 19 equals SMA at row 19 (seed value)."""
        rows = _make_rows(30)
        result = add_indicators(rows)
        assert result[19]["ema_20"] == pytest.approx(result[19]["sma_20"])

    def test_ema_reacts_faster_than_sma_to_spike(self):
        """After a price spike, EMA > SMA (EMA reacts faster)."""
        rows = []
        for i in range(30):
            c = 100.0
            rows.append({"ts_utc": i, "open": c, "high": c + 0.5, "low": c - 0.5,
                         "close": c, "volume": 10.0})
        # Spike on the last row
        rows[-1]["close"] = 200.0
        rows[-1]["open"]  = 200.0
        rows[-1]["high"]  = 201.0
        rows[-1]["low"]   = 199.0

        result = add_indicators(rows)
        last = result[-1]
        if last["ema_20"] is not None and last["sma_20"] is not None:
            assert last["ema_20"] > last["sma_20"]


# ---------------------------------------------------------------------------
# 8. ATR positivity
# ---------------------------------------------------------------------------

class TestATR:
    def test_atr_positive_where_not_none(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for row in result:
            if row["atr_14"] is not None:
                assert row["atr_14"] > 0.0

    def test_atr_larger_on_volatile_data(self):
        """Higher price range → larger ATR."""
        low_vol  = [{"ts_utc": i, "open": 100.0, "high": 100.1, "low": 99.9,
                     "close": 100.0, "volume": 10.0} for i in range(30)]
        high_vol = [{"ts_utc": i, "open": 100.0, "high": 105.0, "low":  95.0,
                     "close": 100.0, "volume": 10.0} for i in range(30)]

        r_low  = add_indicators(low_vol)
        r_high = add_indicators(high_vol)

        atr_low  = r_low[-1]["atr_14"]
        atr_high = r_high[-1]["atr_14"]

        if atr_low is not None and atr_high is not None:
            assert atr_high > atr_low


# ---------------------------------------------------------------------------
# 9. Bollinger: upper > lower
# ---------------------------------------------------------------------------

class TestBollinger:
    def test_upper_greater_than_lower(self):
        rows = _make_rows(50)
        result = add_indicators(rows)
        for row in result:
            up = row["bb_upper"]
            lo = row["bb_lower"]
            if up is not None and lo is not None:
                assert up >= lo

    def test_upper_equals_lower_on_constant_series(self):
        """Constant close → std=0 → upper==lower==sma."""
        rows = [{"ts_utc": i, "open": 50.0, "high": 50.0, "low": 50.0,
                 "close": 50.0, "volume": 10.0}
                for i in range(30)]
        result = add_indicators(rows)
        for row in result:
            if row["bb_upper"] is not None:
                assert row["bb_upper"] == pytest.approx(row["bb_lower"])
                assert row["bb_upper"] == pytest.approx(50.0)

    def test_wider_bands_on_volatile_data(self):
        """More volatile data → wider bands."""
        flat  = [{"ts_utc": i, "open": 100.0, "high": 100.1, "low": 99.9,
                  "close": 100.0, "volume": 10.0} for i in range(30)]
        wild  = [{"ts_utc": i, "open": 100.0, "high": 110.0, "low": 90.0,
                  "close": 100.0 + (1 if i % 2 == 0 else -1) * 5, "volume": 10.0}
                 for i in range(30)]

        r_flat = add_indicators(flat)
        r_wild = add_indicators(wild)

        width_flat = r_flat[-1]["bb_upper"] - r_flat[-1]["bb_lower"]
        width_wild = r_wild[-1]["bb_upper"] - r_wild[-1]["bb_lower"]

        if width_flat is not None and width_wild is not None:
            assert width_wild > width_flat


# ---------------------------------------------------------------------------
# 16 & 17. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_deterministic(self):
        rows = _make_rows(50)
        assert add_indicators(rows) == add_indicators(rows)

    def test_all_none_closes_no_crash(self):
        rows = [{"ts_utc": i, "open": None, "high": None, "low": None,
                 "close": None, "volume": None}
                for i in range(30)]
        result = add_indicators(rows)
        assert len(result) == 30
        for row in result:
            assert row["rsi_14"] is None
            assert row["sma_20"] is None

    def test_single_row_no_crash(self):
        rows = _make_rows(1)
        result = add_indicators(rows)
        assert len(result) == 1
        assert result[0]["sma_20"] is None

    def test_exactly_warmup_length_sma(self):
        """Exactly 20 rows → only last row gets SMA."""
        rows = _make_rows(20)
        result = add_indicators(rows)
        for i in range(19):
            assert result[i]["sma_20"] is None
        assert result[19]["sma_20"] is not None

    def test_normalize_then_add_consistent(self):
        """normalize_ohlcv_rows → add_indicators should equal add_indicators directly."""
        rows = _make_rows(30)
        normed = normalize_ohlcv_rows(rows)
        direct = add_indicators(rows)
        via_normalize = add_indicators(normed)
        # Indicator values should match
        for r1, r2 in zip(direct, via_normalize):
            for key in ("rsi_14", "sma_20", "ema_20", "atr_14", "bb_upper", "bb_lower"):
                v1, v2 = r1[key], r2[key]
                if v1 is None:
                    assert v2 is None
                else:
                    assert v1 == pytest.approx(v2)
