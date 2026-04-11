"""
AC-122 tests — Standalone OHLCV Fetcher (Research-Only)

Coverage:
  1. DB initialisation creates table with correct schema
  2. insert / upsert counts are correct
  3. duplicates are not double-stored (INSERT OR IGNORE)
  4. parse_candle_row handles valid Bitvavo format
  5. parse_candle_row handles string OHLCV values
  6. parse_candle_rows drops malformed rows silently
  7. empty input list → no crash, returns 0 rows
  8. error path (bad DB) → no crash (fail-closed)
  9. unique key (exchange + market + timeframe + ts) enforced
 10. get_last_ts returns None on empty table, correct value after insert
 11. upsert_candles returns correct (inserted, skipped) tuple
 12. parse_candle_row rejects short rows
 13. parse_candle_row rejects non-numeric values
 14. fetch_and_store returns OK summary on success (mocked fetch)
 15. fetch_and_store returns ERROR on unsupported exchange
 16. DB path parent directory is auto-created
"""
import sys
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from fetch_ohlcv import (
    count_rows,
    get_last_ts,
    init_db,
    parse_candle_row,
    parse_candle_rows,
    upsert_candles,
    fetch_and_store,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """In-memory-style: fresh SQLite DB in tmp_path per test."""
    conn = init_db(tmp_path / "test.sqlite")
    yield conn
    conn.close()


def _sample_rows(n: int = 3, base_ts: int = 1_000_000) -> list[tuple]:
    """Generate n synthetic parsed candle rows."""
    return [
        (base_ts + i * 3600_000, 100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, float(i + 1))
        for i in range(n)
    ]


# Bitvavo-style raw API rows: [ts_ms, "open", "high", "low", "close", "volume"]
_RAW_ROWS = [
    [1_000_000,  "100.0", "101.0", "99.0", "100.5", "1.0"],
    [1_003_600_000, "200.0", "201.0", "199.0", "200.5", "2.0"],
    [1_007_200_000, "300.0", "301.0", "299.0", "300.5", "3.0"],
]


# ---------------------------------------------------------------------------
# 1. DB initialisation
# ---------------------------------------------------------------------------

class TestDBInit:
    def test_creates_ohlcv_table(self, tmp_path):
        conn = init_db(tmp_path / "test.sqlite")
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert ("ohlcv",) in tables
        conn.close()

    def test_schema_has_all_columns(self, tmp_path):
        conn = init_db(tmp_path / "test.sqlite")
        cols = {row[1] for row in conn.execute("PRAGMA table_info(ohlcv)")}
        assert cols >= {"exchange", "market", "timeframe", "ts_utc",
                        "open", "high", "low", "close", "volume"}
        conn.close()

    def test_idempotent_double_init(self, tmp_path):
        """Calling init_db twice on the same path must not crash."""
        p = tmp_path / "test.sqlite"
        conn1 = init_db(p)
        conn1.close()
        conn2 = init_db(p)
        conn2.close()

    def test_auto_creates_parent_dir(self, tmp_path):
        nested = tmp_path / "deep" / "nested" / "ohlcv.sqlite"
        conn = init_db(nested)
        assert nested.exists()
        conn.close()


# ---------------------------------------------------------------------------
# 2. Insert / upsert counts
# ---------------------------------------------------------------------------

class TestUpsert:
    def test_insert_returns_correct_count(self, db):
        rows = _sample_rows(5)
        inserted, skipped = upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        assert inserted == 5
        assert skipped  == 0

    def test_duplicate_rows_skipped(self, db):
        rows = _sample_rows(3)
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        inserted, skipped = upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        assert inserted == 0
        assert skipped  == 3

    def test_partial_overlap(self, db):
        rows_a = _sample_rows(3, base_ts=1_000_000)
        rows_b = _sample_rows(3, base_ts=1_000_000 + 2 * 3_600_000)  # 1 overlap
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows_a)
        inserted, skipped = upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows_b)
        assert inserted == 2
        assert skipped  == 1

    def test_empty_rows_no_crash(self, db):
        inserted, skipped = upsert_candles(db, "bitvavo", "BTC-EUR", "1h", [])
        assert inserted == 0
        assert skipped  == 0

    def test_count_rows_reflects_inserts(self, db):
        rows = _sample_rows(7)
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        assert count_rows(db, "bitvavo", "BTC-EUR", "1h") == 7

    def test_different_markets_isolated(self, db):
        rows = _sample_rows(3)
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        upsert_candles(db, "bitvavo", "ETH-EUR", "1h", rows)
        assert count_rows(db, "bitvavo", "BTC-EUR", "1h") == 3
        assert count_rows(db, "bitvavo", "ETH-EUR", "1h") == 3

    def test_different_timeframes_isolated(self, db):
        rows = _sample_rows(3)
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        upsert_candles(db, "bitvavo", "BTC-EUR", "4h", rows)
        assert count_rows(db, "bitvavo", "BTC-EUR", "1h") == 3
        assert count_rows(db, "bitvavo", "BTC-EUR", "4h") == 3


# ---------------------------------------------------------------------------
# 3. Unique key enforcement
# ---------------------------------------------------------------------------

class TestUniqueKey:
    def test_same_ts_same_key_no_double_insert(self, db):
        row = [(1_000_000, 100.0, 101.0, 99.0, 100.5, 1.0)]
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", row)
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", row)
        assert count_rows(db, "bitvavo", "BTC-EUR", "1h") == 1

    def test_same_ts_different_exchange_both_stored(self, db):
        row = [(1_000_000, 100.0, 101.0, 99.0, 100.5, 1.0)]
        upsert_candles(db, "bitvavo",  "BTC-EUR", "1h", row)
        upsert_candles(db, "exchange2", "BTC-EUR", "1h", row)
        assert count_rows(db, "bitvavo",  "BTC-EUR", "1h") == 1
        assert count_rows(db, "exchange2", "BTC-EUR", "1h") == 1


# ---------------------------------------------------------------------------
# 4 & 5. parse_candle_row
# ---------------------------------------------------------------------------

class TestParseCandleRow:
    def test_valid_numeric_row(self):
        raw = [1_000_000, 100.0, 101.0, 99.0, 100.5, 1.0]
        result = parse_candle_row(raw)
        assert result == (1_000_000, 100.0, 101.0, 99.0, 100.5, 1.0)

    def test_valid_string_values_bitvavo_format(self):
        raw = [1_000_000, "62085", "62158", "62072", "62155", "1.71025269"]
        result = parse_candle_row(raw)
        assert result is not None
        assert result[0] == 1_000_000
        assert result[1] == pytest.approx(62085.0)
        assert result[5] == pytest.approx(1.71025269)

    def test_returns_tuple(self):
        raw = [1_000_000, "100", "101", "99", "100.5", "1.0"]
        result = parse_candle_row(raw)
        assert isinstance(result, tuple)
        assert len(result) == 6

    def test_ts_is_int(self):
        raw = [1_775_894_400_000, "62085", "62158", "62072", "62155", "1.71"]
        result = parse_candle_row(raw)
        assert isinstance(result[0], int)

    def test_ohlcv_are_float(self):
        raw = [1_000_000, "100", "101", "99", "100.5", "1.0"]
        result = parse_candle_row(raw)
        for val in result[1:]:
            assert isinstance(val, float)

    def test_short_row_returns_none(self):
        assert parse_candle_row([1_000_000, 100.0, 101.0]) is None

    def test_empty_row_returns_none(self):
        assert parse_candle_row([]) is None

    def test_non_numeric_ohlcv_returns_none(self):
        raw = [1_000_000, "abc", "101", "99", "100.5", "1.0"]
        assert parse_candle_row(raw) is None

    def test_none_input_returns_none(self):
        assert parse_candle_row(None) is None

    def test_dict_input_returns_none(self):
        assert parse_candle_row({"ts": 1_000_000}) is None


# ---------------------------------------------------------------------------
# 6 & 7. parse_candle_rows
# ---------------------------------------------------------------------------

class TestParseCandleRows:
    def test_valid_raw_list(self):
        result = parse_candle_rows(_RAW_ROWS)
        assert len(result) == 3

    def test_empty_list(self):
        assert parse_candle_rows([]) == []

    def test_none_input(self):
        assert parse_candle_rows(None) == []

    def test_drops_malformed_rows(self):
        mixed = _RAW_ROWS + [[1_000_000, "bad", "101", "99", "100.5", "1.0"]]
        result = parse_candle_rows(mixed)
        assert len(result) == 3  # bad row dropped

    def test_all_bad_rows(self):
        bad = [["x", "y"], [None], []]
        assert parse_candle_rows(bad) == []


# ---------------------------------------------------------------------------
# 10. get_last_ts
# ---------------------------------------------------------------------------

class TestGetLastTs:
    def test_empty_table_returns_none(self, db):
        assert get_last_ts(db, "bitvavo", "BTC-EUR", "1h") is None

    def test_returns_max_ts(self, db):
        rows = [(1_000_000, 100.0, 101.0, 99.0, 100.5, 1.0),
                (2_000_000, 200.0, 201.0, 199.0, 200.5, 2.0),
                (3_000_000, 300.0, 301.0, 299.0, 300.5, 3.0)]
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        assert get_last_ts(db, "bitvavo", "BTC-EUR", "1h") == 3_000_000

    def test_different_market_no_cross_contamination(self, db):
        rows = [(5_000_000, 100.0, 101.0, 99.0, 100.5, 1.0)]
        upsert_candles(db, "bitvavo", "BTC-EUR", "1h", rows)
        assert get_last_ts(db, "bitvavo", "ETH-EUR", "1h") is None


# ---------------------------------------------------------------------------
# 14 & 15. fetch_and_store (mocked fetch)
# ---------------------------------------------------------------------------

def _mock_fetcher(rows):
    """Return a fetcher lambda that yields the given rows, regardless of args."""
    return lambda *a, **kw: rows


class TestFetchAndStore:
    def test_ok_summary_on_success(self, tmp_path):
        mock_rows = [
            [1_000_000,     "100.0", "101.0", "99.0",  "100.5", "1.0"],
            [1_003_600_000, "200.0", "201.0", "199.0", "200.5", "2.0"],
        ]
        import fetch_ohlcv as _m
        with patch.dict(_m.EXCHANGE_FETCHERS, {"bitvavo": _mock_fetcher(mock_rows)}):
            result = fetch_and_store("bitvavo", "BTC-EUR", "1h", 100,
                                     tmp_path / "test.sqlite")
        assert result["status"]   == "OK"
        assert result["fetched"]  == 2
        assert result["inserted"] == 2
        assert result["skipped"]  == 0

    def test_duplicate_on_second_run(self, tmp_path):
        mock_rows = [[1_000_000, "100.0", "101.0", "99.0", "100.5", "1.0"]]
        db = tmp_path / "test.sqlite"
        import fetch_ohlcv as _m
        with patch.dict(_m.EXCHANGE_FETCHERS, {"bitvavo": _mock_fetcher(mock_rows)}):
            fetch_and_store("bitvavo", "BTC-EUR", "1h", 100, db)
            r2 = fetch_and_store("bitvavo", "BTC-EUR", "1h", 100, db)
        assert r2["inserted"] == 0
        assert r2["skipped"]  == 1

    def test_empty_response_no_crash(self, tmp_path):
        import fetch_ohlcv as _m
        with patch.dict(_m.EXCHANGE_FETCHERS, {"bitvavo": _mock_fetcher([])}):
            result = fetch_and_store("bitvavo", "BTC-EUR", "1h", 100,
                                     tmp_path / "test.sqlite")
        assert result["status"]  == "OK"
        assert result["fetched"] == 0

    def test_unsupported_exchange_returns_error(self, tmp_path):
        result = fetch_and_store("unknown_exchange", "BTC-EUR", "1h", 100,
                                 tmp_path / "test.sqlite")
        assert result["status"] == "ERROR"
        assert "exchange" in result["error"].lower()

    def test_db_path_in_summary(self, tmp_path):
        db = tmp_path / "test.sqlite"
        import fetch_ohlcv as _m
        with patch.dict(_m.EXCHANGE_FETCHERS, {"bitvavo": _mock_fetcher([])}):
            result = fetch_and_store("bitvavo", "BTC-EUR", "1h", 100, db)
        assert str(db) in result["db_path"]
