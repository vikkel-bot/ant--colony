"""
AC-122: Standalone OHLCV Fetcher (Research-Only)

Fetches OHLCV candles from a crypto exchange and stores them in a local
SQLite database for use by research/backtest modules.

NO pipeline impact. NO execution. NO live trading. Read-only data collection.

Usage:
    python scripts/fetch_ohlcv.py --exchange bitvavo --market BTC-EUR --timeframe 1h --limit 500
    python scripts/fetch_ohlcv.py --exchange bitvavo --market BTC-EUR ETH-EUR --timeframe 1h 4h 1d

Database:
    data/ohlcv/ohlcv.sqlite   (relative to repo root, never in ANT_OUT)

Restart-safe:
    Resumes from last stored candle per (exchange, market, timeframe).
    Duplicate candles are silently skipped (INSERT OR IGNORE).

Supported exchanges:
    bitvavo  — public candle endpoint, no auth required

Supported markets (first version):
    BTC-EUR, ETH-EUR, ADA-EUR, BNB-EUR

Supported timeframes:
    1h, 4h, 1d
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = _REPO_ROOT / "data" / "ohlcv" / "ohlcv.sqlite"

SUPPORTED_MARKETS    = {"BTC-EUR", "ETH-EUR", "ADA-EUR", "BNB-EUR"}
SUPPORTED_TIMEFRAMES = {"1h", "4h", "1d"}
MAX_LIMIT            = 1000   # Bitvavo hard cap per request

# Bitvavo public REST base (no auth needed for candles)
_BITVAVO_BASE = "https://api.bitvavo.com/v2"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ohlcv (
    exchange  TEXT    NOT NULL,
    market    TEXT    NOT NULL,
    timeframe TEXT    NOT NULL,
    ts_utc    INTEGER NOT NULL,
    open      REAL    NOT NULL,
    high      REAL    NOT NULL,
    low       REAL    NOT NULL,
    close     REAL    NOT NULL,
    volume    REAL    NOT NULL,
    PRIMARY KEY (exchange, market, timeframe, ts_utc)
)
"""

_INSERT_OR_IGNORE = """
INSERT OR IGNORE INTO ohlcv
    (exchange, market, timeframe, ts_utc, open, high, low, close, volume)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_GET_LAST_TS = """
SELECT MAX(ts_utc) FROM ohlcv
WHERE exchange = ? AND market = ? AND timeframe = ?
"""

_COUNT_ROWS = """
SELECT COUNT(*) FROM ohlcv
WHERE exchange = ? AND market = ? AND timeframe = ?
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure the schema exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def get_last_ts(conn: sqlite3.Connection,
                exchange: str, market: str, timeframe: str) -> Optional[int]:
    """Return the most recent stored timestamp (ms) or None if no data yet."""
    row = conn.execute(_GET_LAST_TS, (exchange, market, timeframe)).fetchone()
    return row[0] if row and row[0] is not None else None


def count_rows(conn: sqlite3.Connection,
               exchange: str, market: str, timeframe: str) -> int:
    row = conn.execute(_COUNT_ROWS, (exchange, market, timeframe)).fetchone()
    return row[0] if row else 0


def upsert_candles(conn: sqlite3.Connection,
                   exchange: str, market: str, timeframe: str,
                   rows: list[tuple]) -> tuple[int, int]:
    """
    Insert rows into the database.
    Returns (inserted, skipped).
    Each row is (ts_utc, open, high, low, close, volume).
    Duplicates are silently skipped via INSERT OR IGNORE.
    """
    if not rows:
        return 0, 0

    before = count_rows(conn, exchange, market, timeframe)
    conn.executemany(
        _INSERT_OR_IGNORE,
        [(exchange, market, timeframe, r[0], r[1], r[2], r[3], r[4], r[5])
         for r in rows],
    )
    conn.commit()
    after = count_rows(conn, exchange, market, timeframe)

    inserted = after - before
    skipped  = len(rows) - inserted
    return inserted, skipped


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_candle_row(raw: list) -> Optional[tuple]:
    """
    Parse one raw API row into (ts_utc_ms, open, high, low, close, volume).
    Returns None if the row is malformed.
    Bitvavo format: [ts_ms, "open", "high", "low", "close", "volume"]
    """
    if not isinstance(raw, (list, tuple)) or len(raw) < 6:
        return None
    try:
        return (
            int(raw[0]),
            float(raw[1]),
            float(raw[2]),
            float(raw[3]),
            float(raw[4]),
            float(raw[5]),
        )
    except (TypeError, ValueError):
        return None


def parse_candle_rows(raw_list: list) -> list[tuple]:
    """Parse a list of raw API rows. Silently drops malformed rows."""
    if not isinstance(raw_list, list):
        return []
    result = []
    for raw in raw_list:
        parsed = parse_candle_row(raw)
        if parsed is not None:
            result.append(parsed)
    return result


# ---------------------------------------------------------------------------
# Exchange fetch — Bitvavo
# ---------------------------------------------------------------------------

def _build_bitvavo_url(market: str, timeframe: str,
                       limit: int, since_ms: Optional[int]) -> str:
    url = f"{_BITVAVO_BASE}/{market}/candles?interval={timeframe}&limit={limit}"
    if since_ms is not None:
        url += f"&start={since_ms}"
    return url


def fetch_candles_bitvavo(market: str, timeframe: str,
                          limit: int, since_ms: Optional[int],
                          timeout: int = 15) -> list:
    """
    Fetch candles from Bitvavo public API.
    Returns list of raw rows, or [] on any error.
    """
    url = _build_bitvavo_url(market, timeframe, limit, since_ms)
    req = urllib.request.Request(url, headers={"User-Agent": "ant-colony-research/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if not isinstance(data, list):
            return []
        return data
    except urllib.error.HTTPError as exc:
        if exc.code == 400 and since_ms is not None:
            # Bitvavo returns 400 when start= is beyond available data range.
            # This means we are already up to date — not an error.
            return []
        print(f"  [WARN] HTTP {exc.code} fetching {market} {timeframe}: {exc.reason}",
              file=sys.stderr)
        return []
    except urllib.error.URLError as exc:
        print(f"  [WARN] Network error fetching {market} {timeframe}: {exc.reason}",
              file=sys.stderr)
        return []
    except Exception as exc:
        print(f"  [WARN] Unexpected error fetching {market} {timeframe}: {exc}",
              file=sys.stderr)
        return []


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

EXCHANGE_FETCHERS = {
    "bitvavo": fetch_candles_bitvavo,
}


def fetch_and_store(
    exchange: str,
    market: str,
    timeframe: str,
    limit: int,
    db_path: Path,
) -> dict:
    """
    Fetch OHLCV candles for one (exchange, market, timeframe) combination
    and upsert into the database.

    Returns a summary dict with counts.
    """
    if exchange not in EXCHANGE_FETCHERS:
        return {
            "exchange": exchange, "market": market, "timeframe": timeframe,
            "status": "ERROR", "error": f"Unsupported exchange: {exchange}",
            "fetched": 0, "inserted": 0, "skipped": 0,
        }

    conn = init_db(db_path)
    try:
        last_ts = get_last_ts(conn, exchange, market, timeframe)
        since_ms = (last_ts + 1) if last_ts is not None else None

        raw_rows = EXCHANGE_FETCHERS[exchange](market, timeframe, limit, since_ms)
        parsed   = parse_candle_rows(raw_rows)
        inserted, skipped = upsert_candles(conn, exchange, market, timeframe, parsed)

        return {
            "exchange":  exchange,
            "market":    market,
            "timeframe": timeframe,
            "status":    "OK",
            "fetched":   len(parsed),
            "inserted":  inserted,
            "skipped":   skipped,
            "db_path":   str(db_path),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_summary(results: list[dict]) -> None:
    print()
    print("=== AC-122 OHLCV FETCH SUMMARY ===")
    for r in results:
        if r["status"] == "OK":
            print(
                f"  {r['exchange']:10s}  {r['market']:10s}  {r['timeframe']:4s}"
                f"  fetched={r['fetched']:5d}  inserted={r['inserted']:5d}"
                f"  skipped={r['skipped']:5d}"
            )
        else:
            print(
                f"  {r['exchange']:10s}  {r['market']:10s}  {r['timeframe']:4s}"
                f"  ERROR: {r.get('error', '?')}"
            )
    # All rows share the same db_path
    db_paths = {r.get("db_path") for r in results if r.get("db_path")}
    for p in sorted(db_paths):
        print(f"  db: {p}")
    print()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-122: Fetch OHLCV candles for research (no pipeline impact).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/fetch_ohlcv.py --exchange bitvavo --market BTC-EUR --timeframe 1h --limit 500
  python scripts/fetch_ohlcv.py --exchange bitvavo --market BTC-EUR ETH-EUR --timeframe 1h 4h 1d
        """,
    )
    p.add_argument("--exchange",  default="bitvavo",
                   help="Exchange name (default: bitvavo)")
    p.add_argument("--market",    nargs="+", default=["BTC-EUR"],
                   metavar="MARKET",
                   help="Market(s) to fetch, e.g. BTC-EUR ETH-EUR")
    p.add_argument("--timeframe", nargs="+", default=["1h"],
                   metavar="TF",
                   help="Timeframe(s): 1h 4h 1d")
    p.add_argument("--limit",     type=int, default=500,
                   help="Max candles per request (default: 500, max: 1000)")
    p.add_argument("--db",        default=str(DEFAULT_DB_PATH),
                   help=f"SQLite database path (default: {DEFAULT_DB_PATH})")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    exchange  = args.exchange.lower()
    markets   = [m.upper() for m in args.market]
    timeframes = [t.lower() for t in args.timeframe]
    limit     = min(max(1, args.limit), MAX_LIMIT)
    db_path   = Path(args.db)

    results = []
    for market in markets:
        for timeframe in timeframes:
            print(f"  Fetching {exchange} {market} {timeframe} limit={limit} ...",
                  end=" ", flush=True)
            r = fetch_and_store(exchange, market, timeframe, limit, db_path)
            if r["status"] == "OK":
                print(f"fetched={r['fetched']} inserted={r['inserted']} skipped={r['skipped']}")
            else:
                print(f"ERROR: {r.get('error', '?')}")
            results.append(r)

    _print_summary(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
