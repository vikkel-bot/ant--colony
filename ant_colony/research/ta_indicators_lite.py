"""
AC-123: TA Indicators (Research-Only, Pure Compute Layer)

Pure functions that operate on list[dict] OHLCV rows and return new rows
with indicator fields appended. No I/O, no state, no storage, no execution.

Indicators added per row:
  rsi_14   — Relative Strength Index (Wilder, period 14)
  sma_20   — Simple Moving Average of close (period 20)
  ema_20   — Exponential Moving Average of close (period 20, SMA-seeded)
  atr_14   — Average True Range (Wilder, period 14)
  bb_upper — Bollinger Band upper (period 20, 2σ)
  bb_lower — Bollinger Band lower (period 20, 2σ)

Warmup:
  Rows with insufficient history receive None for that indicator field.
  No forward fill. No backfill. No arbitrary defaults.

Input contract:
  - list[dict], each dict contains: ts_utc, open, high, low, close, volume
  - rows are chronologically ordered (ascending ts_utc)
  - numeric fields may be int, float, or numeric strings

Output contract:
  - same number of rows as input
  - all original fields preserved, never mutated
  - indicator fields appended
  - None where warmup is insufficient or value is non-numeric

Usage:
    from ta_indicators_lite import normalize_ohlcv_rows, add_indicators

    rows = [{"ts_utc": ..., "open": ..., "high": ...,
             "low": ..., "close": ..., "volume": ...}, ...]
    enriched = add_indicators(rows)
"""
from __future__ import annotations

import math
import statistics
from typing import Optional

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_float(v) -> Optional[float]:
    """Convert v to float, returning None on failure or non-finite result."""
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _extract(rows: list[dict], key: str) -> list[Optional[float]]:
    return [_to_float(r.get(key)) for r in rows]


# ---------------------------------------------------------------------------
# Indicator series — each returns a list of the same length as the input,
# with None for warmup/invalid positions.
# ---------------------------------------------------------------------------

def _sma_series(values: list[Optional[float]], period: int) -> list[Optional[float]]:
    """Simple moving average. None for first period-1 rows."""
    n = len(values)
    result: list[Optional[float]] = [None] * n
    for i in range(period - 1, n):
        window = values[i - period + 1 : i + 1]
        if any(v is None for v in window):
            continue
        result[i] = sum(window) / period  # type: ignore[arg-type]
    return result


def _ema_series(values: list[Optional[float]], period: int) -> list[Optional[float]]:
    """
    Exponential moving average seeded with SMA of first `period` rows.
    None for first period-1 rows.
    If a value is None mid-series, that row outputs None but EMA state is
    frozen (not reset) — the next valid value continues from last known EMA.
    """
    n = len(values)
    result: list[Optional[float]] = [None] * n
    if n < period:
        return result

    seed = values[:period]
    if any(v is None for v in seed):
        return result

    k = 2.0 / (period + 1)
    ema = sum(seed) / period  # type: ignore[arg-type]
    result[period - 1] = ema

    for i in range(period, n):
        v = values[i]
        if v is None:
            result[i] = None
            # EMA state preserved — next valid value continues from `ema`
        else:
            ema = v * k + ema * (1.0 - k)
            result[i] = ema

    return result


def _true_range_series(
    highs: list[Optional[float]],
    lows:  list[Optional[float]],
    closes: list[Optional[float]],
) -> list[Optional[float]]:
    """
    True Range series.
    TR[0] = high[0] - low[0]  (no prev close)
    TR[i] = max(H-L, |H-Cprev|, |L-Cprev|)
    """
    n = len(highs)
    trs: list[Optional[float]] = [None] * n

    h0, l0 = highs[0], lows[0]
    if h0 is not None and l0 is not None:
        trs[0] = h0 - l0

    for i in range(1, n):
        h, l, cp = highs[i], lows[i], closes[i - 1]
        if h is None or l is None or cp is None:
            trs[i] = None
        else:
            trs[i] = max(h - l, abs(h - cp), abs(l - cp))

    return trs


def _atr_series(
    highs:  list[Optional[float]],
    lows:   list[Optional[float]],
    closes: list[Optional[float]],
    period: int = 14,
) -> list[Optional[float]]:
    """
    Average True Range using Wilder's smoothing.
    Seeded with simple average of first `period` True Ranges.
    None for first period-1 rows.
    """
    n = len(highs)
    result: list[Optional[float]] = [None] * n
    trs = _true_range_series(highs, lows, closes)

    if n < period:
        return result

    seed = trs[:period]
    if any(v is None for v in seed):
        return result

    atr = sum(seed) / period  # type: ignore[arg-type]
    result[period - 1] = atr

    for i in range(period, n):
        tr = trs[i]
        if tr is None:
            result[i] = None
        else:
            atr = (atr * (period - 1) + tr) / period
            result[i] = atr

    return result


def _rsi_series(
    closes: list[Optional[float]],
    period: int = 14,
) -> list[Optional[float]]:
    """
    RSI using Wilder's smoothing.
    First RSI value at index `period` (needs period+1 closes).
    Rows 0..period-1 get None.
    """
    n = len(closes)
    result: list[Optional[float]] = [None] * n

    if n < period + 1:
        return result

    # Build gain/loss series (length n-1, index i covers close[i]→close[i+1])
    gains:  list[Optional[float]] = [None] * (n - 1)
    losses: list[Optional[float]] = [None] * (n - 1)
    for i in range(n - 1):
        c0, c1 = closes[i], closes[i + 1]
        if c0 is None or c1 is None:
            pass  # leave None
        else:
            d = c1 - c0
            gains[i]  = max(d, 0.0)
            losses[i] = max(-d, 0.0)

    # Seed: simple average of first period gains/losses
    # gains[0..period-1] drive the first RSI (at row index `period`)
    seed_g = gains[:period]
    seed_l = losses[:period]
    if any(v is None for v in seed_g) or any(v is None for v in seed_l):
        return result

    avg_gain = sum(seed_g) / period   # type: ignore[arg-type]
    avg_loss = sum(seed_l) / period   # type: ignore[arg-type]

    rs = avg_gain / (avg_loss + 1e-12)
    result[period] = 100.0 - 100.0 / (1.0 + rs)

    # Wilder smoothing for subsequent rows
    # gains[i] drives row i+1
    for j in range(period + 1, n):
        g = gains[j - 1]
        l = losses[j - 1]
        if g is None or l is None:
            result[j] = None
        else:
            avg_gain = (avg_gain * (period - 1) + g) / period
            avg_loss = (avg_loss * (period - 1) + l) / period
            rs = avg_gain / (avg_loss + 1e-12)
            result[j] = 100.0 - 100.0 / (1.0 + rs)

    return result


def _bollinger_series(
    closes: list[Optional[float]],
    period: int = 20,
    k: float = 2.0,
) -> tuple[list[Optional[float]], list[Optional[float]]]:
    """
    Bollinger Bands (upper, lower). Uses population std dev.
    Returns (upper_series, lower_series).
    None for first period-1 rows.
    """
    n = len(closes)
    upper: list[Optional[float]] = [None] * n
    lower: list[Optional[float]] = [None] * n

    for i in range(period - 1, n):
        window = closes[i - period + 1 : i + 1]
        if any(v is None for v in window):
            continue
        mean = sum(window) / period  # type: ignore[arg-type]
        variance = sum((x - mean) ** 2 for x in window) / period  # type: ignore[operator]
        std = math.sqrt(variance)
        upper[i] = mean + k * std
        lower[i] = mean - k * std

    return upper, lower


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_ohlcv_rows(rows: list[dict]) -> list[dict]:
    """
    Normalize OHLCV rows to consistent numeric types.

    - Returns a new list (input not mutated).
    - Each output dict has the same keys as the input dict.
    - Numeric fields (open, high, low, close, volume) are converted to float
      or None if conversion fails.
    - Non-dict items are silently dropped.
    - Empty input → [].
    """
    if not rows:
        return []

    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        norm = dict(row)
        for field in ("open", "high", "low", "close", "volume"):
            if field in norm:
                norm[field] = _to_float(norm[field])
        result.append(norm)

    return result


def add_indicators(rows: list[dict]) -> list[dict]:
    """
    Add TA indicator fields to OHLCV rows.

    Input:  list[dict] with at minimum ts_utc, open, high, low, close, volume.
            Rows must be chronologically ordered (ascending ts_utc).
    Output: new list[dict] with all original fields preserved and the
            following fields appended:

      rsi_14   — RSI (Wilder, period 14)
      sma_20   — Simple MA of close (period 20)
      ema_20   — Exponential MA of close (period 20, SMA-seeded)
      atr_14   — Average True Range (Wilder, period 14)
      bb_upper — Bollinger upper (period 20, 2σ)
      bb_lower — Bollinger lower (period 20, 2σ)

    None is used for warmup rows or rows with missing/invalid source data.
    Input rows are not mutated. Returns [] for empty input.
    """
    if not rows:
        return []

    normed = normalize_ohlcv_rows(rows)
    if not normed:
        return []

    closes = _extract(normed, "close")
    highs  = _extract(normed, "high")
    lows   = _extract(normed, "low")

    rsi14        = _rsi_series(closes, 14)
    sma20        = _sma_series(closes, 20)
    ema20        = _ema_series(closes, 20)
    atr14        = _atr_series(highs, lows, closes, 14)
    bb_up, bb_lo = _bollinger_series(closes, 20, 2.0)

    output = []
    for i, row in enumerate(normed):
        new_row = dict(row)
        new_row["rsi_14"]   = rsi14[i]
        new_row["sma_20"]   = sma20[i]
        new_row["ema_20"]   = ema20[i]
        new_row["atr_14"]   = atr14[i]
        new_row["bb_upper"] = bb_up[i]
        new_row["bb_lower"] = bb_lo[i]
        output.append(new_row)

    return output
