"""
AC_DEV - Phase 3: Research Backtest Runner
==========================================
Laadt candle data uit productie data_cache (read-only).
Draait enhanced edges backtest per market.
Schrijft research_backtest_report.json naar AC_DEV ANT_OUT.

STANDALONE - raakt bitvavo-bot_clean NIET aan.
Geen imports van productie modules.
"""
from __future__ import annotations

import json
import os
import sys
import statistics
from datetime import datetime, timezone
from typing import List, Optional, Dict


# ── paden ──────────────────────────────────────────────────────────────────
ROOT        = r"C:\Trading\AC_DEV"
ANT_OUT     = os.path.join(ROOT, "ANT_OUT")
OUT_PATH    = os.path.join(ANT_OUT, "research_backtest_report.json")

# Candle data uit productie (read-only)
PROD_DATA_CACHE = r"C:\Users\vikke\OneDrive\bitvavo-bot_clean\data_cache"

MARKETS   = ["BTC-EUR", "ETH-EUR", "SOL-EUR", "XRP-EUR", "ADA-EUR", "BNB-EUR"]
INTERVAL  = "4h"

# ── helpers ────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def atomic_write(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def load_candles(market: str, interval: str) -> Optional[List]:
    """Probeer candles te laden uit productie data_cache."""
    fname = f"{market}_{interval}_candles.json"
    path = os.path.join(PROD_DATA_CACHE, fname)
    if not os.path.exists(path):
        # Fallback: probeer ook root
        alt = os.path.join(
            r"C:\Users\vikke\OneDrive\bitvavo-bot_clean",
            f"{market}_{interval}_candles.json"
        )
        if os.path.exists(alt):
            path = alt
        else:
            return None
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception as e:
        print(f"    ERROR loading {path}: {e}")
        return None

# ── indicators (inline) ────────────────────────────────────────────────────
def sma(xs: List[float], n: int) -> List[float]:
    if len(xs) < n:
        return []
    out, s = [], sum(xs[:n])
    out.append(s / n)
    for i in range(n, len(xs)):
        s += xs[i] - xs[i - n]
        out.append(s / n)
    return out

def stdev_window(xs: List[float], n: int) -> List[float]:
    if len(xs) < n:
        return []
    return [statistics.pstdev(xs[i - n + 1:i + 1]) for i in range(n - 1, len(xs))]

def rsi(xs: List[float], n: int = 14) -> List[float]:
    if len(xs) < n + 1:
        return []
    gains = [max(xs[i] - xs[i-1], 0) for i in range(1, len(xs))]
    losses = [max(xs[i-1] - xs[i], 0) for i in range(1, len(xs))]
    out = []
    for i in range(n - 1, len(gains)):
        ag = sum(gains[i - n + 1:i + 1]) / n
        al = sum(losses[i - n + 1:i + 1]) / n
        rs = ag / (al + 1e-12)
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out

def bollinger(closes: List[float], n: int = 20, k: float = 2.0) -> Dict:
    mid = sma(closes, n)
    sd = stdev_window(closes, n)
    if not mid or not sd:
        return {"mid": [], "upper": [], "lower": [], "width": []}
    upper, lower, width = [], [], []
    for m, s in zip(mid, sd):
        u, l = m + k * s, m - k * s
        upper.append(u); lower.append(l)
        width.append((u - l) / m if m else 0.0)
    return {"mid": mid, "upper": upper, "lower": lower, "width": width}

# ── signals ────────────────────────────────────────────────────────────────
class Signal:
    def __init__(self, edge_name, entry_price, tp_price, sl_price, hold_bars, strength):
        self.edge_name   = edge_name
        self.entry_price = entry_price
        self.tp_price    = tp_price
        self.sl_price    = sl_price
        self.hold_bars   = hold_bars
        self.strength    = strength

def detect_mean_reversion(closes: List[float]) -> Optional[Signal]:
    if len(closes) < 50:
        return None
    rs = rsi(closes, 14)
    bb = bollinger(closes, 20, 2.0)
    if not rs or not bb["lower"]:
        return None
    cur      = closes[-1]
    cur_rsi  = rs[-1]
    cur_low  = bb["lower"][-1]
    cur_mid  = bb["mid"][-1]
    cur_wid  = bb["width"][-1]
    if cur_rsi > 25.0 or cur_wid > 0.08 or cur > cur_low * 1.01:
        return None
    strength = min(1.0, ((25.0 - cur_rsi) / 25.0) + max(0.0, (0.08 - cur_wid) / 0.08))
    return Signal("mean_reversion", cur, round(cur_mid, 8), round(cur * 0.975, 8), 6, round(strength, 4))

def detect_volume_breakout(closes: List[float], volumes: List[float]) -> Optional[Signal]:
    if len(closes) < 30 or len(volumes) < 30:
        return None
    cur, prev   = closes[-1], closes[-2]
    avg_vol     = sum(volumes[-21:-1]) / 20.0
    vol_mult    = volumes[-1] / (avg_vol + 1e-12)
    ret         = (cur / prev - 1.0) if prev else 0.0
    if vol_mult < 2.0 or ret < 0.0075:
        return None
    strength = min(1.0, (vol_mult / 3.0) * 0.6 + (ret / 0.02) * 0.4)
    return Signal("volume_breakout", cur, round(cur * 1.02, 8), round(cur * 0.985, 8), 4, round(strength, 4))

# ── backtest ────────────────────────────────────────────────────────────────
def max_drawdown(curve: List[float]) -> float:
    peak, mdd = curve[0], 0.0
    for x in curve:
        if x > peak:
            peak = x
        dd = (peak - x) / peak if peak > 0 else 0.0
        if dd > mdd:
            mdd = dd
    return mdd

def run_backtest(closes: List[float], volumes: Optional[List[float]] = None,
                 initial_equity: float = 1000.0, risk_frac: float = 0.25) -> dict:
    equity = initial_equity
    curve  = [equity]
    trades = []

    # Pre-compute indicators eenmalig — O(n) in plaats van O(n²)
    n = len(closes)
    all_rsi  = rsi(closes, 14)         # lengte n-14
    all_bb   = bollinger(closes, 20, 2.0)
    rsi_off  = 14       # rsi[0] correspondeert met closes[14]
    bb_off   = 19       # bb[0] correspondeert met closes[19]

    i = 50
    while i < n - 1:
        # Indices in pre-computed arrays
        ri = i - rsi_off
        bi = i - bb_off

        sig = None
        if ri >= 0 and bi >= 0 and all_bb["lower"]:
            # Bounds check
            if ri >= len(all_rsi) or bi >= len(all_bb["lower"]):
                curve.append(equity); i += 1; continue
            cur_rsi = all_rsi[ri]
            cur_low = all_bb["lower"][bi]
            cur_mid = all_bb["mid"][bi]
            cur_wid = all_bb["width"][bi]
            cur     = closes[i]
            if cur_rsi <= 25.0 and cur_wid <= 0.08 and cur <= cur_low * 1.01:
                strength = min(1.0, ((25.0 - cur_rsi) / 25.0) + max(0.0, (0.08 - cur_wid) / 0.08))
                sig = Signal("mean_reversion", cur, round(cur_mid, 8), round(cur * 0.975, 8), 6, round(strength, 4))

        if sig is None and volumes and bi >= 0 and i >= 2:
            prev = closes[i - 1]
            avg_vol  = sum(volumes[max(0, i-20):i]) / max(1, min(20, i))
            vol_mult = volumes[i] / (avg_vol + 1e-12)
            ret      = (closes[i] / prev - 1.0) if prev else 0.0
            if vol_mult >= 2.0 and ret >= 0.0075:
                strength = min(1.0, (vol_mult / 3.0) * 0.6 + (ret / 0.02) * 0.4)
                sig = Signal("volume_breakout", closes[i], round(closes[i] * 1.02, 8), round(closes[i] * 0.985, 8), 4, round(strength, 4))

        if sig is None:
            curve.append(equity); i += 1; continue

        entry_price = closes[i]
        exit_i, exit_price, exit_reason = None, None, "TIME"
        max_j = min(len(closes) - 1, i + sig.hold_bars)

        for j in range(i + 1, max_j + 1):
            px = closes[j]
            if px <= sig.sl_price:
                exit_i, exit_price, exit_reason = j, px, "SL"; break
            if px >= sig.tp_price:
                exit_i, exit_price, exit_reason = j, px, "TP"; break

        if exit_i is None:
            exit_i = max_j; exit_price = closes[exit_i]

        pnl_frac = (exit_price / entry_price) - 1.0
        equity  *= 1.0 + (pnl_frac * risk_frac)
        curve.append(equity)
        trades.append({"edge": sig.edge_name, "pnl_frac": pnl_frac,
                        "exit_reason": exit_reason, "entry_i": i, "exit_i": exit_i})
        i = exit_i + 1

    wins      = sum(1 for t in trades if t["pnl_frac"] > 0)
    gross_win = sum(t["pnl_frac"] for t in trades if t["pnl_frac"] > 0)
    gross_los = abs(sum(t["pnl_frac"] for t in trades if t["pnl_frac"] <= 0))
    pf        = round(gross_win / gross_los, 4) if gross_los > 0 else None
    wr        = round(wins / len(trades), 4) if trades else None

    return {
        "initial_equity":    initial_equity,
        "ending_equity":     round(equity, 2),
        "return_pct":        round((equity / initial_equity - 1.0) * 100, 2),
        "closed_trades":     len(trades),
        "wins":              wins,
        "losses":            len(trades) - wins,
        "winrate":           wr,
        "profit_factor":     pf,
        "max_drawdown_pct":  round(max_drawdown(curve) * 100, 2),
        "verdict":           _verdict(pf, wr, len(trades)),
    }

def _verdict(pf, wr, n) -> str:
    if n < 10:
        return "INSUFFICIENT_DATA"
    if pf is None:
        return "NO_LOSSES_YET"
    if pf >= 1.3 and wr >= 0.45:
        return "PROMISING"
    if pf >= 1.0:
        return "MARGINAL"
    return "REJECT"

# ── main ───────────────────────────────────────────────────────────────────
def main() -> int:
    print("=" * 55)
    print("AC_DEV Phase 3 — Research Backtest Runner")
    print("=" * 55)
    print(f"  Data cache: {PROD_DATA_CACHE}")
    print()

    results = {}
    for market in MARKETS:
        print(f"  {market} ...", end=" ", flush=True)
        candles = load_candles(market, INTERVAL)

        if not candles:
            print("NO DATA")
            results[market] = {"status": "NO_DATA", "candles": 0}
            continue

        # Candle formaat: ofwel lijst van lijsten, of {"meta":..., "candles":[...]}
        try:
            if isinstance(candles, dict) and "candles" in candles:
                raw = candles["candles"]
            else:
                raw = candles
            closes  = [float(c[4]) for c in raw]
            volumes = [float(c[5]) for c in raw] if len(raw[0]) > 5 else None
        except (IndexError, TypeError, ValueError) as e:
            print(f"PARSE_ERROR: {e}")
            results[market] = {"status": "PARSE_ERROR"}
            continue

        bt = run_backtest(closes, volumes)
        bt["candles"]  = len(closes)
        bt["interval"] = INTERVAL
        bt["status"]   = "OK"
        results[market] = bt

        print(f"trades={bt['closed_trades']:3d}  "
              f"PF={str(bt['profit_factor']):>6}  "
              f"WR={str(bt['winrate']):>6}  "
              f"DD={bt['max_drawdown_pct']:5.1f}%  "
              f"→ {bt['verdict']}")

    output = {
        "version":          "research_backtest_v1",
        "ts_utc":           utc_now(),
        "source_component": "research_backtest_runner",
        "source":           "AC_DEV",
        "interval":         INTERVAL,
        "markets":          results,
    }

    atomic_write(OUT_PATH, output)
    print()
    print(f"  WROTE: {OUT_PATH}")

    # Samenvatting
    promising = [m for m, r in results.items() if r.get("verdict") == "PROMISING"]
    marginal  = [m for m, r in results.items() if r.get("verdict") == "MARGINAL"]
    rejected  = [m for m, r in results.items() if r.get("verdict") == "REJECT"]

    print()
    print("  Verdicts:")
    if promising: print(f"    PROMISING:  {', '.join(promising)}")
    if marginal:  print(f"    MARGINAL:   {', '.join(marginal)}")
    if rejected:  print(f"    REJECT:     {', '.join(rejected)}")
    print()
    return 0

if __name__ == "__main__":
    sys.exit(main())
