"""
AC_DEV — BNB Deep-Dive Analyse
================================
Analyseert BNB-EUR backtest resultaten in detail:
- Trades per jaar / kwartaal
- Stabiliteit over marktregimes
- Vergelijking met andere markten
- Verdict: verdient hogere allocatie?

Gebruik:
    python bnb_deepdive.py
    python bnb_deepdive.py --market SOL-EUR   (vergelijk met andere)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import List, Optional, Dict

# ── config ─────────────────────────────────────────────────────────────────
DATA_CACHE = r"C:\Users\vikke\OneDrive\bitvavo-bot_clean\data_cache"
ANT_OUT    = r"C:\Trading\AC_DEV\ANT_OUT"
INTERVAL   = "4h"

INTERVAL_MS = 14_400_000  # 4h in ms

# ── helpers ────────────────────────────────────────────────────────────────
def load_candles(market: str) -> list:
    path = os.path.join(DATA_CACHE, f"{market}_{INTERVAL}_candles.json")
    with open(path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    return data["candles"] if isinstance(data, dict) else data

def ts_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def ts_to_year(ms: int) -> int:
    return ts_to_dt(ms).year

def ts_to_quarter(ms: int) -> str:
    dt = ts_to_dt(ms)
    return f"{dt.year}-Q{(dt.month - 1) // 3 + 1}"

# ── indicators (zelfde als backtest runner) ───────────────────────────────
def sma(xs, n):
    if len(xs) < n: return []
    out, s = [], sum(xs[:n])
    out.append(s / n)
    for i in range(n, len(xs)):
        s += xs[i] - xs[i - n]
        out.append(s / n)
    return out

def stdev_window(xs, n):
    import statistics
    if len(xs) < n: return []
    return [statistics.pstdev(xs[i - n + 1:i + 1]) for i in range(n - 1, len(xs))]

def rsi(xs, n=14):
    if len(xs) < n + 1: return []
    gains = [max(xs[i] - xs[i-1], 0) for i in range(1, len(xs))]
    losses = [max(xs[i-1] - xs[i], 0) for i in range(1, len(xs))]
    out = []
    for i in range(n - 1, len(gains)):
        ag = sum(gains[i - n + 1:i + 1]) / n
        al = sum(losses[i - n + 1:i + 1]) / n
        rs = ag / (al + 1e-12)
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out

def bollinger(closes, n=20, k=2.0):
    mid = sma(closes, n)
    sd  = stdev_window(closes, n)
    if not mid or not sd: return {"mid": [], "upper": [], "lower": [], "width": []}
    upper, lower, width = [], [], []
    for m, s in zip(mid, sd):
        u, l = m + k * s, m - k * s
        upper.append(u); lower.append(l)
        width.append((u - l) / m if m else 0.0)
    return {"mid": mid, "upper": upper, "lower": lower, "width": width}

# ── backtest met trade details ─────────────────────────────────────────────
def run_detailed_backtest(closes: list, timestamps: list, volumes=None,
                          initial_equity=1000.0, risk_frac=0.25) -> dict:
    n = len(closes)
    all_rsi = rsi(closes, 14)
    all_bb  = bollinger(closes, 20, 2.0)
    rsi_off, bb_off = 14, 19

    equity = initial_equity
    curve  = [equity]
    trades = []
    i = 50

    while i < n - 1:
        ri, bi = i - rsi_off, i - bb_off
        sig = None

        if ri >= 0 and bi >= 0 and all_bb["lower"]:
            cur_rsi = all_rsi[ri]
            cur_low = all_bb["lower"][bi]
            cur_mid = all_bb["mid"][bi]
            cur_wid = all_bb["width"][bi]
            cur     = closes[i]
            if cur_rsi <= 25.0 and cur_wid <= 0.08 and cur <= cur_low * 1.01:
                strength = min(1.0, ((25.0 - cur_rsi) / 25.0) +
                               max(0.0, (0.08 - cur_wid) / 0.08))
                sig = {
                    "edge": "mean_reversion",
                    "entry": cur,
                    "tp": round(cur_mid, 8),
                    "sl": round(cur * 0.975, 8),
                    "hold": 6,
                    "strength": round(strength, 4),
                    "rsi": round(cur_rsi, 2),
                    "bb_width": round(cur_wid, 4),
                }

        if sig is None:
            curve.append(equity); i += 1; continue

        entry_ts    = timestamps[i]
        entry_price = closes[i]
        exit_i, exit_price, exit_reason = None, None, "TIME"
        max_j = min(n - 1, i + sig["hold"])

        for j in range(i + 1, max_j + 1):
            px = closes[j]
            if px <= sig["sl"]:
                exit_i, exit_price, exit_reason = j, px, "SL"; break
            if px >= sig["tp"]:
                exit_i, exit_price, exit_reason = j, px, "TP"; break

        if exit_i is None:
            exit_i = max_j; exit_price = closes[exit_i]

        pnl_frac = (exit_price / entry_price) - 1.0
        equity  *= 1.0 + (pnl_frac * risk_frac)
        curve.append(equity)

        trades.append({
            "entry_ts":    entry_ts,
            "exit_ts":     timestamps[exit_i],
            "entry_price": entry_price,
            "exit_price":  exit_price,
            "pnl_frac":    round(pnl_frac, 6),
            "pnl_pct":     round(pnl_frac * 100, 3),
            "exit_reason": exit_reason,
            "win":         pnl_frac > 0,
            "strength":    sig["strength"],
            "rsi_at_entry": sig["rsi"],
            "year":        ts_to_year(entry_ts),
            "quarter":     ts_to_quarter(entry_ts),
        })
        i = exit_i + 1

    return {"equity": equity, "curve": curve, "trades": trades}

# ── analyse ────────────────────────────────────────────────────────────────
def analyse_by_period(trades: list, key: str) -> dict:
    periods: Dict[str, list] = {}
    for t in trades:
        p = t[key]
        periods.setdefault(p, []).append(t)

    result = {}
    for period, ts in sorted(periods.items()):
        wins  = sum(1 for t in ts if t["win"])
        gross_win = sum(t["pnl_frac"] for t in ts if t["win"])
        gross_los = abs(sum(t["pnl_frac"] for t in ts if not t["win"]))
        pf = round(gross_win / gross_los, 3) if gross_los > 0 else None
        wr = round(wins / len(ts), 3) if ts else None
        avg_pnl = round(sum(t["pnl_pct"] for t in ts) / len(ts), 3) if ts else None
        result[period] = {
            "trades": len(ts),
            "wins":   wins,
            "losses": len(ts) - wins,
            "winrate": wr,
            "profit_factor": pf,
            "avg_pnl_pct": avg_pnl,
            "gross_win_pct": round(gross_win * 100, 2),
            "gross_loss_pct": round(gross_los * 100, 2),
        }
    return result

def max_drawdown(curve: list) -> float:
    peak, mdd = curve[0], 0.0
    for x in curve:
        peak = max(peak, x)
        dd = (peak - x) / peak if peak > 0 else 0.0
        mdd = max(mdd, dd)
    return mdd

def print_period_table(data: dict, label: str) -> None:
    print(f"\n  {'─'*65}")
    print(f"  {label}")
    print(f"  {'─'*65}")
    print(f"  {'Period':<12} {'Trades':>7} {'WR':>7} {'PF':>7} {'AvgPnL':>8} {'Status'}")
    print(f"  {'─'*65}")
    for period, r in data.items():
        pf  = r["profit_factor"]
        wr  = r["winrate"]
        n   = r["trades"]
        avg = r["avg_pnl_pct"]
        if pf is None or n < 5:
            status = "SKIP (n<5)"
        elif pf >= 1.3 and wr >= 0.45:
            status = "✓ STRONG"
        elif pf >= 1.0:
            status = "~ OK"
        else:
            status = "✗ WEAK"
        pf_str  = f"{pf:.3f}" if pf else "  N/A"
        wr_str  = f"{wr:.1%}" if wr else "  N/A"
        avg_str = f"{avg:+.2f}%" if avg else "  N/A"
        print(f"  {period:<12} {n:>7} {wr_str:>7} {pf_str:>7} {avg_str:>8}   {status}")

# ── main ───────────────────────────────────────────────────────────────────
def run_market(market: str) -> dict:
    print(f"\n{'═'*65}")
    print(f"  Deep-Dive: {market}")
    print(f"{'═'*65}")

    candles = load_candles(market)
    closes  = [float(c[4]) for c in candles]
    timestamps = [int(c[0]) for c in candles]
    volumes = [float(c[5]) for c in candles] if len(candles[0]) > 5 else None

    result  = run_detailed_backtest(closes, timestamps, volumes)
    trades  = result["trades"]
    curve   = result["curve"]
    equity  = result["equity"]

    if not trades:
        print("  Geen trades gevonden.")
        return {}

    # Totaal overzicht
    wins      = sum(1 for t in trades if t["win"])
    gross_win = sum(t["pnl_frac"] for t in trades if t["win"])
    gross_los = abs(sum(t["pnl_frac"] for t in trades if not t["win"]))
    pf  = round(gross_win / gross_los, 4) if gross_los > 0 else None
    wr  = round(wins / len(trades), 4)
    mdd = round(max_drawdown(curve) * 100, 2)
    ret = round((equity / 1000.0 - 1.0) * 100, 2)

    print(f"\n  TOTAAL  ({len(trades)} trades, {ts_to_dt(timestamps[0]).date()} → {ts_to_dt(timestamps[-1]).date()})")
    print(f"  Return:        {ret:+.1f}%")
    print(f"  Profit Factor: {pf}")
    print(f"  Winrate:       {wr:.1%}")
    print(f"  Max Drawdown:  {mdd}%")

    # Exit verdeling
    sl_count   = sum(1 for t in trades if t["exit_reason"] == "SL")
    tp_count   = sum(1 for t in trades if t["exit_reason"] == "TP")
    time_count = sum(1 for t in trades if t["exit_reason"] == "TIME")
    print(f"\n  Exit verdeling:")
    print(f"    TP (profit):  {tp_count:4d} ({tp_count/len(trades):.1%})")
    print(f"    SL (stop):    {sl_count:4d} ({sl_count/len(trades):.1%})")
    print(f"    TIME (expiry):{time_count:4d} ({time_count/len(trades):.1%})")

    # Per jaar
    by_year = analyse_by_period(trades, "year")
    print_period_table(by_year, "STABILITEIT PER JAAR")

    # Per kwartaal
    by_quarter = analyse_by_period(trades, "quarter")
    print_period_table(by_quarter, "STABILITEIT PER KWARTAAL")

    # Sla op
    output = {
        "market": market,
        "total_trades": len(trades),
        "profit_factor": pf,
        "winrate": wr,
        "max_drawdown_pct": mdd,
        "return_pct": ret,
        "by_year": by_year,
        "by_quarter": by_quarter,
        "trades": trades,
    }
    path = os.path.join(ANT_OUT, f"deepdive_{market.replace('-','_')}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\n  → Opgeslagen: {path}")
    return output

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", default="BNB-EUR")
    parser.add_argument("--all",    action="store_true")
    args = parser.parse_args()

    markets = ["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","BNB-EUR"] if args.all else [args.market]

    for m in markets:
        run_market(m)

    print(f"\n{'═'*65}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
