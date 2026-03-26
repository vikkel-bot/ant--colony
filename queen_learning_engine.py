"""
AC_DEV — Queen Learning Engine
================================
Draait periodiek een rolling backtest per markt.
Zoekt betere parameters via grid search.
Schrijft alleen een aanbeveling als bewijs sterk genoeg is
over meerdere opeenvolgende maanden (PF >= drempel, N >= min_months).

Nooit een automatische aanpassing — altijd menselijke goedkeuring vereist.

Gebruik:
    python queen_learning_engine.py
    python queen_learning_engine.py --market BNB-EUR
    python queen_learning_engine.py --window-months 6 --min-months 3
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import statistics
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict

# ── config ─────────────────────────────────────────────────────────────────
DATA_CACHE = r"C:\Users\vikke\OneDrive\bitvavo-bot_clean\data_cache"
ANT_OUT    = r"C:\Trading\AC_DEV\ANT_OUT"
INTERVAL   = "4h"
INTERVAL_MS = 14_400_000

MARKETS = ["BTC-EUR", "ETH-EUR", "SOL-EUR", "XRP-EUR", "ADA-EUR", "BNB-EUR"]

# Evidence drempels
PF_THRESHOLD    = 1.30   # minimum profit factor
MIN_MONTHS      = 3      # minimum aaneengesloten sterke maanden
WINDOW_MONTHS   = 6      # backtest window in maanden
MIN_TRADES      = 8      # minimum trades in window voor geldig resultaat

# Parameter grid
GRID = {
    "rsi_threshold": [20, 25, 30],          # RSI entry drempel
    "bb_width_max":  [0.06, 0.08, 0.10],    # max BB breedte
    "sl_pct":        [0.020, 0.025, 0.030], # stop loss %
    "tp_mult":       [1.5, 2.0, 2.5],       # TP als mult van BB-mid afstand
    "hold_bars":     [4, 6, 8],             # max hold tijd
    "size_mult":     [0.5, 0.75, 1.0],      # position sizing
}

# Standaard params (huidige productie)
DEFAULT_PARAMS = {
    "rsi_threshold": 25,
    "bb_width_max":  0.08,
    "sl_pct":        0.025,
    "tp_mult":       2.0,
    "hold_bars":     6,
    "size_mult":     1.0,
}

# ── helpers ────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def months_ago(n: int) -> int:
    dt = datetime.now(timezone.utc) - timedelta(days=30 * n)
    return int(dt.timestamp() * 1000)

def ts_to_month(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{dt.year}-{dt.month:02d}"

def load_candles(market: str) -> list:
    path = os.path.join(DATA_CACHE, f"{market}_{INTERVAL}_candles.json")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    return data["candles"] if isinstance(data, dict) else data

def load_evidence(market: str) -> dict:
    path = os.path.join(ANT_OUT, f"learning_evidence_{market.replace('-','_')}.json")
    if not os.path.exists(path):
        return {"market": market, "months": {}, "streak": 0, "best_params": None}
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def save_evidence(market: str, evidence: dict) -> None:
    path = os.path.join(ANT_OUT, f"learning_evidence_{market.replace('-','_')}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2)

# ── indicators ────────────────────────────────────────────────────────────
def sma(xs, n):
    if len(xs) < n: return []
    out, s = [], sum(xs[:n])
    out.append(s / n)
    for i in range(n, len(xs)):
        s += xs[i] - xs[i - n]
        out.append(s / n)
    return out

def stdev_window(xs, n):
    if len(xs) < n: return []
    return [statistics.pstdev(xs[i - n + 1:i + 1]) for i in range(n - 1, len(xs))]

def calc_rsi(xs, n=14):
    if len(xs) < n + 1: return []
    gains  = [max(xs[i] - xs[i-1], 0) for i in range(1, len(xs))]
    losses = [max(xs[i-1] - xs[i], 0) for i in range(1, len(xs))]
    out = []
    for i in range(n - 1, len(gains)):
        ag = sum(gains[i - n + 1:i + 1]) / n
        al = sum(losses[i - n + 1:i + 1]) / n
        rs = ag / (al + 1e-12)
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out

def calc_bb(closes, n=20, k=2.0):
    mid = sma(closes, n)
    sd  = stdev_window(closes, n)
    if not mid or not sd: return {"mid": [], "lower": [], "width": []}
    lower, width = [], []
    for m, s in zip(mid, sd):
        lower.append(m - k * s)
        width.append((m + k*s - (m - k*s)) / m if m else 0.0)
    return {"mid": mid, "lower": lower, "width": width}

# ── parameterized backtest ─────────────────────────────────────────────────
def run_backtest(closes: List[float], timestamps: List[int],
                 params: dict, start_ms: int, end_ms: int) -> dict:
    """
    Backtest met gegeven parameters over een tijdvenster.
    Geeft metrics terug: trades, winrate, profit_factor, max_dd, return_pct
    """
    rsi_thr  = params["rsi_threshold"]
    bb_max   = params["bb_width_max"]
    sl_pct   = params["sl_pct"]
    tp_mult  = params["tp_mult"]
    hold     = params["hold_bars"]
    size_m   = params["size_mult"]
    risk_frac = 0.25 * size_m

    # Filter op tijdvenster
    indices = [i for i, ts in enumerate(timestamps) if start_ms <= ts <= end_ms]
    if len(indices) < 60:
        return {"valid": False, "reason": "insufficient_data"}

    start_i = indices[0]
    end_i   = indices[-1]

    # Pre-compute indicatoren over volledig beschikbare data
    all_rsi = calc_rsi(closes[:end_i + 1], 14)
    all_bb  = calc_bb(closes[:end_i + 1], 20, 2.0)
    rsi_off, bb_off = 14, 19

    equity = 1000.0
    curve  = [equity]
    trades = []
    i = max(start_i, 50)

    while i <= end_i - 1:
        ri = i - rsi_off
        bi = i - bb_off
        if ri < 0 or bi < 0 or not all_bb["lower"]:
            i += 1; continue

        # Bounds check voor pre-computed arrays
        if ri >= len(all_rsi) or bi >= len(all_bb["lower"]):
            i += 1; continue

        cur_rsi = all_rsi[ri]
        cur_low = all_bb["lower"][bi]
        cur_mid = all_bb["mid"][bi]
        cur_wid = all_bb["width"][bi]
        cur     = closes[i]

        if cur_rsi <= rsi_thr and cur_wid <= bb_max and cur <= cur_low * 1.01:
            entry_price = cur
            # TP is afstand naar BB-mid * multiplier
            tp_dist = abs(cur_mid - cur)
            tp_price = cur + tp_dist * tp_mult
            sl_price = cur * (1 - sl_pct)

            exit_i, exit_price, exit_reason = None, None, "TIME"
            max_j = min(end_i, i + hold)
            for j in range(i + 1, max_j + 1):
                px = closes[j]
                if px <= sl_price:
                    exit_i, exit_price, exit_reason = j, px, "SL"; break
                if px >= tp_price:
                    exit_i, exit_price, exit_reason = j, px, "TP"; break
            if exit_i is None:
                exit_i = max_j; exit_price = closes[exit_i]

            pnl = (exit_price / entry_price) - 1.0
            equity *= 1.0 + (pnl * risk_frac)
            curve.append(equity)
            trades.append({"pnl": pnl, "win": pnl > 0, "reason": exit_reason,
                            "month": ts_to_month(timestamps[i])})
            i = exit_i + 1
        else:
            curve.append(equity); i += 1

    if len(trades) < MIN_TRADES:
        return {"valid": False, "reason": f"too_few_trades:{len(trades)}"}

    wins      = sum(1 for t in trades if t["win"])
    gross_win = sum(t["pnl"] for t in trades if t["win"])
    gross_los = abs(sum(t["pnl"] for t in trades if not t["win"]))
    pf  = round(gross_win / gross_los, 4) if gross_los > 0 else None
    wr  = round(wins / len(trades), 4)

    peak, mdd = curve[0], 0.0
    for x in curve:
        peak = max(peak, x)
        mdd  = max(mdd, (peak - x) / peak if peak > 0 else 0.0)

    return {
        "valid":         True,
        "trades":        len(trades),
        "winrate":       wr,
        "profit_factor": pf,
        "max_dd_pct":    round(mdd * 100, 2),
        "return_pct":    round((equity / 1000.0 - 1.0) * 100, 2),
        "params":        params,
    }

# ── grid search ────────────────────────────────────────────────────────────
def grid_search(closes, timestamps, start_ms, end_ms) -> Optional[dict]:
    """
    Test alle parametercombinaties. Geeft beste terug op basis van PF.
    Alleen combinaties met PF >= threshold en voldoende trades.
    """
    import itertools

    keys   = list(GRID.keys())
    values = list(GRID.values())
    best   = None

    for combo in itertools.product(*values):
        params = dict(zip(keys, combo))
        result = run_backtest(closes, timestamps, params, start_ms, end_ms)
        if not result["valid"]:
            continue
        pf = result["profit_factor"]
        if pf is None or pf < PF_THRESHOLD:
            continue
        if best is None or pf > best["profit_factor"]:
            best = result

    return best

# ── evidence accumulator ───────────────────────────────────────────────────
def update_evidence(market: str, closes: list, timestamps: list,
                    window_months: int) -> dict:
    """
    Test de huidige maand met grid search.
    Update evidence: hoeveel opeenvolgende maanden scoort de beste config sterk?
    """
    now_ms    = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms  = months_ago(window_months)
    this_month = ts_to_month(now_ms)

    print(f"    Grid search over {window_months}m window ({ts_to_month(start_ms)} → {this_month})...")

    best = grid_search(closes, timestamps, start_ms, now_ms)
    evidence = load_evidence(market)

    if best is None:
        print(f"    Geen sterke config gevonden (PF < {PF_THRESHOLD} of te weinig trades)")
        evidence["months"][this_month] = {"result": "weak", "pf": None}
        # Reset streak
        evidence["streak"] = 0
        save_evidence(market, evidence)
        return evidence

    pf = best["profit_factor"]
    print(f"    Beste config: PF={pf:.4f}, trades={best['trades']}, "
          f"WR={best['winrate']:.1%}, DD={best['max_dd_pct']}%")
    print(f"    Params: RSI≤{best['params']['rsi_threshold']}, "
          f"BB≤{best['params']['bb_width_max']:.2f}, "
          f"SL={best['params']['sl_pct']:.1%}, "
          f"hold={best['params']['hold_bars']}bars, "
          f"size={best['params']['size_mult']}")

    evidence["months"][this_month] = {
        "result": "strong",
        "pf": pf,
        "params": best["params"],
        "trades": best["trades"],
        "winrate": best["winrate"],
        "max_dd_pct": best["max_dd_pct"],
    }

    # Bereken streak: hoeveel opeenvolgende maanden sterk?
    sorted_months = sorted(evidence["months"].keys())
    streak = 0
    for m in reversed(sorted_months):
        if evidence["months"][m].get("result") == "strong":
            streak += 1
        else:
            break
    evidence["streak"] = streak

    # Update best_params alleen als streak lang genoeg
    if streak >= MIN_MONTHS:
        # Kies de meest voorkomende params over de streak-maanden
        streak_months = sorted_months[-streak:]
        param_counts: Dict[str, Dict] = {}
        for m in streak_months:
            p = evidence["months"][m].get("params", {})
            key = json.dumps(p, sort_keys=True)
            param_counts[key] = p

        # Meest recente wins als er meerdere zijn
        evidence["best_params"] = list(param_counts.values())[-1]
        evidence["streak_months"] = streak_months
        print(f"    ✓ Streak: {streak} maanden sterk → aanbeveling beschikbaar")
    else:
        print(f"    Streak: {streak}/{MIN_MONTHS} — nog niet genoeg bewijs")

    save_evidence(market, evidence)
    return evidence

# ── aanbeveling schrijven ──────────────────────────────────────────────────
def write_advice(all_evidence: Dict[str, dict]) -> None:
    """
    Schrijft queen_learning_advice.json met alle markten die klaar zijn
    voor aanpassing. Dit is een AANBEVELING — geen automatische aanpassing.
    """
    advice = {
        "version":   "queen_learning_advice_v1",
        "ts_utc":    utc_now(),
        "generated_by": "queen_learning_engine",
        "status":    "PENDING_APPROVAL",
        "note":      "Dit is een aanbeveling. Menselijke goedkeuring vereist voor implementatie.",
        "markets":   {},
    }

    has_advice = False
    for market, evidence in all_evidence.items():
        streak = evidence.get("streak", 0)
        best   = evidence.get("best_params")
        current = DEFAULT_PARAMS.copy()

        if streak >= MIN_MONTHS and best:
            # Vergelijk met huidige defaults
            changes = {}
            for k, v in best.items():
                if k in current and current[k] != v:
                    changes[k] = {"from": current[k], "to": v}

            if changes:
                advice["markets"][market] = {
                    "status":        "RECOMMEND_CHANGE",
                    "streak_months": evidence.get("streak_months", []),
                    "streak":        streak,
                    "current_params": current,
                    "recommended_params": best,
                    "changes":       changes,
                }
                has_advice = True
                print(f"  ✓ {market}: aanbeveling klaar ({streak} maanden sterk)")
                for k, v in changes.items():
                    print(f"       {k}: {v['from']} → {v['to']}")
            else:
                advice["markets"][market] = {
                    "status": "OPTIMAL",
                    "note":   "Huidige params zijn al optimaal",
                    "streak": streak,
                }
                print(f"  ✓ {market}: huidige params al optimaal ({streak} maanden)")
        else:
            advice["markets"][market] = {
                "status": "INSUFFICIENT_EVIDENCE",
                "streak": streak,
                "needed": MIN_MONTHS,
            }

    advice["has_actionable_advice"] = has_advice

    path = os.path.join(ANT_OUT, "queen_learning_advice.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(advice, f, indent=2)

    print(f"\n  → Advice geschreven: {path}")
    if has_advice:
        print("  ⚠ Aanbevelingen aanwezig — controleer queen_learning_advice.json")
    else:
        print("  Geen aanbevelingen klaar (onvoldoende bewijs)")

# ── main ───────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",        default=None)
    parser.add_argument("--window-months", type=int, default=WINDOW_MONTHS)
    parser.add_argument("--min-months",    type=int, default=MIN_MONTHS)
    parser.add_argument("--pf-threshold",  type=float, default=PF_THRESHOLD)
    args = parser.parse_args()

    markets = [args.market] if args.market else MARKETS

    print("=" * 60)
    print("Queen Learning Engine")
    print(f"  Window:   {args.window_months} maanden")
    print(f"  Drempel:  PF ≥ {args.pf_threshold}, {args.min_months} aaneengesloten maanden")
    print(f"  Markten:  {', '.join(markets)}")
    print("=" * 60)
    print()

    os.makedirs(ANT_OUT, exist_ok=True)
    all_evidence = {}

    for market in markets:
        print(f"  {market}")
        candles = load_candles(market)
        if len(candles) < 200:
            print(f"    SKIP — te weinig data ({len(candles)} candles)")
            continue

        closes     = [float(c[4]) for c in candles]
        timestamps = [int(c[0]) for c in candles]

        evidence = update_evidence(
            market, closes, timestamps,
            window_months=args.window_months,
        )
        all_evidence[market] = evidence
        print()

    print("─" * 60)
    write_advice(all_evidence)

    # Samenvatting
    ready = sum(1 for e in all_evidence.values()
                if e.get("streak", 0) >= MIN_MONTHS)
    print(f"\n  Markten klaar voor aanpassing: {ready}/{len(all_evidence)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
