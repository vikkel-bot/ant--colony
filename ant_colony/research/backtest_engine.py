from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import math

from enhanced_edges_catalog import detect_extreme_mean_reversion, detect_volume_breakout

@dataclass
class Trade:
    edge_name: str
    entry_i: int
    exit_i: int
    entry_price: float
    exit_price: float
    pnl_frac: float
    exit_reason: str

def max_drawdown(equity: List[float]) -> float:
    peak = equity[0]
    mdd = 0.0
    for x in equity:
        if x > peak:
            peak = x
        dd = 0.0 if peak <= 0 else (peak - x) / peak
        if dd > mdd:
            mdd = dd
    return mdd

def run_single_market_backtest(
    closes: List[float],
    volumes: Optional[List[float]] = None,
    initial_equity: float = 1000.0,
    risk_fraction: float = 0.25
) -> Dict:
    equity = initial_equity
    curve = [equity]
    trades: List[Trade] = []

    i = 50
    while i < len(closes) - 1:
        sig = detect_extreme_mean_reversion(closes[:i + 1])
        if sig is None and volumes is not None:
            sig = detect_volume_breakout(closes[:i + 1], volumes[:i + 1])

        if sig is None or sig.side != "long":
            curve.append(equity)
            i += 1
            continue

        entry_i = i
        entry_price = closes[i]
        exit_i = None
        exit_price = None
        exit_reason = "TIME"

        max_j = min(len(closes) - 1, i + sig.hold_bars)
        for j in range(i + 1, max_j + 1):
            px = closes[j]
            if px <= sig.sl_price:
                exit_i = j
                exit_price = px
                exit_reason = "SL"
                break
            if px >= sig.tp_price:
                exit_i = j
                exit_price = px
                exit_reason = "TP"
                break

        if exit_i is None:
            exit_i = max_j
            exit_price = closes[exit_i]

        pnl_frac = (exit_price / entry_price) - 1.0
        equity *= 1.0 + (pnl_frac * risk_fraction)
        curve.append(equity)

        trades.append(Trade(
            edge_name=sig.edge_name,
            entry_i=entry_i,
            exit_i=exit_i,
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_frac=pnl_frac,
            exit_reason=exit_reason
        ))

        i = exit_i + 1

    wins = sum(1 for t in trades if t.pnl_frac > 0)
    losses = sum(1 for t in trades if t.pnl_frac <= 0)
    gross_win = sum(t.pnl_frac for t in trades if t.pnl_frac > 0)
    gross_loss = abs(sum(t.pnl_frac for t in trades if t.pnl_frac <= 0))
    pf = None if gross_loss == 0 else gross_win / gross_loss

    return {
        "initial_equity": initial_equity,
        "ending_equity": round(equity, 8),
        "closed_trades": len(trades),
        "wins": wins,
        "losses": losses,
        "winrate": None if len(trades) == 0 else round(wins / len(trades), 6),
        "profit_factor": None if pf is None else round(pf, 6),
        "max_drawdown_frac": round(max_drawdown(curve), 6),
        "equity_curve_points": len(curve),
        "trades": [asdict(t) for t in trades],
    }