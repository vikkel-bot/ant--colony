# edge3_core_v1.py
# EDGE3 Mean Reversion v2 reclaim - CORE (no IO) - v3.4.1 DD brake + exit-reason cooldown
#
# Includes:
# - Maker/taker fees + slippage
# - Maker fill probability (deterministic)
# - Extra adverse slippage on STOP exits
# - Volatility regime filter (ATR percentile gate)
# - Equity curve + drawdown analytics
# - Position sizing via position_fraction (0..1)
# - NEW: Drawdown Brake (dynamic risk throttle based on equity drawdown from peak)
# - NEW: Exit-reason cooldown (post TP / post SL / post TIME)
#
# IMPORTANT:
# - Pending is set on breach regardless of regime
# - Regime filter blocks ENTRY only
# - Position fraction scales position notional; unallocated cash stays idle
# - DD brake only affects sizing at ENTRY
# - Sanity check: exact 0 delta

from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple
import math

getcontext().prec = 80


def D(x: Any) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def sma(values: List[Decimal], period: int, idx: int) -> Optional[Decimal]:
    if idx + 1 < period:
        return None
    s = Decimal("0")
    for j in range(idx - period + 1, idx + 1):
        s += values[j]
    return s / D(period)


def stdev(values: List[Decimal], period: int, idx: int) -> Optional[Decimal]:
    if idx + 1 < period:
        return None
    mu = sma(values, period, idx)
    if mu is None:
        return None
    s = Decimal("0")
    for j in range(idx - period + 1, idx + 1):
        d = values[j] - mu
        s += d * d
    var = s / D(period)
    # match original behavior: sqrt via float math for stability
    return D(math.sqrt(float(var)))


def true_range(high: Decimal, low: Decimal, prev_close: Decimal) -> Decimal:
    a = high - low
    b = abs(high - prev_close)
    c = abs(low - prev_close)
    return max(a, b, c)


def atr(highs: List[Decimal], lows: List[Decimal], closes: List[Decimal], period: int, idx: int) -> Optional[Decimal]:
    if idx == 0:
        return None
    if idx + 1 < period + 1:
        return None
    s = Decimal("0")
    for j in range(idx - period + 1, idx + 1):
        tr = true_range(highs[j], lows[j], closes[j - 1])
        s += tr
    return s / D(period)


def percentile_rank(sorted_vals: List[Decimal], x: Decimal) -> Decimal:
    # returns rank in [0,1]
    n = len(sorted_vals)
    if n == 0:
        return Decimal("0")
    # count <= x
    lo = 0
    hi = n
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_vals[mid] <= x:
            lo = mid + 1
        else:
            hi = mid
    return D(lo) / D(n)


def apply_slippage(price: Decimal, side: str, slippage_bps: Decimal) -> Decimal:
    # buy: price increases; sell: price decreases
    slip = slippage_bps / D("10000")
    if side == "buy":
        return price * (D("1") + slip)
    else:
        return price * (D("1") - slip)


def fee_amount(notional: Decimal, fee_rate: Decimal) -> Decimal:
    return notional * fee_rate


@dataclass
class Fees:
    taker: Decimal
    maker: Decimal


@dataclass
class ExecModel:
    slippage_bps: Decimal


@dataclass
class StrategyParams:
    bb_period: int = 20
    bb_std: Decimal = D("2.0")
    breach_by: str = "low"  # "low" or "close"
    reclaim_buffer_bps: Decimal = D("0")
    signal_filter: str = "off"  # "off" | "close_above_sma" | "close_below_sma"

    tp_pct: Decimal = D("0.008")
    sl_pct: Decimal = D("0.010")
    max_hold_bars: int = 72
    cooldown_bars: int = 0

    # NEW: cooldown by exit reason (overrides/augments cooldown_bars)
    post_tp_cooldown_bars: int = 0
    post_sl_cooldown_bars: int = 0
    post_time_cooldown_bars: int = 0

    fill_probability: Decimal = D("0.70")
    entry_mode: str = "close_taker"  # "close_taker" or "limit_maker"
    reclaim_limit_offset_bps: Decimal = D("0")

    vol_filter: str = "off"  # "off" or "atr_percentile"
    atr_period: int = 14
    atr_regime_window: int = 200
    atr_regime_percentile: Decimal = D("0.50")

    position_fraction: Decimal = D("0.5")

    dd_brake: str = "off"  # "off" or "on"
    dd_brake_level1_pct: Decimal = D("0.02")
    dd_brake_level2_pct: Decimal = D("0.05")
    dd_brake_mult1: Decimal = D("0.85")
    dd_brake_mult2: Decimal = D("0.40")

    stop_extra_slippage_bps: Decimal = D("1")


@dataclass
class TradeRow:
    entry_ts: int
    exit_ts: int
    side: str  # "long" or "short"
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    pnl: Decimal
    reason: str
    bars_held: int
    dd_at_entry_pct: Decimal


def _calc_drawdown_metrics(equity_curve: List[Tuple[int, Decimal]]) -> Dict[str, Any]:
    if not equity_curve:
        return {
            "max_drawdown_pct": 0.0,
            "max_drawdown_eur": 0.0,
            "max_drawdown_duration_bars": 0,
        }
    peak = equity_curve[0][1]
    max_dd = Decimal("0")
    max_dd_eur = Decimal("0")
    dd_start_idx = 0
    max_dur = 0
    cur_dd_start = None

    for i, (_, eq) in enumerate(equity_curve):
        if eq > peak:
            peak = eq
            cur_dd_start = None
        dd = (peak - eq)
        if peak > 0:
            dd_pct = dd / peak
        else:
            dd_pct = Decimal("0")

        if dd_pct > 0 and cur_dd_start is None:
            cur_dd_start = i

        if dd_pct == 0 and cur_dd_start is not None:
            dur = i - cur_dd_start
            if dur > max_dur:
                max_dur = dur
            cur_dd_start = None

        if dd_pct > max_dd:
            max_dd = dd_pct
            max_dd_eur = dd

    # close open dd segment
    if cur_dd_start is not None:
        dur = (len(equity_curve) - 1) - cur_dd_start
        if dur > max_dur:
            max_dur = dur

    return {
        "max_drawdown_pct": float(max_dd),
        "max_drawdown_eur": float(max_dd_eur),
        "max_drawdown_duration_bars": int(max_dur),
    }


def _calc_trade_streaks(trades: List[TradeRow]) -> Dict[str, Any]:
    max_consec_losses = 0
    cur = 0
    for t in trades:
        if t.pnl <= 0:
            cur += 1
            max_consec_losses = max(max_consec_losses, cur)
        else:
            cur = 0
    return {"max_consecutive_losses": int(max_consec_losses)}


def _exit_reason_cooldown(p: StrategyParams, exit_reason: str) -> int:
    """Cooldown to apply after closing a trade, based on exit reason."""
    base = int(p.cooldown_bars)
    if exit_reason == "TP":
        return max(base, int(p.post_tp_cooldown_bars))
    if exit_reason == "SL":
        return max(base, int(p.post_sl_cooldown_bars))
    if exit_reason == "TIME":
        return max(base, int(p.post_time_cooldown_bars))
    return base


def dd_brake_multiplier(equity: Decimal, equity_peak: Decimal, p: StrategyParams) -> Decimal:
    if p.dd_brake != "on":
        return D("1")

    if equity_peak <= 0:
        return D("1")

    dd = (equity_peak - equity) / equity_peak

    if dd >= p.dd_brake_level2_pct:
        return p.dd_brake_mult2
    if dd >= p.dd_brake_level1_pct:
        return p.dd_brake_mult1
    return D("1")


def backtest_edge3_core(
    ts: List[int],
    opens: List[Decimal],
    highs: List[Decimal],
    lows: List[Decimal],
    closes: List[Decimal],
    fees: Fees,
    execm: ExecModel,
    p: StrategyParams,
    initial_equity: Decimal,
    regime_mult_by_bar=None
) -> Dict[str, Any]:

    n = len(ts)
    if regime_mult_by_bar is not None and len(regime_mult_by_bar) != n:
        raise ValueError("regime_mult_by_bar length mismatch")
    if not (len(opens) == len(highs) == len(lows) == len(closes) == n):
        raise ValueError("OHLC lengths mismatch")

    equity = D(initial_equity)
    initial = D(initial_equity)
    equity_peak = D(initial_equity)
    total_pnl = D("0")

    in_pos = False
    entry_price = D("0")
    qty = D("0")
    entry_fee = D("0")
    entry_ts = 0
    bars_held = 0
    tp_price = D("0")
    sl_price = D("0")

    cooldown_left = 0
    pending = False
    pending_set_count = 0

    closed_trades = 0
    wins = 0
    gross_profit = D("0")
    gross_loss = D("0")

    equity_curve: List[Tuple[int, Decimal]] = []
    trades: List[TradeRow] = []

    # debug counters
    breach_count = 0
    reclaim_signal_count = 0
    reclaim_fill_count = 0
    reclaim_miss_count = 0
    entry_fill_count = 0
    vol_block_count = 0
    size_block_count = 0

    dd_brake_lvl0_count = 0
    dd_brake_lvl1_count = 0
    dd_brake_lvl2_count = 0

    # precompute ATR for regime filter
    atr_series: List[Optional[Decimal]] = [None] * n
    for i in range(n):
        a = atr(highs, lows, closes, p.atr_period, i)
        atr_series[i] = a

    for i in range(n):
        # cooldown tick
        if cooldown_left > 0:
            cooldown_left -= 1

        bb_mid = sma(closes, p.bb_period, i)
        bb_sd = stdev(closes, p.bb_period, i)
        if bb_mid is None or bb_sd is None:
            equity_curve.append((ts[i], equity))
            continue
        lower = bb_mid - (p.bb_std * bb_sd)

        # ---- Manage open position ----
        if in_pos:
            bars_held += 1

            sl_hit = (lows[i] <= sl_price)
            tp_hit = (highs[i] >= tp_price)

            exit_reason = None
            exit_price = None
            exit_fee = None

            if sl_hit:
                exit_reason = "SL"
                raw = sl_price
                total_slip = execm.slippage_bps + p.stop_extra_slippage_bps
                exec_px = apply_slippage(raw, "sell", total_slip)
                exit_price = exec_px
                exit_fee = fee_amount(qty * exec_px, fees.taker)

            elif tp_hit:
                exit_reason = "TP"
                exec_px = tp_price
                exit_price = exec_px
                exit_fee = fee_amount(qty * exec_px, fees.maker)

            elif bars_held >= p.max_hold_bars:
                exit_reason = "TIME"
                raw = closes[i]
                exec_px = apply_slippage(raw, "sell", execm.slippage_bps)
                exit_price = exec_px
                exit_fee = fee_amount(qty * exec_px, fees.taker)

            if exit_reason is not None:
                entry_notional = qty * entry_price
                exit_notional = qty * exit_price
                pnl = (exit_notional - entry_notional) - entry_fee - exit_fee

                equity = equity + pnl
                total_pnl = equity - initial  # exact

                closed_trades += 1
                if pnl > 0:
                    wins += 1
                    gross_profit += pnl
                else:
                    gross_loss += (-pnl)

                # peak update immediately after equity change
                if equity > equity_peak:
                    equity_peak = equity

                trades.append(
                    TradeRow(
                        entry_ts=entry_ts,
                        exit_ts=ts[i],
                        side=("long" if qty > D("0") else "short"),
                        entry_price=entry_price,
                        exit_price=exit_price,
                        qty=qty,
                        pnl=pnl,
                        reason=exit_reason,
                        bars_held=bars_held,
                        dd_at_entry_pct=D("0"),
                    )
                )

                in_pos = False
                pending = False
                bars_held = 0
                cooldown_left = _exit_reason_cooldown(p, exit_reason)

        # equity curve point each bar
        equity_curve.append((ts[i], equity))

        # ---- Entry logic ----
        if in_pos:
            continue

        # global cooldown blocks entries
        if cooldown_left > 0:
            continue

        # breach condition
        breach = False
        if p.breach_by == "low":
            breach = lows[i] < lower
        else:
            breach = closes[i] < lower
        if breach:
            breach_count += 1
            if not pending:
                pending = True
                pending_set_count += 1

        # reclaim condition
        reclaim_buffer = p.reclaim_buffer_bps / D("10000")
        reclaim_level = lower * (D("1") + reclaim_buffer)
        reclaim = closes[i] > reclaim_level
        if reclaim:
            reclaim_signal_count += 1

        # optional signal filter using SMA200 on close (simple)
        if p.signal_filter != "off":
            filt = sma(closes, 200, i)
            if filt is None:
                continue
            if p.signal_filter == "close_above_sma":
                if closes[i] <= filt:
                    continue
            elif p.signal_filter == "close_below_sma":
                if closes[i] >= filt:
                    continue

        # vol regime filter (blocks ENTRY only)
        if p.vol_filter == "atr_percentile":
            a = atr_series[i]
            if a is None:
                continue
            # regime window requires enough past ATR values
            if i + 1 < p.atr_regime_window:
                continue
            window = [x for x in atr_series[i - p.atr_regime_window + 1 : i + 1] if x is not None]
            if len(window) < p.atr_regime_window:
                continue
            window_sorted = sorted(window)
            rank = percentile_rank(window_sorted, a)
            if rank >= p.atr_regime_percentile:
                vol_block_count += 1
                continue

        # require pending + reclaim to fire
        if not (pending and reclaim):
            continue

        # determine entry price & fees
        if p.entry_mode == "close_taker":
            raw = closes[i]
            exec_px = apply_slippage(raw, "buy", execm.slippage_bps)
            entry_px = exec_px
            fee_rate = fees.taker

        else:
            off = p.reclaim_limit_offset_bps / D("10000")
            limit_px = reclaim_level * (D("1") + off)

            # deterministic fill model:
            # fill if price trades at/through limit (low <= limit), plus probability gate
            maker_touch = lows[i] <= limit_px
            if not maker_touch:
                reclaim_miss_count += 1
                pending = False
                continue

            # probability gate
            if p.fill_probability <= 0:
                reclaim_miss_count += 1
                pending = False
                continue
            if p.fill_probability < 1:
                # deterministic hash-like gate using bar index
                # if (i % denom) >= num -> miss
                denom = 1000
                num = int(float(p.fill_probability) * denom)
                if (i % denom) >= num:
                    reclaim_miss_count += 1
                    pending = False
                    continue

            entry_px = limit_px
            fee_rate = fees.maker
            reclaim_fill_count += 1

        # position sizing (fractional notional)
        mult = dd_brake_multiplier(equity, equity_peak, p)
        dd = (equity_peak - equity) / equity_peak if equity_peak > 0 else D("0")
        if dd >= p.dd_brake_level2_pct:
            dd_brake_lvl2_count += 1
        elif dd >= p.dd_brake_level1_pct:
            dd_brake_lvl1_count += 1
        else:
            dd_brake_lvl0_count += 1

        base_frac = p.position_fraction
        regime_mult = D("1")
        if regime_mult_by_bar is not None:
            regime_mult = regime_mult_by_bar[i]
        base_frac = base_frac * regime_mult
        eff_frac = base_frac * mult

        if eff_frac <= 0:
            size_block_count += 1
            pending = False
            continue

        notional = equity * eff_frac
        if notional <= 0:
            size_block_count += 1
            pending = False
            continue

        qty = notional / entry_px
        entry_fee = fee_amount(notional, fee_rate)

        entry_ts = ts[i]
        entry_price = entry_px
        tp_price = entry_price * (D("1") + p.tp_pct)
        sl_price = entry_price * (D("1") - p.sl_pct)
        bars_held = 0

        in_pos = True
        entry_fill_count += 1
        pending = False

    # if still in pos at end: close at last close taker
    if in_pos and n > 0:
        raw = closes[-1]
        exec_px = apply_slippage(raw, "sell", execm.slippage_bps)
        exit_fee = fee_amount(qty * exec_px, fees.taker)
        pnl = (qty * exec_px - qty * entry_price) - entry_fee - exit_fee
        equity = equity + pnl
        total_pnl = equity - initial

        closed_trades += 1
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        else:
            gross_loss += (-pnl)

        trades.append(
            TradeRow(
                entry_ts=entry_ts,
                exit_ts=ts[-1],
                entry_price=entry_price,
                exit_price=exec_px,
                qty=qty,
                pnl=pnl,
                reason="EOD",
                bars_held=bars_held,
                dd_at_entry_pct=D("0"),
            )
        )

        equity_curve.append((ts[-1], equity))

    # compute stats
    winrate = (D(wins) / D(closed_trades)) if closed_trades > 0 else D("0")
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None

    dd = _calc_drawdown_metrics(equity_curve)
    streaks = _calc_trade_streaks(trades)

    avg_trade_pnl = (total_pnl / D(closed_trades)) if closed_trades > 0 else D("0")
    years = (D(ts[-1] - ts[0]) / D(1000 * 60 * 60 * 24 * 365)) if n > 1 else D("0")
    trades_per_year = float(D(closed_trades) / years) if years > 0 else None

    analytics = {
        **dd,
        **streaks,
        "avg_trade_pnl_eur": float(avg_trade_pnl),
        "trades_per_year": trades_per_year,
        "position_fraction_base": float(base_frac),
        "cooldown_bars": int(p.cooldown_bars),
        "post_tp_cooldown_bars": int(p.post_tp_cooldown_bars),
        "post_sl_cooldown_bars": int(p.post_sl_cooldown_bars),
        "post_time_cooldown_bars": int(p.post_time_cooldown_bars),
        "dd_brake": p.dd_brake,
        "dd_brake_level1_pct": float(p.dd_brake_level1_pct),
        "dd_brake_level2_pct": float(p.dd_brake_level2_pct),
        "dd_brake_mult1": float(p.dd_brake_mult1),
        "dd_brake_mult2": float(p.dd_brake_mult2),
    }

    debug = {
        "breach_count": breach_count,
        "pending_set_count": pending_set_count,
        "reclaim_signal_count": reclaim_signal_count,
        "reclaim_fill_count": reclaim_fill_count,
        "reclaim_miss_count": reclaim_miss_count,
        "entry_fill_count": entry_fill_count,
        "vol_block_count": vol_block_count,
        "size_block_count": size_block_count,
        "dd_brake_lvl0_count": dd_brake_lvl0_count,
        "dd_brake_lvl1_count": dd_brake_lvl1_count,
        "dd_brake_lvl2_count": dd_brake_lvl2_count,
    }

    result = {
        "market": None,
        "interval": None,
        "start_iso": None,
        "end_iso": None,
        "closed_trades": int(closed_trades),
        "winrate": float(winrate),
        "profit_factor": float(profit_factor) if profit_factor is not None else None,
        "total_pnl_eur": float(total_pnl),
        "ending_equity": float(equity),
        "sanity_ending_minus_initial": float(equity - initial),
        "sanity_total_pnl_minus_delta": float(total_pnl - (equity - initial)),
        "analytics": analytics,
        "debug": debug,
        "equity_curve": [(t, float(eq)) for (t, eq) in equity_curve],
        "trades": [asdict(t) for t in trades],
    }

    return result