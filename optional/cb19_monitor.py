# cb19_monitor.py
# CB-19 Live Monitoring Dashboard v1 (observability layer)
# - Computes rolling PF/winrate, DD, fill ratios, and fail-safe flags from EDGE3 results
# - No strategy changes; read-only analytics

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import json
import math
import os
import time


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _profit_factor(pnls: List[float]) -> Optional[float]:
    gross_profit = 0.0
    gross_loss = 0.0
    for p in pnls:
        if p > 0:
            gross_profit += p
        else:
            gross_loss += (-p)
    if gross_loss <= 0:
        return None if gross_profit <= 0 else float("inf")
    return gross_profit / gross_loss


def _winrate(pnls: List[float]) -> float:
    if not pnls:
        return 0.0
    wins = sum(1 for p in pnls if p > 0)
    return wins / len(pnls)


def _max_drawdown_from_equity(equity_curve: List[Tuple[int, float]]) -> float:
    # returns fraction (e.g. 0.12 == 12%)
    if not equity_curve:
        return 0.0
    peak = equity_curve[0][1]
    max_dd = 0.0
    for _, eq in equity_curve:
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _fmt_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


@dataclass
class CB19Thresholds:
    rolling_n: int = 20
    pf_fail_below: float = 1.0
    dd_fail_above: float = 0.12  # 12% default; tune later without touching EDGE3
    fill_ratio_min: float = 0.30
    fill_ratio_max: float = 0.95
    vol_block_ratio_max: float = 0.60  # fraction of pending entries blocked by vol-filter
    size_block_ratio_max: float = 0.60  # fraction of pending entries blocked by sizing/insufficient notional


def compute_cb19_snapshot(
    edge3_result: Dict[str, Any],
    thresholds: Optional[CB19Thresholds] = None,
) -> Dict[str, Any]:
    thr = thresholds or CB19Thresholds()

    trades = edge3_result.get("trades", []) or []
    equity_curve_raw = edge3_result.get("equity_curve", []) or []
    debug = edge3_result.get("debug", {}) or {}
    analytics = edge3_result.get("analytics", {}) or {}

    # Normalize equity curve -> list[(ts, eq)]
    equity_curve: List[Tuple[int, float]] = []
    for row in equity_curve_raw:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            equity_curve.append((int(row[0]), _safe_float(row[1])))
        elif isinstance(row, dict) and "ts" in row and "equity" in row:
            equity_curve.append((int(row["ts"]), _safe_float(row["equity"])))

    pnls_all = [_safe_float(t.get("pnl")) for t in trades]
    last_n_pnls = pnls_all[-thr.rolling_n :] if len(pnls_all) >= 1 else []

    pf_roll = _profit_factor(last_n_pnls)
    wr_roll = _winrate(last_n_pnls)
    pf_all = _profit_factor(pnls_all)
    wr_all = _winrate(pnls_all)

    max_dd = _safe_float(analytics.get("max_drawdown_pct", _max_drawdown_from_equity(equity_curve)))
    # analytics.max_drawdown_pct is already fraction (per core), keep consistent
    max_dd_frac = max_dd

    closed_trades = int(edge3_result.get("closed_trades") or len(trades))
    ending_equity = _safe_float(edge3_result.get("ending_equity"))
    total_pnl = _safe_float(edge3_result.get("total_pnl_eur"))
    profit_factor = edge3_result.get("profit_factor", None)
    winrate = edge3_result.get("winrate", None)

    # Fill ratio proxy (maker reclaim fills vs reclaim signals)
    reclaim_signal = float(debug.get("reclaim_signal_count", 0) or 0)
    reclaim_fill = float(debug.get("reclaim_fill_count", 0) or 0)
    fill_ratio = (reclaim_fill / reclaim_signal) if reclaim_signal > 0 else None

    # Vol block ratio proxy (vol blocks per times pending was set)
    pending_set = float(debug.get("pending_set_count", 0) or 0)
    vol_block = float(debug.get("vol_block_count", 0) or 0)
    size_block = float(debug.get("size_block_count", 0) or 0)

    vol_block_ratio = (vol_block / pending_set) if pending_set > 0 else None
    size_block_ratio = (size_block / pending_set) if pending_set > 0 else None

    # Fail-safe flags
    flags: Dict[str, Any] = {}

    if pf_roll is not None and math.isfinite(pf_roll):
        flags["pf_roll_fail"] = pf_roll < thr.pf_fail_below
    else:
        flags["pf_roll_fail"] = True  # unknown PF -> treat as fail-safe

    flags["dd_fail"] = max_dd_frac > thr.dd_fail_above

    if fill_ratio is None:
        flags["fill_ratio_fail"] = True
    else:
        flags["fill_ratio_fail"] = not (thr.fill_ratio_min <= fill_ratio <= thr.fill_ratio_max)

    if vol_block_ratio is None:
        flags["vol_block_fail"] = False  # if no pending signals, don’t fail
    else:
        flags["vol_block_fail"] = vol_block_ratio > thr.vol_block_ratio_max

    if size_block_ratio is None:
        flags["size_block_fail"] = False
    else:
        flags["size_block_fail"] = size_block_ratio > thr.size_block_ratio_max

    flags["any_fail"] = any(bool(v) for v in flags.values())

    snapshot = {
        "cb": "CB-19",
        "version": "v1",
        "ts_unix": int(time.time()),
        "market": edge3_result.get("market"),
        "interval": edge3_result.get("interval"),
        "start_iso": edge3_result.get("start_iso"),
        "end_iso": edge3_result.get("end_iso"),
        "headline": {
            "ending_equity": ending_equity,
            "total_pnl_eur": total_pnl,
            "closed_trades": closed_trades,
            "profit_factor_all": pf_all,
            "winrate_all": wr_all,
            "max_dd_frac": max_dd_frac,
        },
        "rolling": {
            "n": thr.rolling_n,
            "profit_factor": pf_roll,
            "winrate": wr_roll,
        },
        "proxies": {
            "fill_ratio": fill_ratio,
            "vol_block_ratio": vol_block_ratio,
            "size_block_ratio": size_block_ratio,
            "reclaim_signal_count": int(reclaim_signal),
            "reclaim_fill_count": int(reclaim_fill),
            "pending_set_count": int(pending_set),
            "vol_block_count": int(vol_block),
            "size_block_count": int(size_block),
        },
        "thresholds": asdict(thr),
        "flags": flags,
    }

    # Keep also original EDGE3 fields for convenience (read-only)
    snapshot["edge3"] = {
        "profit_factor": profit_factor,
        "winrate": winrate,
        "analytics": analytics,
        "debug": debug,
    }

    return snapshot


def print_cb19_dashboard(snapshot: Dict[str, Any]) -> None:
    h = snapshot.get("headline", {})
    r = snapshot.get("rolling", {})
    p = snapshot.get("proxies", {})
    f = snapshot.get("flags", {})

    pf_roll = r.get("profit_factor", None)
    wr_roll = r.get("winrate", None)

    def fmt_pf(x: Any) -> str:
        if x is None:
            return "None"
        try:
            if x == float("inf"):
                return "INF"
            return f"{float(x):.3f}"
        except Exception:
            return "None"

    def fmt_ratio(x: Any) -> str:
        if x is None:
            return "None"
        try:
            return f"{float(x):.3f}"
        except Exception:
            return "None"

    print("")
    print("========================================")
    print(f" CB-19 LIVE MONITORING DASHBOARD v1")
    print("========================================")
    print(f" Market: {snapshot.get('market')}   TF: {snapshot.get('interval')}")
    print(f" Range : {snapshot.get('start_iso')}  ->  {snapshot.get('end_iso')}")
    print("----------------------------------------")
    print(f" Equity End : {h.get('ending_equity'):.2f} EUR")
    print(f" Total PnL  : {h.get('total_pnl_eur'):.2f} EUR")
    print(f" Trades     : {h.get('closed_trades')}")
    print(f" MaxDD      : {_fmt_pct(_safe_float(h.get('max_dd_frac')))}")
    print("----------------------------------------")
    print(f" Rolling (last {r.get('n')} trades)")
    print(f"   PF       : {fmt_pf(pf_roll)}")
    print(f"   Winrate  : {_fmt_pct(_safe_float(wr_roll))}")
    print("----------------------------------------")
    print(" Proxies")
    print(f"   FillRatio     : {fmt_ratio(p.get('fill_ratio'))} (fills/signals)")
    print(f"   VolBlockRatio : {fmt_ratio(p.get('vol_block_ratio'))} (vol_blocks/pending)")
    print(f"   SizeBlockRatio: {fmt_ratio(p.get('size_block_ratio'))} (size_blocks/pending)")
    print("----------------------------------------")
    print(" Fail-safe flags")
    for k in ["pf_roll_fail", "dd_fail", "fill_ratio_fail", "vol_block_fail", "size_block_fail", "any_fail"]:
        print(f"   {k:<16} : {bool(f.get(k))}")
    print("========================================")
    print("")


def write_snapshot_json(path: str, snapshot: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)


if __name__ == "__main__":
    # Utility mode: read EDGE3 result JSON from stdin or a file path passed as first arg
    import sys

    if len(sys.argv) >= 2 and os.path.exists(sys.argv[1]):
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            edge3 = json.load(f)
    else:
        edge3 = json.loads(sys.stdin.read())

    snap = compute_cb19_snapshot(edge3)
    print_cb19_dashboard(snap)
    # default write
    write_snapshot_json(os.path.join("reports", "cb19_snapshot.json"), snap)