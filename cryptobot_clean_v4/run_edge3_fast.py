from __future__ import annotations

import argparse
import datetime as dt
import json
import os

from edge3_core_v1 import backtest_edge3


REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
SNAP_PATH   = os.path.join(REPORTS_DIR, "edge3_snapshot.json")


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--market", default=os.getenv("CB20_MARKET", "BTC-EUR"))
    p.add_argument("--interval", default=os.getenv("CB20_INTERVAL", "4h"))
    p.add_argument("--start-iso", required=True)
    p.add_argument("--end-iso", required=True)

    p.add_argument("--initial-equity", type=float, default=1000.0)
    p.add_argument("--taker-fee", type=float, default=0.0025)
    p.add_argument("--maker-fee", type=float, default=0.0015)
    p.add_argument("--slippage-bps", type=float, default=3.0)

    p.add_argument("--entry-mode", default="limit_maker", choices=["market", "limit_maker", "limit_taker"])
    p.add_argument("--reclaim-limit-offset-bps", type=float, default=-10.0)
    p.add_argument("--fill-prob", type=float, default=0.70)
    p.add_argument("--stop-extra-slip-bps", type=float, default=1.0)

    p.add_argument("--vol-filter", default="atr_percentile", choices=["off", "atr_percentile"])
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--atr-regime-window", type=int, default=200)
    p.add_argument("--atr-regime-percentile", type=float, default=0.50)

    p.add_argument("--position-fraction", type=float, default=0.50)
    p.add_argument("--dump-json", action="store_true", help="also print full result JSON to stdout")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    res = backtest_edge3(
        market=args.market,
        interval=args.interval,
        start_iso=args.start_iso,
        end_iso=args.end_iso,
        initial_equity=args.initial_equity,
        taker_fee=args.taker_fee,
        maker_fee=args.maker_fee,
        slippage_bps=args.slippage_bps,
        entry_mode=args.entry_mode,
        reclaim_limit_offset_bps=args.reclaim_limit_offset_bps,
        fill_prob=args.fill_prob,
        stop_extra_slip_bps=args.stop_extra_slip_bps,
        vol_filter=args.vol_filter,
        atr_period=args.atr_period,
        atr_regime_window=args.atr_regime_window,
        atr_regime_percentile=args.atr_regime_percentile,
        position_fraction=args.position_fraction,
    )

    closed_trades = res.get("closed_trades")
    winrate = res.get("winrate")
    pf = res.get("profit_factor")
    ending_equity = res.get("ending_equity")
    maxdd = res.get("analytics", {}).get("max_drawdown_frac") or res.get("analytics", {}).get("max_drawdown_pct")

    print("")
    print("========== EDGE3 SUMMARY ==========")
    print(f"Closed trades : {closed_trades}")
    print(f"Winrate       : {float(winrate):.3f}")
    print(f"Profit factor : {float(pf):.3f}")
    print(f"Ending equity : {float(ending_equity):.2f}")
    if maxdd is not None:
        try:
            print(f"Max DD        : {float(maxdd):.3f}")
        except Exception:
            print(f"Max DD        : {maxdd}")
    print("===================================")
    print("")

    snapshot = {
        "ts_utc": dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat(),
        "market": args.market,
        "interval": args.interval,
        "closed_trades": closed_trades,
        "winrate": float(winrate),
        "profit_factor": float(pf),
        "ending_equity": float(ending_equity),
        "max_dd": float(maxdd) if maxdd is not None else None,
        "position_fraction": float(args.position_fraction),
        "params": {
            "entry_mode": args.entry_mode,
            "reclaim_limit_offset_bps": args.reclaim_limit_offset_bps,
            "fill_prob": args.fill_prob,
            "vol_filter": args.vol_filter,
            "atr_period": args.atr_period,
            "atr_regime_window": args.atr_regime_window,
            "atr_regime_percentile": args.atr_regime_percentile,
        },
    }

    atomic_write_json(SNAP_PATH, snapshot)
    print(f"Snapshot written to {SNAP_PATH}")

    if args.dump_json:
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()