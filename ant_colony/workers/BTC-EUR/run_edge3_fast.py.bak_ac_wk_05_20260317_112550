from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from decimal import Decimal
from typing import Any, Dict, List

import edge3_fetch_cache as fc
import edge3_core_v1 as core

ROOT = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(ROOT, "reports")
DATA_CACHE_DIR = os.path.join(ROOT, "data_cache")

def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)

def utc_now_iso() -> str:
    # avoid deprecated utcnow()
    return dt.datetime.now(dt.timezone.utc).isoformat()

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

    # EDGE3 execution knobs (these belong in StrategyParams in this core)
    p.add_argument("--entry-mode", default="limit_maker", choices=["close_taker", "limit_maker", "market", "close"])
    p.add_argument("--reclaim-limit-offset-bps", type=float, default=-10.0)
    p.add_argument("--fill-prob", type=float, default=0.70)
    p.add_argument("--stop-extra-slip-bps", type=float, default=1.0)

    p.add_argument("--vol-filter", default="atr_percentile", choices=["off", "atr_percentile"])
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--atr-regime-window", type=int, default=200)
    p.add_argument("--atr-regime-percentile", type=float, default=0.50)

    p.add_argument("--position-fraction", type=float, default=0.50)

    # optional: override core defaults if you want later
    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--sl-pct", type=float, default=None)
    p.add_argument("--max-hold-bars", type=int, default=None)
    p.add_argument("--cooldown-bars", type=int, default=None)

    p.add_argument("--dump-json", action="store_true")
    p.add_argument("--dump-snapshot-path", default=os.path.join(REPORTS_DIR, "edge3_snapshot.json"))
    return p.parse_args()

def _to_dec_list(xs: List[Any]) -> List[Decimal]:
    return [Decimal(str(x)) for x in xs]

def main() -> int:
    args = parse_args()
    snapshot_path = args.dump_snapshot_path

    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)
        os.makedirs(DATA_CACHE_DIR, exist_ok=True)

        start_ms = fc.iso_to_ms(args.start_iso)
        end_ms = fc.iso_to_ms(args.end_iso)
        cache_path = os.path.join(DATA_CACHE_DIR, f"{args.market}_{args.interval}_candles.json")

        candles = fc.fetch_bitvavo_candles(
            market=args.market,
            interval=args.interval,
            start_ms=start_ms,
            end_ms=end_ms,
            cache_path=cache_path,
            limit=1000,
            sleep_s=0.0,
            verbose=False,  # keep logs clean
        )
        if not candles or len(candles) < 300:
            raise RuntimeError(f"Not enough candles fetched: n={len(candles) if candles else 0}")

        ts = [int(r[0]) for r in candles]
        opens = _to_dec_list([r[1] for r in candles])
        highs = _to_dec_list([r[2] for r in candles])
        lows  = _to_dec_list([r[3] for r in candles])
        closes= _to_dec_list([r[4] for r in candles])

        # Core signatures (from your print):
        # Fees(taker, maker)
        fees = core.Fees(Decimal(str(args.taker_fee)), Decimal(str(args.maker_fee)))

        # ExecModel(slippage_bps)
        execm = core.ExecModel(Decimal(str(args.slippage_bps)))

        # StrategyParams has ALL the knobs we care about
        sp_kwargs: Dict[str, Any] = {
            "entry_mode": str(args.entry_mode),
            "reclaim_limit_offset_bps": Decimal(str(args.reclaim_limit_offset_bps)),
            "fill_probability": Decimal(str(args.fill_prob)),
            "stop_extra_slippage_bps": Decimal(str(args.stop_extra_slip_bps)),
            "vol_filter": str(args.vol_filter),
            "atr_period": int(args.atr_period),
            "atr_regime_window": int(args.atr_regime_window),
            "atr_regime_percentile": Decimal(str(args.atr_regime_percentile)),
            "position_fraction": Decimal(str(args.position_fraction)),
        }

        # Optional overrides (leave defaults if not provided)
        if args.tp_pct is not None:
            sp_kwargs["tp_pct"] = Decimal(str(args.tp_pct))
        if args.sl_pct is not None:
            sp_kwargs["sl_pct"] = Decimal(str(args.sl_pct))
        if args.max_hold_bars is not None:
            sp_kwargs["max_hold_bars"] = int(args.max_hold_bars)
        if args.cooldown_bars is not None:
            sp_kwargs["cooldown_bars"] = int(args.cooldown_bars)

        p = core.StrategyParams(**sp_kwargs)

        # backtest_edge3_core(..., initial_equity, regime_mult_by_bar=None)
        res = core.backtest_edge3_core(
            ts=ts, opens=opens, highs=highs, lows=lows, closes=closes,
            fees=fees, execm=execm, p=p,
            initial_equity=Decimal(str(args.initial_equity)),
        )

        closed_trades = res.get("closed_trades")
        winrate = res.get("winrate")
        pf = res.get("profit_factor")
        ending_equity = res.get("ending_equity")
        maxdd = None
        try:
            maxdd = res.get("analytics", {}).get("max_drawdown_frac")
        except Exception:
            pass

        snapshot = {
            "ts_utc": utc_now_iso(),
            "market": args.market,
            "interval": args.interval,
            "status": "OK",
            "closed_trades": closed_trades,
            "winrate": float(winrate) if winrate is not None else None,
            "profit_factor": float(pf) if pf is not None else None,
            "ending_equity": float(ending_equity) if ending_equity is not None else None,
            "max_dd": float(maxdd) if maxdd is not None else None,
            "position_fraction": float(args.position_fraction),
            "initial_equity": float(args.initial_equity),
            "settings": {
                "taker_fee": args.taker_fee,
                "maker_fee": args.maker_fee,
                "slippage_bps": args.slippage_bps,
                "entry_mode": args.entry_mode,
                "reclaim_limit_offset_bps": args.reclaim_limit_offset_bps,
                "fill_prob": args.fill_prob,
                "stop_extra_slip_bps": args.stop_extra_slip_bps,
                "vol_filter": args.vol_filter,
                "atr_period": args.atr_period,
                "atr_regime_window": args.atr_regime_window,
                "atr_regime_percentile": args.atr_regime_percentile,
            },
        }

        atomic_write_json(snapshot_path, snapshot)

        if args.dump_json:
            print(json.dumps(res, indent=2))

        return 0

    except Exception as e:
        fatal = {
            "ts_utc": utc_now_iso(),
            "market": args.market,
            "interval": args.interval,
            "status": "EDGE3_FATAL",
            "error": repr(e),
        }
        try:
            atomic_write_json(snapshot_path, fatal)
        except Exception:
            pass
        print(f"EDGE3_FATAL: {repr(e)}")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
