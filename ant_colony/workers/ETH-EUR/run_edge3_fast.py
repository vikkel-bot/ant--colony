from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import edge3_fetch_cache as fc
import edge3_core_v1 as core

ROOT = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(ROOT, "reports")
DATA_CACHE_DIR = os.path.join(ROOT, "data_cache")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ANT_OUT_DIR = r"C:\Trading\ANT_OUT"
PROBE_STATUS_PATH = os.path.join(ANT_OUT_DIR, "eth_worker_adapter_probe_status.json")


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--market", default=os.getenv("CB20_MARKET", "ETH-EUR"))
    p.add_argument("--interval", default=os.getenv("CB20_INTERVAL", "4h"))
    p.add_argument("--start-iso", required=True)
    p.add_argument("--end-iso", required=True)

    p.add_argument("--initial-equity", type=float, default=1000.0)
    p.add_argument("--taker-fee", type=float, default=0.0025)
    p.add_argument("--maker-fee", type=float, default=0.0015)
    p.add_argument("--slippage-bps", type=float, default=3.0)

    p.add_argument("--entry-mode", default="limit_maker", choices=["close_taker", "limit_maker", "market", "close"])
    p.add_argument("--reclaim-limit-offset-bps", type=float, default=-10.0)
    p.add_argument("--fill-prob", type=float, default=0.70)
    p.add_argument("--stop-extra-slip-bps", type=float, default=1.0)

    p.add_argument("--vol-filter", default="atr_percentile", choices=["off", "atr_percentile"])
    p.add_argument("--atr-period", type=int, default=14)
    p.add_argument("--atr-regime-window", type=int, default=200)
    p.add_argument("--atr-regime-percentile", type=float, default=0.50)

    p.add_argument("--position-fraction", type=float, default=0.50)

    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--sl-pct", type=float, default=None)
    p.add_argument("--max-hold-bars", type=int, default=None)
    p.add_argument("--cooldown-bars", type=int, default=None)

    p.add_argument("--dump-json", action="store_true")
    p.add_argument("--dump-snapshot-path", default=os.path.join(REPORTS_DIR, "edge3_snapshot.json"))

    # AC-WK-05: optional read-only adapter probe
    p.add_argument("--adapter-probe", action="store_true")
    p.add_argument("--adapter-probe-limit", type=int, default=50)
    p.add_argument("--adapter-probe-out", default=os.path.join(ANT_OUT_DIR, "eth_worker_adapter_probe_hook.json"))

    return p.parse_args()


def _to_dec_list(xs: List[Any]) -> List[Decimal]:
    return [Decimal(str(x)) for x in xs]


def candle_row_to_obj(row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, list) or len(row) < 6:
        return None

    ts_ms = int(row[0])
    ts_utc = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ts_ms": ts_ms,
        "ts_utc": ts_utc,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
    }


def diff_pct(a: Any, b: Any) -> Optional[float]:
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return None
        return ((a - b) / b) * 100.0
    except Exception:
        return None


def write_probe_status(path: str, probe: Dict[str, Any], probe_enabled: bool) -> None:
    diff = probe.get("diff") or {}
    meta = probe.get("meta") or {}
    adapter_meta = meta.get("adapter_meta") or {}

    parity_ok = (
        bool(diff.get("same_ts")) and
        diff.get("open_diff_pct") == 0.0 and
        diff.get("high_diff_pct") == 0.0 and
        diff.get("low_diff_pct") == 0.0 and
        diff.get("close_diff_pct") == 0.0 and
        diff.get("volume_diff_pct") == 0.0
    )

    payload = {
        "ts_utc": utc_now_iso(),
        "last_probe_ts_utc": probe.get("ts_utc"),
        "probe_enabled": bool(probe_enabled),
        "adapter_ok": bool(probe.get("adapter_ok")),
        "parity_ok": bool(parity_ok),
        "latency_ms": adapter_meta.get("latency_ms"),
        "adapter_source": meta.get("adapter_source"),
        "market": probe.get("market"),
        "interval": probe.get("interval"),
        "error": probe.get("error"),
    }

    atomic_write_json(path, payload)


def run_adapter_probe(
    *,
    market: str,
    interval: str,
    limit: int,
    legacy_candles: List[Any],
    cache_path: str,
    out_path: str,
) -> None:
    """
    Non-invasive read-only verification hook.
    Must never break EDGE3 runtime.
    """
    probe: Dict[str, Any] = {
        "ts_utc": utc_now_iso(),
        "market": market,
        "interval": interval,
        "hook": "AC-WK-05",
        "mode": "read_only_probe",
        "legacy_ok": len(legacy_candles) >= 2,
        "adapter_ok": False,
        "cache_path_used": cache_path,
        "compare_mode": "last_closed_candle",
        "legacy_last_live": candle_row_to_obj(legacy_candles[-1]) if len(legacy_candles) >= 1 else None,
        "legacy_last_closed": candle_row_to_obj(legacy_candles[-2]) if len(legacy_candles) >= 2 else None,
        "adapter_last_live": None,
        "adapter_last_closed": None,
        "diff": None,
        "meta": {
            "probe_limit": limit,
        },
        "error": None,
    }

    try:
        from ant_colony.worker_io import get_worker_market_data

        adapter_result = get_worker_market_data(
            market=market,
            interval=interval,
            limit=limit,
        )

        adapter_rows = adapter_result.get("rows") or []

        probe["adapter_ok"] = bool(adapter_result.get("ok")) and len(adapter_rows) >= 2
        probe["adapter_last_live"] = adapter_rows[-1] if len(adapter_rows) >= 1 else None
        probe["adapter_last_closed"] = adapter_rows[-2] if len(adapter_rows) >= 2 else None
        probe["meta"]["adapter_count"] = adapter_result.get("count")
        probe["meta"]["adapter_meta"] = adapter_result.get("meta")
        probe["meta"]["adapter_source"] = adapter_result.get("source")

        legacy_last_closed = probe.get("legacy_last_closed")

        if legacy_last_closed and adapter_rows:
            legacy_ts = legacy_last_closed.get("ts_utc")
            adapter_match = next((r for r in adapter_rows if r.get("ts_utc") == legacy_ts), None)

            if adapter_match:
                probe["adapter_last_closed"] = adapter_match
                probe["diff"] = {
                    "same_ts": True,
                    "open_diff_pct": diff_pct(adapter_match.get("open"), legacy_last_closed.get("open")),
                    "high_diff_pct": diff_pct(adapter_match.get("high"), legacy_last_closed.get("high")),
                    "low_diff_pct": diff_pct(adapter_match.get("low"), legacy_last_closed.get("low")),
                    "close_diff_pct": diff_pct(adapter_match.get("close"), legacy_last_closed.get("close")),
                    "volume_diff_pct": diff_pct(adapter_match.get("volume"), legacy_last_closed.get("volume")),
                }
            else:
                probe["diff"] = {
                    "same_ts": False,
                    "open_diff_pct": None,
                    "high_diff_pct": None,
                    "low_diff_pct": None,
                    "close_diff_pct": None,
                    "volume_diff_pct": None,
                }

    except Exception as e:
        probe["error"] = repr(e)

    try:
        atomic_write_json(out_path, probe)
    except Exception:
        pass

    try:
        write_probe_status(PROBE_STATUS_PATH, probe, True)
    except Exception:
        pass


def main() -> int:
    args = parse_args()
    snapshot_path = args.dump_snapshot_path

    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)
        os.makedirs(DATA_CACHE_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(args.adapter_probe_out), exist_ok=True)

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
            verbose=False,
        )
        if not candles or len(candles) < 300:
            raise RuntimeError(f"Not enough candles fetched: n={len(candles) if candles else 0}")

        # AC-WK-05 probe hook: optional and fully non-invasive
        probe_enabled = args.adapter_probe or os.getenv("ANT_ENABLE_ADAPTER_PROBE") == "1"

        if probe_enabled:
            try:
                run_adapter_probe(
                    market=args.market,
                    interval=args.interval,
                    limit=args.adapter_probe_limit,
                    legacy_candles=candles,
                    cache_path=cache_path,
                    out_path=args.adapter_probe_out,
                )
            except BaseException as e:
                try:
                    atomic_write_json(PROBE_STATUS_PATH, {
                        "ts_utc": utc_now_iso(),
                        "last_probe_ts_utc": None,
                        "probe_enabled": True,
                        "adapter_ok": False,
                        "parity_ok": False,
                        "latency_ms": None,
                        "adapter_source": "bitvavo_adapter",
                        "market": args.market,
                        "interval": args.interval,
                        "error": f"probe_main_guard:{repr(e)}",
                    })
                except Exception:
                    pass

        ts = [int(r[0]) for r in candles]
        opens = _to_dec_list([r[1] for r in candles])
        highs = _to_dec_list([r[2] for r in candles])
        lows  = _to_dec_list([r[3] for r in candles])
        closes= _to_dec_list([r[4] for r in candles])

        fees = core.Fees(Decimal(str(args.taker_fee)), Decimal(str(args.maker_fee)))
        execm = core.ExecModel(Decimal(str(args.slippage_bps)))

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

        if args.tp_pct is not None:
            sp_kwargs["tp_pct"] = Decimal(str(args.tp_pct))
        if args.sl_pct is not None:
            sp_kwargs["sl_pct"] = Decimal(str(args.sl_pct))
        if args.max_hold_bars is not None:
            sp_kwargs["max_hold_bars"] = int(args.max_hold_bars)
        if args.cooldown_bars is not None:
            sp_kwargs["cooldown_bars"] = int(args.cooldown_bars)

        p = core.StrategyParams(**sp_kwargs)

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
                "adapter_probe": bool(args.adapter_probe),
                "adapter_probe_limit": int(args.adapter_probe_limit),
                "adapter_probe_out": args.adapter_probe_out,
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





