from __future__ import annotations

import argparse
import json
from dataclasses import dataclass

import pandas as pd


@dataclass
class Edge4Config:
    donch_lookback: int = 20
    atr_len: int = 14
    atr_min_frac: float = 0.008
    ema_len: int = 200
    breakout_buffer_frac: float = 0.001
    allow_long: bool = True
    allow_short: bool = True


def make_demo_df(rows: int = 260) -> pd.DataFrame:
    data = []
    price = 100.0

    for i in range(rows):
        if i < 80:
            drift = 0.05
        elif i < 160:
            drift = -0.03
        else:
            drift = 0.12

        wave = ((i % 7) - 3) * 0.08
        close = price + drift + wave
        high = close + 0.6
        low = close - 0.6
        open_ = price
        volume = 1000 + (i % 10) * 25

        data.append(
            {
                "timestamp": f"bar_{i}",
                "open": round(open_, 6),
                "high": round(high, 6),
                "low": round(low, 6),
                "close": round(close, 6),
                "volume": volume,
            }
        )
        price = close

    return pd.DataFrame(data)


def add_indicators(df: pd.DataFrame, cfg: Edge4Config) -> pd.DataFrame:
    out = df.copy()

    out["prev_close"] = out["close"].shift(1)

    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - out["prev_close"]).abs()
    tr3 = (out["low"] - out["prev_close"]).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    out["atr"] = out["tr"].rolling(cfg.atr_len, min_periods=cfg.atr_len).mean()
    out["atr_frac"] = out["atr"] / out["close"]

    out["ema"] = out["close"].ewm(span=cfg.ema_len, adjust=False).mean()

    out["donch_high"] = out["high"].shift(1).rolling(cfg.donch_lookback, min_periods=cfg.donch_lookback).max()
    out["donch_low"] = out["low"].shift(1).rolling(cfg.donch_lookback, min_periods=cfg.donch_lookback).min()

    out["long_break_level"] = out["donch_high"] * (1.0 + cfg.breakout_buffer_frac)
    out["short_break_level"] = out["donch_low"] * (1.0 - cfg.breakout_buffer_frac)

    return out


def build_signals(df: pd.DataFrame, cfg: Edge4Config) -> pd.DataFrame:
    out = add_indicators(df, cfg)

    trend_up = out["close"] > out["ema"]
    trend_down = out["close"] < out["ema"]
    vol_ok = out["atr_frac"] >= cfg.atr_min_frac

    long_signal = vol_ok & trend_up & (out["high"] > out["long_break_level"])
    short_signal = vol_ok & trend_down & (out["low"] < out["short_break_level"])

    if not cfg.allow_long:
        long_signal = False
    if not cfg.allow_short:
        short_signal = False

    out["edge4_long_signal"] = long_signal.fillna(False)
    out["edge4_short_signal"] = short_signal.fillna(False)

    return out


def recent_signals(df: pd.DataFrame, n: int = 10):
    if "edge4_long_signal" not in df.columns:
        return []

    sig = df[(df["edge4_long_signal"]) | (df["edge4_short_signal"])]

    out = []
    for i, row in sig.tail(n).iterrows():
        out.append(
            {
                "index": int(i),
                "close": float(row["close"]),
                "long": bool(row["edge4_long_signal"]),
                "short": bool(row["edge4_short_signal"]),
            }
        )

    return out


def summarize_signals(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "long_signals": int(df["edge4_long_signal"].sum()) if "edge4_long_signal" in df.columns else 0,
        "short_signals": int(df["edge4_short_signal"].sum()) if "edge4_short_signal" in df.columns else 0,
        "first_index": None if len(df) == 0 else str(df.index[0]),
        "last_index": None if len(df) == 0 else str(df.index[-1]),
        "recent_signals": recent_signals(df),
    }


def export_signal_csv(df: pd.DataFrame, out_csv: str) -> None:
    export_cols = []

    if "timestamp" in df.columns:
        export_cols.append("timestamp")

    export_cols.extend(
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ema",
            "atr",
            "atr_frac",
            "donch_high",
            "donch_low",
            "long_break_level",
            "short_break_level",
            "edge4_long_signal",
            "edge4_short_signal",
        ]
    )

    keep = [c for c in export_cols if c in df.columns]
    out = df[keep].copy()
    out.to_csv(out_csv, index=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="EDGE4 breakout research engine")
    parser.add_argument("--csv", default="", help="Path to OHLCV csv")
    parser.add_argument("--demo", type=int, default=1, help="Use built-in demo data (1=yes, 0=no)")
    parser.add_argument("--out-csv", default="", help="Optional export path for signal csv")
    parser.add_argument("--donch-lookback", type=int, default=20)
    parser.add_argument("--atr-len", type=int, default=14)
    parser.add_argument("--atr-min-frac", type=float, default=0.008)
    parser.add_argument("--ema-len", type=int, default=200)
    parser.add_argument("--breakout-buffer-frac", type=float, default=0.001)
    args = parser.parse_args()

    if args.demo == 1:
        df = make_demo_df()
    else:
        if not args.csv:
            raise SystemExit("Provide --csv when --demo 0 is used.")
        df = pd.read_csv(args.csv)

    required = {"open", "high", "low", "close"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"CSV missing required columns: {missing}")

    cfg = Edge4Config(
        donch_lookback=args.donch_lookback,
        atr_len=args.atr_len,
        atr_min_frac=args.atr_min_frac,
        ema_len=args.ema_len,
        breakout_buffer_frac=args.breakout_buffer_frac,
    )

    out = build_signals(df, cfg)

    if args.out_csv:
        export_signal_csv(out, args.out_csv)

    print(json.dumps(summarize_signals(out), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())