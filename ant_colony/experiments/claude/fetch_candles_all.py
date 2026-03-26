"""
AC_DEV — Candle Data Fetcher
=============================
Haalt 4h candles op voor alle markten via Bitvavo public API.
Slaat op in productie data_cache (zelfde locatie als BTC-EUR).
Geen API key nodig — public endpoint.

Gebruik:
    python fetch_candles_all.py
    python fetch_candles_all.py --market ETH-EUR
    python fetch_candles_all.py --start 2023-01-01
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, List, Optional

# ── config ─────────────────────────────────────────────────────────────────
DATA_CACHE = r"C:\Users\vikke\OneDrive\bitvavo-bot_clean\data_cache"
INTERVAL   = "4h"
START_ISO  = "2022-01-01"
END_ISO    = None          # None = nu

MARKETS = [
    "BTC-EUR",
    "ETH-EUR",
    "SOL-EUR",
    "XRP-EUR",
    "ADA-EUR",
    "BNB-EUR",
]

INTERVAL_MS = {
    "1h":  3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

# Bitvavo max 1000 candles per request
LIMIT = 1000

# ── helpers ────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def iso_to_ms(iso: str) -> int:
    s = iso.strip()
    if len(s) == 10:
        s += "T00:00:00Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def http_get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "AC_DEV-fetcher/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def atomic_write(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)

def load_existing(path: str) -> tuple[list, Optional[int]]:
    """Laad bestaande cache. Geeft (candles, laatste_ts) terug."""
    if not os.path.exists(path):
        return [], None
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        if isinstance(data, list):
            candles = data
        else:
            candles = data.get("candles", [])
        if candles:
            return candles, int(candles[-1][0])
        return [], None
    except Exception:
        return [], None

# ── fetcher ────────────────────────────────────────────────────────────────
def fetch_market(market: str, interval: str, start_ms: int, end_ms: int) -> List:
    """Haal alle candles op voor een markt via paginering."""
    interval_step = INTERVAL_MS.get(interval, 14_400_000)
    batch_window  = interval_step * LIMIT   # tijdvenster per request
    all_candles: List = []
    cursor = start_ms

    while cursor < end_ms:
        batch_end = min(cursor + batch_window, end_ms)
        url = (
            f"https://api.bitvavo.com/v2/{market}/candles"
            f"?interval={interval}&limit={LIMIT}&start={cursor}&end={batch_end}"
        )
        try:
            batch = http_get(url)
        except Exception as e:
            print(f"    FETCH ERROR: {e}")
            break

        if not batch or not isinstance(batch, list):
            # Geen data in dit venster, spring vooruit
            cursor = batch_end + interval_step
            continue

        batch.sort(key=lambda c: c[0])

        existing_ts = {c[0] for c in all_candles}
        new = [c for c in batch if c[0] not in existing_ts]
        all_candles.extend(new)

        # Cursor naar volgende venster
        cursor = batch_end + interval_step
        time.sleep(0.15)

        time.sleep(0.15)   # respecteer rate limit

    all_candles.sort(key=lambda c: c[0])
    return all_candles

# ── main ───────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",   default=None, help="Specifieke markt (default: alle)")
    parser.add_argument("--interval", default=INTERVAL)
    parser.add_argument("--start",    default=START_ISO)
    parser.add_argument("--end",      default=None)
    parser.add_argument("--force",    action="store_true", help="Herlaad ook al bestaande data")
    args = parser.parse_args()

    markets  = [args.market] if args.market else MARKETS
    start_ms = iso_to_ms(args.start)
    end_ms   = iso_to_ms(args.end) if args.end else now_ms()

    print("=" * 55)
    print(f"AC_DEV — Candle Fetcher  ({args.interval})")
    print(f"  Van:  {args.start}")
    print(f"  Tot:  {args.end or 'nu'}")
    print(f"  Cache: {DATA_CACHE}")
    print("=" * 55)
    print()

    os.makedirs(DATA_CACHE, exist_ok=True)
    results = {}

    for market in markets:
        fname    = f"{market}_{args.interval}_candles.json"
        path     = os.path.join(DATA_CACHE, fname)
        existing, last_ts = load_existing(path)

        if existing and not args.force:
            # Incrementeel: haal alleen nieuwe candles op
            fetch_from = last_ts + INTERVAL_MS.get(args.interval, 14_400_000)
            if fetch_from >= end_ms:
                print(f"  ✓ {market:<10} al up-to-date ({len(existing)} candles)")
                results[market] = {"status": "UP_TO_DATE", "candles": len(existing)}
                continue
            print(f"  ↻ {market:<10} update vanaf {datetime.fromtimestamp(fetch_from/1000, timezone.utc).date()} ...", end=" ", flush=True)
        else:
            fetch_from = start_ms
            print(f"  ↓ {market:<10} volledig ophalen ...", end=" ", flush=True)

        new_candles = fetch_market(market, args.interval, fetch_from, end_ms)

        if not new_candles:
            print("GEEN DATA")
            results[market] = {"status": "NO_DATA"}
            continue

        # Samenvoegen met bestaande
        all_candles = existing + new_candles
        # Dedupliceer en sorteer
        seen = {}
        for c in all_candles:
            seen[c[0]] = c
        all_candles = sorted(seen.values(), key=lambda c: c[0])

        # Sla op in productie data_cache formaat
        payload = {
            "meta": {
                "market":   market,
                "interval": args.interval,
                "start_ms": int(all_candles[0][0]),
                "end_ms":   int(all_candles[-1][0]),
            },
            "candles": all_candles,
        }
        atomic_write(path, payload)

        n_new = len(new_candles)
        n_total = len(all_candles)
        print(f"{n_new} nieuw  |  {n_total} totaal  →  {path.split(chr(92))[-1]}")
        results[market] = {"status": "OK", "new": n_new, "total": n_total}

    print()
    ok     = sum(1 for r in results.values() if r.get("status") in ("OK", "UP_TO_DATE"))
    failed = sum(1 for r in results.values() if r.get("status") == "NO_DATA")
    print(f"  Klaar: {ok} OK  |  {failed} geen data")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
