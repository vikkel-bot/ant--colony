# edge3_fetch_cache.py
# Bitvavo PUBLIC candles fetcher with local JSON cache (resume + progress) v1.1
#
# Cache format:
# {
#   "meta": {"market": "...", "interval": "...", "start_ms": ..., "end_ms": ...},
#   "candles": [ [ts, open, high, low, close, volume], ... ]
# }
#
# Features:
# - Resume from existing cache if partial data already saved
# - Writes checkpoint every batch (atomic replace)
# - Progress print: batches, candles, cursor timestamp
# - Adjustable sleep (can be 0.0)

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


def http_get_json(url: str, timeout_s: int = 30) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "EDGE3-cache/1.1"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def interval_to_ms(interval: str) -> int:
    m = {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "30m": 30 * 60_000,
        "1h": 60 * 60_000,
        "2h": 2 * 60 * 60_000,
        "4h": 4 * 60 * 60_000,
        "6h": 6 * 60 * 60_000,
        "8h": 8 * 60 * 60_000,
        "12h": 12 * 60 * 60_000,
        "1d": 24 * 60 * 60_000,
    }
    if interval not in m:
        raise ValueError(f"Unsupported interval: {interval}")
    return m[interval]


def _atomic_write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def load_cache(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        # Backward compat with v1.0: pure list candles
        return {"meta": {}, "candles": data}
    if not isinstance(data, dict) or "candles" not in data:
        raise RuntimeError("Cache file invalid format.")
    if not isinstance(data["candles"], list):
        raise RuntimeError("Cache candles invalid (not a list).")
    return data


def save_cache(path: str, market: str, interval: str, start_ms: int, end_ms: int, candles: List[List[Any]]) -> None:
    payload = {
        "meta": {
            "market": market,
            "interval": interval,
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
        },
        "candles": candles,
    }
    _atomic_write_json(path, payload)


def _existing_progress(cache_path: str) -> Tuple[List[List[Any]], Optional[int]]:
    if not os.path.exists(cache_path):
        return [], None
    data = load_cache(cache_path)
    candles = data.get("candles", [])
    if not candles:
        return [], None
    # candles are sorted by ts, but ensure
    candles = sorted(candles, key=lambda r: int(r[0]))
    last_ts = int(candles[-1][0])
    return candles, last_ts


def fetch_bitvavo_candles(
    *,
    market: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    cache_path: Optional[str] = None,
    limit: int = 1000,
    sleep_s: float = 0.0,
    verbose: bool = True,
) -> List[List[Any]]:
    """
    Returns rows: [ts, open, high, low, close, volume]
    If cache_path is provided and exists, resume from last_ts+step.
    """
    base = f"https://api.bitvavo.com/v2/{urllib.parse.quote(market)}/candles"
    step = interval_to_ms(interval)
    max_window = step * limit

    existing: List[List[Any]] = []
    cur = start_ms

    if cache_path:
        existing, last_ts = _existing_progress(cache_path)
        if last_ts is not None and last_ts >= start_ms:
            cur = max(cur, last_ts + step)
            if verbose:
                print(f"Resume cache: have {len(existing)} candles, last_ts={last_ts}, resuming from {cur}")

    # Use dict for dedupe as we go
    uniq: Dict[int, List[Any]] = {int(r[0]): r for r in existing}
    batch = 0

    if verbose:
        print(f"Fetching Bitvavo candles: {market} {interval} start={start_ms} end={end_ms} sleep_s={sleep_s}")

    while cur < end_ms:
        batch += 1
        chunk_end = min(end_ms, cur + max_window)
        params = {
            "interval": interval,
            "start": str(cur),
            "end": str(chunk_end),
            "limit": str(limit),
        }
        url = base + "?" + urllib.parse.urlencode(params)

        data = http_get_json(url)
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected Bitvavo response: {data}")

        got = 0
        for row in data:
            if isinstance(row, list) and len(row) >= 6:
                ts = int(row[0])
                if start_ms <= ts < end_ms:
                    uniq[ts] = [ts, row[1], row[2], row[3], row[4], row[5]]
                    got += 1

        # Advance cursor
        if len(data) > 0:
            last = int(data[-1][0])
            nxt = last + step
            cur = nxt if nxt > cur else cur + step
        else:
            cur = cur + max_window

        if verbose:
            print(f"Batch {batch}: got={got} total={len(uniq)} next_cur={cur}")

        # Checkpoint write every batch (so Ctrl+C never loses everything)
        if cache_path:
            out = [uniq[k] for k in sorted(uniq.keys())]
            save_cache(cache_path, market, interval, start_ms, end_ms, out)

        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))

    out = [uniq[k] for k in sorted(uniq.keys())]
    if verbose:
        print(f"Done. Candles total: n={len(out)}")
    return ou

# Backward-compatible alias used by older runners
def fetch_candles_cached(market: str, interval: str, start_iso: str, end_iso: str):
    """Compatibility wrapper. Returns candles list."""
    return fetch_bitvavo_candles(market, interval, start_iso, end_iso)
