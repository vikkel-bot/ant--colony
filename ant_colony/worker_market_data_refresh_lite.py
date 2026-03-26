import os
import re
import json
import glob
from datetime import datetime, UTC

ANT_OUT = r"C:\Trading\ANT_OUT"
OUT_PATH = os.path.join(ANT_OUT, "worker_market_data.json")

MARKETS = [
    "ADA-EUR",
    "BNB-EUR",
    "BTC-EUR",
    "ETH-EUR",
    "SOL-EUR",
    "XRP-EUR",
]

FILE_PATTERNS = [
    "*_worker_adapter_probe_hook.json",
    "*_worker_adapter_probe_status.json",
    "*_worker_dual_path_probe.json",
    "*_worker_market_data_smoke.json",
    "*_market_data*.json",
    "*_summary.json",
]


def load_json(path, default=None):
    if not os.path.exists(path):
        return {} if default is None else default
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {} if default is None else default


def save_json_atomic(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def pick(dct, *keys, default=None):
    if not isinstance(dct, dict):
        return default
    for key in keys:
        if key in dct and dct.get(key) is not None:
            return dct.get(key)
    return default


def deep_get(dct, path, default=None):
    cur = dct
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def parse_market_from_filename(path):
    name = os.path.basename(path).upper()

    for market in MARKETS:
        if market in name:
            return market

    m = re.match(r"^([A-Z0-9]+)[_\-]", name)
    if m:
        sym = m.group(1)
        candidate = f"{sym}-EUR"
        if candidate in MARKETS:
            return candidate

    return None


def candidate_files():
    seen = set()
    out = []
    for pattern in FILE_PATTERNS:
        for path in glob.glob(os.path.join(ANT_OUT, pattern)):
            if path not in seen and os.path.isfile(path):
                seen.add(path)
                out.append(path)
    return sorted(out)


def row_to_price(row):
    if not isinstance(row, dict):
        return None
    close = to_float(pick(row, "close", "last_price", "market_price", default=0.0), 0.0)
    if close <= 0.0:
        return None
    return {
        "last_price": close,
        "ts_utc": pick(row, "ts_utc", "ts", default=None)
    }


def extract_price_info(data):
    if not isinstance(data, dict):
        return None

    # 1. direct scalar fields
    direct_candidates = [
        ("last_price", pick(data, "last_price")),
        ("close", pick(data, "close")),
        ("price", pick(data, "price")),
        ("mark_price", pick(data, "mark_price")),
        ("market_price", pick(data, "market_price")),
    ]
    for field_name, raw_value in direct_candidates:
        price = to_float(raw_value, 0.0)
        if price > 0.0:
            return {
                "last_price": price,
                "ts_utc": pick(data, "ts_utc", "ts", "timestamp_utc", "timestamp", default=None),
                "field": field_name
            }

    # 2. nested probe / adapter structures
    nested_candidates = [
        ("adapter_last_closed.close", deep_get(data, ["adapter_last_closed", "close"]), deep_get(data, ["adapter_last_closed", "ts_utc"])),
        ("adapter_last_live.close", deep_get(data, ["adapter_last_live", "close"]), deep_get(data, ["adapter_last_live", "ts_utc"])),
        ("legacy_last_closed.close", deep_get(data, ["legacy_last_closed", "close"]), deep_get(data, ["legacy_last_closed", "ts_utc"])),
        ("legacy_last_live.close", deep_get(data, ["legacy_last_live", "close"]), deep_get(data, ["legacy_last_live", "ts_utc"])),
        ("adapter_last.close", deep_get(data, ["adapter_last", "close"]), deep_get(data, ["adapter_last", "ts_utc"])),
        ("edge3_last.close", deep_get(data, ["edge3_last", "close"]), deep_get(data, ["edge3_last", "ts_utc"])),
        ("last_row.close", deep_get(data, ["last_row", "close"]), deep_get(data, ["last_row", "ts_utc"])),
        ("first_row.close", deep_get(data, ["first_row", "close"]), deep_get(data, ["first_row", "ts_utc"])),
    ]
    for field_name, raw_value, ts_value in nested_candidates:
        price = to_float(raw_value, 0.0)
        if price > 0.0:
            return {
                "last_price": price,
                "ts_utc": ts_value or pick(data, "ts_utc", "ts", default=None),
                "field": field_name
            }

    # 3. rows arrays
    rows = deep_get(data, ["rows"], default=None)
    if isinstance(rows, list) and rows:
        last = rows[-1]
        row_px = row_to_price(last)
        if row_px:
            return {
                "last_price": row_px["last_price"],
                "ts_utc": row_px["ts_utc"] or pick(data, "ts_utc", "ts", default=None),
                "field": "rows[-1].close"
            }

    data_rows = deep_get(data, ["data", "rows"], default=None)
    if isinstance(data_rows, list) and data_rows:
        last = data_rows[-1]
        row_px = row_to_price(last)
        if row_px:
            return {
                "last_price": row_px["last_price"],
                "ts_utc": row_px["ts_utc"] or pick(data, "ts_utc", "ts", default=None),
                "field": "data.rows[-1].close"
            }

    # 4. worker_market_price_feed style: markets is a list, not dict
    markets_list = deep_get(data, ["markets"], default=None)
    if isinstance(markets_list, list):
        # handled outside per-market extraction, so ignore here
        return None

    return None


def extract_from_market_price_feed(data):
    out = {}
    markets = deep_get(data, ["markets"], default=None)
    if not isinstance(markets, list):
        return out

    for row in markets:
        if not isinstance(row, dict):
            continue
        market = pick(row, "market", default=None)
        if market not in MARKETS:
            continue
        market_price = to_float(pick(row, "market_price", "last_price", default=0.0), 0.0)
        if market_price <= 0.0:
            continue
        out[market] = {
            "market": market,
            "last_price": round(market_price, 8),
            "ts_utc": pick(row, "ts_utc", "ts", default=None),
            "source_field": "markets[].market_price",
            "source_component": pick(data, "component", "version", default="worker_market_price_feed"),
        }
    return out


def main():
    now_utc = datetime.now(UTC).isoformat()

    per_market = {}

    files = candidate_files()

    for path in files:
        data = load_json(path, {})

        feed_rows = extract_from_market_price_feed(data)
        for market, row in feed_rows.items():
            row["source_file"] = path
            prev = per_market.get(market)
            new_ts = str(row.get("ts_utc") or "")
            prev_ts = str((prev or {}).get("ts_utc") or "")
            replace = (prev is None) or (new_ts >= prev_ts)
            if replace:
                row["state"] = "OK"
                per_market[market] = row

        market = pick(data, "market", default=None)
        if not market:
            market = parse_market_from_filename(path)

        if market not in MARKETS:
            continue

        px = extract_price_info(data)
        if not px:
            continue

        row = {
            "market": market,
            "last_price": round(to_float(px.get("last_price", 0.0), 0.0), 8),
            "ts_utc": px.get("ts_utc"),
            "source_file": path,
            "source_field": px.get("field"),
            "source_component": pick(data, "component", "version", default="unknown"),
            "state": "OK"
        }

        prev = per_market.get(market)
        new_ts = str(row.get("ts_utc") or "")
        prev_ts = str((prev or {}).get("ts_utc") or "")

        replace = False
        if prev is None:
            replace = True
        elif new_ts and prev_ts:
            replace = new_ts >= prev_ts
        elif new_ts and not prev_ts:
            replace = True

        if replace:
            per_market[market] = row

    markets_out = {}
    zero_price_count = 0
    priced_count = 0
    missing_markets = []

    for market in MARKETS:
        row = per_market.get(market)
        if row is None:
            markets_out[market] = {
                "market": market,
                "last_price": 0.0,
                "ts_utc": None,
                "source_file": None,
                "source_field": None,
                "source_component": None,
                "state": "MISSING"
            }
            zero_price_count += 1
            missing_markets.append(market)
            continue

        price = to_float(row.get("last_price", 0.0), 0.0)
        if price > 0.0:
            priced_count += 1
            row["state"] = "OK"
        else:
            zero_price_count += 1
            row["state"] = "ZERO_PRICE"

        markets_out[market] = row

    out = {
        "component": "worker_market_data_refresh_lite",
        "ts_utc": now_utc,
        "market_count": len(MARKETS),
        "priced_count": priced_count,
        "zero_price_count": zero_price_count,
        "missing_markets": missing_markets,
        "markets": markets_out
    }

    save_json_atomic(OUT_PATH, out)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
