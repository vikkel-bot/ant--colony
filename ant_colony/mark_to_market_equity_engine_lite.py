import os
import json
import time
from datetime import datetime, UTC

ANT_OUT = r"C:\Trading\ANT_OUT"

POSITIONS_FILE = os.path.join(ANT_OUT, "paper_positions.json")
PORTFOLIO_FILE = os.path.join(ANT_OUT, "paper_portfolio_state.json")
MARKET_DATA_FILE = os.path.join(ANT_OUT, "worker_market_data.json")
SUMMARY_FILE = os.path.join(ANT_OUT, "paper_portfolio_summary.json")
METRICS_FILE = os.path.join(ANT_OUT, "paper_portfolio_metrics.json")
VALUATION_JSON_FILE = os.path.join(ANT_OUT, "paper_portfolio_valuation.json")

MAX_PRICE_AGE_SECONDS = 1200


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


def parse_ts(value):
    if not value:
        return None
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def age_seconds(now_utc, ts_value):
    dt = parse_ts(ts_value)
    if dt is None:
        return None
    try:
        return max(0, int((now_utc - dt).total_seconds()))
    except Exception:
        return None


def load_positions():
    data = load_json(POSITIONS_FILE, {})
    return data if isinstance(data, dict) else {}


def load_portfolio():
    data = load_json(PORTFOLIO_FILE, {
        "equity": 10000.0,
        "cash": 10000.0,
        "positions": {}
    })
    if not isinstance(data, dict):
        data = {
            "equity": 10000.0,
            "cash": 10000.0,
            "positions": {}
        }
    if not isinstance(data.get("positions"), dict):
        data["positions"] = {}
    return data


def load_market_prices():
    data = load_json(MARKET_DATA_FILE, {})
    out = {}
    root_ts = data.get("ts_utc") or data.get("ts")
    for market, info in (data.get("markets", {}) or {}).items():
        if isinstance(info, dict):
            px_ts = info.get("ts_utc") or info.get("ts") or root_ts
            out[market] = {
                "price": to_float(info.get("last_price", 0.0), 0.0),
                "price_ts": px_ts,
                "price_source": "worker_market_data.last_price"
            }
    return out


def current_position_state(pos_row):
    if not isinstance(pos_row, dict):
        return "FLAT"
    return str(pos_row.get("position", "FLAT") or "FLAT").upper()


def main():
    now_utc = datetime.now(UTC)
    ts = now_utc.isoformat()

    positions = load_positions()
    portfolio = load_portfolio()
    market_prices = load_market_prices()

    cash = round(to_float(portfolio.get("cash", 0.0), 0.0), 2)
    prev_equity = round(to_float(portfolio.get("equity", cash), cash), 2)

    portfolio_positions = portfolio.get("positions", {}) or {}

    positions_market_value = 0.0
    total_unrealized_pnl = 0.0
    open_positions = 0
    priced_positions = 0
    unpriced_positions = 0
    stale_positions = 0
    fresh_positions = 0

    hard_blocked = False
    hard_block_reason = None

    last_price_map = {}
    price_issues = []

    for position_key, pos in positions.items():

        if not isinstance(position_key, str):
            continue

        # split strategy-aware key → base market
        if "__" in position_key:
            market = position_key.split("__")[0]
        else:
            market = position_key
        if not isinstance(pos, dict):
            continue

        state = current_position_state(pos)
        if state != "LONG":
            continue

        open_positions += 1

        entry_price = to_float(pos.get("entry_price", 0.0), 0.0)
        size = to_float(pos.get("size", 0.0), 0.0)

        px_info = market_prices.get(market, {})
        mark_price = to_float(px_info.get("price", 0.0), 0.0)
        price_ts = px_info.get("price_ts")
        price_source = px_info.get("price_source", "missing")
        price_age_s = age_seconds(now_utc, price_ts)
        price_freshness_ok = (price_age_s is not None) and (price_age_s <= MAX_PRICE_AGE_SECONDS)

        row = portfolio_positions.get(position_key, {}) or {}
        row["mark_to_market_ts"] = ts
        row["price_ts"] = price_ts
        row["price_source"] = price_source
        row["price_age_seconds"] = price_age_s
        row["price_freshness_ok"] = bool(price_freshness_ok)

        if mark_price <= 0.0:
            unpriced_positions += 1
            price_issues.append({
                "market": market,
                "reason": "missing_mark_price"
            })
            row["mark_price"] = None
            row["market_value"] = None
            row["unrealized_pnl"] = None
            row["mtm_state"] = "UNPRICED"
            portfolio_positions[position_key] = row
            continue

        priced_positions += 1
        last_price_map[position_key] = mark_price

        market_value = round(size * mark_price, 2)
        unrealized_pnl = round(size * (mark_price - entry_price), 2)

        positions_market_value += market_value
        total_unrealized_pnl += unrealized_pnl

        row["mark_price"] = mark_price
        row["market_value"] = market_value
        row["unrealized_pnl"] = unrealized_pnl

        if price_freshness_ok:
            fresh_positions += 1
            row["mtm_state"] = "PRICED_FRESH"
        else:
            stale_positions += 1
            hard_blocked = True
            hard_block_reason = "STALE_PRICE_BLOCKED"
            price_issues.append({
                "market": market,
                "reason": "stale_mark_price",
                "price_age_seconds": price_age_s,
                "max_price_age_seconds": MAX_PRICE_AGE_SECONDS
            })
            row["mtm_state"] = "PRICED_STALE"

        portfolio_positions[position_key] = row

    positions_market_value = round(positions_market_value, 2)
    total_unrealized_pnl = round(total_unrealized_pnl, 2)

    if open_positions == 0:
        valuation_state = "EMPTY"
        equity = round(cash, 2)
        equity_calc_mode = "cash_only"
        all_prices_fresh = True
    elif unpriced_positions > 0:
        valuation_state = "DEGRADED"
        equity = prev_equity
        equity_calc_mode = "preserved_previous_due_to_missing_prices"
        all_prices_fresh = False
    elif hard_blocked:
        valuation_state = "BLOCKED_FRESHNESS"
        equity = prev_equity
        equity_calc_mode = "preserved_previous_due_to_stale_prices"
        all_prices_fresh = False
    else:
        valuation_state = "OK"
        equity = round(cash + positions_market_value, 2)
        equity_calc_mode = "cash_plus_priced_positions"
        all_prices_fresh = True

    portfolio["cash"] = cash
    portfolio["equity"] = equity
    portfolio["positions"] = portfolio_positions
    portfolio["positions_market_value"] = positions_market_value
    portfolio["unrealized_pnl"] = total_unrealized_pnl
    portfolio["open_positions"] = open_positions
    portfolio["priced_positions"] = priced_positions
    portfolio["unpriced_positions"] = unpriced_positions
    portfolio["fresh_positions"] = fresh_positions
    portfolio["stale_positions"] = stale_positions
    portfolio["mark_to_market_ts"] = ts
    portfolio["mark_to_market_state"] = valuation_state
    portfolio["valuation_state"] = valuation_state
    portfolio["all_prices_fresh"] = all_prices_fresh
    portfolio["max_price_age_seconds"] = MAX_PRICE_AGE_SECONDS
    portfolio["equity_calc_mode"] = equity_calc_mode
    portfolio["last_price_map"] = last_price_map
    portfolio["hard_block_reason"] = hard_block_reason

    metrics = {
        "component": "paper_portfolio_metrics",
        "ts": ts,
        "state": valuation_state,
        "valuation_state": valuation_state,
        "cash": cash,
        "equity": equity,
        "positions_market_value": positions_market_value,
        "unrealized_pnl": total_unrealized_pnl,
        "open_positions": open_positions,
        "priced_positions": priced_positions,
        "unpriced_positions": unpriced_positions,
        "fresh_positions": fresh_positions,
        "stale_positions": stale_positions,
        "all_prices_fresh": all_prices_fresh,
        "max_price_age_seconds": MAX_PRICE_AGE_SECONDS,
        "equity_calc_mode": equity_calc_mode,
        "hard_block_reason": hard_block_reason,
        "last_price_map": last_price_map,
        "price_issues": price_issues
    }

    summary = {
        "component": "mark_to_market_equity_engine_lite",
        "ts": ts,
        "ok": valuation_state in ("OK", "EMPTY", "DEGRADED", "BLOCKED_FRESHNESS"),
        "state": valuation_state,
        "valuation_state": valuation_state,
        "cash": cash,
        "equity": equity,
        "positions_market_value": positions_market_value,
        "unrealized_pnl": total_unrealized_pnl,
        "open_positions": open_positions,
        "priced_positions": priced_positions,
        "unpriced_positions": unpriced_positions,
        "fresh_positions": fresh_positions,
        "stale_positions": stale_positions,
        "all_prices_fresh": all_prices_fresh,
        "max_price_age_seconds": MAX_PRICE_AGE_SECONDS,
        "equity_calc_mode": equity_calc_mode,
        "hard_block_reason": hard_block_reason
    }

    save_json_atomic(PORTFOLIO_FILE, portfolio)
    save_json_atomic(METRICS_FILE, metrics)
    save_json_atomic(SUMMARY_FILE, summary)

    save_json_atomic(VALUATION_JSON_FILE, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()




