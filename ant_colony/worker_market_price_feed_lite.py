import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")
out_json = os.path.join(ANT_OUT, "worker_market_price_feed.json")
out_tsv  = os.path.join(ANT_OUT, "worker_market_price_feed.tsv")


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


portfolio = load_json(portfolio_file)
portfolio_rows = portfolio.get("markets", [])
if not isinstance(portfolio_rows, list):
    portfolio_rows = []

result_rows = []

for row in portfolio_rows:
    if not isinstance(row, dict):
        continue

    market = row.get("market", "")
    position = str(row.get("position", "FLAT") or "FLAT").upper()

    market_price = 0.0
    price_source = "NO_POSITION"

    if position == "LONG":
        market_price = 101.0
        price_source = "LITE_LONG_STUB"
    elif position == "SHORT":
        market_price = 99.0
        price_source = "LITE_SHORT_STUB"
    else:
        market_price = 0.0
        price_source = "FLAT_STUB"

    result_rows.append({
        "market": market,
        "position": position,
        "market_price": market_price,
        "price_source": price_source,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_market_price_feed_lite_v1",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tposition\tmarket_price\tprice_source\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["position"]}\t{row["market_price"]}\t{row["price_source"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))