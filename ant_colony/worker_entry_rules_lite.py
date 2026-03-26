import json
import os
from datetime import datetime, timezone

ANT_OUT = r"C:\Trading\ANT_OUT"

selection_file = os.path.join(ANT_OUT, "worker_strategy_selection.json")
portfolio_file = os.path.join(ANT_OUT, "worker_portfolio_state.json")

out_json = os.path.join(ANT_OUT, "worker_entry_rules.json")
out_tsv  = os.path.join(ANT_OUT, "worker_entry_rules.tsv")


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


selection_doc = load_json(selection_file)
portfolio_doc = load_json(portfolio_file)

selection_markets = selection_doc.get("markets", {})
if not isinstance(selection_markets, dict):
    selection_markets = {}

portfolio_rows = portfolio_doc.get("markets", [])
if not isinstance(portfolio_rows, list):
    portfolio_rows = []

portfolio_by_market = {}
for row in portfolio_rows:
    if isinstance(row, dict):
        market = row.get("market")
        if market:
            portfolio_by_market[market] = row

all_markets = sorted(set(selection_markets.keys()) | set(portfolio_by_market.keys()))
result_rows = []

for market in all_markets:
    sel = selection_markets.get(market, {}) or {}
    port = portfolio_by_market.get(market, {}) or {}

    selected_strategy = str(sel.get("selected_strategy", "NONE") or "NONE").upper()
    selected_bias = str(sel.get("selected_bias", "NEUTRAL") or "NEUTRAL").upper()
    selection_reason = str(sel.get("selection_reason", "") or "")
    current_position = str(port.get("position", "FLAT") or "FLAT").upper()

    entry_allowed = False
    entry_action = "NO_ENTRY"
    entry_reason = "DEFAULT_BLOCK"

    if current_position in ("LONG", "SHORT"):
        entry_allowed = False
        entry_action = "NO_ENTRY"
        entry_reason = "POSITION_ALREADY_OPEN"
    elif selected_strategy == "NONE":
        entry_allowed = False
        entry_action = "NO_ENTRY"
        entry_reason = "NO_ACTIVE_STRATEGY"
    elif selected_bias not in ("LONG", "SHORT"):
        entry_allowed = False
        entry_action = "NO_ENTRY"
        entry_reason = "INVALID_ENTRY_BIAS"
    elif selected_bias == "LONG":
        entry_allowed = True
        entry_action = "ENTRY_LONG_READY"
        entry_reason = selection_reason or "LONG_SIGNAL_READY"
    elif selected_bias == "SHORT":
        entry_allowed = True
        entry_action = "ENTRY_SHORT_READY"
        entry_reason = selection_reason or "SHORT_SIGNAL_READY"

    result_rows.append({
        "market": market,
        "current_position": current_position,
        "entry_allowed": entry_allowed,
        "entry_strategy": selected_strategy,
        "entry_bias": selected_bias,
        "entry_action": entry_action,
        "entry_reason": entry_reason,
        "ts_utc": now_utc_iso()
    })

result = {
    "version": "worker_entry_rules_lite_v1",
    "markets": result_rows
}

with open(out_json, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

with open(out_tsv, "w", encoding="utf-8") as f:
    f.write("market\tcurrent_position\tentry_allowed\tentry_strategy\tentry_bias\tentry_action\tentry_reason\n")
    for row in result_rows:
        f.write(
            f'{row["market"]}\t{row["current_position"]}\t{row["entry_allowed"]}\t{row["entry_strategy"]}\t{row["entry_bias"]}\t{row["entry_action"]}\t{row["entry_reason"]}\n'
        )

print(json.dumps({
    "ok": True,
    "output_json": out_json,
    "output_tsv": out_tsv,
    "markets": len(result_rows),
    "version": result["version"]
}, indent=2))