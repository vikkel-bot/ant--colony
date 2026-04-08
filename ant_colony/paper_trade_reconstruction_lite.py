import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EXECUTION_LOG_PATH = OUT_DIR / "paper_execution_log.jsonl"
POSITIONS_PATH = OUT_DIR / "paper_positions.json"
PORTFOLIO_STATE_PATH = OUT_DIR / "paper_portfolio_state.json"
PORTFOLIO_SUMMARY_PATH = OUT_DIR / "paper_portfolio_summary.json"

OUT_TRADES_PATH = OUT_DIR / "paper_trade_reconstruction.json"
OUT_TRADES_TSV_PATH = OUT_DIR / "paper_trade_reconstruction.tsv"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def load_jsonl(path: Path):
    rows = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    except Exception:
        pass
    return rows


def to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers, rows):
    lines = ["`t".join(headers)]
    for row in rows:
        lines.append("`t".join(str(row.get(h, "")) for h in headers))
    path.write_text("`n".join(lines), encoding="utf-8")


def infer_position_key(market: str, decision_id: str):
    """
    AC43: leid position_key en strategy af uit decision_id.
    Verwacht formaat: {position_key}_{cycle_id}_{action}
    Bijv: BTC-EUR__EDGE4_20260407T092720Z_ENTER_LONG -> (BTC-EUR__EDGE4, EDGE4)
    Fallback naar (market, "UNKNOWN") voor pre-AC40 of ontbrekende decision_id.
    """
    if not decision_id:
        return market, "UNKNOWN"
    for suffix in ("_ENTER_LONG", "_EXIT_LONG", "_NO_ACTION"):
        if decision_id.endswith(suffix):
            rest = decision_id[:-len(suffix)]  # verwijder actie-suffix
            idx = rest.rfind("_")              # verwijder cycle_id
            if idx > 0:
                pos_key = rest[:idx]
                if "__" in pos_key:
                    strategy = pos_key.split("__", 1)[1]
                    return pos_key, strategy
            break
    # Fallback: geen strategy info afleidbaar
    return market, "UNKNOWN"


def build_trade_id(position_key, execution_id, decision_id):
    if execution_id:
        return f"{position_key}|{execution_id}"
    if decision_id:
        return f"{position_key}|{decision_id}"
    return f"{position_key}|UNKNOWN"


def main():
    ts = utc_now_ts()

    execution_log = load_jsonl(EXECUTION_LOG_PATH)
    positions = load_json(POSITIONS_PATH, {})
    portfolio_state = load_json(PORTFOLIO_STATE_PATH, {})
    portfolio_summary = load_json(PORTFOLIO_SUMMARY_PATH, {})

    # AC43: keyed op position_key i.p.v. market zodat EDGE3 en EDGE4 apart staan
    open_trade_by_key = {}
    closed_trades = []
    ignored_events = 0

    for row in execution_log:
        market = str(row.get("market", "") or "")
        action = str(row.get("action", "") or "").upper()

        if not market:
            ignored_events += 1
            continue

        # SKIP-entries (AC41.1 guards) negeren
        if action == "SKIP":
            continue

        decision_id = str(row.get("decision_id", "") or "")
        position_key, strategy = infer_position_key(market, decision_id)

        if action == "ENTER_LONG":
            trade = {
                "trade_id": build_trade_id(
                    position_key,
                    decision_id.replace("_ENTER_LONG", ""),
                    decision_id
                ),
                "market": market,
                "position_key": position_key,
                "strategy": strategy,
                "state": "OPEN",
                "entry_ts": row.get("ts_utc"),
                "exit_ts": None,
                "entry_price": to_float(row.get("price", 0.0), 0.0),
                "exit_price": 0.0,
                "size": to_float(row.get("size", 0.0), 0.0),
                "entry_notional_eur": to_float(row.get("notional_eur", 0.0), 0.0),
                "exit_notional_eur": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "holding_state": "OPEN",
                "entry_decision_id": decision_id,
                "exit_decision_id": None,
                "entry_reason": row.get("reason"),
                "exit_reason": None,
            }
            open_trade_by_key[position_key] = trade

        elif action == "EXIT_LONG":
            current = open_trade_by_key.get(position_key)
            if not current:
                # AC43 fallback: probeer market-only key voor pre-AC40 logs
                current = open_trade_by_key.get(market)
            if not current:
                ignored_events += 1
                continue

            current["state"] = "CLOSED"
            current["holding_state"] = "CLOSED"
            current["exit_ts"] = row.get("ts_utc")
            current["exit_price"] = to_float(row.get("price", 0.0), 0.0)
            current["exit_notional_eur"] = to_float(row.get("notional_eur", 0.0), 0.0)
            current["realized_pnl"] = round(to_float(row.get("realized_pnl", 0.0), 0.0), 2)
            current["exit_decision_id"] = decision_id
            current["exit_reason"] = row.get("reason")
            closed_trades.append(current)
            open_trade_by_key.pop(position_key, None)
            open_trade_by_key.pop(market, None)

    open_trades = []
    last_price_map = (portfolio_state.get("last_price_map", {}) or {})
    portfolio_positions = (portfolio_state.get("positions", {}) or {})

    for position_key, trade in sorted(open_trade_by_key.items()):
        market = trade["market"]
        # AC43: zoek positie op position_key; fallback naar market voor pre-AC40
        pos = (
            positions.get(position_key)
            or portfolio_positions.get(position_key)
            or positions.get(market)
            or portfolio_positions.get(market)
            or {}
        )
        if str(pos.get("position", "FLAT")).upper() == "LONG":
            mark_price = to_float(
                last_price_map.get(market),
                trade.get("entry_price", 0.0)
            )
            entry_price = to_float(trade.get("entry_price", 0.0), 0.0)
            size = to_float(trade.get("size", 0.0), 0.0)
            unrealized = round(size * (mark_price - entry_price), 2)

            trade["mark_price"] = mark_price
            trade["unrealized_pnl"] = unrealized
            trade["feedback_score"] = unrealized
            trade["feedback_label"] = (
                "POSITIVE" if unrealized > 0 else ("NEGATIVE" if unrealized < 0 else "FLAT")
            )
            open_trades.append(trade)
        else:
            ignored_events += 1

    for trade in closed_trades:
        pnl = round(to_float(trade.get("realized_pnl", 0.0), 0.0), 2)
        trade["mark_price"] = to_float(trade.get("exit_price", 0.0), 0.0)
        trade["feedback_score"] = pnl
        trade["feedback_label"] = (
            "POSITIVE" if pnl > 0 else ("NEGATIVE" if pnl < 0 else "FLAT")
        )

    all_trades = open_trades + closed_trades

    realized_total = round(
        sum(to_float(x.get("realized_pnl", 0.0), 0.0) for x in closed_trades), 2
    )
    unrealized_total = round(
        sum(to_float(x.get("unrealized_pnl", 0.0), 0.0) for x in open_trades), 2
    )

    out = {
        "component": "paper_trade_reconstruction_lite",
        "ts_utc": ts,
        "trade_count_total": len(all_trades),
        "open_trade_count": len(open_trades),
        "closed_trade_count": len(closed_trades),
        "ignored_events": ignored_events,
        "realized_pnl_total": realized_total,
        "unrealized_pnl_total": unrealized_total,
        "portfolio_equity": to_float(portfolio_summary.get("equity", 0.0), 0.0),
        "portfolio_cash": to_float(portfolio_summary.get("cash", 0.0), 0.0),
        "valuation_state": portfolio_summary.get("valuation_state"),
        "rows": all_trades
    }

    write_json(OUT_TRADES_PATH, out)
    write_tsv(
        OUT_TRADES_TSV_PATH,
        [
            "trade_id",
            "market",
            "position_key",
            "strategy",
            "state",
            "entry_ts",
            "exit_ts",
            "entry_price",
            "mark_price",
            "exit_price",
            "size",
            "entry_notional_eur",
            "exit_notional_eur",
            "unrealized_pnl",
            "realized_pnl",
            "feedback_score",
            "feedback_label",
            "entry_decision_id",
            "exit_decision_id",
            "entry_reason",
            "exit_reason"
        ],
        all_trades
    )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
