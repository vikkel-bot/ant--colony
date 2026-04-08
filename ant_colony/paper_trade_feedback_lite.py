import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

TRADES_PATH = OUT_DIR / "paper_trade_reconstruction.json"
OUT_FEEDBACK_PATH = OUT_DIR / "paper_trade_feedback.json"
OUT_FEEDBACK_TSV_PATH = OUT_DIR / "paper_trade_feedback.tsv"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers, rows):
    lines = ["`t".join(headers)]
    for row in rows:
        lines.append("`t".join(str(row.get(h, "")) for h in headers))
    path.write_text("`n".join(lines), encoding="utf-8")


def main():
    ts = utc_now_ts()
    trades = load_json(TRADES_PATH, {})
    rows = trades.get("rows", []) if isinstance(trades, dict) else []

    feedback_rows = []
    for row in rows:
        market = row.get("market")
        # AC43: position_key en strategy doorsturen; fallback naar market/UNKNOWN
        position_key = row.get("position_key") or market
        strategy = row.get("strategy") or "UNKNOWN"

        feedback_rows.append({
            "market": market,
            "position_key": position_key,
            "strategy": strategy,
            "trade_id": row.get("trade_id"),
            "state": row.get("state"),
            "entry_ts": row.get("entry_ts"),
            "exit_ts": row.get("exit_ts"),
            "feedback_score": row.get("feedback_score", 0.0),
            "feedback_label": row.get("feedback_label", "FLAT"),
            "realized_pnl": row.get("realized_pnl", 0.0),
            "unrealized_pnl": row.get("unrealized_pnl", 0.0),
            "entry_decision_id": row.get("entry_decision_id"),
            "exit_decision_id": row.get("exit_decision_id"),
        })

    out = {
        "component": "paper_trade_feedback_lite",
        "ts_utc": ts,
        "feedback_rows_total": len(feedback_rows),
        "trade_count_total": int(trades.get("trade_count_total", 0)),
        "open_trade_count": int(trades.get("open_trade_count", 0)),
        "closed_trade_count": int(trades.get("closed_trade_count", 0)),
        "realized_pnl_total": trades.get("realized_pnl_total", 0.0),
        "unrealized_pnl_total": trades.get("unrealized_pnl_total", 0.0),
        "portfolio_equity": trades.get("portfolio_equity", 0.0),
        "portfolio_cash": trades.get("portfolio_cash", 0.0),
        "valuation_state": trades.get("valuation_state"),
        "rows": feedback_rows
    }

    write_json(OUT_FEEDBACK_PATH, out)
    write_tsv(
        OUT_FEEDBACK_TSV_PATH,
        [
            "market",
            "position_key",
            "strategy",
            "trade_id",
            "state",
            "entry_ts",
            "exit_ts",
            "feedback_score",
            "feedback_label",
            "realized_pnl",
            "unrealized_pnl",
            "entry_decision_id",
            "exit_decision_id"
        ],
        feedback_rows
    )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
