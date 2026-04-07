import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")

PORTFOLIO_STATE_PATH = OUT_DIR / "paper_portfolio_state.json"
PORTFOLIO_VALUATION_PATH = OUT_DIR / "paper_portfolio_valuation.json"
TRADE_RECON_PATH = OUT_DIR / "paper_trade_reconstruction.json"
EXECUTION_SUMMARY_PATH = OUT_DIR / "paper_execution_summary.json"

OUT_JSON_PATH = OUT_DIR / "paper_portfolio_consistency_check.json"
OUT_TSV_PATH = OUT_DIR / "paper_portfolio_consistency_check.tsv"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return int(default)


def round2(v):
    return round(to_float(v, 0.0), 2)


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers, row):
    lines = ["`t".join(headers)]
    lines.append("`t".join(str(row.get(h, "")) for h in headers))
    path.write_text("`n".join(lines), encoding="utf-8")


def main():
    ts_utc = utc_now_ts()

    state_obj = load_json(PORTFOLIO_STATE_PATH, {})
    valuation_obj = load_json(PORTFOLIO_VALUATION_PATH, {})
    recon_obj = load_json(TRADE_RECON_PATH, {})
    exec_summary_obj = load_json(EXECUTION_SUMMARY_PATH, {})

    issues = []
    missing_inputs = []

    if not state_obj:
        missing_inputs.append(str(PORTFOLIO_STATE_PATH))
    if not valuation_obj:
        missing_inputs.append(str(PORTFOLIO_VALUATION_PATH))
    if not recon_obj:
        missing_inputs.append(str(TRADE_RECON_PATH))

    if missing_inputs:
        issues.append("MISSING_INPUTS")

    cash_state = round2(state_obj.get("cash", 0.0) if isinstance(state_obj, dict) else 0.0)
    cash_valuation = round2(valuation_obj.get("cash", 0.0) if isinstance(valuation_obj, dict) else 0.0)
    equity_valuation = round2(valuation_obj.get("equity", 0.0) if isinstance(valuation_obj, dict) else 0.0)
    positions_market_value = round2(valuation_obj.get("positions_market_value", 0.0) if isinstance(valuation_obj, dict) else 0.0)
    unrealized_pnl = round2(valuation_obj.get("unrealized_pnl", 0.0) if isinstance(valuation_obj, dict) else 0.0)
    valuation_state = str(valuation_obj.get("valuation_state", valuation_obj.get("state", "UNKNOWN")) or "UNKNOWN")

    open_positions_state = 0
    if isinstance(state_obj, dict):
        positions = state_obj.get("positions", {})
        if isinstance(positions, dict):
            open_positions_state = sum(
                1
                for _, p in positions.items()
                if isinstance(p, dict) and str(p.get("position", "FLAT")).upper() != "FLAT"
            )
        else:
            open_positions_state = to_int(state_obj.get("position_count", 0), 0)

    open_positions_valuation = to_int(
        valuation_obj.get("open_positions", 0) if isinstance(valuation_obj, dict) else 0, 0
    )
    open_positions_reconstructed = to_int(
        recon_obj.get("open_trade_count", 0) if isinstance(recon_obj, dict) else 0, 0
    )
    trade_count_total = to_int(
        recon_obj.get("trade_count_total", 0) if isinstance(recon_obj, dict) else 0, 0
    )

    cash_diff_state_vs_valuation = round2(cash_state - cash_valuation)
    open_positions_diff_state_vs_valuation = open_positions_state - open_positions_valuation
    open_positions_diff_state_vs_reconstruction = open_positions_state - open_positions_reconstructed
    open_positions_diff_valuation_vs_reconstruction = open_positions_valuation - open_positions_reconstructed

    equity_sanity_expected = round2(cash_valuation + positions_market_value)
    equity_diff_vs_expected = round2(equity_valuation - equity_sanity_expected)
    equity_sanity_ok = abs(equity_diff_vs_expected) < 0.01

    if abs(cash_diff_state_vs_valuation) >= 0.01:
        issues.append("CASH_MISMATCH_STATE_VS_VALUATION")

    if open_positions_diff_state_vs_valuation != 0:
        issues.append("OPEN_POSITIONS_MISMATCH_STATE_VS_VALUATION")

    if open_positions_diff_state_vs_reconstruction != 0:
        issues.append("OPEN_POSITIONS_MISMATCH_STATE_VS_RECONSTRUCTION")

    if open_positions_diff_valuation_vs_reconstruction != 0:
        issues.append("OPEN_POSITIONS_MISMATCH_VALUATION_VS_RECONSTRUCTION")

    if valuation_state != "OK":
        issues.append("VALUATION_NOT_OK")

    if not equity_sanity_ok:
        issues.append("EQUITY_SANITY_FAIL")

    consistency_ok = len(issues) == 0
    state = "OK" if consistency_ok else "DEGRADED"

    out = {
        "component": "paper_portfolio_consistency_check_lite",
        "ts_utc": ts_utc,
        "state": state,
        "consistency_ok": consistency_ok,
        "issues": issues,
        "missing_inputs": missing_inputs,
        "source_files": {
            "paper_portfolio_state": str(PORTFOLIO_STATE_PATH),
            "paper_portfolio_valuation": str(PORTFOLIO_VALUATION_PATH),
            "paper_trade_reconstruction": str(TRADE_RECON_PATH),
            "paper_execution_summary": str(EXECUTION_SUMMARY_PATH),
        },
        "cash_state": cash_state,
        "cash_valuation": cash_valuation,
        "cash_diff_state_vs_valuation": cash_diff_state_vs_valuation,
        "equity_valuation": equity_valuation,
        "positions_market_value": positions_market_value,
        "unrealized_pnl": unrealized_pnl,
        "equity_sanity_expected": equity_sanity_expected,
        "equity_diff_vs_expected": equity_diff_vs_expected,
        "equity_sanity_ok": equity_sanity_ok,
        "valuation_state": valuation_state,
        "open_positions_state": open_positions_state,
        "open_positions_valuation": open_positions_valuation,
        "open_positions_reconstructed": open_positions_reconstructed,
        "open_positions_diff_state_vs_valuation": open_positions_diff_state_vs_valuation,
        "open_positions_diff_state_vs_reconstruction": open_positions_diff_state_vs_reconstruction,
        "open_positions_diff_valuation_vs_reconstruction": open_positions_diff_valuation_vs_reconstruction,
        "trade_count_total": trade_count_total,
        "paper_execution_summary": {
            "intents_processed": to_int(exec_summary_obj.get("intents_processed", 0) if isinstance(exec_summary_obj, dict) else 0, 0),
            "intents_allowed": to_int(exec_summary_obj.get("intents_allowed", 0) if isinstance(exec_summary_obj, dict) else 0, 0),
            "intents_skipped": to_int(exec_summary_obj.get("intents_skipped", 0) if isinstance(exec_summary_obj, dict) else 0, 0),
            "position_count": to_int(exec_summary_obj.get("position_count", 0) if isinstance(exec_summary_obj, dict) else 0, 0),
        }
    }

    write_json(OUT_JSON_PATH, out)
    write_tsv(
        OUT_TSV_PATH,
        [
            "component",
            "ts_utc",
            "state",
            "consistency_ok",
            "cash_state",
            "cash_valuation",
            "cash_diff_state_vs_valuation",
            "equity_valuation",
            "positions_market_value",
            "unrealized_pnl",
            "equity_sanity_expected",
            "equity_diff_vs_expected",
            "equity_sanity_ok",
            "valuation_state",
            "open_positions_state",
            "open_positions_valuation",
            "open_positions_reconstructed",
            "open_positions_diff_state_vs_valuation",
            "open_positions_diff_state_vs_reconstruction",
            "open_positions_diff_valuation_vs_reconstruction",
            "trade_count_total",
            "issues",
        ],
        {
            "component": out["component"],
            "ts_utc": out["ts_utc"],
            "state": out["state"],
            "consistency_ok": out["consistency_ok"],
            "cash_state": out["cash_state"],
            "cash_valuation": out["cash_valuation"],
            "cash_diff_state_vs_valuation": out["cash_diff_state_vs_valuation"],
            "equity_valuation": out["equity_valuation"],
            "positions_market_value": out["positions_market_value"],
            "unrealized_pnl": out["unrealized_pnl"],
            "equity_sanity_expected": out["equity_sanity_expected"],
            "equity_diff_vs_expected": out["equity_diff_vs_expected"],
            "equity_sanity_ok": out["equity_sanity_ok"],
            "valuation_state": out["valuation_state"],
            "open_positions_state": out["open_positions_state"],
            "open_positions_valuation": out["open_positions_valuation"],
            "open_positions_reconstructed": out["open_positions_reconstructed"],
            "open_positions_diff_state_vs_valuation": out["open_positions_diff_state_vs_valuation"],
            "open_positions_diff_state_vs_reconstruction": out["open_positions_diff_state_vs_reconstruction"],
            "open_positions_diff_valuation_vs_reconstruction": out["open_positions_diff_valuation_vs_reconstruction"],
            "trade_count_total": out["trade_count_total"],
            "issues": "|".join(out["issues"]),
        }
    )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
