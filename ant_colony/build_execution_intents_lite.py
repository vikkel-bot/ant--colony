import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")
COMBINED_STATUS_PATH = OUT_DIR / "combined_colony_status.json"
OUT_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
TEST_OVERRIDE_PATH = OUT_DIR / "paper_execution_test_override.json"
PORTFOLIO_SUMMARY_PATH = OUT_DIR / "paper_portfolio_summary.json"


def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    if not path.exists():
        return None, f"missing:{path.name}"

    encodings = ["utf-8-sig", "utf-8"]
    last_error = None

    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            return json.loads(text), None
        except Exception as e:
            last_error = e

    return None, f"read_error:{path.name}:{last_error}"


def safe_str(value, default=""):
    if value is None:
        return default
    return str(value)


def infer_action(market_row):
    readiness = market_row.get("execution_readiness") or {}
    if not readiness.get("allowed", False):
        return "NO_ACTION"

    edge3 = market_row.get("edge3") or {}
    health = market_row.get("health") or {}

    if edge3.get("gate") == "ALLOW" and health.get("health_gate") == "ALLOW":
        return "ENTER_LONG"

    return "NO_ACTION"


def add_reason(reason_counts, reason):
    key = safe_str(reason, "UNKNOWN")
    reason_counts[key] = int(reason_counts.get(key, 0)) + 1


def load_test_override():
    obj, err = load_json(TEST_OVERRIDE_PATH)
    if err or not isinstance(obj, dict):
        return {
            "enabled": False,
            "market": None,
            "action": "ENTER_LONG",
            "reason": "TEST_OVERRIDE"
        }

    return {
        "enabled": bool(obj.get("enabled", False)),
        "market": obj.get("market"),
        "action": safe_str(obj.get("action"), "ENTER_LONG"),
        "reason": safe_str(obj.get("reason"), "TEST_OVERRIDE")
    }


def load_portfolio_summary():
    obj, err = load_json(PORTFOLIO_SUMMARY_PATH)
    if err or not isinstance(obj, dict):
        return {}
    return obj


def freshness_block_active(portfolio_summary: dict):
    valuation_state = safe_str(portfolio_summary.get("valuation_state"))
    all_prices_fresh = bool(portfolio_summary.get("all_prices_fresh", False))
    return (valuation_state == "BLOCKED_FRESHNESS") or (not all_prices_fresh)


def main():
    combined, combined_err = load_json(COMBINED_STATUS_PATH)
    portfolio_summary = load_portfolio_summary()
    now_ts = utc_now_ts()
    test_override = load_test_override()

    if combined_err or not isinstance(combined, dict):
        summary = {
            "version": "execution_summary_v7",
            "ts_utc": now_ts,
            "source_component": "build_execution_intents_lite",
            "combined_status_ok": False,
            "combined_status_error": combined_err,
            "markets_total": 0,
            "allowed_count": 0,
            "blocked_count": 0,
            "freshness_ok": False,
            "freshness_breakdown": {},
            "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
            "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
            "test_override": test_override,
            "reason_counts": {
                "MISSING_COMBINED_STATUS": 1 if combined_err and str(combined_err).startswith("missing:") else 0,
                "COMBINED_STATUS_ERROR": 1 if combined_err and not str(combined_err).startswith("missing:") else 0
            },
            "markets": {},
        }
        OUT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"WROTE {OUT_SUMMARY_PATH}")
        return

    cycle_id = combined.get("cycle_id")
    combined_ts_utc = combined.get("ts_utc")
    combined_version = combined.get("version")
    markets = combined.get("markets", {}) or {}
    combined_freshness = combined.get("freshness", {}) or {}

    summary_markets = {}
    allowed_count = 0
    blocked_count = 0
    reason_counts = {}

    freshness_block = freshness_block_active(portfolio_summary)

    for market in sorted(markets.keys()):
        market_row = markets.get(market) or {}
        edge3 = market_row.get("edge3") or {}
        health = market_row.get("health") or {}
        readiness = dict(market_row.get("execution_readiness") or {})
        action = infer_action(market_row)

        allowed = bool(readiness.get("allowed", False))
        reason = readiness.get("reason")
        override_applied = False

        if test_override["enabled"] and safe_str(test_override["market"]) == market:
            allowed = True
            action = test_override["action"]
            reason = test_override["reason"]
            readiness["allowed"] = True
            readiness["reason"] = test_override["reason"]
            override_applied = True

        if freshness_block:
            allowed = False
            action = "NO_ACTION"
            reason = "FRESHNESS_BLOCK"
            readiness["allowed"] = False
            readiness["reason"] = "FRESHNESS_BLOCK"
            override_applied = False

        decision_id = f"{market}_{safe_str(cycle_id, 'NO_CYCLE')}_{action}"

        intent = {
            "version": "execution_intent_v4",
            "ts_utc": now_ts,
            "source_component": "build_execution_intents_lite",
            "cycle_id": cycle_id,
            "market": market,
            "decision_id": decision_id,
            "action": action,
            "strategy": "EDGE4" if action in ("ENTER_LONG", "EXIT_LONG") else "NONE",
            "bias": "LONG" if action == "ENTER_LONG" else "NEUTRAL",
            "size_mult": health.get("health_size_mult"),
            "edge3_gate": edge3.get("gate"),
            "health": health.get("health_gate"),
            "execution_allowed": allowed,
            "block_reason": reason,
            "execution_readiness": readiness,
            "test_override_applied": override_applied,
            "source_files": {
                "combined_status": str(COMBINED_STATUS_PATH),
                "test_override": str(TEST_OVERRIDE_PATH),
                "portfolio_summary": str(PORTFOLIO_SUMMARY_PATH),
            },
            "source_meta": {
                "combined_status_version": combined_version,
                "combined_status_ts_utc": combined_ts_utc,
                "combined_status_cycle_id": cycle_id,
                "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
                "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
            }
        }

        out_path = OUT_DIR / f"{market}_execution_intent.json"
        out_path.write_text(json.dumps(intent, indent=2), encoding="utf-8")

        summary_markets[market] = {
            "allowed": allowed,
            "reason": reason,
            "action": action,
            "test_override_applied": override_applied,
            "intent_file": str(out_path),
        }

        if allowed:
            allowed_count += 1
        else:
            blocked_count += 1

        add_reason(reason_counts, reason)
        print(f"WROTE {out_path}")

    summary = {
        "version": "execution_summary_v7",
        "ts_utc": now_ts,
        "source_component": "build_execution_intents_lite",
        "combined_status_ok": True,
        "combined_status_error": None,
        "markets_total": len(markets),
        "allowed_count": allowed_count,
        "blocked_count": blocked_count,
        "freshness_ok": bool(combined_freshness.get("freshness_ok", False)),
        "freshness_breakdown": {
            "edge3_fresh": combined_freshness.get("edge3_fresh"),
            "edge4_fresh": combined_freshness.get("edge4_fresh"),
            "health_fresh": combined_freshness.get("health_fresh"),
            "execution_control_fresh": combined_freshness.get("execution_control_fresh"),
        },
        "portfolio_valuation_state": safe_str(portfolio_summary.get("valuation_state")),
        "portfolio_all_prices_fresh": bool(portfolio_summary.get("all_prices_fresh", False)),
        "test_override": test_override,
        "reason_counts": reason_counts,
        "markets": summary_markets,
    }

    OUT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"WROTE {OUT_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
