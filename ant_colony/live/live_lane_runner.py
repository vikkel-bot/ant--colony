"""
AC-146/AC-147/AC-153/AC-168/AC-177/AC-187/AC-191: Live Lane Runner

Loads live lane config + macro freeze config, runs all guards, emits JSON.

Gate order (fail-closed at each step):
  1. Live lane guard (isolation constraints)
  2. Enabled flag
  3. Macro freeze guard
  4. Live execution gate (live_enabled + allow_broker_execution)
  5. (AC-168) Persistent execution via execute_and_persist_live_order
     — only when intake_record is supplied and all gates pass
     — (AC-177) intake_record is enriched with market_regime_at_entry /
       volatility_at_entry from cb20_regime.json before executor call

AC-187: main() writes an observational heartbeat JSON after every run
  attempt (including blocked/fail-closed outcomes). Heartbeat is written
  to {base_output_dir}/heartbeat.json and never affects execution
  decisions.

Constraints (hard):
- No reads from paper/simulation artefacts
- No writes outside own lane scope
- Output is JSON only
"""
from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ant_colony.live.live_lane_guard import validate
from ant_colony.live.macro_freeze_guard import check as macro_check
from ant_colony.live.macro_freeze_guard import load_macro_config
from ant_colony.live.live_execution_gate import evaluate_live_execution_gate

# ---------------------------------------------------------------------------
# AC-177: cb20 regime reader
# ---------------------------------------------------------------------------

# Workers live at ant_colony/workers/{MARKET}/reports/cb20_regime.json
# relative to the repo root (two levels up from this file's package dir).
_LIVE_DIR = Path(__file__).resolve().parent          # ant_colony/live/
_ANT_COLONY_DIR = _LIVE_DIR.parent                   # ant_colony/
_WORKERS_DIR = _ANT_COLONY_DIR / "workers"

_VALID_REGIMES = frozenset({"BULL", "BEAR", "SIDEWAYS", "UNKNOWN"})
_VALID_VOLATILITIES = frozenset({"LOW", "MID", "HIGH", "UNKNOWN"})

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "live_lane_config.json"
_DEFAULT_BASE_OUTPUT_DIR = r"C:\Trading\ANT_LIVE"
_HEARTBEAT_FILENAME = "heartbeat.json"

# Default auto-freeze result used when caller does not supply one.
# Fail-safe: CLEAR means auto-freeze is not blocking — the controlled live
# intake gate (AC-162) still validates all live conditions before execution.
_AUTO_FREEZE_CLEAR_DEFAULT: dict[str, Any] = {
    "allow": True,
    "reason": "AUTO_FREEZE_CLEAR",
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
}


def _load_cb20_regime(market: str) -> dict[str, str]:
    """
    AC-177: Load market_regime_at_entry and volatility_at_entry from the
    cb20_regime.json snapshot for the given market.

    Returns {"market_regime_at_entry": str, "volatility_at_entry": str}.
    Falls back to "UNKNOWN" for any field that is absent, invalid, or
    unreadable. Never raises.
    """
    regime = "UNKNOWN"
    volatility = "UNKNOWN"
    try:
        snap_path = _WORKERS_DIR / market / "reports" / "cb20_regime.json"
        if snap_path.exists():
            snap = json.loads(snap_path.read_text(encoding="utf-8"))
            tr = str(snap.get("trend_regime") or "").strip().upper()
            vl = str(snap.get("vol_regime") or "").strip().upper()
            if tr in _VALID_REGIMES:
                regime = tr
            if vl in _VALID_VOLATILITIES:
                volatility = vl
    except Exception:  # noqa: BLE001
        pass
    return {"market_regime_at_entry": regime, "volatility_at_entry": volatility}


def _write_heartbeat(
    result: dict[str, Any],
    base_output_dir: str | None = None,
) -> None:
    """
    AC-187: Write an observational heartbeat JSON file after each run attempt.

    Captures component, timestamp, last run status, and host. Never affects
    execution decisions. Never raises.
    """
    try:
        out_dir = Path(base_output_dir or _DEFAULT_BASE_OUTPUT_DIR)
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        state   = result.get("state", "UNKNOWN")
        heartbeat = {
            "component":    "live_lane_runner",
            "ts_utc":       now_str,
            "last_run_utc": now_str,
            "last_status":  state,
            "lane":         result.get("lane", "unknown"),
            "ok":           state != "BLOCKED",
            "reason":       result.get("reason"),
            "host":         socket.gethostname(),
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / _HEARTBEAT_FILENAME
        tmp  = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(heartbeat, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# AC-191: open-position scan + price fetch helpers
# ---------------------------------------------------------------------------

import re as _re
_SAFE_NAME_RE_RUNNER = _re.compile(r"[^a-zA-Z0-9_\-]")


def _find_open_position(cfg: dict[str, Any]) -> dict[str, Any] | None:
    """
    AC-191: Scan execution dir for an open position matching market/strategy.

    Returns the execution artifact dict when an open position exists (i.e.
    the artifact has no corresponding exit file).  Returns None when no open
    position is found.
    Fail-closed: unreadable artifacts or missing broker_order_id_entry cause
    an immediate return of None so callers block safely.
    Never raises.
    """
    try:
        base_output_dir = cfg.get("base_output_dir")
        if not base_output_dir:
            return None
        lane     = str(cfg.get("lane") or "live")
        market   = str(cfg.get("market") or "")
        strategy = str(cfg.get("strategy") or "")
        exec_dir = Path(base_output_dir) / lane / "execution"
        exit_dir = Path(base_output_dir) / lane / "exit"
        if not exec_dir.exists():
            return None
        for exec_file in sorted(exec_dir.glob("*.json")):
            try:
                data = json.loads(exec_file.read_text(encoding="utf-8"))
            except Exception:
                return None  # unreadable → fail-closed
            file_market   = str(data.get("market") or "")
            file_strategy = str(data.get("strategy_key") or data.get("strategy") or "")
            if file_market != market or file_strategy != strategy:
                continue
            broker_order_id = str(data.get("broker_order_id_entry") or "").strip()
            if not broker_order_id:
                return None  # missing id → fail-closed
            safe_id   = _SAFE_NAME_RE_RUNNER.sub("_", broker_order_id)
            exit_file = exit_dir / f"{safe_id}.json"
            if not exit_file.exists():
                return data  # open position found
        return None
    except Exception:  # noqa: BLE001
        return None


def _fetch_current_price(adapter: Any, market: str) -> float | None:
    """
    AC-191: Fetch the most recent close price for market via get_market_data.

    Returns float price on success, None on any error.
    Never raises.
    """
    try:
        if adapter is None or not hasattr(adapter, "get_market_data"):
            return None
        result = adapter.get_market_data(market, "1m", limit=1)
        if not result.get("ok"):
            return None
        rows = result.get("data", {}).get("rows", [])
        if not rows:
            return None
        price = rows[-1].get("close")
        if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
            return None
        return float(price)
    except Exception:  # noqa: BLE001
        return None


def load_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load lane config from JSON. Returns empty dict on error (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run(
    config: dict[str, Any] | None = None,
    macro_config: dict[str, Any] | None = None,
    *,
    intake_record: dict[str, Any] | None = None,
    auto_freeze_result: dict[str, Any] | None = None,
    _adapter: Any = None,
) -> dict[str, Any]:
    """
    Run all live lane guards and, when gates are open and an intake record is
    provided, execute the order and persist artifacts to disk (AC-168).

    Parameters:
        config            — lane config dict; loaded from JSON if None
        macro_config      — macro freeze config dict; loaded from JSON if None
        intake_record     — AC-150 validated intake dict; if None the runner
                            returns LIVE_GATE_READY without executing
        auto_freeze_result — pre-evaluated AC-155 auto-freeze result;
                            defaults to CLEAR when not supplied
        _adapter          — injectable broker adapter (for tests)

    Returns a JSON-serialisable dict. When execution succeeds the dict
    includes "state": "EXECUTED" and "artifacts" with disk paths.
    When a gate blocks, "state": "BLOCKED". Never raises.
    """
    if config is None:
        config = load_config()

    guard = validate(config)
    market = config.get("market", "unknown")
    strategy = config.get("strategy", "unknown")
    lane = config.get("lane", "unknown")

    # Gate 1: lane isolation constraints
    if not guard["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": guard["reason"],
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "market": market,
            "strategy": strategy,
        }

    # Gate 2: lane must be explicitly enabled
    if not config.get("enabled", False):
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": "LANE_DISABLED",
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "market": market,
            "strategy": strategy,
        }

    # Gate 3: macro freeze / risk override
    if macro_config is None:
        macro_config = load_macro_config()

    macro = macro_check(macro_config)
    risk_state = macro.get("risk_state", "UNKNOWN")
    if not macro["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": "MACRO_FREEZE_ACTIVE",
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
        }

    # Gate 4: live execution gate (live_enabled + allow_broker_execution)
    gate = evaluate_live_execution_gate(config, macro_config)
    live_enabled = gate.get("live_enabled", False)
    allow_broker = gate.get("allow_broker_execution", False)

    if not gate["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": gate["reason"],
            "live_enabled": live_enabled,
            "allow_broker_execution": allow_broker,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
        }

    # All gates are open.  Without an intake record, check for autonomous TP/SL
    # exit (AC-191) before returning LIVE_GATE_READY.
    if intake_record is None:
        freeze = auto_freeze_result if auto_freeze_result is not None else _AUTO_FREEZE_CLEAR_DEFAULT
        auto_exit = _check_auto_exit(
            config, macro_config, freeze, risk_state, lane, market, strategy, _adapter
        )
        if auto_exit is not None:
            return auto_exit
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "LIVE_GATE_READY",
            "live_enabled": True,
            "allow_broker_execution": True,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
            "note": "gate open; no execution in AC-153",
        }

    # Gate 5 (AC-168 / AC-188): persistent live execution — entry or exit path
    freeze = auto_freeze_result if auto_freeze_result is not None else _AUTO_FREEZE_CLEAR_DEFAULT

    # AC-188: if the intake carries an exit_intent, route to the exit executor.
    if isinstance(intake_record, dict) and "exit_intent" in intake_record:
        return _run_exit(
            intake_record["exit_intent"],
            config,
            macro_config,
            freeze,
            lane,
            market,
            strategy,
            risk_state,
            _adapter,
        )

    # AC-177: enrich intake with regime context from cb20_regime.json.
    # Caller-supplied fields take precedence; file values fill gaps only.
    enriched_intake = dict(intake_record)
    _regime_ctx = _load_cb20_regime(market)
    if "market_regime_at_entry" not in enriched_intake:
        enriched_intake["market_regime_at_entry"] = _regime_ctx["market_regime_at_entry"]
    if "volatility_at_entry" not in enriched_intake:
        enriched_intake["volatility_at_entry"] = _regime_ctx["volatility_at_entry"]

    from ant_colony.live.bitvavo_live_executor import execute_and_persist_live_order

    exec_result = execute_and_persist_live_order(
        enriched_intake,
        config,
        macro_config,
        freeze,
        _adapter=_adapter,
    )

    if not exec_result.get("ok"):
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": exec_result.get("reason", "EXECUTION_FAILED"),
            "gate": exec_result.get("gate", "G_BROKER_CALL"),
            "live_enabled": True,
            "allow_broker_execution": True,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
        }

    return {
        "component": "live_lane_runner",
        "lane": lane,
        "state": "EXECUTED",
        "live_enabled": True,
        "allow_broker_execution": True,
        "risk_state": risk_state,
        "market": market,
        "strategy": strategy,
        "execution_result": exec_result.get("execution_result"),
        "artifacts": exec_result.get("artifacts"),
    }


def _check_auto_exit(
    config: dict[str, Any],
    macro_config: dict[str, Any],
    freeze: dict[str, Any],
    risk_state: str,
    lane: str,
    market: str,
    strategy: str,
    _adapter: Any,
) -> dict[str, Any] | None:
    """
    AC-191: Autonomous TP/SL exit check.

    Called when no intake_record is present. Scans for an open position;
    if found, fetches the current price and evaluates TP/SL.

    Returns:
        EXIT_EXECUTED result dict  when exit is triggered and succeeds
        BLOCKED result dict        when price cannot be fetched (fail-closed)
                                   or when exit_signal returns an error
        None                       when no open position or within range
    Never raises.
    """
    try:
        open_pos = _find_open_position(config)
        if open_pos is None:
            return None  # no open position — nothing to check

        # Price fetch is required. Fail-closed if unavailable.
        current_price = _fetch_current_price(_adapter, market)
        if current_price is None:
            return {
                "component": "live_lane_runner",
                "lane":      lane,
                "state":     "BLOCKED",
                "reason":    "PRICE_FETCH_FAILED",
                "live_enabled": True,
                "allow_broker_execution": True,
                "risk_state": risk_state,
                "market":    market,
                "strategy":  strategy,
            }

        from ant_colony.live.live_exit_signal import evaluate_exit_signal
        signal = evaluate_exit_signal(open_pos, config, current_price)

        if signal is None:
            return None  # within range — no exit

        if isinstance(signal, dict) and not signal.get("ok", True):
            # evaluate_exit_signal returned a fail-closed error
            return {
                "component": "live_lane_runner",
                "lane":      lane,
                "state":     "BLOCKED",
                "reason":    f"EXIT_SIGNAL_ERROR: {signal.get('reason')}",
                "live_enabled": True,
                "allow_broker_execution": True,
                "risk_state": risk_state,
                "market":    market,
                "strategy":  strategy,
            }

        # TP or SL triggered — route to exit executor
        return _run_exit(
            signal,
            config,
            macro_config,
            freeze,
            lane,
            market,
            strategy,
            risk_state,
            _adapter,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "component": "live_lane_runner",
            "lane":      lane,
            "state":     "BLOCKED",
            "reason":    f"AUTO_EXIT_ERROR: {exc}",
            "live_enabled": True,
            "allow_broker_execution": True,
            "risk_state": risk_state,
            "market":    market,
            "strategy":  strategy,
        }


def _run_exit(
    exit_intent: Any,
    config: dict[str, Any],
    macro_config: dict[str, Any],
    freeze: dict[str, Any],
    lane: str,
    market: str,
    strategy: str,
    risk_state: str,
    _adapter: Any,
) -> dict[str, Any]:
    """
    AC-188: Execute a live exit and persist the exit artifact.

    Calls the existing exit executor, then writes an exit artifact to
    {base_output_dir}/{lane}/exit/ so the open-position guard can confirm
    the position is closed.  Fail-closed; never raises.
    """
    try:
        from ant_colony.live.bitvavo_live_exit_executor import execute_live_exit
        from ant_colony.live.live_artifact_writer import write_exit_artifact
        from datetime import datetime, timezone

        exit_result = execute_live_exit(
            exit_intent, config, macro_config, freeze, _adapter=_adapter
        )

        if not exit_result.get("ok"):
            return {
                "component": "live_lane_runner",
                "lane":      lane,
                "state":     "BLOCKED",
                "reason":    exit_result.get("reason", "EXIT_FAILED"),
                "market":    market,
                "strategy":  strategy,
                "risk_state": risk_state,
            }

        # Build artifact record combining exit intent with result
        ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        artifact = {
            **({k: v for k, v in exit_intent.items()} if isinstance(exit_intent, dict) else {}),
            "ok":                True,
            "reason":            "LIVE_EXIT_EXECUTED",
            "exit_execution_raw": exit_result.get("exit_execution_raw"),
            "ts_recorded_utc":   ts_now,
        }

        base_output_dir = config.get("base_output_dir") if isinstance(config, dict) else None
        if base_output_dir:
            write_exit_artifact(base_output_dir, lane, artifact)

        return {
            "component":    "live_lane_runner",
            "lane":         lane,
            "state":        "EXIT_EXECUTED",
            "live_enabled": True,
            "allow_broker_execution": True,
            "risk_state":   risk_state,
            "market":       market,
            "strategy":     strategy,
            "exit_result":  exit_result,
            "exit_artifact_dir": (
                str(Path(base_output_dir) / lane / "exit") if base_output_dir else None
            ),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "component": "live_lane_runner",
            "lane":      lane,
            "state":     "BLOCKED",
            "reason":    f"EXIT_UNEXPECTED_ERROR: {exc}",
            "market":    market,
            "strategy":  strategy,
            "risk_state": risk_state,
        }


def main() -> None:
    config = load_config()
    result = run(config=config)
    # AC-187: observational heartbeat — never affects execution decisions
    _write_heartbeat(result, config.get("base_output_dir"))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
