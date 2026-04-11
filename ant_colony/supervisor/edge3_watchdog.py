"""
AC-145: EDGE3 Watchdog (God Alert System)

Monitors EDGE3 worker snapshot files per market and sends Telegram alerts
within minutes when a failure is detected. Designed to prevent silent
EDGE3_FATAL states going undetected.

Checks per market every `check_interval_seconds`:
  A. SNAPSHOT_MISSING  -- edge3_snapshot.json does not exist
  B. STALE_DATA        -- snapshot ts_utc is older than stale_threshold_seconds
  C. EDGE3_FATAL       -- snapshot status/state != "OK"
  D. GATE_BLOCK        -- cb21 combined/health gate == "BLOCK"

Alert deduplication: per (market, alert_type) cooldown (default 10 min).

Design:
  - No dependency on queen / pipeline modules.
  - No file writes. Print JSON status to stdout only.
  - Any per-market error is caught and logged; watchdog never stops.
  - Telegram send: stdlib urllib only, max 3 retries, never raises.
  - telegram_enabled=false -> skip HTTP entirely (for tests/dry-run).

Usage:
    python ant_colony/supervisor/edge3_watchdog.py
    python ant_colony/supervisor/edge3_watchdog.py --config path/to/watchdog_config.json
    python ant_colony/supervisor/edge3_watchdog.py --once        # single check + exit
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "watchdog_config.json"

_DEFAULT_CHECK_INTERVAL  = 60
_DEFAULT_STALE_THRESHOLD = 300
_DEFAULT_COOLDOWN        = 600

# Alert type constants
ALERT_SNAPSHOT_MISSING = "SNAPSHOT_MISSING"
ALERT_STALE_DATA       = "STALE_DATA"
ALERT_EDGE3_FATAL      = "EDGE3_FATAL"
ALERT_GATE_BLOCK       = "GATE_BLOCK"

# Sentinel for OK state
_STATE_OK = "OK"


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: Path = _DEFAULT_CONFIG_PATH) -> dict:
    """
    Load watchdog config from JSON file.
    Returns empty dict with safe defaults on any error (never raises).
    """
    try:
        return json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[WARN] Could not load config from {config_path}: {exc}", flush=True)
        return {}


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: object) -> Optional[datetime]:
    """
    Parse an ISO-8601 timestamp string into a timezone-aware datetime (UTC).
    Returns None on any parse failure.
    Handles: "+00:00" suffix, "Z" suffix, microseconds, no sub-seconds.
    """
    if not isinstance(ts_str, str) or not ts_str:
        return None
    # Normalise "Z" suffix to "+00:00" for fromisoformat
    s = ts_str.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass
    # Fallback patterns
    for fmt in ("%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(ts_str[:19], fmt[:len(fmt)])
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Safe JSON reader
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> tuple[Optional[dict], Optional[str]]:
    """
    Read and parse a JSON file.
    Returns (data, None) on success, (None, error_str) on any failure.
    Never raises.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None, f"not a JSON object: {path.name}"
        return data, None
    except FileNotFoundError:
        return None, f"FILE_NOT_FOUND:{path}"
    except json.JSONDecodeError as exc:
        return None, f"JSON_DECODE_ERROR:{exc}"
    except Exception as exc:
        return None, f"READ_ERROR:{exc}"


# ---------------------------------------------------------------------------
# Per-market check (pure: injectable `now`)
# ---------------------------------------------------------------------------

def check_market(
    market: str,
    config: dict,
    now: Optional[datetime] = None,
) -> list[dict]:
    """
    Run all checks for a single market.

    Returns a list of alert dicts (empty = all OK).
    Never raises.

    Each alert dict:
      {market, event, reason, ts_utc, component}
    """
    now = now or datetime.now(timezone.utc)
    base    = Path(config.get("snapshot_base_path", ""))
    stale_s = int(config.get("stale_threshold_seconds", _DEFAULT_STALE_THRESHOLD))

    snapshot_path = base / market / "reports" / "edge3_snapshot.json"
    cb21_path     = base / market / "reports" / "edge3_cb21_meta.json"

    alerts: list[dict] = []

    # --- A. Snapshot file exists? ---
    snap_data, snap_err = _read_json(snapshot_path)
    if snap_data is None:
        alerts.append(_alert(market, ALERT_SNAPSHOT_MISSING,
                             f"edge3_snapshot.json missing or unreadable: {snap_err}",
                             now))
        # Can't do B/C without snapshot; still try cb21 for D
    else:
        # --- B. Timestamp stale? ---
        ts_dt = _parse_ts(snap_data.get("ts_utc"))
        if ts_dt is None:
            alerts.append(_alert(market, ALERT_STALE_DATA,
                                 "ts_utc missing or unparseable in snapshot", now))
        else:
            age_s = (now - ts_dt).total_seconds()
            if age_s > stale_s:
                alerts.append(_alert(
                    market, ALERT_STALE_DATA,
                    f"snapshot age {age_s:.0f}s > threshold {stale_s}s", now,
                ))

        # --- C. State/status check ---
        # Support both "state" (spec) and "status" (actual field)
        state = snap_data.get("state") or snap_data.get("status") or ""
        if str(state).upper() != _STATE_OK.upper():
            alerts.append(_alert(market, ALERT_EDGE3_FATAL,
                                 f"edge3 state={state!r}", now))

    # --- D. Gate check (cb21) ---
    cb21_data, cb21_err = _read_json(cb21_path)
    if cb21_data is not None:
        # Support "gate" (spec), "edge3_combined_gate", "edge3_health_gate" (actual)
        gate = (
            cb21_data.get("gate")
            or cb21_data.get("edge3_combined_gate")
            or cb21_data.get("edge3_health_gate")
            or ""
        )
        if str(gate).upper() == "BLOCK":
            alerts.append(_alert(market, ALERT_GATE_BLOCK,
                                 f"gate={gate!r} in cb21_meta", now))

    return alerts


def _alert(market: str, event: str, reason: str, now: datetime) -> dict:
    return {
        "market":    market,
        "event":     event,
        "reason":    reason,
        "ts_utc":    now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "component": "edge3_watchdog",
    }


# ---------------------------------------------------------------------------
# Cooldown helpers
# ---------------------------------------------------------------------------

def _is_cooled_down(
    cooldown_map: dict,
    market: str,
    event: str,
    now: datetime,
    cooldown_s: int,
) -> bool:
    """Return True if enough time has passed since last alert for this (market, event)."""
    last = cooldown_map.get((market, event))
    if last is None:
        return True
    return (now - last).total_seconds() >= cooldown_s


def _record_sent(
    cooldown_map: dict,
    market: str,
    event: str,
    now: datetime,
) -> None:
    cooldown_map[(market, event)] = now


# ---------------------------------------------------------------------------
# Telegram sender (never raises)
# ---------------------------------------------------------------------------

def send_telegram_alert(message: str, config: dict) -> bool:
    """
    Send a Telegram message via Bot API.

    Returns True if sent successfully, False otherwise.
    Retries up to 3 times. Never raises.
    """
    if not config.get("telegram_enabled", False):
        return False

    token   = config.get("telegram_bot_token", "")
    chat_id = config.get("telegram_chat_id", "")

    if not token or not chat_id:
        return False
    if token in ("PUT_YOUR_TOKEN_HERE", "") or chat_id in ("PUT_YOUR_CHAT_ID_HERE", ""):
        print("[WARN] Telegram credentials not configured.", flush=True)
        return False

    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8")
    headers = {"Content-Type": "application/json"}

    for attempt in range(1, 4):
        try:
            req  = urllib.request.Request(url, data=payload, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return True
                print(f"[WARN] Telegram HTTP {resp.status} (attempt {attempt}/3)",
                      flush=True)
        except urllib.error.URLError as exc:
            print(f"[WARN] Telegram attempt {attempt}/3 failed: {exc}", flush=True)
        except Exception as exc:
            print(f"[WARN] Telegram unexpected error attempt {attempt}/3: {exc}",
                  flush=True)
        if attempt < 3:
            time.sleep(1)

    return False


# ---------------------------------------------------------------------------
# Single check cycle (injectable `now` for tests)
# ---------------------------------------------------------------------------

def run_once(
    config: dict,
    cooldown_map: dict,
    now: Optional[datetime] = None,
    _send_fn=None,
) -> dict:
    """
    Run one full check cycle across all configured markets.

    Args:
        config:      watchdog config dict.
        cooldown_map: mutable dict tracking last-sent per (market, event).
        now:         injectable clock (defaults to datetime.now(utc)).
        _send_fn:    injectable send function for testing
                     (defaults to send_telegram_alert).

    Returns:
        Status dict: {component, ts_utc, markets_checked, alerts_sent, state, alerts}.
    """
    now = now or datetime.now(timezone.utc)
    if _send_fn is None:
        _send_fn = send_telegram_alert

    markets        = config.get("markets", [])
    cooldown_s     = int(config.get("alert_cooldown_seconds", _DEFAULT_COOLDOWN))
    alerts_sent    = 0
    all_alerts:    list[dict] = []
    markets_checked = 0

    for market in markets:
        try:
            market_alerts = check_market(market, config, now=now)
            markets_checked += 1
        except Exception as exc:
            # Belt-and-suspenders: check_market should never raise, but guard anyway
            print(f"[ERROR] Unexpected error checking {market}: {exc}", flush=True)
            market_alerts = [_alert(market, "WATCHDOG_ERROR", str(exc), now)]
            markets_checked += 1

        for alert in market_alerts:
            event = alert.get("event", "")
            if not _is_cooled_down(cooldown_map, market, event, now, cooldown_s):
                continue  # Cooldown active — suppress

            msg = json.dumps(alert, ensure_ascii=False)
            sent = _send_fn(msg, config)
            _record_sent(cooldown_map, market, event, now)

            if sent:
                alerts_sent += 1
            else:
                # Always count as "sent" from a tracking perspective —
                # the alert was triggered even if Telegram delivery failed.
                alerts_sent += 1

            all_alerts.append(alert)

    state = "ALERTING" if all_alerts else "OK"

    status = {
        "component":       "edge3_watchdog",
        "ts_utc":          now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "markets_checked": markets_checked,
        "alerts_sent":     alerts_sent,
        "state":           state,
        "alerts":          all_alerts,
    }
    print(json.dumps({k: v for k, v in status.items() if k != "alerts"},
                     ensure_ascii=False),
          flush=True)
    return status


# ---------------------------------------------------------------------------
# Continuous loop
# ---------------------------------------------------------------------------

def run_loop(config: dict, _send_fn=None) -> None:
    """
    Run watchdog loop indefinitely. Never raises.

    Ctrl-C exits cleanly.
    """
    interval   = int(config.get("check_interval_seconds", _DEFAULT_CHECK_INTERVAL))
    cooldown_map: dict = {}

    print(f"[INFO] edge3_watchdog starting. interval={interval}s "
          f"markets={config.get('markets', [])}", flush=True)

    while True:
        try:
            run_once(config, cooldown_map, _send_fn=_send_fn)
        except KeyboardInterrupt:
            print("[INFO] edge3_watchdog stopped by user.", flush=True)
            return
        except Exception as exc:
            # Should never happen — but if it does, log and continue
            print(f"[ERROR] run_once crashed unexpectedly: {exc}", flush=True)

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("[INFO] edge3_watchdog stopped by user.", flush=True)
            return


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser():
    import argparse
    p = argparse.ArgumentParser(
        description="AC-145: EDGE3 watchdog (God Alert System)."
    )
    p.add_argument(
        "--config", default=str(_DEFAULT_CONFIG_PATH),
        help=f"Path to watchdog_config.json (default: {_DEFAULT_CONFIG_PATH})",
    )
    p.add_argument(
        "--once", action="store_true",
        help="Run a single check cycle and exit (useful for testing/cron)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args   = _build_parser().parse_args(argv)
    config = load_config(Path(args.config))

    if args.once:
        cooldown_map: dict = {}
        status = run_once(config, cooldown_map)
        return 0 if status["state"] == "OK" else 1

    run_loop(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
