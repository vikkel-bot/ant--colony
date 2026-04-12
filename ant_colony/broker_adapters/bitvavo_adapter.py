from __future__ import annotations
# === AC-36.5.2 GLOBAL SLEEP GUARD ===
import time as _time_module

_original_sleep = _time_module.sleep

def _safe_sleep(seconds):
    try:
        s = float(seconds)
    except Exception:
        s = 0.0
    if s < 0.0:
        s = 0.0
    return _original_sleep(s)

_time_module.sleep = _safe_sleep
# === END PATCH ===

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import types

from .base import BrokerAdapter, utc_now_iso


class BitvavoAdapter(BrokerAdapter):
    """
    Bitvavo-first broker adapter for Ant Colony.

    Current scope:
    - live test_connection()
    - live get_market_data()
    - placeholder implementations for the rest
    """

    adapter_name = "bitvavo"

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        rest_url: str = "https://api.bitvavo.com/v2",
        timeout_connect_s: int = 5,
        timeout_read_s: int = 20,
        max_retries: int = 3,
        min_request_interval_s: float = 0.20,
        ops_log_path: Optional[str] = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.getenv("BITVAVO_API_KEY")
        self.api_secret = api_secret if api_secret is not None else os.getenv("BITVAVO_API_SECRET")
        self.rest_url = rest_url.rstrip("/")
        self.timeout_connect_s = timeout_connect_s
        self.timeout_read_s = timeout_read_s
        self.max_retries = max_retries
        self.min_request_interval_s = min_request_interval_s
        self._last_request_ts = 0.0
        self.ops_log_path = Path(ops_log_path) if ops_log_path else None

    def _rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        remaining = self.min_request_interval_s - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_ts = time.monotonic()

    def _write_ops_log(
        self,
        *,
        operation: str,
        market: Optional[str],
        ok: bool,
        latency_ms: Optional[int],
        attempts: int,
        error_type: Optional[str],
    ) -> None:
        if not self.ops_log_path:
            return

        self.ops_log_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "ts_utc": utc_now_iso(),
            "adapter": self.adapter_name,
            "operation": operation,
            "market": market,
            "ok": ok,
            "latency_ms": latency_ms,
            "attempts": attempts,
            "error_type": error_type,
        }

        with self.ops_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _not_implemented(self, operation: str, *, market: Optional[str] = None) -> Dict[str, Any]:
        result = self._result_error(
            operation=operation,
            error_type="NOT_IMPLEMENTED",
            code="SKELETON_ONLY",
            message=f"{operation} is not implemented yet for BitvavoAdapter",
            retryable=False,
            attempts=0,
        )
        self._write_ops_log(
            operation=operation,
            market=market,
            ok=False,
            latency_ms=None,
            attempts=0,
            error_type="NOT_IMPLEMENTED",
        )
        return result

    def _import_client(self):
        from python_bitvavo_api.bitvavo import Bitvavo
        return Bitvavo

    def _make_client(self):
        Bitvavo = self._import_client()
        client = Bitvavo(
            {
                "APIKEY": self.api_key,
                "APISECRET": self.api_secret,
                "RESTURL": self.rest_url,
                "ACCESSWINDOW": 10000,
                "DEBUGGING": False,
            }
        )

        original_wait_for_reset = getattr(client, "waitForReset", None)

        if callable(original_wait_for_reset):
            def _safe_wait_for_reset(self_ref, wait_time):
                try:
                    safe_wait = float(wait_time)
                except Exception:
                    safe_wait = 0.0

                if safe_wait < 0.0:
                    safe_wait = 0.0

                try:
                    return original_wait_for_reset(safe_wait)
                except ValueError as e:
                    if "sleep length must be non-negative" in str(e):
                        time.sleep(0.0)
                        return None
                    raise

            client.waitForReset = types.MethodType(_safe_wait_for_reset, client)

        return client

    @staticmethod
    def _ms_to_iso(ms: Any) -> str:
        ts = float(ms) / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_connection(self) -> Dict[str, Any]:
        operation = "test_connection"
        t0 = time.perf_counter()

        if not self.api_key or not self.api_secret:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="AUTH_FAILED",
                code="MISSING_CREDENTIALS",
                message="Bitvavo credentials missing",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
            )
            self._write_ops_log(
                operation=operation,
                market=None,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="AUTH_FAILED",
            )
            return result

        try:
            self._import_client()
        except Exception as exc:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="UNKNOWN_ERROR",
                code="BITVAVO_IMPORT_FAILED",
                message="Failed to import python_bitvavo_api.bitvavo.Bitvavo",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
                raw_error=str(exc),
            )
            self._write_ops_log(
                operation=operation,
                market=None,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="UNKNOWN_ERROR",
            )
            return result

        attempts = 0
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            attempts = attempt
            try:
                self._rate_limit()
                client = self._make_client()
                balances = client.balance({})

                if isinstance(balances, dict) and balances.get("errorCode") is not None:
                    error_code = str(balances.get("errorCode"))
                    error_msg = str(balances.get("error", "Bitvavo returned an error"))
                    retryable = error_code in {"105", "429", "500", "502", "503", "504"}
                    error_type = "RATE_LIMITED" if error_code == "429" else "BROKER_REJECTED"

                    latency_ms = int((time.perf_counter() - t0) * 1000)
                    result = self._result_error(
                        operation=operation,
                        error_type=error_type,
                        code=error_code,
                        message=error_msg,
                        retryable=retryable,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        raw_error=balances,
                    )
                    self._write_ops_log(
                        operation=operation,
                        market=None,
                        ok=False,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        error_type=error_type,
                    )
                    return result

                balances_count = len(balances) if isinstance(balances, list) else None

                latency_ms = int((time.perf_counter() - t0) * 1000)
                result = self._result_ok(
                    operation=operation,
                    data={
                        "reachable": True,
                        "authenticated": True,
                        "account_id": None,
                        "balances_available": isinstance(balances, list),
                        "balances_count": balances_count,
                        "mode": "live",
                    },
                    latency_ms=latency_ms,
                    attempts=attempts,
                )
                self._write_ops_log(
                    operation=operation,
                    market=None,
                    ok=True,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error_type=None,
                )
                return result

            except Exception as exc:
                last_error = str(exc)
                if attempt < self.max_retries:
                    time.sleep(attempt)

        latency_ms = int((time.perf_counter() - t0) * 1000)
        result = self._result_error(
            operation=operation,
            error_type="NETWORK_ERROR",
            code="BITVAVO_CONNECTION_FAILED",
            message="Bitvavo test_connection failed after retries",
            retryable=True,
            latency_ms=latency_ms,
            attempts=attempts,
            raw_error=last_error,
        )
        self._write_ops_log(
            operation=operation,
            market=None,
            ok=False,
            latency_ms=latency_ms,
            attempts=attempts,
            error_type="NETWORK_ERROR",
        )
        return result

    def get_market_data(self, market: str, interval: str, limit: int = 200) -> Dict[str, Any]:
        operation = "get_market_data"
        t0 = time.perf_counter()

        if not market or not interval:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="INVALID_REQUEST",
                code="MISSING_ARGUMENTS",
                message="market and interval are required",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
            )
            self._write_ops_log(
                operation=operation,
                market=market,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="INVALID_REQUEST",
            )
            return result

        if limit <= 0:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="INVALID_REQUEST",
                code="INVALID_LIMIT",
                message="limit must be > 0",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
            )
            self._write_ops_log(
                operation=operation,
                market=market,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="INVALID_REQUEST",
            )
            return result

        try:
            self._import_client()
        except Exception as exc:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="UNKNOWN_ERROR",
                code="BITVAVO_IMPORT_FAILED",
                message="Failed to import python_bitvavo_api.bitvavo.Bitvavo",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
                raw_error=str(exc),
            )
            self._write_ops_log(
                operation=operation,
                market=market,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="UNKNOWN_ERROR",
            )
            return result

        attempts = 0
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            attempts = attempt
            try:
                self._rate_limit()
                client = self._make_client()

                raw = client.candles(
                    market,
                    interval,
                    {
                        "limit": int(limit),
                    }
                )

                if isinstance(raw, dict) and raw.get("errorCode") is not None:
                    error_code = str(raw.get("errorCode"))
                    error_msg = str(raw.get("error", "Bitvavo returned an error"))
                    retryable = error_code in {"105", "429", "500", "502", "503", "504"}
                    error_type = "RATE_LIMITED" if error_code == "429" else "BROKER_REJECTED"

                    latency_ms = int((time.perf_counter() - t0) * 1000)
                    result = self._result_error(
                        operation=operation,
                        error_type=error_type,
                        code=error_code,
                        message=error_msg,
                        retryable=retryable,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        raw_error=raw,
                    )
                    self._write_ops_log(
                        operation=operation,
                        market=market,
                        ok=False,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        error_type=error_type,
                    )
                    return result

                rows = []
                if isinstance(raw, list):
                    for item in raw:
                        if not isinstance(item, (list, tuple)) or len(item) < 6:
                            continue
                        rows.append(
                            {
                                "ts_utc": self._ms_to_iso(item[0]),
                                "open": float(item[1]),
                                "high": float(item[2]),
                                "low": float(item[3]),
                                "close": float(item[4]),
                                "volume": float(item[5]),
                            }
                        )

                rows = sorted(rows, key=lambda r: r["ts_utc"])

                latency_ms = int((time.perf_counter() - t0) * 1000)
                result = self._result_ok(
                    operation=operation,
                    data={
                        "market": market,
                        "interval": interval,
                        "rows": rows,
                        "count": len(rows),
                    },
                    latency_ms=latency_ms,
                    attempts=attempts,
                )
                self._write_ops_log(
                    operation=operation,
                    market=market,
                    ok=True,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error_type=None,
                )
                return result

            except Exception as exc:
                last_error = str(exc)
                if attempt < self.max_retries:
                    time.sleep(attempt)

        latency_ms = int((time.perf_counter() - t0) * 1000)
        result = self._result_error(
            operation=operation,
            error_type="NETWORK_ERROR",
            code="BITVAVO_MARKET_DATA_FAILED",
            message="Bitvavo get_market_data failed after retries",
            retryable=True,
            latency_ms=latency_ms,
            attempts=attempts,
            raw_error=last_error,
        )
        self._write_ops_log(
            operation=operation,
            market=market,
            ok=False,
            latency_ms=latency_ms,
            attempts=attempts,
            error_type="NETWORK_ERROR",
        )
        return result

    def place_order(self, order_request: Dict[str, Any]) -> Dict[str, Any]:
        operation = "place_order"
        t0 = time.perf_counter()

        if not isinstance(order_request, dict):
            latency_ms = int((time.perf_counter() - t0) * 1000)
            return self._result_error(
                operation=operation,
                error_type="INVALID_REQUEST",
                code="INVALID_ORDER_REQUEST",
                message="order_request must be a dict",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
            )

        market = order_request.get("market")
        side = order_request.get("side")
        order_type = order_request.get("order_type")
        qty = order_request.get("qty")

        missing = [f for f, v in [("market", market), ("side", side), ("order_type", order_type), ("qty", qty)] if not v and v != 0]
        if missing:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="INVALID_REQUEST",
                code="MISSING_ORDER_FIELDS",
                message=f"missing required order fields: {missing}",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
            )
            self._write_ops_log(
                operation=operation,
                market=market,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="INVALID_REQUEST",
            )
            return result

        body: Dict[str, Any] = {"amount": str(float(qty))}
        if order_type == "limit":
            intended_price = order_request.get("intended_entry_price")
            if intended_price is None:
                latency_ms = int((time.perf_counter() - t0) * 1000)
                result = self._result_error(
                    operation=operation,
                    error_type="INVALID_REQUEST",
                    code="MISSING_LIMIT_PRICE",
                    message="intended_entry_price required for limit orders",
                    retryable=False,
                    latency_ms=latency_ms,
                    attempts=0,
                )
                self._write_ops_log(
                    operation=operation,
                    market=market,
                    ok=False,
                    latency_ms=latency_ms,
                    attempts=0,
                    error_type="INVALID_REQUEST",
                )
                return result
            body["price"] = str(float(intended_price))

        client_request_id = order_request.get("client_request_id")
        if client_request_id:
            body["clientOrderId"] = str(client_request_id)

        try:
            self._import_client()
        except Exception as exc:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            result = self._result_error(
                operation=operation,
                error_type="UNKNOWN_ERROR",
                code="BITVAVO_IMPORT_FAILED",
                message="Failed to import python_bitvavo_api.bitvavo.Bitvavo",
                retryable=False,
                latency_ms=latency_ms,
                attempts=0,
                raw_error=str(exc),
            )
            self._write_ops_log(
                operation=operation,
                market=market,
                ok=False,
                latency_ms=latency_ms,
                attempts=0,
                error_type="UNKNOWN_ERROR",
            )
            return result

        attempts = 0
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            attempts = attempt
            try:
                self._rate_limit()
                client = self._make_client()
                raw = client.placeOrder(market, side, order_type, body)

                if isinstance(raw, dict) and raw.get("errorCode") is not None:
                    error_code = str(raw.get("errorCode"))
                    error_msg = str(raw.get("error", "Bitvavo returned an error"))
                    retryable = error_code in {"105", "429", "500", "502", "503", "504"}
                    error_type = "RATE_LIMITED" if error_code == "429" else "BROKER_REJECTED"

                    latency_ms = int((time.perf_counter() - t0) * 1000)
                    result = self._result_error(
                        operation=operation,
                        error_type=error_type,
                        code=error_code,
                        message=error_msg,
                        retryable=retryable,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        raw_error=raw,
                    )
                    self._write_ops_log(
                        operation=operation,
                        market=market,
                        ok=False,
                        latency_ms=latency_ms,
                        attempts=attempts,
                        error_type=error_type,
                    )
                    return result

                order_id = str(raw.get("orderId", "")) if isinstance(raw, dict) else ""
                status = str(raw.get("status", "")) if isinstance(raw, dict) else ""

                latency_ms = int((time.perf_counter() - t0) * 1000)
                result = self._result_ok(
                    operation=operation,
                    data={
                        "market": market,
                        "order_id": order_id,
                        "status": status,
                        "side": side,
                        "order_type": order_type,
                        "qty": qty,
                        "raw": raw if isinstance(raw, dict) else {},
                    },
                    latency_ms=latency_ms,
                    attempts=attempts,
                )
                self._write_ops_log(
                    operation=operation,
                    market=market,
                    ok=True,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    error_type=None,
                )
                return result

            except Exception as exc:
                last_error = str(exc)
                if attempt < self.max_retries:
                    time.sleep(attempt)

        latency_ms = int((time.perf_counter() - t0) * 1000)
        result = self._result_error(
            operation=operation,
            error_type="NETWORK_ERROR",
            code="BITVAVO_PLACE_ORDER_FAILED",
            message="Bitvavo place_order failed after retries",
            retryable=True,
            latency_ms=latency_ms,
            attempts=attempts,
            raw_error=last_error,
        )
        self._write_ops_log(
            operation=operation,
            market=market,
            ok=False,
            latency_ms=latency_ms,
            attempts=attempts,
            error_type="NETWORK_ERROR",
        )
        return result

    def cancel_order(self, broker_order_id: str, market: Optional[str] = None) -> Dict[str, Any]:
        return self._not_implemented("cancel_order", market=market)

    def get_open_orders(self, market: Optional[str] = None) -> Dict[str, Any]:
        return self._not_implemented("get_open_orders", market=market)

    def get_positions(self) -> Dict[str, Any]:
        return self._not_implemented("get_positions")

    def get_fills(self, market: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        return self._not_implemented("get_fills", market=market)



