from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class BrokerAdapter(ABC):
    """
    Abstract broker adapter contract for Ant Colony.

    Rules:
    - No strategy logic
    - No regime logic
    - No indicator calculations
    - No dashboard logic
    - Only broker transport / normalization / error handling
    """

    adapter_name: str = "base"

    def _result_ok(
        self,
        operation: str,
        data: Optional[Dict[str, Any]] = None,
        *,
        latency_ms: Optional[int] = None,
        attempts: int = 1,
        rate_limited: bool = False,
        meta_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta = {
            "latency_ms": latency_ms,
            "attempts": attempts,
            "rate_limited": rate_limited,
        }
        if meta_extra:
            meta.update(meta_extra)

        return {
            "ok": True,
            "adapter": self.adapter_name,
            "operation": operation,
            "ts_utc": utc_now_iso(),
            "data": data if data is not None else {},
            "error": None,
            "meta": meta,
        }

    def _result_error(
        self,
        operation: str,
        error_type: str,
        message: str,
        *,
        code: Optional[str] = None,
        retryable: bool = False,
        latency_ms: Optional[int] = None,
        attempts: int = 1,
        rate_limited: bool = False,
        raw_error: Optional[Any] = None,
        meta_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta = {
            "latency_ms": latency_ms,
            "attempts": attempts,
            "rate_limited": rate_limited,
        }
        if raw_error is not None:
            meta["raw_error"] = raw_error
        if meta_extra:
            meta.update(meta_extra)

        return {
            "ok": False,
            "adapter": self.adapter_name,
            "operation": operation,
            "ts_utc": utc_now_iso(),
            "data": None,
            "error": {
                "type": error_type,
                "code": code,
                "message": message,
                "retryable": retryable,
            },
            "meta": meta,
        }

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_market_data(self, market: str, interval: str, limit: int = 200) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def place_order(self, order_request: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, broker_order_id: str, market: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_open_orders(self, market: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_positions(self) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_fills(self, market: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        raise NotImplementedError