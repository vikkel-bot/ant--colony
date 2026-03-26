from __future__ import annotations

from typing import Any, Dict, Optional

from ant_colony.broker_adapters import BitvavoAdapter


def _adapter_source_name(adapter: Any) -> str:
    name = getattr(adapter, "adapter_name", None)
    if name:
        return f"{name}_adapter"
    return "unknown_adapter"


def get_worker_market_data(
    *,
    market: str,
    interval: str,
    limit: int = 200,
    adapter: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Read-only worker market data interface.

    Purpose:
    - hide adapter envelope details from worker callers
    - provide a stable worker-facing data contract
    - keep broker-specific logic outside worker code

    Current backend:
    - default adapter is BitvavoAdapter
    - caller may inject another adapter with get_market_data()

    Notes:
    - read-only only
    - no indicators
    - no strategy logic
    - no caching logic here
    """

    use_adapter = adapter or BitvavoAdapter()
    source = _adapter_source_name(use_adapter)

    result = use_adapter.get_market_data(
        market=market,
        interval=interval,
        limit=limit,
    )

    if not result.get("ok"):
        return {
            "ok": False,
            "source": source,
            "adapter": "bitvavo",
            "market": market,
            "interval": interval,
            "count": 0,
            "rows": [],
            "error": result.get("error"),
            "meta": result.get("meta"),
        }

    data = result.get("data") or {}
    rows = data.get("rows") or []

    return {
        "ok": True,
        "source": source,
        "adapter": "bitvavo",
        "market": data.get("market", market),
        "interval": data.get("interval", interval),
        "count": len(rows),
        "rows": rows,
        "error": None,
        "meta": result.get("meta"),
    }

