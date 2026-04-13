"""
AC-163: Tests for Bitvavo operatorId support in live order path

Verifies:
  A. Missing operatorId (no env var, not in request) → block before broker call
  B. operatorId in order_request → passed as operatorId in body to placeOrder
  C. operatorId from adapter.operator_id (env var path) → passed to placeOrder
  D. order_request operatorId overrides adapter-level operator_id
  E. operatorId from intake_record flows through executor to adapter
  F. Regression: dry-mode executor (mock adapter) unaffected
  G. Regression: existing field validation (missing market etc.) still fires first
  H. Regression: limit order validation still fires before operator_id check
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.broker_adapters.bitvavo_adapter import BitvavoAdapter
from ant_colony.live.bitvavo_live_executor import execute_first_live_order

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = "2026-04-13T10:00:00Z"

_BASE_ORDER_REQUEST = {
    "market": "BNB-EUR",
    "side": "buy",
    "order_type": "market",
    "qty": 0.08,
}

_LIVE_INTAKE = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50.0,
    "allow_broker_execution": True,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "ts_intake_utc": _NOW,
}

_LIVE_LANE_CFG = {
    "lane": "live_test",
    "enabled": True,
    "live_enabled": True,
    "market": "BNB-EUR",
    "strategy": "EDGE3",
    "max_notional_eur": 50,
    "max_positions": 1,
    "allow_broker_execution": True,
    "allow_shared_state": False,
    "allow_paper_inputs": False,
    "base_output_dir": "C:\\Trading\\ANT_LIVE",
}

_MACRO_NORMAL = {
    "risk_state": "NORMAL",
    "reason": "",
    "freeze_new_entries": False,
    "updated_ts_utc": "",
}

_AUTO_FREEZE_CLEAR = {
    "allow": True,
    "reason": "AUTO_FREEZE_CLEAR",
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
}

_MOCK_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-001",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-001",
            "market": "BNB-EUR",
            "side": "buy",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
            "price": "601.5",
            "created": 1744459200000,
        },
    },
    "error": None,
    "meta": {"latency_ms": 120, "attempts": 1, "rate_limited": False},
}


def _make_adapter(**kwargs) -> BitvavoAdapter:
    """Adapter with dummy credentials so credential checks pass."""
    return BitvavoAdapter(
        api_key="test-key",
        api_secret="test-secret",
        **kwargs,
    )


def _order_request(**overrides):
    r = dict(_BASE_ORDER_REQUEST)
    r.update(overrides)
    return r


def _intake(**overrides):
    r = dict(_LIVE_INTAKE)
    r.update(overrides)
    return r


class _MockAdapterOk:
    """Mock adapter that records the last order_request it received."""

    def __init__(self):
        self.last_order_request = None

    def place_order(self, order_request):
        self.last_order_request = dict(order_request)
        return dict(_MOCK_BROKER_RESPONSE_OK)


# ---------------------------------------------------------------------------
# A. Missing operatorId → block before broker call
# ---------------------------------------------------------------------------

def test_A_missing_operator_id_blocks_order():
    """No operator_id in request and none on adapter → MISSING_OPERATOR_ID."""
    adapter = _make_adapter()  # no operator_id kwarg, no env var set
    # Ensure env var is absent
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    result = adapter.place_order(_order_request())

    assert result["ok"] is False
    err = result["error"]
    assert err["type"] == "MISSING_OPERATOR_ID"
    assert err["code"] == "OPERATOR_ID_REQUIRED"
    assert "operatorId" in err["message"] or "operator_id" in err["message"]
    assert err["retryable"] is False


def test_A_missing_operator_id_no_broker_call():
    """Verify that placeOrder on the Bitvavo client is never called when operatorId missing."""
    adapter = _make_adapter()
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    mock_client = MagicMock()
    with patch.object(adapter, "_make_client", return_value=mock_client):
        adapter.place_order(_order_request())

    mock_client.placeOrder.assert_not_called()


# ---------------------------------------------------------------------------
# B. operatorId in order_request → forwarded to placeOrder body
# ---------------------------------------------------------------------------

def test_B_operator_id_in_request_passed_to_bitvavo():
    """operator_id in order_request is included as operatorId in the placeOrder body."""
    adapter = _make_adapter()
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-001", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request(operator_id="OP-XYZ"))

    assert result["ok"] is True
    assert captured_body.get("operatorId") == "OP-XYZ"


# ---------------------------------------------------------------------------
# C. operatorId from adapter-level config (env var path)
# ---------------------------------------------------------------------------

def test_C_operator_id_from_adapter_config():
    """Adapter constructed with operator_id kwarg (simulating env var) forwards it."""
    adapter = _make_adapter(operator_id="OP-FROM-ENV")
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-002", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())  # no operator_id in request

    assert result["ok"] is True
    assert captured_body.get("operatorId") == "OP-FROM-ENV"


def test_C_operator_id_from_env_var():
    """BITVAVO_OPERATOR_ID env var is picked up by the adapter constructor."""
    os.environ["BITVAVO_OPERATOR_ID"] = "OP-FROM-ENVVAR"
    try:
        adapter = BitvavoAdapter(api_key="k", api_secret="s")
        assert adapter.operator_id == "OP-FROM-ENVVAR"

        captured_body = {}

        def fake_place_order(market, side, order_type, body):
            captured_body.update(body)
            return {"orderId": "ORD-003", "status": "filled"}

        mock_client = MagicMock()
        mock_client.placeOrder.side_effect = fake_place_order

        with patch.object(adapter, "_make_client", return_value=mock_client):
            result = adapter.place_order(_order_request())

        assert result["ok"] is True
        assert captured_body.get("operatorId") == "OP-FROM-ENVVAR"
    finally:
        os.environ.pop("BITVAVO_OPERATOR_ID", None)


# ---------------------------------------------------------------------------
# D. order_request operator_id overrides adapter-level operator_id
# ---------------------------------------------------------------------------

def test_D_request_operator_id_overrides_adapter_level():
    """operator_id in order_request takes priority over adapter.operator_id."""
    adapter = _make_adapter(operator_id="OP-ADAPTER")
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-004", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request(operator_id="OP-REQUEST"))

    assert result["ok"] is True
    assert captured_body.get("operatorId") == "OP-REQUEST"


# ---------------------------------------------------------------------------
# E. operatorId from intake_record flows through executor
# ---------------------------------------------------------------------------

def test_E_operator_id_from_intake_flows_to_adapter():
    """intake_record.operator_id is forwarded to the adapter's order_request."""
    mock_adapter = _MockAdapterOk()

    result = execute_first_live_order(
        _intake(operator_id="OP-INTAKE"),
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=mock_adapter,
    )

    assert result["ok"] is True
    assert mock_adapter.last_order_request is not None
    assert mock_adapter.last_order_request.get("operator_id") == "OP-INTAKE"


def test_E_missing_operator_id_in_intake_blocks_at_executor():
    """Executor blocks at Gate G before calling adapter when operatorId is absent."""
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    class _ShouldNotBeCalled:
        def place_order(self, order_request):
            raise AssertionError("adapter.place_order must not be called when operatorId missing")

    result = execute_first_live_order(
        _intake(),  # no operator_id key
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_ShouldNotBeCalled(),
    )

    assert result["ok"] is False
    assert result["reason"] == "BROKER_CONFIG_INVALID: operatorId missing"
    assert result["gate"] == "G_BROKER_CALL"


def test_E_operator_id_from_env_var_flows_through_executor():
    """BITVAVO_OPERATOR_ID env var is picked up by the executor and forwarded to adapter."""
    os.environ["BITVAVO_OPERATOR_ID"] = "OP-EXECUTOR-ENV"
    try:
        mock_adapter = _MockAdapterOk()
        result = execute_first_live_order(
            _intake(),  # no operator_id in intake
            _LIVE_LANE_CFG,
            _MACRO_NORMAL,
            _AUTO_FREEZE_CLEAR,
            _adapter=mock_adapter,
        )
        assert result["ok"] is True
        assert mock_adapter.last_order_request.get("operator_id") == "OP-EXECUTOR-ENV"
    finally:
        os.environ.pop("BITVAVO_OPERATOR_ID", None)


# ---------------------------------------------------------------------------
# F. Regression: dry-mode executor (mock adapter) unaffected
# ---------------------------------------------------------------------------

def test_F_regression_dry_mode_mock_adapter_unaffected():
    """Mock adapter returns ok=True regardless; executor succeeds as before."""
    mock_adapter = _MockAdapterOk()

    result = execute_first_live_order(
        _intake(operator_id="OP-REGRESSION"),
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=mock_adapter,
    )

    assert result["ok"] is True
    assert result["gate"] == "I_SCHEMA"


# ---------------------------------------------------------------------------
# G. Regression: missing required fields still caught before operator_id check
# ---------------------------------------------------------------------------

def test_G_regression_missing_market_caught_before_operator_id():
    """MISSING_ORDER_FIELDS fires before MISSING_OPERATOR_ID."""
    adapter = _make_adapter()
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    result = adapter.place_order({"side": "buy", "order_type": "market", "qty": 0.1})

    assert result["ok"] is False
    assert result["error"]["code"] == "MISSING_ORDER_FIELDS"


def test_G_regression_invalid_order_request_type_caught_first():
    """Non-dict order_request is caught before operator_id check."""
    adapter = _make_adapter()
    result = adapter.place_order("not-a-dict")

    assert result["ok"] is False
    assert result["error"]["code"] == "INVALID_ORDER_REQUEST"


# ---------------------------------------------------------------------------
# H. Regression: limit order missing price caught before operator_id check
# ---------------------------------------------------------------------------

def test_H_regression_limit_order_missing_price_caught_before_operator_id():
    """MISSING_LIMIT_PRICE fires before MISSING_OPERATOR_ID for limit orders."""
    adapter = _make_adapter()
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    result = adapter.place_order(
        _order_request(order_type="limit")  # no intended_entry_price
    )

    assert result["ok"] is False
    assert result["error"]["code"] == "MISSING_LIMIT_PRICE"
