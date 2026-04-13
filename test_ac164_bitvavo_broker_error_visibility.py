"""
AC-164: Tests that the exact Bitvavo broker error is visible in the live order path.

Verifies:
  A. Exception text during placeOrder → surfaces in adapter error.message
  B. Exception text → surfaces in executor reason string (BROKER_CALL_FAILED: <text>)
  C. Broker error response (errorCode) → exact error text in adapter error.message
  D. Broker error response → surfaces in executor reason string
  E. Attempt count is included in the exception-exhaustion message
  F. Regression: successful order path still returns ok=True
  G. Regression: AC-163 operator_id block reason unchanged
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
    "operator_id": "OP-TEST",
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
    "operator_id": "OP-TEST",
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
    return BitvavoAdapter(api_key="test-key", api_secret="test-secret", **kwargs)


def _order_request(**overrides):
    r = dict(_BASE_ORDER_REQUEST)
    r.update(overrides)
    return r


def _intake(**overrides):
    r = dict(_LIVE_INTAKE)
    r.update(overrides)
    return r


class _MockAdapterOk:
    def __init__(self):
        self.last_order_request = None

    def place_order(self, order_request):
        self.last_order_request = dict(order_request)
        return dict(_MOCK_BROKER_RESPONSE_OK)


# ---------------------------------------------------------------------------
# A. Exception text surfaces in adapter error.message
# ---------------------------------------------------------------------------

def test_A_exception_text_in_adapter_error_message():
    """When placeOrder raises, the exception text appears in error.message (not just raw_error)."""
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = RuntimeError("Connection timed out: BNB-EUR")

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is False
    msg = result["error"]["message"]
    assert "Connection timed out: BNB-EUR" in msg


def test_A_exception_text_survives_multiple_retries():
    """After exhausting all retries, the LAST exception text is in error.message."""
    adapter = _make_adapter(max_retries=3)

    responses = [
        RuntimeError("attempt 1 failed"),
        RuntimeError("attempt 2 failed"),
        RuntimeError("final error: SSL handshake failed"),
    ]

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = responses

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is False
    msg = result["error"]["message"]
    assert "final error: SSL handshake failed" in msg
    # Earlier errors are NOT required to appear — only the last one matters
    assert "attempt 1 failed" not in msg


# ---------------------------------------------------------------------------
# B. Exception text surfaces in executor reason string
# ---------------------------------------------------------------------------

def test_B_exception_text_in_executor_reason():
    """Executor reason contains the exact exception text from the adapter."""

    class _ErrorAdapter:
        def place_order(self, order_request):
            return {
                "ok": False,
                "error": {
                    "type": "NETWORK_ERROR",
                    "code": "BITVAVO_PLACE_ORDER_FAILED",
                    "message": "Bitvavo place_order failed after 1 attempt(s): operatorId parameter is required.",
                    "retryable": True,
                },
                "meta": {},
            }

    result = execute_first_live_order(
        _intake(),
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_ErrorAdapter(),
    )

    assert result["ok"] is False
    assert result["gate"] == "G_BROKER_CALL"
    assert "operatorId parameter is required." in result["reason"]
    assert result["reason"].startswith("BROKER_CALL_FAILED: ")


def test_B_executor_reason_includes_full_adapter_message():
    """The full adapter message string (not a truncated version) reaches the executor reason."""

    long_error = "Bitvavo place_order failed after 3 attempt(s): HTTP 503 Service Unavailable — retry later"

    class _ErrorAdapter:
        def place_order(self, order_request):
            return {
                "ok": False,
                "error": {
                    "type": "NETWORK_ERROR",
                    "code": "BITVAVO_PLACE_ORDER_FAILED",
                    "message": long_error,
                    "retryable": True,
                },
                "meta": {},
            }

    result = execute_first_live_order(
        _intake(),
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_ErrorAdapter(),
    )

    assert result["ok"] is False
    assert result["reason"] == f"BROKER_CALL_FAILED: {long_error}"


# ---------------------------------------------------------------------------
# C. Broker error response (errorCode) → exact error text in adapter error.message
# ---------------------------------------------------------------------------

def test_C_broker_error_code_response_in_adapter_message():
    """When Bitvavo returns {errorCode, error}, that error text is in error.message."""
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.return_value = {
        "errorCode": 205,
        "error": "operatorId parameter is required.",
    }

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is False
    assert result["error"]["message"] == "operatorId parameter is required."
    assert result["error"]["code"] == "205"


def test_C_broker_error_code_sets_correct_type():
    """Non-rate-limit errorCode → error.type == BROKER_REJECTED."""
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.return_value = {
        "errorCode": 205,
        "error": "some broker rule violated",
    }

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is False
    assert result["error"]["type"] == "BROKER_REJECTED"


# ---------------------------------------------------------------------------
# D. Broker error response → surfaces in executor reason string
# ---------------------------------------------------------------------------

def test_D_broker_error_response_in_executor_reason():
    """Executor reason contains the exact Bitvavo errorCode message."""
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.return_value = {
        "errorCode": 205,
        "error": "operatorId parameter is required.",
    }

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = execute_first_live_order(
            _intake(),
            _LIVE_LANE_CFG,
            _MACRO_NORMAL,
            _AUTO_FREEZE_CLEAR,
            _adapter=adapter,
        )

    assert result["ok"] is False
    assert result["gate"] == "G_BROKER_CALL"
    assert "operatorId parameter is required." in result["reason"]
    assert result["reason"].startswith("BROKER_CALL_FAILED: ")


# ---------------------------------------------------------------------------
# E. Attempt count is included in the exception-exhaustion message
# ---------------------------------------------------------------------------

def test_E_attempt_count_in_exhaustion_message():
    """After retries, the message includes how many attempts were made."""
    adapter = _make_adapter(max_retries=2)

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = ConnectionError("network down")

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is False
    msg = result["error"]["message"]
    assert "2 attempt(s)" in msg
    assert "network down" in msg


# ---------------------------------------------------------------------------
# F. Regression: successful order path still returns ok=True
# ---------------------------------------------------------------------------

def test_F_regression_successful_order_unaffected():
    """A successful placeOrder response still returns ok=True with correct data."""
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.return_value = {
        "orderId": "ORD-REGR-001",
        "status": "filled",
        "market": "BNB-EUR",
        "side": "buy",
    }

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_order_request())

    assert result["ok"] is True
    assert result["data"]["order_id"] == "ORD-REGR-001"
    assert result["data"]["status"] == "filled"


def test_F_regression_executor_success_path_unaffected():
    """Full executor success path still returns ok=True with execution_result."""
    mock_adapter = _MockAdapterOk()

    result = execute_first_live_order(
        _intake(),
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=mock_adapter,
    )

    assert result["ok"] is True
    assert result["gate"] == "I_SCHEMA"
    assert result["execution_result"] is not None


# ---------------------------------------------------------------------------
# G. Regression: AC-163 operator_id block reason unchanged
# ---------------------------------------------------------------------------

def test_G_regression_ac163_operator_id_block_unchanged():
    """AC-163 BROKER_CONFIG_INVALID block reason is unaffected by AC-164 changes."""
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    class _ShouldNotBeCalled:
        def place_order(self, order_request):
            raise AssertionError("adapter must not be called")

    result = execute_first_live_order(
        {**_LIVE_INTAKE, "operator_id": None},  # strip operator_id
        _LIVE_LANE_CFG,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_ShouldNotBeCalled(),
    )

    assert result["ok"] is False
    assert result["reason"] == "BROKER_CONFIG_INVALID: operatorId missing"
    assert result["gate"] == "G_BROKER_CALL"
