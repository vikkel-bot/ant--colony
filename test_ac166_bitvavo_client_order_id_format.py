"""
AC-166: Tests for Bitvavo clientOrderId format normalization.

Verifies:
  A. Short alphanumeric internal id → passed through unchanged
  B. Internal id with underscores (standard colony format) → underscores stripped
  C. Stripped id still <= 32 chars → used directly (no hash)
  D. Internal id that is too long after stripping → replaced by 32-char SHA256 hex
  E. The real colony client_request_id (REQ_lane_market_...) is normalized correctly
  F. Normalized id is always alphanumeric-only
  G. Normalized id is always <= 32 chars
  H. Normalization is deterministic (same input → same output)
  I. Empty / whitespace-only internal id → BROKER_REQUEST_INVALID block
  J. id with only special chars (no alphanumeric) → BROKER_REQUEST_INVALID block
  K. No client_request_id in order_request → clientOrderId absent from body (optional field)
  L. Adapter body key clientOrderId matches Bitvavo constraint regex
  M. Regression: operator_id block (AC-163) still fires before clientOrderId logic
  N. Regression: body None sweep (AC-165) still fires
  O. Regression: successful order still returns ok=True
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.broker_adapters.bitvavo_adapter import BitvavoAdapter, _to_bitvavo_client_order_id

_BITVAVO_ID_RE = re.compile(r'^[a-zA-Z0-9]{1,32}$')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter(**kwargs) -> BitvavoAdapter:
    return BitvavoAdapter(api_key="test-key", api_secret="test-secret",
                          operator_id="OP-TEST", **kwargs)


def _valid_request(**overrides):
    r = {
        "market": "BNB-EUR",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "operator_id": "OP-TEST",
    }
    r.update(overrides)
    return r


# ---------------------------------------------------------------------------
# A. Short alphanumeric id → passed through unchanged
# ---------------------------------------------------------------------------

def test_A_short_alphanumeric_id_unchanged():
    assert _to_bitvavo_client_order_id("REQ123") == "REQ123"


def test_A_exactly_32_alphanumeric_chars_unchanged():
    s = "a" * 32
    assert _to_bitvavo_client_order_id(s) == s


# ---------------------------------------------------------------------------
# B. Underscores stripped
# ---------------------------------------------------------------------------

def test_B_underscores_stripped():
    result = _to_bitvavo_client_order_id("REQ_live_test")
    assert "_" not in result
    assert result == "REQlivetest"


def test_B_hyphens_stripped():
    result = _to_bitvavo_client_order_id("REQ-live-test")
    assert "-" not in result
    assert result == "REQlivetest"


# ---------------------------------------------------------------------------
# C. Stripped id <= 32 chars → used directly (no hash)
# ---------------------------------------------------------------------------

def test_C_stripped_short_id_used_directly():
    # "REQ_buy" → "REQbuy" (6 chars, no hash needed)
    result = _to_bitvavo_client_order_id("REQ_buy")
    assert result == "REQbuy"
    assert len(result) == 6


# ---------------------------------------------------------------------------
# D. Too long after stripping → 32-char SHA256 hex
# ---------------------------------------------------------------------------

def test_D_too_long_after_stripping_uses_sha256():
    long_id = "REQ_" + "a" * 100  # stripped = "REQ" + "a"*100 = 103 chars → hash
    result = _to_bitvavo_client_order_id(long_id)
    expected = hashlib.sha256(long_id.encode()).hexdigest()[:32]
    assert result == expected
    assert len(result) == 32


def test_D_hash_output_is_hex_alphanumeric():
    long_id = "REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z_extra_padding"
    result = _to_bitvavo_client_order_id(long_id)
    assert re.match(r'^[a-f0-9]{32}$', result), f"Expected 32-char hex, got: {result!r}"


# ---------------------------------------------------------------------------
# E. Real colony client_request_id is normalized correctly
# ---------------------------------------------------------------------------

def test_E_real_colony_id_normalized():
    # This is exactly what broker_request_builder.py generates
    colony_id = "REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z"

    result = _to_bitvavo_client_order_id(colony_id)

    # After stripping underscores: "REQlivetestBNBEUREDGE3buymarket20260413T100000Z" = 47 chars → hash
    stripped = re.sub(r'[^a-zA-Z0-9]', '', colony_id)
    assert len(stripped) == 47  # confirms hash path is taken

    expected = hashlib.sha256(colony_id.encode()).hexdigest()[:32]
    assert result == expected
    assert len(result) == 32
    assert _BITVAVO_ID_RE.match(result)


def test_E_real_colony_id_reaches_bitvavo_body():
    """Verify the normalized id lands in body['clientOrderId'] in the actual adapter call."""
    adapter = _make_adapter(max_retries=1)
    colony_id = "REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z"

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-001", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_valid_request(client_request_id=colony_id))

    assert result["ok"] is True
    expected_id = hashlib.sha256(colony_id.encode()).hexdigest()[:32]
    assert captured_body.get("clientOrderId") == expected_id


# ---------------------------------------------------------------------------
# F. Normalized id is always alphanumeric-only
# ---------------------------------------------------------------------------

def test_F_result_always_alphanumeric():
    cases = [
        "REQ_live_test",
        "REQ-2026-04-13T10:00:00Z",
        "abc!@#$%^&*()123",
        "hello world",
        "test__id__here",
    ]
    for raw in cases:
        result = _to_bitvavo_client_order_id(raw)
        if result:  # non-empty means it should be valid
            assert _BITVAVO_ID_RE.match(result), f"Invalid for input {raw!r}: {result!r}"


# ---------------------------------------------------------------------------
# G. Normalized id is always <= 32 chars
# ---------------------------------------------------------------------------

def test_G_result_always_max_32_chars():
    cases = [
        "a" * 200,
        "REQ_" + "x" * 200,
        "Z" * 33,
    ]
    for raw in cases:
        result = _to_bitvavo_client_order_id(raw)
        assert len(result) <= 32, f"Too long for input {raw!r}: len={len(result)}"


# ---------------------------------------------------------------------------
# H. Normalization is deterministic
# ---------------------------------------------------------------------------

def test_H_normalization_is_deterministic():
    raw = "REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z"
    results = {_to_bitvavo_client_order_id(raw) for _ in range(10)}
    assert len(results) == 1, "Normalization must be deterministic"


# ---------------------------------------------------------------------------
# I. Empty / whitespace-only id → block
# ---------------------------------------------------------------------------

def test_I_empty_id_returns_empty_string():
    assert _to_bitvavo_client_order_id("") == ""
    assert _to_bitvavo_client_order_id(None) == ""  # type: ignore[arg-type]


def test_I_whitespace_only_id_blocks_adapter():
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = AssertionError("must not be called")

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_valid_request(client_request_id="   "))

    assert result["ok"] is False
    assert result["error"]["code"] == "BROKER_REQUEST_INVALID"
    assert "clientOrderId invalid" in result["error"]["message"]
    mock_client.placeOrder.assert_not_called()


# ---------------------------------------------------------------------------
# J. Only special chars → block
# ---------------------------------------------------------------------------

def test_J_special_chars_only_id_blocks_adapter():
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = AssertionError("must not be called")

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_valid_request(client_request_id="___---!!!"))

    assert result["ok"] is False
    assert result["error"]["code"] == "BROKER_REQUEST_INVALID"
    assert "clientOrderId invalid" in result["error"]["message"]
    mock_client.placeOrder.assert_not_called()


# ---------------------------------------------------------------------------
# K. No client_request_id → clientOrderId absent from body
# ---------------------------------------------------------------------------

def test_K_no_client_request_id_means_no_client_order_id_in_body():
    """client_request_id is optional; if absent, clientOrderId is not sent to Bitvavo."""
    adapter = _make_adapter(max_retries=1)

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-002", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_valid_request())  # no client_request_id

    assert result["ok"] is True
    assert "clientOrderId" not in captured_body


# ---------------------------------------------------------------------------
# L. clientOrderId in body matches Bitvavo constraint regex
# ---------------------------------------------------------------------------

def test_L_client_order_id_in_body_matches_bitvavo_regex():
    adapter = _make_adapter(max_retries=1)
    colony_id = "REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z"

    captured_body = {}

    def fake_place_order(market, side, order_type, body):
        captured_body.update(body)
        return {"orderId": "ORD-003", "status": "filled"}

    mock_client = MagicMock()
    mock_client.placeOrder.side_effect = fake_place_order

    with patch.object(adapter, "_make_client", return_value=mock_client):
        adapter.place_order(_valid_request(client_request_id=colony_id))

    cid = captured_body.get("clientOrderId", "")
    assert _BITVAVO_ID_RE.match(cid), f"clientOrderId {cid!r} fails Bitvavo regex"


# ---------------------------------------------------------------------------
# M. Regression: AC-163 operator_id block fires before clientOrderId logic
# ---------------------------------------------------------------------------

def test_M_regression_ac163_operator_id_block_fires_first():
    adapter = _make_adapter()
    adapter.operator_id = None
    os.environ.pop("BITVAVO_OPERATOR_ID", None)

    result = adapter.place_order(_valid_request(operator_id=None,
                                                client_request_id="REQ_live_test"))

    assert result["ok"] is False
    assert result["error"]["code"] == "OPERATOR_ID_REQUIRED"


# ---------------------------------------------------------------------------
# N. Regression: body None sweep (AC-165) still fires
# ---------------------------------------------------------------------------

def test_N_regression_ac165_none_sweep_still_fires():
    """MISSING_ORDER_FIELDS still blocks None market before clientOrderId is reached."""
    adapter = _make_adapter()
    result = adapter.place_order(_valid_request(market=None))

    assert result["ok"] is False
    assert result["error"]["code"] == "MISSING_ORDER_FIELDS"


# ---------------------------------------------------------------------------
# O. Regression: successful order still returns ok=True
# ---------------------------------------------------------------------------

def test_O_regression_successful_order():
    adapter = _make_adapter(max_retries=1)

    mock_client = MagicMock()
    mock_client.placeOrder.return_value = {"orderId": "ORD-OK", "status": "filled"}

    with patch.object(adapter, "_make_client", return_value=mock_client):
        result = adapter.place_order(_valid_request(
            client_request_id="REQ_live_test_BNBEUR_EDGE3_buy_market_20260413T100000Z"
        ))

    assert result["ok"] is True
    assert result["data"]["order_id"] == "ORD-OK"
