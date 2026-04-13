"""
AC-151: Tests for Broker Request Builder (dry only)

Verifies:
  A. Valid long intake  → ok=True, broker_request present
  B. Valid short intake → ok=True, broker_request present
  C. Invalid intake passes through as fail-closed
  D. Invalid market → fail-closed via intake layer
  E. Invalid strategy_key → fail-closed
  F. Invalid order_type → fail-closed
  G. qty * price > max_notional → fail-closed
  H. operator_approved=False is allowed (bool, no live execution yet)
  I. client_request_id is non-empty deterministic string
  J. ts_request_utc is set and matches intake ts
  K. Builder never raises exceptions
  L. broker_request contains all required payload fields
  M. No broker imports in source
  N. No file IO markers in source
  O. No network/http markers in source
  P. Determinism: same input → same output
  Q. Extra intake fields do not appear in broker_request
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.broker_request_builder import build_broker_request, _PAYLOAD_KEYS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_LONG = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50,
    "allow_broker_execution": False,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "ts_intake_utc": "2026-04-01T10:00:00Z",
}

_VALID_SHORT = {
    **_VALID_LONG,
    "position_side": "short",
    "order_side": "sell",
}


def _intake(**overrides) -> dict:
    r = dict(_VALID_LONG)
    r.update(overrides)
    return r


def _intake_without(field: str) -> dict:
    r = dict(_VALID_LONG)
    del r[field]
    return r


# ---------------------------------------------------------------------------
# A. Valid long intake → ok=True
# ---------------------------------------------------------------------------

class TestValidLong:
    def test_ok_true(self):
        result = build_broker_request(_VALID_LONG)
        assert result["ok"] is True

    def test_reason_broker_request_ready(self):
        result = build_broker_request(_VALID_LONG)
        assert result["reason"] == "BROKER_REQUEST_READY"

    def test_broker_request_present(self):
        result = build_broker_request(_VALID_LONG)
        assert result["broker_request"] is not None
        assert isinstance(result["broker_request"], dict)

    def test_order_side_buy(self):
        assert build_broker_request(_VALID_LONG)["broker_request"]["order_side"] == "buy"

    def test_market_correct(self):
        assert build_broker_request(_VALID_LONG)["broker_request"]["market"] == "BNB-EUR"

    def test_strategy_key_correct(self):
        assert build_broker_request(_VALID_LONG)["broker_request"]["strategy_key"] == "EDGE3"

    def test_limit_order_type_allowed(self):
        result = build_broker_request(_intake(order_type="limit"))
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# B. Valid short intake → ok=True
# ---------------------------------------------------------------------------

class TestValidShort:
    def test_short_ok_true(self):
        result = build_broker_request(_VALID_SHORT)
        assert result["ok"] is True

    def test_short_order_side_sell(self):
        assert build_broker_request(_VALID_SHORT)["broker_request"]["order_side"] == "sell"

    def test_short_lane_correct(self):
        assert build_broker_request(_VALID_SHORT)["broker_request"]["lane"] == "live_test"


# ---------------------------------------------------------------------------
# C. Invalid intake → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidIntakeFails:
    def test_missing_required_field_blocked(self):
        result = build_broker_request(_intake_without("qty"))
        assert result["ok"] is False
        assert result["broker_request"] is None

    def test_reason_mentions_intake(self):
        result = build_broker_request(_intake_without("market"))
        assert "intake" in result["reason"].lower()

    def test_empty_dict_blocked(self):
        result = build_broker_request({})
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# D. Invalid market → fail-closed via intake
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        result = build_broker_request(_intake(market="BTC-EUR"))
        assert result["ok"] is False

    def test_empty_market_blocked(self):
        result = build_broker_request(_intake(market=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# E. Invalid strategy_key → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        result = build_broker_request(_intake(strategy_key="RSI_SIMPLE"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# F. Invalid order_type → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidOrderType:
    def test_unknown_order_type_blocked(self):
        result = build_broker_request(_intake(order_type="stop_limit"))
        assert result["ok"] is False

    def test_empty_order_type_blocked(self):
        result = build_broker_request(_intake(order_type=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# G. qty * price > max_notional → fail-closed
# ---------------------------------------------------------------------------

class TestNotionalBreach:
    def test_notional_breach_blocked(self):
        # 0.09 * 600 = 54 > 50
        result = build_broker_request(
            _intake(qty=0.09, intended_entry_price=600.0, max_notional_eur=50)
        )
        assert result["ok"] is False

    def test_notional_at_limit_allowed(self):
        # 0.08333 * 600 ≈ 49.998 <= 50
        result = build_broker_request(
            _intake(qty=0.08333, intended_entry_price=600.0, max_notional_eur=50)
        )
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# H. operator_approved=False is allowed (bool, no live execution yet)
# ---------------------------------------------------------------------------

class TestOperatorApproved:
    def test_operator_approved_false_allowed(self):
        result = build_broker_request(_intake(operator_approved=False))
        assert result["ok"] is True
        assert result["broker_request"]["operator_approved"] is False

    def test_operator_approved_true_allowed(self):
        result = build_broker_request(_intake(operator_approved=True))
        assert result["ok"] is True
        assert result["broker_request"]["operator_approved"] is True

    def test_operator_approved_string_blocked(self):
        result = build_broker_request(_intake(operator_approved="yes"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# I. client_request_id is a UUID; colony_request_ref holds the internal trace
# ---------------------------------------------------------------------------

class TestClientRequestId:
    def test_non_empty(self):
        crid = build_broker_request(_VALID_LONG)["broker_request"]["client_request_id"]
        assert isinstance(crid, str)
        assert crid.strip() != ""

    def test_is_valid_uuid(self):
        import uuid
        crid = build_broker_request(_VALID_LONG)["broker_request"]["client_request_id"]
        # Must parse as UUID v4 without raising
        parsed = uuid.UUID(crid, version=4)
        assert str(parsed) == crid

    def test_unique_per_call(self):
        # UUIDs are random — two calls must not produce the same value
        r1 = build_broker_request(dict(_VALID_LONG))
        r2 = build_broker_request(dict(_VALID_LONG))
        assert r1["broker_request"]["client_request_id"] != \
               r2["broker_request"]["client_request_id"]

    def test_differs_for_different_side(self):
        long_r = build_broker_request(_VALID_LONG)
        short_r = build_broker_request(_VALID_SHORT)
        assert long_r["broker_request"]["client_request_id"] != \
               short_r["broker_request"]["client_request_id"]

    # colony_request_ref carries the internal trace (AC-169)
    def test_colony_request_ref_present(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "colony_request_ref" in br

    def test_colony_request_ref_starts_with_req(self):
        ref = build_broker_request(_VALID_LONG)["broker_request"]["colony_request_ref"]
        assert ref.startswith("REQ_")

    def test_colony_request_ref_contains_market_fragment(self):
        ref = build_broker_request(_VALID_LONG)["broker_request"]["colony_request_ref"]
        assert "BNBEUR" in ref

    def test_colony_request_ref_contains_strategy_fragment(self):
        ref = build_broker_request(_VALID_LONG)["broker_request"]["colony_request_ref"]
        assert "EDGE3" in ref

    def test_colony_request_ref_deterministic(self):
        r1 = build_broker_request(dict(_VALID_LONG))
        r2 = build_broker_request(dict(_VALID_LONG))
        assert r1["broker_request"]["colony_request_ref"] == \
               r2["broker_request"]["colony_request_ref"]


# ---------------------------------------------------------------------------
# J. ts_request_utc set and matches intake ts
# ---------------------------------------------------------------------------

class TestTsRequestUtc:
    def test_ts_request_utc_present(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "ts_request_utc" in br
        assert isinstance(br["ts_request_utc"], str)
        assert br["ts_request_utc"].strip() != ""

    def test_ts_request_utc_matches_intake(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert br["ts_request_utc"] == _VALID_LONG["ts_intake_utc"]


# ---------------------------------------------------------------------------
# K. Builder never raises exceptions
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_input", [
        None, 42, "string", [], True, {},
    ])
    def test_no_exception_on_bad_input(self, bad_input):
        result = build_broker_request(bad_input)
        assert isinstance(result, dict)
        assert "ok" in result

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0):
            result = build_broker_request(v)
            assert isinstance(result, dict)

    def test_failed_result_has_broker_request_none(self):
        result = build_broker_request(None)
        assert result["broker_request"] is None


# ---------------------------------------------------------------------------
# L. broker_request contains all required payload fields
# ---------------------------------------------------------------------------

class TestPayloadFields:
    def test_all_payload_keys_present(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        for key in _PAYLOAD_KEYS:
            assert key in br, f"missing key in broker_request: {key}"

    def test_key_order_matches_contract(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert list(br.keys()) == list(_PAYLOAD_KEYS)

    def test_qty_matches_intake(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert br["qty"] == _VALID_LONG["qty"]

    def test_intended_entry_price_matches_intake(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert br["intended_entry_price"] == _VALID_LONG["intended_entry_price"]


# ---------------------------------------------------------------------------
# M. No broker imports in source
# ---------------------------------------------------------------------------

_BROKER_MARKERS = (
    "broker_adapters",
    "bitvavo",
    "place_order",
    "create_order",
    "requests.post",
    "requests.get",
    "httpx",
    "urllib",
)


class TestNoBrokerImports:
    def test_source_has_no_broker_markers(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_request_builder.py")
        text = src.read_text(encoding="utf-8")
        for marker in _BROKER_MARKERS:
            assert marker not in text, (
                f"broker_request_builder.py references broker: {marker!r}"
            )


# ---------------------------------------------------------------------------
# N. No file IO markers in source
# ---------------------------------------------------------------------------

_FILE_IO_MARKERS = (
    "open(",
    "write_text",
    "read_text",
    "os.path",
    "os.makedirs",
)


class TestNoFileIO:
    def test_source_has_no_file_io(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_request_builder.py")
        text = src.read_text(encoding="utf-8")
        for marker in _FILE_IO_MARKERS:
            assert marker not in text, (
                f"broker_request_builder.py contains file IO: {marker!r}"
            )


# ---------------------------------------------------------------------------
# O. No network/http markers in source
# ---------------------------------------------------------------------------

_NETWORK_MARKERS = (
    "import requests",
    "import httpx",
    "import aiohttp",
    "http.client",
    "urllib.request",
    "socket.",
)


class TestNoNetwork:
    def test_source_has_no_network_markers(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_request_builder.py")
        text = src.read_text(encoding="utf-8")
        for marker in _NETWORK_MARKERS:
            assert marker not in text, (
                f"broker_request_builder.py references network: {marker!r}"
            )


# ---------------------------------------------------------------------------
# P. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_colony_request_ref_deterministic(self):
        # colony_request_ref (internal trace) is deterministic for the same input
        r1 = build_broker_request(dict(_VALID_LONG))
        r2 = build_broker_request(dict(_VALID_LONG))
        assert r1["broker_request"]["colony_request_ref"] == \
               r2["broker_request"]["colony_request_ref"]

    def test_client_request_id_unique_per_call(self):
        # client_request_id (UUID) is intentionally unique per call
        r1 = build_broker_request(dict(_VALID_LONG))
        r2 = build_broker_request(dict(_VALID_LONG))
        assert r1["broker_request"]["client_request_id"] != \
               r2["broker_request"]["client_request_id"]

    def test_different_order_type_different_ref(self):
        r_market = build_broker_request(_intake(order_type="market"))
        r_limit = build_broker_request(_intake(order_type="limit"))
        assert r_market["broker_request"]["colony_request_ref"] != \
               r_limit["broker_request"]["colony_request_ref"]


# ---------------------------------------------------------------------------
# Q. Extra intake fields do not appear in broker_request
# ---------------------------------------------------------------------------

class TestExtraFieldsStripped:
    def test_position_side_not_in_broker_request(self):
        # position_side is an intake field, not a payload field
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "position_side" not in br

    def test_allow_broker_execution_not_in_broker_request(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "allow_broker_execution" not in br

    def test_risk_state_not_in_broker_request(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "risk_state" not in br

    def test_freeze_new_entries_not_in_broker_request(self):
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "freeze_new_entries" not in br

    def test_ts_intake_utc_not_in_broker_request(self):
        # mapped to ts_request_utc; original key should not appear
        br = build_broker_request(_VALID_LONG)["broker_request"]
        assert "ts_intake_utc" not in br
