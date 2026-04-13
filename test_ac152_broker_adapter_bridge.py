"""
AC-152: Tests for Broker Adapter Bridge (dry only)

Verifies:
  A. Valid long intake  → ok=True, adapter_command present
  B. Valid short intake → ok=True, side=sell
  C. adapter == "bitvavo"
  D. mode == "dry"
  E. request_type == "place_order"
  F. Payload correctly populated from intake
  G. Invalid intake → fail-closed
  H. Invalid market → fail-closed via intake layer
  I. Invalid strategy_key → fail-closed
  J. qty <= 0 → fail-closed
  K. intended_entry_price <= 0 → fail-closed
  L. Notional breach → fail-closed
  M. client_request_id preserved exactly
  N. ts_command_utc is deterministic and matches request ts
  O. Builder never raises exceptions
  P. No broker imports in source
  Q. No network/http markers in source
  R. No file IO markers in source
  S. Determinism: same input → same output
  T. adapter_command top-level structure correct
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.broker_adapter_bridge import build_broker_adapter_command

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


def _cmd(intake: dict | None = None, **overrides) -> dict:
    return build_broker_adapter_command(intake or _intake(**overrides))


def _ac(intake: dict | None = None, **overrides) -> dict:
    return _cmd(intake, **overrides)["adapter_command"]


# ---------------------------------------------------------------------------
# A. Valid long intake → ok=True
# ---------------------------------------------------------------------------

class TestValidLong:
    def test_ok_true(self):
        assert _cmd()["ok"] is True

    def test_reason_correct(self):
        assert _cmd()["reason"] == "BROKER_ADAPTER_COMMAND_READY"

    def test_adapter_command_present(self):
        result = _cmd()
        assert result["adapter_command"] is not None
        assert isinstance(result["adapter_command"], dict)


# ---------------------------------------------------------------------------
# B. Valid short intake
# ---------------------------------------------------------------------------

class TestValidShort:
    def test_short_ok_true(self):
        assert build_broker_adapter_command(_VALID_SHORT)["ok"] is True

    def test_short_side_sell(self):
        assert _ac(_VALID_SHORT)["payload"]["side"] == "sell"


# ---------------------------------------------------------------------------
# C. adapter == "bitvavo"
# ---------------------------------------------------------------------------

class TestAdapterName:
    def test_adapter_is_bitvavo(self):
        assert _ac()["adapter"] == "bitvavo"


# ---------------------------------------------------------------------------
# D. mode == "dry"
# ---------------------------------------------------------------------------

class TestMode:
    def test_mode_is_dry(self):
        assert _ac()["mode"] == "dry"

    def test_mode_not_live(self):
        assert _ac()["mode"] != "live"


# ---------------------------------------------------------------------------
# E. request_type == "place_order"
# ---------------------------------------------------------------------------

class TestRequestType:
    def test_request_type_correct(self):
        assert _ac()["request_type"] == "place_order"


# ---------------------------------------------------------------------------
# F. Payload correctly populated
# ---------------------------------------------------------------------------

class TestPayloadPopulated:
    def test_market_in_payload(self):
        assert _ac()["payload"]["market"] == "BNB-EUR"

    def test_side_buy_for_long(self):
        assert _ac()["payload"]["side"] == "buy"

    def test_order_type_in_payload(self):
        assert _ac()["payload"]["order_type"] == "market"

    def test_qty_in_payload(self):
        assert _ac()["payload"]["qty"] == _VALID_LONG["qty"]

    def test_intended_entry_price_in_payload(self):
        assert _ac()["payload"]["intended_entry_price"] == _VALID_LONG["intended_entry_price"]

    def test_max_notional_eur_in_payload(self):
        assert _ac()["payload"]["max_notional_eur"] == _VALID_LONG["max_notional_eur"]

    def test_strategy_key_in_payload(self):
        assert _ac()["payload"]["strategy_key"] == "EDGE3"

    def test_operator_approved_in_payload(self):
        assert _ac()["payload"]["operator_approved"] is True

    def test_limit_order_type_allowed(self):
        result = _cmd(order_type="limit")
        assert result["ok"] is True
        assert result["adapter_command"]["payload"]["order_type"] == "limit"

    def test_operator_approved_false_preserved(self):
        assert _ac(operator_approved=False)["payload"]["operator_approved"] is False


# ---------------------------------------------------------------------------
# G. Invalid intake → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidIntake:
    def test_missing_field_blocked(self):
        r = dict(_VALID_LONG)
        del r["qty"]
        result = build_broker_adapter_command(r)
        assert result["ok"] is False
        assert result["adapter_command"] is None

    def test_empty_dict_blocked(self):
        result = build_broker_adapter_command({})
        assert result["ok"] is False

    def test_reason_mentions_build(self):
        result = build_broker_adapter_command({})
        assert "broker request" in result["reason"].lower()


# ---------------------------------------------------------------------------
# H. Invalid market → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        assert _cmd(market="BTC-EUR")["ok"] is False

    def test_empty_market_blocked(self):
        assert _cmd(market="")["ok"] is False


# ---------------------------------------------------------------------------
# I. Invalid strategy_key → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        assert _cmd(strategy_key="RSI_SIMPLE")["ok"] is False


# ---------------------------------------------------------------------------
# J. qty <= 0 → fail-closed
# ---------------------------------------------------------------------------

class TestQtyBounds:
    def test_zero_qty_blocked(self):
        assert _cmd(qty=0)["ok"] is False

    def test_negative_qty_blocked(self):
        assert _cmd(qty=-1.0)["ok"] is False

    def test_bool_qty_blocked(self):
        assert _cmd(qty=True)["ok"] is False


# ---------------------------------------------------------------------------
# K. intended_entry_price <= 0 → fail-closed
# ---------------------------------------------------------------------------

class TestPriceBounds:
    def test_zero_price_blocked(self):
        assert _cmd(intended_entry_price=0)["ok"] is False

    def test_negative_price_blocked(self):
        assert _cmd(intended_entry_price=-50.0)["ok"] is False


# ---------------------------------------------------------------------------
# L. Notional breach → fail-closed
# ---------------------------------------------------------------------------

class TestNotionalBreach:
    def test_breach_blocked(self):
        # 0.09 * 600 = 54 > 50
        assert _cmd(qty=0.09, intended_entry_price=600.0, max_notional_eur=50)["ok"] is False

    def test_at_limit_allowed(self):
        assert _cmd(qty=0.08333, intended_entry_price=600.0, max_notional_eur=50)["ok"] is True


# ---------------------------------------------------------------------------
# M. client_request_id preserved exactly
# ---------------------------------------------------------------------------

class TestClientRequestId:
    def test_client_request_id_present(self):
        ac = _ac()
        assert "client_request_id" in ac
        assert isinstance(ac["client_request_id"], str)
        assert ac["client_request_id"].strip() != ""

    def test_client_request_id_is_valid_uuid(self):
        import uuid
        ac = _ac()
        crid = ac["client_request_id"]
        parsed = uuid.UUID(crid, version=4)
        assert str(parsed) == crid

    def test_client_request_id_unique_per_call(self):
        # UUID is intentionally unique per call
        r1 = build_broker_adapter_command(dict(_VALID_LONG))
        r2 = build_broker_adapter_command(dict(_VALID_LONG))
        assert r1["adapter_command"]["client_request_id"] != \
               r2["adapter_command"]["client_request_id"]


# ---------------------------------------------------------------------------
# N. ts_command_utc deterministic and matches intake ts
# ---------------------------------------------------------------------------

class TestTsCommandUtc:
    def test_ts_command_utc_present(self):
        ac = _ac()
        assert "ts_command_utc" in ac
        assert ac["ts_command_utc"].strip() != ""

    def test_ts_command_utc_matches_intake_ts(self):
        ac = _ac()
        assert ac["ts_command_utc"] == _VALID_LONG["ts_intake_utc"]

    def test_ts_command_utc_deterministic(self):
        r1 = build_broker_adapter_command(dict(_VALID_LONG))
        r2 = build_broker_adapter_command(dict(_VALID_LONG))
        assert r1["adapter_command"]["ts_command_utc"] == \
               r2["adapter_command"]["ts_command_utc"]


# ---------------------------------------------------------------------------
# O. Builder never raises exceptions
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_input", [
        None, 42, "string", [], True, {},
    ])
    def test_no_exception_on_bad_input(self, bad_input):
        result = build_broker_adapter_command(bad_input)
        assert isinstance(result, dict)
        assert "ok" in result

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0):
            result = build_broker_adapter_command(v)
            assert isinstance(result, dict)

    def test_failed_result_adapter_command_none(self):
        assert build_broker_adapter_command(None)["adapter_command"] is None


# ---------------------------------------------------------------------------
# P. No broker imports in source
# ---------------------------------------------------------------------------

_BROKER_MARKERS = (
    "broker_adapters",
    "bitvavo_adapter",
    "place_order(",
    "create_order(",
    "requests.post",
    "requests.get",
    "httpx",
    "urllib.request",
)


class TestNoBrokerImports:
    def test_source_has_no_broker_markers(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_adapter_bridge.py")
        text = src.read_text(encoding="utf-8")
        for marker in _BROKER_MARKERS:
            assert marker not in text, (
                f"broker_adapter_bridge.py references broker: {marker!r}"
            )


# ---------------------------------------------------------------------------
# Q. No network/http markers
# ---------------------------------------------------------------------------

_NETWORK_MARKERS = (
    "import requests",
    "import httpx",
    "import aiohttp",
    "http.client",
    "socket.",
)


class TestNoNetwork:
    def test_source_has_no_network_markers(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_adapter_bridge.py")
        text = src.read_text(encoding="utf-8")
        for marker in _NETWORK_MARKERS:
            assert marker not in text, (
                f"broker_adapter_bridge.py references network: {marker!r}"
            )


# ---------------------------------------------------------------------------
# R. No file IO markers
# ---------------------------------------------------------------------------

_FILE_IO_MARKERS = ("open(", "write_text", "read_text", "os.path", "os.makedirs")


class TestNoFileIO:
    def test_source_has_no_file_io(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "broker_adapter_bridge.py")
        text = src.read_text(encoding="utf-8")
        for marker in _FILE_IO_MARKERS:
            assert marker not in text, (
                f"broker_adapter_bridge.py contains file IO: {marker!r}"
            )


# ---------------------------------------------------------------------------
# S. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_input_stable_fields_match(self):
        # All fields except client_request_id (UUID) are deterministic
        r1 = build_broker_adapter_command(dict(_VALID_LONG))
        r2 = build_broker_adapter_command(dict(_VALID_LONG))
        ac1 = {k: v for k, v in r1["adapter_command"].items() if k != "client_request_id"}
        ac2 = {k: v for k, v in r2["adapter_command"].items() if k != "client_request_id"}
        assert ac1 == ac2

    def test_different_side_different_payload(self):
        long_r = build_broker_adapter_command(_VALID_LONG)
        short_r = build_broker_adapter_command(_VALID_SHORT)
        assert long_r["adapter_command"]["payload"]["side"] != \
               short_r["adapter_command"]["payload"]["side"]


# ---------------------------------------------------------------------------
# T. adapter_command top-level structure
# ---------------------------------------------------------------------------

class TestAdapterCommandStructure:
    _TOP_LEVEL_KEYS = ("adapter", "mode", "request_type", "payload",
                       "client_request_id", "ts_command_utc")
    _PAYLOAD_KEYS = ("market", "side", "order_type", "qty",
                     "intended_entry_price", "max_notional_eur",
                     "strategy_key", "operator_approved")

    def test_top_level_keys_present(self):
        ac = _ac()
        for key in self._TOP_LEVEL_KEYS:
            assert key in ac, f"missing top-level key: {key}"

    def test_payload_keys_present(self):
        payload = _ac()["payload"]
        for key in self._PAYLOAD_KEYS:
            assert key in payload, f"missing payload key: {key}"

    def test_intake_fields_not_leaked_to_top_level(self):
        ac = _ac()
        for field in ("position_side", "allow_broker_execution",
                      "risk_state", "freeze_new_entries", "ts_intake_utc"):
            assert field not in ac, f"intake field leaked to top-level: {field}"
