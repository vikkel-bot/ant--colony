"""
AC-121 tests — Execution Slippage Model (Paper-Only)

Coverage:
  1. NoSlippage returns exact price for BUY and SELL
  2. FixedSlippage raises price for BUY, lowers for SELL
  3. FixedBasisPointsSlippage raises price for BUY, lowers for SELL
  4. 0 spread / 0 bps → no change (identical to NoSlippage)
  5. Invalid side → ValueError (BUY/LONG/SELL/SHORT are valid)
  6. Invalid price (zero, negative, non-numeric) → ValueError
  7. LONG and SHORT aliases work identically to BUY and SELL
  8. Slippage is always adverse (fill_price worsens relative to raw)
  9. Integration: paper_execution_runner_lite imports NoSlippage as default
 10. Integration: _SLIPPAGE_MODEL default = NoSlippage instance
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "ant_colony"))

from execution_slippage_model import (
    FixedBasisPointsSlippage,
    FixedSlippage,
    NoSlippage,
)

_PRICE = 100.0


# ---------------------------------------------------------------------------
# 1. NoSlippage
# ---------------------------------------------------------------------------

class TestNoSlippage:
    def setup_method(self):
        self.m = NoSlippage()

    def test_buy_returns_exact_price(self):
        assert self.m.apply(_PRICE, "BUY") == _PRICE

    def test_sell_returns_exact_price(self):
        assert self.m.apply(_PRICE, "SELL") == _PRICE

    def test_long_alias_returns_exact_price(self):
        assert self.m.apply(_PRICE, "LONG") == _PRICE

    def test_short_alias_returns_exact_price(self):
        assert self.m.apply(_PRICE, "SHORT") == _PRICE

    def test_case_insensitive_buy(self):
        assert self.m.apply(_PRICE, "buy") == _PRICE

    def test_case_insensitive_sell(self):
        assert self.m.apply(_PRICE, "sell") == _PRICE

    def test_returns_float(self):
        assert isinstance(self.m.apply(_PRICE, "BUY"), float)

    def test_large_price(self):
        assert self.m.apply(50000.0, "BUY") == 50000.0

    def test_small_price(self):
        assert self.m.apply(0.0001, "BUY") == pytest.approx(0.0001)


# ---------------------------------------------------------------------------
# 2. FixedSlippage
# ---------------------------------------------------------------------------

class TestFixedSlippage:
    def test_buy_price_increases(self):
        m = FixedSlippage(spread=0.20)
        fill = m.apply(_PRICE, "BUY")
        assert fill > _PRICE

    def test_sell_price_decreases(self):
        m = FixedSlippage(spread=0.20)
        fill = m.apply(_PRICE, "SELL")
        assert fill < _PRICE

    def test_buy_half_spread_applied(self):
        m = FixedSlippage(spread=0.20)
        assert m.apply(_PRICE, "BUY") == pytest.approx(_PRICE + 0.10)

    def test_sell_half_spread_applied(self):
        m = FixedSlippage(spread=0.20)
        assert m.apply(_PRICE, "SELL") == pytest.approx(_PRICE - 0.10)

    def test_buy_sell_symmetric(self):
        m = FixedSlippage(spread=0.50)
        buy  = m.apply(_PRICE, "BUY")
        sell = m.apply(_PRICE, "SELL")
        assert pytest.approx(buy - _PRICE) == _PRICE - sell

    def test_long_alias_same_as_buy(self):
        m = FixedSlippage(spread=0.20)
        assert m.apply(_PRICE, "LONG") == m.apply(_PRICE, "BUY")

    def test_short_alias_same_as_sell(self):
        m = FixedSlippage(spread=0.20)
        assert m.apply(_PRICE, "SHORT") == m.apply(_PRICE, "SELL")

    def test_zero_spread_no_change_buy(self):
        m = FixedSlippage(spread=0.0)
        assert m.apply(_PRICE, "BUY") == _PRICE

    def test_zero_spread_no_change_sell(self):
        m = FixedSlippage(spread=0.0)
        assert m.apply(_PRICE, "SELL") == _PRICE

    def test_negative_spread_raises(self):
        with pytest.raises(ValueError):
            FixedSlippage(spread=-0.10)

    def test_returns_float(self):
        m = FixedSlippage(spread=0.20)
        assert isinstance(m.apply(_PRICE, "BUY"), float)


# ---------------------------------------------------------------------------
# 3. FixedBasisPointsSlippage
# ---------------------------------------------------------------------------

class TestFixedBasisPointsSlippage:
    def test_buy_price_increases(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        assert m.apply(_PRICE, "BUY") > _PRICE

    def test_sell_price_decreases(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        assert m.apply(_PRICE, "SELL") < _PRICE

    def test_buy_exact_value(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        # 100 * (1 + 5/10000) = 100.05
        assert m.apply(100.0, "BUY") == pytest.approx(100.05)

    def test_sell_exact_value(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        # 100 * (1 - 5/10000) = 99.95
        assert m.apply(100.0, "SELL") == pytest.approx(99.95)

    def test_10_bps_buy(self):
        m = FixedBasisPointsSlippage(bps=10.0)
        assert m.apply(1000.0, "BUY") == pytest.approx(1001.0)

    def test_10_bps_sell(self):
        m = FixedBasisPointsSlippage(bps=10.0)
        assert m.apply(1000.0, "SELL") == pytest.approx(999.0)

    def test_long_alias_same_as_buy(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        assert m.apply(_PRICE, "LONG") == m.apply(_PRICE, "BUY")

    def test_short_alias_same_as_sell(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        assert m.apply(_PRICE, "SHORT") == m.apply(_PRICE, "SELL")

    def test_zero_bps_no_change_buy(self):
        m = FixedBasisPointsSlippage(bps=0.0)
        assert m.apply(_PRICE, "BUY") == _PRICE

    def test_zero_bps_no_change_sell(self):
        m = FixedBasisPointsSlippage(bps=0.0)
        assert m.apply(_PRICE, "SELL") == _PRICE

    def test_negative_bps_raises(self):
        with pytest.raises(ValueError):
            FixedBasisPointsSlippage(bps=-1.0)

    def test_returns_float(self):
        m = FixedBasisPointsSlippage(bps=5.0)
        assert isinstance(m.apply(_PRICE, "BUY"), float)

    def test_proportional_to_price(self):
        m = FixedBasisPointsSlippage(bps=10.0)
        fill_100  = m.apply(100.0,  "BUY")
        fill_1000 = m.apply(1000.0, "BUY")
        assert fill_100  == pytest.approx(100.1)
        assert fill_1000 == pytest.approx(1001.0)


# ---------------------------------------------------------------------------
# 4. Invalid side
# ---------------------------------------------------------------------------

class TestInvalidSide:
    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_empty_side_raises(self, model):
        with pytest.raises(ValueError):
            model.apply(_PRICE, "")

    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_garbage_side_raises(self, model):
        with pytest.raises(ValueError):
            model.apply(_PRICE, "ENTER")

    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_numeric_side_raises(self, model):
        with pytest.raises(ValueError):
            model.apply(_PRICE, "1")


# ---------------------------------------------------------------------------
# 5. Invalid price
# ---------------------------------------------------------------------------

class TestInvalidPrice:
    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_zero_price_raises(self, model):
        with pytest.raises(ValueError):
            model.apply(0.0, "BUY")

    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_negative_price_raises(self, model):
        with pytest.raises(ValueError):
            model.apply(-1.0, "BUY")

    @pytest.mark.parametrize("model", [
        NoSlippage(),
        FixedSlippage(spread=0.10),
        FixedBasisPointsSlippage(bps=5.0),
    ])
    def test_none_price_raises(self, model):
        with pytest.raises((ValueError, TypeError)):
            model.apply(None, "BUY")


# ---------------------------------------------------------------------------
# 6. Adverse fill — slippage always hurts
# ---------------------------------------------------------------------------

class TestAdverseFill:
    def test_fixed_buy_fill_worse_than_raw(self):
        m = FixedSlippage(spread=0.10)
        assert m.apply(_PRICE, "BUY") > _PRICE

    def test_fixed_sell_fill_worse_than_raw(self):
        m = FixedSlippage(spread=0.10)
        assert m.apply(_PRICE, "SELL") < _PRICE

    def test_bps_buy_fill_worse_than_raw(self):
        m = FixedBasisPointsSlippage(bps=3.0)
        assert m.apply(_PRICE, "BUY") > _PRICE

    def test_bps_sell_fill_worse_than_raw(self):
        m = FixedBasisPointsSlippage(bps=3.0)
        assert m.apply(_PRICE, "SELL") < _PRICE


# ---------------------------------------------------------------------------
# 7. Integration — paper_execution_runner_lite hook
# ---------------------------------------------------------------------------

class TestIntegrationRunner:
    def test_runner_imports_no_slippage(self):
        """paper_execution_runner_lite must import NoSlippage."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        assert hasattr(runner, "_SLIPPAGE_MODEL")

    def test_default_slippage_model_is_noslippage(self):
        """Default _SLIPPAGE_MODEL must be a NoSlippage instance."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        assert isinstance(runner._SLIPPAGE_MODEL, NoSlippage)

    def test_default_model_preserves_buy_price(self):
        """With default NoSlippage, BUY fill == raw price."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        assert runner._SLIPPAGE_MODEL.apply(1234.56, "BUY") == 1234.56

    def test_default_model_preserves_sell_price(self):
        """With default NoSlippage, SELL fill == raw price."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        assert runner._SLIPPAGE_MODEL.apply(1234.56, "SELL") == 1234.56

    def test_bps_model_produces_adverse_buy(self):
        """FixedBasisPointsSlippage substituted into runner produces adverse buy fill."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        original = runner._SLIPPAGE_MODEL
        try:
            runner._SLIPPAGE_MODEL = FixedBasisPointsSlippage(bps=5.0)
            fill = runner._SLIPPAGE_MODEL.apply(1000.0, "BUY")
            assert fill == pytest.approx(1000.5)
        finally:
            runner._SLIPPAGE_MODEL = original  # always restore

    def test_bps_model_produces_adverse_sell(self):
        """FixedBasisPointsSlippage substituted into runner produces adverse sell fill."""
        import importlib
        runner = importlib.import_module("paper_execution_runner_lite")
        original = runner._SLIPPAGE_MODEL
        try:
            runner._SLIPPAGE_MODEL = FixedBasisPointsSlippage(bps=5.0)
            fill = runner._SLIPPAGE_MODEL.apply(1000.0, "SELL")
            assert fill == pytest.approx(999.5)
        finally:
            runner._SLIPPAGE_MODEL = original  # always restore
