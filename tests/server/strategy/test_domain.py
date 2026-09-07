from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from bullet_trade.server.strategy.domain import (
    MONEY_SCALE,
    BrokerFill,
    FillPriceSource,
    OrderSide,
    PortfolioIntent,
    Position,
    PositionLot,
    ReconciliationResult,
    ReconciliationState,
    SHANGHAI_TZ,
    StrategyEvent,
    StrategyAccount,
    money_to_units,
    price_to_units,
    units_to_decimal,
)


def test_scaled_values_are_deterministic_and_reject_float():
    assert money_to_units("10000") == 100_000_000
    assert money_to_units(Decimal("1.23456")) == 12_346
    assert price_to_units("3.1415926") == 3_141_593
    assert units_to_decimal(12_346, MONEY_SCALE) == Decimal("1.2346")
    with pytest.raises(TypeError):
        money_to_units(0.1)


def test_strategy_account_exposes_available_cash_and_checks_reserve():
    account = StrategyAccount(
        account_id="good-etf",
        strategy_id="good_etf",
        physical_account_id="qmt-main",
        initial_capital_units=100_000_000,
        cash_units=80_000_000,
        reserved_cash_units=30_000_000,
        ledger_version=2,
        event_seq=4,
    )
    assert account.available_cash_units == 50_000_000
    with pytest.raises(ValueError, match="cannot exceed"):
        StrategyAccount(
            account_id="bad",
            strategy_id="bad",
            physical_account_id="qmt-main",
            initial_capital_units=1,
            cash_units=1,
            reserved_cash_units=2,
            ledger_version=0,
            event_seq=0,
        )


def test_position_and_fill_invariants():
    with pytest.raises(ValueError, match="sellable_qty"):
        Position("good-etf", "510300.XSHG", 100, 200, 1_000_000)
    fill = BrokerFill(
        fill_id="fill-1",
        order_id="order-1",
        fingerprint="fp-1",
        security="510300.XSHG",
        side=OrderSide.BUY,
        quantity=100,
        price_units=3_500_000,
        commission_units=0,
        tax_units=0,
        traded_at=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
    )
    assert fill.traded_at.tzinfo == SHANGHAI_TZ
    assert fill.traded_at.hour == 9
    zero = replace(fill, price_units=0, price_source=FillPriceSource.ZERO_FALLBACK, price_known=False)
    assert zero.price_units == 0
    for changes in (
        {"price_units": 0},
        {"price_units": -1},
        {"price_source": FillPriceSource.ZERO_FALLBACK, "price_known": False},
        {"price_units": 0, "price_source": FillPriceSource.ZERO_FALLBACK, "price_known": True},
    ):
        with pytest.raises(ValueError):
            replace(fill, **changes)
    with pytest.raises(ValueError, match="quantity"):
        BrokerFill(
            fill_id="fill-1",
            order_id="order-1",
            fingerprint="fp-1",
            security="510300.XSHG",
            side=OrderSide.BUY,
            quantity=0,
            price_units=3_500_000,
            commission_units=0,
            tax_units=0,
            traded_at=datetime.now(timezone.utc),
        )


def test_position_lot_allows_t0_and_rejects_pre_acquisition_sellable_date():
    acquired = date(2026, 8, 10)
    lot = PositionLot(
        lot_id="lot-1",
        account_id="good-etf",
        security="510300.XSHG",
        acquired_trade_date=acquired,
        sellable_from_trade_date=acquired + timedelta(days=1),
        original_qty=100,
        remaining_qty=100,
        cost_price_units=3_500_000,
    )
    assert lot.sellable_from_trade_date > lot.acquired_trade_date
    t0_lot = PositionLot(
        lot_id="lot-2",
        account_id="good-etf",
        security="518880.XSHG",
        acquired_trade_date=acquired,
        sellable_from_trade_date=acquired,
        original_qty=100,
        remaining_qty=100,
        cost_price_units=3_500_000,
    )
    assert t0_lot.sellable_from_trade_date == acquired
    with pytest.raises(ValueError, match="sellable_from_trade_date"):
        PositionLot(
            lot_id="lot-3",
            account_id="good-etf",
            security="510300.XSHG",
            acquired_trade_date=acquired,
            sellable_from_trade_date=acquired - timedelta(days=1),
            original_qty=100,
            remaining_qty=100,
            cost_price_units=3_500_000,
        )


def test_intent_and_event_payloads_snapshot_mutable_input():
    targets = {"510300.XSHG": 100}
    intent = PortfolioIntent("intent-1", "good-etf", "rebalance-1", 0, targets)
    targets["510300.XSHG"] = -1
    assert intent.targets["510300.XSHG"] == 100
    with pytest.raises(TypeError):
        intent.targets["510300.XSHG"] = 200

    payload = {"fills": [{"quantity": 100}]}
    event = StrategyEvent(
        "good-etf",
        1,
        "FILL_BOOKED",
        payload,
        datetime.now(timezone.utc),
    )
    payload["fills"][0]["quantity"] = -1
    assert event.payload["fills"][0]["quantity"] == 100

    details = {"differences": ["cash"]}
    result = ReconciliationResult(
        "reconcile-1",
        "qmt-main",
        ReconciliationState.BLOCKED,
        details,
    )
    details["differences"].append("position")
    assert result.details["differences"] == ("cash",)
