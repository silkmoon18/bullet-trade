from concurrent.futures import ThreadPoolExecutor
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from threading import Event

import pytest

from bullet_trade.server.strategy import (
    BrokerFill,
    BrokerOrder,
    FillPriceSource,
    MarketMark,
    OrderSide,
    OrderState,
    SQLiteCapitalService,
    SQLiteFillBookingService,
    SQLiteStrategyAPI,
    SQLiteStrategyRepository,
    SQLiteValuationService,
    ValuationReadinessError,
    money_to_units,
    price_to_units,
)
from bullet_trade.server.strategy.domain import NAV_SCALE, SHANGHAI_TZ


SECURITY = "510050.XSHG"
AS_OF = datetime(2026, 8, 11, 10, 0, tzinfo=SHANGHAI_TZ)


@pytest.fixture
def ledger_services(tmp_path):
    database_path = tmp_path / "valuation.db"
    repository = SQLiteStrategyRepository(database_path)
    repository.initialize()
    repository.create_physical_account("qmt-main", "QMT", "account-1")
    capital = SQLiteCapitalService(database_path)
    capital.calibrate_broker_available_cash("qmt-main", money_to_units("20000"))
    capital.ensure_strategy_account(
        "good-etf", "good_etf", "qmt-main", money_to_units("10000")
    )
    return (
        database_path,
        repository,
        capital,
        SQLiteFillBookingService(database_path),
        SQLiteValuationService(database_path),
    )


def _order(order_id, side, quantity, trading_day=date(2026, 8, 10)):
    return BrokerOrder(
        order_id=order_id,
        account_id="good-etf",
        intent_id=None,
        client_tag="tag-{}".format(order_id),
        broker_order_id="broker-{}".format(order_id),
        security=SECURITY,
        side=side,
        requested_qty=quantity,
        filled_qty=0,
        state=OrderState.SUBMITTED,
        trading_day=trading_day,
    )


def _fill(
    fill_id,
    order_id,
    side,
    quantity,
    price,
    commission,
    tax="0",
    traded_day=date(2026, 8, 10),
):
    return BrokerFill(
        fill_id=fill_id,
        order_id=order_id,
        fingerprint="fp-{}".format(fill_id),
        broker_trade_id="trade-{}".format(fill_id),
        security=SECURITY,
        side=side,
        quantity=quantity,
        price_units=price_to_units(price),
        commission_units=money_to_units(commission),
        tax_units=money_to_units(tax),
        traded_at=datetime(
            traded_day.year, traded_day.month, traded_day.day, 10, 0,
            tzinfo=SHANGHAI_TZ,
        ),
    )


def _mark(price="2.50", as_of=AS_OF):
    return MarketMark(
        security=SECURITY,
        price_units=price_to_units(price),
        as_of=as_of,
        source="qmt",
    )


def _buy(capital, booking, quantity=1000):
    booking.register_order(_order("buy-1", OrderSide.BUY, quantity))
    capital.reserve_cash("good-etf", money_to_units("2100"), 0, "buy-1")
    return booking.book_fill(
        "good-etf",
        _fill("f-buy", "buy-1", OrderSide.BUY, quantity, "2", "5"),
        1,
        sellable_from_trade_date=date(2026, 8, 11),
    )


def test_cash_only_snapshot_has_unit_nav(ledger_services):
    _, _, _, _, valuation = ledger_services

    snapshot = valuation.create_snapshot(
        "good-etf", {}, AS_OF, max_mark_age=timedelta(minutes=1)
    )

    assert snapshot.cash_units == money_to_units("10000")
    assert snapshot.positions_value_units == 0
    assert snapshot.total_assets_units == money_to_units("10000")
    assert snapshot.total_pnl_units == 0
    assert snapshot.nav_units == NAV_SCALE
    assert snapshot.performance_ready is True
    assert snapshot.positions == ()


def test_buy_snapshot_uses_real_cash_fees_and_mark(ledger_services):
    _, _, capital, booking, valuation = ledger_services
    booked = _buy(capital, booking)

    snapshot = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark()}, AS_OF, timedelta(minutes=1)
    )

    assert snapshot.ledger_version == booked.account.ledger_version
    assert snapshot.cash_units == money_to_units("7995")
    assert snapshot.fees_units == money_to_units("5")
    assert snapshot.positions_value_units == money_to_units("2500")
    assert snapshot.total_assets_units == money_to_units("10495")
    assert snapshot.realized_pnl_units == 0
    assert snapshot.unrealized_pnl_units == money_to_units("495")
    assert snapshot.total_pnl_units == money_to_units("495")
    assert snapshot.positions[0].remaining_cost_units == money_to_units("2005")
    assert snapshot.positions[0].sellable_qty == 1000


def test_unknown_fill_fee_disables_only_performance_metrics(ledger_services):
    _, _, capital, booking, valuation = ledger_services
    booking.register_order(_order("buy-1", OrderSide.BUY, 1000))
    capital.reserve_cash("good-etf", money_to_units("2100"), 0, "buy-1")
    fill = _fill("f-buy", "buy-1", OrderSide.BUY, 1000, "2", "0")
    fill = BrokerFill(
        fill_id=fill.fill_id,
        order_id=fill.order_id,
        fingerprint=fill.fingerprint,
        broker_trade_id=fill.broker_trade_id,
        security=fill.security,
        side=fill.side,
        quantity=fill.quantity,
        price_units=fill.price_units,
        commission_units=None,
        tax_units=None,
        traded_at=fill.traded_at,
    )
    booking.book_fill(
        "good-etf", fill, 1, sellable_from_trade_date=date(2026, 8, 11)
    )

    snapshot = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark()}, AS_OF, timedelta(minutes=1)
    )

    assert snapshot.cash_units == money_to_units("8000")
    assert snapshot.fees_units == 0
    assert snapshot.fees_known is False
    assert snapshot.unknown_fee_fill_count == 1
    assert snapshot.performance_ready is False
    assert snapshot.performance_blockers == ("unknown_fill_fees",)
    payload = SQLiteStrategyAPI._snapshot_payload(snapshot)
    assert payload["fees"] is None
    assert payload["fees_known"] is False
    assert payload["nav"] is None
    assert payload["returns"] is None
    assert payload["total_pnl"] is None


@pytest.mark.parametrize("source", [FillPriceSource.ORDER_PRICE_FALLBACK, FillPriceSource.ZERO_FALLBACK])
def test_estimated_fill_price_blocks_performance(ledger_services, source):
    _, _, capital, booking, valuation = ledger_services
    booking.register_order(_order("buy-estimated", OrderSide.BUY, 1000))
    capital.reserve_cash(
        "good-etf", money_to_units("2100"), 0, "buy-estimated"
    )
    original = _fill(
        "f-buy-estimated", "buy-estimated", OrderSide.BUY, 1000, "2", "0"
    )
    estimated = BrokerFill(
        fill_id=original.fill_id,
        order_id=original.order_id,
        fingerprint=original.fingerprint,
        broker_trade_id=original.broker_trade_id,
        security=original.security,
        side=original.side,
        quantity=original.quantity,
        price_units=0 if source is FillPriceSource.ZERO_FALLBACK else original.price_units,
        commission_units=original.commission_units,
        tax_units=original.tax_units,
        traded_at=original.traded_at,
        price_source=source,
        price_known=False,
    )
    booking.book_fill(
        "good-etf",
        estimated,
        1,
        sellable_from_trade_date=date(2026, 8, 11),
    )

    snapshot = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark()}, AS_OF, timedelta(minutes=1)
    )

    assert snapshot.unknown_price_fill_count == 1
    assert snapshot.performance_ready is False
    assert snapshot.performance_blockers == ("estimated_fill_prices",)
    payload = SQLiteStrategyAPI._snapshot_payload(snapshot)
    assert payload["unknown_price_fill_count"] == 1
    assert payload["nav"] is None


def test_sell_snapshot_reconciles_realized_and_unrealized_pnl(ledger_services):
    _, _, capital, booking, valuation = ledger_services
    buy = _buy(capital, booking)
    booking.register_order(
        _order("sell-1", OrderSide.SELL, 600, date(2026, 8, 11))
    )
    booking.book_fill(
        "good-etf",
        _fill(
            "f-sell", "sell-1", OrderSide.SELL, 600,
            "2.50", "3", "1", date(2026, 8, 11),
        ),
        buy.account.ledger_version,
    )

    snapshot = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark("3")}, AS_OF, timedelta(minutes=1)
    )

    assert snapshot.cash_units == money_to_units("9491")
    assert snapshot.positions_value_units == money_to_units("1200")
    assert snapshot.total_assets_units == money_to_units("10691")
    assert snapshot.realized_pnl_units == money_to_units("293")
    assert snapshot.unrealized_pnl_units == money_to_units("398")
    assert snapshot.total_pnl_units == money_to_units("691")
    assert snapshot.fees_units == money_to_units("9")


@pytest.mark.parametrize(
    "marks, blocker",
    [
        ({}, "missing_mark"),
        (
            {SECURITY: _mark(as_of=AS_OF - timedelta(minutes=2))},
            "stale_mark",
        ),
        (
            {SECURITY: _mark(as_of=AS_OF + timedelta(seconds=1))},
            "future_mark",
        ),
    ],
)
def test_missing_stale_and_future_marks_block_snapshot(ledger_services, marks, blocker):
    _, _, capital, booking, valuation = ledger_services
    _buy(capital, booking)

    with pytest.raises(ValuationReadinessError) as caught:
        valuation.create_snapshot(
            "good-etf", marks, AS_OF, max_mark_age=timedelta(minutes=1)
        )

    assert any(item.startswith(blocker) for item in caught.value.blockers)


def test_same_ledger_and_marks_produce_same_snapshot_version(ledger_services):
    _, _, capital, booking, valuation = ledger_services
    _buy(capital, booking)
    marks = {SECURITY: _mark()}

    first = valuation.create_snapshot(
        "good-etf", marks, AS_OF, max_mark_age=timedelta(minutes=1)
    )
    second = valuation.create_snapshot(
        "good-etf", marks, AS_OF, max_mark_age=timedelta(minutes=1)
    )

    assert first == second
    assert len(first.snapshot_version) == 64


def test_mark_mapping_is_captured_once_before_validation_and_valuation(
    ledger_services,
):
    _, _, capital, booking, valuation = ledger_services
    _buy(capital, booking)
    current = _mark("2.50")
    future = _mark("9.99", AS_OF + timedelta(days=1))

    class FlippingMarks(Mapping):
        def __init__(self):
            self.reads = 0

        def __getitem__(self, key):
            assert key == SECURITY
            self.reads += 1
            return current if self.reads == 1 else future

        def __iter__(self):
            return iter((SECURITY,))

        def __len__(self):
            return 1

    marks = FlippingMarks()
    snapshot = valuation.create_snapshot(
        "good-etf", marks, AS_OF, max_mark_age=timedelta(minutes=1)
    )

    assert marks.reads == 1
    assert snapshot.positions[0].mark_price_units == current.price_units


def test_non_divisible_fill_fee_is_preserved_without_cost_rounding_loss(
    ledger_services,
):
    _, _, capital, booking, valuation = ledger_services
    booking.register_order(_order("buy-1", OrderSide.BUY, 3))
    capital.reserve_cash("good-etf", money_to_units("4"), 0, "buy-1")
    buy = booking.book_fill(
        "good-etf",
        _fill("f-buy", "buy-1", OrderSide.BUY, 3, "1", "0.0001"),
        1,
        sellable_from_trade_date=date(2026, 8, 11),
    )
    before_sell = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark("1")}, AS_OF, timedelta(minutes=1)
    )
    assert before_sell.positions[0].remaining_cost_units == money_to_units("3.0001")
    assert before_sell.total_pnl_units == money_to_units("-0.0001")

    booking.register_order(
        _order("sell-1", OrderSide.SELL, 1, trading_day=date(2026, 8, 11))
    )
    sold = booking.book_fill(
        "good-etf",
        _fill(
            "f-sell", "sell-1", OrderSide.SELL, 1,
            "1", "0", traded_day=date(2026, 8, 11),
        ),
        buy.account.ledger_version,
    )
    after_sell = valuation.create_snapshot(
        "good-etf", {SECURITY: _mark("1")}, AS_OF, timedelta(minutes=1)
    )

    assert sold.realized_pnl_units == 0
    assert after_sell.positions[0].remaining_cost_units == money_to_units("2.0001")
    assert after_sell.unrealized_pnl_units == money_to_units("-0.0001")
    assert after_sell.total_pnl_units == money_to_units("-0.0001")


def test_post_initial_capital_flow_disables_performance_nav_readiness(ledger_services):
    _, _, capital, _, valuation = ledger_services
    capital.adjust_capital(
        "good-etf",
        "ALLOCATE",
        money_to_units("1000"),
        expected_ledger_version=0,
        external_ref="flow-1",
        reason="increase capital",
    )

    snapshot = valuation.create_snapshot(
        "good-etf", {}, AS_OF, max_mark_age=timedelta(minutes=1)
    )

    assert snapshot.total_assets_units == money_to_units("11000")
    assert snapshot.net_capital_units == money_to_units("11000")
    assert snapshot.total_pnl_units == 0
    assert snapshot.performance_ready is False


def test_snapshot_does_not_mix_account_before_fill_with_position_after_fill(
    ledger_services,
):
    database_path, _, capital, booking, _ = ledger_services
    booking.register_order(_order("buy-1", OrderSide.BUY, 1000))
    account_read = Event()
    allow_positions = Event()

    class PausingValuation(SQLiteValuationService):
        def _after_account_read(self, account):
            account_read.set()
            assert allow_positions.wait(timeout=5)

    valuation = PausingValuation(database_path)
    with ThreadPoolExecutor(max_workers=2) as executor:
        snapshot_future = executor.submit(
            valuation.create_snapshot,
            "good-etf",
            {SECURITY: _mark()},
            AS_OF,
            timedelta(minutes=1),
        )
        assert account_read.wait(timeout=5)
        capital.reserve_cash("good-etf", money_to_units("2100"), 0, "buy-1")
        booking.book_fill(
            "good-etf",
            _fill("f-buy", "buy-1", OrderSide.BUY, 1000, "2", "5"),
            1,
            sellable_from_trade_date=date(2026, 8, 11),
        )
        allow_positions.set()
        snapshot = snapshot_future.result(timeout=5)

    assert snapshot.ledger_version == 0
    assert snapshot.cash_units == money_to_units("10000")
    assert snapshot.positions == ()
