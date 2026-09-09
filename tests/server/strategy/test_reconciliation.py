import asyncio
from dataclasses import replace
from datetime import date, datetime

import pytest

from bullet_trade.server.strategy import (
    BrokerAccountSnapshot,
    BrokerCapabilityProfile,
    BrokerOrder,
    BrokerPositionSnapshot,
    CapabilityState,
    FillPriceSource,
    OrderSide,
    OrderState,
    ReconciliationState,
    SQLiteCapitalService,
    SQLiteFillBookingService,
    SQLiteReconciliationService,
    SQLiteStrategyRepository,
    UnpricedFillPolicy,
    collect_async_broker_snapshot,
    money_to_units,
    price_to_units,
)
from bullet_trade.server.strategy.domain import AccountStatus, SHANGHAI_TZ
from bullet_trade.server.strategy.broker_history import (
    SQLiteBrokerHistoryStore,
    merge_broker_rows,
)
from bullet_trade.server.strategy.schema import connect_database


ACCOUNT_ID = "good-etf"
PHYSICAL_ID = "qmt-main"
SECURITY = "510050.XSHG"


def _capabilities():
    supported = CapabilityState.SUPPORTED
    return BrokerCapabilityProfile(
        adapter_kind="TEST_QMT",
        client_tag_roundtrip=supported,
        stable_order_id=supported,
        stable_trade_id=supported,
        trade_order_link=supported,
        direct_trade_side=supported,
        order_side_for_trade=supported,
        fee_fields=supported,
        order_status=supported,
        current_orders_query=supported,
        current_trades_query=supported,
        working_orders_query=supported,
        order_lookback_days=1,
        trade_lookback_days=1,
    )


def _services(tmp_path, unpriced_fill_policy=UnpricedFillPolicy.STRICT):
    database = tmp_path / "reconciliation.db"
    repository = SQLiteStrategyRepository(database)
    repository.initialize()
    repository.create_physical_account(PHYSICAL_ID, "QMT", "account-1")
    capital = SQLiteCapitalService(database)
    capital.calibrate_broker_available_cash(PHYSICAL_ID, money_to_units("20000"))
    capital.ensure_strategy_account(
        ACCOUNT_ID,
        "good_etf",
        PHYSICAL_ID,
        money_to_units("10000"),
    )
    reconciliation = SQLiteReconciliationService(
        database,
        _capabilities(),
        unpriced_fill_policy=unpriced_fill_policy,
    )
    return database, repository, capital, reconciliation


def _snapshot(cash, positions=(), orders=(), trades=(), day=date(2026, 8, 11)):
    return BrokerAccountSnapshot(
        available_cash_units=money_to_units(cash),
        positions=tuple(positions),
        orders=tuple(orders),
        trades=tuple(trades),
        as_of=datetime(day.year, day.month, day.day, 15, 0, tzinfo=SHANGHAI_TZ),
    )


def _order(order_id="buy-1", broker_order_id="broker-buy-1"):
    return BrokerOrder(
        order_id=order_id,
        account_id=ACCOUNT_ID,
        intent_id=None,
        client_tag="bt:test:{}".format(order_id),
        security=SECURITY,
        side=OrderSide.BUY,
        requested_qty=1000,
        filled_qty=0,
        state=OrderState.SUBMITTED,
        trading_day=date(2026, 8, 11),
        broker_order_id=broker_order_id,
        limit_price_units=price_to_units("2"),
    )


def _broker_order(status="filled"):
    return {
        "order_id": "broker-buy-1",
        "security": SECURITY,
        "status": status,
        "is_buy": True,
    }


def _broker_trade():
    return {
        "trade_id": "trade-1",
        "trade_id_source": "broker",
        "order_id": "broker-buy-1",
        "security": SECURITY,
        "side": "BUY",
        "amount": 1000,
        "price": 2.0,
        "commission_fee": 5.0,
        "commission_known": True,
        "tax": 0.0,
        "tax_known": True,
        "time": "2026-08-11 10:00:00",
    }


def test_matching_empty_account_is_ready(tmp_path):
    _, repository, _, reconciliation = _services(tmp_path)

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000"),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["blockers"] == ()
    assert repository.get_strategy_account(ACCOUNT_ID).status is AccountStatus.ACTIVE
    assert reconciliation.latest(PHYSICAL_ID) == result


def test_latest_reconciliation_is_scoped_to_strategy_account(tmp_path):
    _, _, capital, reconciliation = _services(tmp_path)
    capital.ensure_strategy_account(
        "other-strategy",
        "other_strategy",
        PHYSICAL_ID,
        money_to_units("1000"),
    )
    blocked = reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, _snapshot("0")
    )
    ready = reconciliation.synchronize(
        "other-strategy", PHYSICAL_ID, _snapshot("20000")
    )

    assert blocked.state is ReconciliationState.BLOCKED
    assert ready.state is ReconciliationState.READY
    assert reconciliation.latest(PHYSICAL_ID) == ready
    assert reconciliation.latest(PHYSICAL_ID, ACCOUNT_ID) == blocked
    assert reconciliation.latest(PHYSICAL_ID, "other-strategy") == ready


def test_known_fill_is_booked_then_reconciled_and_replay_is_noop(tmp_path):
    database, repository, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    snapshot = _snapshot(
        "17995",
        positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
        orders=(_broker_order(),),
        trades=(_broker_trade(),),
    )

    first = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
    second = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
    replay_without_broker_order = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            trades=(_broker_trade(),),
        ),
    )

    assert first.state is ReconciliationState.READY
    assert first.details["booked_trade_ids"] == ("trade-1",)
    assert second.state is ReconciliationState.READY
    assert second.details["booked_trade_ids"] == ()
    assert replay_without_broker_order.state is ReconciliationState.READY
    assert replay_without_broker_order.details["booked_trade_ids"] == ()
    account = repository.get_strategy_account(ACCOUNT_ID)
    assert account.cash_units == money_to_units("7995")


def test_t0_fund_fill_is_booked_as_immediately_sellable(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    snapshot = _snapshot(
        "17995",
        positions=(BrokerPositionSnapshot(SECURITY, 1000, 1000),),
        orders=(_broker_order(),),
        trades=(_broker_trade(),),
    )

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        snapshot,
        settlement_cycles={SECURITY: 0},
    )

    connection = connect_database(database)
    try:
        position = connection.execute(
            "SELECT total_qty, sellable_qty FROM positions "
            "WHERE strategy_account_id = ? AND security = ?",
            (ACCOUNT_ID, SECURITY),
        ).fetchone()
    finally:
        connection.close()
    assert result.state is ReconciliationState.READY
    assert tuple(position) == (1000, 1000)


def test_unpriced_completed_sell_stays_unbooked_until_valid_price_arrives(tmp_path):
    database, repository, capital, reconciliation = _services(
        tmp_path, UnpricedFillPolicy.CONSERVATIVE_ORDER_PRICE
    )
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, _snapshot(
        "17995", positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
        orders=(_broker_order(),), trades=(_broker_trade(),),
    ))
    sell = replace(
        _order("sell-zero", "broker-sell-zero"), side=OrderSide.SELL,
        trading_day=date(2026, 8, 12), limit_price_units=None,
    )
    booking.register_order(sell)
    broker_order = dict(
        _broker_order(), order_id=sell.broker_order_id, side="SELL", is_buy=False,
        order_time="2026-08-12 09:30:00", amount=1000, filled=1000,
        order_price=0, broker_price=0, price=0,
    )
    trade = dict(
        _broker_trade(), trade_id="sell-zero-trade", order_id=sell.broker_order_id,
        side="SELL", price=0, traded_price=0, deal_balance=0,
        time="2026-08-12 09:30:01",
    )
    for _ in range(2):
        blocked = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, _snapshot(
            "20090", orders=(broker_order,), trades=(trade,), day=date(2026, 8, 12),
        ))
        assert blocked.state is ReconciliationState.BLOCKED
        assert any(
            "成交待核实" in blocker and SECURITY in blocker
            for blocker in blocked.details["blockers"]
        )
        assert repository.get_strategy_account(ACCOUNT_ID).cash_units == money_to_units("7995")
    db = connect_database(database)
    try:
        assert db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
        assert db.execute("SELECT total_qty FROM positions").fetchone()[0] == 1000
    finally:
        db.close()

    # Only a genuine later fill price permits the existing trade to be booked.
    trade["price"] = 2.1
    for _ in range(2):
        ready = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, _snapshot(
            "20090", orders=(broker_order,), trades=(trade,), day=date(2026, 8, 12),
        ))
        assert ready.state is ReconciliationState.READY
        assert repository.get_strategy_account(ACCOUNT_ID).cash_units == money_to_units("10090")
    db = connect_database(database)
    try:
        assert db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 2
        assert db.execute("SELECT total_qty FROM positions").fetchone()[0] == 0
    finally:
        db.close()


def test_qmt_order_and_trade_ids_can_be_reused_on_a_later_trading_day(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    first = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),),
            trades=(_broker_trade(),),
        ),
    )
    assert first.state is ReconciliationState.READY

    sell = replace(
        _order("sell-2", "broker-buy-1"),
        client_tag="bt:test:sell-2",
        broker_order_id=None,
        side=OrderSide.SELL,
        state=OrderState.PENDING_SUBMIT,
        trading_day=date(2026, 8, 12),
    )
    booking.register_order(sell)
    broker_order = dict(
        _broker_order(),
        side="SELL",
        is_buy=False,
        order_remark=sell.client_tag,
        order_time="2026-08-12 09:30:00",
    )
    broker_trade = dict(
        _broker_trade(),
        side="SELL",
        price=2.1,
        commission_fee=5.0,
        time="2026-08-12 09:30:01",
    )

    second = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "20090",
            orders=(broker_order,),
            trades=(broker_trade,),
            day=date(2026, 8, 12),
        ),
    )

    assert second.state is ReconciliationState.READY
    assert second.details["adopted_order_ids"] == (sell.order_id,)
    connection = connect_database(database)
    try:
        assert connection.execute(
            "SELECT total_qty FROM positions WHERE strategy_account_id = ? "
            "AND security = ?",
            (ACCOUNT_ID, SECURITY),
        ).fetchone()[0] == 0
        fills = connection.execute(
            "SELECT traded_at, broker_trade_id FROM fills ORDER BY traded_at"
        ).fetchall()
    finally:
        connection.close()
    assert [(row["traded_at"][:10], row["broker_trade_id"]) for row in fills] == [
        ("2026-08-11", "trade-1"),
        ("2026-08-12", "trade-1"),
    ]


@pytest.mark.parametrize("side", [OrderSide.BUY, OrderSide.SELL])
@pytest.mark.parametrize("known_fee", [False, True])
def test_zero_fallback_books_quantity_and_only_known_fees_once(tmp_path, side, known_fee):
    database, repository, capital, reconciliation = _services(tmp_path, UnpricedFillPolicy.ZERO_FALLBACK)
    booking = SQLiteFillBookingService(database)
    expected_cash = money_to_units("10000")
    if side is OrderSide.SELL:
        booking.register_order(_order())
        capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
        assert reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, _snapshot(
            "17995", positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),), trades=(_broker_trade(),),
        )).state is ReconciliationState.READY
        expected_cash = money_to_units("7995")
    local = replace(_order("zero-order", "broker-zero"), side=side,
                    trading_day=date(2026, 8, 12), limit_price_units=None)
    booking.register_order(local)
    if side is OrderSide.BUY:
        capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, local.order_id)
    order = dict(_broker_order(), order_id=local.broker_order_id, side=side.value,
                 is_buy=side is OrderSide.BUY, order_time="2026-08-12 09:30:00",
                 amount=1000, filled=1000, order_price=0)
    trade = dict(_broker_trade(), trade_id="zero-trade", order_id=local.broker_order_id,
                 side=side.value, price=0, deal_balance=0, time="2026-08-12 09:30:01",
                 commission_known=known_fee, tax_known=known_fee)
    expected_cash -= money_to_units("5") if known_fee else 0
    positions = (BrokerPositionSnapshot(SECURITY, 1000, 0),) if side is OrderSide.BUY else ()
    snapshot = _snapshot("20000", positions=positions, orders=(order,), trades=(trade,), day=date(2026, 8, 12))
    for attempt in range(2):
        result = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
        assert result.state is ReconciliationState.READY
        assert result.details["booked_trade_ids"] == (("zero-trade",) if attempt == 0 else ())
        account = repository.get_strategy_account(ACCOUNT_ID)
        assert account.cash_units == expected_cash
        assert account.reserved_cash_units == 0
        assert repository.replay_account(ACCOUNT_ID) == account
    db = connect_database(database)
    try:
        row = db.execute("SELECT price_units, price_source, price_known, commission_known, tax_known FROM fills WHERE broker_trade_id = 'zero-trade'").fetchone()
        assert tuple(row) == (0, "ZERO_FALLBACK", 0, int(known_fee), int(known_fee))
        assert db.execute("SELECT total_qty FROM positions").fetchone()[0] == (1000 if side is OrderSide.BUY else 0)
    finally:
        db.close()
    # Zero booking does not silently replace earlier ledger entries if the broker
    # later supplies a different price. An explicit correction is still required.
    trade["price"] = 2.1
    changed = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
    assert changed.state is ReconciliationState.BLOCKED
    assert repository.get_strategy_account(ACCOUNT_ID).cash_units == expected_cash


def test_zero_order_id_trade_is_relinked_by_strategy_client_tag(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    trade = dict(_broker_trade())
    trade.update({"order_id": "0", "order_remark": "bt:test:buy-1"})

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),),
            trades=(trade,),
        ),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["booked_trade_ids"] == ("trade-1",)


@pytest.mark.parametrize("changed_field,changed_value", [
    ("amount", 999), ("side", "SELL"), ("price", 2.1),
])
def test_reused_id_and_wrong_new_remark_do_not_rebind_an_existing_fill(
    tmp_path, changed_field, changed_value,
):
    database, repository, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    snapshot = _snapshot("17995", positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
                         orders=(_broker_order(),), trades=(_broker_trade(),))
    assert reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot).state is ReconciliationState.READY
    original_account = repository.get_strategy_account(ACCOUNT_ID)
    newer = replace(_order("rejected-new", "broker-buy-1"),
                    security="159322.XSHE", state=OrderState.REJECTED)
    booking.register_order(newer)
    snapshot.orders[0]["order_remark"] = newer.client_tag
    snapshot.trades[0]["order_remark"] = newer.client_tag
    for _ in range(2):
        result = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
        assert result.state is ReconciliationState.READY
        assert result.details["booked_trade_ids"] == ()
        assert repository.get_strategy_account(ACCOUNT_ID) == original_account
    db = connect_database(database)
    try:
        assert db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
        assert tuple(db.execute("SELECT state, filled_qty FROM strategy_orders WHERE order_id=?", (newer.order_id,)).fetchone()) == ("REJECTED", 0)
    finally:
        db.close()
    # A saved link does not excuse changed evidence, even without broker orders.
    snapshot.trades[0][changed_field] = changed_value
    replay = replace(snapshot, orders=())
    result = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, replay)
    assert result.state is ReconciliationState.BLOCKED
    assert any("trade_error:" in item for item in result.details["blockers"])
    assert repository.get_strategy_account(ACCOUNT_ID).cash_units == original_account.cash_units


@pytest.mark.parametrize("link", ["tag", "sysid", "ambiguous"])
def test_same_day_same_symbol_same_id_orders_require_unique_fill_link(tmp_path, link):
    database, repository, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    orders, trades = [], []
    for index in range(2):
        local = _order("buy-{}".format(index), "REUSED")
        booking.register_order(local)
        capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), index, local.order_id)
        order = dict(_broker_order(), order_id="REUSED", amount=1000,
                     order_remark=local.client_tag, order_sysid="SYS-{}".format(index))
        trade = dict(_broker_trade(), order_id="REUSED", trade_id="T-{}".format(index))
        if link == "tag":
            trade["order_remark"] = local.client_tag
        elif link == "sysid":
            trade["order_sysid"] = order["order_sysid"]
        orders.append(order)
        trades.append(trade)
    snapshot = _snapshot("15990", positions=(BrokerPositionSnapshot(SECURITY, 2000, 0),),
                         orders=orders, trades=trades)
    store = SQLiteBrokerHistoryStore(database)
    for row in orders:
        store.record_order("default", row, snapshot.as_of)
    for row in trades:
        store.record_trade("default", row, snapshot.as_of)
    # Run through the adapter's history merge after reopening the same database.
    history = SQLiteBrokerHistoryStore(database).snapshot("default")
    snapshot = replace(
        snapshot,
        orders=tuple(merge_broker_rows(orders, history.orders, "order_id")),
        trades=tuple(merge_broker_rows(trades, history.trades, "trade_id")),
    )
    assert len(snapshot.orders) == len(snapshot.trades) == 2
    for _ in range(2):
        result = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)
        if link == "ambiguous":
            assert result.state is ReconciliationState.BLOCKED
            assert any("ambiguous_owned_trade_identity" in x for x in result.details["blockers"])
            assert repository.get_strategy_account(ACCOUNT_ID).cash_units == money_to_units("10000")
        else:
            assert result.state is ReconciliationState.READY
            assert repository.get_strategy_account(ACCOUNT_ID).cash_units == money_to_units("5990")


def test_zero_order_id_trade_is_relinked_by_counter_contract_id(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    order = dict(_broker_order())
    order["order_sysid"] = "SYS-1"
    trade = dict(_broker_trade())
    trade.update({"order_id": 0, "order_sysid": "SYS-1"})

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(order,),
            trades=(trade,),
        ),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["booked_trade_ids"] == ("trade-1",)


def test_conservative_unpriced_fill_is_booked_and_marked_estimated(tmp_path):
    database, _, capital, reconciliation = _services(
        tmp_path,
        UnpricedFillPolicy.CONSERVATIVE_ORDER_PRICE,
    )
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    order = dict(_broker_order())
    order.update({"amount": 1000, "filled": 1000, "order_price": 2.0})
    trade = dict(_broker_trade())
    trade.update({"price": 0, "traded_price": 0, "deal_balance": 0})

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "18000",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(order,),
            trades=(trade,),
        ),
    )

    assert result.state is ReconciliationState.READY
    connection = connect_database(database)
    try:
        fill = connection.execute(
            "SELECT price_units, price_source, price_known FROM fills"
        ).fetchone()
        assert tuple(fill) == (
            price_to_units("2"),
            FillPriceSource.ORDER_PRICE_FALLBACK.value,
            0,
        )
    finally:
        connection.close()


def test_owned_working_sell_freeze_reduces_required_broker_sellable(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    assert reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),),
            trades=(_broker_trade(),),
        ),
    ).state is ReconciliationState.READY

    sell = replace(
        _order("sell-1", "broker-sell-1"),
        client_tag="bt:test:sell-1",
        side=OrderSide.SELL,
    )
    booking.register_order(sell)
    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "17995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(
                {
                    "order_id": "broker-sell-1",
                    "security": SECURITY,
                    "status": "open",
                    "is_buy": False,
                },
            ),
        ),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["strategy_frozen_sell_qty"] == {SECURITY: 1000}


def test_overnight_qmt_sellable_lag_is_deferred_until_morning(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    filled = _snapshot(
        "17995",
        positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
        orders=(_broker_order(),),
        trades=(_broker_trade(),),
    )
    assert reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, filled
    ).state is ReconciliationState.READY

    overnight = replace(
        filled,
        as_of=datetime(2026, 8, 12, 1, 0, tzinfo=SHANGHAI_TZ),
    )
    overnight_result = reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, overnight
    )

    assert overnight_result.state is ReconciliationState.READY
    assert overnight_result.details["blockers"] == ()
    assert len(
        overnight_result.details["deferred_broker_sellable_shortages"]
    ) == 1

    morning = replace(
        filled,
        as_of=datetime(2026, 8, 12, 9, 15, tzinfo=SHANGHAI_TZ),
    )
    morning_result = reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, morning
    )

    assert morning_result.state is ReconciliationState.BLOCKED
    assert any(
        item.startswith("broker_position_insufficient:")
        for item in morning_result.details["blockers"]
    )


def test_unknown_fee_fill_is_booked_and_small_cash_gap_is_tolerated(tmp_path):
    database, repository, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    trade = dict(_broker_trade())
    trade.update(
        {
            "commission_fee": 0.0,
            "commission_known": False,
            "tax": 0.0,
            "tax_known": False,
        }
    )

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "7995",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),),
            trades=(trade,),
        ),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["unknown_fee_order_count"] == 1
    assert result.details["broker_cash_shortfall_units"] == money_to_units("5")
    assert result.details["unknown_fee_cash_tolerance_units"] == money_to_units("5")
    assert repository.get_strategy_account(ACCOUNT_ID).cash_units == money_to_units("8000")
    connection = connect_database(database)
    try:
        fee_row = connection.execute(
            "SELECT commission_known, tax_known FROM fills"
        ).fetchone()
        assert tuple(fee_row) == (0, 0)
    finally:
        connection.close()


def test_unknown_fee_cash_gap_beyond_configured_tolerance_blocks(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    trade = dict(_broker_trade())
    trade.update({"commission_known": False, "tax_known": False})

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "7994.99",
            positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
            orders=(_broker_order(),),
            trades=(trade,),
        ),
    )

    assert result.state is ReconciliationState.BLOCKED
    assert any(
        blocker.startswith("broker_cash_insufficient:")
        for blocker in result.details["blockers"]
    )


def test_unrelated_broker_order_is_ignored_in_shared_account(tmp_path):
    _, repository, _, reconciliation = _services(tmp_path)
    snapshot = _snapshot(
        "20000",
        orders=(
            {
                "order_id": "manual-order",
                "security": SECURITY,
                "status": "open",
                "is_buy": True,
            },
        ),
    )

    result = reconciliation.synchronize(ACCOUNT_ID, PHYSICAL_ID, snapshot)

    assert result.state is ReconciliationState.READY
    assert result.details["blockers"] == ()
    assert result.details["ignored_broker_order_count"] == 1
    assert repository.get_strategy_account(ACCOUNT_ID).status is AccountStatus.ACTIVE


def test_strategy_tag_with_conflicting_broker_id_still_blocks(tmp_path):
    database, _, _, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    local = _order()
    booking.register_order(local)
    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "20000",
            orders=(
                {
                    "order_id": "unexpected-broker-id",
                    "security": SECURITY,
                    "status": "open",
                    "order_remark": local.client_tag,
                },
            ),
        ),
    )

    assert result.state is ReconciliationState.BLOCKED
    assert (
        "owned_order_broker_id_mismatch:unexpected-broker-id"
        in result.details["blockers"]
    )


def test_unrelated_cash_and_positions_do_not_block_shared_account(tmp_path):
    _, _, _, reconciliation = _services(tmp_path)

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "19999",
            positions=(BrokerPositionSnapshot(SECURITY, 100, 100),),
        ),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["blockers"] == ()
    assert result.details["strategy_required_cash_units"] == money_to_units("10000")
    assert result.details["strategy_owned_position_count"] == 0


def test_strategy_owned_cash_and_position_shortage_blocks(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")
    filled = _snapshot(
        "17995",
        positions=(BrokerPositionSnapshot(SECURITY, 1000, 0),),
        orders=(_broker_order(),),
        trades=(_broker_trade(),),
    )
    assert reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, filled
    ).state is ReconciliationState.READY

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot(
            "7000",
            positions=(BrokerPositionSnapshot(SECURITY, 500, 0),),
            orders=(_broker_order(),),
            trades=(_broker_trade(),),
        ),
    )

    assert result.state is ReconciliationState.BLOCKED
    blockers = result.details["blockers"]
    assert any(item.startswith("broker_cash_insufficient:") for item in blockers)
    assert any(item.startswith("broker_position_insufficient:") for item in blockers)


def test_canceled_buy_releases_reservation_before_cash_compare(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000", orders=(_broker_order("canceled"),)),
    )

    assert result.state is ReconciliationState.READY


def test_historical_open_order_does_not_prove_it_is_still_working(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    booking.register_order(_order())
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, "buy-1")

    historical = dict(_broker_order("open"), _broker_history_only=True)
    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000", orders=(historical,)),
    )

    assert result.state is ReconciliationState.BLOCKED
    assert "missing_working_order:broker-buy-1" in result.details["blockers"]


def test_unrelated_trade_is_not_guessed_or_booked(tmp_path):
    _, _, _, reconciliation = _services(tmp_path)
    trade = dict(_broker_trade())
    trade["order_id"] = "manual-order"

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000", trades=(trade,)),
    )

    assert result.state is ReconciliationState.READY
    assert result.details["blockers"] == ()
    assert result.details["ignored_broker_trade_count"] == 1


def test_ready_reconciliation_does_not_clear_manual_kill_switch(tmp_path):
    database, repository, _, reconciliation = _services(tmp_path)
    connection = connect_database(database)
    try:
        connection.execute(
            "UPDATE strategy_accounts SET status = 'TRADING_BLOCKED' WHERE strategy_account_id = ?",
            (ACCOUNT_ID,),
        )
        connection.commit()
    finally:
        connection.close()

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000"),
    )

    assert result.state is ReconciliationState.READY
    assert (
        repository.get_strategy_account(ACCOUNT_ID).status
        is AccountStatus.TRADING_BLOCKED
    )


def test_unproven_qmt_capabilities_persist_blocked_result(tmp_path):
    database, _, _, _ = _services(tmp_path)
    unproven = replace(
        _capabilities(),
        stable_trade_id=CapabilityState.PROBE_REQUIRED,
    )
    reconciliation = SQLiteReconciliationService(database, unproven)

    result = reconciliation.synchronize(
        ACCOUNT_ID,
        PHYSICAL_ID,
        _snapshot("20000"),
    )

    assert result.state is ReconciliationState.BLOCKED
    assert result.details["blockers"][0].startswith("capability:")


def test_async_server_adapter_snapshot_is_supported():
    class Adapter:
        async def get_account_info(self, account):
            return {"available_cash": 10000.0}

        async def get_positions(self, account):
            return [{"security": "510050.SH", "amount": 100,
                     "closeable_amount": 50, "name": "上证50ETF"}]

        async def list_orders(self, account, filters=None):
            assert filters == {"from_broker": True, "include_history": True}
            return []

        async def list_trades(self, account, filters=None):
            assert filters == {"include_history": True}
            return []

    snapshot = asyncio.run(
        collect_async_broker_snapshot(
            Adapter(),
            object(),
            datetime(2026, 8, 11, 15, 0, tzinfo=SHANGHAI_TZ),
        )
    )

    assert snapshot.available_cash_units == money_to_units("10000")
    assert snapshot.positions == (BrokerPositionSnapshot(SECURITY, 100, 50, "上证50ETF"),)


def test_async_snapshot_keeps_unrelated_signed_qmt_position():
    class Adapter:
        async def get_account_info(self, account):
            return {"available_cash": 10000.0}

        async def get_positions(self, account):
            return [
                {
                    "security": "159208.SZ",
                    "amount": -21949,
                    "closeable_amount": -21949,
                }
            ]

        async def list_orders(self, account, filters=None):
            return []

        async def list_trades(self, account, filters=None):
            return []

    snapshot = asyncio.run(collect_async_broker_snapshot(Adapter(), object()))

    assert snapshot.positions == (
        BrokerPositionSnapshot("159208.XSHE", -21949, -21949),
    )


def test_async_server_adapter_wrapped_account_snapshot_is_supported():
    class Adapter:
        async def get_account_info(self, account):
            return {"dtype": "dict", "value": {"available_cash": 10000.0}}

        async def get_positions(self, account):
            return []

        async def list_orders(self, account, filters=None):
            return []

        async def list_trades(self, account, filters=None):
            return []

    snapshot = asyncio.run(collect_async_broker_snapshot(Adapter(), object()))

    assert snapshot.available_cash_units == money_to_units("10000")


def test_submit_unknown_order_is_adopted_by_exact_client_tag(tmp_path):
    database, _, capital, reconciliation = _services(tmp_path)
    booking = SQLiteFillBookingService(database)
    local = replace(_order(), state=OrderState.SUBMIT_UNKNOWN, broker_order_id=None)
    booking.register_order(local)
    capital.reserve_cash(ACCOUNT_ID, money_to_units("2100"), 0, local.order_id)
    broker_order = {
        "order_id": "broker-recovered-1",
        "security": SECURITY,
        "status": "open",
        "is_buy": True,
        "order_remark": "sub:personal|{}".format(local.client_tag),
    }

    result = reconciliation.synchronize(
        ACCOUNT_ID, PHYSICAL_ID, _snapshot("17900", orders=(broker_order,))
    )

    assert result.state is ReconciliationState.READY
    assert result.details["adopted_order_ids"] == (local.order_id,)
    connection = connect_database(database)
    try:
        row = connection.execute(
            "SELECT state, broker_order_id FROM strategy_orders WHERE order_id = ?",
            (local.order_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row["state"] == OrderState.SUBMITTED.value
    assert row["broker_order_id"] == "broker-recovered-1"
