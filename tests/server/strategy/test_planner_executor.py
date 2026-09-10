import asyncio
from dataclasses import replace
from datetime import date, datetime, timedelta

import pytest

from bullet_trade.server.strategy import (
    BrokerAccountSnapshot,
    BrokerCapabilityProfile,
    BrokerOrder,
    BrokerPositionSnapshot,
    ConditionalLimitExecution,
    ConditionalLimitPriceMode,
    CapabilityState,
    MarketMark,
    MarketExecution,
    MarketQuote,
    LimitExecution,
    ExecutionRequest,
    ExecutionType,
    FollowUpPolicy,
    OrderSide,
    OrderState,
    PlannerConfig,
    SQLiteCapitalService,
    SQLiteFillBookingService,
    SQLiteReconciliationService,
    SQLiteStrategyRepository,
    SQLiteTargetExecutionService,
    SQLiteValuationService,
    TargetPlanningError,
    money_to_units,
    price_to_units,
)
from bullet_trade.server.strategy.domain import BrokerFill, IntentState, SHANGHAI_TZ
from bullet_trade.server.strategy.repository import RepositoryError
from bullet_trade.server.strategy.schema import connect_database


ACCOUNT = "good-etf"
STRATEGY_ID = "good_etf"
PHYSICAL = "qmt-main"
A = "510050.XSHG"
B = "510300.XSHG"


def _capabilities():
    supported = CapabilityState.SUPPORTED
    return BrokerCapabilityProfile(
        adapter_kind="TEST",
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


def _setup(tmp_path, as_of=None):
    as_of = as_of or datetime(2026, 8, 11, 10, 0, tzinfo=SHANGHAI_TZ)
    database = tmp_path / "planner.db"
    repository = SQLiteStrategyRepository(database)
    repository.initialize()
    repository.create_physical_account(PHYSICAL, "QMT", "account-1")
    capital = SQLiteCapitalService(database)
    capital.calibrate_broker_available_cash(PHYSICAL, money_to_units("20000"))
    capital.ensure_strategy_account(ACCOUNT, "good_etf", PHYSICAL, money_to_units("10000"))
    reconciliation = SQLiteReconciliationService(database, _capabilities())
    reconciliation.synchronize(
        ACCOUNT,
        PHYSICAL,
        BrokerAccountSnapshot(money_to_units("20000"), (), (), (), as_of),
    )
    marks = {
        A: MarketMark(A, price_to_units("10"), as_of, "test"),
        B: MarketMark(B, price_to_units("5"), as_of, "test"),
    }
    snapshot = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    config = PlannerConfig(
        cash_buffer_units=0,
        trading_enabled=True,
        enabled_strategy_ids=(STRATEGY_ID,),
        max_age=timedelta(minutes=10),
    )
    planner = SQLiteTargetExecutionService(database, config)
    return database, repository, capital, reconciliation, planner, snapshot, marks, as_of


def test_weight_target_creates_one_lot_rounded_buy_and_reserves_cash(tmp_path):
    database, repository, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)

    result = planner.submit_target_weights(
        ACCOUNT, "jq-20260811", {A: 0.5}, snapshot, marks, as_of
    )

    assert len(result.orders) == 1
    assert result.orders[0].side is OrderSide.BUY
    assert result.orders[0].quantity == 500
    assert result.orders[0].limit_price_units == price_to_units("10.02")
    account = repository.get_strategy_account(ACCOUNT)
    assert account.reserved_cash_units == money_to_units("5015")
    connection = connect_database(database)
    try:
        row = connection.execute("SELECT state FROM strategy_orders").fetchone()
        assert row[0] == OrderState.PENDING_SUBMIT.value
    finally:
        connection.close()


def test_planning_uses_own_latest_reconciliation_not_another_strategy(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    connection = connect_database(database)
    try:
        later = (as_of + timedelta(seconds=1)).isoformat()
        connection.execute(
            """
            INSERT INTO reconciliation_runs(
                reconciliation_id, physical_account_id, strategy_account_id,
                state, broker_as_of, details_json, started_at, completed_at
            ) VALUES (?, ?, ?, 'BLOCKED', ?, '{}', ?, ?)
            """,
            ("other-run", PHYSICAL, "other-strategy", later, later, later),
        )
        connection.commit()
    finally:
        connection.close()

    result = planner.submit_target_weights(
        ACCOUNT, "own-ready-run", {A: 0.5}, snapshot, marks, as_of
    )

    assert len(result.orders) == 1


def test_cash_buffer_does_not_shrink_weight_target(tmp_path):
    database, _, _, _, _, snapshot, marks, as_of = _setup(tmp_path)
    planner = SQLiteTargetExecutionService(
        database,
        PlannerConfig(
            cash_buffer_units=money_to_units("100"),
            trading_enabled=True,
            enabled_strategy_ids=(STRATEGY_ID,),
            max_age=timedelta(minutes=10),
        ),
    )

    result = planner.submit_target_weights(
        ACCOUNT, "whole-portfolio-weight", {A: 0.5}, snapshot, marks, as_of
    )

    assert result.orders[0].quantity == 500


def test_same_joinquant_key_does_not_create_duplicate_order(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    first = planner.submit_target_weights(
        ACCOUNT, "same-key", {A: "0.5"}, snapshot, marks, as_of
    )
    second = planner.submit_target_weights(
        ACCOUNT, "same-key", {A: "0.5"}, snapshot, marks, as_of
    )

    assert second.intent.intent_id == first.intent.intent_id
    assert second.orders == ()
    assert second.waiting_for_fills is True
    connection = connect_database(database)
    try:
        assert connection.execute("SELECT COUNT(*) FROM strategy_orders").fetchone()[0] == 1
    finally:
        connection.close()


def test_dispatch_success_attaches_real_broker_order_id(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planned = planner.submit_target_weights(
        ACCOUNT, "dispatch-ok", {A: "0.5"}, snapshot, marks, as_of
    )

    async def submit(payload):
        assert payload["security"] == A
        assert payload["amount"] == 500
        assert payload["order_remark"].startswith("bt:")
        assert payload["wait_timeout"] == 0.0
        return {"order_id": "broker-100", "status": "submitted"}

    result = asyncio.run(planner.dispatch_next(submit))

    assert result.order_id == planned.orders[0].order_id
    assert result.broker_order_id == "broker-100"
    assert result.unknown is False
    connection = connect_database(database)
    try:
        row = connection.execute(
            "SELECT state, broker_order_id FROM strategy_orders"
        ).fetchone()
        assert tuple(row) == (OrderState.SUBMITTED.value, "broker-100")
    finally:
        connection.close()


def test_keep_original_order_is_not_canceled_only_because_time_passed(tmp_path):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planner.submit_target_weights(
        ACCOUNT, "stale-order", {A: "0.5"}, snapshot, marks, as_of
    )

    async def submit(_):
        return {"order_id": "broker-stale"}

    asyncio.run(planner.dispatch_next(submit))

    assert planner.stale_broker_order_ids(ACCOUNT) == ()
    assert planner.stale_broker_order_ids(
        ACCOUNT, datetime.now(SHANGHAI_TZ) + timedelta(minutes=11)
    ) == ()


def test_dispatch_exception_becomes_submit_unknown_and_is_not_retried(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planner.submit_target_weights(
        ACCOUNT, "dispatch-unknown", {A: "0.5"}, snapshot, marks, as_of
    )

    async def submit(_):
        raise TimeoutError("response lost")

    result = asyncio.run(planner.dispatch_next(submit))

    assert result.unknown is True
    assert asyncio.run(planner.dispatch_next(submit)) is None
    connection = connect_database(database)
    try:
        assert connection.execute("SELECT state FROM strategy_orders").fetchone()[0] == "SUBMIT_UNKNOWN"
    finally:
        connection.close()


@pytest.mark.parametrize("terminal", [None, OrderState.REJECTED, OrderState.CANCELED])
@pytest.mark.parametrize("lost_response", [False, True])
def test_delayed_submit_response_does_not_regress_already_observed_order(tmp_path, terminal, lost_response):
    database, repository, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planned = planner.submit_target_weights(ACCOUNT, "early-callback", {A: 0.5}, snapshot, marks, as_of)
    planner.config = replace(planner.config, submission_timeout_seconds=0.01)
    booking = SQLiteFillBookingService(database)

    async def submit(_):
        booking.mark_order_submitted(ACCOUNT, planned.orders[0].order_id, "early-id")
        if terminal is not None:
            booking.finalize_order(ACCOUNT, planned.orders[0].order_id, terminal,
                                   repository.get_strategy_account(ACCOUNT).ledger_version)
        if lost_response:
            await asyncio.Event().wait()
        return {"order_id": "early-id"}

    result = asyncio.run(planner.dispatch_next(submit))
    assert result.unknown is False and result.broker_order_id == "early-id"
    db = connect_database(database)
    try:
        assert db.execute("SELECT state FROM strategy_orders").fetchone()[0] == (terminal or OrderState.SUBMITTED).value
        assert db.execute("SELECT state FROM strategy_operations").fetchone()[0] == "COMPLETED"
        assert db.execute("SELECT state FROM outbox").fetchone()[0] == "DONE"
    finally:
        db.close()


def _add_position(database, capital, acquired_day, sellable_day):
    booking = SQLiteFillBookingService(database)
    booking.register_order(
        BrokerOrder(
            order_id="old-buy",
            account_id=ACCOUNT,
            intent_id=None,
            client_tag="old-buy-tag",
            broker_order_id="broker-old-buy",
            security=A,
            side=OrderSide.BUY,
            requested_qty=1000,
            filled_qty=0,
            state=OrderState.SUBMITTED,
            trading_day=acquired_day,
            limit_price_units=price_to_units("2"),
        )
    )
    capital.reserve_cash(ACCOUNT, money_to_units("2100"), 0, "old-buy")
    booking.book_fill(
        ACCOUNT,
        BrokerFill(
            fill_id="old-fill",
            broker_trade_id="old-trade",
            order_id="old-buy",
            fingerprint="old-fingerprint",
            security=A,
            side=OrderSide.BUY,
            quantity=1000,
            price_units=price_to_units("2"),
            commission_units=money_to_units("5"),
            tax_units=0,
            traded_at=datetime(
                acquired_day.year,
                acquired_day.month,
                acquired_day.day,
                10,
                0,
                tzinfo=SHANGHAI_TZ,
            ),
        ),
        1,
        sellable_from_trade_date=sellable_day,
    )


def test_sell_phase_uses_market_override_before_conditional_buy(tmp_path):
    database, _, capital, reconciliation, _, _, _, as_of = _setup(tmp_path)
    _add_position(database, capital, date(2026, 8, 10), date(2026, 8, 11))
    reconciliation.synchronize(
        ACCOUNT,
        PHYSICAL,
        BrokerAccountSnapshot(
            money_to_units("17995"),
            (BrokerPositionSnapshot(A, 1000, 1000),),
            (),
            (),
            as_of,
        ),
    )
    marks = {
        A: MarketMark(A, price_to_units("2"), as_of, "test"),
        B: MarketMark(B, price_to_units("5"), as_of, "test"),
    }
    snapshot = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    planner = SQLiteTargetExecutionService(
        database,
        PlannerConfig(
            cash_buffer_units=0,
            trading_enabled=True,
            enabled_strategy_ids=(STRATEGY_ID,),
        ),
    )

    result = planner.submit_target_weights(
        ACCOUNT,
        "rotate-a-to-b",
        {A: 0, B: 1},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(
            style=ConditionalLimitExecution(2_000),
            sell_style=MarketExecution(15_000),
        ),
    )

    assert result.waiting_for_fills is True
    assert len(result.orders) == 1
    assert result.orders[0].security == A
    assert result.orders[0].side is OrderSide.SELL
    assert result.orders[0].execution_type is ExecutionType.MARKET
    assert result.orders[0].limit_price_units is None

    connection = connect_database(database)
    try:
        connection.execute(
            "UPDATE strategy_orders SET state = 'REJECTED' WHERE order_id = ?",
            (result.orders[0].order_id,),
        )
        connection.commit()
    finally:
        connection.close()

    follow_up = planner.advance_intent(
        result.intent.intent_id, snapshot, marks, as_of
    )
    assert len(follow_up.orders) == 1
    assert follow_up.orders[0].security == A
    assert follow_up.orders[0].execution_type is ExecutionType.MARKET
    assert follow_up.orders[0].limit_price_units is None


def test_dispatch_attachment_error_keeps_original_error_and_done_outbox(
    tmp_path, monkeypatch
):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planner.submit_target_weights(
        ACCOUNT, "dispatch-attach-error", {A: "0.5"}, snapshot, marks, as_of
    )

    async def submit(_):
        return {"order_id": "broker-accepted"}

    def fail_attachment(*_args):
        raise RepositoryError("injected attachment failure")

    monkeypatch.setattr(planner._booking, "mark_order_submitted", fail_attachment)

    with pytest.raises(
        TargetPlanningError,
        match="ledger attachment failed.*injected attachment failure",
    ):
        asyncio.run(planner.dispatch_next(submit))

    connection = connect_database(database)
    try:
        operation = connection.execute(
            "SELECT state FROM strategy_operations"
        ).fetchone()[0]
        outbox = connection.execute("SELECT state FROM outbox").fetchone()[0]
        order = connection.execute("SELECT state FROM strategy_orders").fetchone()[0]
    finally:
        connection.close()
    assert operation == "COMPLETED"
    assert outbox == "DONE"
    assert order == OrderState.PENDING_SUBMIT.value


def test_global_switch_and_tplus1_both_block_new_buy(tmp_path):
    database, _, capital, reconciliation, _, _, _, _ = _setup(
        tmp_path,
        datetime(2026, 8, 10, 10, 0, tzinfo=SHANGHAI_TZ),
    )
    _add_position(database, capital, date(2026, 8, 10), date(2026, 8, 11))
    as_of = datetime(2026, 8, 10, 10, 1, tzinfo=SHANGHAI_TZ)
    reconciliation.synchronize(
        ACCOUNT,
        PHYSICAL,
        BrokerAccountSnapshot(
            money_to_units("17995"),
            (BrokerPositionSnapshot(A, 1000, 0),),
            (),
            (),
            as_of,
        ),
    )
    marks = {
        A: MarketMark(A, price_to_units("2"), as_of, "test"),
        B: MarketMark(B, price_to_units("5"), as_of, "test"),
    }
    snapshot = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    disabled = SQLiteTargetExecutionService(database, PlannerConfig())
    with pytest.raises(TargetPlanningError, match="global trading switch"):
        disabled.submit_target_weights(ACCOUNT, "disabled", {A: 0}, snapshot, marks, as_of)

    blocked = SQLiteTargetExecutionService(
        database,
        PlannerConfig(
            cash_buffer_units=0,
            trading_enabled=True,
            enabled_strategy_ids=("another-strategy",),
        ),
    )
    with pytest.raises(TargetPlanningError, match="trading allowlist"):
        blocked.submit_target_weights(
            ACCOUNT, "not-allowed", {A: 0}, snapshot, marks, as_of
        )

    planner = SQLiteTargetExecutionService(
        database,
        PlannerConfig(
            cash_buffer_units=0,
            trading_enabled=True,
            enabled_strategy_ids=(STRATEGY_ID,),
        ),
    )
    result = planner.submit_target_weights(
        ACCOUNT, "tplus1", {A: 0, B: 1}, snapshot, marks, as_of
    )
    assert result.orders == ()
    assert result.waiting_for_fills is True


def test_conditional_limit_waits_without_order_then_uses_fixed_boundary(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    execution = ExecutionRequest(
        style=ConditionalLimitExecution(
            2_000, ConditionalLimitPriceMode.BOUNDARY
        )
    )

    waiting = planner.submit_target_weights(
        ACCOUNT,
        "conditional-buy",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=execution,
        quotes={
            A: MarketQuote(
                A,
                as_of,
                ask_price_units=price_to_units("10.03"),
            )
        },
    )

    assert waiting.orders == ()
    assert waiting.waiting_for_trigger is True
    assert planner.quote_triggers_intent(
        waiting.intent.intent_id,
        A,
        MarketQuote(A, as_of, ask_price_units=price_to_units("10.03")),
        as_of,
    ) is False
    assert planner.quote_triggers_intent(
        waiting.intent.intent_id,
        A,
        MarketQuote(A, as_of, ask_price_units=price_to_units("10.01")),
        as_of,
    ) is True
    connection = connect_database(database)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM strategy_orders"
        ).fetchone()[0] == 0
    finally:
        connection.close()

    triggered = planner.submit_target_weights(
        ACCOUNT,
        "conditional-buy",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=execution,
        quotes={
            A: MarketQuote(
                A,
                as_of,
                ask_price_units=price_to_units("10.01"),
            )
        },
    )

    assert triggered.waiting_for_trigger is False
    assert len(triggered.orders) == 1
    assert triggered.orders[0].limit_price_units == price_to_units("10.02")
    assert triggered.orders[0].execution_type is ExecutionType.CONDITIONAL_LIMIT


@pytest.mark.parametrize("with_quote", [False, True])
def test_fixed_etf_limit_keeps_jq_reference_despite_new_quote(tmp_path, with_quote):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    quote = MarketQuote(
        A,
        as_of,
        bid_price_units=price_to_units("9.999"),
        ask_price_units=price_to_units("10.500"),
        last_price_units=price_to_units("10"),
        instrument_type="etf",
        continuous_auction=True,
    )

    result = planner.submit_target_weights(
        ACCOUNT,
        "fixed-etf-buy",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(style=LimitExecution(2_000)),
        quotes={A: quote} if with_quote else {},
    )

    assert len(result.orders) == 1
    assert result.orders[0].limit_price_units == price_to_units("10.02")
    assert result.orders[0].execution_type is ExecutionType.LIMIT


def test_stock_fixed_limit_waits_for_cage_then_submits_original_price(tmp_path):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    stock = "600000.XSHG"
    marks = dict(marks, **{stock: MarketMark(stock, price_to_units("10"), as_of, "jq")})
    execution = ExecutionRequest(style=LimitExecution(2_000))
    outside = MarketQuote(stock, as_of, ask_price_units=price_to_units("9.70"))
    waiting = planner.submit_target_weights(
        ACCOUNT, "stock-fixed", {stock: "0.5"}, snapshot, marks, as_of,
        execution_request=execution, quotes={stock: outside},
    )
    assert waiting.orders == ()
    assert waiting.waiting_for_trigger is True
    inside = MarketQuote(stock, as_of, ask_price_units=price_to_units("9.90"))
    marks[stock] = MarketMark(stock, price_to_units("9.90"), as_of, "qmt")
    submitted = planner.submit_target_weights(
        ACCOUNT, "stock-fixed", {stock: "0.5"}, snapshot, marks, as_of,
        execution_request=execution, quotes={stock: inside},
    )
    assert len(submitted.orders) == 1
    assert submitted.orders[0].limit_price_units == price_to_units("10.02")
    assert planner.reference_prices(waiting.intent.intent_id)[stock] == price_to_units("10")


def test_fixed_limit_cage_rejection_waits_for_newer_quote_before_retry(tmp_path):
    database, repository, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    stock = "600000.XSHG"
    marks = dict(marks)
    marks[stock] = MarketMark(
        stock,
        price_to_units("10"),
        as_of,
        "qmt",
    )
    quote = MarketQuote(
        stock,
        as_of,
        ask_price_units=price_to_units("10.001"),
        instrument_type="stock",
    )
    result = planner.submit_target_weights(
        ACCOUNT,
        "cage-rejection",
        {stock: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(style=LimitExecution(2_000)),
        quotes={stock: quote},
    )
    order = result.orders[0]
    booking = SQLiteFillBookingService(database)
    account = repository.get_strategy_account(ACCOUNT)
    booking.finalize_order(
        ACCOUNT, order.order_id, OrderState.REJECTED, account.ledger_version
    )
    recorded = planner.record_rejected_orders(
        result.intent.intent_id,
        (
            {
                "local_order_id": order.order_id,
                "intent_id": result.intent.intent_id,
                "security": stock,
                "reason": "申报价格超出有效申报价格范围",
            },
        ),
        {stock: quote},
    )

    assert recorded[0]["retryable"] is True
    assert planner.quote_triggers_intent(
        result.intent.intent_id, stock, quote, as_of
    ) is False
    outside = MarketQuote(
        stock, as_of + timedelta(milliseconds=1),
        ask_price_units=price_to_units("9.70"),
    )
    assert planner.quote_triggers_intent(
        result.intent.intent_id, stock, outside, as_of
    ) is False
    newer = MarketQuote(
        stock,
        as_of + timedelta(milliseconds=2),
        ask_price_units=price_to_units("9.90"),
        instrument_type="stock",
    )
    assert planner.quote_triggers_intent(
        result.intent.intent_id, stock, newer, as_of
    ) is True
    refreshed = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    retry = planner.advance_intent(
        result.intent.intent_id, refreshed, marks, as_of, quotes={stock: newer}
    )
    assert retry.orders[0].limit_price_units == order.limit_price_units


def test_fixed_limit_non_cage_rejection_is_not_retried(tmp_path):
    database, repository, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    quote = MarketQuote(A, as_of, ask_price_units=price_to_units("10.001"))
    result = planner.submit_target_weights(
        ACCOUNT,
        "non-cage-rejection",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(style=LimitExecution(2_000)),
        quotes={A: quote},
    )
    order = result.orders[0]
    account = repository.get_strategy_account(ACCOUNT)
    SQLiteFillBookingService(database).finalize_order(
        ACCOUNT, order.order_id, OrderState.REJECTED, account.ledger_version
    )
    recorded = planner.record_rejected_orders(
        result.intent.intent_id,
        (
            {
                "local_order_id": order.order_id,
                "intent_id": result.intent.intent_id,
                "security": A,
                "reason": "可用资金不足",
            },
        ),
        {A: quote},
    )

    assert recorded[0]["retryable"] is False
    newer = MarketQuote(
        A,
        as_of + timedelta(seconds=1),
        ask_price_units=price_to_units("10.002"),
    )
    assert planner.quote_triggers_intent(
        result.intent.intent_id, A, newer, as_of
    ) is False


def test_etf_never_enters_stock_price_cage_retry_loop(tmp_path):
    database, repository, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    quote = MarketQuote(
        A,
        as_of,
        ask_price_units=price_to_units("10.001"),
        instrument_type="etf",
    )
    result = planner.submit_target_weights(
        ACCOUNT,
        "etf-cage-text",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(style=LimitExecution(2_000)),
        quotes={A: quote},
    )
    order = result.orders[0]
    account = repository.get_strategy_account(ACCOUNT)
    SQLiteFillBookingService(database).finalize_order(
        ACCOUNT, order.order_id, OrderState.REJECTED, account.ledger_version
    )

    recorded = planner.record_rejected_orders(
        result.intent.intent_id,
        (
            {
                "local_order_id": order.order_id,
                "intent_id": result.intent.intent_id,
                "security": A,
                "reason": "申报价格超出有效申报价格范围",
            },
        ),
        {A: quote},
    )

    assert recorded[0]["retryable"] is False


def test_working_buy_does_not_block_another_security_trigger(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    execution = ExecutionRequest(
        style=ConditionalLimitExecution(
            2_000, ConditionalLimitPriceMode.BOUNDARY
        )
    )
    first = planner.submit_target_weights(
        ACCOUNT,
        "independent-buy-triggers",
        {A: "0.4", B: "0.4"},
        snapshot,
        marks,
        as_of,
        execution_request=execution,
        quotes={
            A: MarketQuote(
                A, as_of, ask_price_units=price_to_units("10.01")
            ),
            B: MarketQuote(
                B, as_of, ask_price_units=price_to_units("5.02")
            ),
        },
    )
    assert [item.security for item in first.orders] == [A]

    refreshed = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    b_quote = MarketQuote(
        B, as_of, ask_price_units=price_to_units("5.01")
    )
    assert planner.quote_triggers_intent(
        first.intent.intent_id, B, b_quote, as_of
    ) is True

    second = planner.advance_intent(
        first.intent.intent_id,
        refreshed,
        marks,
        as_of,
        quotes={B: b_quote},
    )
    assert [item.security for item in second.orders] == [B]
    assert len(planner.intent_orders(first.intent.intent_id)) == 2


def test_expired_intent_stays_active_until_working_order_is_terminal(tmp_path):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planned = planner.submit_target_weights(
        ACCOUNT,
        "expire-after-order",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
    )

    expired = planner.advance_intent(
        planned.intent.intent_id,
        snapshot,
        marks,
        as_of + timedelta(days=1),
    )

    assert expired.intent.state is IntentState.EXECUTING
    assert expired.waiting_for_fills is True
    assert planner.expired_intents((as_of + timedelta(days=1)).date()) == (
        expired.intent,
    )


def test_market_execution_is_not_encoded_as_limit_order(tmp_path):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planned = planner.submit_target_weights(
        ACCOUNT,
        "market-buy",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(
            style=MarketExecution(15_000),
            follow_up=FollowUpPolicy.NONE,
        ),
    )
    submitted = []

    async def submit(payload):
        submitted.append(payload)
        return {"order_id": "market-1"}

    asyncio.run(planner.dispatch_next(submit))

    assert planned.orders[0].limit_price_units is None
    assert planned.orders[0].execution_type is ExecutionType.MARKET
    assert submitted[0]["style"] == {
        "type": "market",
        "protect_price": "10.15",
    }


def test_follow_up_none_does_not_resubmit_terminal_remainder(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    first = planner.submit_target_weights(
        ACCOUNT,
        "one-shot",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(
            follow_up=FollowUpPolicy.NONE
        ),
    )
    connection = connect_database(database)
    try:
        connection.execute(
            "UPDATE strategy_orders SET state = 'REJECTED' WHERE order_id = ?",
            (first.orders[0].order_id,),
        )
        connection.commit()
    finally:
        connection.close()

    current_snapshot = SQLiteValuationService(database).create_snapshot(
        ACCOUNT, marks, as_of, timedelta(minutes=1)
    )
    result = planner.advance_intent(
        first.intent.intent_id, current_snapshot, marks, as_of
    )

    assert result.orders == ()
    assert result.intent.state.value == "COMPLETED"


def test_daily_intent_expires_instead_of_carrying_to_next_day(tmp_path):
    _, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    waiting = planner.submit_target_weights(
        ACCOUNT,
        "today-only",
        {A: "0.5"},
        snapshot,
        marks,
        as_of,
        execution_request=ExecutionRequest(
            style=ConditionalLimitExecution(2_000)
        ),
    )

    result = planner.advance_intent(
        waiting.intent.intent_id,
        snapshot,
        marks,
        as_of + timedelta(days=1),
    )

    assert result.orders == ()
    assert result.intent.state.value == "CANCELED"


def test_cancel_requested_intent_never_resubmits_after_order_terminal(tmp_path):
    database, _, _, _, planner, snapshot, marks, as_of = _setup(tmp_path)
    planned = planner.submit_target_weights(
        ACCOUNT, "cancel-in-flight", {A: "0.5"}, snapshot, marks, as_of
    )

    async def submit(_payload):
        return {"order_id": "broker-cancel-1"}

    asyncio.run(planner.dispatch_next(submit))
    planner.request_intent_cancellation(planned.intent.intent_id)

    waiting = planner.advance_intent(
        planned.intent.intent_id, snapshot, marks, as_of
    )
    assert waiting.waiting_for_fills is True
    assert waiting.orders == ()

    connection = connect_database(database)
    try:
        connection.execute(
            "UPDATE strategy_orders SET state = 'CANCELED' WHERE intent_id = ?",
            (planned.intent.intent_id,),
        )
        connection.commit()
    finally:
        connection.close()

    canceled = planner.advance_intent(
        planned.intent.intent_id, snapshot, marks, as_of
    )
    assert canceled.intent.state.value == "CANCELED"
    assert canceled.orders == ()
