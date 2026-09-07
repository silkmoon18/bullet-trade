import asyncio
from datetime import datetime, timedelta

import pytest

import bullet_trade.server.strategy.api as strategy_api_module
from bullet_trade.server.config import AccountConfig
from bullet_trade.server.adapters.base import AccountContext
from bullet_trade.server.strategy import (
    LimitExecution,
    BrokerCashMismatchError,
    BrokerCapabilityProfile,
    CapabilityState,
    ConditionalLimitExecution,
    ExecutionRequest,
    LedgerInvariantError,
    MarketMark,
    MarketQuote,
    SQLiteStrategyAPI,
    StrategyAPIConfig,
    money_to_units,
    price_to_units,
    XTQUANT_DIRECT_CAPABILITIES,
    execution_request_to_wire,
)
from bullet_trade.server.strategy.schema import connect_database
from bullet_trade.server.feishu_notifier import TargetBuyPlanNotification
from bullet_trade.server.strategy.domain import IntentState, SHANGHAI_TZ


SECURITY = "510050.XSHG"


@pytest.fixture(autouse=True)
def execution_clock(monkeypatch):
    # Existing execution cases test trading-time behaviour, independently of
    # the developer's wall clock. Session-specific cases override this below.
    monkeypatch.setattr(SQLiteStrategyAPI, "_can_dispatch_now", lambda self: True)


@pytest.mark.parametrize("day,hour,minute,allowed", [
    (7, 9, 29, False), (7, 9, 30, True), (7, 11, 30, False),
    (7, 13, 0, True), (7, 14, 59, True), (7, 15, 0, False),
    (7, 21, 26, False), (12, 10, 0, False), (13, 10, 0, False),
])
def test_execution_session_boundaries(day, hour, minute, allowed):
    assert SQLiteStrategyAPI._is_execution_session(
        datetime(2026, 9, day, hour, minute, tzinfo=SHANGHAI_TZ)
    ) is allowed


@pytest.mark.asyncio
async def test_after_hours_target_is_not_created_or_dispatched(api, monkeypatch):
    service, broker, account, _ = api
    monkeypatch.setattr(service, "_can_dispatch_now", lambda: False)
    with pytest.raises(RuntimeError, match="非发单时段"):
        await service.submit_targets(account, "default", {
            "strategy_id": "good_etf", "idempotency_key": "closed",
            "weights": {SECURITY: 0.5},
            "as_of": "2026-09-07T09:30:00+08:00",  # Cannot spoof wall time.
        })
    assert broker.order_calls == 0
    assert service.planner.active_intents() == ()


@pytest.mark.asyncio
async def test_dispatch_checks_time_before_each_order(api, monkeypatch):
    service, _, _, _ = api
    session_open = [True]
    monkeypatch.setattr(service, "_can_dispatch_now", lambda: session_open[0])
    calls = []

    async def dispatch(submitter, strategy_id):
        calls.append(strategy_id)
        session_open[0] = False
        return "first-dispatch"

    monkeypatch.setattr(service.planner, "dispatch_next", dispatch)
    assert await service._dispatch_pending(None, "good_etf") == ("first-dispatch",)
    assert calls == ["good_etf"]


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


class FakeBroker:
    def __init__(self):
        self.cash = 20_000.0
        self.positions = []
        self.orders = []
        self.trades = []
        self.order_calls = 0
        self.cancel_calls = []

    async def get_account_info(self, account):
        return {"available_cash": self.cash}

    async def get_positions(self, account):
        return list(self.positions)

    async def list_orders(self, account, filters=None):
        return list(self.orders)

    async def list_trades(self, account, filters=None):
        return list(self.trades)

    async def place_order(self, account, payload):
        self.order_calls += 1
        order_id = "broker-{}".format(self.order_calls)
        price = float(payload["style"]["price"])
        amount = int(payload["amount"])
        self.cash -= price * amount + 5.0
        self.orders.append(
            {
                "order_id": order_id,
                "security": payload["security"],
                "status": "open",
                "side": payload["side"],
                "order_remark": payload["order_remark"],
            }
        )
        return {"order_id": order_id}

    async def cancel_order(self, account, order_id):
        self.cancel_calls.append(order_id)
        return {"order_id": order_id, "canceled": True}


class FakeData:
    async def get_current_tick(self, security):
        return {"last_price": 10.0}


class CallbackData(FakeData):
    def __init__(self):
        self.listener = None
        self.subscriptions = []
        self.ask_price = 10.03

    def add_tick_listener(self, callback):
        self.listener = callback

    async def subscribe_execution_quotes(self, symbols):
        self.subscriptions.append(tuple(symbols))

    async def replace_execution_quotes(self, owner, symbols):
        replacement = tuple(symbols)
        if not self.subscriptions or self.subscriptions[-1] != replacement:
            self.subscriptions.append(replacement)

    async def get_current_tick(self, security):
        return {
            "last_price": 10.0,
            "bidPrice": [9.99],
            "askPrice": [self.ask_price],
            "dt": datetime.now(SHANGHAI_TZ).isoformat(),
            "openInt": 13,
        }

    def emit(self, payload):
        assert self.listener is not None
        self.listener(payload)


@pytest.fixture
def api(tmp_path):
    broker = FakeBroker()
    notifications = []
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "strategy.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        FakeData(),
        notifications.append,
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    return service, broker, account, notifications


@pytest.mark.asyncio
async def test_ensure_account_and_real_snapshot(api):
    service, _, account, _ = api

    ensured = await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    snapshot = await service.get_snapshot(
        account, "default", {"strategy_id": "good_etf"}
    )

    assert ensured["created"] is True
    assert ensured["account"]["available_cash"] == 10_000.0
    assert snapshot["available_cash"] == 10_000.0
    assert snapshot["total_value"] == 10_000.0
    assert snapshot["nav"] == 1.0
    assert snapshot["reconciliation"]["state"] == "READY"
    service.startup_ready = False
    assert await service.startup_check(account, "default") is True


@pytest.mark.asyncio
async def test_startup_rebinds_all_but_reconciles_only_enabled_strategy(
    api, monkeypatch
):
    service, _, account, _ = api
    for strategy_id in ("old_strategy", "good_etf"):
        await service.ensure_account(
            account,
            "default",
            {"strategy_id": strategy_id, "initial_capital": 10_000},
        )

    service._runtime_bindings.clear()
    reconciled = []
    synchronize = service._synchronize

    async def record_synchronize(strategy_id, physical_id, snapshot):
        reconciled.append(strategy_id)
        return await synchronize(strategy_id, physical_id, snapshot)

    monkeypatch.setattr(service, "_synchronize", record_synchronize)

    assert await service.startup_check(account, "default") is True
    assert set(service._runtime_bindings) == {
        "old_strategy",
        "good_etf",
    }
    assert reconciled == ["good_etf"]


@pytest.mark.asyncio
@pytest.mark.parametrize("market_open", [True, False])
async def test_resume_dispatches_existing_pending_outbox_without_new_order(api, monkeypatch, market_open):
    service, broker, account, _ = api
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    as_of = datetime.now(SHANGHAI_TZ)
    marks = {
        SECURITY: MarketMark(
            SECURITY,
            price_to_units("10"),
            as_of,
            "test",
        )
    }
    snapshot = service.valuation.create_snapshot(
        "good_etf", marks, as_of, timedelta(minutes=5)
    )
    planned = service.planner.submit_target_weights(
        "good_etf",
        "pending-before-restart",
        {SECURITY: "0.5"},
        snapshot,
        marks,
        as_of,
    )
    assert len(planned.orders) == 1
    assert broker.order_calls == 0
    monkeypatch.setattr(service, "_can_dispatch_now", lambda: market_open)

    await asyncio.wait_for(
        service._resume_intent_locked(planned.intent.intent_id), timeout=2
    )

    assert broker.order_calls == int(market_open)
    connection = connect_database(service.database_path)
    try:
        assert connection.execute("SELECT state FROM outbox").fetchone()[0] == (
            "DONE" if market_open else "PENDING"
        )
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_after_hours_callback_books_fill_without_planning_or_dispatch(api, monkeypatch):
    service, broker, account, _ = api
    await service.ensure_account(account, "default", {
        "strategy_id": "good_etf", "initial_capital": 10_000,
    })
    await service.submit_targets(account, "default", {
        "strategy_id": "good_etf", "idempotency_key": "daytime",
        "weights": {SECURITY: "0.5"}, "marks": {SECURITY: "10"},
    })
    db = connect_database(service.database_path)
    try:
        qty, price = db.execute("SELECT requested_qty, limit_price_units FROM strategy_orders").fetchone()
    finally:
        db.close()
    broker.positions = [{"security": SECURITY, "amount": qty, "closeable_amount": 0}]
    broker.orders[0]["status"] = "filled"
    broker.trades = [{
        "trade_id": "late-fill", "trade_id_source": "broker", "order_id": "broker-1",
        "security": SECURITY, "side": "BUY", "amount": qty, "price": price / 1_000_000,
        "time": datetime.now(SHANGHAI_TZ).isoformat(),
    }]
    monkeypatch.setattr(service, "_can_dispatch_now", lambda: False)
    monkeypatch.setattr(service.planner, "advance_intent", lambda *a, **kw: pytest.fail("closed planning"))
    await service._handle_broker_event("default", "trade")
    assert broker.order_calls == 1 and broker.cancel_calls == []
    db = connect_database(service.database_path)
    try:
        assert db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
        assert db.execute("SELECT total_qty FROM positions").fetchone()[0] == qty
    finally:
        db.close()


@pytest.mark.asyncio
async def test_new_strategy_rebases_external_cash_and_uses_remaining_pool(api):
    service, broker, account, _ = api
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "existing", "initial_capital": 10_000},
    )
    broker.cash = 19_999.0

    result = await service.ensure_account(
        account,
        "default",
        {"strategy_id": "new-strategy", "initial_capital": 9_000},
    )

    assert result["created"] is True
    assert result["account"]["available_cash"] == 9_000.0
    connection = connect_database(service.database_path)
    try:
        pool = connection.execute(
            """
            SELECT unallocated_cash_units, reserved_cash_units
            FROM cash_pools WHERE physical_account_id = ?
            """,
            ("qmt:default",),
        ).fetchone()
    finally:
        connection.close()
    assert tuple(pool) == (money_to_units("999"), 0)


@pytest.mark.asyncio
async def test_new_strategy_rejects_when_remaining_real_cash_is_insufficient(api):
    service, broker, account, _ = api
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "existing", "initial_capital": 10_000},
    )
    broker.cash = 19_999.0

    with pytest.raises(LedgerInvariantError) as exc_info:
        await service.ensure_account(
            account,
            "default",
            {"strategy_id": "new-strategy", "initial_capital": 10_000},
        )

    message = str(exc_info.value)
    assert "ledger_unallocated_available_cash=9999.0000" in message
    assert "requested_initial_capital=10000.0000" in message
    assert "shortfall=1.0000" in message
    assert "broker_available_cash=19999.0000" in message
    assert "new_strategy_id=new-strategy" in message


@pytest.mark.asyncio
async def test_new_strategy_reports_when_broker_cannot_cover_existing_strategies(api):
    service, broker, account, _ = api
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "existing", "initial_capital": 10_000},
    )
    broker.cash = 9_999.0

    with pytest.raises(BrokerCashMismatchError) as exc_info:
        await service.ensure_account(
            account,
            "default",
            {"strategy_id": "new-strategy", "initial_capital": 1_000},
        )

    message = str(exc_info.value)
    assert "broker_available_cash=9999.0000" in message
    assert "ledger_strategy_available_cash=10000.0000" in message
    assert "shortfall=1.0000" in message
    assert "requested_initial_capital=1000.0000" in message
    assert "new_strategy_id=new-strategy" in message


@pytest.mark.asyncio
async def test_ensure_account_ignores_unrelated_shared_account_activity(api):
    service, broker, account, _ = api
    broker.positions = [
        {
            "security": "159208.SZ",
            "amount": -21949,
            "closeable_amount": -21949,
        },
        {
            "security": "510300.SH",
            "amount": 1000,
            "closeable_amount": 1000,
        },
    ]
    broker.orders = [
        {
            "order_id": "manual-order",
            "security": "510300.XSHG",
            "status": "filled",
            "order_remark": "another-strategy",
        }
    ]
    broker.trades = [
        {
            "trade_id": "manual-trade",
            "order_id": "manual-order",
            "security": "510300.XSHG",
            "amount": 1000,
            "price": 4.0,
        }
    ]

    result = await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": "10000"},
    )

    assert result["created"] is True
    assert result["reconciliation"]["state"] == "READY"
    details = result["reconciliation"]["details"]
    assert details["ignored_broker_order_count"] == 1
    assert details["ignored_broker_trade_count"] == 1


@pytest.mark.asyncio
async def test_submit_rechecks_current_account_instead_of_cached_startup_flag(api):
    service, broker, account, _ = api
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10_000}
    )
    service.startup_ready = False
    result = await service.submit_targets(account, "default", {
        "strategy_id": "good_etf", "idempotency_key": "after-reconnect",
        "weights": {SECURITY: "0.5"}, "marks": {SECURITY: "10"},
    })
    assert result["reconciliation"]["state"] == "READY"
    assert service.startup_ready is True
    assert broker.order_calls == 1


@pytest.mark.asyncio
async def test_submit_reports_fresh_blocker_before_valuation_or_order_side_effects(api, monkeypatch):
    service, broker, account, notifications = api
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10_000}
    )
    service.startup_ready = False
    broker.cash = 0
    reads = []
    original_query = broker.get_account_info

    async def query(current_account):
        reads.append(True)
        return await original_query(current_account)

    monkeypatch.setattr(broker, "get_account_info", query)
    monkeypatch.setattr(service.valuation, "create_snapshot", lambda *a, **kw: pytest.fail(
        "blocked submit must report reconciliation before valuation"
    ))
    with pytest.raises(RuntimeError, match="StrategyLedger对账未就绪") as caught:
        await service.submit_targets(account, "default", {
            "strategy_id": "good_etf", "idempotency_key": "still-blocked",
            "weights": {SECURITY: "0.5"}, "marks": {SECURITY: "10"},
        })
    assert "strategy_id=good_etf" in str(caught.value)
    assert "broker_cash_insufficient" in str(caught.value)
    assert reads and broker.order_calls == 0 and broker.cancel_calls == []
    assert notifications[-1].event == "RECONCILIATION_BLOCKED"
    assert service.get_intent({"strategy_id": "good_etf"}) == {}


@pytest.mark.asyncio
async def test_missing_fill_price_blocks_then_recovers_from_valid_evidence_without_duplicate_order(api):
    service, broker, account, _ = api
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10_000}
    )
    request = {
        "strategy_id": "good_etf", "idempotency_key": "recover-unpriced-fill",
        "weights": {SECURITY: "0.5"}, "marks": {SECURITY: "10"},
    }
    submitted = await service.submit_targets(account, "default", request)
    db = connect_database(service.database_path)
    try:
        order = db.execute("SELECT requested_qty, limit_price_units FROM strategy_orders").fetchone()
    finally:
        db.close()
    quantity, actual_price = order[0], order[1] / 1_000_000
    broker.orders[0].update({"status": "filled", "amount": quantity, "filled": quantity})
    broker.positions = [{"security": SECURITY, "amount": quantity, "closeable_amount": 0}]
    broker.trades = [{
        "trade_id": "late-price", "trade_id_source": "broker", "order_id": "broker-1",
        "security": SECURITY, "side": "BUY", "amount": quantity, "price": 0,
        "deal_balance": 0, "time": datetime.now(SHANGHAI_TZ).isoformat(),
        "commission_fee": 5, "commission_known": True, "tax": 0, "tax_known": True,
    }]
    for _ in range(2):
        with pytest.raises(RuntimeError, match="broker trade price is invalid") as caught:
            await service.submit_targets(account, "default", request)
        assert SECURITY in str(caught.value)
        assert "成交待核实" in str(caught.value)
    with pytest.raises(RuntimeError, match="broker trade price is invalid"):
        await service._resume_intent_locked(submitted["intent"]["intent_id"])
    assert broker.order_calls == 1 and broker.cancel_calls == []

    # A later real query supplies the price; never substitute a quote or 0.
    broker.trades[0]["price"] = actual_price
    recovered = await service.submit_targets(account, "default", request)
    await service.submit_targets(account, "default", request)
    assert recovered["reconciliation"]["state"] == "READY"
    assert recovered["intent"]["intent_id"] == submitted["intent"]["intent_id"]
    assert broker.order_calls == 1
    db = connect_database(service.database_path)
    try:
        assert db.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
    finally:
        db.close()


@pytest.mark.asyncio
async def test_submit_targets_is_idempotent_and_exposes_queries(api):
    service, broker, account, notifications = api
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": "10000"},
    )
    request = {
        "strategy_id": "good_etf",
        "idempotency_key": "jq-20260811-open",
        "weights": {SECURITY: "0.5"},
        "marks": {SECURITY: "10"},
        "security_names": {SECURITY: "测试ETF"},
    }

    first = await service.submit_targets(account, "default", request)
    connection = connect_database(service.database_path)
    try:
        connection.execute(
            "UPDATE strategy_orders SET submitted_at = ?",
            ("2020-01-01T10:00:00+08:00",),
        )
        connection.commit()
    finally:
        connection.close()
    second = await service.submit_targets(account, "default", request)
    intent_id = first["intent"]["intent_id"]
    restored = service.get_intent({"strategy_id": "good_etf"})

    assert broker.order_calls == 1
    assert broker.cancel_calls == []
    assert second["cancel_requested_order_ids"] == []
    assert any(item.event == "ORDER_SUBMITTED" for item in notifications)
    assert any(
        item.event == "ORDER_SUBMITTED" and item.security_name == "测试ETF"
        for item in notifications
    )
    assert first["intent"]["intent_id"] == second["intent"]["intent_id"]
    assert restored["intent_id"] == intent_id
    assert restored["weights"] == {SECURITY: 0.5}
    assert first["snapshot"]["available_cash"] == 4_985.0
    assert first["snapshot"]["reserved_cash"] == 5_015.0
    assert service.get_intent(
        {"strategy_id": "good_etf", "intent_id": intent_id}
    )["intent_id"] == intent_id
    assert service.get_reconciliation(
        "default", {"strategy_id": "good_etf"}
    )["reconciliation"]["state"] == "READY"


def test_target_buy_plan_notification_does_not_trade_or_write_ledger(api):
    service, broker, _, notifications = api

    result = service.notify_target_buy_plan(
        {
            "strategy_id": "good_etf",
            "mode": "JQ",
            "occurred_at": "2026-08-13T09:30:00+08:00",
            "items": [
                {
                    "security": "510050.XSHG",
                    "security_name": "上证50ETF",
                    "quantity": 1000,
                    "amount": "2500.00",
                    "reference_price": "2.5000",
                },
                {
                    "security": "159915.XSHE",
                    "quantity": 500,
                    "amount": "750.00",
                    "reference_price": "1.5000",
                },
            ],
        }
    )

    assert result == {
        "accepted": True,
        "item_count": 2,
        "total_amount": 3250.0,
    }
    assert broker.order_calls == 0
    assert len(notifications) == 1
    notification = notifications[0]
    assert isinstance(notification, TargetBuyPlanNotification)
    assert notification.mode == "JQ"
    assert notification.items[0].quantity == 1000
    assert notification.items[0].security_name == "上证50ETF"
    with pytest.raises(Exception, match="not found"):
        service.repository.get_strategy_account("good_etf")


def test_native_tick_retains_same_day_limits_but_not_previous_day(api):
    service, _, _, _ = api
    now = datetime.now(SHANGHAI_TZ)
    service._quote_cache[SECURITY] = MarketQuote(
        SECURITY, now,
        high_limit_units=price_to_units("2.75"),
        low_limit_units=price_to_units("2.25"),
        instrument_type="etf",
    )
    tick = {"lastPrice": 2.50, "askPrice": [2.501], "openInt": 13}
    current = service._quote_from_tick(SECURITY, tick, now)
    assert current.high_limit_units == price_to_units("2.75")
    assert current.low_limit_units == price_to_units("2.25")
    assert current.instrument_type == "etf"
    explicit_zero = service._quote_from_tick(
        SECURITY, dict(tick, high_limit=0), now
    )
    assert explicit_zero.high_limit_units is None
    tomorrow = service._quote_from_tick(SECURITY, tick, now + timedelta(days=1))
    assert tomorrow.high_limit_units is None
    assert tomorrow.low_limit_units is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "security,reason,expected_calls",
    [
        ("600000.XSHG", "超出有效申报价格范围", 2),
        ("600000.XSHG", "可用资金不足", 1),
        (SECURITY, "超出有效申报价格范围", 1),
    ],
)
async def test_repeat_submit_observes_rejection_before_replanning(
    api, security, reason, expected_calls
):
    service, broker, account, _ = api
    service.data_provider = CallbackData()
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10000}
    )
    request = {
        "strategy_id": "good_etf",
        "idempotency_key": "rejected-target",
        "weights": {security: "0.5"},
        "marks": {security: "10"},
        "execution": execution_request_to_wire(
            ExecutionRequest(style=LimitExecution(2_000))
        ),
    }
    first = await service.submit_targets(account, "default", request)
    assert broker.order_calls == 1
    broker.orders[0].update(status="rejected", status_msg=reason)
    broker.cash = 20000.0  # broker released the rejected order's reservation

    second = await service.submit_targets(account, "default", request)
    assert second["planned_orders"] == []
    assert broker.order_calls == 1
    third = await service.submit_targets(account, "default", request)
    assert broker.order_calls == expected_calls
    if expected_calls == 2:
        assert third["planned_orders"][0]["limit_price_units"] == first["planned_orders"][0]["limit_price_units"]


@pytest.mark.asyncio
async def test_stock_original_limit_resumes_on_quote_entering_cage(api):
    service, broker, account, _ = api
    data = CallbackData()
    data.ask_price = 9.70
    data.add_tick_listener(service._on_tick_event)
    service.data_provider = data
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10000}
    )
    stock = "600000.XSHG"
    result = await service.submit_targets(account, "default", {
        "strategy_id": "good_etf", "idempotency_key": "stock-cage-callback",
        "weights": {stock: "0.5"}, "marks": {stock: "10"},
        "execution": execution_request_to_wire(ExecutionRequest(style=LimitExecution(2_000))),
    })
    assert result["planned_orders"] == []
    assert data.subscriptions == [(stock,)]
    assert broker.order_calls == 0
    data.emit({"600000.SH": {
        "lastPrice": 9.90, "askPrice": [9.90], "bidPrice": [9.89], "openInt": 13,
        "dt": datetime.now(SHANGHAI_TZ).isoformat(),
    }})
    for _ in range(100):
        if broker.order_calls:
            break
        await asyncio.sleep(0.01)
    assert broker.order_calls == 1
    connection = connect_database(service.database_path)
    try:
        price = connection.execute("SELECT limit_price_units FROM strategy_orders").fetchone()[0]
    finally:
        connection.close()
    assert price == price_to_units("10.02")


@pytest.mark.asyncio
@pytest.mark.parametrize("cycle,expected_sellable", [(0, 500), (1, 0)])
async def test_snapshot_books_fund_using_qmt_settlement_cycle(
    api, monkeypatch, cycle, expected_sellable
):
    service, broker, account, _ = api
    service.data_provider = CallbackData()
    resolved = []

    async def get_tplus(security):
        resolved.append(security)
        return cycle

    monkeypatch.setattr(service.data_provider, "get_tplus", get_tplus, raising=False)
    await service.ensure_account(
        account, "default", {"strategy_id": "good_etf", "initial_capital": 10000}
    )
    await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "t0-fill",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
        },
    )
    broker.orders[0]["status"] = "filled"
    broker.trades = [{
        "trade_id": "T1",
        "trade_id_source": "broker",
        "order_id": broker.orders[0]["order_id"],
        "security": SECURITY,
        "side": "BUY",
        "amount": 500,
        "price": 10.02,
        "commission_fee": 5.0,
        "commission_known": True,
        "tax": 0.0,
        "tax_known": True,
        "time": datetime.now(SHANGHAI_TZ).isoformat(),
    }]
    broker.positions = [{
        "security": SECURITY,
        "amount": 500,
        "closeable_amount": expected_sellable,
    }]
    snapshot = await service.get_snapshot(
        account, "default", {"strategy_id": "good_etf"}
    )

    assert resolved == [SECURITY]
    assert snapshot["reconciliation"]["details"]["blockers"] == []
    assert snapshot["reconciliation"]["state"] == "READY"
    assert snapshot["positions"][SECURITY]["closeable_amount"] == expected_sellable


@pytest.mark.asyncio
async def test_conditional_target_is_resumed_by_native_tick_callback(
    tmp_path, monkeypatch
):
    log_messages = []
    monkeypatch.setattr(
        strategy_api_module.logger,
        "info",
        lambda message, *args: log_messages.append(message % args),
    )
    broker = FakeBroker()
    data = CallbackData()
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "callback.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        data,
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": "10000"},
    )

    result = await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "conditional-callback",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
            "execution": execution_request_to_wire(
                ExecutionRequest(
                    style=ConditionalLimitExecution(2_000)
                )
            ),
        },
    )

    assert result["planned_orders"] == []
    assert broker.order_calls == 0
    assert data.subscriptions == [(SECURITY,)]

    data.emit(
        {
            "510050.SH": {
                "lastPrice": 10.0,
                "bidPrice": [9.99],
                "askPrice": [10.01],
            }
        }
    )
    await asyncio.sleep(0.02)
    assert broker.order_calls == 0

    # xtdata.subscribe_quote sends {stock: [tick, ...]}.  The first tick
    # reaches the fixed boundary while the later tick moves back outside it;
    # the crossing must not be lost when the callback contains a batch.
    data.emit(
        {
            "510050.SH": [
                {
                    "lastPrice": 10.0,
                    "bidPrice": [9.99],
                    "askPrice": [10.01],
                    "time": int(datetime.now(SHANGHAI_TZ).timestamp() * 1000),
                },
                {
                    "lastPrice": 10.03,
                    "bidPrice": [10.02],
                    "askPrice": [10.03],
                    "time": int(datetime.now(SHANGHAI_TZ).timestamp() * 1000),
                },
            ]
        }
    )
    for _ in range(100):
        if broker.order_calls:
            break
        await asyncio.sleep(0.01)

    assert broker.order_calls == 1
    assert len(log_messages) == 1
    assert log_messages[0].startswith(
        "StrategyLedger 首次收到执行行情 | 510050.XSHG | "
    )
    assert "行情时间=" in log_messages[0]
    assert "接收时间=" in log_messages[0]
    assert "延迟=" in log_messages[0]

    last_log_at = service._quote_last_log_at[SECURITY]
    latest_quote = service._quote_cache[SECURITY]
    service._log_execution_quote_heartbeat(
        SECURITY,
        latest_quote,
        last_log_at + timedelta(seconds=59),
        first=False,
    )
    assert len(log_messages) == 1
    service._log_execution_quote_heartbeat(
        SECURITY,
        latest_quote,
        last_log_at + timedelta(seconds=60),
        first=False,
    )
    assert log_messages[1].startswith(
        "StrategyLedger 执行行情心跳 | 510050.XSHG | "
    )


@pytest.mark.asyncio
async def test_qmt_mark_preserves_timestamp_and_rejects_stale_tick(
    api, monkeypatch
):
    service, _, _, _ = api
    request_time = datetime(2026, 9, 2, 10, 30, tzinfo=SHANGHAI_TZ)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return request_time if tz is not None else request_time.replace(
                tzinfo=None
            )

    monkeypatch.setattr(strategy_api_module, "datetime", FixedDatetime)

    class TimestampedData:
        def __init__(self, tick_time):
            self.tick_time = tick_time

        async def get_current_tick(self, security):
            return {"last_price": 10.0, "time": self.tick_time}

    fresh_time = request_time - timedelta(seconds=30)
    service.data_provider = TimestampedData(
        int(fresh_time.timestamp() * 1000)
    )
    marks = await service._marks(
        None, request_time, "good_etf", (SECURITY,)
    )
    assert marks[SECURITY].as_of == fresh_time

    stale_time = request_time - timedelta(minutes=6)
    service.data_provider = TimestampedData(stale_time.isoformat())
    with pytest.raises(ValueError, match="QMT mark is stale"):
        await service._marks(
            None, request_time, "good_etf", (SECURITY,)
        )


def test_valuation_marks_allow_session_break_but_execution_quotes_stay_strict(
    api,
):
    service, _, _, _ = api
    lunch = datetime(2026, 9, 2, 12, 56, tzinfo=SHANGHAI_TZ)
    lunch_mark = datetime(2026, 9, 2, 11, 29, 59, tzinfo=SHANGHAI_TZ)
    after_close = datetime(2026, 9, 2, 16, 57, tzinfo=SHANGHAI_TZ)
    close_mark = datetime(2026, 9, 2, 15, 0, 38, tzinfo=SHANGHAI_TZ)

    assert service._valuation_mark_is_fresh(lunch_mark, lunch) is True
    assert service._valuation_mark_is_fresh(close_mark, after_close) is True
    assert service._market_time_is_fresh(lunch_mark, lunch) is False
    assert service._market_time_is_fresh(close_mark, after_close) is False


@pytest.mark.asyncio
async def test_idle_intent_can_be_canceled_before_risk_replacement(tmp_path):
    broker = FakeBroker()
    data = CallbackData()
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "strategy-cancel.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        data,
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    submitted = await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "waiting-to-cancel",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
            "execution": execution_request_to_wire(
                ExecutionRequest(
                    style=ConditionalLimitExecution(2_000)
                )
            ),
        },
    )

    canceled = await service.cancel_intent(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "intent_id": submitted["intent"]["intent_id"],
        },
    )

    assert canceled["canceled"] is True
    assert canceled["intent"]["state"] == "CANCELED"
    assert data.subscriptions[-1] == ()


@pytest.mark.asyncio
async def test_midnight_expiry_cancels_working_order_and_closes_intent(tmp_path):
    class CancelingBroker(FakeBroker):
        def __init__(self):
            super().__init__()
            self.reserved_by_order = {}

        async def place_order(self, account, payload):
            before = self.cash
            result = await super().place_order(account, payload)
            self.reserved_by_order[result["order_id"]] = before - self.cash
            return result

        async def cancel_order(self, account, order_id):
            result = await super().cancel_order(account, order_id)
            for row in self.orders:
                if row["order_id"] == order_id:
                    row["status"] = "canceled"
            self.cash += self.reserved_by_order.pop(order_id, 0.0)
            return result

    broker = CancelingBroker()
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "strategy-midnight.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        FakeData(),
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    submitted = await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "midnight-expiry",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
        },
    )
    result = await service.expire_previous_day_intents(
        datetime.now(SHANGHAI_TZ) + timedelta(days=1)
    )

    assert result.expired == 1
    assert result.cancel_requests == 1
    assert result.canceled == 1
    assert result.pending == 0
    assert len(broker.cancel_calls) == 1
    restored = service.get_intent(
        {
            "strategy_id": "good_etf",
            "intent_id": submitted["intent"]["intent_id"],
        }
    )
    assert restored["state"] == "CANCELED"
    assert restored["orders"][0]["state"] == "CANCELED"
    assert service.repository.get_strategy_account(
        "good_etf"
    ).reserved_cash_units == 0


@pytest.mark.asyncio
async def test_midnight_expiry_recovers_terminal_intent_with_working_order(
    tmp_path,
):
    class CancelingBroker(FakeBroker):
        async def cancel_order(self, account, order_id):
            result = await super().cancel_order(account, order_id)
            for row in self.orders:
                if row["order_id"] == order_id:
                    row["status"] = "canceled"
            return result

    broker = CancelingBroker()
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "strategy-terminal-expiry.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        FakeData(),
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    submitted = await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "terminal-midnight-expiry",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
        },
    )
    intent_id = submitted["intent"]["intent_id"]
    service.planner._set_state(intent_id, IntentState.CANCELED)

    result = await service.expire_previous_day_intents(
        datetime.now(SHANGHAI_TZ) + timedelta(days=1)
    )

    assert result.expired == 1
    assert result.canceled == 1
    restored = service.get_intent(
        {"strategy_id": "good_etf", "intent_id": intent_id}
    )
    assert restored["orders"][0]["state"] == "CANCELED"


@pytest.mark.asyncio
async def test_midnight_expiry_closes_day_order_left_working_in_broker_history(
    tmp_path,
):
    broker = FakeBroker()
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            database_path=tmp_path / "strategy-day-order-expiry.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
            cash_buffer_units=0,
            max_age=timedelta(minutes=5),
        ),
        broker,
        _capabilities(),
        FakeData(),
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": 10_000},
    )
    submitted = await service.submit_targets(
        account,
        "default",
        {
            "strategy_id": "good_etf",
            "idempotency_key": "broker-history-expiry",
            "weights": {SECURITY: "0.5"},
            "marks": {SECURITY: "10"},
        },
    )

    result = await service.expire_previous_day_intents(
        datetime.now(SHANGHAI_TZ) + timedelta(days=1)
    )

    assert result.canceled == 1
    restored = service.get_intent(
        {
            "strategy_id": "good_etf",
            "intent_id": submitted["intent"]["intent_id"],
        }
    )
    assert restored["orders"][0]["state"] == "CANCELED"
    assert service.repository.get_strategy_account(
        "good_etf"
    ).reserved_cash_units == 0


@pytest.mark.parametrize(
    ("legacy_mode", "normalised_mode"),
    [
        ("SHADOW", "JQ"),
        ("SIGNAL_ONLY", "JQ"),
        ("JQ_PAPER", "JQ"),
        ("REMOTE", "QMT_REMOTE"),
        ("LIVE", "QMT_REMOTE"),
    ],
)
def test_target_buy_plan_accepts_legacy_mode_aliases(
    api, legacy_mode, normalised_mode
):
    service, broker, _, notifications = api

    service.notify_target_buy_plan(
        {
            "strategy_id": "good_etf",
            "mode": legacy_mode,
            "items": [{"security": SECURITY, "quantity": 100, "amount": "250.00"}],
        }
    )

    assert broker.order_calls == 0
    assert notifications[0].mode == normalised_mode


@pytest.mark.asyncio
async def test_initial_cash_shortage_fails_without_strategy_account(api):
    service, broker, account, _ = api
    broker.cash = 9_999.0

    with pytest.raises(Exception, match="insufficient"):
        await service.ensure_account(
            account,
            "default",
            {"strategy_id": "good_etf", "initial_capital": "10000"},
        )

    with pytest.raises(Exception, match="not found"):
        service.repository.get_strategy_account("good_etf")


def test_money_scale_is_still_exact():
    assert money_to_units("10000") == 100_000_000


@pytest.mark.asyncio
async def test_unverified_capabilities_are_deferred_while_trading_is_disabled(tmp_path):
    broker = FakeBroker()
    notifications = []
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(tmp_path / "blocked.db"),
        broker,
        XTQUANT_DIRECT_CAPABILITIES,
        FakeData(),
        notifications.append,
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))
    result = await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": "10000"},
    )
    assert result["reconciliation"]["state"] == "READY"
    assert result["reconciliation"]["details"][
        "capability_verification_required"
    ] is False
    assert notifications == []


@pytest.mark.asyncio
async def test_unverified_capabilities_block_when_trading_is_enabled(tmp_path):
    broker = FakeBroker()
    notifications = []
    service = SQLiteStrategyAPI(
        StrategyAPIConfig(
            tmp_path / "blocked-live.db",
            trading_enabled=True,
            enabled_strategy_ids=("good_etf",),
        ),
        broker,
        XTQUANT_DIRECT_CAPABILITIES,
        FakeData(),
        notifications.append,
    )
    account = AccountContext(AccountConfig("default", "qmt-account"))

    result = await service.ensure_account(
        account,
        "default",
        {"strategy_id": "good_etf", "initial_capital": "10000"},
    )

    assert result["reconciliation"]["state"] == "BLOCKED"
    assert notifications[-1].event == "RECONCILIATION_BLOCKED"
    assert notifications[-1].strategy_id == "good_etf"
    assert "capability" in notifications[-1].detail
