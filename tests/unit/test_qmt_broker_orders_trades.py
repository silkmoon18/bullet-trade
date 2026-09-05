import pytest

from bullet_trade.broker.qmt import QmtBroker
from bullet_trade.core.models import OrderStatus


@pytest.mark.unit
def test_qmt_broker_get_orders_filters(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    monkeypatch.setattr(
        broker,
        "sync_orders",
        lambda: [
            {"order_id": "1", "security": "000001.XSHE", "status": "open"},
            {"order_id": "2", "security": "000002.XSHE", "status": "filled"},
        ],
    )

    orders = broker.get_orders(status=OrderStatus.open)
    ids = {item.get("order_id") for item in orders}
    assert "1" in ids
    assert "2" not in ids


@pytest.mark.unit
def test_qmt_broker_get_trades_mapping(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        def __init__(self):
            self.trade_id = "t1"
            self.order_id = "o1"
            self.stock_code = "000001.SZ"
            self.trade_volume = 100
            self.trade_price = 10.0
            self.trade_time = "2025-01-01 09:31:00"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trades = broker.get_trades(order_id="o1")
    assert len(trades) == 1
    trade = trades[0]
    assert trade.get("trade_id") == "t1"
    assert trade.get("trade_id_source") == "broker"
    assert trade.get("order_id") == "o1"
    assert trade.get("security") == "000001.XSHE"


@pytest.mark.unit
def test_native_zero_sell_price_is_not_fabricated_from_quote_or_cost():
    from bullet_trade.server.strategy.broker_contract import (
        BrokerContractError, normalize_trade_evidence, UnpricedFillPolicy,
    )

    # The September 4 simulator response had both native fields equal to zero.
    row = QmtBroker(account_id="demo").normalize_trade_event({
        "traded_id": "sell-zero", "order_id": "sell-order",
        "stock_code": "560210.SH", "order_type": 24,
        "traded_volume": 100, "traded_price": 0, "traded_amount": 0,
        "traded_time": "2026-09-04 09:30:11",
        "last_price": 1.2, "open_price": 1.1,
    })
    assert row["price"] == 0
    assert row["deal_balance"] == 0
    assert row["commission_known"] is False
    orders = {"sell-order": {
        "amount": 100, "filled": 100, "is_buy": False,
        "order_price": 1.0, "price": 0,
    }}
    with pytest.raises(BrokerContractError, match="price is invalid"):
        normalize_trade_evidence(row, orders)
    evidence = normalize_trade_evidence(
        row, orders, UnpricedFillPolicy.CONSERVATIVE_ORDER_PRICE,
    )
    assert evidence.price_known is False
    assert evidence.commission_units is None
    assert evidence.tax_units is None


@pytest.mark.unit
def test_qmt_broker_get_trades_prefers_traded_price_and_preserves_zero_commission(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        def __init__(self):
            self.trade_id = "t2"
            self.order_id = "o2"
            self.stock_code = "510050.SH"
            self.trade_volume = 33800
            self.traded_price = 2.906
            self.trade_price = 0.0
            self.deal_balance = 98222.8
            self.commission_fee = 0.0
            self.tax = 0.0
            self.trade_time = "2026-03-31 09:45:21"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="o2")[0]
    assert trade["price"] == pytest.approx(2.906)
    assert trade["traded_price"] == pytest.approx(2.906)
    assert trade["deal_balance"] == pytest.approx(98222.8)
    assert trade["commission"] == pytest.approx(0.0)
    assert trade["commission_fee"] == pytest.approx(0.0)
    assert trade["commission_known"] is True
    assert trade["tax_known"] is True


@pytest.mark.unit
def test_qmt_broker_skips_zero_primary_trade_price(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        trade_id = "t-zero-primary"
        order_id = "o-zero-primary"
        stock_code = "510050.SH"
        trade_volume = 100
        traded_price = 0.0
        trade_price = 2.5
        trade_time = "2026-08-31 13:14:21"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="o-zero-primary")[0]
    assert trade["price"] == pytest.approx(2.5)


@pytest.mark.unit
def test_qmt_broker_maps_xttrade_id_and_optional_broker_commission_extension(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        traded_id = "xt-trade-1"
        order_id = "xt-order-1"
        stock_code = "510050.SH"
        traded_volume = 100
        traded_price = 2.5
        traded_time = "2026-08-19 09:31:00"
        used_commission = 5.25

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="xt-order-1")[0]
    assert trade["trade_id"] == "xt-trade-1"
    assert trade["trade_id_source"] == "broker"
    assert trade["commission_fee"] == pytest.approx(5.25)
    assert trade["commission_known"] is True
    assert trade["tax_known"] is False


@pytest.mark.unit
def test_qmt_broker_ignores_zero_order_id_and_preserves_link_fields():
    broker = QmtBroker(account_id="demo")

    class DummyTrade:
        trade_id = "xt-trade-zero-order"
        order_id = 0
        entrust_id = "xt-order-fallback"
        order_sysid = "counter-contract-1"
        stock_code = "510050.SH"
        traded_volume = 100
        traded_price = 2.5
        traded_time = "2026-08-28 09:31:00"

    trade = broker.normalize_trade_event(DummyTrade())

    assert trade is not None
    assert trade["order_id"] == "xt-order-fallback"
    assert trade["order_sysid"] == "counter-contract-1"


@pytest.mark.unit
def test_qmt_broker_marks_synthetic_trade_id_and_unknown_fees(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        order_id = "o3"
        stock_code = "510050.SH"
        trade_volume = 100
        trade_price = 2.5
        trade_time = "2026-08-10 10:00:00"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="o3")[0]
    assert trade["trade_id"]
    assert trade["trade_id_source"] == "synthetic"
    assert trade["commission_known"] is False
    assert trade["tax_known"] is False


@pytest.mark.unit
def test_qmt_broker_does_not_mark_invalid_fee_values_as_known(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrade:
        trade_id = "t-invalid-fees"
        order_id = "o-invalid-fees"
        stock_code = "510050.SH"
        trade_volume = 100
        trade_price = 2.5
        commission_fee = "bad"
        tax = "NaN"

    class DummyTrader:
        def query_stock_trades(self, account):
            return [DummyTrade()]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    trade = broker.get_trades(order_id="o-invalid-fees")[0]
    assert trade["commission_fee"] == 0.0
    assert trade["commission_known"] is False
    assert trade["tax"] == 0.0
    assert trade["tax_known"] is False


@pytest.mark.unit
def test_qmt_broker_order_snapshot_fields(monkeypatch):
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    monkeypatch.setattr(
        broker,
        "sync_orders",
        lambda: [
            {
                "order_id": "1",
                "security": "000001.XSHE",
                "status": "open",
                "amount": 1000,
                "filled": 300,
                "price": 10.2,
                "order_type": "buy",
                "order_remark": "bt:alpha:abcd1234",
                "strategy_name": "alpha",
            }
        ],
    )

    orders = broker.get_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.get("filled") == 300
    assert order.get("is_buy") is True
    assert order.get("order_remark") == "bt:alpha:abcd1234"
    assert order.get("strategy_name") == "alpha"


@pytest.mark.unit
def test_qmt_broker_sync_orders_market_row_does_not_treat_broker_price_as_requested_order_price():
    broker = QmtBroker(account_id="demo")
    broker._connected = True

    class DummyTrader:
        def query_stock_orders(self, account):
            return [
                {
                    "order_id": "1098912911",
                    "stock_code": "159967.SZ",
                    "order_status": "filled",
                    "order_volume": 1000,
                    "traded_volume": 1000,
                    "price": 0.634,
                    "traded_price": 0.634,
                    "price_type": "market",
                    "order_type": "sell",
                }
            ]

    broker._xt_trader = DummyTrader()
    broker._xt_account = object()

    row = broker.sync_orders()[0]
    assert row["price"] == pytest.approx(0.634)
    assert row["order_price"] is None
    assert row["broker_price"] == pytest.approx(0.634)
