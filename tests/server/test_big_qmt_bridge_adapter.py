"""Local end-to-end transport tests with fake QMT APIs, never real orders."""
import asyncio
import importlib.util
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter, BigQmtGatewayError, _normalize_order, build_big_qmt_bundle
from bullet_trade.server.adapters.big_qmt_bridge import BridgeBrokerAdapter
from bullet_trade.server.config import AccountConfig, ServerConfig


def make_bundle(monkeypatch, tmp_path):
    monkeypatch.setenv("BIG_QMT_TRANSPORT", "bridge")
    monkeypatch.setenv("BIG_QMT_GATEWAY_PASSWORD", "test-token")
    monkeypatch.setenv("BIG_QMT_BRIDGE_PORT", "0")
    config = ServerConfig(server_type="big_qmt", accounts=[AccountConfig("default", "fake")],
                          strategy_database_path=str(tmp_path / "ledger.db"))
    router = AccountRouter(config.accounts)
    return build_big_qmt_bundle(config, router), router.get("default")


@asynccontextmanager
async def connected(monkeypatch, tmp_path):
    bundle, account = make_bundle(monkeypatch, tmp_path)
    broker, data = bundle.broker_adapter, bundle.data_adapter
    await broker.start()
    await data.start()
    path = Path(__file__).parents[2] / "helpers/big_qmt_bridge.py"
    spec = importlib.util.spec_from_file_location("qmt_bridge_e2e", path)
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    script.BRIDGE_PORT = broker.client.bridge.port
    script.BRIDGE_TOKEN = "test-token"
    script.ENABLE_TRADING = script.ENABLE_CANCEL = True
    orders, trades, writes, subscriptions = [], [], [], []
    def place(*args):
        writes.append(args)
        now = time.localtime()
        orders.append(dict(m_strOrderSysID="native-order-" + str(len(writes)), m_strInstrumentID=args[3].split(".")[0],
            m_strExchangeID=args[3].split(".")[1], m_nOpType=args[0], m_nVolumeTotalOriginal=args[6],
            m_nVolumeTraded=0, m_nOrderStatus=50, m_dLimitPrice=args[5], m_strRemark=args[9],
            m_strInsertDate=time.strftime("%Y%m%d", now), m_strInsertTime=time.strftime("%H:%M:%S", now)))
        script.order_callback(context, orders[-1])
    def cancel(order_id, *args):
        for row in orders:
            if row["m_strOrderSysID"] == order_id:
                row["m_nOrderStatus"] = 54
                script.order_callback(context, row)
    script.passorder, script.cancel = place, cancel
    script.get_trade_detail_data = lambda account, typ, kind: {
        "account": [dict(m_dAvailable=10000, m_dBalance=10000, m_dInstrumentValue=0)],
        "position": [], "order": orders, "deal": trades}[kind]
    def subscribe(code, **kwargs):
        subscriptions.append(code)
        return len(subscriptions)
    context = SimpleNamespace(get_instrument_detail=lambda _: {"UpStopPrice": 1.1, "DownStopPrice": .9, "PriceTick": .001},
        get_full_tick=lambda codes: {c: dict(lastPrice=1, askPrice=[1.001], bidPrice=[.999], time=int(time.time()*1000)) for c in codes},
        subscribe_quote=subscribe, unsubscribe_quote=lambda _: None,
        get_stock_list_in_sector=lambda _: ["511880.SH"])
    runtime = script.BridgeRuntime(context, "fake")
    script._runtime = runtime
    async def pump():
        while True:
            runtime.pump()
            await asyncio.sleep(.005)
    task = asyncio.create_task(pump())
    try:
        for _ in range(100):
            if runtime.welcomed:
                break
            await asyncio.sleep(.01)
        assert runtime.welcomed
        yield SimpleNamespace(bundle=bundle, broker=broker, data=data, account=account, runtime=runtime,
                              script=script, orders=orders, trades=trades, writes=writes, subscriptions=subscriptions)
    finally:
        runtime.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await data.stop()
        await broker.stop()


def test_factory_preserves_http_default(monkeypatch, tmp_path):
    bundle, account = make_bundle(monkeypatch, tmp_path)
    assert isinstance(bundle.broker_adapter, BridgeBrokerAdapter)
    monkeypatch.delenv("BIG_QMT_TRANSPORT")
    cfg = ServerConfig(accounts=[account.config])
    assert type(build_big_qmt_bundle(cfg, AccountRouter(cfg.accounts)).broker_adapter) is BigQmtBrokerAdapter
    monkeypatch.setenv("BIG_QMT_TRANSPORT", "typo")
    with pytest.raises(ValueError):
        build_big_qmt_bundle(cfg, AccountRouter(cfg.accounts))


@pytest.mark.parametrize("status, expected", [(53, "partly_canceled"), (54, "cancelled"), (55, "partly_filled")])
def test_official_native_statuses(status, expected):
    assert _normalize_order({"raw_status": status})["status"] == expected


@pytest.mark.asyncio
async def test_roundtrip_native_query_submit_partial_fill_cancel_and_history(monkeypatch, tmp_path):
    async with connected(monkeypatch, tmp_path) as e:
        events = []
        e.broker.add_event_listener(lambda *args: events.append(args))
        info = await e.broker.get_account_info(e.account)
        assert info["available_cash"] == 10000
        assert e.broker.qmt_status()["ready"] and e.broker.qmt_status()["cancel_order_enabled"]
        tag = "bt:abcdef:123456789abc"
        result = await e.broker.place_order(e.account, dict(security="510300.XSHG", amount=100, side="BUY",
            style={"type": "limit", "price": "1.002"}, order_remark=tag, wait_timeout=0))
        assert result["order_id"] == "native-order-1" and result["status"] == "open"
        assert len(e.writes) == 1 and e.writes[0][4:7] == (11, 1.002, 100)
        assert result["order_remark"] == tag
        e.orders[0].update(m_nVolumeTraded=40, m_nOrderStatus=55)
        e.script.order_callback(None, e.orders[0])
        trade = dict(m_strOrderSysID="native-order-1", m_strTradeID="native-fill", m_strInstrumentID="510300",
            m_strExchangeID="SH", m_nOpType=23, m_nVolume=40, m_dTradePrice=1.002,
            m_strTradeDate=time.strftime("%Y%m%d"), m_strTradeTime=time.strftime("%H:%M:%S"), m_strRemark=tag)
        e.trades.append(trade)
        e.script.deal_callback(None, trade)
        e.script.deal_callback(None, trade)  # Duplicate callback: durable history remains unique.
        await asyncio.sleep(.05)
        assert any(kind == "trade" for _, kind, _ in events)
        assert (await e.broker.list_orders(e.account))[0]["status"] == "partly_filled"
        fills = await e.broker.list_trades(e.account)
        assert len(fills) == 1 and fills[0]["commission_known"] is False
        cancel = await e.broker.cancel_order_request(e.account, {"order_id": result["order_id"]})
        assert cancel["value"] is True and cancel["status"] == "cancelled"
        e.orders.clear()
        e.trades.clear()  # Simulate native next-day query returning no previous records.
        assert await e.broker.list_orders(e.account) == []
        assert len(await e.broker.list_orders(e.account, {"include_history": True})) == 1
        assert len(await e.broker.list_trades(e.account, {"include_history": True})) == 1
        # A fresh adapter reads the SAME existing history; no second ledger file.
        rebuilt, account = make_bundle(monkeypatch, tmp_path)
        store = rebuilt.broker_adapter._history
        assert store.list_orders("default")[0]["order_remark"] == tag
        assert len(store.list_trades("default")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("security, pr_type", [("510300.XSHG", 42), ("159919.XSHE", 47)])
async def test_market_type_and_protection_unchanged(monkeypatch, tmp_path, security, pr_type):
    async with connected(monkeypatch, tmp_path) as e:
        await e.broker.place_order(e.account, dict(security=security, amount=100, side="SELL",
            style={"type": "market", "protect_price": "0.985"}, order_remark="bt:abcdef:123456789abc"))
        assert e.writes[0][0] == 24 and e.writes[0][4:7] == (pr_type, .985, 100)


@pytest.mark.asyncio
async def test_quotes_callbacks_multi_strategy_and_reconnect(monkeypatch, tmp_path):
    async with connected(monkeypatch, tmp_path) as e:
        ticks = []
        e.data.add_tick_listener(ticks.append)
        await e.data.replace_execution_quotes("one", ["510300.XSHG"])
        await e.data.replace_execution_quotes("two", ["159919.XSHE"])
        assert set(e.runtime.subscriptions) == {"510300.SH", "159919.SZ"}
        e.runtime.on_quote({"510300.SH": [{"lastPrice": 1, "time": 100}, {"lastPrice": 2, "time": 200}]})
        await asyncio.sleep(.05)
        assert [r["time"] for r in ticks[-1]["510300.XSHG"]] == [100, 200]
        tick = await e.data.get_current_tick("510300.XSHG")
        assert tick["UpStopPrice"] == 1.1 and tick["PriceTick"] == .001
        e.broker.client.bridge._disconnect()
        await asyncio.sleep(.03)
        assert not e.broker.qmt_status()["ready"]
        e.runtime.next_connect = 0
        for _ in range(100):
            if e.broker.qmt_status()["ready"] and e.data._applied is not None:
                break
            await asyncio.sleep(.01)
        assert e.broker.qmt_status()["ready"]
        await e.data.replace_execution_quotes("one", [])
        assert set(e.runtime.subscriptions) == {"159919.SZ"}
        with pytest.raises(BigQmtGatewayError, match="historical"):
            await e.data.get_history({})


@pytest.mark.asyncio
async def test_disabled_native_gate_is_explicit_not_unknown(monkeypatch, tmp_path):
    async with connected(monkeypatch, tmp_path) as e:
        e.script.ENABLE_TRADING = False
        with pytest.raises(BigQmtGatewayError) as error:
            await e.broker.place_order(e.account, dict(security="510300.XSHG", amount=100, side="BUY",
                style={"type": "limit", "price": "1.002"}))
        assert error.value.broker_called is False and not e.writes
