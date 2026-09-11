"""Full QMT API doubles only: these tests never contact a trading account."""
import importlib.util
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE = Path(__file__).parents[2] / "helpers" / "big_qmt_bridge.py"


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("qmt_bridge_test", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def command(key="one", action="/place_order", **payload):
    return dict(id=key, action=action, deadline=time.time() + 10, payload=dict(
        security="510300.XSHG", side="BUY", amount=100, price=1.002,
        qmt_user_order_id="bt:abcdef:123456789abc", **payload))


def runtime(script, context=None):
    instance = script.BridgeRuntime(context or SimpleNamespace(), "test-account")
    messages = []
    instance.send = messages.append
    return instance, messages


@pytest.mark.parametrize("price_type", [11, 42, 47])
def test_write_once_preserves_server_price_and_type(script, price_type):
    script.ENABLE_TRADING = True
    calls = []
    script.passorder = lambda *args: calls.append(args) or 0
    bridge, messages = runtime(script)
    msg = command(pr_type=price_type)
    bridge.command(msg)
    bridge.command(msg)
    assert len(calls) == 1
    assert calls[0][3:7] == ("510300.SH", price_type, 1.002, 100)
    assert calls[0][8:10] == (2, "bt:abcdef:123456789abc")
    assert messages[0] == messages[1]
    assert messages[0]["value"]["order_id"] == ""
    assert messages[0]["value"]["submit_unknown"] is True


@pytest.mark.parametrize("action", ["/place_order", "/cancel_order"])
def test_writes_disabled_by_default(script, action):
    bridge, messages = runtime(script)
    bridge.command(command(action=action, order_id="test-order"))
    assert messages[-1]["ok"] is False
    assert messages[-1]["broker_called"] is False


def test_expired_no_write_and_read_commands_not_cached(script):
    bridge, messages = runtime(script)
    msg = command()
    msg["deadline"] = 0
    bridge.command(msg)
    assert not bridge.seen
    script.get_trade_detail_data = lambda *args: []
    bridge.command(command("read", "/orders"))
    assert messages[-1]["value"] == []
    assert not bridge.seen


def test_post_call_failure_is_unknown_and_not_replayed(script):
    script.ENABLE_TRADING = True
    calls = []
    def fail(*args):
        calls.append(args)
        raise RuntimeError("lost acknowledgement")
    script.passorder = fail
    bridge, messages = runtime(script)
    bridge.command(command())
    bridge.command(command())
    assert len(calls) == 1
    assert messages[-1]["broker_called"] is True


def test_native_fields_and_unknown_fee_are_not_invented(script):
    row = dict(m_strOrderSysID="real-order", m_strTradeID="real-trade", m_strInstrumentID="510300",
               m_strExchangeID="SH", m_nOpType=23, m_nVolume=100, m_dTradePrice=1.002,
               m_strTradeDate="20260912", m_strTradeTime="09:30:01", m_strRemark="bt:test")
    trade = script._trade(row)
    assert (trade["trade_id"], trade["order_id"], trade["time"]) == ("real-trade", "real-order", "2026-09-12 09:30:01")
    assert trade["commission_fee"] is None and trade["commission_known"] is False
    assert trade["tax"] is None and trade["tax_known"] is False
    assert script._trade({})["time"] is None
    assert script._order({})["order_id"] == ""
    assert script._order({"m_strOrderSysID": "0"})["order_id"] == ""
    assert script._trade({"m_strTradeID": -1})["trade_id"] == ""
    order = script._order({"m_dLimitPrice": 1.002, "m_dTradedPrice": 1.001, "m_strOrderRemark": "bt:tag"})
    assert (order["price"], order["order_price"], order["order_remark"]) == (1.001, 1.002, "bt:tag")


def test_tick_batch_subscription_lifecycle_and_callback_filter(script):
    context = SimpleNamespace(get_instrument_detail=lambda _: {"PriceTick": 0.001},
        subscribe_quote=lambda *args, **kwargs: 1, unsubscribe_quote=lambda _: None)
    bridge, events = runtime(script, context)
    bridge.welcomed = True
    bridge.subscriptions_to(["510300.XSHG"])
    bridge.on_quote({"510300.SH": [{"lastPrice": 1, "time": 100}, {"lastPrice": 2, "time": 200}]})
    assert [row["time"] for row in events[-1]["payload"]["510300.XSHG"]] == [100, 200]
    script._runtime = bridge
    script.order_callback(context, {"m_strAccountID": "another"})
    assert len(events) == 1
    script.stop(context)
    script.deal_callback(context, {})
    assert len(events) == 1 and bridge.closed and not bridge.subscriptions


def test_init_timer_and_historical_bars_do_not_order(script):
    calls = []
    script.BRIDGE_TOKEN = "local-test"
    context = SimpleNamespace(accountID="test-account", set_account=lambda _: None,
                              run_time=lambda *args: calls.append(args))
    script.init(context)
    script.handlebar(context)
    assert calls == [("on_timer", "500nMilliSecond", "20200101000000")]
    assert script._runtime.sock is None
    script.stop(context)
    script.init(context)  # Stop/start releases local resources.


def test_portable_single_file_has_no_strategy_or_external_runtime():
    raw = SOURCE.read_bytes()
    assert raw.decode("ascii") == raw.decode("gbk") == raw.decode("utf-8")
    for forbidden in ("import xtquant", "import bullet_trade", "get_etf_info", "threading", "StrategyLedger"):
        assert forbidden not in raw.decode("ascii")
