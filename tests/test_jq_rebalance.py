"""JQ native execution: no server-style follow-up or staged rebalance."""

import importlib
import pickle
from datetime import datetime, timedelta
from types import SimpleNamespace as NS

import pytest

import helpers.bullet_trade_jq_remote_helper as module


class JQ:
    def __init__(self, cash=10000, holdings=None):
        self.cash = cash
        self.prices = {"OLD": 10.0, "TRIM": 10.0, "BUY": 10.0, "SECOND": 10.0}
        self.positions = {}
        for security, qty in (holdings or {}).items():
            self.positions[security] = NS(total_amount=qty, closeable_amount=qty,
                                          price=10.0, avg_cost=10.0, value=qty * 10.0)
        self.open = {}
        self.calls = []
        self.blocked = set()
        self.pending = set()
        self.partial_once = {}
        self.spendable_limit = None
        self.messages = []
        self.g = NS()
        self.context = NS(current_dt=datetime(2026, 9, 8, 9, 30), portfolio=NS())
        self.refresh()

    def refresh(self):
        value = 0
        for security, pos in self.positions.items():
            pos.price = self.prices[security]
            pos.value = pos.total_amount * pos.price
            value += pos.value
        self.context.portfolio = NS(positions=self.positions, total_value=self.cash + value,
                                    available_cash=self.cash, positions_value=value)

    def fill(self, security, amount):
        price = self.prices[security]
        pos = self.positions.setdefault(security, NS(total_amount=0, closeable_amount=0,
                                                    price=price, avg_cost=price, value=0))
        pos.total_amount += amount
        pos.closeable_amount = max(0, pos.closeable_amount + min(0, amount))
        self.cash -= amount * price
        if pos.total_amount == 0:
            del self.positions[security]
        self.refresh()

    def order(self, security, target, style=None):
        assert style is None  # No remote price band is passed to JQ.
        price = self.prices[security]
        qty = getattr(self.positions.get(security), "total_amount", 0)
        delta = int((target - qty * price) / price / 100) * 100
        self.calls.append((security, target))
        order = NS(order_id=str(len(self.calls)), security=security, amount=abs(delta), filled=0)
        if security in self.pending:
            self.open[order.order_id] = order
            return order
        if security in self.blocked:
            return order  # JQ canceled the unfilled market order.
        if delta > 0:
            cash = self.cash if self.spendable_limit is None else min(self.cash, self.spendable_limit)
            delta = min(delta, int(cash / price / 100) * 100)
        if security in self.partial_once:
            delta = min(delta, self.partial_once.pop(security))
        self.fill(security, delta)
        order.filled = abs(delta)
        return order

    def runtime(self, helper):
        namespace = {
            "g": self.g,
            "order_target": lambda security, qty: self.order(security, qty * self.prices[security]),
            "order_target_value": self.order,
            "get_open_orders": lambda: self.open,
            "cancel_order": lambda order: self.open.pop(order.order_id, None),
            "get_current_data": lambda: {s: NS(last_price=p) for s, p in self.prices.items()},
            "log": NS(info=self.messages.append, warn=self.messages.append, error=self.messages.append),
        }
        state = {"mode": "JQ", "strategy_id": "test", "jq_account_enabled": True,
                 "qmt_account_enabled": False}
        helper._active_state = state
        helper._active_namespace = namespace
        runtime = helper.JoinQuantRuntime(state, namespace)
        runtime.send_target_buy_plan = lambda *args, **kwargs: None
        return runtime


@pytest.fixture
def helper():
    return importlib.reload(module)


def start(jq, runtime, weights):
    return runtime.execute_rebalance(jq.context, weights, jq.prices, "open-20260908")


@pytest.mark.parametrize("mode", ["JQ", "BACKTEST"])
def test_native_rebalance_submits_once_without_waiting_for_failed_sell(helper, mode):
    jq = JQ(cash=1000, holdings={"OLD": 400, "TRIM": 500})
    jq.blocked.add("OLD")
    runtime = jq.runtime(helper)
    runtime.mode = helper.RuntimeMode(mode)
    helper._active_state["mode"] = mode
    result = start(jq, runtime, {"BUY": 0.5, "TRIM": 0.2})
    # Preserve pre-API19 native call order, even when a sell is canceled:
    # clear non-targets, then submit selected targets in decision order.
    assert jq.calls == [("OLD", 0), ("BUY", 5000), ("TRIM", 2000)]
    assert [s for s, _, _ in result["jq_orders"]] == ["OLD", "BUY", "TRIM"]
    assert result["errors"] == []
    assert jq.positions["OLD"].total_amount == 400
    assert jq.positions["BUY"].total_amount == 100  # Native cash clipping is not supplemented.
    assert not hasattr(runtime, "on_bar")
    assert not hasattr(runtime, "_jq_plan")
    assert not hasattr(jq.g, "bt_jq_plan")


def test_pending_sell_does_not_create_a_helper_sell_buy_state_machine(helper):
    jq = JQ(cash=1000, holdings={"OLD": 900})
    jq.pending.add("OLD")
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.8})
    assert jq.calls == [("OLD", 0), ("BUY", 8000)]
    assert len(jq.open) == 1  # The native working sell is not canceled by a minute worker.
    assert jq.positions["BUY"].total_amount == 100


def test_partial_buy_is_left_to_native_platform_without_helper_followup(helper):
    jq = JQ()
    jq.partial_once["BUY"] = 100
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.5, "SECOND": 0.4})
    assert jq.positions["BUY"].total_amount == 100
    assert jq.positions["SECOND"].total_amount == 400
    jq.context.current_dt += timedelta(minutes=1)
    restored = jq.runtime(importlib.reload(helper))
    assert len(jq.calls) == 2
    assert not hasattr(restored, "on_bar")
    assert not hasattr(jq.g, "bt_jq_plan")


@pytest.mark.parametrize("phase", ["SELL", "BUY", "COMPLETED"])
def test_api19_saved_plan_is_ignored_on_upgrade_and_does_not_replay_orders(helper, phase):
    jq = JQ()
    jq.g.bt_jq_plan = {"strategy_id": "test", "phase": phase,
                       "day": "2026-09-08", "key": "open-20260908",
                       "targets": {"OLD": 999999}, "order_ids": ["old-id"]}
    jq.g = pickle.loads(pickle.dumps(jq.g))
    runtime = jq.runtime(helper)
    assert jq.calls == []
    start(jq, runtime, {"BUY": 0.5})
    assert jq.calls == [("BUY", 5000)]
    assert jq.g.bt_jq_plan["phase"] == phase  # Inert old data, never used as execution state.
    assert not hasattr(runtime, "_jq_plan")


def test_none_from_native_order_does_not_start_a_retry_plan(helper):
    jq = JQ()
    runtime = jq.runtime(helper)
    calls = []
    helper._active_namespace["order_target_value"] = lambda *args, **kwargs: calls.append(args)
    result = start(jq, runtime, {"BUY": 0.5, "SECOND": 0.4})
    assert len(calls) == 2
    assert all(order is None for _, _, order in result["jq_orders"])
    assert not hasattr(runtime, "on_bar")


def test_risk_exit_still_uses_native_order_without_rebalance_plan(helper):
    jq = JQ(cash=5000, holdings={"BUY": 500})
    jq.prices["BUY"] = 9
    jq.refresh()
    runtime = jq.runtime(helper)
    result = runtime.execute_risk_management(jq.context, 0.95, 1.1, "risk")
    assert result["errors"] == []
    assert jq.calls == [("BUY", 0)]
    assert "BUY" not in jq.positions
    assert not hasattr(jq.g, "bt_jq_plan")
