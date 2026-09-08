"""JQ market-order continuation, independent of the remote ledger."""

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


def test_failed_exit_blocks_buys_but_target_reduction_also_runs_first(helper):
    jq = JQ(cash=1000, holdings={"OLD": 400, "TRIM": 500})
    jq.blocked.add("OLD")
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.5, "TRIM": 0.2})
    assert jq.calls == [("OLD", 0), ("TRIM", 2000)]
    assert runtime._jq_plan["phase"] == "SELL"
    runtime.on_bar(jq.context)  # Same bar must not repeatedly cancel/retry.
    assert len(jq.calls) == 2
    jq.blocked.clear()
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.calls[2:] == [("OLD", 0), ("BUY", 5000)]
    assert runtime._jq_plan["phase"] == "COMPLETED"
    assert "OLD" not in jq.positions
    assert jq.positions["TRIM"].total_amount == 200
    assert jq.positions["BUY"].total_amount == 500


def test_active_sell_waits_without_duplicate_then_buy_waits_for_fill(helper):
    jq = JQ(cash=1000, holdings={"OLD": 900})
    jq.pending = {"OLD", "BUY"}
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.8})
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.calls == [("OLD", 0)]
    jq.fill("OLD", -900)
    jq.open.clear()
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.calls[-1] == ("BUY", 8000)
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert len(jq.calls) == 2
    jq.fill("BUY", 800)
    jq.open.clear()
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert runtime._jq_plan["phase"] == "COMPLETED"


def test_partial_buy_resumes_from_pickle_without_chasing_completed_targets(helper):
    jq = JQ()
    jq.partial_once["BUY"] = 100
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.5, "SECOND": 0.4})
    assert jq.positions["BUY"].total_amount == 100
    assert runtime._jq_plan["phase"] == "BUY"
    jq.g = pickle.loads(pickle.dumps(jq.g))
    runtime = jq.runtime(importlib.reload(helper))
    start(jq, runtime, {"BUY": 0.5, "SECOND": 0.4})
    assert len(jq.calls) == 2
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.positions["BUY"].total_amount == 500
    assert jq.calls[-1] == ("BUY", 5000)
    assert runtime._jq_plan["phase"] == "COMPLETED"
    jq.prices["BUY"] = 5
    jq.refresh()
    jq.context.current_dt += timedelta(minutes=1)
    start(jq, runtime, {"BUY": 0.5, "SECOND": 0.4})
    runtime.on_bar(jq.context)
    assert len(jq.calls) == 3  # Same decision cannot rebalance again after completion.


def test_lunch_after_close_and_expiry_never_retry_old_target(helper):
    jq = JQ()
    jq.blocked.add("BUY")
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.8})
    for hour, minute in [(11, 30), (12, 0), (15, 0), (23, 0)]:
        jq.context.current_dt = jq.context.current_dt.replace(hour=hour, minute=minute)
        runtime.on_bar(jq.context)
    assert len(jq.calls) == 1
    jq.context.current_dt = datetime(2026, 9, 9, 9, 30)
    runtime.on_bar(jq.context)
    assert runtime._jq_plan["phase"] == "EXPIRED"
    assert len(jq.calls) == 1


def test_t1_unsellable_position_prevents_buying_a_fourth_position(helper):
    jq = JQ(cash=5000, holdings={"OLD": 500})
    jq.positions["OLD"].closeable_amount = 0
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.8})
    assert jq.calls == []
    assert runtime._jq_plan["phase"] == "SELL"


def test_risk_exit_stops_partial_rebalance_from_buying_back(helper):
    jq = JQ()
    jq.partial_once["BUY"] = 100
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.8})
    jq.open["1"] = NS(order_id="1", security="BUY", amount=800, filled=100)
    jq.positions["BUY"].closeable_amount = 100
    jq.prices["BUY"] = 9
    jq.refresh()
    result = runtime.execute_risk_management(jq.context, 0.95, 1.1, "risk")
    assert result["errors"] == []
    assert runtime._jq_plan["phase"] == "CANCELED"
    assert jq.open == {}
    assert "BUY" not in jq.positions
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert len(jq.calls) == 2


def test_round_lot_tolerance_does_not_trigger_an_unwanted_extra_sale(helper):
    jq = JQ(cash=7500, holdings={"TRIM": 250})
    runtime = jq.runtime(helper)
    start(jq, runtime, {"TRIM": 0.15})
    assert jq.calls == [("TRIM", 1500)]  # Exactly one lot reduction.
    assert runtime._jq_plan["phase"] == "COMPLETED"


def test_qmt_only_bar_never_uses_jq_or_remote_order_functions(helper):
    runtime = helper.JoinQuantRuntime({"mode": "QMT_REMOTE", "jq_account_enabled": False,
                                      "qmt_account_enabled": True}, {})
    runtime.on_bar(NS())
    assert runtime._jq_plan is None


def test_selected_buys_keep_weight_order_even_for_existing_positions(helper):
    jq = JQ(cash=8000, holdings={"SECOND": 200})
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.4, "SECOND": 0.5})
    assert jq.calls == [("BUY", 4000), ("SECOND", 5000)]
    assert runtime._jq_plan["phase"] == "COMPLETED"


def test_insufficient_cash_does_not_mark_a_large_remaining_target_completed(helper):
    jq = JQ()
    jq.spendable_limit = 1000
    runtime = jq.runtime(helper)
    start(jq, runtime, {"BUY": 0.3})
    assert runtime._jq_plan["phase"] == "BUY"
    assert jq.positions["BUY"].total_amount == 100
    jq.spendable_limit = None
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.positions["BUY"].total_amount == 300
    assert runtime._jq_plan["phase"] == "COMPLETED"


def test_null_order_is_not_completion_and_future_bar_can_retry(helper):
    jq = JQ()
    runtime = jq.runtime(helper)
    original = helper._active_namespace["order_target_value"]
    helper._active_namespace["order_target_value"] = lambda *args, **kwargs: None
    start(jq, runtime, {"BUY": 0.8})
    assert runtime._jq_plan["phase"] == "BUY"
    assert jq.calls == []
    helper._active_namespace["order_target_value"] = original
    jq.context.current_dt += timedelta(minutes=1)
    runtime.on_bar(jq.context)
    assert jq.positions["BUY"].total_amount == 800
    assert runtime._jq_plan["phase"] == "COMPLETED"
