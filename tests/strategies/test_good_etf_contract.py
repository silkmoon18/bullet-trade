# -*- coding: utf-8 -*-
"""GoodETF strategy-only contract tests.

Runtime protocol, RPC and mode validation are tested in
``tests/test_jq_strategy_runtime.py``. This module verifies that the strategy
keeps only deployment declarations and ETF decision logic.
"""

import ast
import importlib.util
import sys
import types
import uuid
from pathlib import Path

import pandas as pd
import pytest

from helpers import bullet_trade_jq_remote_helper as real_helper


ROOT = Path(__file__).resolve().parents[2]
STRATEGY_PATH = ROOT / "strategies" / "joinquant" / "good_etf.py"


class _Log:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(str(message))

    def warn(self, message):
        self.messages.append(str(message))

    def error(self, message):
        self.messages.append(str(message))

    def set_level(self, *args, **kwargs):
        return None


class _Context:
    def __init__(self, run_type="sim_trade"):
        self.run_params = types.SimpleNamespace(type=run_type)


class _Runtime:
    def __init__(self, mode, portfolio=None):
        self.mode = mode
        self.jq_account_enabled = mode in (
            real_helper.RuntimeMode.BACKTEST,
            real_helper.RuntimeMode.JQ,
            real_helper.RuntimeMode.JQ_QMT_PARALLEL,
        )
        self.qmt_account_enabled = mode in (
            real_helper.RuntimeMode.QMT_REMOTE,
            real_helper.RuntimeMode.JQ_QMT_PARALLEL,
        )
        self.state = {
            "api_version": 19,
            "strategy_id": "good_etf_remote",
            "mode": mode.value,
        }
        self._portfolio = portfolio
        self.advance_result = True
        self.cancel_target_result = True
        self.cancel_order_count = 0
        self.submissions = []
        self.order_calls = []
        self.notifications = []
        self.rebalances = []
        self.platform_configured = False
        self.schedules = []
        self.strategy_events = []

    def configure_platform(self, benchmark="000300.XSHG"):
        self.platform_configured = True

    def schedule_daily(
        self,
        before_market_open,
        market_open,
        risk_management,
        risk_check_times,
        after_market_check,
        reference_security="000300.XSHG",
    ):
        self.schedules.append(
            (
                before_market_open,
                market_open,
                risk_management,
                risk_check_times,
                after_market_check,
                reference_security,
            )
        )

    def log_process_initialize(self):
        return None

    def log_strategy_event(self, message):
        self.strategy_events.append(message)

    @staticmethod
    def security_label(security, known_name=""):
        return security

    def portfolio(self, context):
        return self._portfolio or context.portfolio

    def advance_targets(self, context):
        return self.advance_result

    def cancel_targets(self):
        return self.cancel_target_result

    def cancel_orders(self):
        return self.cancel_order_count

    def order_target(self, security, amount):
        self.order_calls.append(("amount", security, amount))
        return types.SimpleNamespace(order_id="amount-order")

    def order_target_value(self, security, value, limit_price=None):
        self.order_calls.append(("value", security, value, limit_price))
        return types.SimpleNamespace(order_id="value-order")

    def target_buy_plan_item(self, *args, **kwargs):
        return real_helper.JoinQuantRuntime.target_buy_plan_item(
            *args, **kwargs
        )

    def send_target_buy_plan(self, items, occurred_at=None):
        self.notifications.append((items, occurred_at))

    def submit_targets(self, context, weights, marks, key, execution):
        self.submissions.append((context, weights, marks, key, execution))
        return {"intent": {"intent_id": "intent-1", "state": "PLANNED"}}

    def execute_rebalance(self, context, weights, marks, key, execution=None):
        execution = execution or real_helper.default_etf_rebalance_execution()
        self.rebalances.append((context, weights, marks, key, execution))
        result = {"qmt": None, "jq_orders": [], "errors": []}
        if self.qmt_account_enabled:
            result["qmt"] = self.submit_targets(
                context, weights, marks, key, execution
            )
        if self.jq_account_enabled:
            total = float(self._portfolio.total_value)
            for security, weight in weights.items():
                order = types.SimpleNamespace(order_id="value-order")
                result["jq_orders"].append(
                    (security, total * float(weight), order)
                )
        return result

    def execute_risk_management(
        self,
        context,
        stop_loss_ratio,
        take_profit_ratio,
        key,
        stop_loss_execution=None,
        take_profit_execution=None,
    ):
        stop_loss_execution = (
            stop_loss_execution
            or real_helper.default_etf_stop_loss_execution()
        )
        take_profit_execution = (
            take_profit_execution
            or real_helper.default_etf_take_profit_execution()
        )
        checks = []
        stop_loss = []
        take_profit = []
        for security, position in self._portfolio.positions.items():
            action = "HOLD"
            if position.price < position.avg_cost * stop_loss_ratio:
                action = "STOP_LOSS"
                stop_loss.append(security)
            elif position.price > position.avg_cost * take_profit_ratio:
                action = "TAKE_PROFIT"
                take_profit.append(security)
            checks.append({
                "security": security,
                "price": position.price,
                "avg_cost": position.avg_cost,
                "pnl": position.price / position.avg_cost - 1,
                "action": action,
            })
        account = "QMT" if self.qmt_account_enabled else "JQ"
        account_result = {
            "account": account,
            "checks": checks,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "intent": None,
        }
        exits = stop_loss + take_profit
        if self.qmt_account_enabled and exits:
            weights = {
                security: (0.0 if security in exits else position.value / self._portfolio.total_value)
                for security, position in self._portfolio.positions.items()
            }
            execution = stop_loss_execution if stop_loss else take_profit_execution
            account_result["intent"] = self.submit_targets(
                context, weights, {}, key, execution
            )
        return {"accounts": [account_result], "errors": []}

    def log_account_snapshots(self, context):
        return None


def _helper_with_install(runtime, calls):
    helper = types.ModuleType("bullet_trade_jq_remote_helper")
    for name in (
        "RuntimeMode",
        "ExecutionRequest",
        "ConditionalLimitExecution",
        "LimitExecution",
        "ConditionalLimitPriceMode",
        "MarketExecution",
        "FollowUpPolicy",
        "RepricingPolicy",
        "is_hong_kong_etf",
    ):
        setattr(helper, name, getattr(real_helper, name))

    def install(namespace, **kwargs):
        calls.append((namespace, kwargs))
        return runtime

    helper.install_joinquant_runtime = install
    return helper


def _load_strategy(monkeypatch, helper_module=real_helper):
    jqdata = types.ModuleType("jqdata")
    jqdata.g = types.SimpleNamespace()
    jqdata.log = _Log()
    jqdata.__all__ = ["g", "log"]
    monkeypatch.setitem(sys.modules, "jqdata", jqdata)
    monkeypatch.setitem(
        sys.modules, "bullet_trade_jq_remote_helper", helper_module
    )
    name = "good_etf_contract_{}".format(uuid.uuid4().hex)
    spec = importlib.util.spec_from_file_location(name, STRATEGY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_strategy_source_compiles_and_stays_strategy_focused():
    source = STRATEGY_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(STRATEGY_PATH))
    compile(source, str(STRATEGY_PATH), "exec")
    imports = {
        node.names[0].name
        for node in tree.body
        if isinstance(node, ast.Import)
    }
    functions = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    direct_calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert not ({"socket", "ssl", "json", "sqlite3"} & imports)
    assert not ({
        "set_option",
        "set_benchmark",
        "set_order_cost",
        "set_slippage",
        "run_daily",
        "OrderCost",
        "FixedSlippage",
    } & direct_calls)
    assert all(
        execution_name not in source
        for execution_name in (
            "ExecutionRequest",
            "ConditionalLimitExecution",
            "MarketExecution",
            "FollowUpPolicy",
            "RepricingPolicy",
        )
    )
    assert len(source.splitlines()) <= 520
    assert functions == {
        "_install_runtime",
        "_load_tracked_index_names",
        "initialize",
        "process_initialize",
        "before_market_open",
        "market_open",
        "handle_data",
        "handle_risk_management",
        "after_market_check",
    }


def test_risk_check_times_are_top_level_configuration(monkeypatch):
    strategy = _load_strategy(monkeypatch)
    assert strategy.RISK_CHECK_TIMES == ("10:30", "13:30", "14:50")


def test_handle_data_is_only_a_runtime_delegation(monkeypatch):
    strategy = _load_strategy(monkeypatch)
    calls = []
    strategy._runtime = types.SimpleNamespace(on_bar=calls.append)
    context = _Context()
    strategy.handle_data(context, object())
    assert calls == [context]
    tree = ast.parse(STRATEGY_PATH.read_text(encoding="utf-8"))
    callback = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "handle_data")
    assert len(callback.body) == 2  # Docstring plus one helper call, no account logic.


def test_runtime_install_is_one_thin_helper_call(monkeypatch):
    calls = []
    runtime = _Runtime(real_helper.RuntimeMode.JQ)
    strategy = _load_strategy(
        monkeypatch, _helper_with_install(runtime, calls)
    )
    context = _Context()

    state = strategy._install_runtime(context)

    assert state == runtime.state
    assert strategy._runtime is runtime
    namespace, kwargs = calls[0]
    assert namespace is strategy.__dict__
    assert kwargs == {
        "context": context,
        "strategy_id": "good_etf_remote",
        "qmt_initial_capital": 10000,
        "expected_api_version": 19,
        "profile_module": "jq_runtime_config",
        "validate_remote_during_backtest": True,
    }


def test_good_etf_uses_public_hong_kong_etf_filter(monkeypatch):
    strategy = _load_strategy(monkeypatch)

    assert "bt.is_hong_kong_etf(" in STRATEGY_PATH.read_text(encoding="utf-8")
    assert strategy.bt.is_hong_kong_etf("520590.XSHG", "恒科") is True
    assert strategy.bt.is_hong_kong_etf(
        "510001.XSHG", "科技ETF", "恒生科技指数"
    ) is True
    assert strategy.bt.is_hong_kong_etf(
        "510002.XSHG", "央企ETF", "中证中央企业指数"
    ) is False


@pytest.mark.parametrize("excluded_code, excluded_name", [
    ("520890.XSHG", "港红利"),
    ("513320.XSHG", "HK新经济"),
    ("159322.XSHE", "黄金股ETF平安"),
])
def test_hk_short_name_fix_preserves_remaining_filters_ranking_and_weights(
    monkeypatch, excluded_code, excluded_name
):
    strategy = _load_strategy(monkeypatch)
    runtime = _Runtime(real_helper.RuntimeMode.JQ)
    strategy._runtime = runtime
    codes = [excluded_code, "510001.XSHG", "510002.XSHG", "510003.XSHG", "510004.XSHG"]
    names = [excluded_name, "港口航运ETF", "红利ETF", "科技ETF", "低流动性ETF"]
    context = types.SimpleNamespace(
        previous_date=pd.Timestamp("2026-09-03").date(),
        current_dt=pd.Timestamp("2026-09-04 09:30:00"),
    )

    def securities(types_, date):
        assert types_ == ["etf"] and date == context.previous_date
        return pd.DataFrame({"display_name": names}, index=codes)

    def history(count, unit, field, security_list):
        assert count == 1 and unit == "1d"
        assert security_list == codes[1:]
        values = [1e7, 1e7, 1e7, 1e6] if field == "money" else [2.0] * 4
        return pd.DataFrame([values], columns=security_list)

    def extras(field, security_list, end_date, df, count):
        assert field == "unit_net_value" and end_date == context.previous_date
        assert security_list == codes[1:4] and df and count == 1
        return pd.DataFrame([[2.0] * 3], columns=security_list)

    monkeypatch.setattr(strategy, "get_all_securities", securities, raising=False)
    monkeypatch.setattr(strategy, "history", history, raising=False)
    monkeypatch.setattr(strategy, "get_extras", extras, raising=False)
    # Missing index metadata is precisely when the short-name fallback matters.
    monkeypatch.setattr(strategy, "finance", None)
    prices = dict(zip(codes[1:4], [1.0, 1.5, 1.8]))
    monkeypatch.setattr(strategy, "get_current_data", lambda: {
        code: types.SimpleNamespace(last_price=price, paused=False, high_limit=3.0)
        for code, price in prices.items()
    }, raising=False)

    strategy.before_market_open(context)
    assert strategy.g.fund_list.index.tolist() == codes[1:4]
    strategy.market_open(context)
    assert len(runtime.rebalances) == 1
    _, weights, marks, _, _ = runtime.rebalances[0]
    premiums = [abs(price / 2.0 - 1) * 100 for price in prices.values()]
    assert list(weights) == codes[1:4]
    assert weights == {
        code: value / sum(premiums) * 0.95
        for code, value in zip(codes[1:4], premiums)
    }
    assert marks == prices


def test_lifecycle_installs_runtime_before_platform_calls():
    tree = ast.parse(
        STRATEGY_PATH.read_text(encoding="utf-8"),
        filename=str(STRATEGY_PATH),
    )
    for lifecycle in ("initialize", "process_initialize"):
        function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == lifecycle
        )
        body = list(function.body)
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        first = body[0]
        assert isinstance(first, ast.Expr)
        assert isinstance(first.value, ast.Call)
        assert isinstance(first.value.func, ast.Name)
        assert first.value.func.id == "_install_runtime"


def test_strategy_does_not_manage_account_readiness_or_reconciliation():
    source = STRATEGY_PATH.read_text(encoding="utf-8")

    assert ".ensure_ready(" not in source
    assert "ensure_runtime_ready" not in source
    assert ".qmt_account_enabled" not in source
    assert ".jq_account_enabled" not in source
    assert ".account_portfolios(" not in source


def test_initialize_delegates_platform_setup_and_scheduling_to_runtime(
    monkeypatch,
):
    calls = []
    runtime = _Runtime(real_helper.RuntimeMode.JQ)
    strategy = _load_strategy(
        monkeypatch, _helper_with_install(runtime, calls)
    )
    context = _Context()

    strategy.initialize(context)

    assert runtime.platform_configured is True
    assert len(runtime.schedules) == 1
    before, market, risk, times, after, reference = runtime.schedules[0]
    assert before is strategy.before_market_open
    assert market is strategy.market_open
    assert risk is strategy.handle_risk_management
    assert times == strategy.RISK_CHECK_TIMES
    assert after is strategy.after_market_check
    assert reference == "000300.XSHG"
    assert strategy.g.fund_list is None


def test_market_open_emits_one_account_neutral_weight_decision(monkeypatch):
    strategy = _load_strategy(monkeypatch)
    strategy.g.fund_list = pd.DataFrame(
        {"unit_net_value": [2.0]}, index=["510001.XSHG"]
    )
    current_data = {
        "510001.XSHG": types.SimpleNamespace(
            last_price=1.0, paused=False, high_limit=1.1
        )
    }
    old_position = types.SimpleNamespace(total_amount=100, avg_cost=1.0)
    portfolio = types.SimpleNamespace(
        total_value=10000.0,
        positions={"510002.XSHG": old_position},
    )
    runtime = _Runtime(real_helper.RuntimeMode.JQ, portfolio)
    strategy._runtime = runtime
    monkeypatch.setattr(
        strategy, "get_current_data", lambda: current_data, raising=False
    )

    strategy.market_open(
        types.SimpleNamespace(
            current_dt=pd.Timestamp("2026-08-19 09:30:00")
        )
    )

    assert len(runtime.rebalances) == 1
    _, weights, marks, key, _ = runtime.rebalances[0]
    assert weights == {"510001.XSHG": 0.95}
    assert marks == {"510001.XSHG": 1.0}
    assert key == "open-20260819"


def test_market_open_preserves_discount_ranking_filters_and_weights(
    monkeypatch,
):
    strategy = _load_strategy(monkeypatch)
    codes = [
        "510001.XSHG",
        "510002.XSHG",
        "510003.XSHG",
        "510004.XSHG",
        "510005.XSHG",
    ]
    strategy.g.fund_list = pd.DataFrame(
        {"unit_net_value": [2.0] * len(codes)}, index=codes
    )
    current_data = {
        "510001.XSHG": types.SimpleNamespace(
            last_price=1.0, paused=False, high_limit=1.1
        ),
        "510002.XSHG": types.SimpleNamespace(
            last_price=1.5, paused=False, high_limit=1.6
        ),
        "510003.XSHG": types.SimpleNamespace(
            last_price=1.8, paused=False, high_limit=1.9
        ),
        # 溢价标的必须排除。
        "510004.XSHG": types.SimpleNamespace(
            last_price=2.1, paused=False, high_limit=2.2
        ),
        # 即使折价更深，停牌标的也必须在折价排序前排除。
        "510005.XSHG": types.SimpleNamespace(
            last_price=0.5, paused=True, high_limit=0.6
        ),
    }
    portfolio = types.SimpleNamespace(total_value=10000.0, positions={})
    runtime = _Runtime(real_helper.RuntimeMode.JQ, portfolio)
    strategy._runtime = runtime
    monkeypatch.setattr(
        strategy, "get_current_data", lambda: current_data, raising=False
    )

    strategy.market_open(
        types.SimpleNamespace(
            current_dt=pd.Timestamp("2026-08-19 09:30:00")
        )
    )

    assert len(runtime.rebalances) == 1
    _, weights, marks, key, _ = runtime.rebalances[0]
    assert list(weights) == codes[:3]
    assert weights == pytest.approx({
        "510001.XSHG": 50.0 / 85.0 * 0.95,
        "510002.XSHG": 25.0 / 85.0 * 0.95,
        "510003.XSHG": 10.0 / 85.0 * 0.95,
    })
    assert marks == {
        "510001.XSHG": 1.0,
        "510002.XSHG": 1.5,
        "510003.XSHG": 1.8,
    }
    assert key == "open-20260819"


def test_remote_stop_loss_preempts_waiting_rebalance(monkeypatch):
    strategy = _load_strategy(monkeypatch)
    position = types.SimpleNamespace(
        price=9.0, avg_cost=10.0, value=9000.0
    )
    portfolio = types.SimpleNamespace(
        total_value=10000.0,
        positions={"510001.XSHG": position},
    )
    runtime = _Runtime(real_helper.RuntimeMode.QMT_REMOTE, portfolio)
    runtime.advance_result = False
    strategy._runtime = runtime

    strategy.handle_risk_management(
        types.SimpleNamespace(
            current_dt=pd.Timestamp("2026-08-20 10:30:00")
        )
    )

    assert len(runtime.submissions) == 1
    assert runtime.submissions[0][1] == {"510001.XSHG": 0.0}
    assert isinstance(
        runtime.submissions[0][4].style, real_helper.MarketExecution
    )


def test_remote_take_profit_keeps_reference_price_limit_execution(monkeypatch):
    strategy = _load_strategy(monkeypatch)
    position = types.SimpleNamespace(
        price=11.1, avg_cost=10.0, value=11100.0
    )
    portfolio = types.SimpleNamespace(
        total_value=11100.0,
        positions={"510001.XSHG": position},
    )
    runtime = _Runtime(real_helper.RuntimeMode.QMT_REMOTE, portfolio)
    strategy._runtime = runtime

    strategy.handle_risk_management(
        types.SimpleNamespace(
            current_dt=pd.Timestamp("2026-08-20 10:30:00")
        )
    )

    assert len(runtime.submissions) == 1
    assert runtime.submissions[0][1] == {"510001.XSHG": 0.0}
    execution = runtime.submissions[0][4]
    assert isinstance(execution.style, real_helper.LimitExecution)
    assert execution.style.price_band_ppm == 2_000
    assert execution.sell_style is None
