# -*- coding: utf-8 -*-
"""helpers/bullet_trade_jq_remote_helper.py（L00 精简契约）测试套件。

每个用例用 importlib.reload 重置 helper 模块级安装签名/状态与 _MODULE_TOKEN。
"""

import importlib
import socket
import sys
import types

import pytest

import helpers.bullet_trade_jq_remote_helper as _helper

PROFILE = "qmt-main"
STRATEGY_ID = "good_etf"
PROFILE_MODULE = "test_jq_runtime_config"
BLOCKED_MUTATIONS = tuple(sorted({
    "order", "order_value", "order_percent", "order_target",
    "order_target_value", "order_target_percent", "cancel_order",
}))


@pytest.fixture()
def helper():
    return importlib.reload(_helper)


def _context(run_type):
    return types.SimpleNamespace(run_params=types.SimpleNamespace(type=run_type))


def _valid_profile(**overrides):
    value = {"host": "127.0.0.1", "token": "unit-token"}
    value.update(overrides)
    return value


def _profile_module(
    monkeypatch, *, version=3, profiles=None, strategies=None,
    default_profile=PROFILE
):
    module = types.ModuleType(PROFILE_MODULE)
    module.PROFILE_SCHEMA_VERSION = version
    module.DEFAULT_PROFILE = default_profile
    module.PROFILES = {PROFILE: _valid_profile()} if profiles is None else profiles
    module.STRATEGIES = {} if strategies is None else strategies
    monkeypatch.setitem(sys.modules, PROFILE_MODULE, module)
    monkeypatch.setattr(
        _helper,
        "ensure_runtime_ready",
        lambda initial_capital, context: getattr(context, "portfolio", None),
    )


def _install(helper, namespace=None, context=None, *, mode="JQ", **kwargs):
    kwargs.setdefault("strategy_id", STRATEGY_ID)
    kwargs.setdefault("profile_module", PROFILE_MODULE)
    if type(mode) is str and mode.strip().upper() in (
        "JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL"
    ):
        module = sys.modules.get(kwargs["profile_module"])
        if module is not None and type(getattr(module, "STRATEGIES", None)) is dict:
            settings = module.STRATEGIES.setdefault(STRATEGY_ID, {})
            if type(settings) is dict:
                selected_mode = mode.strip().upper()
                settings["jq_account_enabled"] = selected_mode in (
                    "JQ", "JQ_QMT_PARALLEL"
                )
                settings["qmt_account_enabled"] = selected_mode in (
                    "QMT_REMOTE", "JQ_QMT_PARALLEL"
                )
    return helper.install_strategy_runtime(
        {} if namespace is None else namespace,
        context=_context("sim_trade") if context is None else context,
        mode=mode, **kwargs)


def _state(mode, run_type, **extra):
    jq_enabled = mode in ("BACKTEST", "JQ", "JQ_QMT_PARALLEL")
    qmt_enabled = mode in ("QMT_REMOTE", "JQ_QMT_PARALLEL")
    state = {
        "api_version": 19,
        "profile_schema_version": 3,
        "profile": None if mode == "BACKTEST" else PROFILE,
        "mode": mode,
        "run_type": run_type,
        "strategy_id": STRATEGY_ID,
        "jq_account_enabled": jq_enabled,
        "qmt_account_enabled": qmt_enabled,
        "enabled": mode != "BACKTEST",
        "orders_enabled": True,
        "production_ready": False,
        "reason": "backtest",
    }
    state.update(extra)
    return state


def test_public_contract_exports_and_constants(helper):
    assert {
        "PROFILE_SCHEMA_VERSION",
        "PortfolioView",
        "PositionView",
        "STRATEGY_RUNTIME_API_VERSION",
        "STRATEGY_RUNTIME_HELPER_MARKER",
        "HONG_KONG_ETF_KEYWORDS",
        "HONG_KONG_ETF_CODE_DENYLIST",
        "is_hong_kong_etf",
        "ensure_account",
        "get_intent",
        "get_portfolio",
        "get_reconciliation",
        "install_strategy_runtime",
        "notify_target_buy_plan",
        "submit_targets",
        "ExecutionRequest",
        "ConditionalLimitExecution",
        "LimitExecution",
        "MarketExecution",
        "default_etf_rebalance_execution",
        "default_etf_stop_loss_execution",
        "default_etf_take_profit_execution",
        "get_configured_execution_mode",
        "get_configured_account_switches",
        "submit_runtime_targets",
        "cancel_runtime_targets",
    }.issubset(set(helper.__all__))
    assert helper.STRATEGY_RUNTIME_API_VERSION == 19
    assert helper.STRATEGY_RUNTIME_HELPER_MARKER == (
        "bullet-trade-joinquant-runtime-helper-v19"
    )
    assert helper.PROFILE_SCHEMA_VERSION == 3


def test_public_hong_kong_etf_filter_supports_defaults_and_overrides(helper):
    assert helper.is_hong_kong_etf("520590.XSHG", "恒科") is True
    assert helper.is_hong_kong_etf("520890.XSHG", "港红利") is True
    assert helper.is_hong_kong_etf("520890.XSHG", "") is True
    assert helper.is_hong_kong_etf("520999.XSHG", "港红利ETF") is True
    assert helper.is_hong_kong_etf("510002.XSHG", "港口航运ETF") is False
    assert helper.is_hong_kong_etf("510003.XSHG", "红利ETF") is False
    assert helper.is_hong_kong_etf("513320.XSHG", "HK新经济") is True
    assert helper.is_hong_kong_etf("513320.XSHG", "") is True
    assert helper.is_hong_kong_etf("159322.XSHE", "黄金股ETF平安") is True
    assert helper.is_hong_kong_etf("513999.XSHG", "hk新经济") is True
    assert helper.is_hong_kong_etf(
        "159999.XSHE", "黄金股票ETF", "中证沪深港黄金产业股票指数"
    ) is True
    assert helper.is_hong_kong_etf("513100.XSHG", "纳指ETF") is False
    assert helper.is_hong_kong_etf("518880.XSHG", "黄金ETF") is False
    assert helper.is_hong_kong_etf(
        "159322.XSHE", "黄金股ETF平安", code_denylist=(), keywords=()
    ) is False
    assert helper.is_hong_kong_etf(
        "510001.XSHG", "科技ETF", "恒生科技指数"
    ) is True
    assert helper.is_hong_kong_etf(
        "510002.XSHG", "央企ETF", "中证中央企业指数"
    ) is False
    assert helper.is_hong_kong_etf(
        "510002.XSHG",
        "央企ETF",
        keywords=("央企",),
        code_denylist=(),
    ) is True


def test_default_etf_execution_policies_preserve_strategy_behavior(helper):
    rebalance = helper.default_etf_rebalance_execution()
    stop_loss = helper.default_etf_stop_loss_execution()
    take_profit = helper.default_etf_take_profit_execution()

    assert isinstance(rebalance.style, helper.LimitExecution)
    assert rebalance.style.price_band_ppm == 2_000
    assert isinstance(rebalance.sell_style, helper.MarketExecution)
    assert rebalance.sell_style.protect_price_band_ppm == 15_000
    assert isinstance(stop_loss.style, helper.MarketExecution)
    assert stop_loss.style.protect_price_band_ppm == 15_000
    assert isinstance(take_profit.style, helper.LimitExecution)
    assert take_profit.style.price_band_ppm == 2_000
    assert take_profit.sell_style is None
    assert stop_loss.follow_up is helper.FollowUpPolicy.UNTIL_FILLED_TODAY


def test_runtime_owns_platform_setup_and_schedule(helper):
    calls = []

    class Value:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    logger = types.SimpleNamespace(
        set_level=lambda *args: calls.append(("set_level", args)),
        info=lambda message: calls.append(("log", message)),
    )
    namespace = {
        "log": logger,
        "set_option": lambda *args: calls.append(("set_option", args)),
        "set_benchmark": lambda value: calls.append(("benchmark", value)),
        "OrderCost": Value,
        "set_order_cost": lambda value, **kwargs: calls.append(
            ("order_cost", value.kwargs, kwargs)
        ),
        "FixedSlippage": Value,
        "set_slippage": lambda value: calls.append(
            ("slippage", value.args)
        ),
        "run_daily": lambda callback, *args, **kwargs: calls.append(
            ("run_daily", callback, args, kwargs)
        ),
    }
    runtime = helper.JoinQuantRuntime(
        _state("JQ", "sim_trade"), namespace
    )
    callbacks = [lambda context: None for _ in range(4)]

    runtime.configure_platform()
    runtime.schedule_daily(
        callbacks[0],
        callbacks[1],
        callbacks[2],
        ("10:30", "13:30", "14:50"),
        callbacks[3],
    )

    assert ("set_option", ("avoid_future_data", True)) in calls
    assert ("set_option", ("use_real_price", True)) in calls
    assert ("benchmark", "000300.XSHG") in calls
    assert (
        "order_cost",
        {
            "close_tax": 0.0,
            "open_commission": 0.00025,
            "close_commission": 0.00025,
            "min_commission": 5,
        },
        {"type": "fund"},
    ) in calls
    assert ("slippage", (0.002,)) in calls
    schedules = [call for call in calls if call[0] == "run_daily"]
    assert [call[2] for call in schedules] == [
        ("09:20",),
        ("09:30",),
        (),
        (),
        (),
        (),
    ]
    assert [call[3].get("time") for call in schedules] == [
        None,
        None,
        "10:30",
        "13:30",
        "14:50",
        "14:55",
    ]


def test_execution_value_objects_are_typed_and_immutable(helper):
    first = helper.ExecutionRequest()
    second = helper.ExecutionRequest()

    assert first == second
    assert hash(first) == hash(second)
    assert first.style is not second.style
    assert first.style == helper.LimitExecution(2_000)
    assert first.style.execution_type is helper.ExecutionType.LIMIT
    assert "LimitExecution" in repr(first)
    with pytest.raises(AttributeError):
        first.follow_up = helper.FollowUpPolicy.NONE


def test_strategy_mode_is_per_strategy_and_missing_key_defaults_to_jq(
    helper, monkeypatch
):
    _profile_module(
        monkeypatch,
        strategies={"other": {
            "jq_account_enabled": False,
            "qmt_account_enabled": True,
        }},
    )

    assert helper.get_configured_execution_mode(
        STRATEGY_ID, PROFILE_MODULE
    ) == "JQ"

    sys.modules[PROFILE_MODULE].STRATEGIES[STRATEGY_ID] = {
        "jq_account_enabled": False,
        "qmt_account_enabled": True,
    }
    assert helper.get_configured_execution_mode(
        STRATEGY_ID, PROFILE_MODULE
    ) == "QMT_REMOTE"


def test_joinquant_runtime_facade_resolves_sim_mode_from_strategy_config(
    helper, monkeypatch
):
    _profile_module(
        monkeypatch,
        strategies={STRATEGY_ID: {
            "jq_account_enabled": False,
            "qmt_account_enabled": True,
        }},
    )
    namespace = {
        name: lambda *args, **kwargs: None for name in BLOCKED_MUTATIONS
    }

    runtime = helper.install_joinquant_runtime(
        namespace,
        context=_context("sim_trade"),
        strategy_id=STRATEGY_ID,
        qmt_initial_capital="10000",
        profile_module=PROFILE_MODULE,
    )

    assert runtime.mode is helper.RuntimeMode.QMT_REMOTE
    assert runtime.state["mode"] == "QMT_REMOTE"
    assert runtime.state["production_ready"] is True


def test_missing_strategy_uses_default_profile_and_jq(helper, monkeypatch):
    _profile_module(
        monkeypatch,
        strategies={"other": {
            "jq_account_enabled": False,
            "qmt_account_enabled": True,
        }},
    )

    runtime = helper.install_joinquant_runtime(
        {},
        context=_context("sim_trade"),
        strategy_id=STRATEGY_ID,
        qmt_initial_capital="10000",
        profile_module=PROFILE_MODULE,
    )

    assert runtime.mode is helper.RuntimeMode.JQ
    assert runtime.state["profile"] == PROFILE


def test_strategy_can_override_reusable_connection_profile(helper, monkeypatch):
    _profile_module(
        monkeypatch,
        profiles={
            PROFILE: _valid_profile(),
            "qmt-backup": _valid_profile(host="127.0.0.2"),
        },
        strategies={
            STRATEGY_ID: {
                "profile": "qmt-backup",
                "jq_account_enabled": True,
                "qmt_account_enabled": False,
            }
        },
    )

    runtime = helper.install_joinquant_runtime(
        {},
        context=_context("sim_trade"),
        strategy_id=STRATEGY_ID,
        qmt_initial_capital="10000",
        profile_module=PROFILE_MODULE,
    )

    assert runtime.state["profile"] == "qmt-backup"
    assert helper._active_profile["host"] == "127.0.0.2"


def test_joinquant_runtime_facade_backtest_does_not_load_profile_by_default(
    helper
):
    runtime = helper.install_joinquant_runtime(
        {},
        context=_context("full_backtest"),
        strategy_id=STRATEGY_ID,
        qmt_initial_capital="10000",
        profile_module="module_that_does_not_exist",
        validate_remote_during_backtest=False,
    )

    assert runtime.mode is helper.RuntimeMode.BACKTEST
    assert runtime.state["reason"] == "backtest"


def test_backtest_remote_validation_is_reused_after_runtime_reinstall(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    namespace = {"g": types.SimpleNamespace()}
    calls = []
    portfolio = helper.PortfolioView({
        "account_id": "paper",
        "as_of": "2026-08-20T09:30:00",
        "snapshot_version": "snapshot-1",
        "ledger_version": 1,
        "cash": 10000,
        "reserved_cash": 0,
        "available_cash": 10000,
        "positions_value": 0,
        "total_value": 10000,
        "starting_cash": 10000,
        "total_pnl": 0,
        "realized_pnl": 0,
        "unrealized_pnl": 0,
        "fees": 0,
        "nav": 1,
        "returns": 0,
        "performance_ready": True,
        "positions": {},
    })
    assert portfolio.fees_known is True

    def ensure_account(initial_capital):
        calls.append(("ensure", initial_capital))
        return {"reconciliation": {"state": "READY"}}

    def get_portfolio():
        calls.append(("portfolio", None))
        return portfolio

    def get_reconciliation():
        calls.append(("reconciliation", None))
        return {"reconciliation": {"state": "READY"}}

    monkeypatch.setattr(helper, "ensure_account", ensure_account)
    monkeypatch.setattr(helper, "get_portfolio", get_portfolio)
    monkeypatch.setattr(helper, "get_reconciliation", get_reconciliation)

    kwargs = {
        "context": _context("full_backtest"),
        "strategy_id": STRATEGY_ID,
        "qmt_initial_capital": "10000",
        "profile_module": PROFILE_MODULE,
        "validate_remote_during_backtest": True,
    }
    first = helper.install_joinquant_runtime(namespace, **kwargs)
    second = helper.install_joinquant_runtime(namespace, **kwargs)

    assert calls == [
        ("ensure", "10000"),
        ("portfolio", None),
        ("reconciliation", None),
    ]
    assert first.state["remote_validation"] == second.state["remote_validation"]
    assert namespace["g"].bt_remote_validation["total_value"] == 10000


def test_portfolio_view_preserves_unknown_remote_performance(helper):
    portfolio = helper.PortfolioView({
        "account_id": "paper",
        "as_of": "2026-08-20T09:30:00",
        "snapshot_version": "snapshot-unknown-fees",
        "ledger_version": 2,
        "cash": 8000,
        "reserved_cash": 0,
        "available_cash": 8000,
        "positions_value": 2000,
        "total_value": 10000,
        "starting_cash": 10000,
        "total_pnl": None,
        "realized_pnl": None,
        "unrealized_pnl": None,
        "fees": None,
        "fees_known": False,
        "unknown_fee_fill_count": 1,
        "nav": None,
        "returns": None,
        "performance_blockers": ["unknown_fill_fees"],
        "performance_ready": False,
        "positions": {
            "510050.XSHG": {
                "security": "510050.XSHG",
                "total_amount": 200,
                "closeable_amount": 200,
                "avg_cost": 10.0,
                "price": 10.1,
                "mark_as_of": "2026-08-20T09:29:59+08:00",
                "mark_source": "qmt",
                "value": 2020,
                "unrealized_pnl": 20,
            }
        },
    })

    assert portfolio.fees is None
    assert portfolio.fees_known is False
    assert portfolio.nav is None
    assert portfolio.performance_blockers == ("unknown_fill_fees",)
    assert portfolio.positions["510050.XSHG"].mark_as_of == (
        "2026-08-20T09:29:59+08:00"
    )
    assert portfolio.positions["510050.XSHG"].mark_source == "qmt"


def test_qmt_execution_summary_logs_target_fill_remainder_and_reserved_cash(
    helper, monkeypatch
):
    logger = types.SimpleNamespace(messages=[])
    logger.info = logger.messages.append
    logger.warn = logger.messages.append
    runtime = helper.JoinQuantRuntime(
        _state("QMT_REMOTE", "sim_trade"), {"log": logger}
    )
    portfolio = helper.PortfolioView(
        {
            "account_id": "paper",
            "as_of": "2026-09-02T14:55:00+08:00",
            "snapshot_version": "snapshot-progress",
            "ledger_version": 3,
            "cash": 6652.0,
            "reserved_cash": 2712.18,
            "available_cash": 3940.72,
            "positions_value": 3340.70,
            "total_value": 9993.60,
            "starting_cash": 10000,
            "total_pnl": None,
            "realized_pnl": None,
            "unrealized_pnl": None,
            "fees": None,
            "fees_known": False,
            "performance_blockers": ["unknown_fill_fees"],
            "performance_ready": False,
            "positions": {
                "589290.XSHG": {
                    "security": "589290.XSHG",
                    "total_amount": 500,
                    "closeable_amount": 0,
                    "avg_cost": 0.774,
                    "price": 0.779,
                    "value": 389.5,
                }
            },
        }
    )
    monkeypatch.setattr(
        helper,
        "get_intent",
        lambda *args, **kwargs: {
            "trading_day": "2026-09-02",
            "state": "EXECUTING",
            "targets": {"589290.XSHG": 4000, "589700.XSHG": 1600},
            "orders": [
                {
                    "security": "589290.XSHG",
                    "requested_qty": 4000,
                    "filled_qty": 500,
                    "state": "PARTIALLY_FILLED",
                }
            ],
        },
    )

    runtime._log_qmt_execution_summary(portfolio)

    assert "冻结资金=2712.18" in logger.messages[0]
    assert any(
        "589290.XSHG" in message
        and "目标=4000" in message
        and "当前=500" in message
        and "待买3500" in message
        and "累计成交=500" in message
        for message in logger.messages
    )
    assert any(
        "589700.XSHG" in message
        and "目标=1600" in message
        and "状态=等待价格" in message
        for message in logger.messages
    )


def test_portfolio_view_rejects_inconsistent_fee_status(helper):
    payload = {
        "account_id": "paper",
        "as_of": "2026-08-20T09:30:00",
        "snapshot_version": "bad-fees",
        "ledger_version": 2,
        "cash": 8000,
        "reserved_cash": 0,
        "available_cash": 8000,
        "positions_value": 2000,
        "total_value": 10000,
        "starting_cash": 10000,
        "total_pnl": None,
        "realized_pnl": None,
        "unrealized_pnl": None,
        "fees": None,
        "fees_known": True,
        "nav": None,
        "returns": None,
        "performance_ready": False,
        "positions": {},
    }

    with pytest.raises(RuntimeError, match="费用标记"):
        helper.PortfolioView(payload)


def test_runtime_facade_builds_round_lot_plan_and_logs_notification(
    helper, monkeypatch
):
    logger = types.SimpleNamespace(messages=[])
    logger.info = logger.messages.append
    logger.warn = logger.messages.append
    namespace = {"g": types.SimpleNamespace(), "log": logger}
    runtime = helper.JoinQuantRuntime(
        _state("JQ", "sim_trade"), namespace
    )
    sent = []
    monkeypatch.setattr(
        helper,
        "notify_target_buy_plan",
        lambda items, occurred_at=None: sent.append(
            (items, occurred_at)
        )
        or {
            "accepted": True,
            "item_count": len(items),
            "total_amount": 2000.0,
        },
    )
    item = runtime.target_buy_plan_item(
        "510300.XSHG", 3000.0, 1000.0, 2.0
    )

    result = runtime.send_target_buy_plan(
        [item], occurred_at="2026-08-20 09:30:00"
    )

    assert item == {
        "security": "510300.XSHG",
        "quantity": 1000,
        "amount": 2000.0,
        "reference_price": 2.0,
    }
    assert result["accepted"] is True
    assert sent == [([item], "2026-08-20 09:30:00")]
    assert any("计划卡片已提交" in message for message in logger.messages)


def test_runtime_facade_notification_failure_does_not_escape(
    helper, monkeypatch
):
    logger = types.SimpleNamespace(messages=[])
    logger.info = logger.messages.append
    logger.warn = logger.messages.append
    runtime = helper.JoinQuantRuntime(
        _state("JQ", "sim_trade"),
        {"g": types.SimpleNamespace(), "log": logger},
    )
    monkeypatch.setattr(
        helper,
        "notify_target_buy_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("network")
        ),
    )

    assert runtime.send_target_buy_plan([{"security": "510300.XSHG"}]) is None
    assert any("通知失败" in message for message in logger.messages)


@pytest.mark.parametrize("run_type", ["simple_backtest", "full_backtest"])
@pytest.mark.parametrize("context_kind", ["attr", "dict"])
def test_backtest_state_exact_without_profile_or_namespace_mutation(
    helper, run_type, context_kind
):
    if context_kind == "dict":
        run_params = {"type": run_type}
    else:
        run_params = types.SimpleNamespace(type=run_type)
    context = types.SimpleNamespace(run_params=run_params)
    original_order = lambda *args: "native"  # noqa: E731
    namespace = {"order": original_order, "get_orders": lambda: "query"}

    # profile 模块名合法但不存在；BACKTEST 不得导入它。
    state = _install(
        helper, namespace, context,
        mode="BACKTEST", profile_module="module_that_must_not_be_imported")

    assert state == _state("BACKTEST", run_type)
    assert "blocked_mutations" not in state
    assert namespace["order"] is original_order
    assert namespace["get_orders"]() == "query"
    assert namespace["__bt_strategy_runtime_state__"]["mode"] == "BACKTEST"


def test_backtest_rejects_sim_trade_run_type(helper):
    with pytest.raises(RuntimeError, match="仅允许聚宽回测"):
        _install(helper, mode="BACKTEST")


def test_jq_keeps_native_orders_and_loads_notification_profile(helper, monkeypatch):
    _profile_module(monkeypatch)
    original_order = lambda *args: "native"  # noqa: E731
    namespace = {"order": original_order}

    state = _install(
        helper,
        namespace,
        mode="JQ",
    )

    assert state == _state(
        "JQ", "sim_trade", reason="jq", profile_module=PROFILE_MODULE
    )
    assert namespace["order"] is original_order
    assert namespace["order"]("510300.XSHG", 100) == "native"
    assert namespace["__bt_strategy_runtime_state__"]["mode"] == "JQ"
    assert helper._active_profile["host"] == "127.0.0.1"


def test_jq_rejects_backtest_run_type(helper, monkeypatch):
    _profile_module(monkeypatch)
    with pytest.raises(RuntimeError, match="JQ仅允许聚宽模拟交易"):
        _install(helper, context=_context("full_backtest"), mode="JQ")


@pytest.mark.parametrize("mode", ["QMT_REMOTE"])
def test_remote_modes_reject_backtest_run_type(helper, monkeypatch, mode):
    _profile_module(monkeypatch)
    with pytest.raises(RuntimeError, match="仅允许聚宽模拟交易"):
        _install(helper, context=_context("full_backtest"), mode=mode)


def test_jq_state_exact(helper, monkeypatch):
    _profile_module(monkeypatch)
    assert _install(helper) == _state(
        "JQ", "sim_trade", reason="jq", profile_module=PROFILE_MODULE
    )


def test_qmt_remote_state_exact(helper, monkeypatch):
    _profile_module(monkeypatch)
    assert _install(helper, mode="QMT_REMOTE") == _state(
        "QMT_REMOTE", "sim_trade",
        reason="qmt_remote_profile_validated",
        profile_module=PROFILE_MODULE,
        mirror_jq_orders=False,
        blocked_mutations=BLOCKED_MUTATIONS)


def test_qmt_remote_replaces_seven_mutations_with_guards(helper, monkeypatch):
    _profile_module(monkeypatch)
    sentinel = object()
    namespace = {"order": sentinel, "get_orders": sentinel}
    state = _install(helper, namespace, mode="QMT_REMOTE")

    assert state["blocked_mutations"] == BLOCKED_MUTATIONS  # 七名排序元组
    for name in BLOCKED_MUTATIONS:
        assert namespace[name] is not sentinel  # 无论原名是否存在都被替换
        with pytest.raises(RuntimeError, match="QMT_REMOTE模式禁止交易变更"):
            namespace[name]("510001.XSHG", 100)
    assert namespace["get_orders"] is sentinel  # 非交易名字不受影响


def test_namespace_must_be_plain_dict(helper):
    class DictSubclass(dict):
        pass

    for candidate in (DictSubclass(), "not-a-dict"):
        with pytest.raises(RuntimeError, match="namespace必须是普通dict"):
            _install(helper, candidate)


@pytest.mark.parametrize(
    "version", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, "15", True, None]
)
def test_expected_api_version_must_equal_current(helper, version):
    with pytest.raises(RuntimeError, match="API版本不匹配"):
        _install(helper, expected_api_version=version)


def test_backtest_remote_validation_loads_profile_without_installing_guards(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    namespace = {"order": lambda *args: "native"}

    state = _install(
        helper,
        namespace,
        context=_context("simple_backtest"),
        mode="BACKTEST",
        validate_remote=True,
    )

    assert state["remote_validation_enabled"] is True
    assert state["reason"] == "backtest_remote_validation"
    assert namespace["order"]("510300.XSHG", 100) == "native"
    assert helper._active_profile["host"] == "127.0.0.1"


@pytest.mark.parametrize("mode", ["JQ", "QMT_REMOTE"])
def test_validate_remote_is_rejected_outside_backtest(helper, mode):
    with pytest.raises(RuntimeError, match="仅用于BACKTEST"):
        _install(helper, mode=mode, validate_remote=True)


def test_mode_is_normalised(helper, monkeypatch):
    _profile_module(monkeypatch)
    assert _install(helper, mode=" jq ")["mode"] == "JQ"


@pytest.mark.parametrize("mode", ["paper", "JQ_PAPER", "SIGNAL_ONLY", "", None, 1])
def test_invalid_mode_rejected(helper, mode):
    with pytest.raises(RuntimeError, match="运行模式必须是"):
        _install(helper, mode=mode)


@pytest.mark.parametrize("value", ["", " padded", "x" * 129, "bad name", 7])
def test_invalid_strategy_identifier_rejected(helper, value):
    with pytest.raises(RuntimeError, match="strategy_id"):
        _install(helper, strategy_id=value)


@pytest.mark.parametrize("name", ["", "bad name", "foo-bar", 7])
def test_invalid_profile_module_name_rejected(helper, name):
    with pytest.raises(RuntimeError, match="模块名"):
        _install(helper, profile_module=name)


def test_profile_module_import_failure_breaks_exception_chain(helper):
    with pytest.raises(RuntimeError, match="无法加载运行配置模块") as excinfo:
        _install(helper, profile_module="definitely_missing_jq_runtime_config")
    assert excinfo.value.__context__ is None
    assert excinfo.value.__cause__ is None


def test_profile_schema_version_mismatch(helper, monkeypatch):
    _profile_module(monkeypatch, version=1)
    with pytest.raises(RuntimeError, match="schema版本不匹配"):
        _install(helper)


def test_profiles_must_be_plain_dict(helper, monkeypatch):
    _profile_module(monkeypatch, profiles=["not-a-dict"])
    with pytest.raises(RuntimeError, match="必须定义字典 PROFILES"):
        _install(helper)


def test_missing_profile_rejected(helper, monkeypatch):
    _profile_module(monkeypatch, profiles={"other": _valid_profile()})
    with pytest.raises(RuntimeError, match="DEFAULT_PROFILE在PROFILES中不存在"):
        _install(helper)


def test_unknown_profile_field_rejected_without_echo(helper, monkeypatch):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(secret_field="x")})
    with pytest.raises(RuntimeError, match="包含未知字段") as excinfo:
        _install(helper)
    assert "secret_field" not in str(excinfo.value)


def test_missing_required_field_rejected(helper, monkeypatch):
    profile = _valid_profile()
    del profile["token"]
    _profile_module(monkeypatch, profiles={PROFILE: profile})
    with pytest.raises(RuntimeError, match="缺少必填字段: token"):
        _install(helper)


@pytest.mark.parametrize("host", ["", "has space", "x" * 256])
def test_invalid_host_rejected(helper, monkeypatch, host):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(host=host)})
    with pytest.raises(RuntimeError, match=r"profile\.host"):
        _install(helper)


@pytest.mark.parametrize("token", ["", " padded", 7])
def test_invalid_token_rejected(helper, monkeypatch, token):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(token=token)})
    with pytest.raises(RuntimeError, match=r"profile\.token"):
        _install(helper)


@pytest.mark.parametrize("port", [0, 65536, "58620", True])
def test_invalid_port_rejected(helper, monkeypatch, port):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(port=port)})
    with pytest.raises(RuntimeError, match=r"profile\.port"):
        _install(helper)


def test_multiple_strategies_can_share_one_connection_profile(helper, monkeypatch):
    _profile_module(
        monkeypatch,
        strategies={
            STRATEGY_ID: {"profile": PROFILE},
            "another_strategy": {"profile": PROFILE},
        },
    )
    assert _install(helper)["profile"] == PROFILE


_NUMERIC_FIELDS = {
    "rpc_timeout": (5.0, 300.0),
}
_OPTIONAL_STRING_FIELDS = ["account_key", "tls_cert"]


@pytest.mark.parametrize("field", sorted(_NUMERIC_FIELDS))
@pytest.mark.parametrize("value", ["10", True, None])
def test_profile_numeric_fields_reject_non_numeric(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    with pytest.raises(RuntimeError, match="必须是有限数值"):
        _install(helper)


@pytest.mark.parametrize("field", sorted(_NUMERIC_FIELDS))
@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_profile_numeric_fields_reject_non_finite(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    with pytest.raises(RuntimeError, match="必须是有限数值"):
        _install(helper)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("rpc_timeout", 4.9),
        ("rpc_timeout", 300.1),
    ],
)
def test_profile_numeric_fields_reject_out_of_range(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    with pytest.raises(RuntimeError, match="必须在"):
        _install(helper)


@pytest.mark.parametrize(
    ("field", "value"),
    [(f, bound) for f, bounds in _NUMERIC_FIELDS.items() for bound in bounds],
)
def test_profile_numeric_fields_accept_boundaries(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    assert _install(helper)["mode"] == "JQ"


@pytest.mark.parametrize("field", _OPTIONAL_STRING_FIELDS)
@pytest.mark.parametrize("value", [None, "main"])
def test_profile_optional_strings_accept_none_and_strings(
    helper, monkeypatch, field, value
):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    assert _install(helper)["mode"] == "JQ"


@pytest.mark.parametrize("field", _OPTIONAL_STRING_FIELDS)
@pytest.mark.parametrize("value", ["", " padded", 7])
def test_profile_optional_strings_reject_invalid(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    with pytest.raises(RuntimeError, match=r"profile\.{}".format(field)):
        _install(helper)


@pytest.mark.parametrize(
    ("field", "value"),
    [("port", 1), ("port", 65535)],
)
def test_profile_port_accepts_boundaries(helper, monkeypatch, field, value):
    _profile_module(monkeypatch, profiles={PROFILE: _valid_profile(**{field: value})})
    assert _install(helper)["mode"] == "JQ"


def test_profile_entry_must_be_plain_dict(helper, monkeypatch):
    _profile_module(monkeypatch, profiles={PROFILE: ["not-a-dict"]})
    with pytest.raises(RuntimeError, match="必须是字典"):
        _install(helper)


def test_profiles_names_must_be_plain_strings(helper, monkeypatch):
    _profile_module(monkeypatch, profiles={1: _valid_profile()})
    with pytest.raises(RuntimeError, match="profile名称必须是普通字符串"):
        _install(helper)


def test_profile_field_names_must_be_plain_strings(helper, monkeypatch):
    profile = _valid_profile()
    profile[1] = "x"
    _profile_module(monkeypatch, profiles={PROFILE: profile})
    with pytest.raises(RuntimeError, match="非普通字符串字段"):
        _install(helper)


def test_idempotent_reinstall_returns_equal_state(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {}
    first = _install(helper, namespace)
    second = _install(helper, namespace)
    assert first == second
    assert first is not second


def test_signature_drift_rejected(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {}
    _install(helper, namespace)
    module = sys.modules[PROFILE_MODULE]
    module.PROFILES["qmt-backup"] = _valid_profile()
    module.STRATEGIES[STRATEGY_ID]["profile"] = "qmt-backup"
    with pytest.raises(RuntimeError, match="签名漂移"):
        _install(helper, namespace)


def test_previous_generation_record_rejected(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {"__bt_strategy_runtime_state__": {"token": object(), "mode": "JQ"}}
    with pytest.raises(RuntimeError, match="上一代helper"):
        _install(helper, namespace)


def test_missing_namespace_record_rejected(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {}
    _install(helper, namespace)
    del namespace["__bt_strategy_runtime_state__"]
    with pytest.raises(RuntimeError, match="运行记录缺失"):
        _install(helper, namespace)


def test_idempotent_reinstall_restores_guards(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {}
    first = _install(helper, namespace, mode="QMT_REMOTE")
    # 模拟平台重建 namespace 后 guard 丢失，恢复出原生 order。
    namespace["order"] = lambda *args: "native"

    second = _install(helper, namespace, mode="QMT_REMOTE")

    assert second == first
    with pytest.raises(RuntimeError, match="QMT_REMOTE模式禁止交易变更"):
        namespace["order"]("510001.XSHG", 100)


def test_reinstall_with_changed_run_type_rejected(helper, monkeypatch):
    _profile_module(monkeypatch)
    namespace = {}
    _install(helper, namespace)
    with pytest.raises(RuntimeError, match="签名漂移"):
        _install(helper, namespace, context=_context("full_backtest"))


class _FakeSocket(object):
    def settimeout(self, timeout):
        self.timeout = timeout

    def close(self):
        self.closed = True


def test_read_only_rpc_retries_connection_then_succeeds(helper, monkeypatch):
    _profile_module(monkeypatch)
    _install(helper)
    attempts = []
    sent = []
    sleeps = []

    def connect(address, timeout):
        attempts.append((address, timeout))
        if len(attempts) < 3:
            raise OSError("temporary network failure")
        return _FakeSocket()

    def read_message(sock):
        if not sent or sent[-1]["type"] == "handshake":
            return {"type": "handshake_ack"}
        return {"type": "response", "id": sent[-1]["id"], "payload": {"ok": True}}

    monkeypatch.setattr(helper.socket, "create_connection", connect)
    monkeypatch.setattr(helper, "_send_message", lambda sock, message: sent.append(message))
    monkeypatch.setattr(helper, "_read_message", read_message)
    monkeypatch.setattr(helper.time, "sleep", sleeps.append)

    assert helper._strategy_request("strategy.get_snapshot", {}) == {"ok": True}
    assert len(attempts) == 3
    assert sleeps == [0.5, 0.5]


def test_submit_targets_does_not_retry_after_request_may_be_sent(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    _install(helper, mode="QMT_REMOTE")
    attempts = []
    reads = []

    def connect(address, timeout):
        attempts.append((address, timeout))
        return _FakeSocket()

    def read_message(sock):
        reads.append(True)
        if len(reads) == 1:
            return {"type": "handshake_ack"}
        raise socket.timeout("response lost")

    monkeypatch.setattr(helper.socket, "create_connection", connect)
    monkeypatch.setattr(helper, "_send_message", lambda sock, message: None)
    monkeypatch.setattr(helper, "_read_message", read_message)
    monkeypatch.setattr(
        helper.time, "sleep", lambda seconds: pytest.fail("must not retry")
    )

    with pytest.raises(RuntimeError, match="请求可能已执行.*停止自动重发"):
        helper.submit_targets({"510300.XSHG": 1}, "same-key")
    assert len(attempts) == 1


def test_jq_cannot_submit_qmt_targets(helper, monkeypatch):
    _profile_module(monkeypatch)
    _install(helper, mode="JQ")

    with pytest.raises(RuntimeError, match="只有启用QMT账户"):
        helper.submit_targets({"510300.XSHG": 1}, "jq-must-not-submit")


def test_typed_execution_request_is_explicitly_encoded_at_rpc_boundary(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    _install(helper, mode="QMT_REMOTE")
    calls = []
    monkeypatch.setattr(
        helper,
        "_strategy_request",
        lambda action, payload: calls.append((action, payload)) or {},
    )
    execution = helper.ExecutionRequest(
        style=helper.ConditionalLimitExecution(
            2_000, helper.ConditionalLimitPriceMode.BOUNDARY
        ),
        follow_up=helper.FollowUpPolicy.UNTIL_FILLED_TODAY,
        repricing=helper.RepricingPolicy.KEEP_ORIGINAL,
        sell_style=helper.MarketExecution(15_000),
    )

    helper.submit_targets(
        {"510300.XSHG": 1}, "typed-execution", execution=execution
    )

    assert calls == [
        (
            "strategy.submit_targets",
            {
                "weights": {"510300.XSHG": 1},
                "idempotency_key": "typed-execution",
                "execution": {
                    "schema_version": 2,
                    "style": {
                        "type": "CONDITIONAL_LIMIT",
                        "price_band_ppm": 2_000,
                        "price_mode": "BOUNDARY",
                    },
                    "sell_style": {
                        "type": "MARKET",
                        "protect_price_band_ppm": 15_000,
                    },
                    "follow_up": "UNTIL_FILLED_TODAY",
                    "repricing": "KEEP_ORIGINAL",
                },
            },
        )
    ]


def test_cancel_runtime_targets_clears_confirmed_remote_intent(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    _install(helper, mode="QMT_REMOTE")
    helper._runtime_target_state = {"intent_id": "intent-1"}
    calls = []
    monkeypatch.setattr(
        helper,
        "_strategy_request",
        lambda action, payload: calls.append((action, payload))
        or {"canceled": True},
    )

    assert helper.cancel_runtime_targets() is True
    assert helper._runtime_target_state is None
    assert calls == [
        ("strategy.cancel_intent", {"intent_id": "intent-1"})
    ]


@pytest.mark.parametrize("terminal_state", ["COMPLETED", "CANCELED", "FAILED"])
def test_advance_runtime_targets_clears_every_terminal_result(
    helper, monkeypatch, terminal_state
):
    _profile_module(monkeypatch)
    _install(helper, mode="QMT_REMOTE")
    helper._runtime_target_state = {
        "intent_id": "intent-1",
        "idempotency_key": "open-20260826",
        "weights": {"510300.XSHG": 0.95},
        "marks": {"510300.XSHG": 4.0},
        "execution": helper.ExecutionRequest(),
    }
    monkeypatch.setattr(
        helper, "get_intent", lambda intent_id: {"state": "EXECUTING"}
    )
    monkeypatch.setattr(
        helper,
        "submit_runtime_targets",
        lambda *args, **kwargs: {"intent": {"state": terminal_state}},
    )

    assert helper.advance_runtime_targets(object()) is True
    assert helper._runtime_target_state is None


def test_notify_target_buy_plan_uses_jq_mode_and_no_retry_after_send(
    helper, monkeypatch
):
    _profile_module(monkeypatch)
    _install(helper, mode="JQ")
    sent = []
    reads = []

    monkeypatch.setattr(
        helper.socket,
        "create_connection",
        lambda address, timeout: _FakeSocket(),
    )
    monkeypatch.setattr(
        helper, "_send_message", lambda sock, message: sent.append(message)
    )

    def read_message(sock):
        reads.append(True)
        if len(reads) == 1:
            return {"type": "handshake_ack"}
        raise socket.timeout("response lost")

    monkeypatch.setattr(helper, "_read_message", read_message)
    monkeypatch.setattr(
        helper.time, "sleep", lambda seconds: pytest.fail("must not retry")
    )

    with pytest.raises(RuntimeError, match="请求可能已执行.*停止自动重发"):
        helper.notify_target_buy_plan(
            [{"security": "510300.XSHG", "quantity": 1000, "amount": 2500}]
        )
    requests = [item for item in sent if item.get("type") == "request"]
    assert len(requests) == 1
    assert requests[0]["action"] == "strategy.notify_target_buy_plan"
    assert requests[0]["payload"]["mode"] == "JQ"


def test_server_error_is_not_retried_or_echoes_request_payload(helper, monkeypatch):
    _profile_module(monkeypatch)
    _install(helper)
    attempts = []
    reads = []

    monkeypatch.setattr(
        helper.socket,
        "create_connection",
        lambda address, timeout: attempts.append(address) or _FakeSocket(),
    )

    def read_message(sock):
        reads.append(True)
        if len(reads) == 1:
            return {"type": "handshake_ack"}
        return {"type": "error", "code": "ACCOUNT_NOT_READY", "message": "rejected"}

    monkeypatch.setattr(helper, "_send_message", lambda sock, message: None)
    monkeypatch.setattr(helper, "_read_message", read_message)
    monkeypatch.setattr(
        helper.time, "sleep", lambda seconds: pytest.fail("must not retry")
    )

    with pytest.raises(RuntimeError, match="ACCOUNT_NOT_READY: rejected") as excinfo:
        helper._strategy_request("strategy.ensure_account", {"secret": "do-not-echo"})
    assert "do-not-echo" not in str(excinfo.value)
    assert len(attempts) == 1
