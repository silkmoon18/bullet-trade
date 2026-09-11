"""No broker/network access: load the directly editable QMT script against fakes."""
import ast
import datetime as dt
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import tokenize

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
NOW = dt.datetime(2026, 9, 11, 9, 30)


def read_strategy():
    with tokenize.open(str(ROOT / "good_etf.py")) as stream:
        return stream.read()


@pytest.fixture
def mod(monkeypatch):
    module = types.ModuleType("native_good_etf_test")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile((ROOT / "good_etf.py").read_bytes(), "good_etf.py", "exec"), module.__dict__)
    return module


class FakeQmt:
    def __init__(self):
        self.cash, self.sent, self.canceled, self.history_queries = 10000, [], [], []
        self.order_rows, self.deal_rows, self.position_rows = [], [], []
        self.details, self.ticks, self.pcf = {}, {}, {}
        self.fail_send = False

    def get_instrument_detail(self, code):
        return self.details.get(code, dict(InstrumentName="测试ETF", OpenDate=20200101, ExpireDate=99999999,
            PriceTick=0.001, IsTrading=None, UpStopPrice=20.0, DownStopPrice=0.01))

    def get_full_tick(self, codes):
        return {code: self.ticks.get(code, dict(lastPrice=1.0, timetag="20260911 09:30:01", openInt=13)) for code in codes}

    def get_trade_detail_data(self, account, kind, datatype):
        return {"account": [dict(m_dAvailable=self.cash)], "position": self.position_rows,
                "order": self.order_rows, "deal": self.deal_rows}[datatype]

    def get_history_trade_detail_data(self, *args):
        self.history_queries.append(args)
        return []

    def get_etf_info(self, code):
        return self.pcf.get(code, {})

    def passorder(self, *args):
        self.sent.append(args)
        if self.fail_send:
            raise TimeoutError("test timeout after dispatch")
        return None

    def cancel(self, *args):
        self.canceled.append(args)
        return True

    def get_trading_dates(self, *args):
        return ["20260910", "20260911"]

    def set_account(self, account):
        self.account = account

    def run_time(self, *args):
        self.schedule = args

    def namespace(self):
        return {name: getattr(self, name) for name in (
            "get_trade_detail_data", "get_history_trade_detail_data", "passorder", "cancel", "get_etf_info")}


@pytest.fixture
def env(mod, tmp_path):
    fake = FakeQmt()
    config = mod.Settings()
    config.STATE_DIR, config.ENABLE_TRADING, config.RETRY_SECONDS = str(tmp_path), True, 0
    data = mod.QmtData(fake, fake.namespace(), config, lambda text: None)
    events = []
    executor = mod.Executor(data, config, "mock-account", lambda *args: events.append(args))
    yield fake, executor, data, config, events
    executor.close()


def target(executor, code="510300.SH", qty=1000, ref=1.0, style="limit"):
    executor.state["targets"][code] = executor.target(qty, ref, style, "20260911")


def latest(executor):
    return list(executor.state["orders"].values())[-1]


def row(order, **kw):
    code, exchange = order["code"].split(".")
    result = dict(m_strAccountID="mock-account", m_strInstrumentID=code, m_strExchangeID=exchange,
                  m_strRemark=order["tag"], m_strOrderSysID="mock-system-id", m_strInsertDate=order["day"],
                  m_strTradeDate=order["day"], m_nOffsetFlag=48 if order["side"] == "BUY" else 49)
    result.update(kw)
    return result


def test_selection_contract_multisecurity(mod):
    codes = ["510001.SH", "510002.SH", "510003.SH", "510004.SH", "510005.SH", "510006.SH", "510007.SH"]
    frame = pd.DataFrame({"unit_net_value": [2.0] * 7}, index=codes)
    prices = [1.90, 1.98, 1.80, 1.96, 1.50, 1.40, 2.10]
    quotes = {code: dict(last_price=p, paused=False, high_limit=3) for code, p in zip(codes, prices)}
    quotes[codes[4]]["paused"] = True
    quotes[codes[5]]["high_limit"] = 1.40
    result = mod.select(frame, quotes)
    expected_codes = [codes[2], codes[0], codes[3]]
    raw = [abs(quotes[code]["last_price"] / 2 - 1) * 100 for code in expected_codes]
    assert list(result.weights) == expected_codes
    assert result.weights == {code: weight / sum(raw) * 0.95 for code, weight in zip(expected_codes, raw)}
    assert result.marks == {code: quotes[code]["last_price"] for code in expected_codes}


def test_prepare_filter_order_boundaries_and_nav(mod):
    class Data:
        def universe(self, previous):
            return [("520590.SH", "恒科", ""), ("510000.SH", "普通ETF", "恒生指数"),
                    ("510001.SH", "低边界", ""), ("510002.SH", "高边界", ""), ("510003.SH", "科技", "")]

        def daily(self, codes, previous):
            assert codes == ["510001.SH", "510002.SH", "510003.SH"]
            assert previous == "20260910"
            return pd.DataFrame({"money": [5e6, 2e7, 1e7]}, index=codes)

        def nav(self, codes, previous, today):
            assert codes == ["510003.SH"]
            return pd.DataFrame({"unit_net_value": [1.2]}, index=codes)
    frame = mod.prepare(Data(), "20260910", "20260911")
    assert frame.index.tolist() == ["510003.SH"]
    assert mod.is_hong_kong_etf("513320.SH", "新经济")
    assert not mod.is_hong_kong_etf("513500.SH", "标普500")


def test_risk_strict_thresholds_and_empty_selection(mod):
    assert mod.risk_signal(1, .95) is None
    assert mod.risk_signal(1, 1.10) is None
    assert mod.risk_signal(1, .949) == "stop_loss"
    assert mod.risk_signal(1, 1.101) == "take_profit"
    result = mod.select(pd.DataFrame({"unit_net_value": [1.]}, index=["510300.SH"]),
                        {"510300.SH": dict(last_price=1.1, paused=False, high_limit=2)})
    assert result == mod.Decision({}, {})


def test_nav_requires_dates_not_iopv(env):
    fake, ex, data, config, events = env
    fake.pcf["510300.SH"] = dict(nav=1.2, iopv=1.1, preTradingDay="", tradingDay="")
    with pytest.raises(RuntimeError, match="单位净值"):
        data.nav(["510300.SH"], "20260910", "20260911")
    fake.pcf["510300.SH"].update(preTradingDay="20260910", tradingDay="20260911")
    assert data.nav(["510300.SH"], "20260910", "20260911").iloc[0, 0] == 1.2
    fake.pcf["510300.SH"]["preTradingDay"] = "20260909"
    with pytest.raises(RuntimeError):
        data.nav(["510300.SH"], "20260910", "20260911")


def test_quote_formats_suspension_none_is_trading(env):
    fake, ex, data, config, events = env
    quote = data.quotes(["510300.SH"], NOW)["510300.SH"]
    assert quote["paused"] is False  # 官方示例 IsTrading=None，不能当成全部停牌
    fake.ticks["510300.SH"] = dict(lastPrice=1, time=int(NOW.timestamp() * 1000), openInt=17)
    assert data.quotes(["510300.SH"], NOW)["510300.SH"]["paused"] is True
    fake.ticks["510300.SH"]["openInt"] = 13
    fake.ticks["510300.SH"]["time"] = int((NOW - dt.timedelta(days=1)).timestamp() * 1000)
    with pytest.raises(RuntimeError):
        data.quotes(["510300.SH"], NOW)


def test_no_preopen_quote_used_as_opening_price(env):
    fake, ex, data, config, events = env
    fake.ticks["510300.SH"] = dict(lastPrice=1, timetag="20260911 09:25:00", openInt=12)
    with pytest.raises(RuntimeError, match="开盘后"):
        data.quotes(["510300.SH"], NOW)


def test_previous_daily_date_and_missing_inner_join(env):
    fake, ex, data, config, events = env
    fake.get_market_data_ex = lambda *args, **kwargs: {
        "510300.SH": pd.DataFrame({"high": [1.2], "low": [1], "amount": [1e7]}, index=["20260910"]),
        "510500.SH": pd.DataFrame({"high": [1.2], "low": [1], "amount": [1e7]}, index=["20260909"])}
    result = data.daily(["510300.SH", "510500.SH"], "20260910")
    assert result.index.tolist() == ["510300.SH"]
    assert result.iloc[0]["money"] == 1e7


def test_nav_file_date_and_valid_override(env, tmp_path):
    fake, ex, data, config, events = env
    config.NAV_FILE = str(tmp_path / "nav.json")
    Path(config.NAV_FILE).write_text(json.dumps(dict(date="20260910", nav={"510300.SH": 1.05})))
    assert data.nav(["510300.SH"], "20260910", "20260911").iloc[0, 0] == 1.05
    with pytest.raises(RuntimeError, match="日期"):
        data.nav(["510300.SH"], "20260909", "20260910")


def test_fixed_opening_limit_not_ask_and_no_cage(env):
    fake, ex, data, config, events = env
    fake.ticks["510300.SH"] = dict(lastPrice=2, askPrice=[2.1], time=int(NOW.timestamp() * 1000))
    target(ex)
    ex.advance(NOW)
    args = fake.sent[0]
    assert args[:2] == (23, 1101)
    assert args[4:7] == (11, 1.002, 1000)
    assert args[8] == 2
    assert len(args[9]) < 24
    assert ex.state["cash"] == 10000  # 发单不是成交
    ex.advance(NOW)
    assert len(fake.sent) == 1


def test_partial_fills_deduplicate_and_fee_unknown(env):
    fake, ex, data, config, events = env
    target(ex, qty=3000)
    ex.advance(NOW)
    order = latest(ex)
    fill = row(order, m_strTradeID="trade1", m_nVolume=2000, m_dPrice=1.0)
    ex.report("deal", fill)
    ex.report("deal", fill)
    assert ex.state["cash"] == 8000
    assert ex.state["positions"]["510300.SH"]["qty"] == 2000
    assert next(iter(ex.state["fills"].values()))["fee"] is None
    ex.report("deal", dict(fill, m_dCommission=5.0))
    ex.report("deal", dict(fill, m_dCommission=5.0))
    assert ex.state["cash"] == 7995
    assert len(events) == 2  # 一次下单、一次成交


def test_status_55_is_live_53_is_terminal_wait_for_deal(env, mod):
    fake, ex, data, config, events = env
    target(ex, qty=1000)
    ex.advance(NOW)
    order = latest(ex)
    ex.report("order", row(order, m_nOrderStatus=55, m_nVolumeTraded=200))
    assert not mod.settled(order)
    ex.advance(NOW)
    assert len(fake.sent) == 1
    ex.report("order", row(order, m_nOrderStatus=53, m_nVolumeTraded=200))
    ex.advance(NOW)
    assert len(fake.sent) == 1  # 部撤回报在成交明细之前到达，不得抢跑
    ex.report("deal", row(order, m_strTradeID="trade1", m_nVolume=200, m_dPrice=1.0))
    fake.position_rows = [dict(m_strInstrumentID="510300", m_strExchangeID="SH", m_nVolume=200, m_nCanUseVolume=0)]
    ex.advance(NOW)
    assert len(fake.sent) == 2
    assert fake.sent[-1][6] == 800


def test_sell_then_buy_and_t1(env):
    fake, ex, data, config, events = env
    ex.state["positions"]["510300.SH"] = dict(qty=1000, cost=1000)
    ex.state["cash"] = 9000
    fake.position_rows = [dict(m_strInstrumentID="510300", m_strExchangeID="SH", m_nVolume=1000, m_nCanUseVolume=0)]
    target(ex, qty=0, style="market")
    target(ex, code="159001.SZ", qty=1000)
    ex.advance(NOW)
    assert not fake.sent
    fake.position_rows[0]["m_nCanUseVolume"] = 1000
    ex.advance(NOW)
    assert len(fake.sent) == 1 and fake.sent[0][0] == 24
    assert fake.sent[0][4:6] == (42, .985)
    order = latest(ex)
    ex.report("order", row(order, m_nOrderStatus=56, m_nVolumeTraded=1000))
    ex.advance(NOW)
    assert len(fake.sent) == 1
    ex.report("deal", row(order, m_strTradeID="sell1", m_nVolume=1000, m_dPrice=1.0, m_dCommission=0))
    fake.position_rows = []
    ex.advance(NOW)
    assert fake.sent[-1][0] == 23 and fake.sent[-1][3] == "159001.SZ"
    assert ex.state["cash"] == 10000


def test_manual_position_not_owned_and_no_nonexistent_sell(env, mod):
    fake, ex, data, config, events = env
    fake.position_rows = [dict(m_strInstrumentID="510300", m_strExchangeID="SH", m_nVolume=1000, m_nCanUseVolume=1000)]
    ex.rebalance(mod.Decision({}, {}), NOW)
    ex.advance(NOW)
    assert not fake.sent and ex.state["cash"] == 10000


def test_unknown_submit_not_retried_after_restart(env, mod):
    fake, ex, data, config, events = env
    target(ex)
    fake.fail_send = True
    with pytest.raises(RuntimeError, match="未知"):
        ex.advance(NOW)
    ex.close()
    restored = mod.Executor(data, config, "mock-account", lambda *args: None)
    try:
        restored.advance(NOW)
        assert len(fake.sent) == 1
    finally:
        restored.close()


def test_midnight_cancel_does_not_fake_completion(env, mod):
    fake, ex, data, config, events = env
    target(ex)
    ex.advance(NOW)
    order = latest(ex)
    ex.report("order", row(order, m_nOrderStatus=50, m_nVolumeTraded=0))
    ex.expire("20260912")
    assert fake.canceled and not ex.state["targets"]
    assert not mod.settled(order)
    ex.sync("20260912")
    assert len(fake.history_queries) == 2
    assert not mod.settled(order)


def test_disabled_has_no_order_or_cancel(env, mod):
    fake, ex, data, config, events = env
    target(ex)
    config.ENABLE_TRADING = False
    ex.advance(NOW)
    ex.rebalance(mod.Decision({"510300.SH": .95}, {"510300.SH": 1}), NOW)
    ex.cancel(dict(status=50, sysid="x", code="510300.SH"))
    assert not fake.sent and not fake.canceled


def test_rejection_does_not_retry_and_capital_not_reset(env):
    fake, ex, data, config, events = env
    target(ex)
    ex.advance(NOW)
    ex.report("order", row(latest(ex), m_nOrderStatus=57, m_nVolumeTraded=0, m_strCancelInfo="模拟柜台不支持"))
    ex.advance(NOW)
    assert len(fake.sent) == 1
    assert any("模拟柜台不支持" in event[-1] for event in events)
    assert ex.state["cash"] == 10000


def test_buy_cash_reservations_across_symbols(env):
    fake, ex, data, config, events = env
    ex.state["cash"] = 150
    target(ex, code="510300.SH", qty=100)
    target(ex, code="159001.SZ", qty=100)
    ex.advance(NOW)
    assert len(fake.sent) == 1


def test_risk_cancels_buy_and_does_not_reprice_existing_exit(env, mod):
    fake, ex, data, config, events = env
    target(ex, qty=1000)
    ex.advance(NOW)
    order = latest(ex)
    ex.report("deal", row(order, m_strTradeID="first", m_nVolume=200, m_dPrice=1))
    ex.report("order", row(order, m_nOrderStatus=55, m_nVolumeTraded=200))
    fake.ticks["510300.SH"] = dict(lastPrice=.90, time=int(NOW.timestamp() * 1000))
    ex.risk(mod.risk_signal, NOW)
    assert fake.canceled and ex.state["targets"]["510300.SH"]["qty"] == 0
    fake.ticks["510300.SH"]["lastPrice"] = .89
    ex.risk(mod.risk_signal, NOW)
    assert ex.state["targets"]["510300.SH"]["reference"] == .90


def test_historical_bars_never_send_and_strategy_boundary(mod):
    fake = FakeQmt()
    for _ in range(100):
        mod.handlebar(fake)
    assert fake.sent == []
    tree = ast.parse(read_strategy())
    decision_functions = {"prepare", "select", "risk_signal", "init", "stop", "handlebar", "on_timer",
                          "order_callback", "deal_callback", "orderError_callback"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in decision_functions]
    assert {node.name for node in nodes} == decision_functions
    calls = {call.func.id for node in nodes for call in ast.walk(node)
             if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)}
    assert not ({"passorder", "cancel", "get_trade_detail_data", "open", "run_time"} & calls)
    imports = {node.module for node in tree.body if isinstance(node, ast.ImportFrom)}
    assert not imports & {"data", "runtime", "settings", "execution"}


def test_configuration_is_only_at_script_top(mod):
    source = read_strategy()
    tree = ast.parse(source, feature_version=(3, 6))
    top, rest = source.split("# ==================== 配置结束", 1)
    config_names = {node.targets[0].id for node in ast.parse(top).body
                    if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)}
    assert config_names == {
        "ACCOUNT_ID", "STRATEGY_ID", "ENABLE_TRADING", "INITIAL_CAPITAL", "STATE_DIR",
        "ETF_SECTOR", "NAV_FILE", "BACKTEST_DATA_FILE", "TRACKED_INDEX_NAMES", "FEISHU_WEBHOOK",
        "MAX_HOLD_NUM", "MIN_MONEY", "MAX_MONEY", "STOP_LOSS_RATIO", "TAKE_PROFIT_RATIO",
        "DEPLOY_RATIO", "SKIP_SUSPENDED_LIMITUP", "HK_WORDS", "HK_CODES",
        "PREPARE_TIME", "OPEN_TIME", "RISK_CHECK_TIMES", "SNAPSHOT_TIME",
        "BUY_PREMIUM", "PROFIT_DISCOUNT", "MARKET_PROTECTION", "SH_NATIVE_MARKET",
        "QUERY_SECONDS", "RETRY_SECONDS", "CASH_RESERVE"}
    for name in config_names:
        assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                       and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name]
        assert len(assignments) == 1, name
        assert getattr(mod.Settings(), name) == getattr(mod, name)
    assert "class Settings" in rest
    assert mod.ENABLE_TRADING is False


def test_top_configuration_used_by_native_entry(mod, monkeypatch, tmp_path):
    fake = FakeQmt()
    for name, setting in dict(ACCOUNT_ID="configured-account", STATE_DIR=str(tmp_path),
                             INITIAL_CAPITAL=4321.0, BUY_PREMIUM=.004,
                             QUERY_SECONDS=9, PREPARE_TIME="09:19", OPEN_TIME="09:31",
                             RISK_CHECK_TIMES=("10:45",), SNAPSHOT_TIME="14:56").items():
        monkeypatch.setattr(mod, name, setting)
    mod.init(fake)
    try:
        runtime = mod._runtime
        assert fake.account == "configured-account"
        assert runtime.executor.state["capital"] == 4321
        assert runtime.settings.BUY_PREMIUM == .004
        assert runtime.settings.QUERY_SECONDS == 9
        assert runtime.times == ("09:19", "09:31", ("10:45",), "14:56")
        assert fake.sent == []
    finally:
        mod.stop(fake)


def test_stop_releases_lock_and_restart_preserves_ledger(mod, monkeypatch, tmp_path):
    fake = FakeQmt()
    monkeypatch.setattr(mod, "ACCOUNT_ID", "mock-account")
    monkeypatch.setattr(mod, "STATE_DIR", str(tmp_path))
    mod.init(fake)
    runtime = mod._runtime
    ex = runtime.executor
    target(ex)
    ex.state["cash"] = 8000
    ex.state["positions"]["510300.SH"] = dict(qty=2000, cost=2000)
    ex.save()
    before = Path(ex.path).read_bytes()

    # 停止阶段只关闭本地资源，不访问 QMT，也不改写/删除已有账本。
    def forbidden(*args, **kwargs):
        raise AssertionError("stopped runtime must not access QMT or ledger")
    for name in ("query", "save", "report", "sync", "expire", "advance"):
        monkeypatch.setattr(ex, name, forbidden)
    mod.stop(fake)
    mod.stop(fake)  # 停止回调可重复调用
    assert mod._runtime is None and runtime.closed and ex.lock_file.closed
    mod.on_timer(fake)
    mod.order_callback(fake, {})
    mod.deal_callback(fake, {})
    mod.orderError_callback(fake, {}, "late callback")
    runtime.tick(NOW)  # 已排队且持有旧实例引用的回调也不再操作
    runtime.on_report("order", {})
    runtime.on_report("deal", {})
    runtime.on_error({}, "late callback")
    assert Path(ex.path).read_bytes() == before
    assert not fake.sent and not fake.canceled

    mod.init(fake)
    try:
        assert mod._runtime.executor.state == ex.state
        assert mod._runtime.executor.path == ex.path
        assert not mod._runtime.executor.lock_file.closed
    finally:
        mod.stop(fake)


@pytest.mark.parametrize("fail_at", ["set_account", "run_time"])
def test_start_failure_releases_lock_without_gc(mod, monkeypatch, tmp_path, fail_at):
    fake = FakeQmt()
    monkeypatch.setattr(mod, "ACCOUNT_ID", "mock-account")
    monkeypatch.setattr(mod, "STATE_DIR", str(tmp_path))
    original = getattr(fake, fail_at)
    failed_runtimes = []
    def fail(*args):
        failed_runtimes.append(mod._runtime)
        raise RuntimeError("startup test failure")
    monkeypatch.setattr(fake, fail_at, fail)
    with pytest.raises(RuntimeError, match="startup test failure") as caught:
        mod.init(fake)
    assert caught.value and failed_runtimes  # 保持异常与旧实例引用，不能靠 GC 释放锁
    assert failed_runtimes[0].executor.lock_file.closed
    assert mod._runtime is None
    monkeypatch.setattr(fake, fail_at, original)
    mod.init(fake)
    try:
        assert not fake.sent and not fake.canceled
    finally:
        mod.stop(fake)


def test_repeated_init_does_not_unlock_running_instance(mod, monkeypatch, tmp_path):
    fake = FakeQmt()
    monkeypatch.setattr(mod, "ACCOUNT_ID", "mock-account")
    monkeypatch.setattr(mod, "STATE_DIR", str(tmp_path))
    mod.init(fake)
    runtime = mod._runtime
    try:
        with pytest.raises(RuntimeError, match="仍在运行"):
            mod.init(fake)
        assert mod._runtime is runtime and not runtime.closed
        with pytest.raises(RuntimeError, match="账本锁获取失败"):
            mod.Executor(runtime.data, runtime.settings, "mock-account", lambda *args: None)
    finally:
        mod.stop(fake)


def test_top_decision_configuration_changes_decisions(mod, monkeypatch):
    monkeypatch.setattr(mod, "MAX_HOLD_NUM", 1)
    monkeypatch.setattr(mod, "DEPLOY_RATIO", .8)
    monkeypatch.setattr(mod, "STOP_LOSS_RATIO", .9)
    frame = pd.DataFrame({"unit_net_value": [2., 2.]}, index=["510300.SH", "510500.SH"])
    quotes = {"510300.SH": dict(last_price=1., paused=False, high_limit=3.),
              "510500.SH": dict(last_price=1.5, paused=False, high_limit=3.)}
    assert mod.select(frame, quotes).weights == {"510300.SH": .8}
    assert mod.risk_signal(1., .94) is None
    assert mod.risk_signal(1., .89) == "stop_loss"


def test_script_imports_with_no_sibling_modules_or_build(tmp_path, monkeypatch):
    import builtins
    original_import = builtins.__import__
    def checked_import(name, *args, **kwargs):
        if name.split(".")[0] in {"settings", "data", "runtime", "execution", "build",
                                 "jqdata", "xtquant", "bullet_trade"}:
            raise AssertionError("unexpected runtime dependency: " + name)
        return original_import(name, *args, **kwargs)
    path = tmp_path / "good_etf.py"
    path.write_bytes((ROOT / "good_etf.py").read_bytes())
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(builtins, "__import__", checked_import)
    namespace = {"__name__": "isolated_qmt_script"}
    exec(compile(path.read_bytes(), str(path), "exec"), namespace)
    assert namespace["ENABLE_TRADING"] is False
    assert callable(namespace["init"]) and callable(namespace["handlebar"])
    assert list(tmp_path.iterdir()) == [path]  # 导入不建账本、不访问任何 QMT 账号 API


def test_source_encoding_matches_qmt_gbk_save():
    raw = (ROOT / "good_etf.py").read_bytes()
    encoding, _ = tokenize.detect_encoding(io.BytesIO(raw).readline)
    assert encoding == "gbk"
    source = raw.decode("gbk")
    assert "完整 QMT 内置 Python 单文件策略" in source
    assert source.encode("gbk") == raw
    compile(raw, "good_etf.py", "exec")
    ast.parse(source, feature_version=(3, 6))
    # 模拟 QMT 将剪贴板文字按 GBK 保存：声明必须与实际字节相符。
    qmt_saved_bytes = source.replace("\r\n", "\n").encode("gbk")
    compile(qmt_saved_bytes, "QMT_pasted_strategy.py", "exec")
    broken = source.replace("# encoding:gbk", "# coding: utf-8", 1).encode("gbk")
    with pytest.raises(SyntaxError, match="utf-8"):
        compile(broken, "QMT_wrong_encoding.py", "exec")


def test_direct_parity_with_existing_joinquant_strategy(mod, monkeypatch):
    path = ROOT.parents[2] / "tests" / "strategies" / "test_good_etf_contract.py"
    spec = importlib.util.spec_from_file_location("old_good_etf_contract", path)
    old_test = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old_test)
    old = old_test._load_strategy(monkeypatch)
    old._runtime = old_test._Runtime(old_test.real_helper.RuntimeMode.JQ)
    codes = ["520590.XSHG", "510001.XSHG", "510002.XSHG", "510003.XSHG", "510004.XSHG", "510005.XSHG"]
    names = ["恒科", "港口ETF", "红利ETF", "科技ETF", "低流动性ETF", "高边界"]
    money = dict(zip(codes, [1e7, 1e7, 1e7, 1e7, 5e6, 2e7]))
    old.get_all_securities = lambda *args: pd.DataFrame({"display_name": names}, index=codes)
    old.finance = None
    old.history = lambda count, unit, field, security_list: pd.DataFrame(
        [[money[code] if field == "money" else 2.0 for code in security_list]], columns=security_list)
    old.get_extras = lambda field, security_list, **kw: pd.DataFrame([[2.] * len(security_list)], columns=security_list)
    quotes = {code: types.SimpleNamespace(last_price=price, paused=False, high_limit=3.)
              for code, price in zip(codes[1:4], [1.8, 1.5, 1.])}
    old.get_current_data = lambda: quotes
    context = types.SimpleNamespace(previous_date=NOW.date() - dt.timedelta(days=1), current_dt=NOW)
    old.before_market_open(context)
    old.market_open(context)
    _, weights, marks, _, _ = old._runtime.rebalances[0]
    class Data:
        def universe(self, _):
            return [(code.replace("XSHG", "SH"), name, "") for code, name in zip(codes, names)]
        def daily(self, codes, _):
            return pd.DataFrame({"money": [money[code.replace(".SH", ".XSHG")] for code in codes]}, index=codes)
        def nav(self, codes, *_):
            return pd.DataFrame({"unit_net_value": [2.] * len(codes)}, index=codes)
    frame = mod.prepare(Data(), "20260910", "20260911")
    result = mod.select(frame, {code.replace("XSHG", "SH"): vars(quote) for code, quote in quotes.items()})
    assert result.weights == {code.replace("XSHG", "SH"): weight for code, weight in weights.items()}
    assert result.marks == {code.replace("XSHG", "SH"): mark for code, mark in marks.items()}


def test_runtime_rejects_backtest_before_creating_ledger(mod, tmp_path):
    fake = FakeQmt()
    fake.do_back_test = True
    config = mod.Settings()
    config.STATE_DIR, config.ACCOUNT_ID = str(tmp_path), "mock-account"
    with pytest.raises(RuntimeError, match="不支持历史回测"):
        mod.Runtime(fake, fake.namespace(), config, mod.prepare, mod.select, mod.risk_signal)
    assert list(tmp_path.iterdir()) == []


def test_same_account_strategy_cannot_run_twice(env, mod):
    fake, ex, data, config, events = env
    with pytest.raises(RuntimeError, match="账本锁获取失败") as first:
        mod.Executor(data, config, "mock-account", lambda *args: None)
    with pytest.raises(RuntimeError, match="账本锁获取失败") as second:
        mod.Executor(data, config, "mock-account", lambda *args: None)
    assert isinstance(first.value.__cause__, OSError) and second.value
    ex.close()
    restored = mod.Executor(data, config, "mock-account", lambda *args: None)
    restored.close()


@pytest.mark.parametrize("failure", ["capital", "json", "save"])
def test_ledger_init_failure_releases_lock_without_gc(env, mod, monkeypatch, failure):
    fake, ex, data, config, events = env
    ex.close()
    original = Path(ex.path).read_bytes()
    old_save = mod.Executor.save
    if failure == "capital":
        config.INITIAL_CAPITAL += 1
    elif failure == "json":
        Path(ex.path).write_text("invalid json", encoding="utf-8")
    else:
        def fail_save(self):
            raise OSError("test save failure")
        monkeypatch.setattr(mod.Executor, "save", fail_save)
    with pytest.raises((RuntimeError, ValueError, OSError)) as caught:
        mod.Executor(data, config, "mock-account", lambda *args: None)
    assert caught.value  # 保持异常 traceback，验证无需依赖析构/垃圾回收
    config.INITIAL_CAPITAL = ex.state["capital"]
    Path(ex.path).write_bytes(original)
    monkeypatch.setattr(mod.Executor, "save", old_save)
    restored = mod.Executor(data, config, "mock-account", lambda *args: None)
    restored.close()


def test_invalid_fill_id_and_direction_not_booked(env):
    fake, ex, data, config, events = env
    target(ex)
    ex.advance(NOW)
    order = latest(ex)
    with pytest.raises(RuntimeError, match="编号"):
        ex.report("deal", row(order, m_strTradeID="0", m_nVolume=100, m_dPrice=1))
    with pytest.raises(RuntimeError, match="方向"):
        ex.report("deal", row(order, m_strTradeID="1", m_nVolume=100, m_dPrice=1, m_nOffsetFlag=49))
    assert ex.state["cash"] == 10000 and order["booked"] == 0


def test_native_sell_types_precision_and_original_protection(env):
    fake, ex, data, config, events = env
    goal = ex.target(0, 1.015, "profit", "20260911")
    assert ex.order_price("510300.SH", goal, "SELL") == (1.013, 11)
    goal["style"] = "market"
    assert ex.order_price("159001.SZ", goal, "SELL") == (1.0, 11)
    assert ex.order_price("510300.SH", goal, "SELL") == (1.0, 42)
    config.SH_NATIVE_MARKET = False
    assert ex.order_price("510300.SH", goal, "SELL") == (1.0, 11)


def test_runtime_schedule_open_once_restart(mod, tmp_path):
    fake = FakeQmt()
    config = mod.Settings()
    config.STATE_DIR, config.ACCOUNT_ID, config.ENABLE_TRADING = str(tmp_path), "mock-account", True
    prepared = []
    def prepare(*args):
        prepared.append(True)
        return pd.DataFrame({"unit_net_value": [1.1]}, index=["510300.SH"])
    runtime = mod.Runtime(fake, fake.namespace(), config, prepare, mod.select, mod.risk_signal)
    try:
        runtime.start("09:20", "09:30", ("10:30",), "14:55")
        runtime.tick(NOW)
        runtime.tick(NOW)
        assert len(fake.sent) == 1 and prepared == [True]
        runtime.executor.close()
        restored = mod.Runtime(fake, fake.namespace(), config, prepare, mod.select, mod.risk_signal)
        try:
            restored.start("09:20", "09:30", ("10:30",), "14:55")
            restored.tick(NOW)
            assert len(fake.sent) == 1 and prepared == [True]
        finally:
            restored.executor.close()
    finally:
        runtime.executor.close()
