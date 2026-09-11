"""Synthetic fixtures only: no QMT terminal, JoinQuant login or broker calls."""
import datetime as dt
import importlib.util
import json

import pandas as pd
import pytest

from test_native import FakeQmt, ROOT, mod


class BacktestQmt(FakeQmt):
    def __init__(self):
        super().__init__()
        self.do_back_test, self.period, self.dividend_type = True, "1m", "none"
        self.capital, self.balance = 20000., 20000.
        self.start, self.end = "2026-09-11 00:00:00", "2026-09-14 23:59:59"
        self.now, self.barpos = dt.datetime(2026, 9, 11, 9, 31), 0
        self.money, self.prices, self.flags = {}, {}, {}
        self.orders, self.queries, self.market_calls = [], [], []
        self.offset, self.missing, self.no_open_bar = 0, set(), True

    def get_bar_timetag(self, pos):
        return int(self.now.replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp() * 1000)

    def get_trading_dates(self, *args):
        return ["20260910", "20260911", "20260914"]

    def get_market_data_ex(self, fields, codes, **kwargs):
        self.market_calls.append((fields, codes, kwargs))
        assert kwargs["dividend_type"] == "none" and not kwargs["subscribe"] and not kwargs["fill_data"]
        end = dt.datetime.strptime(kwargs["end_time"], "%Y%m%d%H%M%S")
        assert end <= self.now
        if kwargs["period"] == "1d":
            return {code: pd.DataFrame({"high": [2.], "low": [1.], "amount": [self.money.get(code, 1e7)]},
                                      index=[end.replace(hour=0, minute=0, second=0)]) for code in codes}
        assert kwargs["period"] == "1m" and fields == ["close", "suspendFlag"]
        if self.no_open_bar and end.strftime("%H:%M") == "09:30":
            return {}
        return {code: pd.DataFrame({"close": [self.prices.get(code, 1.)], "suspendFlag": [self.flags.get(code, 0)]},
                                  index=[end + dt.timedelta(minutes=self.offset)])
                for code in codes if code not in self.missing}

    def get_trade_detail_data(self, account, kind, datatype):
        assert account == "good_etf_backtest"
        self.queries.append((account, datatype))
        if datatype == "account":
            return [dict(m_dBalance=self.balance, m_dAvailable=self.cash)]
        return super().get_trade_detail_data(account, kind, datatype)

    def order_target_value(self, code, target, context, account):
        assert self.do_back_test and context is self and account == "good_etf_backtest"
        self.orders.append((code, target))  # 仅记录提交；不伪造平台成交

    def namespace(self):
        return dict(super().namespace(), order_target_value=self.order_target_value)

    def get_full_tick(self, *args):
        raise AssertionError("backtest must not read current ticks")

    def get_instrument_detail(self, *args):
        raise AssertionError("backtest must not read today's metadata")

    def get_etf_info(self, *args):
        raise AssertionError("backtest must not read current PCF")

    def set_account(self, *args):
        raise AssertionError("backtest must not bind a broker account")

    def run_time(self, *args):
        raise AssertionError("backtest must not create a realtime timer")

    def passorder(self, *args):
        raise AssertionError("backtest must use backtest-only native order API")

    def cancel(self, *args):
        raise AssertionError("backtest must not cancel broker orders")


def history_document():
    securities = {code: dict(name=name, nav=2., high_limit=3.) for code, name in [
        ("510001.SH", "港口ETF"), ("510002.SH", "普通ETF"), ("510003.SH", "科技ETF"),
        ("510004.SH", "流动性边界"), ("520590.SH", "恒科")]}
    return {"schema": 1, "sessions": {
        "20260911": {"previous_date": "20260910", "securities": securities},
        "20260914": {"previous_date": "20260911", "securities": securities}}}


@pytest.fixture
def bt(mod, tmp_path):
    config = mod.Settings()
    path = tmp_path / "history.json"
    path.write_text(json.dumps(history_document()), encoding="utf-8")
    config.BACKTEST_DATA_FILE = str(path)
    config.ACCOUNT_ID, config.STATE_DIR = "must-not-use", str(tmp_path / "live-ledger")
    fake = BacktestQmt()
    fake.prices = {"510001.SH": 1.9, "510002.SH": 1.5, "510003.SH": 1., "520590.SH": .01}
    fake.money["510004.SH"] = 5e6
    runtime = mod.create_runtime(fake, fake.namespace(), config, mod.prepare, mod.select, mod.risk_signal)
    runtime.start("09:20", "09:30", ("10:30", "13:30", "14:50"), "14:55")
    yield fake, runtime, config
    runtime.close()
    assert not (tmp_path / "live-ledger").exists()


def test_backtest_isolated_native_entry_and_panel_capital(bt, mod, monkeypatch):
    fake, runtime, config = bt
    runtime.close()
    for name in ("BACKTEST_DATA_FILE", "ACCOUNT_ID", "STATE_DIR"):
        monkeypatch.setattr(mod, name, getattr(config, name))
    for name, fn in fake.namespace().items():
        monkeypatch.setattr(mod, name, fn, raising=False)
    assert mod.ENABLE_TRADING is False
    mod.init(fake)
    try:
        assert isinstance(mod._runtime, mod.BacktestRuntime)
        mod.handlebar(fake)
        assert sum(target for _, target in fake.orders) == pytest.approx(20000 * .95)
        assert fake.capital == 20000 and mod.INITIAL_CAPITAL != fake.capital
        mod.on_timer(fake)
        mod.order_callback(fake, {})
        mod.deal_callback(fake, {})
        mod.orderError_callback(fake, {}, "ignored broker callback")
        assert len(fake.orders) == 3
    finally:
        mod.stop(fake)


def test_backtest_selection_and_single_equity_budget(bt, mod):
    fake, runtime, _ = bt
    runtime.handlebar()
    assert [code for code, _ in fake.orders] == ["510003.SH", "510002.SH", "510001.SH"]
    raw = [abs(price / 2 - 1) * 100 for price in (1., 1.5, 1.9)]
    assert [amount for _, amount in fake.orders] == [fake.balance * (x / sum(raw) * .95) for x in raw]
    assert not fake.sent and not fake.canceled
    runtime.handlebar()
    assert len(fake.orders) == 3


def test_first_completed_minute_then_next_day(bt):
    fake, runtime, _ = bt
    fake.now = fake.now.replace(minute=30)
    runtime.handlebar()
    assert not fake.orders
    fake.now = fake.now.replace(minute=31)
    runtime.handlebar()
    assert len(fake.orders) == 3
    fake.now = fake.now.replace(day=14)
    runtime.handlebar()
    assert len(fake.orders) == 6
    assert runtime.data.session["previous_date"] == "20260911"


def test_historical_pool_and_nav_never_use_today(bt):
    fake, runtime, _ = bt
    runtime.data.sessions["20260911"]["securities"]["510003.SH"]["nav"] = None
    runtime.handlebar()
    assert "510003.SH" not in [code for code, _ in fake.orders]
    assert not any("520590.SH" in codes for _, codes, _ in fake.market_calls)


def test_historical_suspension_price_limit_and_explicit_no_limit(bt):
    fake, runtime, _ = bt
    fake.flags["510001.SH"] = 1
    rows = runtime.data.sessions["20260911"]["securities"]
    rows["510002.SH"]["high_limit"] = 1.5
    rows["510003.SH"]["high_limit"] = None
    runtime.handlebar()
    assert fake.orders == [("510003.SH", 19000.)]


def test_platform_equity_and_snapshot_no_fee_or_fill_fabrication(bt, capsys):
    fake, runtime, _ = bt
    fake.balance = 19000.
    runtime.handlebar()
    assert sum(amount for _, amount in fake.orders) == pytest.approx(19000 * .95)
    assert not fake.position_rows  # 不能因为提交了目标就在脚本内伪造成交
    fake.now = fake.now.replace(hour=14, minute=55)
    runtime.handlebar()
    assert "净值=0.950000" in capsys.readouterr().out
    assert len(fake.orders) == 3


@pytest.mark.parametrize("case", ["future", "stale", "missing_quote", "missing_flag", "missing_limit", "missing_day", "wrong_previous", "no_nav"])
def test_bad_history_stops_before_any_order(bt, case):
    fake, runtime, _ = bt
    rows = runtime.data.sessions["20260911"]["securities"]
    if case in ("future", "stale"):
        fake.offset = 1 if case == "future" else -1
    elif case == "missing_quote":
        fake.missing.add("510002.SH")
    elif case == "missing_flag":
        fake.flags["510002.SH"] = None
    elif case == "missing_limit":
        del rows["510002.SH"]["high_limit"]
    elif case == "missing_day":
        del runtime.data.sessions["20260911"]
    elif case == "wrong_previous":
        runtime.data.sessions["20260911"]["previous_date"] = "20260911"
    else:
        for row in rows.values():
            row["nav"] = None
    with pytest.raises(RuntimeError):
        runtime.handlebar()
    assert runtime.closed and not fake.orders


def test_reduce_before_increase_and_no_minute_chasing(bt):
    fake, runtime, _ = bt
    fake.position_rows = [dict(m_strInstrumentID="510001", m_strExchangeID="SH", m_nVolume=10000, m_dOpenPrice=1.9)]
    runtime.handlebar()
    assert fake.orders[0][0] == "510001.SH"
    assert len(fake.orders) == 3
    fake.now = fake.now.replace(minute=32)
    runtime.handlebar()
    assert len(fake.orders) == 3  # 不自行创建分钟追单状态机


@pytest.mark.parametrize("price, expected", [(.95, False), (.949, True), (1.10, False), (1.101, True)])
def test_risk_uses_backtest_position_cost(bt, price, expected):
    fake, runtime, _ = bt
    fake.now = fake.now.replace(hour=10, minute=30)
    fake.position_rows = [dict(m_strInstrumentID="510001", m_strExchangeID="SH", m_nVolume=100, m_dOpenPrice=1.)]
    fake.prices["510001.SH"] = price
    runtime.handlebar()
    assert fake.orders == ([("510001.SH", 0.)] if expected else [])


def test_empty_selection_clears_but_no_candidates_does_not(bt):
    fake, runtime, _ = bt
    fake.prices = {code: 2.1 for code in history_document()["sessions"]["20260911"]["securities"]}
    fake.position_rows = [dict(m_strInstrumentID="510001", m_strExchangeID="SH", m_nVolume=100, m_dOpenPrice=2.)]
    runtime.handlebar()
    assert fake.orders == [("510001.SH", 0.)]
    fake.orders.clear()
    fake.now = fake.now.replace(day=14)
    fake.money = {code: 0 for code in fake.prices}
    runtime.handlebar()
    assert not fake.orders


def test_warmup_and_mode_switch_cannot_trade(bt):
    fake, runtime, _ = bt
    fake.now = fake.now.replace(year=2001)
    runtime.handlebar()
    assert not fake.orders and not fake.market_calls
    fake.do_back_test = False
    with pytest.raises(RuntimeError, match="回测环境已切换"):
        runtime.handlebar()
    with pytest.raises(RuntimeError):
        runtime.submit("510001.SH", 1000)
    assert not fake.orders and not fake.queries


@pytest.mark.parametrize("field, setting", [("period", "1d"), ("dividend_type", "front"), ("capital", 0)])
def test_bad_backtest_setup_does_not_create_ledger(bt, mod, field, setting):
    fake, _, config = bt
    setattr(fake, field, setting)
    with pytest.raises(RuntimeError):
        mod.create_runtime(fake, fake.namespace(), config, mod.prepare, mod.select, mod.risk_signal)


def test_missing_file_explains_data_requirement(bt, mod):
    fake, _, config = bt
    config.BACKTEST_DATA_FILE = ""
    with pytest.raises(RuntimeError, match="BACKTEST_DATA_FILE"):
        mod.create_runtime(fake, fake.namespace(), config, mod.prepare, mod.select, mod.risk_signal)


def test_backtest_timestamp_formats(mod):
    expected = dt.datetime(2026, 9, 11, 9, 31)
    epoch = int(expected.replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp() * 1000)
    for stamp in (epoch, str(epoch), 20260911093100, "20260911093100", pd.Timestamp(expected),
                  expected.replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).astimezone(dt.timezone.utc)):
        assert mod.backtest_time(stamp) == expected


def test_export_history_dates_nulls_and_no_future_prices():
    spec = importlib.util.spec_from_file_location("qmt_history_export", ROOT / "export_backtest_data.py")
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    previous, today = dt.date(2026, 9, 10), dt.date(2026, 9, 11)
    class Research:
        def get_trade_days(self, **kw):
            return [previous, today] if "count" in kw else [today]
        def get_all_securities(self, types, date):
            assert types == ["etf"] and date == previous
            return pd.DataFrame({"display_name": ["ETF1", "ETF2", "ETF3"]}, index=["510001.XSHG", "510002.XSHG", "510003.XSHG"])
        def get_extras(self, field, codes, **kw):
            assert field == "unit_net_value" and kw["end_date"] == previous and kw["start_date"] == previous
            return pd.DataFrame([[1., float("nan"), 2.]], columns=codes, index=[pd.Timestamp(previous)])
        def get_price(self, codes, **kw):
            assert kw["fields"] == ["high_limit"] and kw["end_date"] == today and kw["fq"] is None
            return pd.DataFrame({"time": [pd.Timestamp(today)] * 3, "code": codes, "high_limit": [1.1, float("inf"), None]})
    result = exporter.build_history(Research(), str(today), str(today))
    rows = result["sessions"]["20260911"]["securities"]
    assert rows["510001.SH"]["nav"] == 1.
    assert rows["510002.SH"]["high_limit"] is None and rows["510002.SH"]["nav"] is None
    assert "high_limit" not in rows["510003.SH"]
    json.dumps(result, allow_nan=False)
