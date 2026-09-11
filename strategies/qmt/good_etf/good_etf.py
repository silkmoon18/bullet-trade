# encoding:gbk
"""GoodETF：完整 QMT 内置 Python 单文件策略。

直接编辑本文件顶部配置，然后全选复制到 QMT Python 策略编辑器。
无需 build、聚宽、MiniQMT、xtquant 或本地辅助模块。
默认不向柜台发单；回测自动使用 QMT 虚拟账户，客户端能力仍须实际验证。
"""

# ==================== 用户配置：只需修改此区域 ====================

# ---- 账户、交易开关和策略归属资金 ----
ACCOUNT_ID = ""  # Use the account selected in QMT, or fill locally; never commit it.
STRATEGY_ID = "good_etf_qmt" # 同一账户只运行一个本策略实例，不与旧 remote 同时交易
ENABLE_TRADING = False     # 仅实时：False=观察；True=柜台发单。回测始终走虚拟撮合
INITIAL_CAPITAL = 10000.0   # 分给本策略的初始资金；已有账本不能改数字来重置
STATE_DIR = r"C:\qmt_good_etf_data"  # 客户端上的私有账本目录；重启仍使用原目录

# ---- 数据与通知 ----
ETF_SECTOR = "沪深ETF"      # 客户端全市场 ETF 板块名，请核对成员范围
NAV_FILE = ""              # 可选：前一交易日单位净值 JSON 路径；空值使用可验证日期的 PCF
BACKTEST_DATA_FILE = ""    # 回测必填：历史 ETF 池/前日单位净值/涨停价 JSON；实时不读取
TRACKED_INDEX_NAMES = {}    # 可选：{"510300.SH": "沪深300"}，补充跟踪指数名称
FEISHU_WEBHOOK = ""        # 可选飞书机器人 webhook；空值关闭通知，不要公开凭据

# ---- 选股与风控：保留原 GoodETF 参数及严格比较边界 ----
MAX_HOLD_NUM = 3           # 折价最深的前 N 只
MIN_MONEY = 5e6            # 前一交易日成交额严格大于 500 万元
MAX_MONEY = 2e7            # 前一交易日成交额严格小于 2000 万元
STOP_LOSS_RATIO = 0.95     # 现价严格低于策略持仓成本的 95% 时止损
TAKE_PROFIT_RATIO = 1.10   # 现价严格高于策略持仓成本的 110% 时止盈
DEPLOY_RATIO = 0.95        # 目标权重合计 95%，保留现金缓冲
SKIP_SUSPENDED_LIMITUP = True  # True=选股前排除停牌/涨停；False=不做此筛选
HK_WORDS = ("港股", "恒生", "H股", "香港", "港股通", "沪港深", "沪深港", "HK", "恒科", "港红利")
HK_CODES = {"520590.SH", "520890.SH", "513320.SH", "159322.SZ"}  # 港股属性明确的代码兜底表

# ---- 决策时间：中国时区，格式 HH:MM ----
PREPARE_TIME = "09:20"      # 盘前准备；缺失时在开盘决策现场补跑
OPEN_TIME = "09:30"         # 每日选股调仓，不在错过该分钟后补做开盘决策
RISK_CHECK_TIMES = ("10:30", "13:30", "14:50")
SNAPSHOT_TIME = "14:55"     # 组合/净值/费用快照

# ---- 委托执行：参考价保持固定，ETF 不做股票价格笼子等待 ----
BUY_PREMIUM = 0.002        # 买入上限=开盘决策 lastPrice * 1.002，不使用卖一追价
PROFIT_DISCOUNT = 0.002    # 止盈卖出下限=触发时 lastPrice * 0.998
MARKET_PROTECTION = 0.015 # 调仓卖出/止损的保护距离=参考价的 1.5%
SH_NATIVE_MARKET = True   # True=沪市原生五档即成剩撤；False=保护价固定限价
# 深圳仍用保护价固定限价，不声称具备同等的原生市价保护。
QUERY_SECONDS = 5         # 委托/成交查询补漏间隔，单位秒；不是行情 tick 频率
RETRY_SECONDS = 3         # 确认剩余已撤且成交入账后，续单最小间隔（秒）
CASH_RESERVE = 5.0        # 每笔活动买单预留金额（元）；不记作手续费

# ==================== 配置结束：以下通常无需修改 ====================

import hashlib
import json
import math
import os
import time
import uuid
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import builtins
import datetime as dt
from typing import NamedTuple
import pandas as pd
import threading
import urllib.request


class Decision(NamedTuple):
    weights: dict
    marks: dict


# ==================== 策略决策与 QMT 入口 ====================

def prepare(data, previous, today):
    codes = [code for code, name, index in data.universe(previous)
             if not is_hong_kong_etf(code, name, index)]
    frame = data.daily(codes, previous)
    frame = frame[(frame["money"] > MIN_MONEY) & (frame["money"] < MAX_MONEY)]
    if frame.empty:
        return None  # 和原策略一致：预处理无候选，不等于清空持仓
    return frame.join(data.nav(frame.index.tolist(), previous, today), how="inner")


def select(frame, quotes):
    frame = frame.copy()
    frame["last_price"] = [quotes[code]["last_price"] for code in frame.index]
    if SKIP_SUSPENDED_LIMITUP:
        frame = frame.loc[[code for code in frame.index
                           if not quotes[code]["paused"]
                           and frame.loc[code, "last_price"] < quotes[code]["high_limit"]]]
    frame["premium"] = (frame["last_price"] / frame["unit_net_value"] - 1) * 100
    selected = frame[frame["premium"] < 0].sort_values(["premium"], ascending=True).head(MAX_HOLD_NUM)
    raw = selected["premium"].abs().tolist()
    total = sum(raw) or 1.0
    return Decision(
        {code: float(weight / total * DEPLOY_RATIO) for code, weight in zip(selected.index, raw)},
        {code: float(selected.loc[code, "last_price"]) for code in selected.index})


def risk_signal(cost, price):
    if price < cost * STOP_LOSS_RATIO:
        return "stop_loss"
    if price > cost * TAKE_PROFIT_RATIO:
        return "take_profit"
    return None


_runtime = None


def init(ContextInfo):
    global _runtime
    if _runtime is not None:
        raise RuntimeError("本策略实例仍在运行，请先在 QMT 中停止再启动")
    _runtime = create_runtime(ContextInfo, globals(), Settings(), prepare, select, risk_signal)
    try:
        _runtime.start(PREPARE_TIME, OPEN_TIME, RISK_CHECK_TIMES, SNAPSHOT_TIME)
    except BaseException:
        _runtime.close()
        _runtime = None
        raise


def stop(ContextInfo):
    global _runtime
    if _runtime is not None:
        _runtime.close()
        _runtime = None


def handlebar(ContextInfo):
    runtime = _runtime
    if runtime is not None:
        runtime.handlebar()  # 仅回测处理历史 bar，实时仍不在这里报单


def on_timer(ContextInfo):
    runtime = _runtime
    if runtime is not None:
        runtime.tick()


def order_callback(ContextInfo, orderInfo):
    runtime = _runtime
    if runtime is not None:
        runtime.on_report("order", orderInfo)


def deal_callback(ContextInfo, dealInfo):
    runtime = _runtime
    if runtime is not None:
        runtime.on_report("deal", dealInfo)


def orderError_callback(ContextInfo, orderArgs, errMsg):
    runtime = _runtime
    if runtime is not None:
        runtime.on_error(orderArgs, errMsg)


# ==================== QMT 数据、账本与执行辅助 ====================

class Settings:
    """为执行辅助代码保存顶部配置快照；不需要用户另行维护配置文件。"""

    def __init__(self):
        self.__dict__.update((name, setting) for name, setting in globals().items()
                             if name.isupper())


def is_hong_kong_etf(code, name, index_name=""):
    text = "{} {}".format(name, index_name).lower()
    return code in HK_CODES or any(word.lower() in text for word in HK_WORDS)


def value(obj, name, default=None):
    return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)


def positive(number):
    try:
        number = float(number)
        return number if math.isfinite(number) and 0 < number < 1e12 else None
    except (ValueError, TypeError):
        return None


def date_text(raw):
    if isinstance(raw, (int, float)) and raw > 1e11:
        return dt.datetime.fromtimestamp(raw / 1000).strftime("%Y%m%d")
    return str(raw).replace("-", "")[:8]


class QmtData:
    def __init__(self, context, namespace, settings, log=print):
        self.c, self.namespace, self.settings, self.log = context, namespace, settings, log
        self.details = {}
        self.last_quote_time = "未获取"

    def api(self, name):
        fn = self.namespace.get(name) or getattr(builtins, name, None)
        if not callable(fn):
            raise RuntimeError("QMT 内置接口不可用: " + name)
        return fn

    def calendar(self, now):
        end = now.strftime("%Y%m%d")
        dates = self.c.get_trading_dates("000001.SH", "", end, 10, "1d")
        days = sorted({date_text(day) for day in dates if date_text(day) <= end})
        previous = [day for day in days if day < end]
        if not previous:
            raise RuntimeError("交易日历缺失，先补充 QMT 行情数据")
        return previous[-1], end in days

    def detail(self, code):
        if code not in self.details:
            method = getattr(self.c, "get_instrument_detail", None) or self.c.get_instrumentdetail
            row = method(code)
            if not row:
                raise RuntimeError("合约资料缺失: " + code)
            self.details[code] = row
        return self.details[code]

    def label(self, code):
        return "{}({})".format(code, self.detail(code).get("InstrumentName", "未知名称"))

    def universe(self, previous):
        self.details.clear()  # 涨跌停/交易状态等按日重新读取
        codes = self.c.get_stock_list_in_sector(self.settings.ETF_SECTOR)
        if not codes:
            raise RuntimeError("ETF 板块为空，请检查 ETF_SECTOR 和客户端数据")
        rows = []
        for code in codes:
            detail = self.detail(code)
            if date_text(detail.get("OpenDate", "")) > previous:
                continue
            end = date_text(detail.get("ExpireDate", ""))
            if end and end != "0" and end <= previous:
                continue
            rows.append((code, detail.get("InstrumentName", ""),
                         self.settings.TRACKED_INDEX_NAMES.get(code, "")))
        self.log("ETF 全市场={}；QMT 无聚宽基金指数库，缺失跟踪指数时使用名称/代码过滤".format(len(rows)))
        return rows

    def daily(self, codes, previous):
        if not codes:
            return pd.DataFrame(columns=["high_price", "low_price", "money"])
        data = self.c.get_market_data_ex(
            ["high", "low", "amount"], codes, period="1d", end_time=previous + "235959",
            count=1, dividend_type="none", fill_data=False, subscribe=False)
        rows, missing = {}, []
        for code in codes:
            frame = data.get(code)
            if frame is None or frame.empty or date_text(frame.index[-1]) != previous:
                missing.append(code)
                continue  # 对应原策略 inner join / NaN 流动性过滤的缺失数据行为
            bar = frame.iloc[-1]
            rows[code] = [float(bar["high"]), float(bar["low"]), float(bar["amount"])]
        if missing:
            self.log("前日日线缺失/过期 {} 只: {}；请补充日线数据".format(len(missing), missing[:10]))
        if not rows:
            raise RuntimeError("全体前日日线缺失，请先补充 QMT 日线数据")
        return pd.DataFrame.from_dict(rows, orient="index", columns=["high_price", "low_price", "money"])

    def nav(self, codes, previous, today):
        overrides = {}
        if self.settings.NAV_FILE:
            with open(self.settings.NAV_FILE, encoding="utf-8-sig") as stream:
                document = json.load(stream)
            if date_text(document.get("date")) != previous:
                raise RuntimeError("NAV_FILE 日期必须是前一交易日 " + previous)
            overrides = document["nav"]
        rows, missing = {}, []
        for code in codes:
            nav = positive(overrides.get(code))
            if nav is None:
                try:
                    info = self.api("get_etf_info")(code)
                    if (date_text(info.get("preTradingDay")) == previous
                            and date_text(info.get("tradingDay")) == today):
                        nav = positive(info.get("nav"))
                except Exception:
                    pass  # 原策略 inner join 允许个别基金净值缺失，但必须报告
            if nav is None:
                missing.append(code)
            else:
                rows[code] = nav
        if missing:
            self.log("前日单位净值缺失/日期不明 {} 只: {}；未使用 IOPV".format(len(missing), missing[:10]))
        if not rows:
            raise RuntimeError("无可验证的前日单位净值，跳过交易；请检查 PCF 或提供 NAV_FILE")
        return pd.DataFrame.from_dict(rows, orient="index", columns=["unit_net_value"])

    def quotes(self, codes, now):
        if not codes:
            return {}
        ticks = self.c.get_full_tick(list(codes))
        rows = {}
        for code in codes:
            tick = ticks.get(code, {})
            price = positive(tick.get("lastPrice"))
            stamp = tick.get("time") or tick.get("stime") or tick.get("timetag")
            detail = self.detail(code)
            paused = tick.get("openInt") in (1, 16, 17, 20) or detail.get("IsTrading") is False
            if paused and price is not None:
                # 停牌没有新 tick 属正常情况，仍返回停牌标记交由决策过滤。
                rows[code] = dict(last_price=price, paused=True,
                                  high_limit=positive(detail.get("UpStopPrice")) or float("inf"))
                continue
            if price is None or date_text(stamp) != now.strftime("%Y%m%d"):
                raise RuntimeError("最新行情无效/不是今天: " + code)
            if isinstance(stamp, (int, float)):
                quote_clock = dt.datetime.fromtimestamp(stamp / 1000).strftime("%H%M%S")
            else:
                digits = "".join(char for char in str(stamp) if char.isdigit())
                quote_clock = digits[8:14]
            if now.strftime("%H%M%S") >= "093000" and quote_clock < "093000":
                raise RuntimeError("等待开盘后的 lastPrice: " + code)
            self.last_quote_time = str(stamp)
            rows[code] = dict(last_price=price, paused=paused,
                              high_limit=positive(detail.get("UpStopPrice")) or float("inf"))
        return rows


# 迅投内置 Python 枚举：53 部撤、54 已撤、55 部成、56 已成、57 废单。
TERMINAL = {53, 54, 56, 57}


def settled(order):
    return order["status"] in TERMINAL and order["booked"] >= order["filled"]


class Executor:
    def __init__(self, data, settings, account, emit):
        self.data, self.settings, self.account, self.emit = data, settings, account, emit
        identity = account + ":" + settings.STRATEGY_ID
        self.prefix = "ge" + hashlib.sha256(identity.encode()).hexdigest()[:6]
        os.makedirs(settings.STATE_DIR, exist_ok=True)
        self.path = os.path.join(settings.STATE_DIR, self.prefix + ".json")
        self.lock_file = open(self.path + ".lock", "a+b")
        try:
            self.lock_file.seek(0)
            # QMT 停止策略不一定退出 Python 进程，必须由 stop 主动关闭句柄。
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise RuntimeError("账本锁获取失败，可能仍有同一账户/策略的实例占用；"
                                   "请停止重复实例，旧版停止后仍报错可重启 QMT；"
                                   "不要删除账本或更换策略 ID 绕过锁: " + self.path) from exc
            self.state = dict(identity=identity, capital=settings.INITIAL_CAPITAL,
                              cash=settings.INITIAL_CAPITAL, positions={}, targets={},
                              orders={}, fills={}, jobs=[], allocated=False)
            if os.path.exists(self.path):
                with open(self.path, encoding="utf-8") as stream:
                    self.state = json.load(stream)
                if (self.state["identity"] != identity
                        or self.state["capital"] != settings.INITIAL_CAPITAL):
                    raise RuntimeError("账本身份或初始资金不一致；不能靠改配置重置已交易账本")
            self.save()
        except BaseException:
            self.close()
            raise

    def close(self):
        self.lock_file.close()

    def save(self):
        with open(self.path + ".tmp", "w", encoding="utf-8") as stream:
            json.dump(self.state, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(self.path + ".tmp", self.path)

    def query(self, kind):
        rows = self.data.api("get_trade_detail_data")(self.account, "stock", kind)
        if rows is None:
            raise RuntimeError("QMT 查询未返回数据: " + kind)
        return list(rows)

    def broker_cash(self):
        rows = self.query("account")
        if len(rows) != 1:
            raise RuntimeError("QMT 账户未就绪")
        cash = float(value(rows[0], "m_dAvailable", float("nan")))
        if not math.isfinite(cash) or cash < 0:
            raise RuntimeError("QMT 可用资金无效")
        return cash

    def positions(self):
        return {str(value(p, "m_strInstrumentID")) + "." + str(value(p, "m_strExchangeID")): p
                for p in self.query("position")}

    def allocate(self):
        cash = self.broker_cash()
        if not self.state["allocated"]:
            if cash < self.state["capital"]:
                raise RuntimeError("证券账户可用资金不足以分配策略初始资金")
            self.state["allocated"] = True
            self.save()

    def find_order(self, row):
        if str(value(row, "m_strAccountID", self.account)) != self.account:
            return None
        tag = str(value(row, "m_strRemark", ""))
        if tag in self.state["orders"]:
            return self.state["orders"][tag]
        if tag.startswith(self.prefix):
            raise RuntimeError("发现本策略委托但本地记录缺失；请恢复原账本，不可自动接管")
        sysid = str(value(row, "m_strOrderSysID", ""))
        day = date_text(value(row, "m_strTradeDate") or value(row, "m_strInsertDate"))
        matches = [order for order in self.state["orders"].values()
                   if sysid not in ("", "0", "None") and order["sysid"] == sysid and order["day"] == day]
        return matches[0] if len(matches) == 1 else None

    def report(self, kind, row):
        order = self.find_order(row)
        if order is None:
            return
        before = dict(order)
        code = str(value(row, "m_strInstrumentID")) + "." + str(value(row, "m_strExchangeID"))
        side = int(value(row, "m_nOffsetFlag", -1))
        if code != order["code"] or side != (48 if order["side"] == "BUY" else 49):
            raise RuntimeError("委托/成交标的或方向与原下单不一致: " + order["code"])
        sysid = str(value(row, "m_strOrderSysID", ""))
        if sysid not in ("", "0", "None"):
            if order["sysid"] and order["sysid"] != sysid:
                raise RuntimeError("同一投资备注对应多笔委托，停止推进")
            order["sysid"] = sysid
        if kind == "order":
            status = int(value(row, "m_nOrderStatus", 255))
            filled = int(value(row, "m_nVolumeTraded", 0))
            if status == 56:
                filled = order["qty"]
            if filled < 0 or filled > order["qty"]:
                raise RuntimeError("委托累计成交量无效")
            # 防止较早的查询覆盖终态回调，但累计成交仍取最大值。
            if order["status"] not in TERMINAL:
                order["status"] = status
            order["filled"] = max(order["filled"], filled)
            if status == 57:
                order["error"] = str(value(row, "m_strCancelInfo") or value(row, "m_strErrorMsg") or "未提供废单原因")
                self.emit("委托拒绝", code, "原因: " + order["error"])
        else:
            self.book_fill(order, row)
        if order != before:
            self.save()

    def book_fill(self, order, row):
        trade_id = str(value(row, "m_strTradeID", ""))
        if trade_id in ("", "0", "None"):
            raise RuntimeError("真实成交编号缺失，不得记成 0 或重复入账")
        qty = int(value(row, "m_nVolume", 0))
        price = positive(value(row, "m_dPrice"))
        if qty <= 0 or price is None:
            raise RuntimeError("真实成交数量/价格无效")
        key = order["tag"] + ":" + trade_id
        fee = value(row, "m_dCommission", value(row, "m_dComission"))
        fee = float(fee) if fee is not None else None
        if fee is not None and (not math.isfinite(fee) or fee < 0 or fee >= qty * price):
            fee = None
        if key in self.state["fills"]:
            old = self.state["fills"][key]
            if old["qty"] != qty or old["price"] != price:
                raise RuntimeError("同一成交编号的数据变化，需人工核对")
            if fee is not None and old["fee"] != fee:
                self.state["cash"] -= fee - (old["fee"] or 0)
                old["fee"] = fee
                self.save()
            return
        if order["booked"] + qty > order["qty"]:
            raise RuntimeError("累计成交超过原委托量")
        pos = self.state["positions"].setdefault(order["code"], dict(qty=0, cost=0.0))
        amount = qty * price
        if order["side"] == "BUY":
            pos["cost"] += amount
            pos["qty"] += qty
            self.state["cash"] -= amount + (fee or 0)
        else:
            if qty > pos["qty"]:
                raise RuntimeError("成交卖出数量超过策略归属持仓")
            pos["cost"] *= (pos["qty"] - qty) / pos["qty"]
            pos["qty"] -= qty
            self.state["cash"] += amount - (fee or 0)
        order["booked"] += qty
        self.state["fills"][key] = dict(qty=qty, price=price, fee=fee)
        # 先落盘再通知；通知失败不得导致成交重复入账。
        self.save()
        self.emit("成交回报", order["code"], "方向: {}\n数量: {}\n单价: {:.3f}\n金额: {:.2f}\n手续费: {}\n成交编号: {}".format(
            order["side"], qty, price, amount, "未知" if fee is None else fee, trade_id))

    def sync(self, today):
        for kind in ("order", "deal"):
            for row in self.query(kind):
                self.report(kind, row)
        # 只为未结清的跨日委托查历史，不假设“今天查不到=已撤”。
        days = {order["day"] for order in self.state["orders"].values()
                if order["day"] < today and not settled(order)}
        if days:
            for kind in ("order", "deal"):
                groups = self.data.api("get_history_trade_detail_data")(
                    self.account, "stock", kind, min(days), today)
                for _, rows in groups:
                    for row in rows:
                        self.report(kind, row)

    def mark_to_market(self, now):
        positions = {code: pos for code, pos in self.state["positions"].items() if pos["qty"]}
        quotes = self.data.quotes(positions, now)
        return self.state["cash"] + sum(pos["qty"] * quotes[code]["last_price"] for code, pos in positions.items())

    def target(self, qty, reference, style, day):
        return dict(qty=qty, reference=reference, style=style, day=day)

    def rebalance(self, decision, now):
        day = now.strftime("%Y%m%d")
        self.sync(day)
        if any(not settled(o) for o in self.state["orders"].values()):
            raise RuntimeError("旧委托尚未结清，不能创建新调仓")
        equity = self.mark_to_market(now)
        positions = self.state["positions"]
        old_codes = [code for code, pos in positions.items() if pos["qty"] and code not in decision.marks]
        marks = dict(decision.marks)
        marks.update({code: row["last_price"] for code, row in self.data.quotes(old_codes, now).items()})
        targets = {}
        for code, ref in marks.items():
            qty = int(equity * decision.weights.get(code, 0) / ref / 100) * 100
            current = positions.get(code, {}).get("qty", 0)
            targets[code] = self.target(qty, ref, "market" if current > qty else "limit", day)
            self.emit("目标计划", code, "目标数量: {}\n单价: {:.3f}\n目标金额: {:.2f}\n权重: {:.2%}".format(
                qty, ref, qty * ref, decision.weights.get(code, 0)))
        # 观察模式不生成可以在重启后意外恢复的交易目标。
        if self.settings.ENABLE_TRADING:
            self.state["targets"] = targets
            self.save()

    def risk(self, signal, now):
        positions = {code: pos for code, pos in self.state["positions"].items() if pos["qty"]}
        quotes = self.data.quotes(positions, now)
        for code, pos in positions.items():
            price = quotes[code]["last_price"]
            reason = signal(pos["cost"] / pos["qty"], price)
            if not reason:
                continue
            style = "market" if reason == "stop_loss" else "profit"
            existing = self.state["targets"].get(code)
            if existing and existing["qty"] == 0 and existing["style"] == style:
                continue  # 已在退出，不重设固定参考价或反复撤原单
            self.emit("风控目标", code, "原因: {}\n单价: {:.3f}\n目标数量: 0".format(reason, price))
            if self.settings.ENABLE_TRADING:
                self.state["targets"][code] = self.target(0, price, style, now.strftime("%Y%m%d"))
                self.save()
                for order in self.state["orders"].values():
                    if order["code"] == code and not settled(order):
                        self.cancel(order)

    def cancel(self, order):
        if not self.settings.ENABLE_TRADING or order["status"] in TERMINAL:
            return
        if not order["sysid"]:
            raise RuntimeError("待撤委托尚无柜台编号，等待回报，不重发")
        if time.time() - order.get("cancel_at", 0) < 30:
            return
        order["cancel_at"] = time.time()
        self.save()
        accepted = self.data.api("cancel")(order["sysid"], self.account, "stock", self.data.c)
        self.emit("撤单请求", order["code"], "已发送: {}；实际撤单以委托终态为准".format(bool(accepted)))

    def expire(self, today):
        for order in self.state["orders"].values():
            if order["day"] < today and not settled(order):
                self.cancel(order)
        targets = {code: target for code, target in self.state["targets"].items() if target["day"] == today}
        if targets != self.state["targets"]:
            self.state["targets"] = targets
            self.save()

    def order_price(self, code, target, side):
        rate = (1 + self.settings.BUY_PREMIUM) if side == "BUY" else (
            1 - (self.settings.PROFIT_DISCOUNT if target["style"] == "profit" else self.settings.MARKET_PROTECTION))
        detail = self.data.detail(code)
        tick = positive(detail.get("PriceTick")) or 0.001
        price = target["reference"] * rate
        upper, lower = positive(detail.get("UpStopPrice")), positive(detail.get("DownStopPrice"))
        if upper:
            price = min(price, upper)
        if lower:
            price = max(price, lower)
        # 不超出策略可接受价格；ETF 精度不套用股票的两位小数。
        rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        price = float((Decimal(str(price)) / Decimal(str(tick))).to_integral_value(rounding=rounding) * Decimal(str(tick)))
        pr_type = 42 if (side == "SELL" and target["style"] == "market" and code.endswith(".SH")
                        and self.settings.SH_NATIVE_MARKET) else 11
        return price, pr_type

    def advance(self, now):
        if not self.settings.ENABLE_TRADING:
            return
        today, clock = now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")
        if not ("09:30:00" <= clock < "11:30:00" or "13:00:00" <= clock < "14:57:00"):
            return
        targets, orders, positions = self.state["targets"], self.state["orders"], self.state["positions"]
        if not targets:
            return
        self.allocate()
        physical = self.positions()
        for code, pos in positions.items():
            if pos["qty"] > int(value(physical.get(code), "m_nVolume", 0)):
                raise RuntimeError("券商持仓不足策略归属数量: " + code + "；可能手动卖出或回报尚未同步")
        if any(o["day"] != today and not settled(o) for o in orders.values()):
            raise RuntimeError("跨日委托尚未结清，等待历史回报")
        sell_pending = any(positions.get(code, {}).get("qty", 0) > target["qty"] for code, target in targets.items())
        sell_pending = sell_pending or any(o["side"] == "SELL" and not settled(o) for o in orders.values())
        reserved = sum((o["qty"] - o["booked"]) * o["price"] + self.settings.CASH_RESERVE
                       for o in orders.values() if o["side"] == "BUY" and not settled(o))
        cash = min(self.state["cash"] - reserved, self.broker_cash())
        for code, target in targets.items():
            if target["day"] != today:
                continue
            prior = [o for o in orders.values() if o["code"] == code and o["day"] == today]
            if any(not settled(o) for o in prior):
                for order in prior:
                    if not settled(order) and order.get("target") != target:
                        self.cancel(order)
                    elif not settled(order) and not order["sysid"] and time.time() - order["sent_at"] >= 30:
                        self.emit("等待委托回报", code, "原因: 下单后尚无柜台编号\n处理: 查询确认，不重复发送\n备注: " + order["tag"])
                continue
            if prior and time.time() - prior[-1]["sent_at"] < self.settings.RETRY_SECONDS:
                continue
            if any(o["status"] == 57 for o in prior):
                continue  # ETF 不做股票笼子重试；不支持类型/余额等废单需查看原因
            current = positions.get(code, {}).get("qty", 0)
            delta = target["qty"] - current
            if delta == 0 or (delta > 0 and sell_pending):
                continue
            side = "BUY" if delta > 0 else "SELL"
            price, pr_type = self.order_price(code, target, side)
            if not positive(price):
                raise RuntimeError("委托价格无效: " + code)
            if side == "BUY":
                qty = min(delta, max(0, int((cash - self.settings.CASH_RESERVE) / price / 100) * 100))
            else:
                available = int(value(physical.get(code), "m_nCanUseVolume", 0))
                qty = min(-delta, available)
                if qty < current:
                    qty = qty // 100 * 100
            if qty <= 0:
                continue  # T+1 可卖为0不报账本损坏；等待，且不跳过先卖后买
            order = dict(tag=self.prefix + uuid.uuid4().hex[:14], code=code, side=side, qty=qty,
                         price=price, day=today, sysid="", status=48, filled=0, booked=0,
                         sent_at=time.time(), error="", target=dict(target))
            orders[order["tag"]] = order
            self.save()  # 必须先保存投资备注，再发送；异常结果不允许自动重发
            try:
                self.data.api("passorder")(23 if side == "BUY" else 24, 1101, self.account,
                    code, pr_type, price, qty, self.settings.STRATEGY_ID, 2, order["tag"], self.data.c)
            except Exception as exc:
                order["error"] = "下单结果未知: " + str(exc)
                self.save()
                raise RuntimeError(order["error"])
            self.emit("下单请求", code, "方向: {}\n数量: {}\n单价: {:.3f}\n金额: {:.2f}\n类型: {}\n状态: 等待柜台回报\n备注: {}".format(
                side, qty, price, qty * price, "五档即成剩撤" if pr_type == 42 else "固定限价", order["tag"]))
            if side == "BUY":
                cash -= qty * price + self.settings.CASH_RESERVE


class Runtime:
    def __init__(self, context, namespace, settings, prepare_fn, select_fn, risk_fn):
        self.c, self.settings = context, settings
        self.prepare_fn, self.select_fn, self.risk_fn = prepare_fn, select_fn, risk_fn
        self.data = QmtData(context, namespace, settings, self.log)
        self.account = settings.ACCOUNT_ID or str(namespace.get("account", ""))
        if not self.account:
            raise RuntimeError("请在 QMT 策略交易界面选择账户，或填写 ACCOUNT_ID")
        if positive_capital(settings.INITIAL_CAPITAL) is False:
            raise RuntimeError("INITIAL_CAPITAL 必须为正数")
        if getattr(context, "do_back_test", False):
            raise RuntimeError("本版本使用每日更新 PCF，仅支持实时运行，不支持历史回测")
        self.guard = threading.RLock()
        self.closed = False
        self.notices, self.last_sync, self.day, self.frame = {}, 0, "", None
        self.calendar_checked_at = 0
        self.executor = Executor(self.data, settings, self.account, self.emit)

    def log(self, text):
        print("{} [{}] {}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.settings.STRATEGY_ID, text))

    def handlebar(self):
        pass  # 实时初始化遍历历史 bar，不能在这里触发 quickTrade=2

    def emit(self, title, code="", body=""):
        try:
            label = self.data.label(code) if code else ""
        except Exception:
            label = code  # 名称查询失败不能影响成交入账或发单状态
        title = "[{}] {} {}".format(self.settings.STRATEGY_ID, title, label).strip()
        key = (title, body)
        if key in self.notices:  # 相同原因本次运行只通知一次，避免每秒刷卡片
            return
        self.notices[key] = True
        self.log(title + " | " + body.replace("\n", " | "))
        if not self.settings.FEISHU_WEBHOOK:
            return
        payload = {"msg_type": "interactive", "card": {
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": "策略ID: " + self.settings.STRATEGY_ID + "\n" + body}}]}}
        # 只有通知线程可访问网络，不在工作线程调用任何 QMT API。
        def send():
            try:
                request = urllib.request.Request(self.settings.FEISHU_WEBHOOK,
                    data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=5) as response:
                    result = json.load(response)
                if result.get("code", result.get("StatusCode", 0)) != 0:
                    self.log("飞书通知失败（交易入账不受影响）")
            except Exception:
                self.log("飞书通知发送失败（交易入账不受影响）")
        threading.Thread(target=send, daemon=True).start()

    def start(self, prepare_time, open_time, risk_times, snapshot_time):
        self.times = prepare_time, open_time, tuple(risk_times), snapshot_time
        self.c.set_account(self.account)
        self.c.run_time("on_timer", "1nSecond", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.log("完整 QMT 原生版 | 交易={} | 账本={} | 请勿与旧 remote 同时运行".format(
            self.settings.ENABLE_TRADING, self.executor.path))

    def close(self):
        # stop 时交易连接已断开，只释放本地资源，不查询、下单或撤单。
        with self.guard:
            if self.closed:
                return
            self.closed = True
            self.executor.close()
            self.log("策略已停止 | 账本锁已释放 | 账本保留；未撤销柜台委托")

    def on_report(self, kind, row):
        with self.guard:
            if self.closed:
                return
            try:
                self.executor.report(kind, row)
                self.last_sync = 0  # 下一次定时器查询补全，并推进剩余量；回调内不递归下单
            except Exception as exc:
                self.emit("回报处理阻断", body="原因: " + str(exc))

    def on_error(self, args, error):
        # 官方 orderArgs.strategyName 的格式为 策略名_&&&_投资备注。
        tag = str(value(args, "strategyName", "")).rsplit("_&&&_", 1)[-1]
        with self.guard:
            if self.closed:
                return
            order = self.executor.state["orders"].get(tag)
            if order is not None:
                order["error"] = str(error)
                self.executor.save()
                self.emit("下单异常", order["code"], "原因: " + str(error) + "\n处理: 等待委托查询确认，不盲目重发")
            elif str(value(args, "strategyName", "")).startswith(self.settings.STRATEGY_ID):
                self.emit("下单异常", body="原因: " + str(error) + "\n处理: 未能关联投资备注，请检查柜台委托")

    def tick(self, now=None):
        now = now or dt.datetime.now()
        with self.guard:
            if self.closed:
                return
            try:
                today, clock = now.strftime("%Y%m%d"), now.strftime("%H:%M")
                if today != self.day or (not self.is_trading_day and "09:00" <= clock <= "15:00"
                                         and time.time() - self.calendar_checked_at >= 60):
                    self.previous, self.is_trading_day = self.data.calendar(now)
                    self.calendar_checked_at = time.time()
                    if today != self.day:
                        self.frame, self.day, self.notices = None, today, {}
                if time.time() - self.last_sync >= self.settings.QUERY_SECONDS:
                    self.executor.sync(today)
                    self.executor.expire(today)  # 午夜/重启处理隔日委托，绝不把未知直接当已撤
                    self.last_sync = time.time()
                if not self.is_trading_day:
                    return
                prepare_at, open_at, risk_times, snapshot_at = self.times
                key = today + ":" + clock
                if key not in self.executor.state["jobs"]:
                    if clock == prepare_at:
                        self.frame = self.prepare_fn(self.data, self.previous, today)
                        self.log("盘前候选: {}".format(0 if self.frame is None else len(self.frame)))
                    elif clock == open_at:
                        if self.frame is None:
                            self.frame = self.prepare_fn(self.data, self.previous, today)
                        if self.frame is not None and not self.frame.empty:
                            quotes = self.data.quotes(self.frame.index.tolist(), now)
                            self.executor.rebalance(self.select_fn(self.frame, quotes), now)
                        else:
                            self.log("预处理无候选，跳过本轮；没有发出清仓指令")
                    elif clock in risk_times:
                        self.executor.risk(self.risk_fn, now)
                    elif clock == snapshot_at:
                        equity = self.executor.mark_to_market(now)
                        fills = list(self.executor.state["fills"].values())
                        unknown = sum(row["fee"] is None for row in fills)
                        self.emit("尾盘快照", body="策略权益: {:.2f}\n净值: {:.6f}\n已知手续费: {:.2f}\n未知手续费成交: {} 笔\n最后行情时间: {}".format(
                            equity, equity / self.settings.INITIAL_CAPITAL,
                            sum(row["fee"] or 0 for row in fills), unknown, self.data.last_quote_time))
                    else:
                        key = None
                    if key:
                        self.executor.state["jobs"].append(key)
                        self.executor.save()
                self.executor.advance(now)
            except Exception as exc:
                self.emit("运行阻断", body="原因: " + str(exc) + "\n处理: 保留账本与委托，下一次调度重新检查")


def positive_capital(amount):
    from math import isfinite
    return isinstance(amount, (int, float)) and isfinite(amount) and amount > 0


# ==================== QMT 原生回测适配（不创建实时账本） ====================

def backtest_time(raw):
    """QMT 毫秒时间戳按中国时区解析；兼容历史 DataFrame 的日期索引。"""
    if isinstance(raw, (dt.datetime, pd.Timestamp)):
        if raw.tzinfo is not None:
            raw = raw.astimezone(dt.timezone(dt.timedelta(hours=8)))
        return raw.replace(tzinfo=None)
    text = str(raw)
    if text.isdigit() and len(text) == 14:
        return dt.datetime.strptime(text, "%Y%m%d%H%M%S")
    if isinstance(raw, (int, float)) or (text.isdigit() and len(text) == 13):
        return dt.datetime.fromtimestamp(float(raw) / 1000, dt.timezone(dt.timedelta(hours=8))).replace(tzinfo=None)
    return pd.to_datetime(text).to_pydatetime().replace(tzinfo=None)


class HistoricalQuoteMissing(RuntimeError):
    pass


class BacktestData(QmtData):
    def __init__(self, context, namespace, settings, log):
        super().__init__(context, namespace, settings, log)
        if not settings.BACKTEST_DATA_FILE:
            raise RuntimeError("回测需填写 BACKTEST_DATA_FILE：先导出历史 ETF 池和单位净值；不能使用今日 PCF")
        with open(settings.BACKTEST_DATA_FILE, encoding="utf-8-sig") as stream:
            document = json.load(stream)
        if document.get("schema") != 1 or not document.get("sessions"):
            raise RuntimeError("回测数据格式错误：需要 schema=1 和非空 sessions")
        self.sessions, self.session = document["sessions"], None

    def set_day(self, today, previous):
        session = self.sessions.get(today)
        if (not session or session.get("previous_date") != previous
                or previous >= today or not isinstance(session.get("securities"), dict)):
            raise RuntimeError("历史数据缺失或前交易日不匹配: " + today + " / " + previous)
        self.session = session
        self.today = today

    def universe(self, previous):
        if previous != self.session["previous_date"]:
            raise RuntimeError("历史 ETF 池日期不匹配")
        return [(code, row["name"], row.get("index_name", ""))
                for code, row in self.session["securities"].items()]

    def nav(self, codes, previous, today):
        if today != self.today or previous != self.session["previous_date"]:
            raise RuntimeError("历史单位净值日期不匹配")
        rows = {code: positive(self.session["securities"][code].get("nav")) for code in codes}
        missing = [code for code, nav in rows.items() if nav is None]
        if missing:
            self.log("历史前日净值缺失 {} 只: {}；按原逻辑排除，不使用 PCF/IOPV".format(len(missing), missing[:10]))
        rows = {code: nav for code, nav in rows.items() if nav is not None}
        if not rows:
            raise RuntimeError("全体历史前日单位净值缺失: " + previous)
        return pd.DataFrame.from_dict(rows, orient="index", columns=["unit_net_value"])

    def label(self, code):
        row = self.session["securities"].get(code, {}) if self.session else {}
        return "{}({})".format(code, row.get("name", "历史名称缺失"))

    def quotes(self, codes, now):
        if not codes:
            return {}
        end = now.strftime("%Y%m%d%H%M%S")
        frames = self.c.get_market_data_ex(
            ["close", "suspendFlag"], list(codes), period="1m",
            start_time=now.strftime("%Y%m%d") + "093000", end_time=end, count=1,
            dividend_type="none", fill_data=False, subscribe=False)
        rows = {}
        for code in codes:
            frame = frames.get(code)
            if frame is None or frame.empty:
                raise HistoricalQuoteMissing("历史分钟行情缺失: " + code + " / " + end)
            stamp, bar = backtest_time(frame.index[-1]), frame.iloc[-1]
            if stamp < now:
                raise HistoricalQuoteMissing("历史分钟行情过期: " + code + " / " + end)
            if stamp > now:
                raise RuntimeError("历史接口返回未来行情，拒绝使用: " + code + " / " + end)
            price = positive(bar.get("close"))
            flag = bar.get("suspendFlag")
            if price is None or flag not in (0, 1):
                raise RuntimeError("历史分钟价格或 suspendFlag 缺失: " + code)
            meta = self.session["securities"].get(code, {})
            if "high_limit" not in meta:
                raise RuntimeError("历史涨停价缺失（不能套用今天的涨停价）: " + code)
            ceiling = meta["high_limit"]
            if ceiling is not None and positive(ceiling) is None:
                raise RuntimeError("历史涨停价无效: " + code)
            rows[code] = dict(last_price=price, paused=bool(flag),
                              high_limit=float("inf") if ceiling is None else float(ceiling))
        self.last_quote_time = end
        return rows


class BacktestRuntime:
    """仅使用 QMT 回测账户和撮合；不复用实时订单状态机或持久账本。"""
    def __init__(self, context, namespace, settings, prepare_fn, select_fn, risk_fn):
        if not getattr(context, "do_back_test", False):
            raise RuntimeError("回测适配器不能用于实时交易")
        if str(context.period).lower() != "1m":
            raise RuntimeError("GoodETF 回测请在 QMT 设置 1 分钟周期，不支持用日线伪造盘中风控")
        if context.dividend_type != "none":
            raise RuntimeError("GoodETF 回测请设置不复权，不能用复权成交价与单位净值混算折价")
        if not positive_capital(context.capital):
            raise RuntimeError("请在 QMT 回测面板设置有效初始资金")
        self.c, self.settings = context, settings
        self.prepare_fn, self.select_fn, self.risk_fn = prepare_fn, select_fn, risk_fn
        self.now, self.day, self.jobs, self.closed, self.last_bar = None, "", set(), False, None
        self.data = BacktestData(context, namespace, settings, self.log)
        self.account = "good_etf_backtest"  # 虚拟标识；绝不采用 ACCOUNT_ID 或界面柜台账号
        self.c.account_id = self.account

    def start(self, prepare_time, open_time, risk_times, snapshot_time):
        self.times = prepare_time, open_time, tuple(risk_times), snapshot_time
        self.log("QMT历史回测 | 资金/费用/滑点取回测面板 | 1分钟已完成K线 | 开盘允许09:31首bar | 原生撮合，不模拟柜台追单")

    def log(self, text):
        print("{} [{}][回测] {}".format(self.now or "初始化", self.settings.STRATEGY_ID, text))

    def query(self, kind):
        if self.closed or not getattr(self.c, "do_back_test", False):
            raise RuntimeError("回测已停止或环境已切换，拒绝查询账户")
        rows = self.data.api("get_trade_detail_data")(self.account, "stock", kind)
        if rows is None:
            raise RuntimeError("QMT 回测查询未返回数据: " + kind)
        return list(rows)

    def portfolio(self):
        accounts = self.query("account")
        equity = float(value(accounts[0], "m_dBalance", float("nan"))) if len(accounts) == 1 else float("nan")
        if not math.isfinite(equity) or equity < 0:
            raise RuntimeError("QMT 回测账户未就绪，不能使用柜台资金替代")
        positions = {value(row, "m_strInstrumentID") + "." + value(row, "m_strExchangeID"): row
                     for row in self.query("position") if value(row, "m_nVolume", 0) > 0}
        return equity, positions

    def submit(self, code, target):
        if self.closed or not getattr(self.c, "do_back_test", False):
            raise RuntimeError("回测已停止或环境已切换，拒绝提交")
        self.data.api("order_target_value")(code, float(target), self.c, self.account)
        self.log("回测目标市值 | {} -> {:.2f} | LATEST；实际成交以QMT回测面板为准".format(self.data.label(code), target))

    def rebalance(self, decision):
        equity, positions = self.portfolio()
        codes = list(dict.fromkeys(list(positions) + list(decision.weights)))
        quotes = self.data.quotes(codes, self.now)  # 提交前检查整组行情，避免一半提交后才发现数据错误
        targets = {code: equity * decision.weights.get(code, 0) for code in codes}
        reductions, increases = [], []
        for code in codes:
            current = value(positions.get(code, {}), "m_nVolume", 0) * quotes[code]["last_price"]
            if targets[code] < current:
                reductions.append(code)
            elif targets[code] > current:
                increases.append(code)
        for code in reductions + increases:
            self.submit(code, targets[code])

    def risk(self):
        _, positions = self.portfolio()
        quotes = self.data.quotes(list(positions), self.now)
        for code, row in positions.items():
            cost, price = positive(value(row, "m_dOpenPrice")), quotes[code]["last_price"]
            if cost is None:
                raise RuntimeError("QMT 回测持仓成本缺失: " + code)
            signal = self.risk_fn(cost, price)
            self.log("持仓检查 | {} 成本={:.4f} 现价={:.4f} 信号={}".format(self.data.label(code), cost, price, signal))
            if signal and not quotes[code]["paused"]:
                self.submit(code, 0)

    def handlebar(self):
        try:
            self._handlebar()
        except Exception as exc:
            self.closed = True
            self.log("回测已停止 | 原因: {} | 本次结果不完整，不应作为有效收益报告".format(exc))
            raise

    def _handlebar(self):
        if self.closed:
            return
        if not getattr(self.c, "do_back_test", False):
            raise RuntimeError("回测环境已切换，拒绝执行历史bar")
        now = backtest_time(self.c.get_bar_timetag(self.c.barpos))
        start, end = getattr(self.c, "start", ""), getattr(self.c, "end", "")
        if (start and now < backtest_time(start)) or (end and now > backtest_time(end)):
            return  # QMT 若遍历区间外预热 bar，不将其作为正式回测交易
        if self.last_bar is not None and now <= self.last_bar:
            return
        self.now, self.last_bar = now, now
        today, clock = now.strftime("%Y%m%d"), now.strftime("%H:%M")
        if today != self.day:
            previous, trading = self.data.calendar(now)
            if not trading:
                return
            self.data.set_day(today, previous)
            self.day, self.jobs, self.frame = today, set(), None
        prepare_at, open_at, risk_times, snapshot_at = self.times
        opening_end = (dt.datetime.strptime(open_at, "%H:%M") + dt.timedelta(minutes=1)).strftime("%H:%M")
        if prepare_at <= clock <= opening_end and "prepare" not in self.jobs:
            self.frame = self.prepare_fn(self.data, self.data.session["previous_date"], today)
            self.jobs.add("prepare")
            self.log("盘前候选: {}".format(0 if self.frame is None else len(self.frame)))
        if open_at <= clock <= opening_end and "open" not in self.jobs:
            if self.frame is not None and not self.frame.empty:
                try:
                    quotes = self.data.quotes(self.frame.index.tolist(), now)
                except HistoricalQuoteMissing:
                    if clock == open_at:
                        self.log("开盘分钟线尚未结束，等待下一分钟首根完整bar；不读取未来行情")
                        return
                    raise
                decision = self.select_fn(self.frame, quotes)
                self.log("开盘选股 | 目标权重={}".format(decision.weights))
                self.rebalance(decision)
            else:
                self.log("预处理无候选，跳过本轮；没有发出清仓指令")
            self.jobs.add("open")
        if clock in risk_times and clock not in self.jobs:
            self.jobs.add(clock)
            self.risk()
        if clock == snapshot_at and "snapshot" not in self.jobs:
            self.jobs.add("snapshot")
            equity, positions = self.portfolio()
            self.log("尾盘快照 | 回测权益={:.2f} 净值={:.6f} 持仓数={}".format(equity, equity / self.c.capital, len(positions)))

    def tick(self):
        pass  # 不注册也不处理实时定时器

    def on_report(self, kind, row):
        pass  # 回测成交由平台入账，不重复维护现金/持仓

    def on_error(self, args, error):
        pass

    def close(self):
        self.closed = True


def create_runtime(context, namespace, settings, prepare_fn, select_fn, risk_fn):
    runtime_type = BacktestRuntime if getattr(context, "do_back_test", False) else Runtime
    return runtime_type(context, namespace, settings, prepare_fn, select_fn, risk_fn)
