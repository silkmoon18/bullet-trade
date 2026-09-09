from datetime import datetime

import pytest

from bullet_trade.server.feishu_notifier import (
    FeishuNotifier,
    FeishuTradeNotifier,
    TargetBuyPlanItem,
    TargetBuyPlanNotification,
    TradeNotification,
    reconciliation_notification,
)
from bullet_trade.server.strategy.domain import SHANGHAI_TZ


def test_trade_notification_uses_interactive_card_with_required_fields():
    notifier = FeishuTradeNotifier("https://example.invalid/hook/test")
    payload = notifier.build_payload(
        TradeNotification(
            event="FILLED",
            strategy_id="good_etf",
            security="510050.XSHG",
            security_name="上证50ETF",
            side="BUY",
            status="FILLED",
            quantity=1000,
            price="2.5000",
            amount="2505.00",
            order_id="order-1",
            trade_id="trade-1",
            occurred_at=datetime(2026, 8, 10, 10, 0, tzinfo=SHANGHAI_TZ),
        )
    )

    assert payload["msg_type"] == "interactive"
    assert payload["card"]["header"]["template"] == "green"
    assert payload["card"]["header"]["title"]["content"] == (
        "订单全部成交 · 上证50ETF（510050.XSHG） · good_etf"
    )
    content = payload["card"]["elements"][0]["text"]["content"]
    lines = content.splitlines()
    assert lines[:9] == [
        "**策略ID：** `good_etf`",
        "**标的：** `510050.XSHG`",
        "**名称：** 上证50ETF",
        "**方向：** BUY",
        "**状态：** FILLED",
        "**数量：** 1000",
        "**金额：** ¥2505.00",
        "**单价：** ¥2.5000",
        "**时间：** 2026-08-10 10:00:00",
    ]
    assert "510050.XSHG" in content
    assert "¥2505.00" in content
    assert "1000" in content
    assert "¥2.5000" in content
    assert "order-1" in content
    assert "trade-1" in content


def test_rejected_order_card_keeps_empty_trade_values_visible():
    notifier = FeishuTradeNotifier("https://example.invalid/hook/test")
    payload = notifier.build_payload(
        TradeNotification(
            event="REJECTED",
            security="510300.XSHG",
            side="SELL",
            status="REJECTED",
            quantity=500,
            order_id="order-2",
        )
    )

    assert payload["card"]["header"]["template"] == "red"
    assert payload["card"]["header"]["title"]["content"] == (
        "订单被拒绝 · 510300.XSHG"
    )
    content = payload["card"]["elements"][0]["text"]["content"]
    assert "**金额：** ¥-" in content
    assert "**单价：** ¥-" in content


def test_target_buy_plan_card_lists_items_and_total_amount():
    notifier = FeishuTradeNotifier("https://example.invalid/hook/test")
    payload = notifier.build_payload(
        TargetBuyPlanNotification(
            strategy_id="good_etf",
            mode="JQ",
            items=(
                TargetBuyPlanItem(
                    "510050.XSHG", 1000, "2500.00", "2.5000", "上证50ETF"
                ),
                TargetBuyPlanItem("159915.XSHE", 500, "750.00", "1.5000"),
            ),
            occurred_at=datetime(2026, 8, 13, 9, 30, tzinfo=SHANGHAI_TZ),
        )
    )

    assert payload["card"]["header"]["template"] == "orange"
    assert payload["card"]["header"]["title"]["content"] == (
        "策略目标买入计划 · good_etf · JQ"
    )
    elements = payload["card"]["elements"]
    assert [element["tag"] for element in elements] == [
        "div", "hr", "div", "hr", "div", "hr", "div"
    ]
    assert elements[0]["text"]["content"].splitlines() == [
        "**策略ID：** `good_etf`",
        "**模式：** `JQ`",
        "**时间：** 2026-08-13 09:30:00",
    ]
    assert elements[2]["text"]["content"].splitlines() == [
        "**标的：** `510050.XSHG`",
        "**名称：** 上证50ETF",
        "**目标数量：** 1000 股",
        "**目标金额：** ¥2500.00",
        "**单价：** ¥2.5000",
    ]
    assert elements[4]["text"]["content"].splitlines() == [
        "**标的：** `159915.XSHE`",
        "**目标数量：** 500 股",
        "**目标金额：** ¥750.00",
        "**单价：** ¥1.5000",
    ]
    footer = elements[6]["text"]["content"]
    assert "¥3250.00" in footer
    assert "不代表已提交委托或已经成交" in footer


def test_legacy_notifier_is_drop_in_compatible(monkeypatch):
    monkeypatch.setenv(
        "FEISHU_WEBHOOK_URL", "https://example.invalid/hook/legacy"
    )
    notifier = FeishuNotifier()
    sent = []
    monkeypatch.setattr(notifier, "_send_request", sent.append)

    notifier.queue_message("legacy log message")
    notifier.flush()

    assert notifier.webhook_url.endswith("/legacy")
    assert sent[0]["msg_type"] == "interactive"
    assert "legacy log message" in str(sent[0])


def test_legacy_notifier_accepts_structured_trade_notification(monkeypatch):
    notifier = FeishuNotifier("https://example.invalid/hook/trade")
    sent = []
    monkeypatch.setattr(notifier._sender, "send", sent.append)
    trade = TradeNotification(
        event="FILLED",
        security="510050.XSHG",
        side="BUY",
        status="FILLED",
        quantity=100,
        price="2.50",
        amount="255.00",
    )

    notifier.queue_message(trade)
    notifier.flush()

    assert sent == [trade]


def test_reconciliation_card_explains_each_position_shortage_and_uses_names():
    blockers = (
        "broker_position_insufficient:159086.XSHE:strategy=(1200,1200):broker=(1200,0)",
        "broker_position_insufficient:588370.XSHG:strategy=(600,600):broker=(500,0)",
    )
    notice = reconciliation_notification(
        "good_etf_remote", blockers,
        {"159086.XSHE": "金融科技ETF广发", "588370.XSHG": "科创50增强ETF南方"},
        datetime(2026, 9, 9, 9, 30, tzinfo=SHANGHAI_TZ),
    )
    payload = FeishuTradeNotifier("https://example.invalid/hook").build_payload(notice)
    title = payload["card"]["header"]["title"]["content"]
    content = payload["card"]["elements"][0]["text"]["content"]
    assert payload["card"]["header"]["template"] == "red"
    assert "金融科技ETF广发（159086.XSHE）" in title
    assert "科创50增强ETF南方（588370.XSHG）" in title
    assert title.endswith("good_etf_remote")
    assert "**问题 1：**" in content and "**问题 2：**" in content
    assert "**原因：** QMT可卖数量不足" in content
    assert "**原因：** QMT持仓总量不足" in content
    assert "**账本归属持仓：** 1200 股" in content
    assert "**账本要求可卖（扣除策略卖单冻结）：** 1200 股" in content
    assert "**QMT持仓总量：** 1200 股" in content
    assert "**QMT当前可卖：** 0 股" in content
    assert "**对账时间：** 2026-09-09 09:30:00" in content
    assert "JQ账户独立运行" in content
    assert "不会因此撤销已有柜台委托" in content
    assert "**处理提示：**" in content
    assert all(raw in content for raw in blockers)
    assert "**单价：**" not in content and "**方向：**" not in content
    assert "已完成结算" not in content


def test_reconciliation_cash_units_are_displayed_in_yuan():
    notice = reconciliation_notification("s", (
        "broker_cash_insufficient:strategy_required=100000000:broker=90000000",
    ), {})
    assert "**账本要求可用资金：** ¥10000.00" in notice.detail
    assert "**QMT可用资金：** ¥9000.00" in notice.detail


@pytest.mark.parametrize("raw,reason", [
    ("trade_error:t1:broker fill id was reused with different fields", "成交回报校验或入账失败"),
    ("owned_trade_order_missing:0", "策略成交未找到对应委托"),
    ("missing_working_order:o1", "本地活动委托在柜台查询中缺失"),
    ("capability:stable_trade_id is unsupported", "券商接口能力验证未通过"),
    ("broker_position_insufficient:malformed", "其他对账异常"),
    ("new_unknown_blocker:details", "其他对账异常"),
])
def test_reconciliation_card_keeps_raw_error_with_readable_fallback(raw, reason):
    notice = reconciliation_notification("s", (raw,), {})
    assert "**原因：** " + reason in notice.detail
    assert "**原始错误：** `" + raw + "`" in notice.detail
    assert "**描述：**" in notice.detail


def test_reconciliation_missing_names_and_empty_reasons_still_build_cards():
    notice = reconciliation_notification("s", (
        "broker_position_insufficient:159086.XSHE:strategy=(100,100):broker=(100,0)",
    ), {})
    assert notice.title.endswith("159086.XSHE")
    empty = reconciliation_notification("s", (), {})
    payload = FeishuTradeNotifier("https://example.invalid/hook").build_payload(empty)
    assert "未提供具体阻断原因" in str(payload)
