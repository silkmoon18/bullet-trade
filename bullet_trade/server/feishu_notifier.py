"""Feishu interactive cards for strategy order and fill notifications."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from queue import Queue
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

import requests  # type: ignore[import-untyped]


Number = Union[str, int, float, Decimal]
SHANGHAI_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")
MONEY_SCALE = 10_000
PRICE_SCALE = 1_000_000


@dataclass(frozen=True)
class TradeNotification:
    event: str
    security: str
    side: str
    status: str
    quantity: Optional[int] = None
    price: Optional[Number] = None
    amount: Optional[Number] = None
    order_id: Optional[str] = None
    trade_id: Optional[str] = None
    detail: str = ""
    occurred_at: Optional[datetime] = None
    title: Optional[str] = None
    strategy_id: str = "-"
    security_name: str = ""


@dataclass(frozen=True)
class TargetBuyPlanItem:
    security: str
    quantity: int
    amount: Number
    reference_price: Optional[Number] = None
    security_name: str = ""


@dataclass(frozen=True)
class TargetBuyPlanNotification:
    strategy_id: str
    mode: str
    items: Tuple[TargetBuyPlanItem, ...]
    occurred_at: Optional[datetime] = None


def _signature(timestamp: int, secret: str) -> str:
    text = "{}\n{}".format(timestamp, secret)
    digest = hmac.new(
        text.encode("utf-8"), msg=b"", digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def _display(value: Optional[Number], digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        number = Decimal(str(value))
    except Exception:
        return str(value)
    return "{:.{}f}".format(number, digits)


def _markdown_div(lines: list[str]) -> Dict[str, Any]:
    return {
        "tag": "div",
        "text": {"tag": "lark_md", "content": "\n".join(lines)},
    }


def _security_title(security: str, security_name: str) -> str:
    code = str(security or "").strip()
    name = str(security_name or "").strip()
    if not code or code == "-":
        return name
    if name:
        return "{}（{}）".format(name, code)
    return code


def reconciliation_notification(
    strategy_id: str,
    blockers: Sequence[str],
    security_names: Mapping[str, str],
    occurred_at: Optional[datetime] = None,
) -> TradeNotification:
    """Describe reconciliation evidence without changing its trading decision."""
    lines = [
        "**影响：** 当前QMT策略未通过对账，新目标提交及后续发单被拦截；"
        "不会因此撤销已有柜台委托。JQ账户独立运行。",
    ]
    securities = []
    for index, raw in enumerate(blockers or ("未提供具体阻断原因",), 1):
        raw = str(raw)
        codes = re.findall(r"\b\d{6}\.(?:XSHG|XSHE|XBSE|BJ)\b", raw)
        securities.extend(code for code in codes if code not in securities)
        lines.append("\n**问题 {}：**".format(index))
        for code in dict.fromkeys(codes):
            lines.append("**标的：** {}".format(
                _security_title(code, security_names.get(code, ""))
            ))
        position = re.fullmatch(
            r"broker_position_insufficient:[^:]+:strategy=\((\d+),(\d+)\):"
            r"broker=\((\d+),(\d+)\)", raw,
        )
        cash = re.fullmatch(
            r"broker_cash_insufficient:strategy_required=(\d+):broker=(\d+)", raw,
        )
        if position:
            owned, required, total, sellable = map(int, position.groups())
            total_shortage = total < owned
            lines.extend([
                "**原因：** {}".format(
                    "QMT持仓总量不足" if total_shortage else "QMT可卖数量不足"
                ),
                "**账本归属持仓：** {} 股".format(owned),
                "**账本要求可卖（扣除策略卖单冻结）：** {} 股".format(required),
                "**QMT持仓总量：** {} 股".format(total),
                "**QMT当前可卖：** {} 股".format(sellable),
                "**描述：** {}".format(
                    "QMT持仓不足以覆盖账本归属数量，需核对成交和人工操作。"
                    if total_shortage else
                    "持仓总量足够，但柜台可卖不足；不能据此认定股票已丢失。"
                ),
                "**处理提示：** 核对柜台持仓、冻结委托及结算状态；"
                "差异原因须另行确认，不会自动改写持仓或可卖数量。",
            ])
        elif cash:
            required, available = map(int, cash.groups())
            lines.extend([
                "**原因：** QMT可用资金不足",
                "**账本要求可用资金：** ¥{}".format(
                    _display(Decimal(required) / MONEY_SCALE)
                ),
                "**QMT可用资金：** ¥{}".format(
                    _display(Decimal(available) / MONEY_SCALE)
                ),
                "**描述：** 差额超过当前对账允许的费用容差。",
                "**处理提示：** 核对资金冻结、出入金、成交及费用记录。",
            ])
        else:
            reason = {
                "capability": "券商接口能力验证未通过",
                "trade_error": "成交回报校验或入账失败",
                "order_error": "委托状态同步失败",
                "order_match_error": "柜台委托与本地订单关联失败",
                "owned_broker_order_missing_id": "策略委托缺少有效柜台订单号",
                "owned_order_broker_id_mismatch": "策略委托的柜台订单号不匹配",
                "owned_trade_order_missing": "策略成交未找到对应委托",
                "missing_working_order": "本地活动委托在柜台查询中缺失",
            }.get(raw.split(":", 1)[0], "其他对账异常")
            lines.extend([
                "**原因：** {}".format(reason),
                "**描述：** 当前回报或校验结果未满足账本一致性要求。",
                "**处理提示：** 根据原始错误核对对应委托、成交或能力验证记录。",
            ])
        lines.append("**原始错误：** `{}`".format(raw))
    title = "实盘账实对账已阻断"
    if securities:
        title += " · " + "、".join(
            _security_title(code, security_names.get(code, "")) for code in securities
        )
    return TradeNotification(
        event="RECONCILIATION_BLOCKED", strategy_id=strategy_id,
        security="-", side="-", status="BLOCKED", title=title,
        detail="\n".join(lines), occurred_at=occurred_at,
    )


class FeishuTradeNotifier:
    def __init__(
        self,
        webhook_url: str,
        secret: str = "",
        timeout_seconds: float = 10.0,
    ) -> None:
        if not webhook_url:
            raise ValueError("Feishu webhook_url cannot be empty")
        self.webhook_url = webhook_url
        self.secret = secret
        self.timeout_seconds = timeout_seconds

    def build_payload(
        self,
        notification: Union[TradeNotification, TargetBuyPlanNotification],
    ) -> Dict[str, Any]:
        if isinstance(notification, TargetBuyPlanNotification):
            return self._build_target_buy_plan_payload(notification)
        color = {
            "ORDER_SUBMITTED": "blue",
            "FILL": "green",
            "FILLED": "green",
            "PARTIALLY_FILLED": "turquoise",
            "CANCELED": "grey",
            "REJECTED": "red",
            "RECONCILIATION_BLOCKED": "red",
            "ERROR": "red",
        }.get(notification.event.upper(), "orange")
        title = notification.title or {
            "ORDER_SUBMITTED": "交易委托已提交",
            "FILL": "收到成交回报",
            "FILLED": "订单全部成交",
            "PARTIALLY_FILLED": "订单部分成交",
            "CANCELED": "订单已撤销",
            "REJECTED": "订单被拒绝",
        }.get(notification.event.upper(), "量化交易通知")
        security_title = _security_title(
            notification.security, notification.security_name
        )
        if security_title:
            title = "{} · {}".format(title, security_title)
        if notification.strategy_id and notification.strategy_id != "-":
            title = "{} · {}".format(title, notification.strategy_id)
        occurred_at = notification.occurred_at or datetime.now(SHANGHAI_TZ)
        if occurred_at.tzinfo is not None:
            occurred_at = occurred_at.astimezone(SHANGHAI_TZ)
        lines = [
            "**策略ID：** `{}`".format(notification.strategy_id or "-"),
            "**标的：** `{}`".format(notification.security or "-"),
        ]
        if notification.security_name:
            lines.append("**名称：** {}".format(notification.security_name))
        lines.extend([
            "**方向：** {}".format(notification.side or "-"),
            "**状态：** {}".format(notification.status or "-"),
            "**数量：** {}".format(
                notification.quantity
                if notification.quantity is not None
                else "-"
            ),
            "**金额：** ¥{}".format(_display(notification.amount, 2)),
            "**单价：** ¥{}".format(_display(notification.price, 4)),
            "**时间：** {}".format(occurred_at.strftime("%Y-%m-%d %H:%M:%S")),
        ])
        if notification.order_id:
            lines.append("**订单号：** `{}`".format(notification.order_id))
        if notification.trade_id:
            lines.append("**成交号：** `{}`".format(notification.trade_id))
        if notification.event.upper() == "RECONCILIATION_BLOCKED":
            lines = [
                "**策略ID：** `{}`".format(notification.strategy_id or "-"),
                "**状态：** 对账阻断（BLOCKED）",
                "**对账时间：** {}".format(occurred_at.strftime("%Y-%m-%d %H:%M:%S")),
                notification.detail or "**原因：** 未提供具体阻断原因",
            ]
        elif notification.detail:
            lines.append("**说明：** {}".format(notification.detail))
        payload: Dict[str, Any] = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": color,
                    "title": {"tag": "plain_text", "content": title},
                },
                "elements": [_markdown_div(lines)],
            },
        }
        if self.secret:
            timestamp = int(time.time())
            payload["timestamp"] = str(timestamp)
            payload["sign"] = _signature(timestamp, self.secret)
        return payload

    def _build_target_buy_plan_payload(
        self, notification: TargetBuyPlanNotification
    ) -> Dict[str, Any]:
        occurred_at = notification.occurred_at or datetime.now(SHANGHAI_TZ)
        if occurred_at.tzinfo is not None:
            occurred_at = occurred_at.astimezone(SHANGHAI_TZ)
        total_amount = sum(
            (Decimal(str(item.amount)) for item in notification.items),
            Decimal("0"),
        )
        summary_lines = [
            "**策略ID：** `{}`".format(notification.strategy_id),
            "**模式：** `{}`".format(notification.mode),
            "**时间：** {}".format(occurred_at.strftime("%Y-%m-%d %H:%M:%S")),
        ]
        elements = [_markdown_div(summary_lines), {"tag": "hr"}]
        for item in notification.items:
            item_lines = [
                "**标的：** `{}`".format(item.security),
            ]
            if item.security_name:
                item_lines.append("**名称：** {}".format(item.security_name))
            item_lines.extend([
                "**目标数量：** {} 股".format(item.quantity),
                "**目标金额：** ¥{}".format(_display(item.amount, 2)),
            ])
            if item.reference_price is not None:
                item_lines.append(
                    "**单价：** ¥{}".format(
                        _display(item.reference_price, 4)
                    )
                )
            elements.extend([_markdown_div(item_lines), {"tag": "hr"}])
        elements.append(
            _markdown_div([
                "**计划买入总金额：** ¥{}".format(_display(total_amount, 2)),
                "**说明：** 策略目标计划，不代表已提交委托或已经成交。",
            ])
        )
        payload: Dict[str, Any] = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "orange",
                    "title": {
                        "tag": "plain_text",
                        "content": "策略目标买入计划 · {} · {}".format(
                            notification.strategy_id,
                            notification.mode,
                        ),
                    },
                },
                "elements": elements,
            },
        }
        if self.secret:
            timestamp = int(time.time())
            payload["timestamp"] = str(timestamp)
            payload["sign"] = _signature(timestamp, self.secret)
        return payload

    def send(
        self,
        notification: Union[TradeNotification, TargetBuyPlanNotification],
    ) -> bool:
        try:
            response = requests.post(
                self.webhook_url,
                json=self.build_payload(notification),
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            result = response.json()
            return int(result.get("code", result.get("StatusCode", -1))) == 0
        except Exception:
            return False


class FeishuNotifier:
    """Drop-in replacement for the legacy bt_quant notifier.

    Existing ``FeishuNotifier().queue_message(text)`` calls remain valid. New
    code can pass a :class:`TradeNotification` to ``queue_message`` or call
    ``send_trade`` directly.
    """

    WEBHOOK_URL = ""
    SECRET = ""
    MAX_MSG_LENGTH = 10_000
    MAX_MSG_PER_MINUTE = 10
    FLUSH_INTERVAL = 30

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        secret: Optional[str] = None,
    ) -> None:
        self.webhook_url = (
            webhook_url
            or os.environ.get("FEISHU_WEBHOOK_URL", "")
            or self.WEBHOOK_URL
        )
        self.secret = (
            secret
            if secret is not None
            else os.environ.get("FEISHU_SIGNING_SECRET", self.SECRET)
        )
        self._sender = (
            FeishuTradeNotifier(self.webhook_url, self.secret)
            if self.webhook_url
            else None
        )
        self._message_queue: Queue = Queue()
        self._last_flush_minute: Optional[str] = None
        self._sent_this_minute = 0
        self._lock = threading.Lock()
        self._start_auto_flush()

    def _start_auto_flush(self) -> None:
        def loop() -> None:
            while True:
                time.sleep(self.FLUSH_INTERVAL)
                self.flush()

        threading.Thread(target=loop, daemon=True).start()

    def _builder(self) -> FeishuTradeNotifier:
        return self._sender or FeishuTradeNotifier(
            "https://example.invalid/disabled", self.secret
        )

    def _text_notification(
        self, content: str, title: str = "量化日志通知"
    ) -> TradeNotification:
        return TradeNotification(
            event="MESSAGE",
            security="-",
            side="-",
            status="INFO",
            detail=content,
            title=title,
        )

    def _build_payload(
        self,
        content: Union[str, TradeNotification, TargetBuyPlanNotification],
        msg_type: str = "interactive",
    ) -> Dict[str, Any]:
        del msg_type
        notification = (
            content
            if isinstance(content, (TradeNotification, TargetBuyPlanNotification))
            else self._text_notification(content)
        )
        return self._builder().build_payload(notification)

    def _send_request(self, data: Dict[str, Any]) -> bool:
        if not self.webhook_url:
            return False
        try:
            response = requests.post(
                self.webhook_url,
                json=data,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            return int(result.get("code", result.get("StatusCode", -1))) == 0
        except Exception:
            return False

    def send_trade(
        self,
        notification: Union[TradeNotification, TargetBuyPlanNotification],
    ) -> bool:
        return bool(self._sender and self._sender.send(notification))

    def send_text(
        self, content: str, mentioned_list: Optional[list] = None
    ) -> bool:
        if mentioned_list:
            mentions = " ".join(
                "<at user_id='{}'></at>".format(user_id)
                for user_id in mentioned_list
            )
            content = "{}\n{}".format(content, mentions)
        return self._send_request(self._build_payload(content))

    def send_rich_text(self, title: str, content: str) -> bool:
        notification = self._text_notification(content, title=title)
        return self._send_request(self._build_payload(notification))

    def queue_message(
        self,
        content: Union[str, TradeNotification, TargetBuyPlanNotification],
    ) -> None:
        self._message_queue.put(content)

    def _split_message(self, content: str) -> list:
        return [
            content[index:index + self.MAX_MSG_LENGTH]
            for index in range(0, len(content), self.MAX_MSG_LENGTH)
        ] or [""]

    def flush(self) -> None:
        with self._lock:
            minute = datetime.now().strftime("%Y-%m-%d %H:%M")
            if minute != self._last_flush_minute:
                self._last_flush_minute = minute
                self._sent_this_minute = 0
            while (
                not self._message_queue.empty()
                and self._sent_this_minute < self.MAX_MSG_PER_MINUTE
            ):
                item = self._message_queue.get()
                if isinstance(item, (TradeNotification, TargetBuyPlanNotification)):
                    self.send_trade(item)
                    self._sent_this_minute += 1
                    continue
                for segment in self._split_message(str(item).strip()):
                    if self._sent_this_minute >= self.MAX_MSG_PER_MINUTE:
                        break
                    self.send_text(segment)
                    self._sent_this_minute += 1


def money_units_to_display(units: int) -> Decimal:
    return Decimal(units) / Decimal(MONEY_SCALE)


def price_units_to_display(units: int) -> Decimal:
    return Decimal(units) / Decimal(PRICE_SCALE)
