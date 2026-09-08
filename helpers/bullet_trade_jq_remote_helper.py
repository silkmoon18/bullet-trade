# -*- coding: utf-8 -*-
"""BulletTrade 聚宽策略运行 helper（L00 精简版）。

上传到聚宽研究根目录后，策略通过版本化入口安装运行模式：

    import bullet_trade_jq_remote_helper as bt

    state = bt.install_strategy_runtime(
        globals(),
        context=context,
        mode="BACKTEST",
        strategy_id=STRATEGY_ID,
        validate_remote=True,
    )

职责边界：

- 版本校验：helper marker、运行时 API 版本和 profile schema 版本固定校验，
  不匹配即失败关闭。
- 账户开关：BACKTEST保留聚宽历史回测；模拟交易由jq_account_enabled和
  qmt_account_enabled独立控制。仅QMT账户启用时阻断聚宽原生交易函数；
  两个账户同时启用时分别读取、分别执行，互不复用资金和持仓。
- 冷启动升级：helper/config/策略文件变更必须先停止聚宽策略、确认旧进程
  退出，再由平台启动全新进程。同一进程内重复安装仅在签名完全一致时幂等
  返回；签名漂移或检测到上一代 helper 遗留记录即失败关闭。
- 历史回放：聚宽暂停恢复时仍允许JQ账户重建，但收盘后或跨日回放不得
  向QMT提交调仓、风控目标或计划通知。
- 本 helper 运行在用户自有、可信的策略进程中，不防御同进程恶意 Python
  代码、monkey patch 或热重载攻击（docs/live-ledger/02-decisions.md D021）。
"""

import json
import math
import socket
import ssl
import struct
import time
import uuid
from collections import namedtuple
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple

__all__ = [
    "STRATEGY_RUNTIME_API_VERSION",
    "STRATEGY_RUNTIME_HELPER_MARKER",
    "PROFILE_SCHEMA_VERSION",
    "HONG_KONG_ETF_KEYWORDS",
    "HONG_KONG_ETF_CODE_DENYLIST",
    "is_hong_kong_etf",
    "ExecutionType",
    "RuntimeMode",
    "FollowUpPolicy",
    "RepricingPolicy",
    "ConditionalLimitPriceMode",
    "LimitExecution",
    "ConditionalLimitExecution",
    "MarketExecution",
    "MarketableLimitExecution",
    "ExecutionRequest",
    "default_etf_rebalance_execution",
    "default_etf_stop_loss_execution",
    "default_etf_take_profit_execution",
    "PortfolioView",
    "PositionView",
    "AccountPortfolioView",
    "JoinQuantRuntime",
    "install_joinquant_runtime",
    "install_strategy_runtime",
    "ensure_account",
    "get_portfolio",
    "submit_targets",
    "notify_target_buy_plan",
    "get_intent",
    "get_reconciliation",
    "get_configured_execution_mode",
    "get_configured_account_switches",
    "runtime_portfolio",
    "ensure_runtime_ready",
    "submit_runtime_targets",
    "advance_runtime_targets",
    "cancel_runtime_targets",
    "cancel_runtime_orders",
    "runtime_order_target",
    "runtime_order_target_value",
]

STRATEGY_RUNTIME_API_VERSION = 19
STRATEGY_RUNTIME_HELPER_MARKER = "bullet-trade-joinquant-runtime-helper-v19"
PROFILE_SCHEMA_VERSION = 3
EXECUTION_WIRE_SCHEMA_VERSION = 2
HONG_KONG_ETF_KEYWORDS = (
    "港股",
    "恒生",
    "H股",
    "香港",
    "港股通",
    "沪港深",
    "沪深港",
    "HK",
    "恒科",
    "港红利",
)
# 场内简称/跟踪指数缺失时的已核实兜底；不能按513等前缀排除所有海外ETF。
HONG_KONG_ETF_CODE_DENYLIST = frozenset({
    "520590.XSHG", "520890.XSHG", "513320.XSHG", "159322.XSHE",
})

DEFAULT_RPC_TIMEOUT_SECONDS = 60.0
_RPC_ATTEMPTS = 3
_RPC_RETRY_INTERVAL_SECONDS = 0.5

# 策略 namespace 中的安装记录键；用于识别上一代 helper 遗留状态。
_RUNTIME_STATE_KEY = "__bt_strategy_runtime_state__"

_RUNTIME_MUTATION_NAMES = frozenset(
    {
        "order",
        "order_value",
        "order_percent",
        "order_target",
        "order_target_value",
        "order_target_percent",
        "cancel_order",
    }
)

_PROFILE_REQUIRED_FIELDS = frozenset({"host", "token"})
_PROFILE_OPTIONAL_FIELDS = frozenset(
    {
        "port",
        "account_key",
        "tls_cert",
        "rpc_timeout",
    }
)
_PROFILE_ALLOWED_FIELDS = _PROFILE_REQUIRED_FIELDS | _PROFILE_OPTIONAL_FIELDS
_STRATEGY_ALLOWED_FIELDS = frozenset(
    {"profile", "jq_account_enabled", "qmt_account_enabled"}
)

# 本代 helper 实例标记；namespace 记录中的 token 不同即为上一代遗留。
_MODULE_TOKEN = object()

# 模块级安装记录：同一进程只允许一种安装签名。
_active_signature = None  # type: Optional[Tuple[Any, ...]]
_active_state = None  # type: Optional[Dict[str, Any]]
_active_profile = None  # type: Optional[Dict[str, Any]]
_active_namespace = None  # type: Optional[Dict[str, Any]]
_runtime_target_state = None  # type: Optional[Dict[str, Any]]
_security_name_cache = {}  # type: Dict[str, str]
_TERMINAL_INTENT_STATES = frozenset({"COMPLETED", "CANCELED", "FAILED"})
_QMT_CALLBACK_WINDOW_START_SECONDS = 9 * 60 * 60 + 15 * 60
_QMT_CALLBACK_WINDOW_END_SECONDS = 15 * 60 * 60
_QmtCallbackGate = namedtuple(
    "_QmtCallbackGate", "allowed strategy_time actual_time reason"
)


def _local_wall_clock() -> Any:
    """Return the JoinQuant worker's local wall clock (Asia/Shanghai in production)."""

    return time.localtime()


def _strategy_clock_parts(value: Any) -> Optional[Tuple[int, ...]]:
    """Normalize JoinQuant datetime-like values without adding runtime dependencies."""

    if isinstance(value, str):
        try:
            parsed = time.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
            return tuple(parsed[:6])
        except (TypeError, ValueError):
            return None
    try:
        return (
            int(value.year),
            int(value.month),
            int(value.day),
            int(value.hour),
            int(value.minute),
            int(value.second),
        )
    except (AttributeError, TypeError, ValueError):
        return None


def _qmt_callback_gate(context: Any) -> Any:
    """Reject QMT side effects produced by JoinQuant's historical catch-up replay."""

    strategy = _strategy_clock_parts(getattr(context, "current_dt", None))
    actual = _local_wall_clock()
    actual_parts = tuple(actual[:6])
    actual_text = time.strftime("%Y-%m-%d %H:%M:%S", actual)
    if strategy is None:
        return _QmtCallbackGate(
            False, "<invalid>", actual_text, "策略时间不可识别"
        )
    strategy_text = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
        *strategy
    )
    if strategy[:3] != actual_parts[:3]:
        return _QmtCallbackGate(
            False, strategy_text, actual_text, "策略日期与实际日期不一致"
        )
    actual_seconds = (
        actual_parts[3] * 60 * 60
        + actual_parts[4] * 60
        + actual_parts[5]
    )
    if not (
        _QMT_CALLBACK_WINDOW_START_SECONDS
        <= actual_seconds
        < _QMT_CALLBACK_WINDOW_END_SECONDS
    ):
        return _QmtCallbackGate(
            False,
            strategy_text,
            actual_text,
            "实际时间不在QMT当日执行窗口09:15-15:00",
        )
    return _QmtCallbackGate(True, strategy_text, actual_text, "")


def is_hong_kong_etf(
    security: str,
    display_name: Any = "",
    traced_index_name: Any = "",
    keywords: Optional[Tuple[str, ...]] = None,
    code_denylist: Optional[Any] = None,
) -> bool:
    """Return whether an ETF belongs to the configured Hong Kong universe."""

    active_keywords = (
        HONG_KONG_ETF_KEYWORDS if keywords is None else keywords
    )
    active_denylist = (
        HONG_KONG_ETF_CODE_DENYLIST
        if code_denylist is None
        else code_denylist
    )
    if security in active_denylist:
        return True
    text_parts = [
        value
        for value in (display_name, traced_index_name)
        if isinstance(value, str)
    ]
    haystack = " ".join(text_parts).lower()
    return any([
        keyword.lower() in haystack
        for keyword in active_keywords
        if isinstance(keyword, str) and keyword
    ])


def _security_name(security: str, known_name: Any = "") -> str:
    if isinstance(known_name, str) and known_name.strip():
        normalized = known_name.strip()
        _security_name_cache[security] = normalized
        return normalized
    cached = _security_name_cache.get(security)
    if cached is not None:
        return cached
    name = ""
    if _active_namespace is not None:
        getter = _active_namespace.get("get_security_info")
        if callable(getter):
            try:
                info = getter(security)
                name = (
                    getattr(info, "display_name", None)
                    or getattr(info, "name", None)
                    or ""
                )
            except Exception:
                name = ""
    name = str(name).strip() if name else ""
    _security_name_cache[security] = name
    return name


def _security_label(security: str, known_name: Any = "") -> str:
    name = _security_name(security, known_name)
    return "{}({})".format(security, name) if name else security


def _security_names(securities: Any) -> Dict[str, str]:
    result = {}
    for security in securities:
        name = _security_name(str(security))
        if name:
            result[str(security)] = name
    return result


class ExecutionType(str, Enum):
    LIMIT = "LIMIT"
    CONDITIONAL_LIMIT = "CONDITIONAL_LIMIT"
    MARKET = "MARKET"
    MARKETABLE_LIMIT = "MARKETABLE_LIMIT"


class RuntimeMode(str, Enum):
    BACKTEST = "BACKTEST"
    JQ = "JQ"
    QMT_REMOTE = "QMT_REMOTE"
    JQ_QMT_PARALLEL = "JQ_QMT_PARALLEL"


class FollowUpPolicy(str, Enum):
    NONE = "NONE"
    UNTIL_FILLED_TODAY = "UNTIL_FILLED_TODAY"


class RepricingPolicy(str, Enum):
    KEEP_ORIGINAL = "KEEP_ORIGINAL"
    RECOMPUTE = "RECOMPUTE"


class ConditionalLimitPriceMode(str, Enum):
    BOUNDARY = "BOUNDARY"
    COUNTERPARTY = "COUNTERPARTY"


def _check_band(value: int, field_name: str) -> None:
    if type(value) is not int or not 0 <= value <= 100_000:
        raise ValueError("{}必须是0到100000之间的整数".format(field_name))


_LimitExecutionTuple = namedtuple(
    "LimitExecution", ("price_band_ppm", "execution_type")
)


class LimitExecution(_LimitExecutionTuple):
    __slots__ = ()

    def __new__(cls, price_band_ppm: int = 0) -> "LimitExecution":
        _check_band(price_band_ppm, "price_band_ppm")
        return _LimitExecutionTuple.__new__(
            cls, price_band_ppm, ExecutionType.LIMIT
        )


_ConditionalLimitExecutionTuple = namedtuple(
    "ConditionalLimitExecution",
    ("price_band_ppm", "price_mode", "execution_type"),
)


class ConditionalLimitExecution(_ConditionalLimitExecutionTuple):
    __slots__ = ()

    def __new__(
        cls,
        price_band_ppm: int = 2_000,
        price_mode: ConditionalLimitPriceMode = (
            ConditionalLimitPriceMode.BOUNDARY
        ),
    ) -> "ConditionalLimitExecution":
        _check_band(price_band_ppm, "price_band_ppm")
        if type(price_mode) is not ConditionalLimitPriceMode:
            raise TypeError("price_mode必须是ConditionalLimitPriceMode")
        return _ConditionalLimitExecutionTuple.__new__(
            cls,
            price_band_ppm,
            price_mode,
            ExecutionType.CONDITIONAL_LIMIT,
        )


_MarketExecutionTuple = namedtuple(
    "MarketExecution", ("protect_price_band_ppm", "execution_type")
)


class MarketExecution(_MarketExecutionTuple):
    __slots__ = ()

    def __new__(
        cls, protect_price_band_ppm: int = 15_000
    ) -> "MarketExecution":
        _check_band(protect_price_band_ppm, "protect_price_band_ppm")
        return _MarketExecutionTuple.__new__(
            cls, protect_price_band_ppm, ExecutionType.MARKET
        )


_MarketableLimitExecutionTuple = namedtuple(
    "MarketableLimitExecution", ("price_band_ppm", "execution_type")
)


class MarketableLimitExecution(_MarketableLimitExecutionTuple):
    __slots__ = ()

    def __new__(
        cls, price_band_ppm: int = 15_000
    ) -> "MarketableLimitExecution":
        _check_band(price_band_ppm, "price_band_ppm")
        return _MarketableLimitExecutionTuple.__new__(
            cls, price_band_ppm, ExecutionType.MARKETABLE_LIMIT
        )


_DEFAULT_EXECUTION_STYLE = object()


_ExecutionRequestTuple = namedtuple(
    "ExecutionRequest", ("style", "follow_up", "repricing", "sell_style")
)


class ExecutionRequest(_ExecutionRequestTuple):
    __slots__ = ()

    def __new__(
        cls,
        style: Any = _DEFAULT_EXECUTION_STYLE,
        follow_up: FollowUpPolicy = FollowUpPolicy.UNTIL_FILLED_TODAY,
        repricing: RepricingPolicy = RepricingPolicy.KEEP_ORIGINAL,
        sell_style: Any = None,
    ) -> "ExecutionRequest":
        if style is _DEFAULT_EXECUTION_STYLE:
            style = LimitExecution(2_000)
        if not isinstance(
            style,
            (
                LimitExecution,
                ConditionalLimitExecution,
                MarketExecution,
                MarketableLimitExecution,
            ),
        ):
            raise TypeError("style不是支持的执行类型")
        if type(follow_up) is not FollowUpPolicy:
            raise TypeError("follow_up必须是FollowUpPolicy")
        if type(repricing) is not RepricingPolicy:
            raise TypeError("repricing必须是RepricingPolicy")
        if sell_style is not None and not isinstance(
            sell_style,
            (
                LimitExecution,
                ConditionalLimitExecution,
                MarketExecution,
                MarketableLimitExecution,
            ),
        ):
            raise TypeError("sell_style不是支持的执行类型或None")
        return _ExecutionRequestTuple.__new__(
            cls, style, follow_up, repricing, sell_style
        )


def default_etf_rebalance_execution() -> ExecutionRequest:
    """Market sells, then fixed limits from the original JQ last_price."""

    return ExecutionRequest(
        style=LimitExecution(2_000),
        sell_style=MarketExecution(15_000),
        follow_up=FollowUpPolicy.UNTIL_FILLED_TODAY,
        repricing=RepricingPolicy.KEEP_ORIGINAL,
    )


def default_etf_stop_loss_execution() -> ExecutionRequest:
    """Default ETF stop loss: protected market sell until filled today."""

    return ExecutionRequest(
        style=MarketExecution(15_000),
        follow_up=FollowUpPolicy.UNTIL_FILLED_TODAY,
        repricing=RepricingPolicy.KEEP_ORIGINAL,
    )


def default_etf_take_profit_execution() -> ExecutionRequest:
    """Keep the original take-profit reference price and 0.2% offset."""

    return ExecutionRequest(
        style=LimitExecution(2_000),
        follow_up=FollowUpPolicy.UNTIL_FILLED_TODAY,
        repricing=RepricingPolicy.KEEP_ORIGINAL,
    )


def _style_to_wire(style: Any) -> Dict[str, Any]:
    style_wire = {"type": style.execution_type.value}
    if isinstance(style, (LimitExecution, MarketableLimitExecution)):
        style_wire["price_band_ppm"] = style.price_band_ppm
    elif isinstance(style, ConditionalLimitExecution):
        style_wire["price_band_ppm"] = style.price_band_ppm
        style_wire["price_mode"] = style.price_mode.value
    else:
        style_wire["protect_price_band_ppm"] = style.protect_price_band_ppm
    return style_wire


def _execution_to_wire(request: ExecutionRequest) -> Dict[str, Any]:
    if type(request) is not ExecutionRequest:
        raise TypeError("execution必须是ExecutionRequest")
    return {
        "schema_version": EXECUTION_WIRE_SCHEMA_VERSION,
        "style": _style_to_wire(request.style),
        "sell_style": (
            _style_to_wire(request.sell_style)
            if request.sell_style is not None
            else None
        ),
        "follow_up": request.follow_up.value,
        "repricing": request.repricing.value,
    }


def _style_from_wire(raw_style: Any) -> Any:
    if type(raw_style) is not dict:
        raise RuntimeError("服务器执行类型无效")
    execution_type = ExecutionType(raw_style.get("type"))
    if execution_type is ExecutionType.LIMIT:
        return LimitExecution(int(raw_style["price_band_ppm"]))
    if execution_type is ExecutionType.CONDITIONAL_LIMIT:
        return ConditionalLimitExecution(
            int(raw_style["price_band_ppm"]),
            ConditionalLimitPriceMode(raw_style["price_mode"]),
        )
    if execution_type is ExecutionType.MARKET:
        return MarketExecution(int(raw_style["protect_price_band_ppm"]))
    return MarketableLimitExecution(int(raw_style["price_band_ppm"]))


def _execution_from_wire(value: Dict[str, Any]) -> ExecutionRequest:
    if type(value) is not dict or value.get("schema_version") not in (
        1,
        EXECUTION_WIRE_SCHEMA_VERSION,
    ):
        raise RuntimeError("服务器执行请求版本无效")
    try:
        raw_sell_style = (
            value.get("sell_style")
            if value.get("schema_version") == EXECUTION_WIRE_SCHEMA_VERSION
            else None
        )
        return ExecutionRequest(
            style=_style_from_wire(value.get("style")),
            follow_up=FollowUpPolicy(value["follow_up"]),
            repricing=RepricingPolicy(value["repricing"]),
            sell_style=(
                _style_from_wire(raw_sell_style)
                if raw_sell_style is not None
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("服务器执行请求内容无效") from exc


class _AmbiguousRequestError(RuntimeError):
    """请求可能已执行但响应丢失，禁止在当前调用中盲目重发。"""


class _ServerResponseError(RuntimeError):
    """服务端明确拒绝请求，结果是确定的。"""


class PositionView(object):
    """聚宽策略可直接读取的真实持仓只读视图。"""

    def __init__(self, payload: Dict[str, Any]) -> None:
        self.security = payload["security"]
        self.total_amount = int(payload["total_amount"])
        self.closeable_amount = int(payload["closeable_amount"])
        self.avg_cost = float(payload["avg_cost"])
        self.price = float(payload["price"])
        self.mark_as_of = payload.get("mark_as_of")
        self.mark_source = payload.get("mark_source")
        self.value = float(payload["value"])
        self.unrealized_pnl = float(payload.get("unrealized_pnl", 0.0))


class PortfolioView(object):
    """由真实成交账本生成，不修改聚宽原生模拟账户。"""

    def __init__(self, payload: Dict[str, Any]) -> None:
        self.account_id = payload["account_id"]
        self.as_of = payload["as_of"]
        self.snapshot_version = payload["snapshot_version"]
        self.ledger_version = int(payload["ledger_version"])
        self.cash = float(payload["cash"])
        self.reserved_cash = float(payload["reserved_cash"])
        self.available_cash = float(payload["available_cash"])
        self.positions_value = float(payload["positions_value"])
        self.total_value = float(payload["total_value"])
        self.starting_cash = float(payload["starting_cash"])
        self.total_pnl = _optional_float(payload.get("total_pnl"))
        self.realized_pnl = _optional_float(payload.get("realized_pnl"))
        self.unrealized_pnl = _optional_float(payload.get("unrealized_pnl"))
        self.fees = _optional_float(payload.get("fees"))
        self.fees_known = bool(payload.get("fees_known", self.fees is not None))
        self.unknown_fee_fill_count = int(payload.get("unknown_fee_fill_count", 0))
        self.unknown_price_fill_count = int(
            payload.get("unknown_price_fill_count", 0)
        )
        self.nav = _optional_float(payload.get("nav"))
        self.returns = _optional_float(payload.get("returns"))
        self.performance_blockers = tuple(payload.get("performance_blockers", ()))
        self.performance_ready = bool(payload["performance_ready"])
        if self.fees_known and self.fees is None:
            raise RuntimeError("费用标记为已知但缺少费用数值")
        if self.performance_ready and any(
            value is None
            for value in (
                self.total_pnl,
                self.realized_pnl,
                self.unrealized_pnl,
                self.nav,
                self.returns,
            )
        ):
            raise RuntimeError("绩效标记为可用但缺少绩效数值")
        self.positions = {
            security: PositionView(item)
            for security, item in payload.get("positions", {}).items()
        }


AccountPortfolioView = namedtuple(
    "AccountPortfolioView", ("account", "portfolio")
)


def _optional_float(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError("unsupported JSON value: {}".format(type(value).__name__))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("服务器连接提前关闭")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_message(sock: socket.socket, message: Dict[str, Any]) -> None:
    body = json.dumps(
        message, ensure_ascii=False, default=_json_default
    ).encode("utf-8")
    sock.sendall(struct.pack(">I", len(body)) + body)


def _read_message(sock: socket.socket) -> Dict[str, Any]:
    size = struct.unpack(">I", _recv_exact(sock, 4))[0]
    if size > 32 * 1024 * 1024:
        raise RuntimeError("服务器响应过大")
    result = json.loads(_recv_exact(sock, size).decode("utf-8"))
    if not isinstance(result, dict):
        raise RuntimeError("服务器响应格式无效")
    return result


def _strategy_request(action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _active_profile is None or _active_state is None:
        raise RuntimeError("策略运行时尚未安装")
    profile = _active_profile
    timeout = float(profile["rpc_timeout"])
    connect_timeout = min(timeout, 10.0)
    safe_retry = action not in (
        "strategy.submit_targets",
        "strategy.cancel_intent",
        "strategy.notify_target_buy_plan",
    )
    last_error = None  # type: Optional[Exception]

    for attempt in range(1, _RPC_ATTEMPTS + 1):
        raw_sock = None
        sock = None
        phase = "连接"
        request_may_have_been_sent = False
        try:
            raw_sock = socket.create_connection(
                (profile["host"], profile["port"]), timeout=connect_timeout
            )
            sock = raw_sock
            tls_cert = profile.get("tls_cert")
            if tls_cert:
                phase = "TLS握手"
                context = ssl.create_default_context(cafile=tls_cert)
                sock = context.wrap_socket(
                    raw_sock, server_hostname=profile["host"]
                )
            sock.settimeout(timeout)

            phase = "应用握手"
            _send_message(
                sock,
                {
                    "type": "handshake",
                    "token": profile["token"],
                    "protocol": 1,
                    "features": ["strategy_ledger_v1"],
                    "account_key": profile.get("account_key"),
                },
            )
            handshake = _read_message(sock)
            if handshake.get("type") != "handshake_ack":
                raise RuntimeError("服务器握手失败")

            request_id = uuid.uuid4().hex
            request_payload = dict(payload)
            request_payload["strategy_id"] = _active_state["strategy_id"]
            if profile.get("account_key"):
                request_payload["account_key"] = profile["account_key"]

            phase = "发送请求"
            # sendall 失败时也可能已经发送了部分数据；有副作用的请求必须按未知结果处理。
            request_may_have_been_sent = True
            _send_message(
                sock,
                {
                    "type": "request",
                    "id": request_id,
                    "action": action,
                    "payload": request_payload,
                },
            )
            phase = "接收响应"
            response = _read_message(sock)
            if response.get("type") == "error":
                raise _ServerResponseError(
                    "{}: {}".format(
                        response.get("code", "REQUEST_FAILED"),
                        response.get("message", "server error"),
                    )
                )
            if (
                response.get("type") != "response"
                or response.get("id") != request_id
            ):
                raise RuntimeError("服务器响应与请求不匹配")
            result = response.get("payload")
            if not isinstance(result, dict):
                raise RuntimeError("服务器响应payload无效")
            return result
        except _ServerResponseError as exc:
            raise RuntimeError(str(exc)) from None
        except Exception as exc:
            if not safe_retry and request_may_have_been_sent:
                raise _AmbiguousRequestError(
                    "{}在{}阶段失败：请求可能已执行，已停止自动重发；"
                    "请使用原idempotency_key查询意图和对账结果".format(
                        action, phase
                    )
                ) from None
            last_error = exc
            if attempt < _RPC_ATTEMPTS:
                print(
                    "[BulletTrade RPC] {}阶段失败（第{}/{}次），0.5秒后重试: {}".format(
                        phase, attempt, _RPC_ATTEMPTS, type(exc).__name__
                    )
                )
                time.sleep(_RPC_RETRY_INTERVAL_SECONDS)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    raise RuntimeError(
        "{}请求失败：{}阶段连续尝试{}次仍未成功（{}）".format(
            action, phase, _RPC_ATTEMPTS,
            type(last_error).__name__ if last_error is not None else "unknown",
        )
    ) from None


def ensure_account(initial_capital: Any = "10000") -> Dict[str, Any]:
    """校验真实账户资金并幂等建立策略账户。"""

    return _strategy_request(
        "strategy.ensure_account", {"initial_capital": initial_capital}
    )


def get_portfolio(
    marks: Optional[Dict[str, Any]] = None,
    as_of: Any = None,
) -> PortfolioView:
    payload = {}  # type: Dict[str, Any]
    if marks is not None:
        payload["marks"] = marks
    if as_of is not None:
        payload["as_of"] = as_of
    return PortfolioView(_strategy_request("strategy.get_snapshot", payload))


def submit_targets(
    weights: Dict[str, Any],
    idempotency_key: str,
    marks: Optional[Dict[str, Any]] = None,
    as_of: Any = None,
    execution: Optional[ExecutionRequest] = None,
    security_names: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    if _active_state is None or not _active_state.get("qmt_account_enabled"):
        raise RuntimeError("只有启用QMT账户后才可以提交真实组合目标")
    payload = {"weights": weights, "idempotency_key": idempotency_key}
    if execution is not None:
        payload["execution"] = _execution_to_wire(execution)
    if marks is not None:
        payload["marks"] = marks
    if as_of is not None:
        payload["as_of"] = as_of
    if security_names:
        payload["security_names"] = security_names
    return _strategy_request("strategy.submit_targets", payload)


def notify_target_buy_plan(
    items: Any,
    occurred_at: Any = None,
) -> Dict[str, Any]:
    """发送策略目标买入计划；只通知，不提交订单或修改账本。"""

    if _active_state is None or _active_state.get("mode") == "BACKTEST":
        raise RuntimeError("只有聚宽模拟交易可以发送策略目标计划")
    payload = {
        # 服务器通知协议只区分通知归属：QMT启用时只发送QMT计划，
        # 否则维持原有JQ计划通知；并行运行不重复发送两份卡片。
        "mode": (
            "QMT_REMOTE"
            if _active_state.get("qmt_account_enabled")
            else "JQ"
        ),
        "items": items,
    }
    if occurred_at is not None:
        payload["occurred_at"] = occurred_at
    return _strategy_request("strategy.notify_target_buy_plan", payload)


def get_intent(
    intent_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {}  # type: Dict[str, Any]
    if intent_id:
        payload["intent_id"] = intent_id
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    return _strategy_request("strategy.get_intent", payload)


def get_reconciliation() -> Dict[str, Any]:
    return _strategy_request("strategy.get_reconciliation", {})


def _record_runtime_portfolio(portfolio: PortfolioView) -> None:
    if _active_namespace is None:
        return
    recorder = _active_namespace.get("record")
    if not callable(recorder):
        return
    metrics = {
        "real_cash": portfolio.available_cash,
        "real_total": portfolio.total_value,
        "real_positions": portfolio.positions_value,
    }
    if portfolio.nav is not None:
        metrics["real_nav"] = portfolio.nav
    if portfolio.returns is not None:
        metrics["real_return"] = portfolio.returns
    if portfolio.fees is not None:
        metrics["real_fees"] = portfolio.fees
    recorder(**metrics)


def runtime_portfolio(context: Any) -> Any:
    """Return the primary account view for backwards-compatible callers."""

    if _active_state is None:
        raise RuntimeError("策略运行时尚未安装")
    if not _active_state.get("qmt_account_enabled"):
        return context.portfolio
    portfolio = get_portfolio(as_of=getattr(context, "current_dt", None))
    _record_runtime_portfolio(portfolio)
    return portfolio


def _restore_runtime_targets() -> None:
    global _runtime_target_state
    if _active_state is None or not _active_state.get("qmt_account_enabled"):
        _runtime_target_state = None
        return
    intent = get_intent()
    if not intent or intent.get("state") in _TERMINAL_INTENT_STATES:
        _runtime_target_state = None
        return
    raw_execution = intent.get("execution")
    execution = (
        _execution_from_wire(raw_execution)
        if type(raw_execution) is dict
        else ExecutionRequest()
    )
    _runtime_target_state = {
        "intent_id": intent["intent_id"],
        "idempotency_key": intent["idempotency_key"],
        "weights": dict(intent.get("weights", {})),
        "marks": {},
        "security_names": _security_names(intent.get("weights", {})),
        "execution": execution,
    }


def ensure_runtime_ready(initial_capital: Any, context: Any) -> Any:
    """Validate the remote account and restore the current daily intent."""

    if _active_state is None or not _active_state.get("qmt_account_enabled"):
        return runtime_portfolio(context)
    # READY is evidence from the latest successful broker reconciliation, not
    # a process-lifetime latch.  Clear it before every refresh so a later QMT
    # failure cannot leave a stale True value in the published runtime state.
    _active_state["production_ready"] = False
    ensured = ensure_account(initial_capital)
    reconciliation = ensured.get("reconciliation", {})
    if reconciliation.get("state") != "READY":
        raise RuntimeError(
            "真实账户对账未就绪: {}".format(
                reconciliation.get("details", {}).get("blockers", [])
            )
        )
    portfolio = runtime_portfolio(context)
    _restore_runtime_targets()
    _active_state["production_ready"] = True
    return portfolio


def submit_runtime_targets(
    context: Any,
    weights: Dict[str, Any],
    marks: Dict[str, Any],
    idempotency_key: str,
    execution: ExecutionRequest,
    security_names: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Submit one typed daily target and retain only restart state."""

    global _runtime_target_state
    existing = get_intent(idempotency_key=idempotency_key)
    if existing:
        weights = dict(existing.get("weights", weights))
        if type(existing.get("execution")) is dict:
            execution = _execution_from_wire(existing["execution"])
    result = submit_targets(
        weights,
        idempotency_key,
        marks=marks,
        as_of=getattr(context, "current_dt", None),
        execution=execution,
        security_names=security_names,
    )
    _runtime_target_state = {
        "intent_id": result["intent"]["intent_id"],
        "idempotency_key": idempotency_key,
        "weights": dict(weights),
        "marks": dict(marks),
        "security_names": dict(security_names or {}),
        "execution": execution,
    }
    portfolio = PortfolioView(result["snapshot"])
    _record_runtime_portfolio(portfolio)
    return result


def advance_runtime_targets(context: Any) -> bool:
    """Advance the restored daily intent; return False while it is active."""

    global _runtime_target_state
    if _active_state is None or not _active_state.get("qmt_account_enabled"):
        return True
    if _runtime_target_state is None:
        _restore_runtime_targets()
    if _runtime_target_state is None:
        return True
    intent = get_intent(_runtime_target_state["intent_id"])
    if intent.get("state") in _TERMINAL_INTENT_STATES:
        _runtime_target_state = None
        return True
    result = submit_runtime_targets(
        context,
        _runtime_target_state["weights"],
        _runtime_target_state["marks"],
        _runtime_target_state["idempotency_key"],
        _runtime_target_state["execution"],
        _runtime_target_state.get("security_names"),
    )
    if result["intent"]["state"] in _TERMINAL_INTENT_STATES:
        _runtime_target_state = None
        return True
    return False


def cancel_runtime_targets() -> bool:
    """Cancel the active remote target; return True after it is terminal."""

    global _runtime_target_state
    if _active_state is None or not _active_state.get("qmt_account_enabled"):
        return True
    if _runtime_target_state is None:
        _restore_runtime_targets()
    if _runtime_target_state is None:
        return True
    result = _strategy_request(
        "strategy.cancel_intent",
        {"intent_id": _runtime_target_state["intent_id"]},
    )
    if result.get("canceled"):
        _runtime_target_state = None
        return True
    return False


def cancel_runtime_orders() -> int:
    if _active_state is None:
        raise RuntimeError("策略运行时尚未安装")
    if not _active_state.get("jq_account_enabled"):
        return 0
    if _active_namespace is None:
        raise RuntimeError("策略namespace不可用")
    getter = _active_namespace.get("get_open_orders")
    cancel = _active_namespace.get("cancel_order")
    if not callable(getter) or not callable(cancel):
        raise RuntimeError("聚宽订单函数不可用")
    orders = getter() or {}
    for order_obj in list(orders.values()):
        cancel(order_obj)
    return len(orders)


def runtime_order_target(security: str, amount: int) -> Any:
    if _active_namespace is None:
        raise RuntimeError("策略namespace不可用")
    order_target_fn = _active_namespace.get("order_target")
    if not callable(order_target_fn):
        raise RuntimeError("聚宽order_target不可用")
    return order_target_fn(security, amount)


def runtime_order_target_value(
    security: str, target_value: float, limit_price: Optional[float] = None
) -> Any:
    if _active_namespace is None:
        raise RuntimeError("策略namespace不可用")
    order_target_value_fn = _active_namespace.get("order_target_value")
    if not callable(order_target_value_fn):
        raise RuntimeError("聚宽order_target_value不可用")
    style = None
    if limit_price is not None:
        style_type = _active_namespace.get("LimitOrderStyle")
        if not callable(style_type):
            raise RuntimeError("聚宽LimitOrderStyle不可用")
        style = style_type(limit_price)
    return order_target_value_fn(security, target_value, style=style)


def _run_type_from_context(context: Any) -> Optional[str]:
    run_params = getattr(context, "run_params", None)
    if isinstance(run_params, dict):
        return run_params.get("type")
    return getattr(run_params, "type", None)


def _normalise_runtime_mode(mode: Any) -> str:
    if type(mode) is not str:
        raise RuntimeError(
            "运行模式必须是普通字符串BACKTEST、JQ、QMT_REMOTE或JQ_QMT_PARALLEL"
        )
    value = str.upper(str.strip(mode))
    if value not in ("BACKTEST", "JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL"):
        raise RuntimeError(
            "运行模式必须是BACKTEST、JQ、QMT_REMOTE或JQ_QMT_PARALLEL"
        )
    return value


def _mode_from_account_switches(
    jq_account_enabled: bool,
    qmt_account_enabled: bool,
) -> str:
    if type(jq_account_enabled) is not bool:
        raise RuntimeError("STRATEGIES.jq_account_enabled必须是bool")
    if type(qmt_account_enabled) is not bool:
        raise RuntimeError("STRATEGIES.qmt_account_enabled必须是bool")
    if not jq_account_enabled and not qmt_account_enabled:
        raise RuntimeError("至少启用一个账户")
    if jq_account_enabled and qmt_account_enabled:
        return RuntimeMode.JQ_QMT_PARALLEL.value
    if qmt_account_enabled:
        return RuntimeMode.QMT_REMOTE.value
    return RuntimeMode.JQ.value


def _account_switches_from_mode(mode: str) -> Dict[str, bool]:
    if mode == RuntimeMode.BACKTEST.value:
        return {"jq_account_enabled": True, "qmt_account_enabled": False}
    if mode == RuntimeMode.JQ.value:
        return {"jq_account_enabled": True, "qmt_account_enabled": False}
    if mode == RuntimeMode.QMT_REMOTE.value:
        return {"jq_account_enabled": False, "qmt_account_enabled": True}
    if mode == RuntimeMode.JQ_QMT_PARALLEL.value:
        return {"jq_account_enabled": True, "qmt_account_enabled": True}
    raise RuntimeError("未知运行模式")


def _validate_runtime_identifier(value: Any, field: str) -> str:
    if type(value) is not str:
        raise RuntimeError("{} 必须是非空字符串".format(field))
    if not value or value != str.strip(value) or len(value) > 128:
        raise RuntimeError("{} 必须是非空、无首尾空白且不超过128字符的字符串".format(field))
    if not all(str.isalnum(char) or char in "._-" for char in value):
        raise RuntimeError("{} 只能包含字母、数字、点、下划线和连字符".format(field))
    return value


def _validate_profile_module_name(value: Any) -> str:
    if type(value) is not str:
        raise RuntimeError("profile_module 必须是Python模块名")
    if not value or value != str.strip(value):
        raise RuntimeError("profile_module 必须是Python模块名")
    if not all(str.isidentifier(part) for part in str.split(value, ".")):
        raise RuntimeError("profile_module 必须是合法的Python模块名")
    return value


def _load_runtime_configuration(
    profile_module: str,
    strategy_id: str,
) -> Dict[str, Any]:
    """Resolve one strategy to its account switches and connection profile."""

    strategy_id = _validate_runtime_identifier(strategy_id, "strategy_id")
    profile_module = _validate_profile_module_name(profile_module)
    load_failed = False
    try:
        module = __import__(profile_module, fromlist=["*"])
        schema_version = getattr(module, "PROFILE_SCHEMA_VERSION", None)
        default_profile = getattr(module, "DEFAULT_PROFILE", None)
        strategies = getattr(module, "STRATEGIES", None)
        profiles = getattr(module, "PROFILES", None)
    except BaseException:
        load_failed = True
    if load_failed:
        raise RuntimeError(
            "无法加载运行配置模块 {}；请确认文件已上传且配置可读取".format(
                profile_module
            )
        ) from None

    if type(schema_version) is not int or schema_version != PROFILE_SCHEMA_VERSION:
        raise RuntimeError(
            "运行配置schema版本不匹配: expected={}".format(
                PROFILE_SCHEMA_VERSION
            )
        )
    default_profile = _validate_runtime_identifier(
        default_profile, "DEFAULT_PROFILE"
    )
    if type(strategies) is not dict:
        raise RuntimeError("运行配置模块必须定义字典 STRATEGIES")
    if type(profiles) is not dict:
        raise RuntimeError("运行配置模块必须定义字典 PROFILES")
    if any(type(name) is not str for name in profiles):
        raise RuntimeError("运行配置 PROFILES 的profile名称必须是普通字符串")
    if default_profile not in profiles:
        raise RuntimeError("DEFAULT_PROFILE在PROFILES中不存在")

    for configured_id, settings in strategies.items():
        _validate_runtime_identifier(configured_id, "STRATEGIES策略ID")
        if type(settings) is not dict:
            raise RuntimeError("STRATEGIES中的策略配置必须是字典")
        if any(type(key) is not str for key in settings):
            raise RuntimeError("STRATEGIES中的字段名必须是普通字符串")
        if any(key not in _STRATEGY_ALLOWED_FIELDS for key in settings):
            raise RuntimeError("STRATEGIES包含未知字段；字段名不予回显")
        configured_profile = settings.get("profile", default_profile)
        configured_profile = _validate_runtime_identifier(
            configured_profile, "STRATEGIES.profile"
        )
        if configured_profile not in profiles:
            raise RuntimeError("STRATEGIES引用的profile在PROFILES中不存在")
        _mode_from_account_switches(
            settings.get("jq_account_enabled", True),
            settings.get("qmt_account_enabled", False),
        )

    selected = strategies.get(strategy_id, {})
    profile = selected.get("profile", default_profile)
    jq_account_enabled = selected.get("jq_account_enabled", True)
    qmt_account_enabled = selected.get("qmt_account_enabled", False)
    mode = _mode_from_account_switches(
        jq_account_enabled, qmt_account_enabled
    )
    raw = profiles[profile]
    if type(raw) is not dict:
        raise RuntimeError("profile {} 必须是字典".format(profile))
    if any(type(key) is not str for key in raw):
        raise RuntimeError("profile {} 包含非普通字符串字段".format(profile))
    if any(key not in _PROFILE_ALLOWED_FIELDS for key in raw):
        raise RuntimeError("profile {} 包含未知字段；字段名不予回显".format(profile))
    missing = sorted(_PROFILE_REQUIRED_FIELDS - set(raw))
    if missing:
        raise RuntimeError(
            "profile {} 缺少必填字段: {}".format(profile, ", ".join(missing))
        )

    host = raw.get("host")
    if (
        type(host) is not str
        or not host
        or host != str.strip(host)
        or any(str.isspace(ch) for ch in host)
    ):
        raise RuntimeError("profile.host 必须是非空且不含空白的字符串")
    if len(host) > 255:
        raise RuntimeError("profile.host 长度不能超过255字符")

    token = raw.get("token")
    if type(token) is not str or not token or token != str.strip(token):
        raise RuntimeError("profile.token 必须是非空且无首尾空白的字符串")

    port = raw.get("port", 58620)
    if type(port) is not int or not 1 <= port <= 65535:
        raise RuntimeError("profile.port 必须是1到65535之间的整数")

    numeric_rules = {
        "rpc_timeout": (DEFAULT_RPC_TIMEOUT_SECONDS, 5.0, 300.0),
    }
    numeric_values = {}  # type: Dict[str, float]
    for field, (default, minimum, maximum) in numeric_rules.items():
        value = raw.get(field, default)
        if type(value) not in (int, float):
            raise RuntimeError("profile.{} 必须是有限数值".format(field))
        if type(value) is float and not math.isfinite(value):
            raise RuntimeError("profile.{} 必须是有限数值".format(field))
        if value < minimum or value > maximum:
            raise RuntimeError(
                "profile.{} 必须在{}到{}之间".format(field, minimum, maximum)
            )
        numeric_values[field] = float(value)

    optional_strings = {}  # type: Dict[str, Optional[str]]
    for field in ("account_key", "tls_cert"):
        value = raw.get(field)
        if value is not None and (
            type(value) is not str or not value or value != str.strip(value)
        ):
            raise RuntimeError(
                "profile.{} 必须是非空且无首尾空白的字符串或None".format(field)
            )
        optional_strings[field] = value

    return {
        "mode": mode,
        "jq_account_enabled": jq_account_enabled,
        "qmt_account_enabled": qmt_account_enabled,
        "profile": profile,
        "connection": {
            "host": host,
            "token": token,
            "port": port,
            "account_key": optional_strings["account_key"],
            "tls_cert": optional_strings["tls_cert"],
            "rpc_timeout": numeric_values["rpc_timeout"],
        },
    }


def get_configured_execution_mode(
    strategy_id: str,
    profile_module: str = "jq_runtime_config",
) -> str:
    """Return the internal mode derived from the two public account switches."""

    return _load_runtime_configuration(profile_module, strategy_id)["mode"]


def get_configured_account_switches(
    strategy_id: str,
    profile_module: str = "jq_runtime_config",
) -> Dict[str, bool]:
    """Read public account switches; an absent strategy defaults to JQ only."""

    config = _load_runtime_configuration(profile_module, strategy_id)
    return {
        "jq_account_enabled": config["jq_account_enabled"],
        "qmt_account_enabled": config["qmt_account_enabled"],
    }


def _runtime_mutation_guard(name: str, active_mode: str) -> Callable[..., Any]:
    def blocked(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("{}模式禁止交易变更: {}".format(active_mode, name))

    blocked.__name__ = "runtime_blocked_{}_{}".format(active_mode.lower(), name)
    return blocked


def _install_runtime_guards(
    namespace: Dict[str, Any],
    active_mode: str,
) -> Tuple[str, ...]:
    """把聚宽交易函数统一替换为失败关闭的 guard，返回排序后的阻断名单。"""

    names = tuple(sorted(_RUNTIME_MUTATION_NAMES))
    for name in names:
        namespace[name] = _runtime_mutation_guard(name, active_mode)
    return names


def _build_strategy_runtime_state(
    *,
    mode: str,
    run_type: str,
    strategy_id: str,
    profile: Optional[str],
    profile_module: Optional[str] = None,
    blocked_mutations: Tuple[str, ...] = (),
    validate_remote: bool = False,
) -> Dict[str, Any]:
    switches = _account_switches_from_mode(mode)
    state = {
        "api_version": STRATEGY_RUNTIME_API_VERSION,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "profile": profile,
        "mode": mode,
        "run_type": run_type,
        "strategy_id": strategy_id,
        "jq_account_enabled": switches["jq_account_enabled"],
        "qmt_account_enabled": switches["qmt_account_enabled"],
        "enabled": False,
        "orders_enabled": True,
        "production_ready": False,
        "reason": "backtest",
    }
    if mode == "BACKTEST" and validate_remote:
        state.update(
            {
                "profile_module": profile_module,
                "remote_validation_enabled": True,
                "reason": "backtest_remote_validation",
            }
        )
    elif mode == "JQ":
        state.update(
            {
                "profile_module": profile_module,
                "enabled": True,
                "orders_enabled": True,
                "reason": "jq",
            }
        )
    elif mode == "QMT_REMOTE":
        state.update(
            {
                "profile_module": profile_module,
                "enabled": True,
                "orders_enabled": True,
                "production_ready": False,
                "reason": "qmt_remote_profile_validated",
                "mirror_jq_orders": False,
                "blocked_mutations": blocked_mutations,
            }
        )
    elif mode == "JQ_QMT_PARALLEL":
        state.update(
            {
                "profile_module": profile_module,
                "enabled": True,
                "orders_enabled": True,
                "production_ready": False,
                "reason": "jq_qmt_parallel_profile_validated",
                "mirror_jq_orders": False,
            }
        )
    return state


def install_strategy_runtime(
    namespace: Dict[str, Any],
    *,
    context: Any,
    mode: str,
    strategy_id: str,
    expected_api_version: int = STRATEGY_RUNTIME_API_VERSION,
    profile_module: str = "jq_runtime_config",
    validate_remote: bool = False,
) -> Dict[str, Any]:
    """安装策略运行模式并返回运行时状态；任何校验失败都抛出异常。

    同一进程内重复安装仅在签名完全一致时幂等返回原状态；签名漂移、
    上一代 helper 遗留记录或记录缺失均失败关闭，必须使用干净进程重启。
    """

    global _active_signature, _active_state, _active_profile, _active_namespace

    if type(namespace) is not dict:
        raise RuntimeError("策略namespace必须是普通dict（请传入globals()）")
    if (
        type(expected_api_version) is not int
        or expected_api_version != STRATEGY_RUNTIME_API_VERSION
    ):
        raise RuntimeError(
            "helper运行时API版本不匹配：策略要求={}，实际helper={}；"
            "请同时更新策略和研究根目录helper并重启".format(
                expected_api_version, STRATEGY_RUNTIME_API_VERSION,
            )
        )
    mode = _normalise_runtime_mode(mode)
    if type(validate_remote) is not bool:
        raise RuntimeError("validate_remote必须是bool")
    if validate_remote and mode != "BACKTEST":
        raise RuntimeError("validate_remote仅用于BACKTEST远程预检")
    strategy_id = _validate_runtime_identifier(strategy_id, "strategy_id")
    profile_module = _validate_profile_module_name(profile_module)

    record = namespace.get(_RUNTIME_STATE_KEY)
    if record is not None:
        if type(record) is not dict or record.get("token") is not _MODULE_TOKEN:
            raise RuntimeError(
                "检测到上一代helper运行记录；必须使用干净运行进程重启"
            )
    elif _active_state is not None:
        raise RuntimeError("策略namespace运行记录缺失；必须使用干净运行进程重启")

    run_type = str(_run_type_from_context(context) or "").strip().lower()
    runtime_config = None  # type: Optional[Dict[str, Any]]
    if mode in ("JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL") or validate_remote:
        runtime_config = _load_runtime_configuration(
            profile_module, strategy_id
        )
        if mode in ("JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL") and runtime_config["mode"] != mode:
            raise RuntimeError("安装模式与STRATEGIES配置不一致")
        profile = runtime_config["profile"]  # type: Optional[str]
    else:
        profile = None
    signature = (
        mode,
        profile,
        strategy_id,
        profile_module,
        expected_api_version,
        run_type,
        validate_remote,
    )
    if _active_state is not None:
        if _active_signature != signature:
            raise RuntimeError("策略运行安装签名漂移；必须使用干净运行进程重启")
        if mode == "QMT_REMOTE":
            # 幂等重装仍补齐交易 guard，防止平台重建 namespace 后 guard 丢失
            _install_runtime_guards(namespace, mode)
        return dict(_active_state)

    blocked_mutations = ()  # type: Tuple[str, ...]
    runtime_profile = None  # type: Optional[Dict[str, Any]]
    if mode == "BACKTEST":
        if run_type not in (
            "simple_backtest", "full_backtest"
        ):
            raise RuntimeError(
                "MODE=BACKTEST仅允许聚宽回测，当前run_type={}".format(
                    run_type or "<empty>"
                )
            )
        if validate_remote:
            runtime_profile = runtime_config["connection"]
        state = _build_strategy_runtime_state(
            mode=mode,
            run_type=run_type,
            strategy_id=strategy_id,
            profile=profile,
            profile_module=(profile_module if validate_remote else None),
            validate_remote=validate_remote,
        )
    elif mode == "JQ":
        if run_type != "sim_trade":
            raise RuntimeError(
                "MODE=JQ仅允许聚宽模拟交易，当前run_type={}".format(
                    run_type or "<empty>"
                )
            )
        runtime_profile = runtime_config["connection"]
        state = _build_strategy_runtime_state(
            mode=mode,
            run_type=run_type,
            strategy_id=strategy_id,
            profile=profile,
            profile_module=profile_module,
        )
    elif mode == "QMT_REMOTE":
        if run_type != "sim_trade":
            raise RuntimeError(
                "MODE={} 仅允许聚宽模拟交易，当前run_type={}".format(
                    mode, run_type or "<empty>"
                )
            )
        runtime_profile = runtime_config["connection"]
        blocked_mutations = _install_runtime_guards(namespace, mode)
        state = _build_strategy_runtime_state(
            mode=mode,
            run_type=run_type,
            strategy_id=strategy_id,
            profile=profile,
            profile_module=profile_module,
            blocked_mutations=blocked_mutations,
        )
    else:
        if run_type != "sim_trade":
            raise RuntimeError(
                "MODE={} 仅允许聚宽模拟交易，当前run_type={}".format(
                    mode, run_type or "<empty>"
                )
            )
        runtime_profile = runtime_config["connection"]
        state = _build_strategy_runtime_state(
            mode=mode,
            run_type=run_type,
            strategy_id=strategy_id,
            profile=profile,
            profile_module=profile_module,
        )

    _active_signature = signature
    _active_state = dict(state)
    _active_namespace = namespace
    _security_name_cache.clear()
    if mode in ("JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL") or validate_remote:
        _active_profile = runtime_profile
    namespace[_RUNTIME_STATE_KEY] = {"token": _MODULE_TOKEN, "mode": mode}
    return dict(state)


class JoinQuantRuntime:
    """Small strategy-facing facade shared by all account combinations."""

    def __init__(
        self,
        state: Dict[str, Any],
        namespace: Optional[Dict[str, Any]] = None,
        qmt_initial_capital: Any = None,
    ) -> None:
        self.state = dict(state)
        self.mode = RuntimeMode(self.state["mode"])
        self.jq_account_enabled = bool(
            self.state.get("jq_account_enabled")
        )
        self.qmt_account_enabled = bool(
            self.state.get("qmt_account_enabled")
        )
        self._namespace = namespace
        self._qmt_initial_capital = qmt_initial_capital
        self._qmt_readiness_attempted = False
        # Only plain values live in g: JQ can pickle them across worker restarts.
        self._jq_plan = getattr((namespace or {}).get("g"), "bt_jq_plan", None)
        if self._jq_plan and self._jq_plan.get("strategy_id") != state.get("strategy_id"):
            self._jq_plan = None

    def _set_qmt_ready(self, ready: bool) -> None:
        global _active_state
        self.state["production_ready"] = ready
        if _active_state is not None:
            _active_state["production_ready"] = ready
        self._publish_state()

    def _retry_qmt_readiness(self, context: Any) -> None:
        """Retry a failed startup check immediately before QMT execution."""

        if (
            not self.qmt_account_enabled
            or not self._qmt_readiness_attempted
            or self.state.get("production_ready") is True
        ):
            return
        ensure_runtime_ready(self._qmt_initial_capital, context)
        self._set_qmt_ready(True)
        self._log("info", "QMT账户重新对账通过，恢复QMT执行")

    def _publish_state(self) -> None:
        if self._namespace is None:
            return
        global_state = self._namespace.get("g")
        if global_state is not None:
            setattr(global_state, "bt_runtime", self.state)

    def _log(self, level: str, message: str) -> None:
        if self._namespace is None:
            return
        logger = self._namespace.get("log")
        entry = getattr(logger, level, None)
        if callable(entry):
            entry(message)

    def _qmt_callback_allowed(self, context: Any, operation: str) -> bool:
        gate = _qmt_callback_gate(context)
        if gate.allowed:
            return True
        self._log(
            "warn",
            "检测到聚宽历史回放，已跳过QMT{} | 策略时间={} 实际时间={} 原因={}".format(
                operation,
                gate.strategy_time,
                gate.actual_time,
                gate.reason,
            ),
        )
        return False

    def _platform_api(self, name: str) -> Callable[..., Any]:
        if self._namespace is None:
            raise RuntimeError("策略namespace不可用")
        api = self._namespace.get(name)
        if not callable(api):
            raise RuntimeError("聚宽{}不可用".format(name))
        return api

    def configure_platform(self, benchmark: str = "000300.XSHG") -> None:
        """Apply the shared JoinQuant runtime settings used by ETF strategies."""

        if self._namespace is None:
            raise RuntimeError("策略namespace不可用")
        logger = self._namespace.get("log")
        set_level = getattr(logger, "set_level", None)
        if callable(set_level):
            set_level("system", "error")
        set_option = self._platform_api("set_option")
        set_option("avoid_future_data", True)
        self._platform_api("set_benchmark")(benchmark)
        set_option("use_real_price", True)
        order_cost = self._platform_api("OrderCost")(
            close_tax=0.000,
            open_commission=0.00025,
            close_commission=0.00025,
            min_commission=5,
        )
        self._platform_api("set_order_cost")(order_cost, type="fund")
        slippage = self._platform_api("FixedSlippage")(0.002)
        self._platform_api("set_slippage")(slippage)

    def schedule_daily(
        self,
        before_market_open: Callable[[Any], Any],
        market_open: Callable[[Any], Any],
        risk_management: Callable[[Any], Any],
        risk_check_times: Tuple[str, ...],
        after_market_check: Callable[[Any], Any],
        reference_security: str = "000300.XSHG",
    ) -> None:
        """Register the common ETF strategy schedule on JoinQuant."""

        run_daily = self._platform_api("run_daily")
        run_daily(
            before_market_open,
            "09:20",
            reference_security=reference_security,
        )
        run_daily(
            market_open,
            "09:30",
            reference_security=reference_security,
        )
        for risk_time in risk_check_times:
            run_daily(
                risk_management,
                time=risk_time,
                reference_security=reference_security,
            )
        run_daily(
            after_market_check,
            time="14:55",
            reference_security=reference_security,
        )
        self._log(
            "info",
            "任务调度完成 | 09:20 盘前预处理 | 09:30 开盘下单 | "
            "风控: {} | 14:55 尾盘快照".format("/".join(risk_check_times)),
        )

    def log_process_initialize(self) -> None:
        self._log(
            "info",
            "process_initialize 重建配置 {} | helper API={} marker={}".format(
                time.strftime("%Y-%m-%d %H:%M:%S"),
                STRATEGY_RUNTIME_API_VERSION,
                STRATEGY_RUNTIME_HELPER_MARKER,
            ),
        )

    def log_strategy_event(self, message: str) -> None:
        self._log("info", "[策略通知] {}".format(message))

    def portfolio(self, context: Any) -> Any:
        return runtime_portfolio(context)

    def account_portfolios(self, context: Any) -> Tuple[Any, ...]:
        """Return independent enabled-account snapshots, QMT first."""

        views = []
        if self.qmt_account_enabled:
            try:
                portfolio = get_portfolio(
                    as_of=getattr(context, "current_dt", None)
                )
                _record_runtime_portfolio(portfolio)
                views.append(AccountPortfolioView("QMT", portfolio))
            except Exception as exc:
                self._set_qmt_ready(False)
                if not self.jq_account_enabled:
                    raise
                self._log(
                    "error",
                    "QMT组合快照读取失败，JQ组合快照继续：{}".format(exc),
                )
        if self.jq_account_enabled:
            views.append(AccountPortfolioView("JQ", context.portfolio))
        return tuple(views)

    def log_account_snapshots(self, context: Any) -> None:
        """Read and log enabled accounts without leaking account branches."""

        views = self.account_portfolios(context)
        self._log("info", "===== 尾盘组合快照 =====")
        for view in views:
            account = view.account
            portfolio = view.portfolio
            if account == "QMT":
                fund_message = (
                    "QMT组合资金 | 可用={:.2f} 冻结={:.2f} "
                    "总资产={:.2f} 持仓市值={:.2f}"
                ).format(
                    portfolio.available_cash,
                    float(getattr(portfolio, "reserved_cash", 0.0)),
                    portfolio.total_value,
                    portfolio.positions_value,
                )
            else:
                fund_message = (
                    "{}组合资金 | 可用={:.2f} 总资产={:.2f} "
                    "持仓市值={:.2f}"
                ).format(
                    account,
                    portfolio.available_cash,
                    portfolio.total_value,
                    portfolio.positions_value,
                )
            self._log("info", fund_message)
            if account == "JQ" and self._jq_plan:
                self._log("info", "JQ调仓状态 | 交易日={} 状态={}".format(
                    self._jq_plan["day"], self._jq_plan["phase"],
                ))
            for security in sorted(portfolio.positions):
                position = portfolio.positions[security]
                market_detail = ""
                if account == "QMT":
                    market_detail = " 行情时间={} 来源={}".format(
                        getattr(position, "mark_as_of", None) or "未知",
                        getattr(position, "mark_source", None) or "未知",
                    )
                self._log(
                    "info",
                    "{}组合持仓 | {} 数量={} 可卖={} 成本={:.4f} 现价={:.4f}{}".format(
                        account,
                        _security_label(security),
                        position.total_amount,
                        getattr(position, "closeable_amount", 0),
                        position.avg_cost,
                        position.price,
                        market_detail,
                    ),
                )
            if account == "QMT":
                fee_display = "未知"
                if portfolio.fees_known:
                    fee_display = "{:.2f}".format(portfolio.fees)
                if portfolio.performance_ready:
                    self._log(
                        "info",
                        "真实指标 | NAV={:.6f} 收益={:.2%} 费用={}".format(
                            portfolio.nav,
                            portfolio.returns,
                            fee_display,
                        ),
                    )
                else:
                    self._log(
                        "info",
                        "真实指标 | 暂不可用 | 费用={} | 原因={}".format(
                            fee_display,
                            ",".join(portfolio.performance_blockers),
                        ),
                    )
                self._log_qmt_execution_summary(portfolio, context)

    def _log_qmt_execution_summary(
        self, portfolio: Any, context: Any = None
    ) -> None:
        """Log target, position and order progress from the remote ledger."""

        try:
            current_dt = getattr(context, "current_dt", None)
            key = (
                "open-{}".format(current_dt.strftime("%Y%m%d"))
                if current_dt is not None
                else None
            )
            intent = get_intent(idempotency_key=key) if key else get_intent()
        except Exception as exc:
            self._log("warn", "QMT执行状态读取失败：{}".format(exc))
            return
        if not intent:
            self._log("info", "QMT执行状态 | 当前无活动目标")
            return
        self._log(
            "info",
            "QMT执行状态 | 交易日={} 状态={} 冻结资金={:.2f}".format(
                intent.get("trading_day") or "未知",
                intent.get("state") or "未知",
                float(getattr(portfolio, "reserved_cash", 0.0)),
            ),
        )
        targets = {
            str(security): int(quantity)
            for security, quantity in intent.get("targets", {}).items()
        }
        orders = [
            item for item in intent.get("orders", ()) if type(item) is dict
        ]
        securities = set(targets) | set(portfolio.positions)
        securities.update(
            str(item.get("security"))
            for item in orders
            if item.get("security")
        )
        for security in sorted(securities):
            target = targets.get(security, 0)
            position = portfolio.positions.get(security)
            current = int(getattr(position, "total_amount", 0))
            related = [
                item for item in orders if item.get("security") == security
            ]
            requested = sum(
                int(item.get("requested_qty", 0)) for item in related
            )
            filled = sum(int(item.get("filled_qty", 0)) for item in related)
            states = sorted(
                {str(item.get("state") or "UNKNOWN") for item in related}
            )
            delta = target - current
            difference = (
                "已达目标"
                if delta == 0
                else "待买{}".format(delta)
                if delta > 0
                else "待卖{}".format(-delta)
            )
            self._log(
                "info",
                "QMT执行明细 | {} 目标={} 当前={} 差额={} "
                "累计委托={} 累计成交={} 状态={}".format(
                    _security_label(security),
                    target,
                    current,
                    difference,
                    requested,
                    filled,
                    "/".join(states) if states else "等待价格",
                ),
            )

    def ensure_ready(self, qmt_initial_capital: Any, context: Any) -> Any:
        self._qmt_initial_capital = qmt_initial_capital
        self._qmt_readiness_attempted = self.qmt_account_enabled
        try:
            portfolio = ensure_runtime_ready(qmt_initial_capital, context)
        except Exception as exc:
            self._set_qmt_ready(False)
            if not self.jq_account_enabled:
                raise
            self._log(
                "warn",
                "QMT账户暂未就绪，JQ账户继续运行；QMT将在实际执行前重新对账：{}".format(
                    exc
                ),
            )
            return context.portfolio
        if self.qmt_account_enabled:
            self._set_qmt_ready(True)
        return portfolio

    def submit_targets(
        self,
        context: Any,
        weights: Dict[str, Any],
        marks: Dict[str, Any],
        idempotency_key: str,
        execution: ExecutionRequest,
        security_names: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return submit_runtime_targets(
            context,
            weights,
            marks,
            idempotency_key,
            execution,
            security_names,
        )

    @staticmethod
    def security_name(security: str, known_name: Any = "") -> str:
        return _security_name(security, known_name)

    @staticmethod
    def security_label(security: str, known_name: Any = "") -> str:
        return _security_label(security, known_name)

    def advance_targets(self, context: Any) -> bool:
        return advance_runtime_targets(context)

    def cancel_targets(self) -> bool:
        return cancel_runtime_targets()

    def cancel_orders(self) -> int:
        return cancel_runtime_orders()

    def order_target(self, security: str, amount: int) -> Any:
        return runtime_order_target(security, amount)

    def order_target_value(
        self,
        security: str,
        target_value: float,
        limit_price: Optional[float] = None,
    ) -> Any:
        return runtime_order_target_value(
            security, target_value, limit_price=limit_price
        )

    def notify_target_buy_plan(
        self, items: Any, occurred_at: Any = None
    ) -> Dict[str, Any]:
        return notify_target_buy_plan(items, occurred_at=occurred_at)

    @staticmethod
    def target_buy_plan_item(
        security: str,
        target_value: float,
        current_value: float,
        reference_price: float,
        lot_size: int = 100,
        security_name: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Build one round-lot incremental buy-plan item for notification."""

        if reference_price <= 0 or lot_size <= 0:
            return None
        target_delta = target_value - current_value
        quantity = int(target_delta / reference_price) // lot_size * lot_size
        if quantity <= 0:
            return None
        item = {
            "security": security,
            "quantity": quantity,
            "amount": round(quantity * reference_price, 2),
            "reference_price": reference_price,
        }
        if security_name:
            item["security_name"] = security_name
        return item

    @staticmethod
    def _position_value(position: Any, reference_price: float) -> float:
        if position is None:
            return 0.0
        value = getattr(position, "value", None)
        if value is not None:
            return float(value)
        return float(getattr(position, "total_amount", 0)) * reference_price

    def _target_buy_plan_items(
        self,
        portfolio: Any,
        weights: Dict[str, Any],
        marks: Dict[str, Any],
    ) -> Any:
        total_value = float(portfolio.total_value)
        items = []
        for security, raw_weight in weights.items():
            weight = float(raw_weight)
            reference_price = float(marks.get(security, 0.0))
            position = portfolio.positions.get(security)
            item = self.target_buy_plan_item(
                security,
                total_value * weight,
                self._position_value(position, reference_price),
                reference_price,
                security_name=_security_name(security),
            )
            if item is not None:
                items.append(item)
        return items

    def execute_rebalance(
        self,
        context: Any,
        weights: Dict[str, Any],
        marks: Dict[str, Any],
        idempotency_key: str,
        execution: Optional[ExecutionRequest] = None,
    ) -> Dict[str, Any]:
        """Apply one decision independently to QMT and JQ enabled accounts."""

        execution = execution or default_etf_rebalance_execution()
        result = {"qmt": None, "jq_orders": [], "errors": []}
        notification_items = []
        qmt_submitted = False

        qmt_callback_allowed = (
            self.qmt_account_enabled
            and self._qmt_callback_allowed(context, "调仓执行")
        )
        if self.qmt_account_enabled and not qmt_callback_allowed:
            result["qmt"] = {"skipped_historical_replay": True}
        if qmt_callback_allowed:
            try:
                self._retry_qmt_readiness(context)
                if not self.advance_targets(context):
                    result["qmt"] = {"skipped_active_intent": True}
                else:
                    qmt_portfolio = get_portfolio(
                        as_of=getattr(context, "current_dt", None)
                    )
                    _record_runtime_portfolio(qmt_portfolio)
                    qmt_weights = dict(weights)
                    if not qmt_weights:
                        qmt_weights = {
                            security: 0.0
                            for security in qmt_portfolio.positions
                        }
                    qmt_marks = dict(marks)
                    for security, position in qmt_portfolio.positions.items():
                        qmt_marks.setdefault(security, float(position.price))
                    qmt_security_names = _security_names(
                        set(qmt_weights) | set(qmt_portfolio.positions)
                    )
                    notification_items = self._target_buy_plan_items(
                        qmt_portfolio, weights, marks
                    )
                    if qmt_weights:
                        result["qmt"] = self.submit_targets(
                            context,
                            qmt_weights,
                            qmt_marks,
                            idempotency_key,
                            execution,
                            qmt_security_names,
                        )
                        qmt_submitted = True
            except Exception as exc:
                result["errors"].append(("QMT", str(exc)))
                self._log(
                    "error",
                    "QMT调仓执行异常：{}".format(exc),
                )

        if qmt_submitted:
            self.send_target_buy_plan(
                notification_items,
                occurred_at=getattr(context, "current_dt", None),
            )

        if self.jq_account_enabled:
            try:
                jq_portfolio = context.portfolio
                if not self.qmt_account_enabled:
                    notification_items = self._target_buy_plan_items(
                        jq_portfolio, weights, marks
                    )
                    self.send_target_buy_plan(
                        notification_items,
                        occurred_at=getattr(context, "current_dt", None),
                    )
                self._start_jq_rebalance(context, weights, marks, idempotency_key)
                result["jq_orders"] = self._advance_jq_rebalance(context)
            except Exception as exc:
                result["errors"].append(("JQ", str(exc)))
                self._log(
                    "error",
                    "JQ调仓执行异常：{}".format(exc),
                )
        qmt_result = result.get("qmt")
        if qmt_result is not None and "intent" in qmt_result:
            self._log(
                "info",
                "QMT组合目标已提交 | intent_id={} state={} weights={}".format(
                    qmt_result["intent"]["intent_id"],
                    qmt_result["intent"]["state"],
                    {
                        _security_label(security): value
                        for security, value in weights.items()
                    },
                ),
            )
        elif qmt_result is not None and qmt_result.get(
            "skipped_active_intent"
        ):
            self._log("info", "QMT仍有活动目标，本轮仅继续推进原目标")
        if result["errors"]:
            self._log(
                "warn",
                "目标权重部分执行失败 | accounts={}".format(
                    [account for account, _ in result["errors"]]
                ),
            )
        else:
            self._log(
                "info",
                "目标权重已提交 | {} | 部署比例={:.1%}".format(
                    {
                        _security_label(security): value
                        for security, value in weights.items()
                    },
                    sum(float(value) for value in weights.values()),
                ),
            )
        return result

    def _save_jq_plan(self, plan: Dict[str, Any]) -> None:
        self._jq_plan = plan
        global_state = (self._namespace or {}).get("g")
        if global_state is not None:
            setattr(global_state, "bt_jq_plan", plan)

    def _start_jq_rebalance(self, context, weights, marks, key):
        parts = _strategy_clock_parts(context.current_dt)
        if parts is None:
            raise RuntimeError("JQ调仓策略时间不可识别")
        day = "{:04d}-{:02d}-{:02d}".format(*parts[:3])
        old = self._jq_plan
        if old and old["day"] == day and old["key"] == key:
            return
        self.cancel_orders()
        total = float(context.portfolio.total_value)
        targets = {
            security: 0.0 for security in context.portfolio.positions
            if security not in weights
        }
        targets.update({
            security: total * float(weight) for security, weight in weights.items()
        })
        self._save_jq_plan({
            "strategy_id": self.state.get("strategy_id"), "day": day, "key": key,
            "targets": targets, "marks": dict(marks), "phase": "SELL",
            "done": [], "order_ids": [], "last_bar": None,
        })
        self._log("info", "JQ调仓开始 | 先卖后买 | 目标数={}".format(len(weights)))

    def _jq_gap(self, context, security, target):
        position = context.portfolio.positions.get(security)
        if target == 0:
            held = float(getattr(position, "total_amount", 0.0) or 0.0)
            return (-1.0 if held > 0 else 0.0), 0.0, position
        price = float(getattr(position, "price", 0.0) or 0.0)
        current_data = (self._namespace or {}).get("get_current_data")
        if callable(current_data):
            quote = current_data()[security]
            price = float(getattr(quote, "last_price", 0.0) or price)
        if not math.isfinite(price) or price <= 0:
            price = float(self._jq_plan["marks"].get(security, 0.0))
        if not math.isfinite(price) or price <= 0:
            raise RuntimeError("JQ调仓缺少有效价格：{}".format(_security_label(security)))
        quantity = getattr(position, "total_amount", None)
        value = (
            float(quantity) * price if quantity is not None
            else self._position_value(position, price)
        )
        return target - value, 100 * price, position

    def _cancel_jq_plan(self, reason: str) -> None:
        plan = self._jq_plan
        if not plan or plan["phase"] in ("COMPLETED", "CANCELED", "EXPIRED"):
            return
        plan["phase"] = "EXPIRED" if reason == "跨日过期" else "CANCELED"
        self._save_jq_plan(plan)
        for order in list((self._platform_api("get_open_orders")() or {}).values()):
            if str(getattr(order, "order_id", "")) in plan["order_ids"]:
                self._platform_api("cancel_order")(order)
        self._log("info", "JQ调仓停止 | {}".format(reason))

    def on_bar(self, context: Any) -> None:
        """Thin platform callback: resume JQ only, never issue QMT RPCs."""
        try:
            self._advance_jq_rebalance(context)
        except Exception as exc:
            self._log("error", "JQ调仓续行异常：{}".format(exc))

    def _advance_jq_rebalance(self, context):
        plan = self._jq_plan
        if not self.jq_account_enabled or not plan:
            return []
        if plan["phase"] in ("COMPLETED", "CANCELED", "EXPIRED"):
            return []
        parts = _strategy_clock_parts(context.current_dt)
        if parts is None:
            raise RuntimeError("JQ调仓策略时间不可识别")
        if "{:04d}-{:02d}-{:02d}".format(*parts[:3]) != plan["day"]:
            self._cancel_jq_plan("跨日过期")
            return []
        minute = parts[3] * 60 + parts[4]
        if not (9 * 60 + 30 <= minute < 11 * 60 + 30 or 13 * 60 <= minute < 15 * 60):
            return []
        bar = list(parts[:5])
        if plan["last_bar"] == bar:
            return []
        plan["last_bar"] = bar
        self._save_jq_plan(plan)
        submitted = []

        def active_securities():
            return {
                getattr(order, "security", "") for order in
                (self._platform_api("get_open_orders")() or {}).values()
            }

        def place(security, target):
            order = (
                self.order_target(security, 0) if target == 0
                else self.order_target_value(security, target)
            )
            order_id = getattr(order, "order_id", None)
            if order_id is not None and str(order_id) not in plan["order_ids"]:
                plan["order_ids"].append(str(order_id))
            submitted.append((security, target, order))
            self._save_jq_plan(plan)
            self._log("info", "JQ目标已提交 | {} 目标市值={:.2f} order_id={}".format(
                _security_label(security), target, order_id,
            ))

        if plan["phase"] == "SELL":
            active = active_securities()
            for security, target in plan["targets"].items():
                gap, lot_value, position = self._jq_gap(context, security, target)
                if gap >= 0 or gap > -lot_value + 1e-6 or security in active:
                    continue
                if getattr(position, "closeable_amount", 1) <= 0:
                    continue
                place(security, target)
            active = active_securities()
            waiting = []
            for security, target in plan["targets"].items():
                gap, lot_value, _ = self._jq_gap(context, security, target)
                if (gap < 0 and gap <= -lot_value + 1e-6) or security in active:
                    waiting.append(security)
            if waiting:
                if plan.get("waiting") != waiting:
                    self._log("info", "JQ调仓等待卖出 | {} | 下一分钟继续，不提前买入".format(
                        ", ".join(_security_label(s) for s in waiting),
                    ))
                    plan["waiting"] = waiting
                return submitted
            plan["phase"] = "BUY"
            self._log("info", "JQ减仓已确认，开始买入")

        active = active_securities()
        for security, target in plan["targets"].items():
            if target == 0 or security in plan["done"] or security in active:
                continue
            gap, lot_value, _ = self._jq_gap(context, security, target)
            if gap >= lot_value - 1e-6:
                place(security, target)
                gap, lot_value, _ = self._jq_gap(context, security, target)
            if gap < lot_value - 1e-6:
                plan["done"].append(security)
        active = active_securities()
        completed = all(
            target == 0 or security in plan["done"]
            for security, target in plan["targets"].items()
        )
        if completed and not (active & set(plan["targets"])):
            plan["phase"] = "COMPLETED"
            self._log("info", "JQ调仓已完成 | 实际持仓达到目标整手容差")
        self._save_jq_plan(plan)
        return submitted

    @staticmethod
    def _risk_exits(
        portfolio: Any,
        stop_loss_ratio: float,
        take_profit_ratio: float,
    ) -> Tuple[Any, Any, Any]:
        checks = []
        stop_loss = []
        take_profit = []
        for security, position in portfolio.positions.items():
            price = float(position.price)
            avg_cost = float(position.avg_cost)
            pnl = price / avg_cost - 1.0 if avg_cost else 0.0
            action = "HOLD"
            if avg_cost and price < avg_cost * stop_loss_ratio:
                action = "STOP_LOSS"
                stop_loss.append(security)
            elif avg_cost and price > avg_cost * take_profit_ratio:
                action = "TAKE_PROFIT"
                take_profit.append(security)
            checks.append(
                {
                    "security": security,
                    "price": price,
                    "avg_cost": avg_cost,
                    "pnl": pnl,
                    "action": action,
                    "mark_as_of": getattr(position, "mark_as_of", None),
                    "mark_source": getattr(position, "mark_source", None),
                }
            )
        return checks, stop_loss, take_profit

    def execute_risk_management(
        self,
        context: Any,
        stop_loss_ratio: float,
        take_profit_ratio: float,
        idempotency_key: str,
        stop_loss_execution: Optional[ExecutionRequest] = None,
        take_profit_execution: Optional[ExecutionRequest] = None,
    ) -> Dict[str, Any]:
        """Evaluate each account against its own positions and cost basis."""

        stop_loss_execution = (
            stop_loss_execution or default_etf_stop_loss_execution()
        )
        take_profit_execution = (
            take_profit_execution or default_etf_take_profit_execution()
        )
        result = {"accounts": [], "errors": []}
        qmt_callback_allowed = (
            self.qmt_account_enabled
            and self._qmt_callback_allowed(context, "风控执行")
        )
        if self.qmt_account_enabled and not qmt_callback_allowed:
            result["qmt_skipped_historical_replay"] = True
        if qmt_callback_allowed:
            try:
                self._retry_qmt_readiness(context)
                intent_idle = self.advance_targets(context)
                portfolio = get_portfolio(
                    as_of=getattr(context, "current_dt", None)
                )
                _record_runtime_portfolio(portfolio)
                checks, stop_loss, take_profit = self._risk_exits(
                    portfolio, stop_loss_ratio, take_profit_ratio
                )
                account_result = {
                    "account": "QMT",
                    "portfolio": portfolio,
                    "checks": checks,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "intent": None,
                }
                exits = stop_loss + take_profit
                if exits:
                    if not intent_idle and not self.cancel_targets():
                        account_result["waiting_for_cancel"] = True
                    else:
                        total = float(portfolio.total_value) or 1.0
                        target_weights = {
                            security: (
                                0.0
                                if security in exits
                                else float(position.value) / total
                            )
                            for security, position in portfolio.positions.items()
                        }
                        risk_marks = {
                            security: float(position.price)
                            for security, position in portfolio.positions.items()
                        }
                        account_result["intent"] = self.submit_targets(
                            context,
                            target_weights,
                            risk_marks,
                            idempotency_key,
                            (
                                stop_loss_execution
                                if stop_loss
                                else take_profit_execution
                            ),
                            _security_names(portfolio.positions),
                        )
                result["accounts"].append(account_result)
            except Exception as exc:
                result["errors"].append(("QMT", str(exc)))
                self._log("error", "QMT风控执行异常：{}".format(exc))

        if self.jq_account_enabled:
            try:
                portfolio = context.portfolio
                checks, stop_loss, take_profit = self._risk_exits(
                    portfolio, stop_loss_ratio, take_profit_ratio
                )
                account_result = {
                    "account": "JQ",
                    "portfolio": portfolio,
                    "checks": checks,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "orders": [],
                }
                if stop_loss or take_profit:
                    self._cancel_jq_plan("止盈止损优先，停止旧调仓补买")
                for security in stop_loss + take_profit:
                    order_obj = self.order_target(security, 0)
                    account_result["orders"].append(
                        (security, order_obj)
                    )
                result["accounts"].append(account_result)
            except Exception as exc:
                result["errors"].append(("JQ", str(exc)))
                self._log("error", "JQ风控执行异常：{}".format(exc))
        for account_result in result["accounts"]:
            account = account_result["account"]
            checks = account_result["checks"]
            if not checks:
                self._log("info", "{}风控检查 | 当前无持仓".format(account))
                continue
            self._log(
                "info",
                "{}风控检查开始 | 持仓 {} 只 | {}".format(
                    account, len(checks), getattr(context, "current_dt", None)
                ),
            )
            for check in checks:
                market_detail = ""
                if account == "QMT":
                    market_detail = " 行情时间={} 来源={}".format(
                        check.get("mark_as_of") or "未知",
                        check.get("mark_source") or "未知",
                    )
                self._log(
                    "info",
                    "{}持仓检查 | {} 成本={:.4f} 现价={:.4f} 盈亏={:.2%}{}".format(
                        account,
                        _security_label(check["security"]),
                        check["avg_cost"],
                        check["price"],
                        check["pnl"],
                        market_detail,
                    ),
                )
                if check["action"] != "HOLD":
                    action = (
                        "止损"
                        if check["action"] == "STOP_LOSS"
                        else "止盈"
                    )
                    self._log(
                        "info",
                        "[策略通知] {}触发 | 账户：{} | 标的：{} | "
                        "成本价：{:.4f} | 当前价：{:.4f} | 时间：{}".format(
                            action,
                            account,
                            _security_label(check["security"]),
                            check["avg_cost"],
                            check["price"],
                            getattr(context, "current_dt", None),
                        ),
                    )
            intent = account_result.get("intent")
            if intent is not None:
                exits = (
                    account_result["stop_loss"]
                    + account_result["take_profit"]
                )
                self._log(
                    "info",
                    "QMT风控目标已提交 | intent_id={} exits={}".format(
                        intent["intent"]["intent_id"],
                        [_security_label(security) for security in exits],
                    ),
                )
            if account_result.get("waiting_for_cancel"):
                self._log("warn", "QMT风控目标等待旧调仓订单撤销确认")
        return result

    def send_target_buy_plan(
        self, items: Any, occurred_at: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Send a plan card without allowing notification failure to stop trading."""

        if not items or self.mode is RuntimeMode.BACKTEST:
            return None
        try:
            result = self.notify_target_buy_plan(
                items, occurred_at=occurred_at
            )
            if result.get("accepted"):
                self._log(
                    "info",
                    "策略目标买入计划卡片已提交 | 标的数={} 总金额={:.2f}".format(
                        result.get("item_count", len(items)),
                        float(result.get("total_amount", 0.0)),
                    ),
                )
            else:
                self._log(
                    "warn", "策略目标买入计划卡片未启用或发送队列未接受"
                )
            return result
        except Exception as exc:
            self._log(
                "warn",
                "策略目标买入计划通知失败：{}".format(
                    type(exc).__name__
                ),
            )
            return None


def install_joinquant_runtime(
    namespace: Dict[str, Any],
    *,
    context: Any,
    strategy_id: str,
    qmt_initial_capital: Any,
    profile_module: str = "jq_runtime_config",
    validate_remote_during_backtest: bool = False,
    expected_api_version: int = STRATEGY_RUNTIME_API_VERSION,
) -> JoinQuantRuntime:
    """Resolve the platform run type, install one mode, and run optional preflight."""

    run_type = str(_run_type_from_context(context) or "").strip().lower()
    if run_type in ("simple_backtest", "full_backtest"):
        mode = RuntimeMode.BACKTEST
        validate_remote = validate_remote_during_backtest
    elif run_type == "sim_trade":
        switches = get_configured_account_switches(
            strategy_id, profile_module
        )
        mode = RuntimeMode(
            _mode_from_account_switches(
                switches["jq_account_enabled"],
                switches["qmt_account_enabled"],
            )
        )
        validate_remote = False
    else:
        raise RuntimeError(
            "不支持的聚宽run_type: {}".format(run_type or "<empty>")
        )
    state = install_strategy_runtime(
        namespace,
        context=context,
        mode=mode.value,
        strategy_id=strategy_id,
        expected_api_version=expected_api_version,
        profile_module=profile_module,
        validate_remote=validate_remote,
    )
    runtime = JoinQuantRuntime(
        state, namespace, qmt_initial_capital=qmt_initial_capital
    )
    runtime._publish_state()
    if not validate_remote:
        # Account initialization and reconciliation are runtime concerns.
        # Strategy code installs one facade and never calls readiness APIs or
        # branches on account type.  Parallel mode isolates a temporary QMT
        # failure inside JoinQuantRuntime.ensure_ready; QMT-only remains
        # fail-closed.
        if runtime.qmt_account_enabled:
            runtime.ensure_ready(qmt_initial_capital, context)
        return runtime
    if runtime.state.get("remote_validation"):
        validation = runtime.state["remote_validation"]
        global_state = namespace.get("g")
        if global_state is not None:
            setattr(global_state, "bt_remote_validation", validation)
        return runtime
    ensured = ensure_account(qmt_initial_capital)
    reconciliation = ensured.get("reconciliation", {})
    if reconciliation.get("state") != "READY":
        raise RuntimeError(
            "回测远程预检对账未就绪: {}".format(
                reconciliation.get("details", {}).get("blockers", [])
            )
        )
    portfolio = get_portfolio()
    latest = get_reconciliation().get("reconciliation", {})
    if latest.get("state") != "READY":
        raise RuntimeError(
            "回测远程预检快照对账未就绪: {}".format(
                latest.get("details", {}).get("blockers", [])
            )
        )
    runtime.state["remote_validation"] = {
        "cash": portfolio.available_cash,
        "positions_value": portfolio.positions_value,
        "total_value": portfolio.total_value,
        "position_count": len(portfolio.positions),
        "reconciliation": "READY",
    }
    if _active_state is not None:
        _active_state["remote_validation"] = dict(
            runtime.state["remote_validation"]
        )
    global_state = namespace.get("g")
    if global_state is not None:
        setattr(
            global_state,
            "bt_remote_validation",
            runtime.state["remote_validation"],
        )
    runtime._publish_state()
    runtime._log(
        "info",
        "回测远程预检通过 | 可用资金={:.2f} 持仓市值={:.2f} "
        "总资产={:.2f} 持仓数={}".format(
            portfolio.available_cash,
            portfolio.positions_value,
            portfolio.total_value,
            len(portfolio.positions),
        ),
    )
    return runtime
