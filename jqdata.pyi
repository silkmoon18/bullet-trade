"""Local JoinQuant-compatible API surface.

The declarations intentionally model the stable strategy-facing contract. The
runtime implementation remains :mod:`jqdata`/``bullet_trade.compat.jqdata``.
"""

import datetime as datetime
import math as math
import random as random
import time as time
from datetime import date, datetime as DateTime
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Union

from joinquant_typing import Context as Context
from joinquant_typing import Portfolio as Portfolio
from joinquant_typing import Position as Position
from joinquant_typing import Snapshot as _Snapshot

np: Any
pd: Any
g: Any


class _Logger:
    def debug(self, message: object, *args: object, **kwargs: object) -> None: ...
    def info(self, message: object, *args: object, **kwargs: object) -> None: ...
    def warning(self, message: object, *args: object, **kwargs: object) -> None: ...
    def warn(self, message: object, *args: object, **kwargs: object) -> None: ...
    def error(self, message: object, *args: object, **kwargs: object) -> None: ...
    def critical(self, message: object, *args: object, **kwargs: object) -> None: ...
    def set_level(self, module: str, level: str) -> None: ...


log: _Logger


class OrderStatus(Enum):
    new = "new"
    open = "open"
    filling = "filling"
    partly_canceled = "partly_canceled"
    canceling = "canceling"
    filled = "filled"
    canceled = "canceled"
    rejected = "rejected"
    held = "held"


class OrderStyle(Enum):
    market = "market"
    limit = "limit"


class MarketOrderStyle:
    limit_price: Optional[float]
    buy_price_percent: Optional[float]
    sell_price_percent: Optional[float]
    def __init__(
        self,
        limit_price: Optional[float] = ...,
        buy_price_percent: Optional[float] = ...,
        sell_price_percent: Optional[float] = ...,
    ) -> None: ...


class LimitOrderStyle:
    price: float
    def __init__(self, price: float) -> None: ...


class Order:
    order_id: str
    security: str
    amount: int
    filled: int
    price: float
    status: OrderStatus
    is_buy: bool


class Trade:
    order_id: str
    security: str
    amount: int
    price: float
    commission: float
    tax: float
    trade_id: str


class SubPortfolio(Portfolio, Protocol): ...


class SecurityUnitData(_Snapshot, Protocol):
    security: str


class OrderCost:
    def __init__(
        self,
        open_tax: float = ...,
        close_tax: float = ...,
        open_commission: float = ...,
        close_commission: float = ...,
        min_commission: float = ...,
        close_today_commission: float = ...,
        commission_type: str = ...,
    ) -> None: ...


class PerTrade:
    def __init__(self, buy_cost: float = ..., sell_cost: float = ..., min_cost: float = ...) -> None: ...


class FixedSlippage:
    def __init__(self, value: float = ...) -> None: ...


class PriceRelatedSlippage:
    def __init__(self, ratio: float = ...) -> None: ...


class StepRelatedSlippage:
    def __init__(self, steps: int = ...) -> None: ...


def send_msg(message: str) -> None: ...
def set_message_handler(handler: Optional[Callable[[str], None]]) -> None: ...
def set_benchmark(security: str) -> None: ...
def set_option(key: str, value: Any) -> None: ...
def set_order_cost(order_cost: OrderCost, type: str = ..., ref: Optional[str] = ...) -> None: ...
def set_commission(per_trade: PerTrade) -> None: ...
def set_universe(stocks: Iterable[str]) -> None: ...
def set_slippage(slippage: Any, type: Optional[str] = ..., ref: Optional[str] = ...) -> None: ...

# BulletTrade extension; not provided by the hosted JoinQuant runtime.
def require_data_capabilities(
    required: Sequence[str] = ...,
    optional: Sequence[str] = ...,
    profile: Optional[str] = ...,
    schema_version: Optional[str] = ...,
) -> Any: ...

def order(
    security: str,
    amount: int,
    price: Optional[float] = ...,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = ...,
    wait_timeout: Optional[float] = ...,
    extra: Optional[Dict[str, Any]] = ...,
) -> Optional[Order]: ...
def order_value(
    security: str,
    value: float,
    price: Optional[float] = ...,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = ...,
    wait_timeout: Optional[float] = ...,
) -> Optional[Order]: ...
def order_target(
    security: str,
    amount: int,
    price: Optional[float] = ...,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = ...,
    wait_timeout: Optional[float] = ...,
) -> Optional[Order]: ...
def order_target_value(
    security: str,
    value: float,
    price: Optional[float] = ...,
    style: Optional[Union[OrderStyle, MarketOrderStyle, LimitOrderStyle]] = ...,
    wait_timeout: Optional[float] = ...,
) -> Optional[Order]: ...
def cancel_order(order_or_id: Union[Order, str]) -> bool: ...
def cancel_all_orders() -> int: ...
def get_open_orders() -> Dict[str, Order]: ...
def get_orders(
    order_id: Optional[str] = ...,
    security: Optional[str] = ...,
    status: Optional[object] = ...,
    from_broker: bool = ...,
    strict: bool = ...,
) -> Dict[str, Order]: ...
def get_trades(order_id: Optional[str] = ..., security: Optional[str] = ..., strict: bool = ...) -> Dict[str, Trade]: ...

def run_daily(
    func: Callable[..., Any],
    time: str = ...,
    reference_security: Optional[str] = ...,
) -> None: ...
def run_weekly(
    func: Callable[..., Any],
    weekday: int,
    time: str = ...,
    reference_security: Optional[str] = ...,
    force: bool = ...,
) -> None: ...
def run_monthly(
    func: Callable[..., Any],
    monthday: int,
    time: str = ...,
    reference_security: Optional[str] = ...,
    force: bool = ...,
) -> None: ...
def unschedule_all() -> None: ...

def get_price(*args: Any, **kwargs: Any) -> Any: ...
def history(
    count: int,
    unit: str = ...,
    field: Union[str, List[str]] = ...,
    security_list: Optional[Union[str, List[str]]] = ...,
    df: bool = ...,
    skip_paused: bool = ...,
    fq: str = ...,
) -> Any: ...
def attribute_history(
    security: str,
    count: int,
    unit: str = ...,
    fields: Optional[List[str]] = ...,
    skip_paused: bool = ...,
    df: bool = ...,
    fq: str = ...,
) -> Any: ...
def get_bars(*args: Any, **kwargs: Any) -> Any: ...
def get_ticks(*args: Any, **kwargs: Any) -> Any: ...
def get_current_tick(*args: Any, **kwargs: Any) -> Any: ...
def get_current_data() -> Mapping[str, _Snapshot]: ...
def get_extras(
    info: str,
    security_list: List[str],
    start_date: Optional[Union[str, date, DateTime]] = ...,
    end_date: Optional[Union[str, date, DateTime]] = ...,
    df: bool = ...,
    count: Optional[int] = ...,
) -> Any: ...
def get_fundamentals(*args: Any, **kwargs: Any) -> Any: ...
def get_fundamentals_continuously(*args: Any, **kwargs: Any) -> Any: ...
def get_trade_days(*args: Any, **kwargs: Any) -> Any: ...
def get_trade_day(*args: Any, **kwargs: Any) -> Any: ...
def get_all_securities(types: Union[str, List[str]] = ..., date: Optional[Union[str, date, DateTime]] = ...) -> Any: ...
def get_security_info(*args: Any, **kwargs: Any) -> Any: ...
def get_fund_info(*args: Any, **kwargs: Any) -> Any: ...
def get_index_stocks(*args: Any, **kwargs: Any) -> List[str]: ...
def get_index_weights(*args: Any, **kwargs: Any) -> Any: ...
def get_industry_stocks(*args: Any, **kwargs: Any) -> List[str]: ...
def get_industry(*args: Any, **kwargs: Any) -> Any: ...
def get_concept_stocks(*args: Any, **kwargs: Any) -> List[str]: ...
def get_concept(*args: Any, **kwargs: Any) -> Any: ...
def get_margincash_stocks(*args: Any, **kwargs: Any) -> Any: ...
def get_marginsec_stocks(*args: Any, **kwargs: Any) -> Any: ...
def get_dominant_future(*args: Any, **kwargs: Any) -> Any: ...
def get_future_contracts(*args: Any, **kwargs: Any) -> Any: ...
def get_billboard_list(*args: Any, **kwargs: Any) -> Any: ...
def get_locked_shares(*args: Any, **kwargs: Any) -> Any: ...
def get_split_dividend(*args: Any, **kwargs: Any) -> Any: ...
def set_data_provider(*args: Any, **kwargs: Any) -> None: ...
def get_data_provider(*args: Any, **kwargs: Any) -> Any: ...
def read_file(path: str) -> bytes: ...
def write_file(path: str, content: Union[str, bytes, bytearray, memoryview], append: bool = ...) -> None: ...
def subscribe(security: Union[str, Sequence[str]], frequency: str) -> None: ...
def unsubscribe(security: Union[str, Sequence[str]], frequency: str) -> None: ...
def unsubscribe_all() -> None: ...
def print_portfolio_info(context: Context, top_n: Optional[int] = ..., sort_by: str = ...) -> None: ...
def prettytable_print_df(df: Any, headers: str = ..., show_index: bool = ..., max_rows: int = ...) -> None: ...
