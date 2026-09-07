"""Small, explicit domain model for the first StrategyLedger release.

Money, price and NAV values are stored as scaled integers. Floats are rejected
at the boundary so persistence and replay do not depend on binary rounding.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Optional, Union

from .execution import ExecutionRequest


MONEY_SCALE = 10_000
PRICE_SCALE = 1_000_000
NAV_SCALE = 1_000_000
SHANGHAI_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")

DecimalInput = Union[str, int, Decimal]


class AccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    TRADING_BLOCKED = "TRADING_BLOCKED"
    RECONCILIATION_BLOCKED = "RECONCILIATION_BLOCKED"
    CLOSED = "CLOSED"


class IntentState(str, Enum):
    CREATED = "CREATED"
    PLANNED = "PLANNED"
    EXECUTING = "EXECUTING"
    RECONCILING = "RECONCILING"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderState(str, Enum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class FillPriceSource(str, Enum):
    BROKER_TRADE = "BROKER_TRADE"
    ORDER_PRICE_FALLBACK = "ORDER_PRICE_FALLBACK"
    ZERO_FALLBACK = "ZERO_FALLBACK"


class UnpricedFillPolicy(str, Enum):
    STRICT = "STRICT"
    CONSERVATIVE_ORDER_PRICE = "CONSERVATIVE_ORDER_PRICE"
    ZERO_FALLBACK = "ZERO_FALLBACK"


class ReconciliationState(str, Enum):
    UNKNOWN = "UNKNOWN"
    READY = "READY"
    BLOCKED = "BLOCKED"


def _scaled_int(value: DecimalInput, scale: int, field: str) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("{} must be str, int or Decimal, not float/bool".format(field))
    if not isinstance(value, (str, int, Decimal)):
        raise TypeError("{} must be str, int or Decimal".format(field))
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("{} is not a finite decimal".format(field)) from exc
    if not decimal_value.is_finite():
        raise ValueError("{} is not a finite decimal".format(field))
    return int((decimal_value * scale).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def money_to_units(value: DecimalInput) -> int:
    """Convert yuan to 1/10000-yuan integer units using half-up rounding."""

    return _scaled_int(value, MONEY_SCALE, "money")


def price_to_units(value: DecimalInput) -> int:
    """Convert a price to 1/1000000-yuan integer units using half-up rounding."""

    return _scaled_int(value, PRICE_SCALE, "price")


def units_to_decimal(value: int, scale: int = MONEY_SCALE) -> Decimal:
    if type(value) is not int or type(scale) is not int or scale <= 0:
        raise TypeError("value and positive scale must be integers")
    return Decimal(value) / Decimal(scale)


def as_shanghai_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(SHANGHAI_TZ)


def _require_int(value: int, field: str, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError("{} must be an integer >= {}".format(field, minimum))


def _freeze_json_value(value: object) -> object:
    if value is None or type(value) in (bool, int, float, str):
        return value
    if isinstance(value, Mapping):
        frozen = {}
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("payload keys must be strings")
            frozen[key] = _freeze_json_value(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json_value(item) for item in value)
    raise TypeError("payload values must be JSON-compatible")


def _freeze_json_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    frozen = _freeze_json_value(value)
    if not isinstance(frozen, Mapping):
        raise TypeError("payload must be a mapping")
    return frozen


@dataclass(frozen=True)
class CashPool:
    physical_account_id: str
    unallocated_cash_units: int
    reserved_cash_units: int
    version: int = 0

    def __post_init__(self) -> None:
        _require_int(self.unallocated_cash_units, "unallocated_cash_units")
        _require_int(self.reserved_cash_units, "reserved_cash_units")
        _require_int(self.version, "version")
        if self.reserved_cash_units > self.unallocated_cash_units:
            raise ValueError("reserved_cash_units cannot exceed unallocated_cash_units")

    @property
    def available_cash_units(self) -> int:
        return self.unallocated_cash_units - self.reserved_cash_units


@dataclass(frozen=True)
class StrategyAccount:
    account_id: str
    strategy_id: str
    physical_account_id: str
    initial_capital_units: int
    cash_units: int
    reserved_cash_units: int
    ledger_version: int
    event_seq: int
    status: AccountStatus = AccountStatus.ACTIVE

    def __post_init__(self) -> None:
        _require_int(self.initial_capital_units, "initial_capital_units")
        _require_int(self.cash_units, "cash_units")
        _require_int(self.reserved_cash_units, "reserved_cash_units")
        _require_int(self.ledger_version, "ledger_version")
        _require_int(self.event_seq, "event_seq")
        if self.reserved_cash_units > self.cash_units:
            raise ValueError("reserved_cash_units cannot exceed cash_units")

    @property
    def available_cash_units(self) -> int:
        return self.cash_units - self.reserved_cash_units


@dataclass(frozen=True)
class Position:
    account_id: str
    security: str
    total_qty: int
    sellable_qty: int
    avg_cost_price_units: int
    version: int = 0

    def __post_init__(self) -> None:
        _require_int(self.total_qty, "total_qty")
        _require_int(self.sellable_qty, "sellable_qty")
        _require_int(self.avg_cost_price_units, "avg_cost_price_units")
        _require_int(self.version, "version")
        if self.sellable_qty > self.total_qty:
            raise ValueError("sellable_qty cannot exceed total_qty")


@dataclass(frozen=True)
class PositionLot:
    lot_id: str
    account_id: str
    security: str
    acquired_trade_date: date
    sellable_from_trade_date: date
    original_qty: int
    remaining_qty: int
    cost_price_units: int

    def __post_init__(self) -> None:
        _require_int(self.original_qty, "original_qty", minimum=1)
        _require_int(self.remaining_qty, "remaining_qty")
        _require_int(self.cost_price_units, "cost_price_units")
        if self.remaining_qty > self.original_qty:
            raise ValueError("remaining_qty cannot exceed original_qty")
        if self.sellable_from_trade_date < self.acquired_trade_date:
            raise ValueError(
                "sellable_from_trade_date cannot precede acquired_trade_date"
            )


@dataclass(frozen=True)
class PortfolioIntent:
    intent_id: str
    account_id: str
    idempotency_key: str
    expected_ledger_version: int
    targets: Mapping[str, int]
    state: IntentState = IntentState.CREATED
    trading_day: Optional[date] = None
    execution_request: ExecutionRequest = ExecutionRequest()

    def __post_init__(self) -> None:
        _require_int(self.expected_ledger_version, "expected_ledger_version")
        for security, target_qty in self.targets.items():
            if type(security) is not str or not security:
                raise ValueError("target security cannot be empty")
            _require_int(target_qty, "target_qty")
        object.__setattr__(self, "targets", MappingProxyType(dict(self.targets)))
        if self.trading_day is not None and type(self.trading_day) is not date:
            raise TypeError("trading_day must be date")
        if type(self.execution_request) is not ExecutionRequest:
            raise TypeError("execution_request must be ExecutionRequest")


@dataclass(frozen=True)
class BrokerOrder:
    order_id: str
    account_id: str
    intent_id: Optional[str]
    client_tag: str
    security: str
    side: OrderSide
    requested_qty: int
    filled_qty: int
    state: OrderState
    trading_day: date
    broker_order_id: Optional[str] = None
    limit_price_units: Optional[int] = None

    def __post_init__(self) -> None:
        _require_int(self.requested_qty, "requested_qty", minimum=1)
        _require_int(self.filled_qty, "filled_qty")
        if self.filled_qty > self.requested_qty:
            raise ValueError("filled_qty cannot exceed requested_qty")
        if self.limit_price_units is not None:
            _require_int(self.limit_price_units, "limit_price_units", minimum=1)


@dataclass(frozen=True)
class BrokerFill:
    fill_id: str
    order_id: str
    fingerprint: str
    security: str
    side: OrderSide
    quantity: int
    price_units: int
    commission_units: Optional[int]
    tax_units: Optional[int]
    traded_at: datetime
    broker_trade_id: Optional[str] = None
    price_source: FillPriceSource = FillPriceSource.BROKER_TRADE
    price_known: bool = True

    def __post_init__(self) -> None:
        _require_int(self.quantity, "quantity", minimum=1)
        zero_fallback = self.price_source is FillPriceSource.ZERO_FALLBACK
        _require_int(self.price_units, "price_units", minimum=0 if zero_fallback else 1)
        if zero_fallback and self.price_units != 0:
            raise ValueError("ZERO_FALLBACK price must be zero")
        if self.commission_units is not None:
            _require_int(self.commission_units, "commission_units")
        if self.tax_units is not None:
            _require_int(self.tax_units, "tax_units")
        if not isinstance(self.price_source, FillPriceSource):
            raise ValueError("price_source must be a FillPriceSource")
        if type(self.price_known) is not bool:
            raise ValueError("price_known must be boolean")
        if self.price_known != (self.price_source is FillPriceSource.BROKER_TRADE):
            raise ValueError("price_known does not match price_source")
        object.__setattr__(self, "traded_at", as_shanghai_time(self.traded_at))


@dataclass(frozen=True)
class LedgerEntry:
    account_id: str
    event_seq: int
    entry_type: str
    amount_units: int
    cash_after_units: int
    reserved_after_units: int
    reference_type: Optional[str] = None
    reference_id: Optional[str] = None

    def __post_init__(self) -> None:
        _require_int(self.event_seq, "event_seq", minimum=1)
        if type(self.amount_units) is not int:
            raise ValueError("amount_units must be an integer")
        _require_int(self.cash_after_units, "cash_after_units")
        _require_int(self.reserved_after_units, "reserved_after_units")
        if self.reserved_after_units > self.cash_after_units:
            raise ValueError("reserved_after_units cannot exceed cash_after_units")


@dataclass(frozen=True)
class StrategyEvent:
    account_id: str
    event_seq: int
    event_type: str
    payload: Mapping[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        _require_int(self.event_seq, "event_seq", minimum=1)
        object.__setattr__(self, "payload", _freeze_json_mapping(self.payload))
        object.__setattr__(self, "created_at", as_shanghai_time(self.created_at))


@dataclass(frozen=True)
class ReconciliationResult:
    reconciliation_id: str
    physical_account_id: str
    state: ReconciliationState
    details: Mapping[str, object]
    broker_as_of: Optional[datetime] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", _freeze_json_mapping(self.details))
        if self.broker_as_of is not None:
            object.__setattr__(
                self,
                "broker_as_of",
                as_shanghai_time(self.broker_as_of),
            )
