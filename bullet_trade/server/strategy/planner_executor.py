"""Personal-account target planner and single-process order dispatcher."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Awaitable, Callable, Mapping, Optional, Tuple, Union, cast
from uuid import uuid4

from bullet_trade.core import pricing

from .capital import SQLiteCapitalService
from .domain import (
    MONEY_SCALE,
    NAV_SCALE,
    PRICE_SCALE,
    SHANGHAI_TZ,
    AccountStatus,
    BrokerOrder,
    IntentState,
    OrderSide,
    OrderState,
    PortfolioIntent,
)
from .fill_booking import SQLiteFillBookingService
from .execution import (
    ConditionalLimitExecution,
    ConditionalLimitPriceMode,
    ExecutionRequest,
    ExecutionType,
    FollowUpPolicy,
    LimitExecution,
    MarketExecution,
    MarketQuote,
    MarketableLimitExecution,
    RepricingPolicy,
    execution_request_from_wire,
    execution_request_to_wire,
)
from .idempotency import SQLiteOperationRepository
from .repository import RepositoryError, SQLiteStrategyRepository
from .schema import connect_database
from .valuation import MarketMark, PortfolioSnapshot


DatabasePath = Union[str, Path]
Weight = Union[str, int, float, Decimal]
_PRICE_CAGE_REJECTION_TOKENS = (
    "价格笼子",
    "有效申报价格",
    "有效竞价范围",
    "申报价格范围",
    "超出价格范围",
)


def is_price_cage_rejection(reason: object) -> bool:
    text = str(reason or "").strip().lower()
    return bool(text) and any(
        token.lower() in text for token in _PRICE_CAGE_REJECTION_TOKENS
    )


class TargetPlanningError(RepositoryError):
    pass


@dataclass(frozen=True)
class PlannerConfig:
    lot_size: int = 100
    cash_buffer_units: int = 1_000_000
    minimum_order_units: int = 0
    buy_fee_buffer_units: int = 50_000
    limit_price_offset_ppm: int = 2_000
    max_age: timedelta = timedelta(minutes=5)
    working_order_timeout: timedelta = timedelta(minutes=10)
    order_wait_timeout_seconds: float = 16.0
    trading_enabled: bool = False
    enabled_strategy_ids: Tuple[str, ...] = ()
    allow_buys: bool = True

    def __post_init__(self) -> None:
        if type(self.lot_size) is not int or self.lot_size <= 0:
            raise ValueError("lot_size must be positive")
        for name in ("cash_buffer_units", "minimum_order_units", "buy_fee_buffer_units"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError("{} must be non-negative".format(name))
        if (
            type(self.limit_price_offset_ppm) is not int
            or not 0 <= self.limit_price_offset_ppm <= 50_000
        ):
            raise ValueError("limit_price_offset_ppm must be between 0 and 50000")
        if self.max_age < timedelta(0):
            raise ValueError("max_age cannot be negative")
        if self.working_order_timeout <= timedelta(0):
            raise ValueError("working_order_timeout must be positive")
        if (
            type(self.order_wait_timeout_seconds) not in (int, float)
            or not 0 < self.order_wait_timeout_seconds <= 120
        ):
            raise ValueError("order_wait_timeout_seconds must be in (0, 120]")


@dataclass(frozen=True)
class PlannedOrder:
    order_id: str
    security: str
    side: OrderSide
    quantity: int
    limit_price_units: Optional[int]
    execution_type: ExecutionType = ExecutionType.LIMIT


@dataclass(frozen=True)
class IntentAdvanceResult:
    intent: PortfolioIntent
    orders: Tuple[PlannedOrder, ...]
    waiting_for_fills: bool
    waiting_for_trigger: bool = False


@dataclass(frozen=True)
class DispatchResult:
    order_id: str
    broker_order_id: Optional[str]
    unknown: bool
    error: str = ""


@dataclass(frozen=True)
class IntentOrderProgress:
    security: str
    side: OrderSide
    requested_qty: int
    filled_qty: int
    remaining_qty: int
    state: OrderState
    limit_price_units: Optional[int]


def _trade_value(price_units: int, quantity: int) -> int:
    return (price_units * quantity * MONEY_SCALE + PRICE_SCALE // 2) // PRICE_SCALE


def _weight_units(value: Weight) -> int:
    if isinstance(value, bool):
        raise TargetPlanningError("weight cannot be bool")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TargetPlanningError("invalid target weight") from exc
    if not number.is_finite() or not 0 <= number <= 1:
        raise TargetPlanningError("target weight must be between 0 and 1")
    return int((number * NAV_SCALE).quantize(Decimal("1")))


def _intent(row: sqlite3.Row) -> PortfolioIntent:
    payload = json.loads(row["targets_json"])
    targets = payload.get("target_quantities", payload)
    raw_execution = payload.get("execution_request")
    execution_request = (
        execution_request_from_wire(raw_execution)
        if isinstance(raw_execution, Mapping)
        else ExecutionRequest()
    )
    raw_trading_day = payload.get("trading_day")
    return PortfolioIntent(
        intent_id=row["intent_id"],
        account_id=row["strategy_account_id"],
        idempotency_key=row["idempotency_key"],
        expected_ledger_version=row["expected_ledger_version"],
        targets={key: int(value) for key, value in targets.items()},
        state=IntentState(row["state"]),
        trading_day=(
            datetime.fromisoformat(str(raw_trading_day)).date()
            if raw_trading_day
            else datetime.fromisoformat(row["created_at"]).date()
        ),
        execution_request=execution_request,
    )


class SQLiteTargetExecutionService:
    def __init__(self, database_path: DatabasePath, config: PlannerConfig, notification_handler=None):
        self.database_path = Path(database_path)
        self.config = config
        self._ledger = SQLiteStrategyRepository(database_path)
        self._capital = SQLiteCapitalService(database_path)
        self._operations = SQLiteOperationRepository(database_path)
        self._booking = SQLiteFillBookingService(database_path, notification_handler)

    def submit_target_weights(
        self,
        account_id: str,
        idempotency_key: str,
        weights: Mapping[str, Weight],
        snapshot: PortfolioSnapshot,
        marks: Mapping[str, MarketMark],
        now: Optional[datetime] = None,
        execution_request: Optional[ExecutionRequest] = None,
        quotes: Optional[Mapping[str, MarketQuote]] = None,
        security_names: Optional[Mapping[str, str]] = None,
    ) -> IntentAdvanceResult:
        request = execution_request or ExecutionRequest(
            style=LimitExecution(self.config.limit_price_offset_ppm)
        )
        if type(request) is not ExecutionRequest:
            raise TypeError("execution_request must be ExecutionRequest")
        current = now or datetime.now(SHANGHAI_TZ)
        self._expire_old_intents(account_id, current)
        normalized = {security: _weight_units(value) for security, value in weights.items()}
        if not normalized or any(not security for security in normalized):
            raise TargetPlanningError("target weights cannot be empty")
        if sum(normalized.values()) > NAV_SCALE:
            raise TargetPlanningError("target weights exceed 100%")
        existing = self._intent_by_key(account_id, idempotency_key)
        if existing is not None:
            intent_payload = self._intent_payload(existing.intent_id)
            stored = intent_payload["weights_ppm"]
            if stored != dict(sorted(normalized.items())):
                raise TargetPlanningError("idempotency key has different weights")
            if existing.execution_request != request:
                raise TargetPlanningError(
                    "idempotency key has different execution request"
                )
            return self.advance_intent(
                existing.intent_id, snapshot, marks, current, quotes=quotes
            )
        self._require_ready(account_id, snapshot, current)
        # Weights describe the whole portfolio. Enforce the cash buffer only
        # when buys are made; subtracting it here shrinks existing positions
        # and double-applies a strategy-level reserve such as DEPLOY_RATIO.
        investable = snapshot.total_assets_units
        quantities = {}
        for security, weight in normalized.items():
            mark = marks.get(security)
            if mark is None:
                raise TargetPlanningError("missing target mark: {}".format(security))
            raw = (investable * weight // NAV_SCALE) * PRICE_SCALE
            raw //= mark.price_units * MONEY_SCALE
            quantities[security] = raw // self.config.lot_size * self.config.lot_size
        intent = self._create_intent(
            account_id,
            idempotency_key,
            snapshot.ledger_version,
            normalized,
            quantities,
            marks,
            request,
            current,
            security_names or {},
        )
        return self.advance_intent(
            intent.intent_id, snapshot, marks, current, quotes=quotes
        )

    def advance_intent(
        self,
        intent_id: str,
        snapshot: PortfolioSnapshot,
        marks: Mapping[str, MarketMark],
        now: Optional[datetime] = None,
        quotes: Optional[Mapping[str, MarketQuote]] = None,
    ) -> IntentAdvanceResult:
        intent = self.get_intent(intent_id)
        if intent.state in (IntentState.COMPLETED, IntentState.CANCELED, IntentState.FAILED):
            return IntentAdvanceResult(intent, (), False)
        current = now or datetime.now(SHANGHAI_TZ)
        working = self._working_orders(intent.account_id)
        if intent.trading_day is not None and current.date() != intent.trading_day:
            if any(row["intent_id"] == intent.intent_id for row in working):
                return IntentAdvanceResult(intent, (), True)
            return IntentAdvanceResult(
                self._set_state(intent_id, IntentState.CANCELED), (), False
            )
        if any(row["state"] == OrderState.SUBMIT_UNKNOWN.value for row in working):
            raise TargetPlanningError("submit-unknown order blocks execution")
        intent_working = [
            row for row in working if row["intent_id"] == intent.intent_id
        ]
        if len(intent_working) != len(working):
            return IntentAdvanceResult(intent, (), True)
        intent_payload = self._intent_payload(intent.intent_id)
        if intent_payload.get("cancel_requested") is True:
            if working:
                return IntentAdvanceResult(intent, (), True)
            return IntentAdvanceResult(
                self._set_state(intent_id, IntentState.CANCELED), (), False
            )
        if any(row["side"] == OrderSide.SELL.value for row in intent_working):
            return IntentAdvanceResult(intent, (), True)
        positions = {item.security: item for item in snapshot.positions}
        if intent_working:
            uncovered_delta = False
            for security in sorted(set(intent.targets) | set(positions)):
                position = positions.get(security)
                current_qty = position.total_qty if position else 0
                delta = int(intent.targets.get(security, 0)) - current_qty
                if delta == 0:
                    continue
                side = OrderSide.SELL if delta < 0 else OrderSide.BUY
                if not any(
                    row["security"] == security
                    and row["side"] == side.value
                    for row in intent_working
                ):
                    uncovered_delta = True
                    break
            if not uncovered_delta:
                return IntentAdvanceResult(intent, (), True)
        self._require_ready(intent.account_id, snapshot, current)

        sells = []
        buys = []
        sell_pending = False
        waiting_for_trigger = False
        original_references = {
            security: int(value)
            for security, value in intent_payload.get(
                "reference_prices_units", {}
            ).items()
        }
        for security in sorted(set(intent.targets) | set(positions)):
            position = positions.get(security)
            current = position.total_qty if position else 0
            target = int(intent.targets.get(security, 0))
            delta = target - current
            if delta == 0:
                continue
            side = OrderSide.SELL if delta < 0 else OrderSide.BUY
            if delta < 0:
                sell_pending = True
            if any(
                row["security"] == security and row["side"] == side.value
                for row in intent_working
            ):
                continue
            execution_quote = (quotes or {}).get(security)
            if not self._rejection_allows_quote(
                intent.intent_id, security, execution_quote
            ):
                waiting_for_trigger = True
                continue
            if (
                intent.execution_request.follow_up is FollowUpPolicy.NONE
                and self._order_count(intent.intent_id, security, side) > 0
            ):
                continue
            mark = marks.get(security)
            if mark is None:
                raise TargetPlanningError("missing execution mark: {}".format(security))
            reference_price = mark.price_units
            if (
                intent.execution_request.repricing
                is RepricingPolicy.KEEP_ORIGINAL
            ):
                reference_price = original_references.get(
                    security, reference_price
                )
            prepared = self._prepare_execution(
                intent.execution_request,
                side,
                reference_price,
                execution_quote,
                security,
            )
            if prepared is None:
                waiting_for_trigger = True
                continue
            order_price, reservation_price, execution_type = prepared
            if delta < 0:
                desired = min(-delta, position.sellable_qty if position else 0)
                quantity = desired if target == 0 and desired == current else desired // self.config.lot_size * self.config.lot_size
                if quantity:
                    sells.append(
                        (
                            security,
                            quantity,
                            order_price,
                            reservation_price,
                            execution_type,
                        )
                    )
            elif self.config.allow_buys:
                quantity = delta // self.config.lot_size * self.config.lot_size
                if quantity:
                    buys.append(
                        (
                            security,
                            quantity,
                            order_price,
                            reservation_price,
                            execution_type,
                        )
                    )

        if sell_pending and intent_working:
            return IntentAdvanceResult(
                self._set_state(intent_id, IntentState.EXECUTING), (), True,
                waiting_for_trigger,
            )
        if sell_pending and not sells:
            return IntentAdvanceResult(
                self._set_state(intent_id, IntentState.EXECUTING), (), True,
                waiting_for_trigger,
            )
        selected = sells or self._affordable_buys(snapshot, buys)
        if not selected:
            if waiting_for_trigger or intent_working:
                return IntentAdvanceResult(
                    self._set_state(intent_id, IntentState.EXECUTING),
                    (),
                    bool(intent_working),
                    waiting_for_trigger,
                )
            return IntentAdvanceResult(self._set_state(intent_id, IntentState.COMPLETED), (), False)
        side = OrderSide.SELL if sells else OrderSide.BUY
        orders = tuple(
            self._enqueue(
                intent,
                side,
                security,
                quantity,
                order_price,
                reservation_price,
                execution_type,
                snapshot.as_of,
            )
            for (
                security,
                quantity,
                order_price,
                reservation_price,
                execution_type,
            ) in selected
        )
        return IntentAdvanceResult(
            self._set_state(intent_id, IntentState.EXECUTING),
            orders,
            True,
        )

    async def dispatch_next(
        self,
        submitter: Callable[[Mapping[str, object]], Awaitable[Mapping[str, object]]],
        strategy_account_id: Optional[str] = None,
        *,
        sellable_limits: Optional[Mapping[str, int]] = None,
    ) -> Optional[DispatchResult]:
        claim = self._operations.claim_next(strategy_account_id, sellable_limits=sellable_limits)
        if claim is None:
            return None
        envelope = json.loads(claim.payload_json)
        payload = dict(envelope["payload"])
        payload.setdefault("order_remark", envelope["client_tag"])
        payload.setdefault("wait_timeout", self.config.order_wait_timeout_seconds)
        account_id = self._order_account(claim.operation_id)
        self._operations.begin_submission(claim.outbox_id)
        try:
            response = dict(await submitter(payload))
            broker_id = str(response.get("order_id") or response.get("broker_order_id") or "").strip()
            if not broker_id:
                raise RuntimeError("broker response has no order id")
        except BaseException as exc:
            error = "{}: {}".format(type(exc).__name__, str(exc))
            try:
                self._operations.finish_submission(
                    claim.outbox_id, {"error": error}, unknown=True
                )
                self._booking.mark_order_submit_unknown(
                    account_id, claim.operation_id, error
                )
            except Exception as recovery_exc:
                raise TargetPlanningError(
                    "broker submission failed and ledger recovery also failed: "
                    "{}; recovery={}: {}".format(
                        error,
                        type(recovery_exc).__name__,
                        str(recovery_exc),
                    )
                ) from recovery_exc
            if not isinstance(exc, Exception):
                raise
            return DispatchResult(claim.operation_id, None, True, error)
        try:
            self._operations.finish_submission(claim.outbox_id, response)
            self._booking.mark_order_submitted(
                account_id, claim.operation_id, broker_id
            )
        except Exception as exc:
            raise TargetPlanningError(
                "broker accepted the order but ledger attachment failed: {}: {}".format(
                    type(exc).__name__, str(exc)
                )
            ) from exc
        return DispatchResult(claim.operation_id, broker_id, False)

    def get_intent(self, intent_id: str) -> PortfolioIntent:
        connection = connect_database(self.database_path)
        try:
            row = connection.execute("SELECT * FROM portfolio_intents WHERE intent_id = ?", (intent_id,)).fetchone()
            if row is None:
                raise TargetPlanningError("target intent not found")
            return _intent(cast(sqlite3.Row, row))
        finally:
            connection.close()

    def intent_orders(
        self, intent_id: str
    ) -> Tuple[IntentOrderProgress, ...]:
        self.get_intent(intent_id)
        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                "SELECT security, side, requested_qty, filled_qty, state, "
                "limit_price_units FROM strategy_orders "
                "WHERE intent_id = ? ORDER BY created_at, order_id",
                (intent_id,),
            ).fetchall()
        finally:
            connection.close()
        return tuple(
            IntentOrderProgress(
                security=str(row["security"]),
                side=OrderSide(row["side"]),
                requested_qty=int(row["requested_qty"]),
                filled_qty=int(row["filled_qty"]),
                remaining_qty=int(row["requested_qty"])
                - int(row["filled_qty"]),
                state=OrderState(row["state"]),
                limit_price_units=(
                    int(row["limit_price_units"])
                    if row["limit_price_units"] is not None
                    else None
                ),
            )
            for row in rows
        )

    def stale_broker_order_ids(
        self,
        account_id: str,
        now: Optional[datetime] = None,
    ) -> Tuple[str, ...]:
        current = now or datetime.now(SHANGHAI_TZ)
        result = []
        for row in self._working_orders(account_id):
            if not row["intent_id"]:
                continue
            intent = self.get_intent(str(row["intent_id"]))
            if (
                intent.execution_request.repricing
                is not RepricingPolicy.RECOMPUTE
            ):
                continue
            if row["state"] not in (
                OrderState.SUBMITTED.value,
                OrderState.PARTIALLY_FILLED.value,
            ) or not row["broker_order_id"]:
                continue
            submitted_at = row["submitted_at"] or row["updated_at"]
            if not submitted_at:
                continue
            timestamp = datetime.fromisoformat(submitted_at)
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                timestamp = timestamp.replace(tzinfo=SHANGHAI_TZ)
            if current - timestamp >= self.config.working_order_timeout:
                result.append(str(row["broker_order_id"]))
        return tuple(result)

    def _create_intent(
        self,
        account_id,
        key,
        version,
        weights,
        quantities,
        marks,
        request,
        now,
        security_names,
    ):
        if not key:
            raise TargetPlanningError("idempotency key cannot be empty")
        payload = {
            "weights_ppm": dict(sorted(weights.items())),
            "target_quantities": dict(sorted(quantities.items())),
            "reference_prices_units": {
                security: mark.price_units
                for security, mark in sorted(marks.items())
            },
            "trading_day": now.date().isoformat(),
            "execution_request": execution_request_to_wire(request),
            "security_names": {
                str(security): str(name)
                for security, name in sorted(security_names.items())
                if name
            },
        }
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        connection = connect_database(self.database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM portfolio_intents WHERE strategy_account_id = ? AND idempotency_key = ?",
                (account_id, key),
            ).fetchone()
            if row is not None:
                if json.loads(row["targets_json"]).get("weights_ppm") != payload["weights_ppm"]:
                    raise TargetPlanningError("idempotency key has different weights")
                connection.commit()
                return _intent(cast(sqlite3.Row, row))
            active = connection.execute(
                """
                SELECT intent_id FROM portfolio_intents
                WHERE strategy_account_id = ?
                  AND state IN ('CREATED','PLANNED','EXECUTING','RECONCILING')
                LIMIT 1
                """,
                (account_id,),
            ).fetchone()
            if active is not None:
                raise TargetPlanningError("another target intent is still active")
            intent_id = str(uuid4())
            timestamp = datetime.now(SHANGHAI_TZ).isoformat()
            connection.execute(
                """
                INSERT INTO portfolio_intents(
                    intent_id, strategy_account_id, idempotency_key,
                    expected_ledger_version, state, targets_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'PLANNED', ?, ?, ?)
                """,
                (intent_id, account_id, key, version, text, timestamp, timestamp),
            )
            row = connection.execute("SELECT * FROM portfolio_intents WHERE intent_id = ?", (intent_id,)).fetchone()
            connection.commit()
            return _intent(cast(sqlite3.Row, row))
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _enqueue(
        self,
        intent,
        side,
        security,
        quantity,
        order_price,
        reservation_price,
        execution_type,
        as_of,
    ):
        self._clear_execution_rejection(intent.intent_id, security)
        retry = self._order_count(intent.intent_id, security, side)
        key = "{}:{}:{}:{}".format(intent.intent_id, security, side.value, retry)
        broker_style = {"type": "market"} if execution_type is ExecutionType.MARKET else {
            "type": "limit",
            "price": str(Decimal(order_price) / PRICE_SCALE),
        }
        if execution_type is ExecutionType.MARKET:
            broker_style["protect_price"] = str(
                Decimal(reservation_price) / PRICE_SCALE
            )
        payload = {
            "intent_id": intent.intent_id,
            "security": security,
            "side": side.value,
            "amount": quantity,
            "style": broker_style,
        }
        operation = self._operations.create_operation(
            intent.account_id, "broker.place_order", key, payload, topic="strategy.order"
        ).operation
        if self._find_order(operation.operation_id) is None:
            if side is OrderSide.BUY:
                account = self._ledger.get_strategy_account(intent.account_id)
                self._capital.reserve_cash(
                    intent.account_id,
                    _trade_value(reservation_price, quantity)
                    + self.config.buy_fee_buffer_units,
                    account.ledger_version,
                    operation.operation_id,
                )
            self._booking.register_order(
                BrokerOrder(
                    order_id=operation.operation_id,
                    account_id=intent.account_id,
                    intent_id=intent.intent_id,
                    client_tag=operation.client_tag,
                    security=security,
                    side=side,
                    requested_qty=quantity,
                    filled_qty=0,
                    state=OrderState.PENDING_SUBMIT,
                    trading_day=as_of.date(),
                    limit_price_units=order_price,
                )
            )
        return PlannedOrder(
            operation.operation_id,
            security,
            side,
            quantity,
            order_price,
            execution_type,
        )

    def _affordable_buys(self, snapshot, buys):
        available = max(0, snapshot.available_cash_units - self.config.cash_buffer_units)
        result = []
        for security, desired, order_price, reservation_price, execution_type in buys:
            lot_value = _trade_value(reservation_price, self.config.lot_size)
            quantity = min(desired, max(0, available - self.config.buy_fee_buffer_units) // lot_value * self.config.lot_size)
            value = _trade_value(reservation_price, quantity)
            if quantity and value >= self.config.minimum_order_units:
                result.append(
                    (
                        security,
                        quantity,
                        order_price,
                        reservation_price,
                        execution_type,
                    )
                )
                available -= value + self.config.buy_fee_buffer_units
        return result

    @staticmethod
    def _boundary_price(mark_price_units, side, offset):
        if side is OrderSide.BUY:
            return (mark_price_units * (NAV_SCALE + offset) + NAV_SCALE - 1) // NAV_SCALE
        return max(1, mark_price_units * (NAV_SCALE - offset) // NAV_SCALE)

    def _prepare_execution(self, request, side, reference_price, quote, security):
        style = (
            request.sell_style
            if side is OrderSide.SELL and request.sell_style is not None
            else request.style
        )
        if isinstance(style, MarketExecution):
            protect = self._boundary_price(
                reference_price, side, style.protect_price_band_ppm
            )
            return None, protect, style.execution_type
        price = self._boundary_price(reference_price, side, style.price_band_ppm)
        if isinstance(style, ConditionalLimitExecution):
            if quote is None:
                return None
            counterparty = (
                quote.ask_price_units if side is OrderSide.BUY else quote.bid_price_units
            )
            if counterparty is None or (
                counterparty > price if side is OrderSide.BUY else counterparty < price
            ):
                return None
            if style.price_mode is ConditionalLimitPriceMode.COUNTERPARTY:
                price = counterparty
        elif not isinstance(style, (LimitExecution, MarketableLimitExecution)):
            raise TargetPlanningError("unsupported execution style")
        if not self._limit_can_submit(security, side, price, quote):
            return None
        return price, price, style.execution_type

    @staticmethod
    def _limit_can_submit(security, side, price, quote):
        instrument_type = quote.instrument_type if quote is not None else None
        if pricing.infer_price_cage_board(security, instrument_type) is None:
            return True
        if quote is None:
            return False
        return pricing.limit_price_within_cage(
            security,
            side is OrderSide.BUY,
            price / PRICE_SCALE,
            bid_price=(quote.bid_price_units or 0) / PRICE_SCALE,
            ask_price=(quote.ask_price_units or 0) / PRICE_SCALE,
            last_price=(quote.last_price_units or 0) / PRICE_SCALE,
            preclose=(quote.preclose_units or 0) / PRICE_SCALE,
            instrument_type=instrument_type,
            continuous_auction=quote.continuous_auction is not False,
        )

    def _expire_old_intents(self, account_id, now):
        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                """
                SELECT * FROM portfolio_intents
                WHERE strategy_account_id = ?
                  AND state IN ('CREATED','PLANNED','EXECUTING','RECONCILING')
                """,
                (account_id,),
            ).fetchall()
            for row in rows:
                intent = _intent(cast(sqlite3.Row, row))
                if intent.trading_day == now.date():
                    continue
                working = connection.execute(
                    """
                    SELECT 1 FROM strategy_orders
                    WHERE intent_id = ? AND state IN (
                        'PENDING_SUBMIT','SUBMIT_UNKNOWN','SUBMITTED','PARTIALLY_FILLED'
                    ) LIMIT 1
                    """,
                    (intent.intent_id,),
                ).fetchone()
                if working is not None:
                    raise TargetPlanningError(
                        "previous trading day order still needs reconciliation"
                    )
                connection.execute(
                    "UPDATE portfolio_intents SET state = 'CANCELED', updated_at = ? WHERE intent_id = ?",
                    (now.isoformat(), intent.intent_id),
                )
            connection.commit()
        finally:
            connection.close()

    def active_intents(
        self, account_id: Optional[str] = None
    ) -> Tuple[PortfolioIntent, ...]:
        connection = connect_database(self.database_path)
        try:
            sql = (
                "SELECT * FROM portfolio_intents WHERE state IN "
                "('CREATED','PLANNED','EXECUTING','RECONCILING')"
            )
            params = ()
            if account_id is not None:
                sql += " AND strategy_account_id = ?"
                params = (account_id,)
            sql += " ORDER BY created_at"
            rows = connection.execute(sql, params).fetchall()
            return tuple(_intent(cast(sqlite3.Row, row)) for row in rows)
        finally:
            connection.close()

    def expired_intents(self, before: date) -> Tuple[PortfolioIntent, ...]:
        """Return expired active intents and terminal intents with working orders."""

        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                """
                SELECT DISTINCT intent.*
                FROM portfolio_intents AS intent
                LEFT JOIN strategy_orders AS order_row
                  ON order_row.intent_id = intent.intent_id
                 AND order_row.state IN (
                    'PENDING_SUBMIT','SUBMIT_UNKNOWN','SUBMITTED','PARTIALLY_FILLED'
                 )
                WHERE intent.state IN ('CREATED','PLANNED','EXECUTING','RECONCILING')
                   OR order_row.order_id IS NOT NULL
                ORDER BY intent.created_at
                """
            ).fetchall()
        finally:
            connection.close()
        return tuple(
            intent
            for intent in (_intent(cast(sqlite3.Row, row)) for row in rows)
            if intent.trading_day is not None and intent.trading_day < before
        )

    def finalize_expired_orders(
        self, intent_id: str, before: date
    ) -> Tuple[str, ...]:
        """Close the unfilled remainder of previous-day DAY orders locally."""

        intent = self.get_intent(intent_id)
        if intent.trading_day is None or intent.trading_day >= before:
            raise TargetPlanningError("intent has not expired")
        rows = tuple(
            row
            for row in self._working_orders(intent.account_id)
            if row["intent_id"] == intent_id
        )
        finalized = []
        for row in rows:
            account = self._ledger.get_strategy_account(intent.account_id)
            self._booking.finalize_order(
                intent.account_id,
                str(row["order_id"]),
                OrderState.CANCELED,
                account.ledger_version,
            )
            finalized.append(str(row["order_id"]))
        return tuple(finalized)

    def reference_prices(self, intent_id: str) -> Mapping[str, int]:
        payload = self._intent_payload(intent_id)
        return {
            str(security): int(value)
            for security, value in payload.get(
                "reference_prices_units", {}
            ).items()
        }

    def record_rejected_orders(
        self,
        intent_id: str,
        rejected_orders,
        quotes: Mapping[str, MarketQuote],
    ) -> Tuple[Mapping[str, object], ...]:
        """Persist retry disposition for newly observed broker rejections."""

        intent = self.get_intent(intent_id)
        payload = self._intent_payload(intent_id)
        handled = set(payload.get("handled_rejection_order_ids", ()))
        blocks = dict(payload.get("execution_rejections", {}))
        recorded = []
        for item in rejected_orders:
            if not isinstance(item, Mapping):
                continue
            order_id = str(item.get("local_order_id") or "").strip()
            security = str(item.get("security") or "").strip()
            if (
                not order_id
                or not security
                or order_id in handled
                or str(item.get("intent_id") or "") != intent_id
            ):
                continue
            reason = str(item.get("reason") or "").strip()
            side = self._order_side(order_id)
            style = (
                intent.execution_request.sell_style
                if side is OrderSide.SELL
                and intent.execution_request.sell_style is not None
                else intent.execution_request.style
            )
            quote = quotes.get(security)
            retryable = (
                isinstance(style, (LimitExecution, MarketableLimitExecution, ConditionalLimitExecution))
                and pricing.infer_price_cage_board(
                    security,
                    quote.instrument_type if quote is not None else None,
                )
                is not None
                and is_price_cage_rejection(reason)
            )
            blocks[security] = {
                "local_order_id": order_id,
                "reason": reason,
                "retryable": retryable,
                "retry_after_quote": (
                    quote.as_of.isoformat() if quote is not None else None
                ),
            }
            handled.add(order_id)
            recorded.append(
                {
                    "security": security,
                    "reason": reason,
                    "retryable": retryable,
                }
            )
        if not recorded:
            return ()
        payload["handled_rejection_order_ids"] = sorted(handled)
        payload["execution_rejections"] = blocks
        self._write_intent_payload(intent_id, payload)
        return tuple(recorded)

    def _rejection_allows_quote(
        self, intent_id: str, security: str, quote: Optional[MarketQuote]
    ) -> bool:
        item = self._intent_payload(intent_id).get(
            "execution_rejections", {}
        ).get(security)
        if not isinstance(item, Mapping):
            return True
        if item.get("retryable") is not True or quote is None:
            return False
        raw_after = item.get("retry_after_quote")
        if not raw_after:
            return True
        try:
            retry_after = datetime.fromisoformat(str(raw_after))
        except ValueError:
            return False
        if retry_after.tzinfo is None or retry_after.utcoffset() is None:
            retry_after = retry_after.replace(tzinfo=SHANGHAI_TZ)
        return quote.as_of > retry_after

    def _clear_execution_rejection(
        self, intent_id: str, security: str
    ) -> None:
        payload = self._intent_payload(intent_id)
        blocks = dict(payload.get("execution_rejections", {}))
        if security not in blocks:
            return
        blocks.pop(security, None)
        payload["execution_rejections"] = blocks
        self._write_intent_payload(intent_id, payload)

    def _write_intent_payload(self, intent_id: str, payload) -> None:
        connection = connect_database(self.database_path)
        try:
            connection.execute(
                "UPDATE portfolio_intents SET targets_json = ?, updated_at = ? "
                "WHERE intent_id = ?",
                (
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    datetime.now(SHANGHAI_TZ).isoformat(),
                    intent_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def _order_side(self, order_id: str) -> OrderSide:
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT side FROM strategy_orders WHERE order_id = ?",
                (order_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise TargetPlanningError("rejected order is missing")
        return OrderSide(str(row[0]))

    def quote_triggers_intent(
        self,
        intent_id: str,
        security: str,
        quote: MarketQuote,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return whether one quote can turn a waiting conditional target into an order.

        This deliberately reads only StrategyLedger state.  The comparatively
        expensive broker refresh is performed after the condition is met.
        """

        intent = self.get_intent(intent_id)
        current = now or datetime.now(SHANGHAI_TZ)
        if (
            intent.state
            in (IntentState.COMPLETED, IntentState.CANCELED, IntentState.FAILED)
            or intent.trading_day != current.date()
            or security not in self.reference_prices(intent_id)
            or self.intent_cancel_requested(intent_id)
        ):
            return False
        working = self._working_orders(intent.account_id)
        if any(
            row["intent_id"] != intent.intent_id
            or row["state"] == OrderState.SUBMIT_UNKNOWN.value
            or row["side"] == OrderSide.SELL.value
            for row in working
        ):
            return False
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT total_qty FROM positions "
                "WHERE strategy_account_id = ? AND security = ?",
                (intent.account_id, security),
            ).fetchone()
        finally:
            connection.close()
        position_qty = int(row[0]) if row is not None else 0
        target_qty = int(intent.targets.get(security, 0))
        delta = target_qty - position_qty
        if delta == 0:
            return False
        side = OrderSide.BUY if delta > 0 else OrderSide.SELL
        if working and side is OrderSide.SELL:
            return False
        if any(row["security"] == security for row in working):
            return False
        if not self._rejection_allows_quote(intent_id, security, quote):
            return False
        style = (
            intent.execution_request.sell_style
            if side is OrderSide.SELL
            and intent.execution_request.sell_style is not None
            else intent.execution_request.style
        )
        if (
            intent.execution_request.follow_up is FollowUpPolicy.NONE
            and self._order_count(intent.intent_id, security, side) > 0
        ):
            return False
        reference_price = self.reference_prices(intent_id).get(security)
        if reference_price is None:
            return False
        if not isinstance(style, (LimitExecution, MarketableLimitExecution, ConditionalLimitExecution)):
            return False
        if (
            intent.execution_request.repricing
            is RepricingPolicy.RECOMPUTE
            and quote.last_price_units is not None
        ):
            reference_price = quote.last_price_units
        return (
            self._prepare_execution(
                intent.execution_request,
                side,
                reference_price,
                quote,
                security,
            )
            is not None
        )

    def cancel_intent_if_idle(self, intent_id: str) -> PortfolioIntent:
        """Cancel an active intent only when no broker order remains working."""

        intent = self.get_intent(intent_id)
        working = [
            row
            for row in self._working_orders(intent.account_id)
            if row["intent_id"] == intent_id
        ]
        if working:
            raise TargetPlanningError(
                "working orders must finish cancellation before intent cancellation"
            )
        if intent.state in (
            IntentState.COMPLETED,
            IntentState.CANCELED,
            IntentState.FAILED,
        ):
            return intent
        return self._set_state(intent_id, IntentState.CANCELED)

    def request_intent_cancellation(self, intent_id: str) -> PortfolioIntent:
        """Persist cancellation intent so callbacks can never resume its target."""

        intent = self.get_intent(intent_id)
        if intent.state in (
            IntentState.COMPLETED,
            IntentState.CANCELED,
            IntentState.FAILED,
        ):
            return intent
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT targets_json FROM portfolio_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise TargetPlanningError("target intent not found")
            payload = json.loads(row[0])
            payload["cancel_requested"] = True
            connection.execute(
                "UPDATE portfolio_intents SET targets_json = ?, updated_at = ? "
                "WHERE intent_id = ?",
                (
                    json.dumps(
                        payload, sort_keys=True, separators=(",", ":")
                    ),
                    datetime.now(SHANGHAI_TZ).isoformat(),
                    intent_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        return self.get_intent(intent_id)

    def intent_cancel_requested(self, intent_id: str) -> bool:
        return self._intent_payload(intent_id).get("cancel_requested") is True

    def cancelable_broker_order_ids(
        self, intent_id: str
    ) -> Tuple[str, ...]:
        """Return known working broker orders belonging to one intent."""

        intent = self.get_intent(intent_id)
        result = []
        for row in self._working_orders(intent.account_id):
            if row["intent_id"] != intent_id:
                continue
            if row["state"] in (
                OrderState.PENDING_SUBMIT.value,
                OrderState.SUBMIT_UNKNOWN.value,
            ):
                raise TargetPlanningError(
                    "order submission is unresolved; cancellation is blocked"
                )
            if row["broker_order_id"]:
                result.append(str(row["broker_order_id"]))
        return tuple(result)

    def _require_ready(self, account_id, snapshot, now):
        if not self.config.trading_enabled:
            raise TargetPlanningError("global trading switch is disabled")
        account = self._ledger.get_strategy_account(account_id)
        if account.strategy_id not in self.config.enabled_strategy_ids:
            raise TargetPlanningError("strategy is not in trading allowlist")
        if account.status is not AccountStatus.ACTIVE or snapshot.account_id != account_id:
            raise TargetPlanningError("strategy account is not ready")
        if snapshot.ledger_version != account.ledger_version:
            raise TargetPlanningError("portfolio snapshot ledger version is stale")
        if snapshot.as_of > now + timedelta(seconds=5):
            raise TargetPlanningError("portfolio snapshot is from the future")
        if now - snapshot.as_of > self.config.max_age:
            raise TargetPlanningError("portfolio snapshot is stale")
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT state, broker_as_of FROM reconciliation_runs "
                "WHERE physical_account_id = ? AND strategy_account_id = ? "
                "ORDER BY started_at DESC LIMIT 1",
                (account.physical_account_id, account_id),
            ).fetchone()
        finally:
            connection.close()
        if row is None or row["state"] != "READY":
            raise TargetPlanningError("latest reconciliation is not READY")
        if now - datetime.fromisoformat(row["broker_as_of"]) > self.config.max_age:
            raise TargetPlanningError("latest reconciliation is stale")

    def _working_orders(self, account_id):
        connection = connect_database(self.database_path)
        try:
            return connection.execute(
                "SELECT * FROM strategy_orders WHERE strategy_account_id = ? AND state IN ('PENDING_SUBMIT','SUBMIT_UNKNOWN','SUBMITTED','PARTIALLY_FILLED')",
                (account_id,),
            ).fetchall()
        finally:
            connection.close()

    def _intent_by_key(self, account_id, key):
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT * FROM portfolio_intents WHERE strategy_account_id = ? AND idempotency_key = ?",
                (account_id, key),
            ).fetchone()
            return _intent(cast(sqlite3.Row, row)) if row is not None else None
        finally:
            connection.close()

    def _intent_payload(self, intent_id):
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT targets_json FROM portfolio_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise TargetPlanningError("target intent not found")
            return json.loads(row["targets_json"])
        finally:
            connection.close()

    def _find_order(self, order_id):
        connection = connect_database(self.database_path)
        try:
            return connection.execute("SELECT * FROM strategy_orders WHERE order_id = ?", (order_id,)).fetchone()
        finally:
            connection.close()

    def _order_count(self, intent_id, security, side):
        connection = connect_database(self.database_path)
        try:
            return connection.execute(
                "SELECT COUNT(*) FROM strategy_orders WHERE intent_id = ? AND security = ? AND side = ?",
                (intent_id, security, side.value),
            ).fetchone()[0]
        finally:
            connection.close()

    def _order_account(self, order_id):
        row = self._find_order(order_id)
        if row is None:
            raise TargetPlanningError("outbox order is missing")
        return cast(str, row["strategy_account_id"])

    def _set_state(self, intent_id, state):
        connection = connect_database(self.database_path)
        try:
            connection.execute(
                "UPDATE portfolio_intents SET state = ?, updated_at = ? WHERE intent_id = ?",
                (state.value, datetime.now(SHANGHAI_TZ).isoformat(), intent_id),
            )
            connection.commit()
        finally:
            connection.close()
        return self.get_intent(intent_id)
