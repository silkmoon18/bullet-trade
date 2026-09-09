"""Minimal StrategyLedger RPC facade for one personal QMT account."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union, cast

from bullet_trade.core.pricing import infer_price_cage_board

from .broker_contract import BrokerCapabilityProfile
from ..feishu_notifier import (
    TargetBuyPlanItem,
    TargetBuyPlanNotification,
    reconciliation_notification,
)
from .capital import BrokerCashMismatchError, SQLiteCapitalService
from .domain import (
    MONEY_SCALE,
    NAV_SCALE,
    PRICE_SCALE,
    SHANGHAI_TZ,
    PortfolioIntent,
    UnpricedFillPolicy,
    money_to_units,
    price_to_units,
)
from .execution import (
    ConditionalLimitExecution,
    ExecutionRequest,
    MarketQuote,
    LimitExecution,
    MarketableLimitExecution,
    RepricingPolicy,
    execution_request_from_wire,
    execution_request_to_wire,
)
from .planner_executor import (
    PlannerConfig,
    SQLiteTargetExecutionService,
    TargetPlanningError,
)
from .reconciliation import SQLiteReconciliationService, collect_async_broker_snapshot
from .repository import (
    AccountNotFoundError,
    LedgerInvariantError,
    SQLiteStrategyRepository,
)
from .schema import connect_database
from .valuation import MarketMark, SQLiteValuationService


DatabasePath = Union[str, Path]
logger = logging.getLogger(__name__)
EXECUTION_QUOTE_HEARTBEAT_INTERVAL = timedelta(minutes=1)
VALUATION_MARK_MAX_IDLE_AGE = timedelta(days=10)


def _uses_quote_execution(
    request: ExecutionRequest, security: Optional[str] = None
) -> bool:
    for style in (request.style, request.sell_style):
        if isinstance(style, ConditionalLimitExecution):
            return True
        if isinstance(style, (LimitExecution, MarketableLimitExecution)) and (
            security is None or infer_price_cage_board(security) is not None
        ):
            return True
    return False


@dataclass(frozen=True)
class StrategyAPIConfig:
    database_path: DatabasePath
    trading_enabled: bool = False
    enabled_strategy_ids: Tuple[str, ...] = ()
    allow_buys: bool = True
    max_age: timedelta = timedelta(minutes=5)
    cash_buffer_units: int = money_to_units("100")
    minimum_order_units: int = 0
    buy_fee_buffer_units: int = money_to_units("5")
    unpriced_fill_policy: UnpricedFillPolicy = UnpricedFillPolicy.STRICT


@dataclass(frozen=True)
class ExpiredIntentSweepResult:
    expired: int = 0
    cancel_requests: int = 0
    canceled: int = 0
    pending: int = 0
    unbound: int = 0
    errors: int = 0


def _json_value(value: object) -> object:
    if is_dataclass(value):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _money(units: int) -> float:
    return units / MONEY_SCALE


def _price(units: int) -> float:
    return units / PRICE_SCALE


class SQLiteStrategyAPI:
    """Thin orchestration layer reused by the existing TCP server."""

    def __init__(
        self,
        config: StrategyAPIConfig,
        broker: object,
        capabilities: BrokerCapabilityProfile,
        data_provider: Optional[object] = None,
        notification_handler=None,
        durable_broker_history: bool = False,
    ) -> None:
        self.config = config
        self.database_path = Path(config.database_path)
        self.broker = broker
        self.data_provider = data_provider
        self.notification_handler = notification_handler
        self.startup_ready = False
        self._event_loop = None
        self._runtime_bindings = {}
        self._quote_cache = {}
        self._quote_last_received_at = {}
        self._quote_last_log_at = {}
        self._seen_tick_symbols = set()
        self._rejected_tick_symbols = set()
        self._resume_locks = {}
        self._sellable_waits = {}
        self._dispatch_lock = asyncio.Lock()
        self._background_tasks = set()
        self.repository = SQLiteStrategyRepository(self.database_path)
        self.repository.initialize()
        self.capital = SQLiteCapitalService(self.database_path)
        self.reconciliation = SQLiteReconciliationService(
            self.database_path,
            capabilities,
            notification_handler,
            require_verified_capabilities=config.trading_enabled,
            durable_broker_history=durable_broker_history,
            unknown_fee_tolerance_units_per_order=config.buy_fee_buffer_units,
            unpriced_fill_policy=config.unpriced_fill_policy,
        )
        self.valuation = SQLiteValuationService(self.database_path)
        self.planner = SQLiteTargetExecutionService(
            self.database_path,
            PlannerConfig(
                trading_enabled=config.trading_enabled,
                enabled_strategy_ids=config.enabled_strategy_ids,
                allow_buys=config.allow_buys,
                max_age=config.max_age,
                cash_buffer_units=config.cash_buffer_units,
                minimum_order_units=config.minimum_order_units,
                buy_fee_buffer_units=config.buy_fee_buffer_units,
            ),
            notification_handler,
        )
        add_tick_listener = getattr(
            self.data_provider, "add_tick_listener", None
        )
        if callable(add_tick_listener):
            add_tick_listener(self._on_tick_event)
        add_broker_listener = getattr(
            self.broker, "add_event_listener", None
        )
        if callable(add_broker_listener):
            add_broker_listener(self._on_broker_event)

    async def ensure_account(
        self,
        account_context: object,
        account_key: str,
        payload: Mapping[str, object],
    ) -> Dict[str, object]:
        strategy_id = self._strategy_id(payload)
        self._bind_runtime(strategy_id, account_context, account_key)
        initial = payload.get("initial_capital", "10000")
        initial_units = money_to_units(str(initial) if type(initial) is float else initial)  # type: ignore[arg-type]
        physical_id = self._physical_id(account_key)
        broker_snapshot = await collect_async_broker_snapshot(
            cast(Any, self.broker), account_context
        )
        self._ensure_physical_account(physical_id, account_context)
        try:
            account = self.repository.get_strategy_account(strategy_id)
            created = False
        except AccountNotFoundError:
            try:
                self.capital.calibrate_broker_available_cash(
                    physical_id, broker_snapshot.available_cash_units
                )
            except BrokerCashMismatchError as exc:
                raise BrokerCashMismatchError(
                    "{}; requested_initial_capital={:.4f}, new_strategy_id={}".format(
                        exc,
                        initial_units / MONEY_SCALE,
                        strategy_id,
                    )
                ) from exc
            try:
                ensured = self.capital.ensure_strategy_account(
                    strategy_id, strategy_id, physical_id, initial_units
                )
            except LedgerInvariantError as exc:
                raise LedgerInvariantError(
                    "{}; broker_available_cash={:.4f}, new_strategy_id={}".format(
                        exc,
                        broker_snapshot.available_cash_units / MONEY_SCALE,
                        strategy_id,
                    )
                ) from exc
            account, created = ensured.account, ensured.created
        result = await self._synchronize(
            strategy_id, physical_id, broker_snapshot
        )
        account = self.repository.get_strategy_account(strategy_id)
        self.startup_ready = result.state.value == "READY"
        return {
            "account": self._account_payload(account),
            "created": created,
            "broker_available_cash": _money(broker_snapshot.available_cash_units),
            "broker_positions": _json_value(broker_snapshot.positions),
            "reconciliation": _json_value(result),
        }

    async def get_snapshot(
        self,
        account_context: object,
        account_key: str,
        payload: Mapping[str, object],
    ) -> Dict[str, object]:
        strategy_id = self._strategy_id(payload)
        self._bind_runtime(strategy_id, account_context, account_key)
        snapshot, reconciliation, marks = await self._refresh(
            account_context, account_key, strategy_id, payload
        )
        result = self._snapshot_payload(snapshot)
        result["reconciliation"] = _json_value(reconciliation)
        result["marks"] = {
            security: {"price": _price(mark.price_units), "as_of": mark.as_of.isoformat()}
            for security, mark in marks.items()
        }
        return result

    async def submit_targets(
        self,
        account_context: object,
        account_key: str,
        payload: Mapping[str, object],
    ) -> Dict[str, object]:
        strategy_id = self._strategy_id(payload)
        self._bind_runtime(strategy_id, account_context, account_key)
        key = str(payload.get("idempotency_key") or "").strip()
        weights = payload.get("weights")
        if not key or not isinstance(weights, Mapping):
            raise ValueError("idempotency_key and weights are required")
        raw_security_names = payload.get("security_names", {})
        if not isinstance(raw_security_names, Mapping):
            raise ValueError("security_names must be an object")
        security_names: Dict[str, str] = {}
        for raw_security, raw_name in raw_security_names.items():
            if not isinstance(raw_name, str):
                continue
            security = str(raw_security).strip()
            name = raw_name.strip()
            if security and name:
                security_names[security] = name[:100]
        raw_execution = payload.get("execution")
        if raw_execution is None:
            execution_request = ExecutionRequest()
        elif isinstance(raw_execution, Mapping):
            execution_request = execution_request_from_wire(raw_execution)
        else:
            raise ValueError("execution must be an object")
        if not self._can_dispatch_now():
            raise RuntimeError("当前非发单时段，目标未提交；仅在工作日09:30-11:30、13:00-15:00发单")
        snapshot, reconciliation, marks = await self._refresh(
            account_context, account_key, strategy_id, payload, require_ready=True
        )
        if any(_uses_quote_execution(execution_request, security) for security in marks):
            replace = getattr(
                self.data_provider, "replace_execution_quotes", None
            )
            subscribe = getattr(
                self.data_provider, "subscribe_execution_quotes", None
            )
            if not callable(replace) and not callable(subscribe):
                raise RuntimeError(
                    "current data adapter has no native quote callback support"
                )
        cancel_requested_order_ids = []
        for broker_order_id in self.planner.stale_broker_order_ids(strategy_id):
            await cast(Any, self.broker).cancel_order(
                account_context, broker_order_id
            )
            cancel_requested_order_ids.append(broker_order_id)
        if cancel_requested_order_ids:
            snapshot, reconciliation, marks = await self._refresh(
                account_context, account_key, strategy_id, payload, require_ready=True
            )
        quotes = await self._execution_quotes(
            execution_request, tuple(marks), snapshot.as_of
        )
        # A repeated submit can be the first query to observe a broker
        # rejection. Persist its disposition before planning any remainder.
        for intent in self.planner.active_intents(strategy_id):
            self._record_reconciliation_rejections(
                intent.intent_id, reconciliation, quotes
            )
        advance = self.planner.submit_target_weights(
            strategy_id,
            key,
            cast(Any, weights),
            snapshot,
            marks,
            snapshot.as_of,
            execution_request=execution_request,
            quotes=quotes,
            security_names=security_names,
        )
        if _uses_quote_execution(execution_request):
            await self._sync_quote_subscriptions(strategy_id)
        dispatched = []

        async def submitter(order_payload: Mapping[str, object]) -> Mapping[str, object]:
            return await cast(Any, self.broker).place_order(
                account_context, dict(order_payload)
            )

        dispatched.extend(await self._dispatch_pending(submitter, strategy_id))

        # Pick up immediate fills/rejections and make the returned view real.
        refreshed, reconciliation, _ = await self._refresh(
            account_context, account_key, strategy_id, payload
        )
        self._record_reconciliation_rejections(
            advance.intent.intent_id, reconciliation, quotes
        )
        await self._sync_quote_subscriptions(strategy_id)
        return {
            "intent": _json_value(self.planner.get_intent(advance.intent.intent_id)),
            "planned_orders": _json_value(advance.orders),
            "dispatched_orders": _json_value(dispatched),
            "cancel_requested_order_ids": cancel_requested_order_ids,
            "snapshot": self._snapshot_payload(refreshed),
            "reconciliation": _json_value(reconciliation),
        }

    async def startup_check(self, account_context: object, account_key: str) -> bool:
        physical_id = self._physical_id(account_key)
        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                "SELECT strategy_account_id, strategy_id FROM strategy_accounts "
                "WHERE physical_account_id = ?",
                (physical_id,),
            ).fetchall()
        finally:
            connection.close()
        if not rows:
            self.startup_ready = False
            return False
        account_ids = tuple(str(row["strategy_account_id"]) for row in rows)
        for strategy_account_id in account_ids:
            self._bind_runtime(strategy_account_id, account_context, account_key)
        reconcile_ids = tuple(
            str(row["strategy_account_id"])
            for row in rows
            if not self.config.trading_enabled
            or str(row["strategy_id"]) in self.config.enabled_strategy_ids
        )
        if not reconcile_ids:
            self.startup_ready = False
            return False
        snapshot = await collect_async_broker_snapshot(
            cast(Any, self.broker), account_context
        )
        results = [
            await self._synchronize(strategy_id, physical_id, snapshot)
            for strategy_id in reconcile_ids
        ]
        self.startup_ready = all(
            result.state.value == "READY" for result in results
        )
        if self.startup_ready:
            for strategy_id in reconcile_ids:
                await self._sync_quote_subscriptions(strategy_id)
        return self.startup_ready

    def get_intent(self, payload: Mapping[str, object]) -> Dict[str, object]:
        strategy_id = self._strategy_id(payload)
        intent_id = str(payload.get("intent_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or "").strip()
        if not intent_id:
            connection = connect_database(self.database_path)
            try:
                if idempotency_key:
                    row = connection.execute(
                        "SELECT intent_id FROM portfolio_intents WHERE strategy_account_id = ? AND idempotency_key = ?",
                        (strategy_id, idempotency_key),
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT intent_id FROM portfolio_intents
                        WHERE strategy_account_id = ?
                          AND state NOT IN ('COMPLETED','CANCELED','FAILED')
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (strategy_id,),
                    ).fetchone()
            finally:
                connection.close()
            if row is None:
                return {}
            intent_id = str(row[0])
        intent = self.planner.get_intent(intent_id)
        if intent.account_id != strategy_id:
            raise ValueError("intent does not belong to strategy")
        result = cast(Dict[str, object], _json_value(intent))
        result["execution"] = execution_request_to_wire(
            intent.execution_request
        )
        result["trading_day"] = (
            intent.trading_day.isoformat() if intent.trading_day else None
        )
        connection = connect_database(self.database_path)
        try:
            row = connection.execute(
                "SELECT targets_json FROM portfolio_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
        finally:
            connection.close()
        stored = json.loads(row[0]) if row is not None else {}
        result["weights"] = {
            security: value / NAV_SCALE
            for security, value in stored.get("weights_ppm", {}).items()
        }
        orders = []
        for progress in self.planner.intent_orders(intent_id):
            item = cast(Dict[str, object], _json_value(progress))
            limit_price_units = item.pop("limit_price_units", None)
            item["limit_price"] = (
                _price(cast(int, limit_price_units))
                if limit_price_units is not None
                else None
            )
            orders.append(item)
        result["orders"] = orders
        return result

    async def cancel_intent(
        self,
        account_context: object,
        account_key: str,
        payload: Mapping[str, object],
    ) -> Dict[str, object]:
        """Cancel one strategy intent without allowing overlapping targets."""

        strategy_id = self._strategy_id(payload)
        self._bind_runtime(strategy_id, account_context, account_key)
        intent_id = str(payload.get("intent_id") or "").strip()
        if not intent_id:
            raise ValueError("intent_id is required")
        intent = self.planner.get_intent(intent_id)
        if intent.account_id != strategy_id:
            raise ValueError("intent does not belong to strategy")
        return await self._cancel_bound_intent(
            intent, account_context, account_key
        )

    async def _cancel_bound_intent(
        self,
        intent: PortfolioIntent,
        account_context: object,
        account_key: str,
        expire_before: Optional[date] = None,
    ) -> Dict[str, object]:
        requested = []
        broker_order_ids = self.planner.cancelable_broker_order_ids(
            intent.intent_id
        )
        self.planner.request_intent_cancellation(intent.intent_id)
        if broker_order_ids:
            try:
                for broker_order_id in broker_order_ids:
                    await cast(Any, self.broker).cancel_order(
                        account_context, broker_order_id
                    )
                    requested.append(broker_order_id)
            finally:
                # QMT can report that an exchange DAY order is already invalid
                # when the midnight cancel reaches it.  Refresh even when that
                # response raises so StrategyLedger can observe the terminal
                # broker state instead of retrying forever on stale state.
                physical_id = self._physical_id(account_key)
                broker_snapshot = await collect_async_broker_snapshot(
                    cast(Any, self.broker), account_context
                )
                await self._synchronize(
                    intent.account_id, physical_id, broker_snapshot
                )
        expired_order_ids = ()
        if expire_before is not None:
            expired_order_ids = self.planner.finalize_expired_orders(
                intent.intent_id, expire_before
            )
        canceled = False
        try:
            intent = self.planner.cancel_intent_if_idle(intent.intent_id)
            canceled = intent.state.value == "CANCELED"
        except TargetPlanningError as exc:
            if not requested:
                raise
            logger.info(
                "intent %s cancellation still pending: %s",
                intent.intent_id,
                exc,
            )
            intent = self.planner.get_intent(intent.intent_id)
        await self._sync_quote_subscriptions(intent.account_id)
        result = cast(Dict[str, object], _json_value(intent))
        result["execution"] = execution_request_to_wire(
            intent.execution_request
        )
        return {
            "intent": result,
            "canceled": canceled,
            "cancel_requested_order_ids": requested,
            "expired_order_ids": expired_order_ids,
        }

    async def expire_previous_day_intents(
        self, now: Optional[datetime] = None
    ) -> ExpiredIntentSweepResult:
        """Cancel active broker orders whose target was valid only that day."""

        current = self._as_of(now, datetime.now(SHANGHAI_TZ))
        expired = cancel_requests = canceled = pending = unbound = errors = 0
        for intent in self.planner.expired_intents(current.date()):
            expired += 1
            binding = self._runtime_bindings.get(intent.account_id)
            if binding is None:
                unbound += 1
                continue
            lock = self._resume_locks[intent.account_id]
            try:
                async with lock:
                    result = await self._cancel_bound_intent(
                        intent,
                        binding[0],
                        binding[1],
                        expire_before=current.date(),
                    )
            except Exception:
                errors += 1
                logger.exception(
                    "StrategyLedger 午夜撤单失败 | strategy=%s",
                    intent.account_id,
                )
                continue
            cancel_requests += len(
                result.get("cancel_requested_order_ids", ())
            )
            if result.get("canceled"):
                canceled += 1
            else:
                pending += 1
        return ExpiredIntentSweepResult(
            expired=expired,
            cancel_requests=cancel_requests,
            canceled=canceled,
            pending=pending,
            unbound=unbound,
            errors=errors,
        )

    def get_reconciliation(
        self, account_key: str, payload: Mapping[str, object]
    ) -> Dict[str, object]:
        self._strategy_id(payload)
        result = self.reconciliation.latest(self._physical_id(account_key))
        return {"reconciliation": _json_value(result)}

    def notify_target_buy_plan(
        self, payload: Mapping[str, object]
    ) -> Dict[str, object]:
        strategy_id = self._strategy_id(payload)
        mode = str(payload.get("mode") or "").strip().upper()
        mode = {
            "SHADOW": "JQ",
            "SIGNAL_ONLY": "JQ",
            "JQ_PAPER": "JQ",
            "REMOTE": "QMT_REMOTE",
            "LIVE": "QMT_REMOTE",
        }.get(mode, mode)
        if mode not in ("JQ", "QMT_REMOTE"):
            raise ValueError("mode must be JQ or QMT_REMOTE")
        raw_items = payload.get("items")
        if not isinstance(raw_items, (list, tuple)) or not raw_items:
            raise ValueError("items must be a non-empty list")
        items = []
        total = Decimal("0")
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                raise ValueError("target buy plan item must be a mapping")
            security = str(raw.get("security") or "").strip()
            security_name = str(raw.get("security_name") or "").strip()[:100]
            quantity = raw.get("quantity")
            if not security or type(quantity) is not int or quantity <= 0:
                raise ValueError("security and positive integer quantity are required")
            amount = self._positive_decimal(raw.get("amount"), "amount")
            reference_price = raw.get("reference_price")
            if reference_price is not None:
                reference_price = self._positive_decimal(
                    reference_price, "reference_price"
                )
            items.append(
                TargetBuyPlanItem(
                    security=security,
                    quantity=quantity,
                    amount=amount,
                    reference_price=reference_price,
                    security_name=security_name,
                )
            )
            total += amount
        occurred_at = self._as_of(
            payload.get("occurred_at"), datetime.now(SHANGHAI_TZ)
        )
        accepted = False
        if self.notification_handler is not None:
            try:
                self.notification_handler(
                    TargetBuyPlanNotification(
                        strategy_id=strategy_id,
                        mode=mode,
                        items=tuple(items),
                        occurred_at=occurred_at,
                    )
                )
                accepted = True
            except Exception:
                accepted = False
        return {
            "accepted": accepted,
            "item_count": len(items),
            "total_amount": float(total),
        }

    async def _refresh(
        self, account_context, account_key, strategy_id, payload, *, require_ready=False
    ):
        physical_id = self._physical_id(account_key)
        broker_snapshot = await collect_async_broker_snapshot(
            cast(Any, self.broker), account_context
        )
        reconciliation = await self._synchronize(
            strategy_id, physical_id, broker_snapshot
        )
        # Writes must use this account's fresh reconciliation, not the cached
        # process-wide startup flag. Keep read-only snapshots available when
        # blocked, but stop before valuation/planning/cancellation/dispatch.
        if require_ready and reconciliation.state.value != "READY":
            raise RuntimeError(
                "StrategyLedger对账未就绪 | strategy_id={} | state={} | blockers={}".format(
                    strategy_id,
                    reconciliation.state.value,
                    reconciliation.details.get("blockers", ()),
                )
            )
        as_of = self._as_of(payload.get("as_of"), broker_snapshot.as_of)
        raw_weights = payload.get("weights")
        target_securities = (
            tuple(str(key) for key in raw_weights)
            if isinstance(raw_weights, Mapping)
            else ()
        )
        marks = await self._marks(
            payload.get("marks"), as_of, strategy_id, target_securities
        )
        qmt_mark_times = [
            mark.as_of for mark in marks.values() if mark.source == "qmt"
        ]
        if qmt_mark_times:
            as_of = max((as_of, *qmt_mark_times))
        snapshot = self.valuation.create_snapshot(
            strategy_id,
            marks,
            as_of,
            self._valuation_mark_max_age(as_of),
        )
        broker_sellable = {
            p.security: max(0, p.sellable_qty) for p in broker_snapshot.positions
        }
        snapshot = replace(snapshot, positions=tuple(
            replace(p, sellable_qty=min(
                p.sellable_qty, broker_sellable.get(p.security, 0)
            )) for p in snapshot.positions
        ))
        return snapshot, reconciliation, marks

    async def _synchronize(self, strategy_id, physical_id, broker_snapshot):
        previous = self.reconciliation.latest(physical_id, strategy_id)
        settlement_cycles = await self._settlement_cycles(broker_snapshot)
        result = self.reconciliation.synchronize(
            strategy_id,
            physical_id,
            broker_snapshot,
            settlement_cycles=settlement_cycles,
        )
        self.startup_ready = result.state.value == "READY"
        limits = dict(result.details.get("broker_sellable_limits", {}))
        old_limits = self._sellable_waits.get(strategy_id, {})
        self._sellable_waits[strategy_id] = limits
        if limits != old_limits:
            names = {p.security: p.security_name for p in broker_snapshot.positions}
            for security in sorted(set(limits) | set(old_limits)):
                if limits.get(security) != old_limits.get(security):
                    logger.info(
                        "QMT可卖限制 | strategy_id=%s | %s(%s) | %s",
                        strategy_id, security, names.get(security) or "名称未知",
                        "QMT当前可卖={}；不足部分等待，不阻断整账户".format(limits[security])
                        if security in limits else "可卖限制已解除",
                    )
        if (
            self.notification_handler is not None
            and result.state.value == "BLOCKED"
            and (previous is None or previous.state.value != "BLOCKED" or previous.details != result.details)
        ):
            try:
                blockers = result.details.get("blockers", ())
                if not isinstance(blockers, (list, tuple)):
                    blockers = (blockers,)
                self.notification_handler(
                    reconciliation_notification(
                        strategy_id, tuple(str(item) for item in blockers),
                        {p.security: p.security_name for p in broker_snapshot.positions},
                        occurred_at=broker_snapshot.as_of,
                    )
                )
            except Exception:
                pass
        return result

    async def _settlement_cycles(
        self, broker_snapshot
    ) -> Mapping[str, int]:
        resolver = getattr(self.data_provider, "get_tplus", None)
        if not callable(resolver):
            return {}
        securities = {
            self._joinquant_security(
                str(
                    trade.get("security")
                    or trade.get("stock_code")
                    or trade.get("stockCode")
                    or ""
                )
            )
            for trade in broker_snapshot.trades
            if isinstance(trade, Mapping)
        }
        result = {}
        for security in sorted(item for item in securities if item):
            try:
                cycle = await resolver(security)
            except Exception as exc:
                logger.warning(
                    "StrategyLedger无法识别%s结算周期，保守按T+1: %s",
                    security,
                    exc,
                )
                continue
            if type(cycle) is int and cycle in (0, 1):
                result[security] = cycle
        return result

    async def _marks(
        self,
        raw,
        as_of: datetime,
        strategy_id: str,
        target_securities: Sequence[str] = (),
    ):
        marks: Dict[str, MarketMark] = {}
        if raw is not None:
            if not isinstance(raw, Mapping):
                raise ValueError("marks must be a mapping")
            for security, item in raw.items():
                price = item.get("price") if isinstance(item, Mapping) else item
                mark_as_of = self._as_of(
                    item.get("as_of") if isinstance(item, Mapping) else None,
                    as_of,
                )
                marks[str(security)] = MarketMark(
                    str(security),
                    price_to_units(str(price) if type(price) is float else price),  # type: ignore[arg-type]
                    mark_as_of,
                    "joinquant",
                )
        required = set(self._held_securities(strategy_id)) | {
            str(item) for item in target_securities
        }
        missing = [security for security in sorted(required) if security not in marks]
        for security in missing:
            tick_fn = getattr(self.data_provider, "get_current_tick", None)
            if tick_fn is None:
                raise ValueError("missing mark: {}".format(security))
            tick = await tick_fn(security)
            price = self._tick_price(tick)
            mark_as_of = self._tick_as_of(tick)
            if not self._valuation_mark_is_fresh(
                mark_as_of, datetime.now(SHANGHAI_TZ)
            ):
                raise ValueError(
                    "QMT mark is stale or in the future: {} as_of={}".format(
                        security, mark_as_of.isoformat()
                    )
                )
            marks[security] = MarketMark(
                security, price_to_units(str(price)), mark_as_of, "qmt"
            )
        return marks

    async def _execution_quotes(
        self,
        execution_request: ExecutionRequest,
        securities: Sequence[str],
        as_of: datetime,
    ) -> Mapping[str, MarketQuote]:
        if not _uses_quote_execution(execution_request):
            return {}
        tick_fn = getattr(self.data_provider, "get_current_tick", None)
        if tick_fn is None:
            return {}
        quotes = {}
        for security in securities:
            if not _uses_quote_execution(execution_request, security):
                continue
            tick = await tick_fn(security)
            if not isinstance(tick, Mapping):
                continue
            try:
                quote_as_of = self._tick_as_of(tick)
            except ValueError:
                continue
            quote = self._quote_from_tick(security, tick, quote_as_of)
            if not self._quote_is_fresh(
                quote, datetime.now(SHANGHAI_TZ)
            ):
                continue
            quotes[security] = quote
            self._quote_cache[security] = quote
        return quotes

    def _quote_from_tick(
        self, security: str, tick: Mapping[str, object], as_of: datetime
    ) -> MarketQuote:
        bid = self._best_quote_price(tick, "bid")
        ask = self._best_quote_price(tick, "ask")
        last = self._tick_price_or_none(tick)
        previous = self._quote_cache.get(security)
        if previous is not None and previous.as_of.date() != as_of.date():
            previous = None
        metadata = {}
        for field_name, aliases in (
            ("high_limit_units", ("high_limit", "highLimit", "UpStopPrice", "up_stop_price")),
            ("low_limit_units", ("low_limit", "lowLimit", "DownStopPrice", "down_stop_price")),
            ("preclose_units", ("preclose", "pre_close", "lastClose", "preClose")),
        ):
            # Native ticks omit instrument detail. Preserve same-day metadata,
            # but respect an explicit zero and never carry daily limits overnight.
            metadata[field_name] = (
                getattr(previous, field_name)
                if previous is not None and not any(name in tick for name in aliases)
                else self._tick_price_units(tick, *aliases)
            )
        return MarketQuote(
            security=security,
            as_of=as_of,
            bid_price_units=price_to_units(str(bid)) if bid is not None else None,
            ask_price_units=price_to_units(str(ask)) if ask is not None else None,
            last_price_units=price_to_units(str(last)) if last is not None else None,
            instrument_type=(
                self._tick_instrument_type(tick)
                or (previous.instrument_type if previous is not None else None)
            ),
            continuous_auction=self._tick_continuous_auction(tick, as_of),
            **metadata,
        )

    def _bind_runtime(
        self, strategy_id: str, account_context: object, account_key: str
    ) -> None:
        try:
            self._event_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        self._runtime_bindings[strategy_id] = (
            account_context,
            account_key,
        )
        self._resume_locks.setdefault(strategy_id, asyncio.Lock())

    def _on_tick_event(self, payload: object) -> None:
        loop = self._event_loop
        if loop is None or loop.is_closed():
            return
        safe_payload = dict(payload) if isinstance(payload, Mapping) else payload
        loop.call_soon_threadsafe(self._accept_tick_event, safe_payload)

    def _on_broker_event(
        self, account_key: str, event: str, payload: object = None
    ) -> None:
        loop = self._event_loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(
            self._schedule_background,
            self._handle_broker_event(account_key, event),
        )

    def _schedule_background(self, awaitable) -> None:
        task = asyncio.create_task(awaitable)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_done)

    def _background_done(self, task: "asyncio.Task[object]") -> None:
        self._background_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("StrategyLedger callback task failed")

    def _accept_tick_event(self, payload: object) -> None:
        heartbeat_quotes = {}
        first_symbols = set()
        for security, tick in self._tick_items(payload):
            received_at = datetime.now(SHANGHAI_TZ)
            try:
                quote_as_of = self._tick_as_of(tick)
            except ValueError as exc:
                self._reject_tick_once(security, str(exc))
                continue
            quote = self._quote_from_tick(security, tick, quote_as_of)
            if not self._quote_is_fresh(quote, received_at):
                self._reject_tick_once(
                    security,
                    "execution quote timestamp is stale or in the future",
                )
                continue
            self._quote_cache[security] = quote
            self._quote_last_received_at[security] = received_at
            if security not in self._seen_tick_symbols:
                self._seen_tick_symbols.add(security)
                first_symbols.add(security)
            # One xtdata callback may contain several ticks for one symbol.
            # Execution still evaluates every tick, while the heartbeat below
            # reports the newest market timestamp in the callback batch.
            heartbeat_quotes[security] = (quote, received_at)
            self._schedule_background(self._handle_quote(security, quote))
        for security, (quote, received_at) in heartbeat_quotes.items():
            self._log_execution_quote_heartbeat(
                security,
                quote,
                received_at,
                first=security in first_symbols,
            )

    def _log_execution_quote_heartbeat(
        self,
        security: str,
        quote: MarketQuote,
        received_at: datetime,
        *,
        first: bool,
    ) -> None:
        last_log_at = self._quote_last_log_at.get(security)
        if (
            not first
            and last_log_at is not None
            and received_at - last_log_at
            < EXECUTION_QUOTE_HEARTBEAT_INTERVAL
        ):
            return
        self._quote_last_log_at[security] = received_at
        logger.info(
            "StrategyLedger %s | %s | 行情时间=%s | 接收时间=%s | 延迟=%.3fs",
            "首次收到执行行情" if first else "执行行情心跳",
            security,
            quote.as_of.isoformat(),
            received_at.isoformat(),
            (received_at - quote.as_of).total_seconds(),
        )

    async def _handle_quote(
        self, security: str, quote: Optional[MarketQuote] = None
    ) -> None:
        quote = quote or self._quote_cache.get(security)
        if quote is None:
            return
        for intent in self.planner.active_intents():
            waiting_sellable = security in self._sellable_waits.get(intent.account_id, {})
            if (
                (not waiting_sellable and not _uses_quote_execution(intent.execution_request, security))
                or security not in self.planner.reference_prices(intent.intent_id)
                or intent.account_id not in self._runtime_bindings
                or not self._strategy_reconciliation_enabled(intent.account_id)
            ):
                continue
            if not waiting_sellable and not self.planner.quote_triggers_intent(
                intent.intent_id, security, quote
            ):
                continue
            await self._resume_intent(
                intent.intent_id, {security: quote}
            )

    async def _handle_broker_event(
        self, account_key: str, event: str
    ) -> None:
        if event == "disconnected":
            self.startup_ready = False
            return
        strategy_ids = [
            strategy_id
            for strategy_id, (_, key) in self._runtime_bindings.items()
            if key == account_key
            and self._strategy_reconciliation_enabled(strategy_id)
        ]
        for strategy_id in strategy_ids:
            lock = self._resume_locks[strategy_id]
            async with lock:
                binding = self._runtime_bindings[strategy_id]
                intents = self.planner.active_intents(strategy_id)
                if not intents:
                    await self._refresh(
                        binding[0], binding[1], strategy_id, {}
                    )
                for intent in intents:
                    await self._resume_intent_locked(intent.intent_id)
                await self._sync_quote_subscriptions(strategy_id)

    async def _resume_intent(
        self,
        intent_id: str,
        quote_overrides: Optional[Mapping[str, MarketQuote]] = None,
    ) -> None:
        intent = self.planner.get_intent(intent_id)
        lock = self._resume_locks.get(intent.account_id)
        if lock is None:
            return
        async with lock:
            await self._resume_intent_locked(intent_id, quote_overrides)

    async def _resume_intent_locked(
        self,
        intent_id: str,
        quote_overrides: Optional[Mapping[str, MarketQuote]] = None,
    ) -> None:
        intent = self.planner.get_intent(intent_id)
        binding = self._runtime_bindings.get(intent.account_id)
        if binding is None:
            return
        now = datetime.now(SHANGHAI_TZ)
        if not self._can_dispatch_now():
            # Late/reconnect callbacks still book fills; they must not plan or
            # dispatch the unfinished daytime target using an old closing tick.
            broker_snapshot = await collect_async_broker_snapshot(
                cast(Any, self.broker), binding[0]
            )
            await self._synchronize(
                intent.account_id, self._physical_id(binding[1]), broker_snapshot
            )
            return
        execution_quotes = dict(self._quote_cache)
        if quote_overrides:
            execution_quotes.update(quote_overrides)
        references = self.planner.reference_prices(intent_id)
        if (
            intent.execution_request.repricing
            is RepricingPolicy.RECOMPUTE
        ):
            references = {
                security: (
                    execution_quotes[security].last_price_units
                    if security in execution_quotes
                    and execution_quotes[security].last_price_units
                    is not None
                    else price_units
                )
                for security, price_units in references.items()
            }
        marks = {
            security: {
                "price": _price(price_units),
                "as_of": now.isoformat(),
            }
            for security, price_units in references.items()
        }
        snapshot, reconciliation, market_marks = await self._refresh(
            binding[0],
            binding[1],
            intent.account_id,
            {
                "marks": marks,
                "weights": {security: 0 for security in intent.targets},
                "as_of": now,
            },
            require_ready=True,
        )
        self._record_reconciliation_rejections(
            intent_id, reconciliation, execution_quotes
        )
        self.planner.advance_intent(
            intent_id,
            snapshot,
            market_marks,
            now,
            quotes=execution_quotes,
        )

        async def submitter(order_payload: Mapping[str, object]):
            return await cast(Any, self.broker).place_order(
                binding[0], dict(order_payload)
            )

        await self._dispatch_pending(submitter, intent.account_id)
        await self._sync_quote_subscriptions(intent.account_id)

    def _record_reconciliation_rejections(
        self,
        intent_id: str,
        reconciliation,
        quotes: Mapping[str, MarketQuote],
    ) -> None:
        details = getattr(reconciliation, "details", {})
        if not isinstance(details, Mapping):
            return
        recorded = self.planner.record_rejected_orders(
            intent_id,
            details.get("rejected_orders", ()),
            quotes,
        )
        for item in recorded:
            logger.warning(
                "StrategyLedger 委托拒绝处理 | %s | %s | 原因=%s",
                item.get("security"),
                (
                    "等待新行情确认原定限价进入价格笼子后重试"
                    if item.get("retryable") is True
                    else "非价格笼子拒单，不自动重试"
                ),
                item.get("reason") or "券商未返回原因",
            )

    async def _dispatch_pending(self, submitter, strategy_account_id: str):
        dispatched = []
        async with self._dispatch_lock:
            while self._can_dispatch_now():
                binding = self._runtime_bindings.get(strategy_account_id)
                if binding is None:
                    break
                broker_snapshot = await collect_async_broker_snapshot(
                    cast(Any, self.broker), binding[0]
                )
                reconciliation = await self._synchronize(
                    strategy_account_id, self._physical_id(binding[1]), broker_snapshot
                )
                if reconciliation.state.value != "READY" or not self._can_dispatch_now():
                    break
                dispatch = await self.planner.dispatch_next(
                    submitter, strategy_account_id,
                    sellable_limits={
                        p.security: max(0, p.sellable_qty) for p in broker_snapshot.positions
                    },
                )
                if dispatch is None:
                    break
                dispatched.append(dispatch)
        return tuple(dispatched)

    def _strategy_reconciliation_enabled(self, strategy_account_id: str) -> bool:
        if not self.config.trading_enabled:
            return True
        return strategy_account_id in self.config.enabled_strategy_ids

    async def _sync_quote_subscriptions(self, strategy_id: str) -> None:
        symbols = set()
        for intent in self.planner.active_intents(strategy_id):
            if not self.planner.intent_cancel_requested(intent.intent_id):
                symbols.update(
                    set(self._sellable_waits.get(strategy_id, {}))
                    & set(self.planner.reference_prices(intent.intent_id))
                )
            if _uses_quote_execution(
                intent.execution_request
            ) and not self.planner.intent_cancel_requested(intent.intent_id):
                symbols.update(
                    security
                    for security in self.planner.reference_prices(intent.intent_id)
                    if _uses_quote_execution(intent.execution_request, security)
                )
        replace = getattr(
            self.data_provider, "replace_execution_quotes", None
        )
        if callable(replace):
            await replace(strategy_id, tuple(sorted(symbols)))
            return
        subscribe = getattr(
            self.data_provider, "subscribe_execution_quotes", None
        )
        if symbols and callable(subscribe):
            await subscribe(tuple(sorted(symbols)))

    @classmethod
    def _tick_items(cls, payload: object):
        if not isinstance(payload, Mapping):
            return ()
        direct_code = (
            payload.get("stockCode")
            or payload.get("stock_code")
            or payload.get("security")
            or payload.get("code")
        )
        if direct_code:
            return ((cls._joinquant_security(str(direct_code)), payload),)
        items = []
        for raw_security, tick in payload.items():
            if isinstance(tick, Mapping):
                items.append(
                    (cls._joinquant_security(str(raw_security)), tick)
                )
            elif isinstance(tick, (list, tuple)):
                items.extend(
                    (
                        cls._joinquant_security(str(raw_security)),
                        item,
                    )
                    for item in tick
                    if isinstance(item, Mapping)
                )
        return tuple(items)

    @staticmethod
    def _joinquant_security(value: str) -> str:
        upper = value.strip().upper()
        if upper.endswith(".SH"):
            return upper[:-3] + ".XSHG"
        if upper.endswith(".SZ"):
            return upper[:-3] + ".XSHE"
        if upper.endswith(".BJ"):
            return upper[:-3] + ".XBSE"
        return upper

    @staticmethod
    def _best_quote_price(tick: Mapping[str, object], side: str):
        names = (
            ("ask_price1", "ask1", "askPrice", "ask_price")
            if side == "ask"
            else ("bid_price1", "bid1", "bidPrice", "bid_price")
        )
        for name in names:
            value = tick.get(name)
            if isinstance(value, (list, tuple)):
                value = value[0] if value else None
            elif value is not None and not isinstance(value, (str, bytes)):
                try:
                    value = value[0]  # type: ignore[index]
                except (IndexError, KeyError, TypeError):
                    pass
            try:
                number = float(value) if value is not None else 0.0
            except (TypeError, ValueError):
                continue
            if number > 0:
                return number
        return None

    @staticmethod
    def _tick_price_units(
        tick: Mapping[str, object], *names: str
    ) -> Optional[int]:
        for name in names:
            value = tick.get(name)
            try:
                number = float(value) if value not in (None, "") else 0.0
            except (TypeError, ValueError):
                continue
            if number > 0:
                return price_to_units(str(number))
        return None

    @staticmethod
    def _tick_instrument_type(tick: Mapping[str, object]) -> Optional[str]:
        for name in ("instrument_type", "security_type", "instrumentType"):
            value = tick.get(name)
            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _tick_continuous_auction(
        tick: Mapping[str, object], as_of: datetime
    ) -> bool:
        for name in ("openInt", "stockStatus", "market_status"):
            value = tick.get(name)
            try:
                status = int(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                status = None
            if status == 13:
                return True
            if status in (1, 11, 12, 14, 15, 16, 17, 18, 19, 20, 22, 23):
                return False
        return SQLiteStrategyAPI._is_execution_session(as_of)

    @staticmethod
    def _tick_price_or_none(tick: Mapping[str, object]):
        try:
            return SQLiteStrategyAPI._tick_price(tick)
        except ValueError:
            return None

    def _ensure_physical_account(self, physical_id: str, account_context: object) -> None:
        connection = connect_database(self.database_path)
        try:
            exists = connection.execute(
                "SELECT 1 FROM physical_accounts WHERE physical_account_id = ?",
                (physical_id,),
            ).fetchone()
        finally:
            connection.close()
        if exists is None:
            config = getattr(account_context, "config", None)
            broker_ref = str(getattr(config, "account_id", physical_id))
            self.repository.create_physical_account(
                physical_id, "QMT", broker_ref
            )

    def _held_securities(self, strategy_id: str) -> Sequence[str]:
        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                "SELECT security FROM positions WHERE strategy_account_id = ? AND total_qty > 0",
                (strategy_id,),
            ).fetchall()
            return tuple(row[0] for row in rows)
        finally:
            connection.close()

    @staticmethod
    def _tick_price(tick: object) -> object:
        if not isinstance(tick, Mapping):
            raise ValueError("QMT mark response is invalid")
        for name in ("last_price", "lastPrice", "price", "last"):
            value = tick.get(name)
            if value is not None and float(value) > 0:
                return value
        raise ValueError("QMT mark response has no valid price")

    @classmethod
    def _tick_as_of(cls, tick: object) -> datetime:
        if not isinstance(tick, Mapping):
            raise ValueError("QMT mark response is invalid")
        for name in ("dt", "timetag", "datetime", "time", "timestamp"):
            value = tick.get(name)
            if value not in (None, ""):
                try:
                    return cls._as_of(value, None)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ValueError(
                        "QMT mark response has invalid market timestamp"
                    ) from exc
        raise ValueError("QMT mark response has no market timestamp")

    def _quote_is_fresh(self, quote: MarketQuote, now: datetime) -> bool:
        return self._market_time_is_fresh(quote.as_of, now)

    @staticmethod
    def _is_execution_session(value: datetime) -> bool:
        value = value.astimezone(SHANGHAI_TZ)
        minutes = value.hour * 60 + value.minute
        return value.weekday() < 5 and (
            9 * 60 + 30 <= minutes < 11 * 60 + 30
            or 13 * 60 <= minutes < 15 * 60
        )

    def _can_dispatch_now(self) -> bool:
        return self._is_execution_session(datetime.now(SHANGHAI_TZ))

    def _valuation_mark_max_age(self, now: datetime) -> timedelta:
        current = self._as_of(now, None)
        if self._is_execution_session(current):
            return self.config.max_age
        return max(self.config.max_age, VALUATION_MARK_MAX_IDLE_AGE)

    def _valuation_mark_is_fresh(
        self, market_as_of: datetime, now: datetime
    ) -> bool:
        current = self._as_of(now, None)
        age = current - market_as_of
        return -timedelta(seconds=5) <= age <= self._valuation_mark_max_age(
            current
        )

    def _market_time_is_fresh(
        self, market_as_of: datetime, now: datetime
    ) -> bool:
        current = self._as_of(now, None)
        age = current - market_as_of
        return -timedelta(seconds=5) <= age <= self.config.max_age

    def _reject_tick_once(self, security: str, reason: str) -> None:
        key = (security, reason)
        if key in self._rejected_tick_symbols:
            return
        self._rejected_tick_symbols.add(key)
        logger.warning(
            "StrategyLedger 忽略不可验证新鲜度的执行行情 | %s | %s",
            security,
            reason,
        )

    @staticmethod
    def _as_of(value: object, default: Optional[datetime]) -> datetime:
        if value is None:
            if default is None:
                raise ValueError("timestamp is required")
            result = default
        if isinstance(value, datetime):
            result = value
        elif value is not None:
            text = str(value).strip()
            numeric_text = text[:-2] if text.endswith(".0") else text
            if numeric_text.isdigit() and len(numeric_text) in (8, 12, 14, 17):
                compact = numeric_text[:14]
                format_text = {
                    8: "%Y%m%d",
                    12: "%Y%m%d%H%M",
                    14: "%Y%m%d%H%M%S",
                    17: "%Y%m%d%H%M%S",
                }[len(numeric_text)]
                result = datetime.strptime(compact, format_text)
            else:
                try:
                    numeric = float(text)
                except ValueError:
                    normalized = text.replace("/", "-")
                    result = datetime.fromisoformat(normalized)
                else:
                    if abs(numeric) >= 10**11:
                        numeric /= 1000.0
                    result = datetime.fromtimestamp(numeric, tz=SHANGHAI_TZ)
        if result.tzinfo is None or result.utcoffset() is None:
            result = result.replace(tzinfo=SHANGHAI_TZ)
        return result.astimezone(SHANGHAI_TZ)

    @staticmethod
    def _strategy_id(payload: Mapping[str, object]) -> str:
        strategy_id = str(payload.get("strategy_id") or "").strip()
        if not strategy_id:
            raise ValueError("strategy_id is required")
        return strategy_id

    @staticmethod
    def _positive_decimal(value: object, field: str) -> Decimal:
        if isinstance(value, bool):
            raise ValueError("{} must be a positive number".format(field))
        try:
            number = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("{} must be a positive number".format(field)) from exc
        if not number.is_finite() or number <= 0:
            raise ValueError("{} must be a positive number".format(field))
        return number

    @staticmethod
    def _physical_id(account_key: str) -> str:
        return "qmt:{}".format(account_key or "default")

    @staticmethod
    def _account_payload(account) -> Dict[str, object]:
        return {
            "account_id": account.account_id,
            "strategy_id": account.strategy_id,
            "physical_account_id": account.physical_account_id,
            "initial_capital": _money(account.initial_capital_units),
            "cash": _money(account.cash_units),
            "reserved_cash": _money(account.reserved_cash_units),
            "available_cash": _money(account.available_cash_units),
            "ledger_version": account.ledger_version,
            "event_seq": account.event_seq,
            "status": account.status.value,
        }

    @staticmethod
    def _snapshot_payload(snapshot) -> Dict[str, object]:
        positions = {}
        for item in snapshot.positions:
            positions[item.security] = {
                "security": item.security,
                "total_amount": item.total_qty,
                "closeable_amount": item.sellable_qty,
                "avg_cost": _price(item.avg_cost_price_units),
                "price": _price(item.mark_price_units),
                "mark_as_of": item.mark_as_of.isoformat(),
                "mark_source": item.mark_source,
                "value": _money(item.market_value_units),
                "unrealized_pnl": _money(item.unrealized_pnl_units),
            }
        performance_ready = snapshot.performance_ready
        return {
            "account_id": snapshot.account_id,
            "strategy_id": snapshot.strategy_id,
            "as_of": snapshot.as_of.isoformat(),
            "snapshot_version": snapshot.snapshot_version,
            "ledger_version": snapshot.ledger_version,
            "cash": _money(snapshot.cash_units),
            "reserved_cash": _money(snapshot.reserved_cash_units),
            "available_cash": _money(snapshot.available_cash_units),
            "positions_value": _money(snapshot.positions_value_units),
            "total_value": _money(snapshot.total_assets_units),
            "starting_cash": _money(snapshot.net_capital_units),
            "total_pnl": _money(snapshot.total_pnl_units) if performance_ready else None,
            "realized_pnl": (
                _money(snapshot.realized_pnl_units) if performance_ready else None
            ),
            "unrealized_pnl": (
                _money(snapshot.unrealized_pnl_units) if performance_ready else None
            ),
            "fees": _money(snapshot.fees_units) if snapshot.fees_known else None,
            "fees_known": snapshot.fees_known,
            "unknown_fee_fill_count": snapshot.unknown_fee_fill_count,
            "unknown_price_fill_count": snapshot.unknown_price_fill_count,
            "nav": snapshot.nav_units / NAV_SCALE if performance_ready else None,
            "returns": (
                snapshot.nav_units / NAV_SCALE - 1.0 if performance_ready else None
            ),
            "performance_blockers": list(snapshot.performance_blockers),
            "performance_ready": performance_ready,
            "positions": positions,
        }
