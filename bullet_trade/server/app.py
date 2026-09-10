"""
作者: BruceLee
日期: 2026-03-20
文件说明:
    bullet-trade 服务端核心调度入口。
    本文件负责会话管理、broker/data action 分发、下单幂等、服务端风控、
    tick 订阅转发等能力。
"""

from __future__ import annotations

import asyncio
import json
import ipaddress
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from bullet_trade.core.globals import log
from bullet_trade.core.risk_control import RiskController
from bullet_trade.utils.portfolio_printer import render_account_overview
from bullet_trade.server.feishu_notifier import FeishuNotifier
from bullet_trade.server.strategy import (
    BrokerCapabilityProfile,
    BrokerContractError,
    SQLiteStrategyAPI,
    StrategyAPIConfig,
    UnpricedFillPolicy,
    money_to_units,
    load_verified_capabilities,
)
from bullet_trade.server.strategy.domain import SHANGHAI_TZ

from .adapters.base import AccountRouter, AdapterBundle, AccountContext, SubAccountConfig, VirtualAccountManager
from .config import ServerConfig
from .dashboard import DashboardHTTPServer, DashboardReadModel, tail_log_file
from .session import ClientSession
from .tick import TickSubscriptionManager


_STRATEGY_STARTUP_RETRY_SECONDS = 5.0
_STRATEGY_EXPIRY_RETRY_SECONDS = 60.0


@dataclass
class _IdempotencyEntry:
    fingerprint: str
    result: Dict[str, Any]
    expires_at: float


class ServerApplication:
    """bullet-trade 远程服务应用。

    说明:
        1. 对外暴露统一的 broker/data TCP 协议。
        2. 支持多账户路由、子账户限额、下单幂等。
        3. 可选启用服务端风控，拦截异常下单和频繁撤单。
    """

    def __init__(self, config: ServerConfig, router: AccountRouter, adapters: AdapterBundle):
        self.config = config
        self.router = router
        self.adapters = adapters
        self.virtual_accounts = VirtualAccountManager(config.sub_accounts)
        self.tick_manager: Optional[TickSubscriptionManager] = None
        if adapters.data_adapter:
            self.tick_manager = TickSubscriptionManager(
                adapters.data_adapter,
                interval=1.0,
                max_subscriptions=config.max_subscriptions,
            )
        self._server: Optional[asyncio.AbstractServer] = None
        self._sessions: Set[ClientSession] = set()
        self._created_at = time.time()
        self._ip_allowlist = self._prepare_allowlist(config.allowlist)
        self._shutdown: Optional[asyncio.Event] = None
        self._started: Optional[asyncio.Event] = None
        self._idempotency_cache: Dict[Tuple[str, str, str], _IdempotencyEntry] = {}
        self._idempotency_lock = asyncio.Lock()
        self._strategy_startup_task: Optional[asyncio.Task] = None
        self._strategy_expiry_task: Optional[asyncio.Task] = None
        self._dashboard_sample_task: Optional[asyncio.Task] = None
        self._risk_by_account: Dict[str, RiskController] = {}
        self._risk_locks: Dict[str, asyncio.Lock] = {}
        self.strategy_api: Optional[SQLiteStrategyAPI] = None
        self.feishu_notifier = (
            FeishuNotifier(
                self.config.feishu_webhook_url,
                self.config.feishu_signing_secret,
            )
            if self.config.feishu_webhook_url
            else None
        )
        if self.config.strategy_database_path and self.adapters.broker_adapter:
            capability_fn = getattr(
                self.adapters.broker_adapter, "strategy_ledger_capabilities", None
            )
            if not callable(capability_fn):
                raise RuntimeError("broker adapter does not expose StrategyLedger capabilities")
            capabilities = cast(BrokerCapabilityProfile, capability_fn())
            durable_history_fn = getattr(
                self.adapters.broker_adapter, "has_durable_broker_history", None
            )
            durable_broker_history = bool(
                durable_history_fn() if callable(durable_history_fn) else False
            )
            if self.config.strategy_capabilities_path:
                try:
                    capabilities = load_verified_capabilities(
                        self.config.strategy_capabilities_path,
                        capabilities.adapter_kind,
                        durable_broker_history=durable_broker_history,
                    )
                except BrokerContractError as exc:
                    log.error("QMT能力证明无效，StrategyLedger保持只读: %s", exc)
            self.strategy_api = SQLiteStrategyAPI(
                StrategyAPIConfig(
                    database_path=self.config.strategy_database_path,
                    trading_enabled=self.config.strategy_trading_enabled,
                    enabled_strategy_ids=tuple(self.config.strategy_enabled_ids),
                    allow_buys=self.config.strategy_allow_buys,
                    max_age=timedelta(seconds=self.config.strategy_max_age_seconds),
                    cash_buffer_units=money_to_units(str(self.config.strategy_cash_buffer)),
                    minimum_order_units=money_to_units(str(self.config.strategy_minimum_order)),
                    buy_fee_buffer_units=money_to_units(str(self.config.strategy_buy_fee_buffer)),
                    unpriced_fill_policy=UnpricedFillPolicy(
                        self.config.strategy_unpriced_fill_policy
                    ),
                ),
                self.adapters.broker_adapter,
                capabilities,
                self.adapters.data_adapter,
                self.feishu_notifier.queue_message if self.feishu_notifier else None,
                durable_broker_history=durable_broker_history,
            )
        if self.config.order_risk_enabled:
            for ctx in self.router.list_accounts():
                account_key = ctx.config.key or "default"
                self._risk_by_account[account_key] = RiskController()
                self._risk_locks[account_key] = asyncio.Lock()
        self.dashboard_read_model: Optional[DashboardReadModel] = None
        self.dashboard_server: Optional[DashboardHTTPServer] = None
        if self.config.dashboard_enabled:
            if self.strategy_api is None or not self.config.strategy_database_path:
                raise RuntimeError("dashboard requires StrategyLedger")
            self.dashboard_read_model = DashboardReadModel(
                self.config.strategy_database_path
            )
            self.dashboard_server = DashboardHTTPServer(
                self.config.dashboard_listen,
                self.config.dashboard_port,
                self.config.dashboard_token,
                self._dashboard_payload,
            )

    async def start(self) -> None:
        self._ensure_runtime_events()
        await self._start_components()
        if self.dashboard_server:
            await self.dashboard_server.start()
            log.info(
                "Read-only dashboard API listening on %s:%s",
                self.config.dashboard_listen,
                self.dashboard_server.bound_port,
            )
            self._dashboard_sample_task = asyncio.create_task(
                self._sample_dashboard_loop(), name="dashboard-snapshot-sampler"
            )
        self._server = await asyncio.start_server(self._handle_client, self.config.listen, self.config.port)
        host = self._server.sockets[0].getsockname() if self._server.sockets else (self.config.listen, self.config.port)
        log.info(f"QMT server listening on {host}")
        assert self._started is not None
        self._started.set()
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        self._ensure_runtime_events()
        assert self._shutdown is not None
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        if self._strategy_startup_task:
            self._strategy_startup_task.cancel()
            try:
                await self._strategy_startup_task
            except asyncio.CancelledError:
                pass
            self._strategy_startup_task = None
        if self._strategy_expiry_task:
            self._strategy_expiry_task.cancel()
            try:
                await self._strategy_expiry_task
            except asyncio.CancelledError:
                pass
            self._strategy_expiry_task = None
        if self._dashboard_sample_task:
            self._dashboard_sample_task.cancel()
            try:
                await self._dashboard_sample_task
            except asyncio.CancelledError:
                pass
            self._dashboard_sample_task = None
        if self.dashboard_server:
            await self.dashboard_server.stop()
        if self.tick_manager:
            await self.tick_manager.stop()
        for session in list(self._sessions):
            await session.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self.strategy_api:
            await self.strategy_api.close()
        if self.adapters.broker_adapter:
            try:
                await self.adapters.broker_adapter.stop()
            except Exception:
                pass

    async def _dashboard_payload(
        self, strategy_id: Optional[str], log_limit: int
    ) -> Dict[str, object]:
        """Build one read-only website response from live state and SQLite."""

        server = self._dashboard_server_status()
        generated_at = datetime.now(SHANGHAI_TZ).isoformat()
        if self.dashboard_read_model is None:
            return {"generated_at": generated_at, "server": server}
        strategies = self.dashboard_read_model.list_strategies()
        if strategy_id is None and log_limit == 0:
            return {"generated_at": generated_at, "server": server}
        selected = strategy_id or (
            str(strategies[0]["strategy_id"]) if strategies else None
        )
        snapshot: Optional[Dict[str, object]] = None
        snapshot_error: Optional[str] = None
        if selected:
            try:
                snapshot = await self._dashboard_live_snapshot(selected)
                self.dashboard_read_model.record_snapshot(snapshot)
            except Exception as exc:
                snapshot_error = str(exc)
            activity = self.dashboard_read_model.strategy_activity(selected)
            if snapshot:
                names = activity.get("security_names", {})
                positions = snapshot.get("positions", {})
                if isinstance(names, dict) and isinstance(positions, dict):
                    for security, position in positions.items():
                        if isinstance(position, dict):
                            position.setdefault("name", names.get(security, ""))
        else:
            activity = {
                "intents": [],
                "orders": [],
                "fills": [],
                "history": [],
                "security_names": {},
            }
        return {
            "generated_at": generated_at,
            "server": server,
            "strategies": strategies,
            "selected_strategy_id": selected,
            "snapshot": snapshot,
            "snapshot_error": snapshot_error,
            "activity": activity,
            "logs": tail_log_file(self.config.log_file, log_limit),
        }

    async def _dashboard_live_snapshot(
        self, strategy_id: str
    ) -> Dict[str, object]:
        if self.strategy_api is None or self.dashboard_read_model is None:
            raise RuntimeError("StrategyLedger is unavailable")
        strategy = next(
            (
                item
                for item in self.dashboard_read_model.list_strategies()
                if item.get("strategy_id") == strategy_id
            ),
            None,
        )
        if strategy is None:
            raise KeyError("strategy not found: {}".format(strategy_id))
        physical_id = str(strategy.get("physical_account_id") or "")
        account_key = (
            physical_id.split(":", 1)[1] if ":" in physical_id else physical_id
        )
        account_key = account_key or "default"
        account_context = self.router.get(account_key)
        return await self.strategy_api.get_snapshot(
            account_context, account_key, {"strategy_id": strategy_id}
        )

    async def _sample_dashboard_loop(self) -> None:
        assert self.dashboard_read_model is not None
        interval = self.config.dashboard_sample_interval_seconds
        while True:
            await asyncio.sleep(interval)
            for strategy in self.dashboard_read_model.list_strategies():
                strategy_id = str(strategy.get("strategy_id") or "")
                if not strategy_id:
                    continue
                try:
                    snapshot = await self._dashboard_live_snapshot(strategy_id)
                    self.dashboard_read_model.record_snapshot(snapshot)
                except Exception as exc:
                    log.debug(
                        "dashboard snapshot sample failed for %s: %s",
                        strategy_id,
                        exc,
                    )

    def _dashboard_server_status(self) -> Dict[str, object]:
        raw = self._health_snapshot().get("value", {})
        qmt = dict(raw.get("qmt", {}) or {}) if isinstance(raw, dict) else {}
        state = qmt.get("state")
        if hasattr(state, "value"):
            qmt["state"] = state.value
        return {
            "process_alive": bool(raw.get("process_alive"))
            if isinstance(raw, dict)
            else False,
            "uptime_seconds": raw.get("uptime_seconds")
            if isinstance(raw, dict)
            else None,
            "backend_type": raw.get("backend_type")
            if isinstance(raw, dict)
            else None,
            "qmt": qmt,
            "strategy_ledger_ready": raw.get("strategy_ledger_ready")
            if isinstance(raw, dict)
            else False,
            "trading_enabled": self.config.strategy_trading_enabled,
            "enabled_strategy_ids": list(self.config.strategy_enabled_ids),
            "dashboard_read_only": True,
        }

    def active_features(self) -> List[str]:
        """返回当前配置启用的功能列表。

        Args:
            None。

        Returns:
            List[str]: 配置启用的功能名称。
        """

        features = []
        if self.adapters.data_adapter:
            features.append("data")
        if self.adapters.broker_adapter:
            features.append("broker")
        if self.strategy_api:
            features.append("strategy_ledger_v1")
        if self.dashboard_server:
            features.append("read_only_dashboard")
        return features

    def _qmt_status_snapshot(self) -> Optional[Dict[str, Any]]:
        """读取 QMT adapter 暴露的 readiness 快照。

        Args:
            None。

        Returns:
            Optional[Dict[str, Any]]: QMT guard 快照；非 QMT server 返回 None。
        """

        for adapter in (self.adapters.broker_adapter, self.adapters.data_adapter):
            status_fn = getattr(adapter, "qmt_status", None)
            if callable(status_fn):
                return status_fn()
        return None

    async def wait_started(self) -> None:
        self._ensure_runtime_events()
        assert self._started is not None
        await self._started.wait()

    def _ensure_runtime_events(self) -> None:
        if self._shutdown is None:
            self._shutdown = asyncio.Event()
        if self._started is None:
            self._started = asyncio.Event()

    def register_session(self, session: ClientSession) -> None:
        if len(self._sessions) >= self.config.max_connections:
            raise RuntimeError("连接数达到上限")
        self._sessions.add(session)

    async def unregister_session(self, session: ClientSession) -> None:
        if session in self._sessions:
            self._sessions.remove(session)
        if self.tick_manager:
            await self.tick_manager.remove_session(session)

    def log_access(
        self,
        session: ClientSession,
        action: Optional[str],
        payload: Optional[Dict[str, Any]],
        status: str,
        duration: float,
        error: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        if not getattr(self.config, "access_log_enabled", True):
            return
        data = payload if isinstance(payload, dict) else {}
        account = data.get("account_key") or session.account_key or "-"
        sub_account = data.get("sub_account_id") or session.sub_account_id or "-"
        base = (
            f"[ACCESS] peer={session.peername} session={session.session_id} "
            f"id={request_id or '-'} action={action or '-'} account={account} sub={sub_account} "
            f"status={status} cost={duration * 1000:.1f}ms"
        )
        if error:
            log.warning(f"{base} error={error}")
        else:
            log.info(base)

    async def handle_request(self, session: ClientSession, action: Optional[str], payload: Dict) -> Dict:
        if not action:
            raise ValueError("缺少 action 字段")
        if action == "data.subscribe":
            if not self.tick_manager:
                raise RuntimeError("数据服务未启用")
            symbols = payload.get("securities") or payload.get("symbols") or []
            return await self.tick_manager.subscribe(session, symbols)
        if action == "data.unsubscribe":
            if not self.tick_manager:
                return {"count": 0}
            return await self.tick_manager.unsubscribe(session, payload.get("securities"))
        if action == "data.unsubscribe_all":
            if not self.tick_manager:
                return {"count": 0}
            return await self.tick_manager.unsubscribe(session, None)
        if action == "admin.health":
            return self._health_snapshot()
        if action == "admin.print_account":
            return await self._admin_print_account(session, payload)
        if action.startswith("data."):
            return await self._dispatch_data(action.split(".", 1)[1], payload)
        if action.startswith("broker."):
            return await self._dispatch_broker(session, action.split(".", 1)[1], payload)
        if action.startswith("strategy."):
            return await self._dispatch_strategy(session, action.split(".", 1)[1], payload)
        raise ValueError(f"未知 action: {action}")

    async def _dispatch_strategy(self, session: ClientSession, method: str, payload: Dict) -> Dict:
        if self.strategy_api is None:
            raise RuntimeError("StrategyLedger未启用")
        account_key = payload.get("account_key") or session.account_key
        sub_account_id = payload.get("sub_account_id") or session.sub_account_id
        if sub_account_id:
            raise ValueError("StrategyLedger首版不支持sub_account_id")
        resolved_key, _ = self.virtual_accounts.resolve(account_key, None)
        account_context = self.router.get(resolved_key)
        if method == "ensure_account":
            return await self.strategy_api.ensure_account(account_context, resolved_key, payload)
        if method == "get_snapshot":
            return await self.strategy_api.get_snapshot(account_context, resolved_key, payload)
        if method == "submit_targets":
            return await self.strategy_api.submit_targets(account_context, resolved_key, payload)
        if method == "get_intent":
            return self.strategy_api.get_intent(payload)
        if method == "cancel_intent":
            return await self.strategy_api.cancel_intent(
                account_context, resolved_key, payload
            )
        if method == "get_reconciliation":
            return self.strategy_api.get_reconciliation(resolved_key, payload)
        if method == "notify_target_buy_plan":
            return self.strategy_api.notify_target_buy_plan(payload)
        raise ValueError(f"未知策略接口: {method}")

    async def _dispatch_data(self, method: str, payload: Dict) -> Dict:
        if not self.adapters.data_adapter:
            raise RuntimeError("数据服务未启用")
        if method == "current_tick":
            fn = getattr(self.adapters.data_adapter, "current_tick", None)
            if fn:
                return await fn(payload)
            snapshot_fn = getattr(self.adapters.data_adapter, "get_snapshot", None)
            if snapshot_fn:
                return await snapshot_fn(payload)
            tick_fn = getattr(self.adapters.data_adapter, "get_current_tick", None)
            if tick_fn:
                security = payload.get("security") or payload.get("stock") or payload.get("stockcode")
                return await tick_fn(security)
            raise ValueError("数据接口 current_tick 未实现")
        fn = getattr(self.adapters.data_adapter, method, None)
        if fn is None:
            fn = getattr(self.adapters.data_adapter, f"get_{method}", None)
        if not fn:
            raise ValueError(f"数据接口 {method} 未实现")
        return await fn(payload)

    async def _dispatch_broker(self, session: ClientSession, method: str, payload: Dict) -> Dict:
        if not self.adapters.broker_adapter:
            raise RuntimeError("券商服务未启用")
        account_key = payload.get("account_key") or session.account_key
        sub_account_id = payload.get("sub_account_id") or session.sub_account_id
        resolved_key, sub_cfg = self.virtual_accounts.resolve(account_key, sub_account_id)
        if sub_account_id and "sub_account_id" not in payload:
            payload["sub_account_id"] = (
                sub_cfg.sub_account_id
                if sub_cfg
                else str(sub_account_id).split("@", 1)[0]
            )
        ctx = self.router.get(resolved_key)
        if method == "place_order":
            cached_result = await self._lookup_idempotent_place_result(resolved_key, sub_cfg, payload)
            if cached_result is not None:
                return cached_result
        if method == "place_order":
            await self._maybe_reject_when_paused(payload)
            await self._maybe_fill_price(payload)
            await self.virtual_accounts.ensure_within_limit(sub_cfg, _estimate_order_value(payload))
        impl = method
        fn = getattr(self.adapters.broker_adapter, impl, None)
        if fn is None:
            aliases = {
                "account": "get_account_info",
                "positions": "get_positions",
                "orders": "list_orders",
                "trades": "list_trades",
                "order_status": "get_order_status",
                "place_order": "place_order",
                "cancel_order": "cancel_order",
            }
            alias = aliases.get(method)
            if alias:
                impl = alias
                fn = getattr(self.adapters.broker_adapter, impl, None)
        if not fn:
            raise ValueError(f"券商接口 {method} 未实现")
        args = self._build_broker_args(impl, ctx, payload)
        if method == "place_order" and resolved_key in self._risk_by_account:
            result = await self._place_order_with_server_risk(
                resolved_key=resolved_key,
                ctx=ctx,
                payload=payload,
                fn=fn,
                args=args,
            )
        elif method == "cancel_order" and resolved_key in self._risk_by_account:
            result = await self._cancel_order_with_server_risk(
                resolved_key=resolved_key,
                payload=payload,
                fn=fn,
                args=args,
            )
        else:
            result = await fn(*args)
        paused_msg = (payload.get("meta") or {}).get("paused_warning")
        if paused_msg:
            log.warning(paused_msg + "（已透传给客户端）")
            try:
                if isinstance(result, dict):
                    result.setdefault("warning", paused_msg)
            except Exception:
                pass
        if method == "place_order":
            await self._store_idempotent_place_result(resolved_key, sub_cfg, payload, result)
        if sub_cfg:
            result = _attach_sub_account_id(result, sub_cfg.sub_account_id)
        return result

    async def _place_order_with_server_risk(
        self,
        *,
        resolved_key: str,
        ctx: AccountContext,
        payload: Dict,
        fn,
        args: Tuple,
    ) -> Dict:
        """在服务端风控保护下执行下单。

        Args:
            resolved_key: 解析后的真实父账户 key。
            ctx: 当前账户上下文。
            payload: 原始请求载荷。
            fn: 实际下单函数。
            args: 实际下单参数。

        Returns:
            Dict: 下单结果。
        """
        risk = self._risk_by_account.get(resolved_key)
        if risk is None:
            return await fn(*args)
        lock = self._risk_locks.setdefault(resolved_key, asyncio.Lock())
        async with lock:
            order_value = _estimate_order_value(payload)
            if order_value and order_value > 0:
                positions = await self.adapters.broker_adapter.get_positions(ctx)
                account_info = await self.adapters.broker_adapter.get_account_info(ctx)
                positions_count = _count_open_positions(positions)
                total_value = _extract_total_value(account_info)
                side = str(payload.get("side") or "BUY").upper()
                action = "buy" if side == "BUY" else "sell"
                risk.check_order(
                    order_value=order_value,
                    current_positions_count=positions_count,
                    security=str(payload.get("security") or ""),
                    total_value=total_value,
                    action=action,
                )
                result = await fn(*args)
                risk.record_trade(order_value=order_value, action=action)
                return result
            return await fn(*args)

    async def _cancel_order_with_server_risk(
        self,
        *,
        resolved_key: str,
        payload: Dict,
        fn,
        args: Tuple,
    ) -> Dict:
        """在服务端风控保护下执行撤单。

        Args:
            resolved_key: 解析后的真实父账户 key。
            payload: 原始请求载荷。
            fn: 实际撤单函数。
            args: 实际撤单参数。

        Returns:
            Dict: 撤单结果。
        """
        risk = self._risk_by_account.get(resolved_key)
        if risk is None:
            return await fn(*args)
        lock = self._risk_locks.setdefault(resolved_key, asyncio.Lock())
        order_id = str(payload.get("order_id") or "")
        async with lock:
            risk.check_cancel(order_id=order_id)
            result = await fn(*args)
            ok = False
            if isinstance(result, dict):
                value = result.get("value")
                if isinstance(value, bool):
                    ok = value
                elif value is None:
                    ok = bool(result.get("success", True))
                else:
                    ok = bool(value)
            else:
                ok = bool(result)
            if ok:
                risk.record_cancel(order_id=order_id)
            return result

    async def _lookup_idempotent_place_result(
        self,
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        payload: Dict,
    ) -> Optional[Dict]:
        key = str(payload.get("idempotency_key") or "").strip()
        if not key:
            return None
        cache_key = (resolved_key, sub_cfg.sub_account_id if sub_cfg else "", key)
        fingerprint = _build_place_order_fingerprint(payload)
        now = time.monotonic()
        async with self._idempotency_lock:
            self._purge_expired_idempotency_entries(now)
            entry = self._idempotency_cache.get(cache_key)
            if entry is None:
                return None
            if entry.fingerprint != fingerprint:
                raise ValueError(f"idempotency_key 冲突: {key}")
            return dict(entry.result)

    async def _store_idempotent_place_result(
        self,
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        payload: Dict,
        result: Dict,
    ) -> None:
        key = str(payload.get("idempotency_key") or "").strip()
        if not key:
            return
        cache_key = (resolved_key, sub_cfg.sub_account_id if sub_cfg else "", key)
        fingerprint = _build_place_order_fingerprint(payload)
        entry = _IdempotencyEntry(
            fingerprint=fingerprint,
            result=dict(result or {}),
            expires_at=time.monotonic() + max(1, int(self.config.idempotency_ttl_seconds or 300)),
        )
        async with self._idempotency_lock:
            self._purge_expired_idempotency_entries(time.monotonic())
            self._idempotency_cache[cache_key] = entry

    def _purge_expired_idempotency_entries(self, now: float) -> None:
        expired = [key for key, value in self._idempotency_cache.items() if value.expires_at <= now]
        for key in expired:
            self._idempotency_cache.pop(key, None)

    async def _maybe_fill_price(self, payload: Dict) -> None:
        """
        若下单缺少 price，尝试用数据服务补充最新成交价。

        市价单不能把该价格写回 protect_price；保护价应由 broker adapter 按买卖方向和默认偏移计算。
        """
        try:
            style = payload.get("style") or {}
            price = style.get("price")
            protect_price = style.get("protect_price")
            if price is not None or protect_price is not None:
                return
            security = payload.get("security")
            if not security or not self.adapters.data_adapter:
                return
            data_adapter = self.adapters.data_adapter
            snapshot = None
            snap_fn = getattr(data_adapter, "get_snapshot", None)
            if callable(snap_fn):
                snapshot = await snap_fn({"security": security})
            if not snapshot and hasattr(data_adapter, "get_current_tick"):
                try:
                    tick_fn = getattr(data_adapter, "get_current_tick")
                    snapshot = await tick_fn(security) if callable(tick_fn) else None
                except Exception:
                    snapshot = None
            price = None
            if isinstance(snapshot, dict):
                price = snapshot.get("last_price") or snapshot.get("lastPrice") or snapshot.get("price")
            if price is None and callable(getattr(data_adapter, "get_history", None)):
                hist = await data_adapter.get_history({"security": security, "count": 1, "frequency": "1m"})
                records = hist.get("records") if isinstance(hist, dict) else None
                if records:
                    last = records[-1]
                    if isinstance(last, (list, tuple)) and last:
                        price = last[-1] if isinstance(last[-1], (int, float)) else None
            if price is not None:
                if style.get("type", "").lower() == "market":
                    payload["_estimated_price"] = float(price)
                else:
                    style["price"] = float(price)
                    payload["style"] = style
                    payload.setdefault("price", float(price))
        except Exception:
            # 补价失败不终止下单，交由后续逻辑处理
            pass

    async def _maybe_reject_when_paused(self, payload: Dict) -> None:
        """
        下单前检查停牌，避免静默被券商拒绝；仅在数据服务可用时生效。
        """
        data_adapter = self.adapters.data_adapter
        if not data_adapter:
            return
        security = payload.get("security")
        if not security:
            return

        snapshot = None
        for fn_name in ("get_live_current", "get_snapshot", "get_current_tick"):
            fn = getattr(data_adapter, fn_name, None)
            if not callable(fn):
                continue
            try:
                if fn_name == "get_current_tick":
                    snapshot = await fn(security)  # type: ignore[misc,arg-type]
                else:
                    snapshot = await fn({"security": security})  # type: ignore[arg-type]
                break
            except Exception:
                continue

        if not isinstance(snapshot, dict):
            return

        paused_flag = snapshot.get("paused")
        if paused_flag is None:
            status = str(snapshot.get("status") or "").lower()
            paused_flag = status in {"paused", "halt", "停牌"}

        if paused_flag:
            msg = f"{security} 停牌，拒绝远程委托"
            log.warning(msg + "（仅警告，不阻塞委托）")
            payload.setdefault("meta", {})["paused_warning"] = msg

    def _build_broker_args(self, method: str, ctx: AccountContext, payload: Optional[Dict]) -> Tuple:
        payload = payload or {}
        if method in ("get_account_info", "get_positions"):
            return (ctx,)
        if method == "list_orders":
            filters = payload.get("filters")
            return (ctx, filters or payload)
        if method == "list_trades":
            filters = payload.get("filters")
            return (ctx, filters or payload)
        if method == "get_order_status":
            order_id = payload.get("order_id")
            if not order_id:
                raise ValueError("缺少 order_id")
            return (ctx, order_id)
        if method == "place_order":
            return (ctx, payload)
        if method == "cancel_order":
            order_id = payload.get("order_id")
            if not order_id:
                raise ValueError("缺少 order_id")
            return (ctx, order_id)
        return (ctx, payload)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        address = peer[0] if isinstance(peer, (list, tuple)) else str(peer)
        log.info(f"[CONN] 新连接: {address}, 当前活跃会话数: {len(self._sessions)}")
        if not self._is_ip_allowed(address):
            log.warning(f"拒绝未授权 IP: {address}")
            writer.close()
            await writer.wait_closed()
            return
        session = ClientSession(self, reader, writer, address)
        try:
            await session.run()
        except Exception as exc:
            log.error(f"[CONN] 会话 {session.session_id} 运行异常: {exc}")
        finally:
            log.info(f"[CONN] 连接关闭: {address}, session={session.session_id}")
            await session.close()

    async def _start_components(self) -> None:
        if self.adapters.broker_adapter:
            await self.adapters.broker_adapter.start()
        if self.strategy_api:
            ready = await self._check_strategy_startup(log_failure=True)
            if not ready:
                self._strategy_startup_task = asyncio.create_task(
                    self._retry_strategy_startup(),
                    name="strategy-ledger-startup-retry",
                )
        if self.tick_manager:
            await self.tick_manager.start()
        expire_intents = getattr(
            self.strategy_api, "expire_previous_day_intents", None
        )
        if callable(expire_intents) and self._strategy_expiry_task is None:
            self._strategy_expiry_task = asyncio.create_task(
                self._strategy_expiry_loop(),
                name="strategy-ledger-midnight-expiry",
            )

    async def _check_strategy_startup(self, *, log_failure: bool) -> bool:
        """Reconcile existing strategy accounts after the broker becomes ready."""
        if self.strategy_api is None:
            return True
        all_ready = True
        for account_context in self.router.list_accounts():
            account_key = account_context.config.key or "default"
            try:
                ready = await self.strategy_api.startup_check(
                    account_context, account_key
                )
            except Exception as exc:
                ready = False
                if log_failure:
                    log.error("StrategyLedger启动对账失败，保持只读: %s", exc)
            if not ready:
                all_ready = False
                if log_failure:
                    log.warning(
                        "StrategyLedger账户%s启动未就绪，真实组合下单保持阻断",
                        account_key,
                    )
        return all_ready

    async def _retry_strategy_startup(self) -> None:
        """Retry the one-time startup reconciliation while QMT is connecting."""
        while True:
            await asyncio.sleep(_STRATEGY_STARTUP_RETRY_SECONDS)
            if await self._check_strategy_startup(log_failure=False):
                log.info("QMT连接就绪，StrategyLedger启动对账已自动恢复")
                return

    async def _strategy_expiry_loop(self) -> None:
        """Close previous-day target orders at 00:00 Asia/Shanghai."""

        assert self.strategy_api is not None
        while True:
            now = datetime.now(SHANGHAI_TZ)
            try:
                result = await self.strategy_api.expire_previous_day_intents(
                    now
                )
            except Exception:
                log.exception("StrategyLedger 午夜撤单任务异常")
                delay = _STRATEGY_EXPIRY_RETRY_SECONDS
            else:
                if result.expired:
                    log.info(
                        "StrategyLedger 午夜撤单完成 | 过期意图=%s "
                        "撤单请求=%s 已关闭=%s 待确认=%s 未绑定=%s 错误=%s",
                        result.expired,
                        result.cancel_requests,
                        result.canceled,
                        result.pending,
                        result.unbound,
                        result.errors,
                    )
                if result.pending or result.unbound or result.errors:
                    delay = _STRATEGY_EXPIRY_RETRY_SECONDS
                else:
                    next_midnight = datetime.combine(
                        now.date() + timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=SHANGHAI_TZ,
                    )
                    delay = max(
                        0.5, (next_midnight - now).total_seconds()
                    )
            await asyncio.sleep(delay)

    def _health_snapshot(self) -> Dict:
        value = {
            "process_alive": True,
            "uptime_seconds": max(0.0, time.time() - self._created_at),
            "sessions": len(self._sessions),
            "accounts": [ctx.config.key for ctx in self.router.list_accounts()],
            "features": self.active_features(),
        }
        qmt_status = self._qmt_status_snapshot()
        if qmt_status is not None:
            value["qmt"] = qmt_status
            backend_type = qmt_status.get("backend_type") if isinstance(qmt_status, dict) else None
            if backend_type:
                value["backend_type"] = backend_type
            big_qmt_gateway = qmt_status.get("big_qmt_gateway") if isinstance(qmt_status, dict) else None
            if big_qmt_gateway is not None:
                value["big_qmt_gateway"] = big_qmt_gateway
        if self.strategy_api is not None:
            value["strategy_ledger_ready"] = self.strategy_api.startup_ready
        return {
            "dtype": "dict",
            "value": value,
        }

    def _prepare_allowlist(self, allowlist: List[str]):
        networks = []
        for entry in allowlist:
            try:
                if "/" in entry:
                    networks.append(ipaddress.ip_network(entry, strict=False))
                else:
                    networks.append(ipaddress.ip_network(entry + "/32"))
            except ValueError:
                continue
        return networks

    def _is_ip_allowed(self, ip: Optional[str]) -> bool:
        if not self._ip_allowlist or not ip:
            return True
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in net for net in self._ip_allowlist)

    async def _admin_print_account(self, session: ClientSession, payload: Dict) -> Dict:
        if not self.adapters.broker_adapter:
            raise RuntimeError("券商服务未启用")
        account_key = payload.get("account_key") or session.account_key
        sub_account_id = payload.get("sub_account_id") or session.sub_account_id
        resolved_key, sub_cfg = self.virtual_accounts.resolve(account_key, sub_account_id)
        ctx = self.router.get(resolved_key)
        try:
            info = await self.adapters.broker_adapter.get_account_info(ctx)
            positions = await self.adapters.broker_adapter.get_positions(ctx)
        except Exception as exc:
            raise RuntimeError(f"获取账户信息失败: {exc}")

        # 适配 {"dtype":"dict","value":{...}} 或直接 dict
        if isinstance(info, dict) and info.get("dtype") == "dict" and "value" in info:
            info_dict = dict(info.get("value") or {})
        else:
            info_dict = dict(info or {})

        snapshot = {
            "available_cash": info_dict.get("available_cash"),
            "total_value": info_dict.get("total_value"),
            "positions": positions or [],
        }
        limit = int(payload.get("limit", 20) or 20)
        text = render_account_overview(snapshot, limit=limit)
        if self.config.log_account_snapshot:
            log.info("\n%s", text)
        result = {"dtype": "text", "value": text, "account_key": resolved_key}
        if sub_cfg:
            result["sub_account_id"] = sub_cfg.sub_account_id
        return result


def _estimate_order_value(payload: Dict) -> Optional[float]:
    try:
        amount = abs(float(payload.get("amount") or payload.get("volume") or 0))
    except (TypeError, ValueError):
        amount = 0.0
    style = payload.get("style") or {}
    price = (
        style.get("price")
        or style.get("protect_price")
        or payload.get("price")
        or payload.get("_estimated_price")
    )
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    if amount and price:
        return amount * price
    return None


def _attach_sub_account_id(result: Any, sub_account_id: str) -> Any:
    """给 broker 返回结果追加子账户标识。

    Args:
        result: broker action 的返回值，通常是 dict 或 list[dict]。
        sub_account_id: 已解析出的虚拟子账户 ID。

    Returns:
        Any: 保持原返回结构的结果；dict/list[dict] 会追加 `sub_account_id`。

    Side Effects:
        对 dict/list[dict] 做原地补充，避免旧调用方的返回形态发生变化。
    """

    if isinstance(result, dict):
        result.setdefault("sub_account_id", sub_account_id)
        return result
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict):
                item.setdefault("sub_account_id", sub_account_id)
        return result
    return result


def _build_place_order_fingerprint(payload: Dict) -> str:
    style = payload.get("style") or {}
    normalized = {
        "account_key": str(payload.get("account_key") or ""),
        "sub_account_id": str(payload.get("sub_account_id") or ""),
        "security": str(payload.get("security") or ""),
        "side": str(payload.get("side") or ""),
        "amount": int(payload.get("amount") or payload.get("volume") or 0),
        "style": {
            "type": str(style.get("type") or "limit"),
            "price": style.get("price"),
            "protect_price": style.get("protect_price"),
        },
    }
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)


def _count_open_positions(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            amount = int(row.get("amount") or row.get("volume") or 0)
        except (TypeError, ValueError):
            amount = 0
        if amount > 0:
            count += 1
    return count


def _extract_total_value(payload: Any) -> float:
    if isinstance(payload, dict) and payload.get("dtype") == "dict" and "value" in payload:
        payload = payload.get("value") or {}
    if not isinstance(payload, dict):
        return 0.0
    candidates = (
        payload.get("total_value"),
        payload.get("total_asset"),
        payload.get("portfolio_value"),
    )
    for value in candidates:
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0
