"""Full QMT transport adapter. Execution policy stays in StrategyLedger.

The HTTP backend remains the default. This optional same-host bridge reuses its
field normalization and submission confirmation, and the existing history DB.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time

from bullet_trade.data.api import get_security_tplus_override
from bullet_trade.utils.env_loader import get_env
from ..big_qmt_bridge import QmtBridge
from ..strategy.broker_history import SQLiteBrokerHistoryStore, merge_broker_rows
from .base import AdapterBundle, BROKER_CALL_MARKER_KEY
from .big_qmt import (
    BigQmtBrokerAdapter, BigQmtDataAdapter, BigQmtGatewayClient, BigQmtGatewayError,
    _filter_orders, _filter_trades, _normalize_order, _normalize_trade,
    _normalize_snapshot_tick, load_big_qmt_gateway_config,
)

log = logging.getLogger(__name__)


class BridgeClient(BigQmtGatewayClient):
    def __init__(self, config, bridge):
        super().__init__(config)
        self.bridge = bridge

    async def get(self, path):
        return await self.post(path)

    async def post(self, path, payload=None, *, timeout_seconds=None):
        payload = dict(payload or {})
        payload.pop(BROKER_CALL_MARKER_KEY, None)
        try:
            result = await self.bridge.request(path, payload, timeout_seconds or self.config.timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise BigQmtGatewayError("Big QMT bridge reply timed out; reconcile before retry",
                                     code="SUBMIT_UNKNOWN" if path in {"/place_order", "/cancel_order"} else "BRIDGE_TIMEOUT") from exc
        self._record_success()
        return result

    def qmt_status(self):
        health = self.bridge.health
        return dict(backend_type="big_qmt", transport="bridge", ready=self.bridge.ready,
                    state="ready" if self.bridge.ready else "unavailable",
                    gateway_url="tcp://127.0.0.1:%s" % self.bridge.port,
                    trading_enabled=health.get("trading_enabled"),
                    cancel_order_enabled=health.get("cancel_enabled"),
                    health_cache_age_seconds=max(0, time.monotonic() - self.bridge.last_seen),
                    actions=self.config.action_status, big_qmt_gateway=dict(health))


class BridgeDataAdapter(BigQmtDataAdapter):
    def __init__(self, client):
        super().__init__(client)
        self._tick_listeners = []
        self._owners = {}
        self._applied = None
        self._subscription_lock = asyncio.Lock()
        self._restore_task = None
        client.bridge.listeners.append(self._event)

    async def start(self):
        await self.client.bridge.start()

    async def stop(self):
        if self._restore_task:
            self._restore_task.cancel()
            await asyncio.gather(self._restore_task, return_exceptions=True)
            self._restore_task = None
        await self.client.bridge.stop()

    def add_tick_listener(self, callback):
        if callback not in self._tick_listeners:
            self._tick_listeners.append(callback)

    def _event(self, event, payload):
        if event == "tick":
            for listener in tuple(self._tick_listeners):
                try:
                    listener(payload)
                except Exception:
                    log.exception("Big QMT tick listener failed")
        elif event == "connected":
            self._applied = None
            if self._restore_task is None or self._restore_task.done():
                self._restore_task = asyncio.create_task(self._restore_subscriptions())
        elif event == "disconnected":
            self._applied = None

    async def _restore_subscriptions(self):
        try:
            await self._sync_subscriptions()
        except Exception:
            log.exception("Big QMT quote subscriptions not ready; execution will retry")

    async def _sync_subscriptions(self):
        async with self._subscription_lock:
            desired = set().union(*self._owners.values()) if self._owners else set()
            if desired != self._applied:
                await self.client.post("/data/subscriptions", {"symbols": sorted(desired)})
                self._applied = desired

    async def replace_execution_quotes(self, owner, symbols):
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        if cleaned:
            self._owners[str(owner)] = cleaned
        else:
            self._owners.pop(str(owner), None)
        await self._sync_subscriptions()

    async def get_current_tick(self, symbol):
        raw = await self.client.post("/data/current_tick", {"security": symbol})
        # Keep exchange limits/tick size and source timestamp alongside base fields.
        return dict(raw, **_normalize_snapshot_tick(raw, symbol)) if raw else None

    async def get_snapshot(self, payload):
        return await self.get_current_tick(payload["security"])

    async def get_tplus(self, security):
        override = get_security_tplus_override(security)
        if override is not None:
            return int(override)
        return int(await self.client.post("/data/tplus", {"security": security}))

    async def get_history(self, payload):
        raise BigQmtGatewayError("Bridge is for live execution; keep historical strategy data in JQ",
                                 code="NOT_IMPLEMENTED", broker_called=False)


class BridgeBrokerAdapter(BigQmtBrokerAdapter):
    def __init__(self, config, router, client):
        super().__init__(config, router, client)
        self._listeners = []
        self._history = SQLiteBrokerHistoryStore(config.strategy_database_path) if config.strategy_database_path else None
        self._account = router.list_accounts()[0]
        client.bridge.listeners.append(self._event)

    async def start(self):
        await self.client.bridge.start()

    async def stop(self):
        await self.client.bridge.stop()

    def add_event_listener(self, callback):
        if callback not in self._listeners:
            self._listeners.append(callback)

    def has_durable_broker_history(self):
        return self._history is not None

    def _event(self, event, payload):
        if event == "tick":
            return
        key = self._account.config.key or "default"
        if isinstance(payload, dict) and event in {"order", "trade"}:
            payload = (_normalize_order if event == "order" else _normalize_trade)(payload)
            if self._history:
                # A persistence failure is surfaced, never silently treated as stored.
                getattr(self._history, "record_" + event)(key, payload)
        for callback in tuple(self._listeners):
            try:
                callback(key, event, payload)
            except Exception:
                log.exception("Big QMT broker listener failed: %s", event)

    async def list_orders(self, account, filters=None):
        current = await super().list_orders(account, {})
        if self._history:
            self._history.record_orders(account.config.key or "default", current)
            if (filters or {}).get("include_history"):
                current = list(merge_broker_rows(current, self._history.list_orders(account.config.key or "default"), "order_id"))
        return _filter_orders(current, filters or {})

    async def list_trades(self, account, filters=None):
        current = await super().list_trades(account, {})
        if self._history:
            self._history.record_trades(account.config.key or "default", current)
            if (filters or {}).get("include_history"):
                current = list(merge_broker_rows(current, self._history.list_trades(account.config.key or "default"), "trade_id"))
        return _filter_trades(current, filters or {})

    async def place_order(self, account, payload):
        request = dict(payload)
        # Fork client_tag already fits QMT's 23-byte field. Preserve it verbatim,
        # so callbacks still identify the operation after both processes restart.
        tag = str(request.get("order_remark") or request.get("remark") or "")
        if re.fullmatch(r"bt:[0-9a-f]{6}:[0-9a-f]{12}", tag):
            request["qmt_user_order_id"] = tag
        style = dict(request.get("style") or {})
        if style.get("type") == "market" and not (style.get("market_type") or request.get("market_type")):
            # Match the former xtquant default: SH=42 / SZ=47 five-level IOC.
            style["market_type"] = "five_level_ioc"
            request["style"] = style
        # This only waits for acknowledgement, not fills. Chasing stays server-side.
        if float(request.get("wait_timeout") or 0) <= 0:
            request["wait_timeout"] = 3.0
        return await super().place_order(account, request)


def build_bridge_bundle(config, router):
    accounts = router.list_accounts()
    if len(accounts) != 1 or accounts[0].config.account_type.upper() != "STOCK":
        raise ValueError("Big QMT bridge requires exactly one STOCK broker account; multiple strategy ledgers are supported")
    gateway = load_big_qmt_gateway_config(config)
    for name, status in gateway.action_status.items():
        if name in {"data.history", "data.ensure_cache", "data.get_all_securities", "data.get_index_stocks", "data.get_split_dividend"}:
            status.update(status="unavailable", reason="not exposed by live QMT bridge")
    account = accounts[0].config
    bridge = QmtBridge(gateway.password, account.account_id, account.account_type,
                       int(get_env("BIG_QMT_BRIDGE_PORT", "9001")))
    client = BridgeClient(gateway, bridge)
    return AdapterBundle(data_adapter=BridgeDataAdapter(client) if config.enable_data else None,
                         broker_adapter=BridgeBrokerAdapter(config, router, client) if config.enable_broker else None)
