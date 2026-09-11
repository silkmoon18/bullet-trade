"""Local command/event transport; QMT owns the client, never a blocking server.

No trading policy or account ledger lives here. A lost write is never replayed.
The same-host deadline prevents a queued command executing after its window.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict
from uuid import uuid4

from .adapters.big_qmt import BigQmtGatewayError

log = logging.getLogger(__name__)
MAX_FRAME = 2 * 1024 * 1024


@dataclass
class PendingCommand:
    future: asyncio.Future
    write: bool


class QmtBridge:
    """One local QMT connection, bounded requests, callback fan-out."""

    def __init__(self, token: str, account_id: str, account_type: str, port: int = 9001):
        if not token or not account_id:
            raise ValueError("Big QMT bridge requires token and account_id")
        self.token, self.account_id = token, account_id
        self.account_type, self.port = account_type.upper(), port
        self.server = self.writer = None
        self.pending: Dict[str, PendingCommand] = {}
        self.listeners = []
        self.health: Dict[str, Any] = {}
        self.last_seen = 0.0
        self.tasks = set()

    @property
    def connected(self):
        return self.writer is not None and not self.writer.is_closing()

    @property
    def ready(self):
        return self.connected and time.monotonic() - self.last_seen < 10 and self.health.get("ready") is True

    async def start(self):
        if self.server is None:
            self.server = await asyncio.start_server(self._accept, "127.0.0.1", self.port, limit=MAX_FRAME)
            self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        self._disconnect()
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def emit(self, event, payload=None):
        for callback in tuple(self.listeners):
            try:
                callback(event, payload)
            except Exception:
                log.exception("Big QMT bridge event delivery failed: %s", event)

    def _disconnect(self):
        writer, self.writer = self.writer, None
        self.health = {}
        if writer is not None:
            writer.close()
        for pending in self.pending.values():
            if not pending.future.done():
                pending.future.set_exception(BigQmtGatewayError(
                    "QMT bridge disconnected; query orders before any retry",
                    code="SUBMIT_UNKNOWN" if pending.write else "BRIDGE_OFFLINE",
                    broker_called=None,
                ))
        self.pending.clear()
        if writer is not None:
            self.emit("disconnected")

    async def _accept(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        accepted = False
        try:
            hello = json.loads(await asyncio.wait_for(reader.readline(), 5))
            if not isinstance(hello, dict):
                raise ValueError("Invalid handshake")
            valid = (
                hello.get("type") == "hello" and hello.get("version") == 1
                and hmac.compare_digest(str(hello.get("token", "")).encode(), self.token.encode())
                and str(hello.get("account_id", "")) == self.account_id
                and str(hello.get("account_type", "")).upper() == self.account_type
            )
            if not valid or self.connected:
                return
            self.writer, accepted = writer, True
            self.last_seen = time.monotonic()
            self.health = dict(hello.get("health") or {})
            writer.write(b'{"type":"welcome","version":1}\n')
            await writer.drain()
            self.emit("connected")
            while self.writer is writer:
                raw = await asyncio.wait_for(reader.readline(), 10)
                if not raw:
                    break
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise ValueError("Invalid QMT bridge message")
                self.last_seen = time.monotonic()
                kind = message.get("type")
                if kind == "heartbeat":
                    was_ready = self.health.get("ready") is True
                    self.health = dict(message.get("health") or {})
                    if was_ready != (self.health.get("ready") is True):
                        self.emit("connected" if self.health.get("ready") else "disconnected")
                elif kind == "response":
                    pending = self.pending.get(str(message.get("id")))
                    if pending and not pending.future.done():
                        if message.get("ok") is True:
                            pending.future.set_result(message.get("value"))
                        else:
                            pending.future.set_exception(BigQmtGatewayError(
                                str(message.get("error") or "QMT command failed"),
                                code=str(message.get("code") or "QMT_ACTION_FAILED"),
                                broker_called=message.get("broker_called"),
                            ))
                elif kind == "event":
                    self.emit(str(message.get("event")), message.get("payload"))
                else:
                    raise ValueError("Unsupported QMT bridge message")
        except (ValueError, OSError, asyncio.TimeoutError):
            log.warning("Big QMT bridge connection closed or handshake rejected")
        finally:
            if accepted and self.writer is writer:
                self._disconnect()
            writer.close()
            self.tasks.discard(task)

    async def request(self, action, payload, timeout=10.0):
        write = action in {"/place_order", "/cancel_order"}
        if not self.connected or (write and not self.ready):
            raise BigQmtGatewayError("QMT bridge not ready", code="QMT_NOT_READY", broker_called=False)
        if len(self.pending) >= 100:
            raise BigQmtGatewayError("QMT bridge busy", code="GATEWAY_BUSY", broker_called=False)
        command_id = uuid4().hex
        frame = json.dumps({
            "type": "command", "id": command_id, "action": action,
            "payload": payload, "deadline": time.time() + timeout,
        }, ensure_ascii=True, allow_nan=False).encode() + b"\n"
        if len(frame) > MAX_FRAME:
            raise ValueError("QMT bridge command too large")
        future = asyncio.get_running_loop().create_future()
        self.pending[command_id] = PendingCommand(future, write)
        try:
            self.writer.write(frame)
            # Waiting for a reply also bounds a non-reading peer's drain.
            return await asyncio.wait_for(future, timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # Drop buffered commands on both ends; never resend an uncertain write.
            self._disconnect()
            raise
        except OSError as exc:
            future.cancel()
            self._disconnect()
            raise BigQmtGatewayError("QMT transport failed", code="SUBMIT_UNKNOWN" if write else "BRIDGE_OFFLINE") from exc
        finally:
            self.pending.pop(command_id, None)
