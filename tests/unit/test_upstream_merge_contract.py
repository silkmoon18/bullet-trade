"""Fork contracts across the 0.10.0b3 merge, without broker access."""
import asyncio
from types import SimpleNamespace

import pytest

from bullet_trade.remote.connection import RemoteQmtConnection, RemoteServerError, RemoteSubmissionUnknownError
from bullet_trade.server.config import build_server_config


@pytest.mark.parametrize("called", [False, True, None])
def test_remote_preserves_pre_broker_rejection(called):
    conn = RemoteQmtConnection("127.0.0.1", 0, "test")
    conn._connected.set()
    sent = []

    async def send(message):
        sent.append(message)
        conn._pending[message["id"]].set_exception(
            RemoteServerError("REQUEST_FAILED", "rejection evidence", broker_called=called)
        )

    conn._send = send
    expected = RemoteServerError if called is False else RemoteSubmissionUnknownError
    with pytest.raises(expected, match="rejection evidence"):
        asyncio.run(conn._request_async("broker.place_order", {"idempotency_key": "stable"}))
    assert len(sent) == 1


def test_fork_config_and_upstream_write_limit_coexist(monkeypatch):
    monkeypatch.setenv("QMT_STRATEGY_ENABLED_IDS", "one,two")
    monkeypatch.setenv("QMT_STRATEGY_TRADING_ENABLED", "false")
    monkeypatch.setenv("QMT_SERVER_IDEMPOTENCY_MAX_ENTRIES", "123")
    cfg = build_server_config(SimpleNamespace())
    assert cfg.strategy_enabled_ids == ["one", "two"]
    assert not cfg.strategy_trading_enabled
    assert cfg.idempotency_max_entries == 123
