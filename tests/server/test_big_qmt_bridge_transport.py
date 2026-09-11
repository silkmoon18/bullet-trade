import asyncio
import json

import pytest

from bullet_trade.server.big_qmt_bridge import QmtBridge
from bullet_trade.server.adapters.big_qmt import BigQmtGatewayError


async def connect(bridge, **overrides):
    reader, writer = await asyncio.open_connection("127.0.0.1", bridge.port)
    hello = dict(type="hello", version=1, token="test", account_id="fake", account_type="STOCK", health={"ready": True})
    hello.update(overrides)
    writer.write(json.dumps(hello).encode() + b"\n")
    await writer.drain()
    welcome = await asyncio.wait_for(reader.readline(), 1)
    return reader, writer, welcome


@pytest.mark.asyncio
async def test_roundtrip_and_events():
    bridge = QmtBridge("test", "fake", "STOCK", port=0)
    events = []
    bridge.listeners.append(lambda *args: events.append(args))
    await bridge.start()
    try:
        reader, writer, welcome = await connect(bridge)
        assert json.loads(welcome)["type"] == "welcome" and bridge.ready
        request = asyncio.create_task(bridge.request("/account", {}))
        command = json.loads(await reader.readline())
        writer.write(json.dumps(dict(type="response", id=command["id"], ok=True, value={"available_cash": 100})).encode() + b"\n")
        writer.write(b'{"type":"event","event":"trade","payload":{"trade_id":"fake-fill"}}\n')
        await writer.drain()
        assert await request == {"available_cash": 100}
        await asyncio.sleep(.01)
        assert events[-1] == ("trade", {"trade_id": "fake-fill"})
        writer.close()
    finally:
        await bridge.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{"token": "bad"}, {"account_id": "other"}, {"account_type": "CREDIT"}])
async def test_wrong_identity_not_accepted(overrides):
    bridge = QmtBridge("test", "fake", "STOCK", port=0)
    await bridge.start()
    try:
        _, writer, welcome = await connect(bridge, **overrides)
        assert not welcome and not bridge.ready
        writer.close()
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_lost_write_is_not_replayed_on_reconnect():
    bridge = QmtBridge("test", "fake", "STOCK", port=0)
    await bridge.start()
    try:
        reader, writer, _ = await connect(bridge)
        request = asyncio.create_task(bridge.request("/place_order", {"amount": 100}, timeout=.05))
        command = json.loads(await reader.readline())
        assert command["action"] == "/place_order" and command["deadline"] > 0
        with pytest.raises(asyncio.TimeoutError):
            await request
        writer.close()
        reader2, writer2, _ = await connect(bridge)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(reader2.readline(), .05)
        writer2.close()
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_disconnect_and_explicit_rejection():
    bridge = QmtBridge("test", "fake", "STOCK", port=0)
    await bridge.start()
    try:
        reader, writer, _ = await connect(bridge)
        request = asyncio.create_task(bridge.request("/place_order", {}))
        command = json.loads(await reader.readline())
        writer.write(json.dumps(dict(type="response", id=command["id"], ok=False, code="TRADING_DISABLED", broker_called=False)).encode() + b"\n")
        await writer.drain()
        with pytest.raises(BigQmtGatewayError) as exc:
            await request
        assert exc.value.broker_called is False
        request = asyncio.create_task(bridge.request("/place_order", {}))
        await reader.readline()
        writer.close()
        with pytest.raises(BigQmtGatewayError) as exc:
            await request
        assert exc.value.code == "SUBMIT_UNKNOWN" and exc.value.broker_called is None
    finally:
        await bridge.stop()
