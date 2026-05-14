"""
Tests for the GUI HTTP/WS routes — REST control surface and the events WS
protocol (subscribe / unsubscribe / history).
"""

import json

import pytest
import pytest_asyncio
from litestar.testing import AsyncTestClient

from noob.gui.view import make_view_app

TUBE_ID = "gui-runner-demo"

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client():
    async with AsyncTestClient(app=make_view_app()) as c:
        yield c


class TestRestControl:
    async def test_status_starts_uninitialized(self, client: AsyncTestClient) -> None:
        res = await client.get(f"/run/{TUBE_ID}/status")
        assert res.status_code == 200
        assert res.json()["state"] == "uninitialized"

    async def test_init_then_deinit(self, client: AsyncTestClient) -> None:
        res = await client.post(f"/run/{TUBE_ID}/init")
        assert res.status_code == 200, res.text
        assert res.json()["state"] == "initialized"
        assert res.json()["tube_id"] == TUBE_ID

        res = await client.post(f"/run/{TUBE_ID}/deinit")
        assert res.status_code == 200
        assert res.json()["state"] == "uninitialized"

    async def test_start_requires_init(self, client: AsyncTestClient) -> None:
        res = await client.post(f"/run/{TUBE_ID}/start")
        assert res.status_code == 409

    async def test_start_then_stop(self, client: AsyncTestClient) -> None:
        await client.post(f"/run/{TUBE_ID}/init")
        res = await client.post(f"/run/{TUBE_ID}/start")
        assert res.status_code == 200, res.text
        assert res.json()["state"] == "running"
        res = await client.post(f"/run/{TUBE_ID}/stop")
        assert res.json()["state"] == "initialized"
        await client.post(f"/run/{TUBE_ID}/deinit")


def _needs_binary(envelope: dict) -> bool:
    v = envelope.get("value")
    return isinstance(v, dict) and v.get("kind") in ("ndarray", "bytes")


class TestEventsWS:
    async def test_initial_status_message(self, client: AsyncTestClient) -> None:
        ws = await client.websocket_connect(f"/run/{TUBE_ID}/events")
        with ws:
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "status"
            assert msg["state"] == "uninitialized"

    async def test_event_metadata_broadcast(self, client: AsyncTestClient) -> None:
        await client.post(f"/run/{TUBE_ID}/init")
        ws = await client.websocket_connect(f"/run/{TUBE_ID}/events")
        with ws:
            _ = ws.receive_text()  # initial status
            await client.post(f"/run/{TUBE_ID}/start")
            seen_types: set[str] = set()
            seen_nodes: set[str] = set()
            for _ in range(40):
                msg = json.loads(ws.receive_text())
                seen_types.add(msg.get("type"))
                if msg.get("type") == "event":
                    seen_nodes.add(msg["node_id"])
                if _needs_binary(msg):
                    ws.receive_bytes()
                if "event" in seen_types and len(seen_nodes) >= 2:
                    break
            await client.post(f"/run/{TUBE_ID}/stop")
            await client.post(f"/run/{TUBE_ID}/deinit")

        assert "event" in seen_types
        assert seen_nodes.issubset({"counter", "sine_x", "sine_y", "gradient"})
        assert len(seen_nodes) >= 2

    async def test_subscribe_streams_value_with_binary(
        self, client: AsyncTestClient
    ) -> None:
        await client.post(f"/run/{TUBE_ID}/init")
        ws = await client.websocket_connect(f"/run/{TUBE_ID}/events")
        with ws:
            _ = ws.receive_text()  # initial status
            ws.send_json(
                {
                    "op": "subscribe_values",
                    "node_id": "gradient",
                    "signal": "frame",
                }
            )
            await client.post(f"/run/{TUBE_ID}/start")
            received_value = None
            received_payload = None
            for _ in range(200):
                msg = json.loads(ws.receive_text())
                if _needs_binary(msg):
                    payload = ws.receive_bytes()
                    if (
                        msg.get("type") == "value"
                        and msg.get("node_id") == "gradient"
                        and msg.get("signal") == "frame"
                    ):
                        received_value = msg
                        received_payload = payload
                        break
            await client.post(f"/run/{TUBE_ID}/stop")
            await client.post(f"/run/{TUBE_ID}/deinit")

        assert received_value is not None
        assert received_value["value"]["dtype"] == "uint8"
        assert received_value["value"]["shape"] == [4, 8]
        assert received_payload is not None
        assert len(received_payload) == 4 * 8

    async def test_history_round_trip(self, client: AsyncTestClient) -> None:
        await client.post(f"/run/{TUBE_ID}/init")
        ws = await client.websocket_connect(f"/run/{TUBE_ID}/events")
        with ws:
            _ = ws.receive_text()  # initial status
            await client.post(f"/run/{TUBE_ID}/start")
            # let some events accumulate; consume binary follow-ups
            for _ in range(30):
                msg = json.loads(ws.receive_text())
                if _needs_binary(msg):
                    ws.receive_bytes()
            await client.post(f"/run/{TUBE_ID}/stop")

            ws.send_json(
                {
                    "op": "history",
                    "request_id": "abc",
                    "node_id": "sine_x",
                    "signal": "value",
                    "before_id": None,
                    "limit": 5,
                }
            )
            messages = []
            for _ in range(100):
                msg = json.loads(ws.receive_text())
                if _needs_binary(msg):
                    ws.receive_bytes()
                messages.append(msg)
                if (
                    msg.get("type") == "history_end"
                    and msg.get("request_id") == "abc"
                ):
                    break

            await client.post(f"/run/{TUBE_ID}/deinit")

        begin = [m for m in messages if m.get("type") == "history_begin"]
        values = [m for m in messages if m.get("type") == "history_value"]
        end = [m for m in messages if m.get("type") == "history_end"]
        assert len(begin) == 1
        assert begin[0]["request_id"] == "abc"
        assert begin[0]["count"] == len(values)
        assert len(end) == 1
        for v in values:
            assert v["node_id"] == "sine_x"
            assert v["signal"] == "value"
            assert v["value"]["kind"] == "json"
