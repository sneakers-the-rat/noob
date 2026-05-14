"""
Tests for the GUI :class:`.RunnerManager` -- lifecycle, subscriber fan-out,
and history pagination.
"""

import asyncio
import json

import pytest

from noob.gui.runner_manager import RunnerManager, Subscriber


TUBE_ID = "gui-runner-demo"


async def _drain_until(
    sub: Subscriber, predicate, timeout: float = 2.0
) -> list:
    """Pull frames off a subscriber until `predicate(frames)` is True."""
    frames: list = []
    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate(frames):
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            frame = await asyncio.wait_for(sub.queue.get(), timeout=remaining)
            frames.append(frame)
        except asyncio.TimeoutError:
            break
    return frames


def _parse(frames):
    return [json.loads(f.text) for f in frames]


@pytest.mark.asyncio
async def test_init_deinit_round_trip() -> None:
    mgr = RunnerManager()
    assert mgr.status()["state"] == "uninitialized"

    await mgr.init(TUBE_ID)
    assert mgr.status()["state"] == "initialized"
    assert mgr.tube_id == TUBE_ID

    await mgr.deinit()
    assert mgr.status()["state"] == "uninitialized"
    assert mgr.tube_id is None


@pytest.mark.asyncio
async def test_start_emits_event_metadata_without_values() -> None:
    """Lightweight events broadcast to every subscriber by default — no value payload."""
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub = mgr.add_subscriber()
    await mgr.start()
    try:
        frames = await _drain_until(
            sub, lambda fs: sum(1 for f in fs if '"type": "event"' in f.text) >= 3
        )
    finally:
        await mgr.stop()
        await mgr.deinit()

    msgs = _parse(frames)
    event_msgs = [m for m in msgs if m.get("type") == "event"]
    assert len(event_msgs) >= 3
    for m in event_msgs:
        assert "value" not in m  # default broadcast strips the value
        assert m["node_id"] in {"counter", "sine_x", "sine_y", "gradient"}
    # nothing carried a binary payload — values aren't sent unsubscribed
    assert all(f.binary is None for f in frames)


@pytest.mark.asyncio
async def test_subscribe_signal_streams_values() -> None:
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub = mgr.add_subscriber()
    mgr.subscribe_signal(sub, "sine_x", "value")
    await mgr.start()
    try:
        frames = await _drain_until(
            sub, lambda fs: sum(1 for f in fs if '"type": "value"' in f.text) >= 2
        )
    finally:
        await mgr.stop()
        await mgr.deinit()

    value_msgs = [json.loads(f.text) for f in frames if '"type": "value"' in f.text]
    assert len(value_msgs) >= 2
    for m in value_msgs:
        assert m["node_id"] == "sine_x"
        assert m["signal"] == "value"
        # scalar sine values are JSON-inlined, no binary follow-up
        assert m["value"]["kind"] == "json"


@pytest.mark.asyncio
async def test_subscribe_ndarray_signal_includes_binary_payload() -> None:
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub = mgr.add_subscriber()
    mgr.subscribe_signal(sub, "gradient", "frame")
    await mgr.start()
    try:
        frames = await _drain_until(
            sub,
            lambda fs: sum(
                1
                for f in fs
                if '"type": "value"' in f.text and '"kind": "ndarray"' in f.text
            )
            >= 1,
        )
    finally:
        await mgr.stop()
        await mgr.deinit()

    ndarray_frames = [
        f
        for f in frames
        if '"type": "value"' in f.text and '"kind": "ndarray"' in f.text
    ]
    assert ndarray_frames, "expected at least one ndarray value frame"
    f = ndarray_frames[0]
    msg = json.loads(f.text)
    assert msg["value"]["dtype"] == "uint8"
    assert msg["value"]["shape"] == [4, 8]  # gradient_image height x width per fixture
    assert f.binary is not None
    assert len(f.binary) == 4 * 8


@pytest.mark.asyncio
async def test_unsubscribe_stops_value_stream() -> None:
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub = mgr.add_subscriber()
    mgr.subscribe_signal(sub, "sine_x", "value")
    await mgr.start()
    try:
        # give the runner a moment to emit
        await _drain_until(
            sub, lambda fs: sum(1 for f in fs if '"type": "value"' in f.text) >= 1
        )
        mgr.unsubscribe_signal(sub, "sine_x", "value")
        # drain any in-flight queue
        try:
            while True:
                sub.queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        # let the runner keep producing for a beat
        await asyncio.sleep(0.05)
        # no more value frames should arrive after we cleared the queue
        post = []
        try:
            while True:
                post.append(sub.queue.get_nowait())
        except asyncio.QueueEmpty:
            pass
    finally:
        await mgr.stop()
        await mgr.deinit()

    post_values = [f for f in post if '"type": "value"' in f.text]
    assert post_values == []


@pytest.mark.asyncio
async def test_history_returns_buffered_events() -> None:
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub = mgr.add_subscriber()
    await mgr.start()
    try:
        # let some events accumulate
        await _drain_until(
            sub, lambda fs: sum(1 for f in fs if '"type": "event"' in f.text) >= 6
        )
    finally:
        await mgr.stop()

    try:
        history = mgr.get_history("sine_x", "value", before_id=None, limit=5)
        assert len(history) > 0
        assert len(history) <= 5
        # newest-first
        ids = [h.event["id"] for h in history]
        assert ids == sorted(ids, reverse=True)

        # pagination: ask for older than the oldest we just got
        oldest_id = ids[-1]
        older = mgr.get_history("sine_x", "value", before_id=oldest_id, limit=5)
        for h in older:
            assert h.event["id"] < oldest_id
    finally:
        await mgr.deinit()


@pytest.mark.asyncio
async def test_multiple_subscribers_each_receive_events() -> None:
    mgr = RunnerManager()
    await mgr.init(TUBE_ID)
    sub_a = mgr.add_subscriber()
    sub_b = mgr.add_subscriber()
    await mgr.start()
    try:
        frames_a = await _drain_until(
            sub_a, lambda fs: sum(1 for f in fs if '"type": "event"' in f.text) >= 2
        )
        frames_b = await _drain_until(
            sub_b, lambda fs: sum(1 for f in fs if '"type": "event"' in f.text) >= 2
        )
    finally:
        await mgr.stop()
        await mgr.deinit()

    assert any('"type": "event"' in f.text for f in frames_a)
    assert any('"type": "event"' in f.text for f in frames_b)
