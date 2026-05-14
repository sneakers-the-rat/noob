"""
Manage a tube runner from the GUI server.

Owns one :class:`.AsyncRunner` per server process plus a small custom run
loop, since ``AsyncRunner.run`` is inherited from the synchronous base.

Subscribers are individual WebSocket connections. Each holds an
``asyncio.Queue`` of :class:`.WSFrame` items (text + optional binary). The
manager fans out:

- Lightweight event metadata (no value) to every subscriber.
- Full value payloads only to subscribers that have subscribed to that
  ``(node_id, signal)`` pair.

Per-signal ring buffers retain a bounded history for paged requests.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import uuid4

from noob.event import Event, MetaEvent, MetaSignal
from noob.gui.serialize import EncodedValue, encode_value
from noob.runner import AsyncRunner
from noob.tube import Tube, TubeSpecification

logger = logging.getLogger("noob.gui.runner")


HISTORY_LIMIT = 1000
"""Per-signal cap on retained events for paged history."""

RunState = Literal["uninitialized", "initialized", "running", "stopped", "error"]


@dataclass
class WSFrame:
    """A single websocket transmission: JSON text plus an optional binary follow-up."""

    text: str
    binary: bytes | None = None


@dataclass(eq=False)
class Subscriber:
    """Hashed by object identity so we can put it in sets / dict keys."""

    id_: str
    queue: asyncio.Queue
    signals: set[tuple[str, str]] = field(default_factory=set)


@dataclass
class HistoryEntry:
    event: Event
    encoded: EncodedValue


class RunnerManager:
    """One-tube-per-process runner manager."""

    def __init__(self) -> None:
        self.tube_id: str | None = None
        self.tube: Tube | None = None
        self.runner: AsyncRunner | None = None
        self.state: RunState = "uninitialized"
        self.error: str | None = None

        self._run_task: asyncio.Task | None = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._lock = asyncio.Lock()

        self._subscribers: dict[str, Subscriber] = {}
        self._signal_subscribers: dict[tuple[str, str], set[Subscriber]] = defaultdict(set)
        self._history: dict[tuple[str, str], deque[HistoryEntry]] = defaultdict(
            lambda: deque(maxlen=HISTORY_LIMIT)
        )

    # ---- lifecycle -------------------------------------------------------

    async def init(self, tube_id: str) -> None:
        async with self._lock:
            if self.state in ("initialized", "running") and self.tube_id == tube_id:
                return
            if self.state in ("initialized", "running"):
                raise RuntimeError(
                    f"Manager already holds tube {self.tube_id}, deinit first"
                )

            spec = TubeSpecification.from_yaml(
                TubeSpecification.path_from_id(tube_id), context={"recursive": True}
            )
            self.tube = Tube.from_specification(spec)
            self.tube_id = tube_id
            self.runner = AsyncRunner(tube=self.tube)
            self.runner.add_callback(self._on_event)
            await self.runner.init()
            self.error = None
            self.state = "initialized"

    async def deinit(self) -> None:
        async with self._lock:
            if self.state == "running":
                await self._stop_unlocked()
            if self.runner is not None:
                try:
                    await self.runner.deinit()
                except Exception:  # noqa: BLE001
                    logger.exception("Error during runner deinit")
            self.runner = None
            self.tube = None
            self.tube_id = None
            self._history.clear()
            self.state = "uninitialized"
            self.error = None

    async def start(self) -> None:
        async with self._lock:
            if self.state == "running":
                return
            if self.runner is None:
                raise RuntimeError("Runner not initialized")
            self._stop_event.clear()
            self._run_task = asyncio.create_task(self._run_loop())
            self.state = "running"

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_unlocked()

    async def _stop_unlocked(self) -> None:
        if self.state != "running":
            return
        self._stop_event.set()
        task = self._run_task
        self._run_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self.state = "initialized" if self.runner is not None else "uninitialized"

    async def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                assert self.runner is not None
                await self.runner.process()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception("Run loop error")
            self.error = str(e)
            self.state = "error"

    # ---- subscribers -----------------------------------------------------

    def add_subscriber(self) -> Subscriber:
        sub = Subscriber(id_=uuid4().hex, queue=asyncio.Queue(maxsize=512))
        self._subscribers[sub.id_] = sub
        return sub

    def remove_subscriber(self, sub: Subscriber) -> None:
        self._subscribers.pop(sub.id_, None)
        for key in list(sub.signals):
            self._signal_subscribers[key].discard(sub)
        sub.signals.clear()

    def subscribe_signal(self, sub: Subscriber, node_id: str, signal: str) -> None:
        key = (node_id, signal)
        sub.signals.add(key)
        self._signal_subscribers[key].add(sub)

    def unsubscribe_signal(self, sub: Subscriber, node_id: str, signal: str) -> None:
        key = (node_id, signal)
        sub.signals.discard(key)
        self._signal_subscribers[key].discard(sub)

    # ---- event handling --------------------------------------------------

    def _on_event(self, event: Event | MetaEvent) -> None:
        """Called by the runner inside the eventloop for every emitted event."""
        if event.get("node_id") == "meta":
            return
        value = event.get("value")
        if isinstance(value, MetaSignal):
            return

        key = (event["node_id"], event["signal"])
        encoded = encode_value(value)
        self._history[key].append(HistoryEntry(event=event, encoded=encoded))

        meta = _event_metadata(event)
        meta_frame = WSFrame(text=_dumps({"type": "event", **meta}))
        for sub in list(self._subscribers.values()):
            _enqueue(sub.queue, meta_frame)

        subs = self._signal_subscribers.get(key)
        if subs:
            value_text = _dumps({"type": "value", **meta, "value": encoded.envelope})
            for sub in list(subs):
                _enqueue(sub.queue, WSFrame(text=value_text, binary=encoded.payload))

    # ---- history ---------------------------------------------------------

    def get_history(
        self,
        node_id: str,
        signal: str,
        before_id: int | None = None,
        limit: int = 50,
    ) -> list[HistoryEntry]:
        """
        Retained events for a signal, newest-first, capped at ``limit``.

        If ``before_id`` is given, only events with strictly smaller ids
        are returned -- supports backward pagination by passing the oldest
        seen event id back.
        """
        buf = self._history.get((node_id, signal))
        if not buf:
            return []

        out: list[HistoryEntry] = []
        for entry in reversed(buf):
            if before_id is not None and entry.event["id"] >= before_id:
                continue
            out.append(entry)
            if len(out) >= limit:
                break
        return out

    # ---- status ----------------------------------------------------------

    def status(self) -> dict:
        return {
            "tube_id": self.tube_id,
            "state": self.state,
            "error": self.error,
        }


def _event_metadata(event: Event) -> dict:
    """Lightweight metadata sent for every event (no value)."""
    epoch_serialized = [[s.node_id, s.epoch] for s in event["epoch"]]
    ts = event["timestamp"]
    if isinstance(ts, datetime):
        ts = ts.isoformat()
    return {
        "id": event["id"],
        "node_id": event["node_id"],
        "signal": event["signal"],
        "epoch": epoch_serialized,
        "timestamp": ts,
    }


def _dumps(obj: dict) -> str:
    return json.dumps(obj, default=str)


def _enqueue(queue: asyncio.Queue, frame: WSFrame) -> None:
    """Put a frame on the queue, dropping the oldest if full so slow clients don't stall the runner."""
    try:
        queue.put_nowait(frame)
        return
    except asyncio.QueueFull:
        pass
    try:
        queue.get_nowait()
    except asyncio.QueueEmpty:
        pass
    try:
        queue.put_nowait(frame)
    except asyncio.QueueFull:
        pass
