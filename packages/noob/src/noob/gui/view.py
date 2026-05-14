"""
View and run a noob tube!
"""

import asyncio
import contextlib
import json
import threading
import time
import webbrowser
from collections.abc import AsyncGenerator
from pathlib import Path

from pydantic import ValidationError

from noob.tube import TubeSpecification

try:
    from litestar import Litestar, WebSocket, get, post, websocket, websocket_stream
    from litestar.datastructures import State
    from litestar.exceptions import HTTPException, WebSocketDisconnect
except ImportError as e:
    raise ImportError(
        "GUI dependencies not installed, install noob with the [gui] dependency group"
    ) from e

import uvicorn
import watchfiles
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.logging import LoggingConfig
from litestar.response import Template
from litestar.static_files import create_static_files_router
from litestar.template.config import TemplateConfig

from noob.gui.runner_manager import HistoryEntry, RunnerManager, Subscriber, WSFrame


def _open_browser(
    url: str,
    delay: float = 1,
) -> None:
    time.sleep(delay)
    webbrowser.open(url, 2)


def _manager(state: State) -> RunnerManager:
    return state.runner_manager  # type: ignore[no-any-return]


def _serialize_history(entry: HistoryEntry) -> tuple[dict, bytes | None]:
    epoch = [[s.node_id, s.epoch] for s in entry.event["epoch"]]
    ts = entry.event["timestamp"]
    return {
        "id": entry.event["id"],
        "node_id": entry.event["node_id"],
        "signal": entry.event["signal"],
        "epoch": epoch,
        "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else ts,
        "value": entry.encoded.envelope,
    }, entry.encoded.payload


def make_view_app() -> Litestar:
    @get(path="/view/{tube_id: str}")
    async def view(tube_id: str) -> Template:
        return Template(template_name="view.html.jinja2", context={"tube_id": tube_id})

    @websocket_stream("/spec/{tube_id: str}")
    async def stream_spec(tube_id: str) -> AsyncGenerator[str, None]:
        # yield the initial spec first, then reload whenever it changes
        tube_path = TubeSpecification.path_from_id(tube_id)
        with contextlib.suppress(ValidationError):
            yield TubeSpecification.from_yaml(
                tube_path, context={"recursive": True}
            ).model_dump_json()

        watcher = watchfiles.awatch(tube_path)
        async for _ in watcher:
            # totally fine, the spec is malformed when typing in it sometimes!
            with contextlib.suppress(ValidationError):
                yield TubeSpecification.from_yaml(
                    tube_path, context={"recursive": True}
                ).model_dump_json()

    @post("/run/{tube_id: str}/init", status_code=200)
    async def run_init(tube_id: str, state: State) -> dict:
        manager = _manager(state)
        try:
            await manager.init(tube_id)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(e)) from e
        return manager.status()

    @post("/run/{tube_id: str}/deinit", status_code=200)
    async def run_deinit(tube_id: str, state: State) -> dict:
        manager = _manager(state)
        if manager.tube_id == tube_id:
            await manager.deinit()
        return manager.status()

    @post("/run/{tube_id: str}/start", status_code=200)
    async def run_start(tube_id: str, state: State) -> dict:
        manager = _manager(state)
        if manager.tube_id != tube_id:
            raise HTTPException(status_code=409, detail="tube not initialized")
        await manager.start()
        return manager.status()

    @post("/run/{tube_id: str}/stop", status_code=200)
    async def run_stop(tube_id: str, state: State) -> dict:
        manager = _manager(state)
        if manager.tube_id == tube_id:
            await manager.stop()
        return manager.status()

    @get("/run/{tube_id: str}/status")
    async def run_status(tube_id: str, state: State) -> dict:
        return _manager(state).status()

    @websocket("/run/{tube_id: str}/events")
    async def run_events_ws(socket: WebSocket, tube_id: str, state: State) -> None:
        await socket.accept()
        manager = _manager(state)
        sub = manager.add_subscriber()

        async def send_loop() -> None:
            while True:
                frame: WSFrame = await sub.queue.get()
                await socket.send_text(frame.text)
                if frame.binary is not None:
                    await socket.send_bytes(frame.binary)

        send_task: asyncio.Task | None = None
        try:
            # send initial status so the client doesn't have to race a separate fetch
            await socket.send_text(
                json.dumps({"type": "status", **manager.status()})
            )
            send_task = asyncio.create_task(send_loop())

            # recv loop: when the client disconnects, iter_json raises and we fall through
            async for msg in socket.iter_json():
                await _handle_client_message(socket, manager, sub, msg)
        except WebSocketDisconnect:
            pass
        finally:
            if send_task is not None:
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await send_task
            manager.remove_subscriber(sub)

    async def on_startup(app: Litestar) -> None:
        app.state.runner_manager = RunnerManager()

    async def on_shutdown(app: Litestar) -> None:
        manager: RunnerManager | None = getattr(app.state, "runner_manager", None)
        if manager is not None:
            await manager.deinit()

    logging_config = LoggingConfig(
        root={"level": "INFO", "handlers": ["queue_listener"]},
        formatters={"standard": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}},
        log_exceptions="always",
    )

    app = Litestar(
        route_handlers=[
            view,
            stream_spec,
            run_init,
            run_deinit,
            run_start,
            run_stop,
            run_status,
            run_events_ws,
            create_static_files_router(
                path="/static", directories=[Path(__file__).parents[1] / "_js"]
            ),
        ],
        template_config=TemplateConfig(
            directory=Path(__file__).parent / "templates",
            engine=JinjaTemplateEngine,
        ),
        logging_config=logging_config,
        on_startup=[on_startup],
        on_shutdown=[on_shutdown],
    )
    return app


async def _handle_client_message(
    socket: WebSocket, manager: RunnerManager, sub: Subscriber, msg: dict
) -> None:
    """Dispatch a control message sent by the client over the events WS."""
    op = msg.get("op")
    if op == "subscribe_values":
        manager.subscribe_signal(sub, msg["node_id"], msg["signal"])
    elif op == "unsubscribe_values":
        manager.unsubscribe_signal(sub, msg["node_id"], msg["signal"])
    elif op == "history":
        await _send_history(socket, manager, msg)
    else:
        await socket.send_text(
            json.dumps({"type": "error", "detail": f"unknown op: {op}"})
        )


async def _send_history(socket: WebSocket, manager: RunnerManager, msg: dict) -> None:
    request_id = msg.get("request_id")
    node_id = msg["node_id"]
    signal = msg["signal"]
    before_id = msg.get("before_id")
    limit = int(msg.get("limit", 50))

    entries = manager.get_history(node_id, signal, before_id=before_id, limit=limit)

    await socket.send_text(
        json.dumps(
            {
                "type": "history_begin",
                "request_id": request_id,
                "node_id": node_id,
                "signal": signal,
                "count": len(entries),
            }
        )
    )
    for entry in entries:
        envelope, payload = _serialize_history(entry)
        await socket.send_text(
            json.dumps(
                {"type": "history_value", "request_id": request_id, **envelope}
            )
        )
        if payload is not None:
            await socket.send_bytes(payload)
    await socket.send_text(
        json.dumps({"type": "history_end", "request_id": request_id})
    )


def run_view(tube_id: str) -> None:
    # pretty hacky, but don't see an "on loaded" callback on uvicorn
    url = f"http://127.0.0.1:8000/view/{tube_id}"
    open_browser = threading.Thread(target=lambda: _open_browser(url))
    open_browser.start()

    uvicorn.run(
        "noob.gui.view:make_view_app",
        factory=True,
    )
