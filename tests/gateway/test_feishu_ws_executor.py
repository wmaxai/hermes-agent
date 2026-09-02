"""Regression tests for the Feishu adapter's owned websocket executor.

The official Feishu websocket client runs a private event loop for the whole
lifetime of the connection, so it pins one worker permanently.  It used to be
submitted to the loop's *default* executor, which is sized
``min(32, cpu + 4)``: on a 2-core host six multiplexed Feishu apps exhausted
that shared pool, and every other default-executor task (``asyncio.to_thread``,
reconnects, startup restore, inbound dispatch) queued forever.  The gateway
reported every platform as connected while inbound messages were never
processed.  The client now runs on an adapter-owned pool that is recreated on
demand after a teardown.

Covers: #78318
"""
import asyncio
import concurrent.futures
import os
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.feishu.adapter import (
    FeishuAdapter,
    _FEISHU_WS_POOL_SIZE,
    _run_official_feishu_ws_client,
)
from tests.gateway.test_feishu import _mock_event_dispatcher_builder


def _bare_adapter() -> FeishuAdapter:
    """A FeishuAdapter with only the websocket-executor fields wired (no __init__)."""
    adapter = object.__new__(FeishuAdapter)
    adapter._ws_executor_lock = threading.Lock()
    adapter._ws_executor = None
    return adapter


def _completed_future():
    future = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


def test_ws_executor_recreates_after_shutdown():
    """A reconnect must never be wedged by a pool a prior teardown shut down."""
    adapter = _bare_adapter()
    first = adapter._get_ws_executor()
    first.shutdown(wait=True)
    assert getattr(first, "_shutdown", False) is True

    second = adapter._get_ws_executor()
    assert second is not first
    assert getattr(second, "_shutdown", False) is False
    adapter._shutdown_ws_executor()


def test_ws_executor_is_bounded_and_reused():
    """The pool is per-adapter, capped, and reused instead of leaking new ones."""
    adapter = _bare_adapter()
    executor = adapter._get_ws_executor()
    try:
        assert executor._max_workers == _FEISHU_WS_POOL_SIZE
        assert executor._thread_name_prefix == "hermes-feishu-ws"
        assert adapter._get_ws_executor() is executor
    finally:
        adapter._shutdown_ws_executor()


def test_ws_client_work_runs_on_owned_pool_not_default():
    """Actual work lands on a ``hermes-feishu-ws`` thread, not the default pool."""
    adapter = _bare_adapter()
    captured = {}

    def _work():
        captured["thread"] = threading.current_thread().name
        return "ok"

    pool = adapter._get_ws_executor()
    assert pool.submit(_work).result(timeout=5) == "ok"
    assert captured["thread"].startswith("hermes-feishu-ws")
    adapter._shutdown_ws_executor()


@pytest.mark.asyncio
async def test_connect_websocket_submits_to_owned_executor():
    """``_connect_websocket`` must never hand the WS client to the default pool."""
    adapter = FeishuAdapter(PlatformConfig())
    adapter._app_id = "cli_app"
    adapter._app_secret = "secret_app"
    adapter._connection_mode = "websocket"

    submitted = []

    class _Loop:
        def is_closed(self):
            return False

        def run_in_executor(self, executor, func, *args):
            submitted.append((executor, func, args))
            return _completed_future()

    with (
        patch.dict(
            os.environ,
            {"FEISHU_APP_ID": "cli_app", "FEISHU_APP_SECRET": "secret_app"},
            clear=True,
        ),
        patch("plugins.platforms.feishu.adapter.FEISHU_AVAILABLE", True),
        patch("plugins.platforms.feishu.adapter.FEISHU_WEBSOCKET_AVAILABLE", True),
        patch(
            "plugins.platforms.feishu.adapter.lark",
            SimpleNamespace(LogLevel=SimpleNamespace(INFO="INFO", WARNING="WARNING")),
        ),
        patch("plugins.platforms.feishu.adapter.EventDispatcherHandler") as mock_handler_class,
        patch("plugins.platforms.feishu.adapter.FeishuWSClient", return_value=object()),
        patch.object(adapter, "_hydrate_bot_identity", new=AsyncMock()),
        patch.object(adapter, "_build_lark_client", return_value=object()),
    ):
        _mock_event_dispatcher_builder(mock_handler_class)
        adapter._loop = _Loop()
        await adapter._connect_websocket()

    assert len(submitted) == 1, "expected exactly one websocket executor submission"
    executor, func, _args = submitted[0]
    assert func is _run_official_feishu_ws_client
    # The regression itself: ``None`` here means "the loop's shared default pool".
    assert executor is not None, "websocket client must not run on the default executor"
    assert isinstance(executor, concurrent.futures.ThreadPoolExecutor)
    assert executor._thread_name_prefix == "hermes-feishu-ws"
    assert executor is adapter._ws_executor
    adapter._shutdown_ws_executor()


def test_disconnect_shuts_down_ws_executor():
    """Teardown releases the pool so a later reconnect starts from a clean one."""
    adapter = FeishuAdapter(PlatformConfig())
    adapter._ws_future = None
    adapter._ws_client = None
    adapter._ws_thread_loop = None
    pool = adapter._get_ws_executor()

    asyncio.run(adapter.disconnect())

    assert adapter._ws_executor is None
    assert getattr(pool, "_shutdown", False) is True


def test_disconnect_with_wedged_ws_thread_stays_bounded():
    """Worst case for the added teardown: the websocket thread never returns.

    This is the #99845 / #96801 shape — a wedged client thread plus a
    disconnect.  Teardown must still complete within its documented 10s
    websocket timeout (it must not block on the dead worker), must release
    the pool, and a later reconnect must get a usable pool again.
    """
    adapter = FeishuAdapter(PlatformConfig())
    release = threading.Event()

    async def scenario():
        pool = adapter._get_ws_executor()
        # A ws client thread that will not exit until the test lets it go.
        adapter._ws_future = asyncio.get_running_loop().run_in_executor(
            pool, lambda: release.wait(30)
        )
        started = time.monotonic()
        await adapter.disconnect()
        return time.monotonic() - started, pool

    try:
        elapsed, old_pool = asyncio.run(scenario())
    finally:
        release.set()

    # It spent the advertised websocket wait and nothing more — no hang.
    assert 10.0 <= elapsed < 13.0, f"disconnect took {elapsed:.2f}s"
    assert getattr(old_pool, "_shutdown", False) is True
    assert adapter._ws_executor is None

    # A reconnect after a wedged teardown must be able to run work again.
    new_pool = adapter._get_ws_executor()
    assert new_pool is not old_pool
    assert new_pool.submit(lambda: "ok").result(timeout=5) == "ok"
    adapter._shutdown_ws_executor()
