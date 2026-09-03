"""Regression tests for the Feishu adapter's *owned* websocket executor.

The official #73779 fix moved the long-lived lark WS client back onto the
asyncio *default* executor (``loop.run_in_executor(None, ...)``). That client
runs ``run_forever()`` for the whole connection lifetime and therefore pins one
worker permanently. The default pool is ``min(32, cpu + 4)`` threads, so on a
small host (e.g. 2 cores -> 6 workers) the N-th multiplexed Feishu app's
``connect()`` can no longer obtain a default-executor thread for its startup
``asyncio.to_thread`` calls and the gateway deadlocks *during startup*
(2026-09-03: stuck at profile #7, ``gateway_state`` forever ``"starting"``,
every bot — including ones already "connected" — deaf to inbound).

The adapter now hosts each WS client on its own small pool
(``_get_ws_executor``) and releases it on disconnect (``_shutdown_ws_executor``),
keeping the default executor free while retaining #73779's cross-loop fix.

Covers: #78318 (starvation class) + the 2026-09-03 startup-hang regression.
"""
import asyncio
import concurrent.futures
import inspect
import threading

import pytest

from plugins.platforms.feishu.adapter import (
    _FEISHU_WS_POOL_SIZE,
    FeishuAdapter,
)


def _bare_ws_adapter() -> FeishuAdapter:
    """A FeishuAdapter with only the websocket-executor fields wired (no __init__)."""
    adapter = object.__new__(FeishuAdapter)
    adapter._ws_executor_lock = threading.Lock()
    adapter._ws_executor = None
    return adapter


def test_ws_executor_is_a_separate_pool_with_headroom():
    """The WS pool is adapter-owned, sized _FEISHU_WS_POOL_SIZE, and reused."""
    adapter = _bare_ws_adapter()
    pool = adapter._get_ws_executor()
    assert isinstance(pool, concurrent.futures.ThreadPoolExecutor)
    assert pool is adapter._get_ws_executor()  # reused, not recreated per call
    assert pool._max_workers == _FEISHU_WS_POOL_SIZE >= 1
    adapter._shutdown_ws_executor()


def test_ws_executor_recreates_after_shutdown():
    """A torn-down pool is transparently replaced, so a reconnect is never
    wedged by a dead pool (mirrors the SDK-executor recovery for #10849)."""
    adapter = _bare_ws_adapter()
    first = adapter._get_ws_executor()
    first.shutdown(wait=True)
    assert getattr(first, "_shutdown", False) is True

    second = adapter._get_ws_executor()
    assert second is not first
    assert getattr(second, "_shutdown", False) is False
    adapter._shutdown_ws_executor()


@pytest.mark.asyncio
async def test_ws_client_runs_on_owned_pool_not_default():
    """A long-lived 'forever' client task submitted the way _connect_websocket
    now does must land on the adapter-owned hermes-feishu-ws pool — never the
    loop default executor, which is what starves startup on small hosts."""
    adapter = _bare_ws_adapter()
    loop = asyncio.get_running_loop()

    def _forever_like():
        return threading.current_thread().name

    name = await loop.run_in_executor(adapter._get_ws_executor(), _forever_like)
    assert name.startswith("hermes-feishu-ws"), (
        "WS client escaped onto the default executor; startup will starve"
    )
    adapter._shutdown_ws_executor()


def _run_in_executor_is_owned(source: str) -> bool:
    """True iff the first positional arg to run_in_executor in this body
    references the owned pool rather than None."""
    import re

    call = re.search(r"run_in_executor\(\s*([^,\n]+),", source)
    if not call:
        return False
    first = call.group(1).strip()
    return "_get_ws_executor" in first and first != "None"


def test_connect_websocket_submits_to_owned_pool():
    """Guard against regressing #73779 back onto the default executor."""
    assert _run_in_executor_is_owned(inspect.getsource(FeishuAdapter._connect_websocket)), (
        "_connect_websocket must submit the WS client to _get_ws_executor(), "
        "not the loop default executor (None)"
    )


def test_disconnect_releases_ws_pool():
    """disconnect() must release the owned WS pool (bounded, wait=False)."""
    assert "_shutdown_ws_executor()" in inspect.getsource(FeishuAdapter.disconnect)
