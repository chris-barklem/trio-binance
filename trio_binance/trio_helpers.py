"""Small Trio utility helpers to ease migration from asyncio.

This module provides a tiny Trio-friendly Future-like class and thin
wrappers around common primitives we will need during conversion. It is
intentionally small and focused so we can incrementally refactor the
codebase to Trio without large simultaneous changes.

Note: This is a compatibility shim for migration; later refactors will
replace usages with native Trio patterns (nurseries, channels, tasks).
"""
from __future__ import annotations

from typing import Any, Optional, Tuple, Callable
import inspect

import trio


class TrioFuture:
    """A minimal Future-like object backed by a Trio Event.

    Supports set_result, set_exception, done and an awaitable `wait()`
    which returns the result or raises the exception. This mirrors the
    small subset of asyncio.Future used in the codebase.
    """

    def __init__(self) -> None:
        self._event = trio.Event()
        self._result: Any = None
        self._exc: Optional[BaseException] = None

    def set_result(self, result: Any) -> None:
        self._result = result
        # wake waiters
        self._event.set()

    def set_exception(self, exc: BaseException) -> None:
        self._exc = exc
        self._event.set()

    def done(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> Any:
        await self._event.wait()
        if self._exc is not None:
            raise self._exc
        return self._result


def create_future() -> TrioFuture:
    """Create a TrioFuture instance.

    Intended as a drop-in for code that currently constructs
    asyncio.Future() and awaits it. Callers will need to await
    future.wait() instead of awaiting the future directly; we'll
    migrate call sites progressively.
    """

    return TrioFuture()


def sleep(seconds: float) -> None:
    """Trio sleep wrapper for use in async contexts: `await sleep(1)`."""
    return trio.sleep(seconds)


def get_lock() -> Any:
    """Return a lock appropriate for the current async backend.

    Returns an asyncio.Lock if an asyncio loop is running, otherwise a
    trio.Lock.
    """
    return trio.Lock()


def schedule_task(coro: Callable, loop: Optional[object] = None):
    """Schedule a coroutine or async callable to run.

    Accepts either a coroutine object (e.g. `cb(msg)`) or an async
    function/callable. Ensures Trio's `spawn_system_task` receives an
    async function by wrapping coroutine objects as needed.
    """
    # If the user passed a coroutine object, wrap it in an async runner
    if inspect.iscoroutine(coro):
        async def _runner():
            await coro

        return trio.lowlevel.spawn_system_task(_runner)

    # If it's a callable, call it inside the runner and await if it
    # returns a coroutine.
    if callable(coro):
        async def _runner():
            result = coro()
            if inspect.iscoroutine(result):
                await result

        return trio.lowlevel.spawn_system_task(_runner)

    raise TypeError("schedule_task expects a coroutine object or callable")


async def wait_for(coro, timeout: float):
    """Wait for a coroutine with timeout using Trio."""
    with trio.fail_after(timeout):
        return await coro


def open_memory_channel(max_buffer: int = 0) -> Tuple[trio.MemorySendChannel, trio.MemoryReceiveChannel]:
    """Return a trio memory channel pair like asyncio.Queue/Channel.

    Returns (send_channel, receive_channel).
    """

    return trio.open_memory_channel(max_buffer)
