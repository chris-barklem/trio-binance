import trio
import logging
import inspect
from typing import Optional, Dict, Any, Callable

from trio_binance.async_client import AsyncClient


class TrioApiManager:
    """A Trio-native replacement for ThreadedApiManager.

    This manager is intentionally small: it provides lifecycle methods to
    create an `AsyncClient` and start/stop websocket listeners using Trio
    nurseries.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        requests_params: Optional[Dict[str, Any]] = None,
        tld: str = "com",
        testnet: bool = False,
        session_params: Optional[Dict[str, Any]] = None,
        https_proxy: Optional[str] = None,
    ):
        self._client_params = {
            "api_key": api_key,
            "api_secret": api_secret,
            "requests_params": requests_params,
            "tld": tld,
            "testnet": testnet,
            "session_params": session_params,
            "https_proxy": https_proxy,
        }
        self._client: Optional[AsyncClient] = None
        self._running = False
        self._socket_running: Dict[str, bool] = {}
        self._log = logging.getLogger(__name__)
        self._nursery: Optional[trio.Nursery] = None

    async def start(self):
        """Start the manager and create the AsyncClient in a Trio-native way.

        We prefer to `await AsyncClient.create(...)` directly in Trio. If
        that fails due to an incompatible asyncio-only implementation, we
        fall back to creating the client inside a worker thread using
        `asyncio.run(...)` executed via `trio.to_thread.run_sync`.
        """
        if self._running:
            return
        self._running = True

        # Prefer direct creation in Trio context; let failures propagate so
        # callers can handle them. No asyncio fallback is provided in
        # a strict Trio-only runtime.
        self._client = await AsyncClient.create(**self._client_params)

    async def stop(self):
        self._running = False
        # Stop all sockets
        for k in list(self._socket_running.keys()):
            self._socket_running[k] = False
        if self._client:
            try:
                await self._client.close_connection()
            except Exception:
                pass

    async def start_listener(self, socket, path: str, callback: Callable):
        async with socket as s:
            self._socket_running[path] = True
            while self._socket_running.get(path):
                try:
                    msg = await s.recv()
                except Exception as e:
                    self._log.error(f"Error receiving message: {e}")
                    msg = {"e": "error", "type": e.__class__.__name__, "m": f"{e}"}
                if not msg:
                    # no more messages from socket -> exit listener
                    break
                if callable(callback):
                    result = callback(msg)
                    if inspect.iscoroutine(result):
                        await result
                else:
                    # callback is not callable
                    pass
        if path in self._socket_running:
            del self._socket_running[path]

    def spawn_listener(self, nursery: trio.Nursery, socket, path: str, callback: Callable):
        """Convenience to spawn a listener into an existing nursery."""
        nursery.start_soon(self.start_listener, socket, path, callback)
