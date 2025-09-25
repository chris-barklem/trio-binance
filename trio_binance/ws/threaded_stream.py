import trio
import inspect
import logging
import threading
from typing import Optional, Dict, Any

from trio_binance.async_client import AsyncClient
from trio_binance.helpers import get_loop
from trio_binance.trio_helpers import schedule_task, sleep as trio_sleep
from trio_binance.ws.trio_stream import TrioApiManager


class ThreadedApiManager(threading.Thread):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        requests_params: Optional[Dict[str, Any]] = None,
        tld: str = "com",
        testnet: bool = False,
        session_params: Optional[Dict[str, Any]] = None,
        https_proxy: Optional[str] = None,
        _loop: Optional[object] = None,
    ):
        """Initialise the BinanceSocketManager"""
        super().__init__()
        self._loop: Optional[object] = get_loop() if _loop is None else _loop
        self._client: Optional[AsyncClient] = None
        self._running: bool = True
        self._socket_running: Dict[str, bool] = {}
        self._log = logging.getLogger(__name__)
        self._client_params = {
            "api_key": api_key,
            "api_secret": api_secret,
            "requests_params": requests_params,
            "tld": tld,
            "testnet": testnet,
            "session_params": session_params,
            "https_proxy": https_proxy,
        }

    async def _before_socket_listener_start(self): ...

    async def socket_listener(self):
        try:
            self._client = await AsyncClient.create(loop=self._loop, **self._client_params)
            await self._before_socket_listener_start()
        except Exception as e:
            self._log.error(f"Failed to create client: {e}")
            self.stop()
        while self._running:
            await trio_sleep(0.2)
        while self._socket_running:
            await trio_sleep(0.2)
        self._log.info("Socket listener stopped")

    async def start_listener(self, socket, path: str, callback):
        async with socket as s:
            while self._socket_running[path]:
                try:
                    from trio_binance.trio_helpers import wait_for

                    msg = await wait_for(s.recv(), timeout=3)
                except trio.TooSlowError:
                    ...
                    continue
                except Exception as e:
                    self._log.error(f"Error receiving message: {e}")
                    msg = {
                        "e": "error",
                        "type": e.__class__.__name__,
                        "m": f"{e}",
                    }
                if not msg:
                    continue  # Handle both async and sync callbacks
                if inspect.iscoroutinefunction(callback):
                    schedule_task(callback(msg))
                else:
                    callback(msg)
        del self._socket_running[path]

    def run(self):
        # Run the listener using Trio in this thread
        trio.run(self.socket_listener)

    def stop_socket(self, socket_name):
        if socket_name in self._socket_running:
            self._socket_running[socket_name] = False

    async def stop_client(self):
        if not self._client:
            return
        await self._client.close_connection()

    def stop(self):
        self._log.debug("Stopping ThreadedApiManager")
        if not self._running:
            return
        self._running = False
        if self._client:
            try:
                trio.run(self.stop_client)
            except Exception as e:
                self._log.error(f"Error stopping client: {e}")
        for socket_name in self._socket_running.keys():
            self._socket_running[socket_name] = False


def get_api_manager(use_trio: bool = False, *args, **kwargs):
    """Factory returning a ThreadedApiManager or TrioApiManager.

    - If use_trio is True, returns a new `TrioApiManager` instance.
    - Otherwise returns a `ThreadedApiManager`.
    """
    if use_trio:
        return TrioApiManager(*args, **kwargs)
    return ThreadedApiManager(*args, **kwargs)
