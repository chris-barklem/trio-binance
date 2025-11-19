import trio
import time
import warnings
import gzip
import json
import logging
from socket import gaierror
from typing import Optional
from random import random

# load orjson if available, otherwise default to json
orjson = None
try:
    import orjson as orjson
except ImportError:
    pass

# The `websockets` package exposes a legacy module which currently emits a
# DeprecationWarning on import. We avoid importing legacy internals and
# instead use attribute checks on the websocket object. Silence the specific
# warning to avoid noisy output during tests and consumption.
warnings.filterwarnings(
    "ignore",
    message=r"websockets\.legacy is deprecated",
    category=DeprecationWarning,
)


Proxy = None
proxy_connect = None
try:
    from websockets_proxy import Proxy as w_Proxy, proxy_connect as w_proxy_connect

    Proxy = w_Proxy
    proxy_connect = w_proxy_connect
except ImportError:
    pass

from trio_websocket import open_websocket_url, ConnectionClosed

from trio_binance.exceptions import (
    BinanceWebsocketClosed,
    BinanceWebsocketUnableToConnect,
    BinanceWebsocketQueueOverflow,
)
from trio_binance.helpers import get_loop
from trio_binance.trio_helpers import sleep as trio_sleep, schedule_task
from trio_binance.ws.constants import WSListenerState


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60

    def __init__(
        self,
        url: str,
        path: Optional[str] = None,
        prefix: str = "ws/",
        is_binary: bool = False,
        message_decoder=None,
        exit_coro=None,
        https_proxy: Optional[str] = None,
        max_queue_size: int = 100,
        **kwargs,
    ):
        self._loop = get_loop()
        self._log = logging.getLogger(__name__)
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._message_decoder = message_decoder
        self._conn = None
        self._socket = None
        self.ws = None
        self.ws_state = WSListenerState.INITIALISING
        self._last_message_time = 0.0
        # Use a small compatibility shim for queues so we can support either
        # asyncio.Queue (current behavior) or trio memory channels after
        # migration. The shim exposes async put(), get(), and qsize().
        class _QueueShim:
            def __init__(self, max_buf: int):
                from trio_binance.trio_helpers import open_memory_channel

                self._send, self._recv = open_memory_channel(max_buf)

            async def put(self, item):
                import trio
                try:
                    self._send.send_nowait(item)
                except trio.WouldBlock:
                    raise BinanceWebsocketQueueOverflow

            async def get(self):
                return await self._recv.receive()

            def qsize(self):
                return self._send.statistics().current_buffer_size

            def get_recv(self):
                return self._recv.clone()

        self.max_queue_size = max_queue_size
        self._queue = _QueueShim(self.max_queue_size)
        self._handle_read_loop = None
        self._read_cancel_scope = None
        self._read_task_running = False
        self._https_proxy = https_proxy
        self._ws_kwargs = kwargs
        self._connected_event = trio.Event()

    def json_dumps(self, msg) -> str:
        if orjson:
            return orjson.dumps(msg).decode("utf-8")
        return json.dumps(msg)

    def json_loads(self, msg):
        if orjson:
            return orjson.loads(msg)
        return json.loads(msg)

    async def __aenter__(self):
        await self.connect()
        return self

    async def close(self):
        await self.__aexit__(None, None, None)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._log.debug(f"Closing Websocket {self._url}{self._prefix}{self._path}")
        if self._handle_read_loop:
            await self._kill_read_loop()
        if self._exit_coro:
            await self._exit_coro(self._path)
        if self.ws:
            await self.ws.aclose()
        self.ws = None

    async def connect(self):
        self._log.debug("Establishing new WebSocket connection")
        self.ws_state = WSListenerState.RECONNECTING
        await self._before_connect()
        if not self._handle_read_loop:
            async def _runner():
                self._read_task_running = True
                with trio.CancelScope() as cs:
                    self._read_cancel_scope = cs
                    try:
                        await self._read_loop()
                    finally:
                        self._read_task_running = False
                        self._read_cancel_scope = None
                        self._handle_read_loop = None

            trio.lowlevel.spawn_system_task(_runner)
            self._handle_read_loop = True
        from trio_binance.trio_helpers import wait_for
        try:
            await wait_for(self._connected_event.wait(), timeout=self.MAX_RECONNECT_SECONDS)
        except Exception:
            raise BinanceWebsocketUnableToConnect

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        if self._read_cancel_scope:
            self._read_cancel_scope.cancel()
        while self._read_task_running:
            await trio_sleep(0.1)
        self._log.debug("Finished killing read_loop")

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        if self._message_decoder:
            return self._message_decoder(evt)
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError) as e:
                self._log.error(f"Failed to decompress message: {(e)}")
                raise
            except Exception as e:
                self._log.error(f"Unexpected decompression error: {(e)}")
                raise
        try:
            return self.json_loads(evt)
        except ValueError as e:
            self._log.error(f"JSON Value Error parsing message: Error: {(e)}")
            raise
        except TypeError as e:
            self._log.error(f"JSON Type Error parsing message. Error: {(e)}")
            raise
        except Exception as e:
            self._log.error(f"Unexpected error parsing message. Error: {(e)}")
            raise

    async def _read_loop(self):
        try:
            attempts = 0
            while True:
                if self.ws_state == WSListenerState.EXITING:
                    break
                ws_url = f"{self._url}{getattr(self, '_prefix', '')}{getattr(self, '_path', '')}"
                extra = self._ws_kwargs.get("extra_headers")
                if isinstance(extra, dict):
                    self._ws_kwargs["extra_headers"] = list(extra.items())
                try:
                    async with open_websocket_url(ws_url, **self._ws_kwargs) as ws:
                        self.ws = ws
                        self.ws_state = WSListenerState.STREAMING
                        self._last_message_time = time.time()
                        if not self._connected_event.is_set():
                            self._connected_event.set()
                        while self.ws_state == WSListenerState.STREAMING:
                            from trio_binance.trio_helpers import wait_for
                            try:
                                res = await wait_for(self.ws.get_message(), timeout=self.TIMEOUT)
                            except ConnectionClosed:
                                await self._queue.put({
                                    "e": "error",
                                    "type": "BinanceWebsocketClosed",
                                    "m": "Connection closed. Reconnecting...",
                                })
                                break
                            except trio.TooSlowError:
                                if self._last_message_time and (time.time() - self._last_message_time) > self.NO_MESSAGE_RECONNECT_TIMEOUT:
                                    await self._queue.put({
                                        "e": "error",
                                        "type": "BinanceWebsocketClosed",
                                        "m": "No messages received; reconnecting",
                                    })
                                    break
                                continue
                            res = self._handle_message(res)
                            if res:
                                try:
                                    await self._queue.put(res)
                                except BinanceWebsocketQueueOverflow:
                                    pass
                                else:
                                    self._last_message_time = time.time()
                    self.ws = None
                    if self.ws_state == WSListenerState.EXITING:
                        break
                    self.ws_state = WSListenerState.RECONNECTING
                    if attempts < self.MAX_RECONNECTS:
                        reconnect_wait = self._get_reconnect_wait(attempts)
                        attempts += 1
                        await trio_sleep(reconnect_wait)
                        continue
                    else:
                        raise BinanceWebsocketUnableToConnect
                except (
                    gaierror,
                    BinanceWebsocketUnableToConnect,
                    BinanceWebsocketQueueOverflow,
                ) as e:
                    await self._queue.put({
                        "e": "error",
                        "type": e.__class__.__name__,
                        "m": f"{e}",
                    })
                    # keep reader alive: backoff and retry
                    self.ws = None
                    self.ws_state = WSListenerState.RECONNECTING
                    if attempts < self.MAX_RECONNECTS:
                        reconnect_wait = self._get_reconnect_wait(attempts)
                        attempts += 1
                        await trio_sleep(reconnect_wait)
                        continue
                    else:
                        attempts = 0
                        await trio_sleep(self.MIN_RECONNECT_WAIT)
                        continue
                except trio.Cancelled as e:
                    await self._queue.put({
                        "e": "error",
                        "type": f"{e.__class__.__name__}",
                        "m": f"{e}",
                    })
                    break
                except Exception as e:
                    await self._queue.put({
                        "e": "error",
                        "type": e.__class__.__name__,
                        "m": f"{e}",
                    })
                    # keep reader alive: backoff and retry
                    self.ws = None
                    self.ws_state = WSListenerState.RECONNECTING
                    attempts = 0
                    await trio_sleep(self.MIN_RECONNECT_WAIT)
                    continue
        finally:
            self._handle_read_loop = None
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await trio_sleep(reconnect_wait)
            try:
                await self.connect()
            except Exception as e:
                pass
        else:
            self._log.error(f"Max reconnections {self.MAX_RECONNECTS} reached:")
            # Signal the error
            raise BinanceWebsocketUnableToConnect

    async def recv(self):
        res = None
        while not res:
            try:
                from trio_binance.trio_helpers import wait_for

                res = await wait_for(self._queue.get(), timeout=self.TIMEOUT)
            except trio.TooSlowError:
                self._log.debug(f"no message in {self.TIMEOUT} seconds")
        return res

    def get_recv(self):
        # Ensure the socket is started so messages will flow into the channel
        if not self._handle_read_loop:
            async def _starter():
                try:
                    await self.connect()
                except Exception as e:
                    self._log.error(f"Socket start failed: {e}")

            trio.lowlevel.spawn_system_task(_starter)
        return self._queue.get_recv()

    def start(self):
        if not self._handle_read_loop:
            async def _starter():
                try:
                    await self.connect()
                except Exception as e:
                    self._log.error(f"Socket start failed: {e}")

            trio.lowlevel.spawn_system_task(_starter)

    async def _wait_for_reconnect(self):
        while (
            self.ws_state != WSListenerState.STREAMING
            and self.ws_state != WSListenerState.EXITING
        ):
            await trio_sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2**attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws:
            try:
                await self.ws.aclose()
            except Exception:
                pass
            self.ws = None
        self._reconnects += 1

    def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING
