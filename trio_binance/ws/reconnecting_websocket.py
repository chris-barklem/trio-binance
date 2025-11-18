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
            def __init__(self):
                from trio_binance.trio_helpers import open_memory_channel

                self._send, self._recv = open_memory_channel(0)

            async def put(self, item):
                await self._send.send(item)

            async def get(self):
                return await self._recv.receive()

            def qsize(self):
                # Trio memory channels don't expose qsize; return 0 to be conservative
                return 0

        self._queue = _QueueShim()
        self._handle_read_loop = None
        self._read_cancel_scope = None
        self._read_task_running = False
        self._https_proxy = https_proxy
        self._ws_kwargs = kwargs
        self.max_queue_size = max_queue_size

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
        if self._conn:
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None

    async def connect(self):
        self._log.debug("Establishing new WebSocket connection")
        self.ws_state = WSListenerState.RECONNECTING
        await self._before_connect()

        ws_url = (
            f"{self._url}{getattr(self, '_prefix', '')}{getattr(self, '_path', '')}"
        )

        extra = self._ws_kwargs.get("extra_headers")
        if isinstance(extra, dict):
            self._ws_kwargs["extra_headers"] = list(extra.items())
        # trio_websocket connection
        self._conn = open_websocket_url(ws_url, **self._ws_kwargs)

        try:
            self.ws = await self._conn.__aenter__()
        except Exception as e:  # noqa
            self._log.error(f"Failed to connect to websocket: {e}")
            self.ws_state = WSListenerState.RECONNECTING
            raise e
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        self._last_message_time = time.time()
        await self._after_connect()
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
            while True:
                try:
                    while self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()

                    if self.ws_state == WSListenerState.EXITING:
                        self._log.debug(
                            f"_read_loop {self._path} break for {self.ws_state}"
                        )
                        break
                    elif self.ws_state == WSListenerState.STREAMING:
                        assert self.ws
                        from trio_binance.trio_helpers import wait_for

                        try:
                            res = await wait_for(self.ws.get_message(), timeout=self.TIMEOUT)
                        except ConnectionClosed:
                            self._reconnect()
                            raise BinanceWebsocketClosed("Connection closed. Reconnecting...")
                        res = self._handle_message(res)
                        self._log.debug(f"Received message: {res}")
                        if res:
                            if self._queue.qsize() < self.max_queue_size:
                                await self._queue.put(res)
                            else:
                                raise BinanceWebsocketQueueOverflow(
                                    f"Message queue size {self._queue.qsize()} exceeded maximum {self.max_queue_size}"
                                )
                            self._last_message_time = time.time()
                except trio.TooSlowError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    if self._last_message_time and (time.time() - self._last_message_time) > self.NO_MESSAGE_RECONNECT_TIMEOUT:
                        self._reconnect()
                        await self._queue.put({
                            "e": "error",
                            "type": "BinanceWebsocketClosed",
                            "m": "No messages received; reconnecting",
                        })
                except trio.Cancelled as e:
                    self._log.debug(f"_read_loop cancelled error {e}")
                    await self._queue.put({
                        "e": "error",
                        "type": f"{e.__class__.__name__}",
                        "m": f"{e}",
                    })
                    break
                except (
                    gaierror,
                    BinanceWebsocketClosed,
                ) as e:
                    # reports errors and continue loop
                    self._log.error(f"{e.__class__.__name__} ({e})")
                    await self._queue.put({
                        "e": "error",
                        "type": f"{e.__class__.__name__}",
                        "m": f"{e}",
                    })
                except (
                    BinanceWebsocketUnableToConnect,
                    BinanceWebsocketQueueOverflow,
                    Exception,
                ) as e:
                    # reports errors and break the loop
                    self._log.error(f"Unknown exception: {e.__class__.__name__} ({e})")
                    await self._queue.put({
                        "e": "error",
                        "type": e.__class__.__name__,
                        "m": f"{e}",
                    })
                    break
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
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
            self.ws = None

        if self._conn:
            await self._conn.__aexit__(None, None, None)

        self._reconnects += 1

    def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING
