from typing import Dict
import trio

from trio_binance.trio_helpers import TrioFuture, get_lock, sleep

from .constants import WSListenerState
from .reconnecting_websocket import ReconnectingWebsocket
from trio_binance.exceptions import BinanceAPIException, BinanceWebsocketUnableToConnect


class WebsocketAPI(ReconnectingWebsocket):
    def __init__(self, url: str, tld: str = "com", testnet: bool = False):
        self._tld = tld
        self._testnet = testnet
        # Values are Future-like objects; during migration we use TrioFuture
        # as a minimal drop-in replacement for asyncio.Future.
        self._responses: Dict[str, TrioFuture] = {}
        self._connection_lock = get_lock()  # used to ensure only one connection is established at a time
        # Flag to indicate a connect is in progress (used for double-checked locking)
        self._connecting = False
        super().__init__(url=url, prefix="", path="", is_binary=False)

    def _handle_message(self, msg):
        """Override message handling to support request-response"""
        parsed_msg = super()._handle_message(msg)
        self._log.debug(f"Received message: {parsed_msg}")
        if parsed_msg is None:
            return None
        req_id, exception = None, None
        if "id" in parsed_msg:
            req_id = parsed_msg["id"]
        if "status" in parsed_msg:
            if parsed_msg["status"] != 200:
                exception = BinanceAPIException(
                    parsed_msg, parsed_msg["status"], self.json_dumps(parsed_msg["error"])
                )
        if req_id is not None and req_id in self._responses:
            if exception is not None:
                self._responses[req_id].set_exception(exception)
            else:
                self._responses[req_id].set_result(parsed_msg)
        elif exception is not None:
            raise exception
        else:
            self._log.warning(f"WS api receieved unknown message: {parsed_msg}")

    async def _ensure_ws_connection(self) -> None:
        """Ensure WebSocket connection is established and ready

        This function will:
        1. Check if connection exists and is streaming
        2. Attempt to connect if not
        3. Wait for connection to be ready
        4. Handle reconnection if needed
        """
        # Use double-checked locking: take the lock briefly to decide who will
        # perform the actual connect, then do the connect outside the lock so
        # we don't await while holding it (which can deadlock other coroutines
        # waiting on the same lock).
        try:
            need_connect = False
            async with self._connection_lock:
                if (
                    self.ws is None
                    or self.ws_state != WSListenerState.STREAMING
                ):
                    if not self._connecting:
                        self._connecting = True
                        need_connect = True

            if need_connect:
                # perform connect outside the lock
                await self.connect()

            # Wait for connection to be ready (this may include waiting for
            # another coroutine that is performing the connect)
            retries = 0
            while (
                self.ws_state != WSListenerState.STREAMING
                and retries < self.MAX_RECONNECTS
            ):
                if self.ws_state == WSListenerState.RECONNECTING:
                    self._log.info("Connection is reconnecting, waiting...")
                    await self._wait_for_reconnect()

                elif self.ws is None:
                    self._log.info("Connection lost, reconnecting...")
                    # attempt to connect only if no-one else is doing it
                    do_connect = False
                    async with self._connection_lock:
                        if not self._connecting:
                            self._connecting = True
                            do_connect = True
                    if do_connect:
                        try:
                            await self.connect()
                        finally:
                            async with self._connection_lock:
                                self._connecting = False

                retries += 1
                await trio.sleep(self.MIN_RECONNECT_WAIT)

            # ensure connecting flag is cleared
            async with self._connection_lock:
                self._connecting = False

            if self.ws_state != WSListenerState.STREAMING:
                raise BinanceWebsocketUnableToConnect(
                    f"Failed to establish connection after {retries} attempts"
                )

            self._log.debug("WebSocket connection established")

        except Exception as e:
            self._log.error(f"Error ensuring WebSocket connection: {e}")
            # clear flag on error
            async with self._connection_lock:
                self._connecting = False
            raise BinanceWebsocketUnableToConnect(f"Connection failed: {str(e)}")

    async def request(self, id: str, payload: dict) -> dict:
        """Send request and wait for response"""
        await self._ensure_ws_connection()
        # Create future-like object for response (TrioFuture shim)
        future = TrioFuture()
        self._responses[id] = future

        try:
            # Send request
            if self.ws is None:
                raise BinanceWebsocketUnableToConnect(
                    "Trying to send request while WebSocket is not connected"
                )
            await self.ws.send_message(self.json_dumps(payload))

            # Wait for response (TrioFuture.wait will be awaited by Trio migration)
            # We still call asyncio.wait_for here to preserve timeout semantics
            # for the immediate migration stage. The future object implements
            # an awaitable `wait()` method used by Trio; for compatibility,
            # `asyncio.wait_for` will attempt to await the object directly in
            # environments where asyncio is used. If running under Trio the
            # higher-level migration will replace this with trio.fail_after
            # and await future.wait().
            from trio_binance.trio_helpers import wait_for

            try:
                response = await wait_for(future.wait(), timeout=self.TIMEOUT)
            except AttributeError:
                # If future doesn't expose wait(), fall back to awaiting it
                response = await wait_for(future, timeout=self.TIMEOUT)

            # Check for errors
            if "error" in response:
                raise BinanceWebsocketUnableToConnect(response["error"])

            return response.get("result", response)

        except trio.TooSlowError:
            raise BinanceWebsocketUnableToConnect("Request timed out")
        except Exception as e:
            raise e
        finally:
            self._responses.pop(id, None)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up responses before closing"""
        response_ids = list(self._responses.keys())  # Create a copy of keys
        for req_id in response_ids:
            future = self._responses.pop(req_id)  # Remove and get the future
            if not future.done():
                future.set_exception(
                    BinanceWebsocketUnableToConnect("WebSocket closing")
                )
        await super().__aexit__(exc_type, exc_val, exc_tb)
