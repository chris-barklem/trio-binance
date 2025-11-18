import os
import json
import pytest

import trio
from trio_binance.ws.trio_stream import TrioApiManager

pytestmark = pytest.mark.integration


@pytest.mark.trio
@pytest.mark.integration
async def test_depth_socket_btcusdc_bidask():
    if os.environ.get("TRIO_BINANCE_INTEGRATION") != "1":
        pytest.skip("Integration tests disabled. Set TRIO_BINANCE_INTEGRATION=1 to enable.")

    # Create a Trio memory channel and adapter socket that exposes
    # the same interface (`__aenter__`, `__aexit__`, `recv`) expected by
    # our TrioApiManager.start_listener.
    send, recv = trio.open_memory_channel(0)

    class AdapterSocket:
        def __init__(self, recv, path):
            self._recv = recv
            self._path = path

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def recv(self):
            with trio.fail_after(30):
                return await self._recv.receive()

    path = "/btcusdc@depth5"
    received = []

    async def cb(msg):
        received.append(msg)

    # Use a background thread with asyncio websockets to fetch live data
    # and forward the first message into the Trio channel.
    import threading
    import asyncio
    import websockets
    import ssl

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    def thread_runner():
        async def run():
            url = "wss://stream.binance.com:9443/ws/btcusdc@depth5"
            try:
                async with websockets.connect(url, ssl=ssl_ctx) as ws:
                    msg = await ws.recv()
                    trio.from_thread.run(send.send, msg, trio_token=token)
            except Exception as e:
                trio.from_thread.run(send.send, f"__ERROR__:{e}", trio_token=token)

        asyncio.run(run())

    t = threading.Thread(target=thread_runner, daemon=True)
    t.start()

    mgr = TrioApiManager()
    async with trio.open_nursery() as nursery:
        token = trio.lowlevel.current_trio_token()
        mgr.spawn_listener(nursery, AdapterSocket(recv, path), path, cb)
        with trio.fail_after(35):
            while not received:
                await trio.sleep(0.1)
        await mgr.stop()
        nursery.cancel_scope.cancel()

    msg = received[0]
    assert not str(msg).startswith("__ERROR__"), f"Error from websocket thread: {msg}"
    if isinstance(msg, (bytes, bytearray)):
        msg = msg.decode("utf-8")
    data = json.loads(msg) if isinstance(msg, str) else msg
    assert "bids" in data and "asks" in data
    print("Depth message parsed levels:", len(data["bids"]), len(data["asks"]))
