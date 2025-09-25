import os
import pytest
import trio

from trio_binance.ws.trio_stream import TrioApiManager
from trio_binance.ws.streams import BinanceSocketManager

pytestmark = pytest.mark.integration


@pytest.mark.trio
@pytest.mark.integration
async def test_kline_socket_btcusdc_1m():
    if os.environ.get("TRIO_BINANCE_INTEGRATION") != "1":
        pytest.skip("Integration tests disabled. Set TRIO_BINANCE_INTEGRATION=1 to enable.")

    # Use a background thread with asyncio websockets to fetch live kline data
    import queue
    import threading

    q: "queue.Queue[str]" = queue.Queue()

    def thread_runner():
        import asyncio
        import websockets
        import ssl

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        async def run():
            url = "wss://stream.binance.com:9443/ws/btcusdc@kline_1m"
            try:
                async with websockets.connect(url, ssl=ssl_ctx) as ws:
                    async for msg in ws:
                        q.put(msg)
            except Exception as e:
                q.put(f"__ERROR__:{e}")

        asyncio.run(run())

    t = threading.Thread(target=thread_runner, daemon=True)
    t.start()

    try:
        msg = await trio.to_thread.run_sync(lambda: q.get(timeout=30))
    except Exception as e:
        pytest.fail(f"No kline message received: {e}")

    print("Kline message:", msg)
    assert not str(msg).startswith("__ERROR__"), f"Error from websocket thread: {msg}"
