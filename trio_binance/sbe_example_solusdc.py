import os
from pathlib import Path
import trio
import time
from trio_binance.async_client import AsyncClient
from trio_binance.ws.streams import BinanceSocketManager
from trio_binance.sbe_decoder import SBEDecoder


def _load_env_key() -> str:
    key = os.environ.get("BINANCE_STREAMS_KEY")
    if key:
        return key.strip().strip('"')
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("BINANCE_STREAMS_KEY="):
                return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError("BINANCE_STREAMS_KEY not found in environment or .env")


async def main():
    key = _load_env_key()
    client = await AsyncClient.create(api_key=key)
    bm = BinanceSocketManager(client)
    bm.ws_kwargs["extra_headers"] = [("X-MBX-APIKEY", key)]
    bm.ws_kwargs["subprotocols"] = ["binary"]
    streams = ["solusdc@trade", "solusdc@depth", "solusdc@bestBidAsk"]
    print(f"Connecting to SBE combined: {bm.SBE_URL}stream?streams={'/'.join(streams)}")
    socket = bm.sbe_multiplex_socket(streams)

    decoder = SBEDecoder(symbol="SOLUSDC", precision_settings={"price": 2, "size": 3})

    async def receiver():
        while True:
            try:
                raw = await socket.recv()
            except trio.TooSlowError:
                continue
            decoded = decoder.decode(raw)
            if not decoded:
                continue
            t = decoded.get("type")
            if t == "Trade":
                print(f"TRADE {decoded.get('t')} price={decoded.get('p')} qty={decoded.get('q')} time={decoded.get('T')}")
            elif t == "BestBidAskStreamEvent":
                print(f"BBA bid={decoded.get('b')} ask={decoded.get('a')} u={decoded.get('u')} time={decoded.get('E')}")
            elif t in ("DepthSnapshotStreamEvent", "DepthDiffStreamEvent"):
                print(f"DEPTH {t} u={decoded.get('u')} bids={len(decoded.get('b', []))} asks={len(decoded.get('a', []))}")
            else:
                template_id = decoded.get("templateId")
                if template_id is not None:
                    print(f"UNKNOWN_SBE templateId={template_id}")

    try:
        await socket.connect()
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(receiver)
                await trio.sleep_forever()
        except KeyboardInterrupt:
            pass
    finally:
        await socket.close()
        await client.close_connection()


if __name__ == "__main__":
    trio.run(main)