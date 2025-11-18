import json
import math
import time
import random
from huson.logger import logging
import trio
from trio_binance import AsyncClient
from trio_binance.exceptions import BinanceAPIException
from trio_websocket import open_websocket_url, ConnectionClosed
from huson.dataclasses import BidAsk, AggTrade
from huson.utils import normalize_timestamp, quantize_price, quantize_size

logger = logging.getLogger("websocket_collector")
    
class BinanceStreams:
    def __init__(self, config):
        self.symbol = config.get("symbol", "btcusdt").lower()
        self.api_key = config.get('api_key', '')
        self.api_secret = config.get('api_secret', '')
        self.base_url = "wss://stream.binance.com:9443/stream"
        self.is_testnet = config.get('testnet', False)
        self.running = True
        self.websocket = None

        # Reconnection backoff
        self.initial_delay = 1.0
        self.max_delay = 300.0
        self.backoff_multiplier = 1.5
        self.current_delay = self.initial_delay

        # Health monitoring
        self.last_message_time = time.time()
        self.connection_start_time = None
        self.message_timeout = 60.0

        # Statistics
        self.total_reconnects = 0
        self.total_messages = 0
        self.total_pings_received = 0
        self.total_pongs_sent = 0
        self.total_24h_disconnects = 0

        self.streams = [
            f"{self.symbol}@depth5",
            f"{self.symbol}@aggTrade"
        ]

        self._stop_event = trio.Event()
        self._bidask_seq = 0
        # Precision settings (optional)
        self.client: AsyncClient = None
        self.tick_size = config.get("tick_size")
        self.price_precision = config.get("price_precision")
        self.size_precision = config.get("size_precision")

    async def stop(self):
        self.running = False
        self._stop_event.set()
        if self.websocket:
            await self.websocket.aclose()
            
        if self.client:
            await self.client.close_connection()

    async def start(self, bidask_send, aggtrade_send):
        streams_param = "/".join(self.streams)
        uri = f"{self.base_url}?streams={streams_param}"
        self.client = await AsyncClient.create(self.api_key, self.api_secret, testnet=self.is_testnet)

        await self.fetch_symbol_filters()
        while not self._stop_event.is_set():
            try:
                logger.info(f"Connecting to Binance combined stream: {uri}")
                async with open_websocket_url(uri) as ws:
                    self.websocket = ws
                    self.connection_start_time = time.time()
                    self.last_message_time = time.time()
                    logger.info("Combined stream connected", extra={"total_reconnects": self.total_reconnects})
                    self.current_delay = self.initial_delay

                    async with trio.open_nursery() as nursery:
                        nursery.start_soon(self._monitor_connection)
                        nursery.start_soon(self._heartbeat, ws)
                        while not self._stop_event.is_set():
                            try:
                                message = await ws.get_message()
                            except ConnectionClosed:
                                break

                            self.last_message_time = time.time()
                            self.total_messages += 1

                            # trio-websocket yields str or bytes; Binance sends text JSON
                            if isinstance(message, (bytes, bytearray)):
                                try:
                                    message_text = message.decode("utf-8")
                                except Exception:
                                    # Skip undecodable payloads
                                    continue
                            else:
                                message_text = message

                            try:
                                await self._process_message(message_text, bidask_send, aggtrade_send)
                            except Exception as e:
                                logger.error("Error processing message", exc_info=e)
                                continue
                        nursery.cancel_scope.cancel()

            except ConnectionClosed as e:
                if not self._stop_event.is_set():
                    connection_duration = time.time() - self.connection_start_time if self.connection_start_time else 0
                    is_24h_disconnect = connection_duration > 20 * 60 * 60
                    if is_24h_disconnect:
                        self.total_24h_disconnects += 1
                        logger.warning("WebSocket 24h disconnect detected", extra={
                            "code": e.code,
                            "reason": e.reason,
                            "connection_duration_hours": connection_duration / 3600,
                            "total_24h_disconnects": self.total_24h_disconnects,
                            "reconnecting": True
                        })
                    else:
                        logger.warning("WebSocket connection closed", extra={
                            "code": e.code,
                            "reason": e.reason,
                            "connection_duration_hours": connection_duration / 3600,
                            "reconnecting": True
                        })
                    self.total_reconnects += 1
                else:
                    logger.info("WebSocket closed due to shutdown")
                    break

            except Exception as e:
                if not self._stop_event.is_set():
                    logger.error("WebSocket unexpected error", exc_info=e)
                    self.total_reconnects += 1
                else:
                    break

            # Reconnect with backoff and jitter
            if not self._stop_event.is_set():
                jitter = random.uniform(0.8, 1.2)
                sleep_time = self.current_delay * jitter
                logger.info(f"Reconnecting in {sleep_time:.2f} seconds", extra={
                    "total_reconnects": self.total_reconnects
                })
                await trio.sleep(sleep_time)
                self.current_delay = min(self.current_delay * self.backoff_multiplier, self.max_delay)
            else:
                break

        logger.info("BinanceStreams stopped", extra={
            "total_messages": self.total_messages,
            "total_reconnects": self.total_reconnects,
            "total_pings_received": self.total_pings_received,
            "total_pongs_sent": self.total_pongs_sent
        })
        
    async def _heartbeat(self, ws):
        """Optional heartbeat: send ping periodically and await pong.
        trio-websocket handles server-initiated ping/pong internally, so manual
        replies are not required. This is only to keep the connection warm.
        """
        try:
            while not self._stop_event.is_set():
                payload = str(int(time.time() * 1000)).encode()
                try:
                    await ws.ping(payload)
                except Exception as e:
                    logger.warning("Heartbeat ping failed", exc_info=e)
                    return
                await trio.sleep(30)
        except Exception:
            return

    async def fetch_symbol_filters(self):
        try:
            exchange_info = await self.client.get_exchange_info()
            symbol = self.symbol.upper()
            logger.debug(f"Fetching symbol filters for {symbol} from exchange info.")
            symbols = exchange_info["symbols"]
            if not exchange_info or "symbols" not in exchange_info:
                raise ValueError("Exchange info is not available or does not contain symbols.")
            
            symbol_info = next((s for s in symbols if s['symbol'] == symbol), None)
            if not symbol_info:
                raise ValueError(f"Could not find symbol info for {self.symbol}")
            self.price_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
            self.lot_size_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'), None)
            self.tick_size = float(self.price_filter['tickSize']) if self.price_filter else 0.0
            self.quantity_step = float(self.lot_size_filter['stepSize']) if self.lot_size_filter else 0.00001
            def calc_precision(value: float) -> int:
                if value <= 0:
                    return 0
                return max(0, int(round(-math.log10(value), 0)))

            self.price_precision = calc_precision(self.tick_size)
            self.size_precision = calc_precision(self.quantity_step)

        except Exception as e:
            logger.error(f"Failed to fetch symbol filters: {e}")

    async def _monitor_connection(self):
        while not self._stop_event.is_set() and self.websocket:
            try:
                current_time = time.time()
                time_since_last_message = current_time - self.last_message_time
                if time_since_last_message > self.message_timeout:
                    logger.warning("Connection appears dead", extra={
                        "seconds_since_last_message": time_since_last_message
                    })
                    await self.websocket.aclose()
                    break
                if self.total_messages > 0 and self.total_messages % 1000 == 0:
                    connection_duration = current_time - self.connection_start_time
                    logger.info("Connection health check", extra={
                        "messages_processed": self.total_messages,
                        "connection_duration_minutes": connection_duration / 60,
                        "reconnects": self.total_reconnects
                    })
                await trio.sleep(10)
            except Exception as e:
                logger.error("Connection monitor error", exc_info=e)
                break

    async def _process_message(self, message, bidask_send, aggtrade_send):
        data = json.loads(message)
        stream_name = data.get('stream')
        stream_data = data.get('data', {})

        if not stream_name or not stream_data:
            return

        if '@depth5' in stream_name:
            await self._process_bidask(stream_data, message, bidask_send)
        elif '@aggTrade' in stream_name:
            await self._process_aggtrade(stream_data, message, aggtrade_send)

    async def _process_bidask(self, data, raw_message, sender):
        try:
            if 'lastUpdateId' in data:
                bids = data.get('bids', [])
                asks = data.get('asks', [])
            else:
                bids = data.get('b', [])
                asks = data.get('a', [])

            if bids and asks:
                bid_price, bid_qty = float(bids[0][0]), float(bids[0][1])
                ask_price, ask_qty = float(asks[0][0]), float(asks[0][1])

                self._bidask_seq += 1
                bidask = BidAsk(
                    ts=normalize_timestamp(int(data.get('E', data.get('lastUpdateId', 0)))),
                    id=self._bidask_seq,
                    src_ts=normalize_timestamp(int(data.get('E', data.get('lastUpdateId', 0)))),
                    producer="binance",
                    bid_price=quantize_price(bid_price),
                    bid_qty=quantize_size(bid_qty),
                    ask_price=quantize_price(ask_price),
                    ask_qty=quantize_size(ask_qty),
                    raw=raw_message
                )
                await sender.send(bidask)
        except Exception as e:
            logger.error("Error parsing bidask combined stream", exc_info=e)


    async def _process_aggtrade(self, data, raw_message, sender):
        try:
            aggtrade = AggTrade(
                ts=normalize_timestamp(int(data.get('T', 0))),
                id=int(data.get('a', 0)),
                producer="binance",
                symbol=data.get('s', self.symbol.upper()),
                price=quantize_price(float(data.get('p', 0.0))),
                qty=quantize_size(float(data.get('q', 0.0))),
                is_buyer_maker=bool(data.get('m', False)),
                first_trade_id=int(data['f']) if 'f' in data else None,
                last_trade_id=int(data['l']) if 'l' in data else None,
                raw=raw_message
            )
            logger.debug("Processing aggTrade", extra={
                "id": aggtrade.id,
                "price": aggtrade.price,
                "qty": aggtrade.qty,
                "is_buyer_maker": aggtrade.is_buyer_maker
            })
            await sender.send(aggtrade)
        except Exception as e:
            logger.error("Error parsing aggTrade combined stream", exc_info=e)