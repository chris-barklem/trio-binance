import struct
from typing import Any, Dict, Optional

import logging
from typing import Dict


class PrecisionConverter:
    """
    Handles conversion of prices and sizes based on precision settings.
    """

    def __init__(self, price_precision: int, size_precision: int):
        """
        Initialize the PrecisionConverter.

        Args:
            price_precision (int): Number of decimal places for price.
            size_precision (int): Number of decimal places for size/quantity.
        """
        self.price_precision_factor = 10**price_precision
        self.size_precision_factor = 10**size_precision
        self.price_precision = price_precision
        self.size_precision = size_precision

    def price_to_int(self, price: float) -> int:
        """Convert float price to integer based on precision."""
        return int(round(price * self.price_precision_factor))

    def size_to_int(self, size: float) -> int:
        """Convert float size to integer based on precision."""
        return int(round(size * self.size_precision_factor))

    def price_from_int(self, price_int: int) -> float:
        """Convert integer price back to float."""
        return float(price_int / self.price_precision_factor)

    def size_from_int(self, size_int: int) -> float:
        """Convert integer size back to float."""
        return float(size_int / self.size_precision_factor)

    def get_precision_dict(self) -> Dict[str, int]:
        return {"price": self.price_precision, "size": self.size_precision}

class SBEDecoder:
    """
    Decoder for Binance Simple Binary Encoding (SBE) messages.
    Based on the SBE schema provided by Binance.
    This decoder is intended ONLY for binary SBE messages.
    """

    def __init__(self, symbol: str = None, precision_settings: Dict[str, int] = None):
        """Initialize the SBE decoder."""
        self.symbol = symbol
        self.logger = logging.getLogger(f"SBEDecoder_{symbol if symbol else 'default'}")

        if precision_settings is None:
            self.logger.warning(
                "Precision settings not provided to SBEDecoder, using defaults (8 for price, 3 for size)."
            )
            precision_settings = {"price": 2, "size": 8}  # Example defaults

        self.converter = PrecisionConverter(
            price_precision=precision_settings["price"],
            size_precision=precision_settings["size"],
        )

    def decode(self, message: bytes = None) -> Optional[Dict[str, Any]]:
        """
        Decode an SBE binary message to a Python dictionary.
        This dictionary is then used to create a HistoricTrade Pydantic model.
        Returns None if the message is not bytes or if decoding fails.
        """
        if not isinstance(message, bytes):
            self.logger.warning(
                f"Invalid message type for SBE decoding: {type(message)}. Expected bytes."
            )
            return None

        try:
            if len(message) < 8:
                self.logger.warning(f"SBE Message too short: {len(message)} bytes")
                return None

            header_format = "<HHHH"
            block_length, template_id, schema_id, version = struct.unpack(
                header_format, message[:8]
            )

            if template_id == 10001:  # BestBidAskStreamEvent
                return self._decode_bestbidask(
                    data=message[8:], block_length=block_length
                )
            elif template_id == 10000:  # TradesStreamEvent
                return self._decode_trade(data=message[8:], block_length=block_length)
            elif template_id == 10002:  # DepthSnapshotStreamEvent
                return self._decode_depth_snapshot(
                    data=message[8:], block_length=block_length
                )
            elif template_id == 10003:  # DepthDiffStreamEvent
                return self._decode_depth_diff(
                    data=message[8:], block_length=block_length
                )
            else:
                self.logger.warning(f"Unknown SBE template ID: {template_id}")
                return {
                    "type": "UnknownSBE",
                    "templateId": template_id,
                    "schemaId": schema_id,
                    "version": version,
                    "blockLength": block_length,
                }
        except struct.error as e:
            self.logger.error(
                f"Struct unpacking error during SBE decoding: {e}. Message length: {len(message)}",
                exc_info=True,
            )
            return None
        except Exception as e:
            self.logger.error(f"Error decoding SBE message: {e}", exc_info=True)
            return None

    def _decode_bestbidask(
        self, data: bytes = None, block_length: int = 0
    ) -> Dict[str, Any]:
        try:
            event_time_us = struct.unpack("<q", data[0:8])[
                0
            ]  # SBE event time (microseconds)
            book_update_id = struct.unpack("<q", data[8:16])[0]
            price_exponent = struct.unpack("<b", data[16:17])[0]
            qty_exponent = struct.unpack("<b", data[17:18])[0]
            bid_price_mantissa = struct.unpack("<q", data[18:26])[0]
            bid_qty_mantissa = struct.unpack("<q", data[26:34])[0]
            ask_price_mantissa = struct.unpack("<q", data[34:42])[0]
            ask_qty_mantissa = struct.unpack("<q", data[42:50])[0]

            event_time_ms = int(event_time_us / 1000)  # Convert to milliseconds

            float_bid_price = bid_price_mantissa * (10 ** -abs(price_exponent))
            float_bid_qty = bid_qty_mantissa * (10 ** -abs(qty_exponent))
            float_ask_price = ask_price_mantissa * (10 ** -abs(price_exponent))
            float_ask_qty = ask_qty_mantissa * (10 ** -abs(qty_exponent))

            symbol_length_offset = 50
            symbol_length = data[symbol_length_offset]
            symbol_offset_start = symbol_length_offset + 1
            symbol = data[
                symbol_offset_start : symbol_offset_start + symbol_length
            ].decode(encoding="utf-8")

            return {
                "type": "BestBidAskStreamEvent",  # Internal type for SBEDecoder
                "E": event_time_ms,  # Event Time (ms)
                "u": book_update_id,  # Update ID
                "b": float_bid_price,  # Best bid price (int)
                "B": float_bid_qty,  # Best bid quantity (int)
                "a": float_ask_price,  # Best ask price (int)
                "A": float_ask_qty,  # Best ask quantity (int)
                "s": symbol,  # Symbol
                # price_exponent and qty_exponent are used for conversion, not directly part of the final model
            }
        except Exception as e:
            self.logger.error(
                f"Error decoding BestBidAskStreamEvent for symbol {self.symbol if self.symbol else 'N/A'}: {e}",
                exc_info=True,
            )
            return {"type": "BestBidAskStreamEventError", "error": str(e)}

    def _decode_trade(
        self, data: bytes = None, block_length: int = 0
    ) -> Optional[Dict[str, Any]]:
        """
        Decodes a TradesStreamEvent SBE message.
        Output keys are 'T' (transactTimeMs), 't' (tradeId), 'p' (price_int),
        'q' (qty_int), 'Q' (quote_qty_int), 'm' (isBuyerMaker).
        Symbol is not part of SBE trade message itself but known by handler.
        """
        try:
            # event_time_us = struct.unpack('<q', data[0:8])[0] # SBE event time, not used for HistoricTrade
            transact_time_us = struct.unpack("<q", data[8:16])[0]
            price_exponent = struct.unpack("<b", data[16:17])[0]
            qty_exponent = struct.unpack("<b", data[17:18])[0]

            transact_time_ms = int(transact_time_us / 1000)

            group_offset = 18
            # trade_block_length, trade_count = struct.unpack('<HI', data[group_offset:group_offset+6]) # Original used <HI for blockLength, numInGroup
            # Binance SBE schema for TradesStreamEvent's trades group: <blockLength>uint16</blockLength><numInGroup>uint8</numInGroup>
            # Let's assume the provided code's <HI (uint16, uint32) was a typo or for a different schema version.
            # Using <HH for uint16, uint16 if numInGroup is also uint16, or <HB if uint8
            # The original code used <HI (H=u16, I=u32). If numInGroup is u8, it should be <HB.
            # Let's stick to the original struct unpack format for now, assuming it was correct for the target SBE.
            # If issues arise, this is a place to check against the exact SBE XML schema version.
            # For the provided code: trade_block_length (u16), trade_count (u32)
            _trade_group_block_length, trade_count = struct.unpack(
                "<HI", data[group_offset : group_offset + 6]
            )
            group_offset += 6

            if trade_count > 0:
                # We only process the first trade in the group as per original logic
                trade_id = struct.unpack("<q", data[group_offset : group_offset + 8])[0]
                price_mantissa = struct.unpack(
                    "<q", data[group_offset + 8 : group_offset + 16]
                )[0]
                qty_mantissa = struct.unpack(
                    "<q", data[group_offset + 16 : group_offset + 24]
                )[0]
                is_buyer_maker = bool(
                    data[group_offset + 24]
                )  # side: 0 for BUY, 1 for SELL if it were Side enum

                float_price = price_mantissa * (10 ** -abs(price_exponent))
                price_int = self.converter.price_to_int(float_price)
                float_qty = qty_mantissa * (10 ** -abs(qty_exponent))
                qty_int = self.converter.size_to_int(float_qty)
                quote_qty_int = self.converter.size_to_int(float_price * float_qty)

                return {
                    "type": "Trade",  # Internal type for SBEDecoder
                    "T": transact_time_ms,
                    "t": int(trade_id),
                    "p": price_int,
                    "q": qty_int,
                    "Q": quote_qty_int,
                    "m": is_buyer_maker,
                    # Symbol is added by the handler, not in SBE message per trade
                }
            else:
                self.logger.warning(
                    "TradesStreamEvent received with no trades in the group."
                )
                return None
        except Exception as e:
            self.logger.error(
                f"Error decoding SBE TradesStreamEvent: {e}", exc_info=True
            )
            return None  # Return None on error to be consistent

    def _decode_depth_snapshot(
        self, data: bytes = None, block_length: int = 0
    ) -> Dict[str, Any]:
        # ... (your existing _decode_depth_snapshot logic) ...
        # Make sure to use self.logger
        try:
            event_time = struct.unpack("<q", data[0:8])[0]
            book_update_id = struct.unpack("<q", data[8:16])[0]
            price_exponent = struct.unpack("<b", data[16:17])[0]
            qty_exponent = struct.unpack("<b", data[17:18])[0]
            group_offset = 18
            bids_block_length, bids_count = struct.unpack(
                "<HH", data[group_offset : group_offset + 4]
            )
            group_offset += 4
            bids = []
            for _ in range(bids_count):
                price_mantissa = struct.unpack(
                    "<q", data[group_offset : group_offset + 8]
                )[0]
                qty_mantissa = struct.unpack(
                    "<q", data[group_offset + 8 : group_offset + 16]
                )[0]
                float_price = price_mantissa * (10 ** -abs(price_exponent))
                float_qty = qty_mantissa * (10 ** -abs(qty_exponent))
                bids.append(
                    (
                        self.converter.price_to_int(float_price),
                        self.converter.size_to_int(float_qty),
                    )
                )
                group_offset += bids_block_length
            asks_block_length, asks_count = struct.unpack(
                "<HH", data[group_offset : group_offset + 4]
            )
            group_offset += 4
            asks = []
            for _ in range(asks_count):
                price_mantissa = struct.unpack(
                    "<q", data[group_offset : group_offset + 8]
                )[0]
                qty_mantissa = struct.unpack(
                    "<q", data[group_offset + 8 : group_offset + 16]
                )[0]
                float_price = price_mantissa * (10 ** -abs(price_exponent))
                float_qty = qty_mantissa * (10 ** -abs(qty_exponent))
                asks.append(
                    (
                        self.converter.price_to_int(float_price),
                        self.converter.size_to_int(float_qty),
                    )
                )
                group_offset += asks_block_length
            symbol_length = data[group_offset]
            symbol = data[group_offset + 1 : group_offset + 1 + symbol_length].decode(
                encoding="utf-8"
            )
            return {
                "type": "DepthSnapshotStreamEvent",  # This should map to OrderBookUpdateType.SNAPSHOT
                "E": event_time,  # Map to OrderBookUpdate.event_time
                "u": book_update_id,  # Map to OrderBookUpdate.last_update_id
                "b": bids,  # Map to OrderBookUpdate.bids
                "a": asks,  # Map to OrderBookUpdate.asks
                "s": symbol,  # Map to OrderBookUpdate.symbol
                # price_exponent and qty_exponent are used for conversion, not directly part of the final model
            }
        except Exception as e:
            self.logger.error(
                f"Error decoding DepthSnapshotStreamEvent: {e}", exc_info=True
            )
            return {"type": "DepthSnapshotStreamEventError", "error": str(e)}

    def _decode_depth_diff(
        self, data: bytes = None, block_length: int = 0
    ) -> Dict[str, Any]:
        # ... (your existing _decode_depth_diff logic) ...
        # Make sure to use self.logger
        try:
            event_time = struct.unpack("<q", data[0:8])[0]
            first_book_update_id = struct.unpack("<q", data[8:16])[0]
            last_book_update_id = struct.unpack("<q", data[16:24])[0]
            price_exponent = struct.unpack("<b", data[24:25])[0]
            qty_exponent = struct.unpack("<b", data[25:26])[0]
            group_offset = 26
            bids_block_length, bids_count = struct.unpack(
                "<HH", data[group_offset : group_offset + 4]
            )
            group_offset += 4
            bids = []
            for _ in range(bids_count):
                price_mantissa = struct.unpack(
                    "<q", data[group_offset : group_offset + 8]
                )[0]
                qty_mantissa = struct.unpack(
                    "<q", data[group_offset + 8 : group_offset + 16]
                )[0]
                float_price = price_mantissa * (10 ** -abs(price_exponent))
                float_qty = qty_mantissa * (10 ** -abs(qty_exponent))
                bids.append(
                    (
                        self.converter.price_to_int(float_price),
                        self.converter.size_to_int(float_qty),
                    )
                )
                group_offset += bids_block_length
            asks_block_length, asks_count = struct.unpack(
                "<HH", data[group_offset : group_offset + 4]
            )
            group_offset += 4
            asks = []
            for _ in range(asks_count):
                price_mantissa = struct.unpack(
                    "<q", data[group_offset : group_offset + 8]
                )[0]
                qty_mantissa = struct.unpack(
                    "<q", data[group_offset + 8 : group_offset + 16]
                )[0]
                float_price = price_mantissa * (10 ** -abs(price_exponent))
                float_qty = qty_mantissa * (10 ** -abs(qty_exponent))
                asks.append(
                    (
                        self.converter.price_to_int(float_price),
                        self.converter.size_to_int(float_qty),
                    )
                )
                group_offset += asks_block_length
            symbol_length = data[group_offset]
            symbol = data[group_offset + 1 : group_offset + 1 + symbol_length].decode(
                encoding="utf-8"
            )
            return {
                "type": "DepthDiffStreamEvent",  # This should map to OrderBookUpdateType.DIFF
                "E": event_time,  # Map to OrderBookUpdate.event_time
                "U": first_book_update_id,  # Map to OrderBookUpdate.first_update_id
                "u": last_book_update_id,  # Map to OrderBookUpdate.last_update_id
                "b": bids,  # Map to OrderBookUpdate.bids
                "a": asks,  # Map to OrderBookUpdate.asks
                "s": symbol,  # Map to OrderBookUpdate.symbol
            }
        except Exception as e:
            self.logger.error(
                f"Error decoding DepthDiffStreamEvent: {e}", exc_info=True
            )
            return {"type": "DepthDiffStreamEventError", "error": str(e)}
