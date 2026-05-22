from __future__ import annotations

import asyncio
import contextlib
import json
import time
import traceback
from typing import Any, Optional

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK

from collectors.base import Collector
from storage.parquet_writer import ParquetWriter
from utils.logger import get_logger


class BinanceTradeCollector(Collector):
    """Binance Futures aggTrade websocket collector for any symbol."""

    EXCHANGE = "binance"
    EVENT_TYPE = "aggTrade"
    HEARTBEAT_WARNING_SECONDS = 60.0

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        shared_queue: Optional[asyncio.Queue[dict[str, Any]]] = None,
        connect_timeout: float = 10.0,
        backoff_sequence: Optional[list[float]] = None,
        heartbeat_warning_seconds: float = HEARTBEAT_WARNING_SECONDS,
        queue_maxsize: int = 5000,
        enable_persistence: bool = True,
    ) -> None:
        """
        Initialize Binance trade collector.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT", "ETHUSDT")
            shared_queue: Optional shared asyncio.Queue for multi-symbol setup.
                         If None, creates its own queue.
            connect_timeout: Websocket connection timeout
            backoff_sequence: Reconnect backoff delays
            heartbeat_warning_seconds: Heartbeat timeout threshold
            queue_maxsize: Queue max size (if not using shared_queue)
            enable_persistence: Whether to enable parquet persistence
        """
        symbol_lower = symbol.lower()
        self.symbol = symbol.upper()
        self.url = f"wss://fstream.binance.com/market/ws/{symbol_lower}@aggTrade"
        
        super().__init__(name=f"binance-trade-stream-{symbol}")
        self.logger = get_logger(f"collectors.binance.trade_collector.{symbol}")
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connect_timeout = connect_timeout
        self._backoff_sequence = backoff_sequence or [3.0, 5.0, 10.0, 20.0, 30.0]
        self._heartbeat_warning_seconds = heartbeat_warning_seconds
        self._last_message_time = time.monotonic()

        # Data persistence
        self._enable_persistence = enable_persistence
        
        # Use shared queue or create own
        if shared_queue is not None:
            self._trade_queue = shared_queue
            self._owns_queue = False
        else:
            self._trade_queue = asyncio.Queue(maxsize=queue_maxsize)
            self._owns_queue = True
            
        self._parquet_writer: Optional[ParquetWriter] = None
        self._queue_dropped_count = 0
        self._messages_received_count = 0

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self.logger.info(
            "collector.starting",
            name=self.name,
            url=self.url,
            connect_timeout=self._connect_timeout,
            persistence_enabled=self._enable_persistence,
        )
        self._shutdown.clear()

        # Start parquet writer if persistence is enabled
        if self._enable_persistence:
            self._parquet_writer = ParquetWriter(queue=self._trade_queue, symbol=self.symbol)
            await self._parquet_writer.start()

        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self.logger.info(
            "collector.stop_requested",
            name=self.name,
            queue_dropped_count=self._queue_dropped_count,
        )
        self._shutdown.set()
        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.close(code=1001, reason="shutdown")
            except Exception as error:
                self.logger.warning(
                    "collector.close_error",
                    error=str(error),
                )
        if self._task is not None:
            await self._task
            self._task = None

        # Stop parquet writer and flush remaining data
        if self._parquet_writer is not None:
            await self._parquet_writer.stop()
            self._parquet_writer = None

        self.logger.info("collector.stopped", name=self.name)

    async def _run(self) -> None:
        backoff_index = 0
        while not self._shutdown.is_set():
            try:
                await self._connect_and_stream()
                if self._shutdown.is_set():
                    break
                delay = self._backoff_sequence[min(backoff_index, len(self._backoff_sequence) - 1)]
                self.logger.warning(
                    "collector.reconnecting",
                    reason="connection_closed",
                    delay_seconds=delay,
                )
                await asyncio.sleep(delay)
                backoff_index = min(backoff_index + 1, len(self._backoff_sequence) - 1)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger.error(
                    "collector.error",
                    error=str(error),
                    traceback=traceback.format_exc(),
                )
                delay = self._backoff_sequence[min(backoff_index, len(self._backoff_sequence) - 1)]
                self.logger.warning(
                    "collector.reconnecting",
                    reason="fatal_error",
                    delay_seconds=delay,
                )
                await asyncio.sleep(delay)
                backoff_index = min(backoff_index + 1, len(self._backoff_sequence) - 1)

        self.logger.info("collector.run_complete", name=self.name)

    async def _connect_and_stream(self) -> None:
        self.logger.info("collector.connecting", url=self.url)
        async with websockets.connect(
            self.url,
            open_timeout=self._connect_timeout,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2_000_000,
        ) as websocket:
            self._ws = websocket
            self.logger.info("collector.connected", url=self.url)
            self._last_message_time = time.monotonic()
            heartbeat_task = asyncio.create_task(self._heartbeat_warning_loop())
            queue_stats_task = asyncio.create_task(self._queue_stats_loop())
            try:
                async for raw_message in websocket:
                    if self._shutdown.is_set():
                        break
                    self._last_message_time = time.monotonic()
                    raw_text = self._decode_message(raw_message)
                    self.logger.debug(
                        "collector.raw_message_received",
                        raw_message=raw_text,
                    )
                    payload = self._parse_message(raw_text)
                    if payload is None:
                        continue
                    normalized = self._normalize_event(payload)
                    if normalized is None:
                        continue
                    self._log_trade(normalized)
            except asyncio.CancelledError:
                raise
            except ConnectionClosedOK:
                self.logger.info("collector.disconnected_cleanly")
            except ConnectionClosedError as error:
                self.logger.warning(
                    "collector.disconnected_error",
                    error=str(error),
                )
                raise
            except Exception as error:
                self.logger.error(
                    "collector.stream_error",
                    error=str(error),
                    traceback=traceback.format_exc(),
                )
                raise
            finally:
                heartbeat_task.cancel()
                queue_stats_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
                with contextlib.suppress(asyncio.CancelledError):
                    await queue_stats_task
                self._ws = None

    async def _heartbeat_warning_loop(self) -> None:
        while not self._shutdown.is_set():
            await asyncio.sleep(self._heartbeat_warning_seconds / 2)
            age = time.monotonic() - self._last_message_time
            if age > self._heartbeat_warning_seconds:
                self.logger.warning(
                    "collector.heartbeat_warning",
                    age_seconds=age,
                    threshold_seconds=self._heartbeat_warning_seconds,
                )

    async def _queue_stats_loop(self) -> None:
        """Periodically log queue statistics (~5 seconds interval)."""
        while not self._shutdown.is_set():
            await asyncio.sleep(5.0)
            if self._enable_persistence:
                self.logger.info(
                    "queue_size",
                    current_size=self._trade_queue.qsize(),
                    max_size=self._trade_queue.maxsize,
                    dropped_count=self._queue_dropped_count,
                )

    def _decode_message(self, raw_message: Any) -> str:
        if isinstance(raw_message, (bytes, bytearray)):
            return raw_message.decode("utf-8", errors="replace")
        return str(raw_message)

    def _parse_message(self, raw_text: str) -> Optional[dict[str, Any]]:
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as error:
            self.logger.error(
                "collector.invalid_json",
                error=str(error),
                raw_message=raw_text,
            )
            return None

        if not isinstance(payload, dict):
            self.logger.warning(
                "collector.invalid_payload",
                raw_message=raw_text,
                reason="payload_not_object",
            )
            return None

        return payload

    def _normalize_event(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        event_type = payload.get("e")
        symbol = payload.get("s")
        price = payload.get("p")
        quantity = payload.get("q")
        trade_time = payload.get("T")
        agg_trade_id = payload.get("a")
        maker_side = payload.get("m")

        if event_type != self.EVENT_TYPE or symbol != self.symbol:
            self.logger.warning(
                "collector.unexpected_event",
                event_type=event_type,
                symbol=symbol,
            )
            return None

        if price is None or quantity is None or trade_time is None or agg_trade_id is None:
            self.logger.warning(
                "collector.incomplete_trade",
                payload=payload,
            )
            return None

        side = "SELL" if maker_side else "BUY"
        received_time = int(time.time() * 1000)
        latency_ms = float(received_time - int(trade_time))

        self._messages_received_count += 1

        return {
            "exchange": self.EXCHANGE,
            "symbol": symbol,
            "event_type": event_type,
            "price": float(price),
            "quantity": float(quantity),
            "side": side,
            "trade_time": int(trade_time),
            "received_time": received_time,
            "latency_ms": latency_ms,
            "agg_trade_id": int(agg_trade_id),
        }

    def _log_trade(self, event: dict[str, Any]) -> None:
        """
        Log trade event and push to persistence queue.
        
        Always logs for visibility, then attempts to queue if persistence is enabled.
        """
        self.logger.info(
            "trade.received",
            exchange=event["exchange"],
            symbol=event["symbol"],
            event_type=event["event_type"],
            price=event["price"],
            quantity=event["quantity"],
            side=event["side"],
            trade_time=event["trade_time"],
            received_time=event["received_time"],
            latency_ms=event["latency_ms"],
            agg_trade_id=event["agg_trade_id"],
        )

        # Queue the record for persistence (non-blocking)
        if self._enable_persistence:
            self._queue_trade(event)

    def _queue_trade(self, event: dict[str, Any]) -> None:
        """
        Queue trade event for persistence (non-blocking).
        
        If queue is full, drop the oldest item and log warning.
        This ensures websocket loop never blocks.
        """
        try:
            self._trade_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop oldest item and insert new one
            try:
                self._trade_queue.get_nowait()
                self._trade_queue.put_nowait(event)
                self._queue_dropped_count += 1
                self.logger.warning(
                    "queue.full_drop",
                    symbol=self.symbol,
                    dropped_total=self._queue_dropped_count,
                    queue_size=self._trade_queue.qsize(),
                )
            except asyncio.QueueEmpty:
                # Race condition - queue emptied, try again
                try:
                    self._trade_queue.put_nowait(event)
                except asyncio.QueueFull:
                    # Still full after drop, give up and log
                    self._queue_dropped_count += 1
                    self.logger.error(
                        "queue.full_drop_failed",
                        symbol=self.symbol,
                        dropped_total=self._queue_dropped_count,
                    )

    def get_metrics(self) -> dict[str, Any]:
        """Get current collector metrics for observability."""
        return {
            "symbol": self.symbol,
            "messages_received": self._messages_received_count,
            "queue_size": self._trade_queue.qsize(),
            "queue_dropped_count": self._queue_dropped_count,
        }
