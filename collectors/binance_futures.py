from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

from collectors.base import Collector
from utils.logger import get_logger


class BinanceFuturesCollector(Collector):
    """Async Binance Futures aggregate trade websocket collector."""

    URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
    EXCHANGE = "binance"
    SYMBOL = "BTCUSDT"
    EVENT_TYPE = "aggTrade"

    def __init__(
        self,
        connect_timeout: float = 10.0,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        heartbeat_timeout: float = 20.0,
    ) -> None:
        super().__init__(name="binance-futures-aggtrade")
        self.logger = get_logger("collectors.binance_futures")
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._connect_timeout = connect_timeout
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._heartbeat_timeout = heartbeat_timeout
        self._last_message_time = time.monotonic()
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return

        self.logger.info(
            "collector.starting",
            name=self.name,
            url=self.URL,
            reconnect_delay=self._reconnect_delay,
            heartbeat_timeout=self._heartbeat_timeout,
        )
        self._shutdown.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self.logger.info("collector.stop_requested", name=self.name)
        self._shutdown.set()
        if self._websocket is not None and not self._websocket.closed:
            await self._websocket.close(code=1001, reason="shutdown")

        if self._task is not None:
            await self._task
            self._task = None
        self.logger.info("collector.stopped", name=self.name)

    async def _run(self) -> None:
        backoff = self._reconnect_delay
        while not self._shutdown.is_set():
            try:
                await self._connect_and_consume()
                backoff = self._reconnect_delay
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger.warning(
                    "collector.connection_lost",
                    error=str(error),
                    backoff_seconds=backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_reconnect_delay)

        self.logger.info("collector.run_complete", name=self.name)

    async def _connect_and_consume(self) -> None:
        self.logger.info("collector.connecting", url=self.URL)
        async with websockets.connect(
            self.URL,
            open_timeout=self._connect_timeout,
            ping_interval=15,
            ping_timeout=10,
            close_timeout=5,
        ) as websocket:
            self._websocket = websocket
            self.logger.info("collector.connected", url=self.URL)
            self._last_message_time = time.monotonic()
            heartbeat_task = asyncio.create_task(self._heartbeat_monitor(websocket))
            try:
                while not self._shutdown.is_set():
                    recv_task = asyncio.create_task(websocket.recv())
                    shutdown_task = asyncio.create_task(self._shutdown.wait())
                    done, pending = await asyncio.wait(
                        {recv_task, shutdown_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in pending:
                        task.cancel()

                    if shutdown_task in done:
                        recv_task.cancel()
                        break

                    if recv_task not in done:
                        continue

                    raw_message = recv_task.result()
                    raw_message_text = (
                        raw_message.decode("utf-8", errors="replace")
                        if isinstance(raw_message, (bytes, bytearray))
                        else str(raw_message)
                    )
                    self._last_message_time = time.monotonic()
                    self.logger.debug(
                        "collector.raw_message_received",
                        raw_message=raw_message_text,
                    )

                    payload = self._parse_message(raw_message_text)
                    if payload is None:
                        continue

                    normalized = self._normalize_event(payload)
                    if normalized is None:
                        continue

                    self._log_trade(normalized)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "collector.heartbeat_missed",
                    threshold_seconds=self._heartbeat_timeout,
                )
                await websocket.close(code=1012, reason="heartbeat_timeout")
            except ConnectionClosedOK:
                self.logger.info("collector.connection_closed_cleanly")
            except ConnectionClosedError as error:
                self.logger.warning("collector.connection_closed_error", error=str(error))
                raise
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
                self._websocket = None

    async def _heartbeat_monitor(self, websocket: websockets.WebSocketClientProtocol) -> None:
        while not self._shutdown.is_set():
            await asyncio.sleep(self._heartbeat_timeout / 2)
            age = time.monotonic() - self._last_message_time
            if age > self._heartbeat_timeout:
                self.logger.warning(
                    "collector.heartbeat.timeout",
                    age_seconds=age,
                    max_seconds=self._heartbeat_timeout,
                )
                if not websocket.closed:
                    await websocket.close(code=1012, reason="heartbeat_timeout")
                break

    def _parse_message(self, raw_message: str) -> Optional[dict[str, Any]]:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError as error:
            self.logger.error(
                "collector.invalid_json",
                error=str(error),
                raw_message=raw_message,
            )
            return None

        if not isinstance(payload, dict):
            self.logger.warning(
                "collector.invalid_payload",
                raw_message=raw_message,
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

        if event_type != self.EVENT_TYPE or symbol != self.SYMBOL:
            self.logger.warning(
                "collector.ignored_event",
                event_type=event_type,
                symbol=symbol,
            )
            return None

        if price is None or quantity is None or trade_time is None:
            self.logger.warning(
                "collector.incomplete_trade",
                payload=payload,
                missing_fields=[field for field in ("p", "q", "T") if payload.get(field) is None],
            )
            return None

        received_time = int(time.time() * 1000)
        latency_ms = float(received_time - int(trade_time))

        return {
            "exchange": self.EXCHANGE,
            "symbol": symbol,
            "event_type": event_type,
            "price": float(price),
            "quantity": float(quantity),
            "trade_time": int(trade_time),
            "received_time": received_time,
            "latency_ms": latency_ms,
        }

    def _log_trade(self, event: dict[str, Any]) -> None:
        self.logger.info(
            "trade.received",
            exchange=event["exchange"],
            symbol=event["symbol"],
            event_type=event["event_type"],
            price=event["price"],
            quantity=event["quantity"],
            trade_time=event["trade_time"],
            received_time=event["received_time"],
            latency_ms=event["latency_ms"],
        )
