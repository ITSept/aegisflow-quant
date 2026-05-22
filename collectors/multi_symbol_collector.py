"""Orchestrator to run multiple symbol collectors concurrently with per-symbol queues and metrics."""

from __future__ import annotations

import asyncio
import signal
from typing import Dict, List, Optional

from collectors.binance.trade_collector import BinanceTradeCollector
from configs.symbol_config import SymbolRegistry, get_registry, SymbolConfig
from utils.metrics import MetricsCollector
from utils.logger import get_logger


class MultiSymbolOrchestrator:
    """Create and manage collectors for multiple symbols.

    Uses per-symbol queues (one queue per symbol) to ensure isolation and
    straightforward drop policies.
    """

    def __init__(
        self,
        symbols: Optional[List[SymbolConfig]] = None,
        metrics_interval: float = 5.0,
    ) -> None:
        self.logger = get_logger("collectors.multi_symbol_orchestrator")
        self.registry = get_registry(symbols)
        self.metrics = MetricsCollector(interval_seconds=metrics_interval)
        self._collectors: Dict[str, BinanceTradeCollector] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._main_task: Optional[asyncio.Task[None]] = None
        self._stopping = False

    async def start(self) -> None:
        """Start collectors, writers and metrics collection."""
        self.logger.info("orchestrator.starting")

        # Create per-symbol queues and collectors
        for cfg in self.registry.get_enabled():
            queue = asyncio.Queue(maxsize=cfg.queue_maxsize)
            self._queues[cfg.symbol] = queue
            collector = BinanceTradeCollector(
                symbol=cfg.symbol,
                shared_queue=queue,
                connect_timeout=10.0,
                backoff_sequence=[3.0, 5.0, 10.0, 20.0],
                heartbeat_warning_seconds=60.0,
                enable_persistence=True,
            )
            self._collectors[cfg.symbol] = collector

        # Start metrics collector
        await self.metrics.start()

        # Start each collector (which will also start its parquet writer)
        for symbol, collector in self._collectors.items():
            await collector.start()
            # Register with metrics (collector and its writer)
            self.metrics.register_collector(symbol, collector)
            # Collector.start() creates parquet writer; register if present
            if getattr(collector, "_parquet_writer", None) is not None:
                self.metrics.register_writer(symbol, collector._parquet_writer)

        self.logger.info("orchestrator.started", symbols=list(self._collectors.keys()))

    async def stop(self) -> None:
        """Stop all collectors and metrics, ensuring graceful shutdown."""
        if self._stopping:
            return
        self._stopping = True
        self.logger.info("orchestrator.stop_requested")

        # Stop collectors concurrently
        stop_tasks = [collector.stop() for collector in self._collectors.values()]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Stop metrics (will log final summary)
        await self.metrics.stop()

        self.logger.info("orchestrator.stopped")

    async def run_forever(self) -> None:
        """Run until interrupted (Ctrl+C).

        Registers signal handlers for graceful shutdown.
        """
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        def _signal_handler() -> None:
            self.logger.info("orchestrator.signal_received")
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                # Signals not supported on some platforms (e.g., Windows)
                pass

        await self.start()

        # Wait until signal received
        await stop_event.wait()

        # Begin shutdown
        await self.stop()


async def run_example() -> None:
    """Convenience runner for manual testing."""
    orchestrator = MultiSymbolOrchestrator()
    await orchestrator.run_forever()


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(run_example())
    except KeyboardInterrupt:
        print("Shutdown requested")
