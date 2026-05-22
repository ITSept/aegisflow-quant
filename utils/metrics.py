"""Metrics collection and observability system."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from utils.logger import get_logger


class MetricsCollector:
    """Collects and logs metrics from multiple data sources."""
    
    def __init__(self, interval_seconds: float = 5.0) -> None:
        """
        Initialize metrics collector.
        
        Args:
            interval_seconds: How often to log metrics
        """
        self.interval_seconds = interval_seconds
        self.logger = get_logger("observability.metrics")
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        
        # Data sources
        self._collectors: Dict[str, Any] = {}  # symbol -> BinanceTradeCollector
        self._writers: Dict[str, Any] = {}      # symbol -> ParquetWriter
        self._start_time = time.time()
    
    def register_collector(self, symbol: str, collector: Any) -> None:
        """Register a collector for metrics collection."""
        self._collectors[symbol.upper()] = collector
    
    def register_writer(self, symbol: str, writer: Any) -> None:
        """Register a parquet writer for metrics collection."""
        self._writers[symbol.upper()] = writer
    
    async def start(self) -> None:
        """Start the metrics collection loop."""
        if self._task is not None and not self._task.done():
            return
        
        self.logger.info(
            "metrics.starting",
            interval_seconds=self.interval_seconds,
        )
        self._shutdown.clear()
        self._start_time = time.time()
        self._task = asyncio.create_task(self._collect_loop())
    
    async def stop(self) -> None:
        """Stop the metrics collection."""
        self.logger.info("metrics.stop_requested")
        self._shutdown.set()
        
        if self._task is not None:
            await self._task
            self._task = None
        
        # Log final summary
        await self._log_summary()
        self.logger.info("metrics.stopped")
    
    async def _collect_loop(self) -> None:
        """Main metrics collection loop."""
        try:
            while not self._shutdown.is_set():
                await asyncio.sleep(self.interval_seconds)
                await self._collect_and_log()
        except asyncio.CancelledError:
            self.logger.info("metrics.cancelled")
            raise
        except Exception as error:
            self.logger.error("metrics.error", error=str(error))
            raise
    
    async def _collect_and_log(self) -> None:
        """Collect metrics from all sources and log."""
        try:
            metrics = self._collect_metrics()
            self.logger.info("pipeline.metrics", **metrics)
        except Exception as error:
            self.logger.error("metrics.collection_error", error=str(error))
    
    def _collect_metrics(self) -> Dict[str, Any]:
        """Collect metrics from all registered sources."""
        metrics: Dict[str, Any] = {
            "event": "pipeline.metrics",
            "uptime_seconds": time.time() - self._start_time,
        }
        
        # Collector metrics (per symbol)
        total_messages = 0
        total_dropped = 0
        symbol_details = {}
        
        for symbol, collector in self._collectors.items():
            try:
                coll_metrics = collector.get_metrics()
                symbol_details[f"{symbol}_messages"] = coll_metrics.get("messages_received", 0)
                symbol_details[f"{symbol}_queue_size"] = coll_metrics.get("queue_size", 0)
                symbol_details[f"{symbol}_dropped"] = coll_metrics.get("queue_dropped_count", 0)
                
                total_messages += coll_metrics.get("messages_received", 0)
                total_dropped += coll_metrics.get("queue_dropped_count", 0)
            except Exception as error:
                self.logger.warning(
                    "metrics.collector_error",
                    symbol=symbol,
                    error=str(error),
                )
        
        metrics.update(symbol_details)
        metrics["total_messages_received"] = total_messages
        metrics["total_dropped_count"] = total_dropped
        
        # Writer metrics (per symbol)
        for symbol, writer in self._writers.items():
            try:
                writer_metrics = writer.get_metrics()
                metrics[f"{symbol}_records_written"] = writer_metrics.get("total_records_written", 0)
                metrics[f"{symbol}_batches"] = writer_metrics.get("total_batches", 0)
            except Exception as error:
                self.logger.warning(
                    "metrics.writer_error",
                    symbol=symbol,
                    error=str(error),
                )
        
        return metrics
    
    async def _log_summary(self) -> None:
        """Log final summary metrics."""
        try:
            metrics = self._collect_metrics()
            uptime = time.time() - self._start_time
            
            summary = {
                "event": "shutdown.summary",
                "uptime_seconds": uptime,
                "total_messages_received": metrics.get("total_messages_received", 0),
                "total_dropped_count": metrics.get("total_dropped_count", 0),
            }
            
            # Add per-symbol details
            for symbol in self._collectors.keys():
                summary[f"{symbol}_received"] = metrics.get(f"{symbol}_messages", 0)
                summary[f"{symbol}_written"] = metrics.get(f"{symbol}_records_written", 0)
            
            self.logger.info("pipeline.shutdown_summary", **summary)
        except Exception as error:
            self.logger.error("metrics.summary_error", error=str(error))
