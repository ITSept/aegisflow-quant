"""Simple async parquet writer for trade data with queue-based batching."""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from utils.logger import get_logger


class ParquetWriter:
    """
    Async parquet writer that consumes records from a queue and writes batches to disk.
    
    Batches based on:
    - 100 records received, OR
    - 5 seconds elapsed (whichever comes first)
    """

    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        symbol: str = "BTCUSDT",
        output_dir: str = "data/binance/aggtrade",
        batch_size: int = 100,
        batch_timeout_seconds: float = 5.0,
    ) -> None:
        """
        Initialize the parquet writer.
        
        Args:
            queue: asyncio.Queue containing trade records to write
            symbol: Trading symbol (e.g., "BTCUSDT", "ETHUSDT")
            output_dir: Base directory to write parquet files to
            batch_size: Number of records to batch before writing
            batch_timeout_seconds: Maximum seconds to wait before flushing batch
        """
        self.symbol = symbol.upper()
        self.queue = queue
        # Create symbol-specific subdirectory
        base_path = Path(output_dir)
        self.output_dir = base_path / f"symbol={self.symbol}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.logger = get_logger(f"storage.parquet_writer.{symbol}")
        
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._batch: list[dict[str, Any]] = []
        self._batch_start_time = time.time()
        self._total_written = 0
        self._total_batches = 0

    async def start(self) -> None:
        """Start the parquet writer background task."""
        if self._task is not None and not self._task.done():
            return

        self.logger.info(
            "parquet_writer.starting",
            symbol=self.symbol,
            output_dir=str(self.output_dir),
            batch_size=self.batch_size,
            batch_timeout_seconds=self.batch_timeout_seconds,
        )
        self._shutdown.clear()
        self._batch_start_time = time.time()
        self._task = asyncio.create_task(self._consume_and_write())

    async def stop(self) -> None:
        """
        Stop the parquet writer and flush remaining records.
        
        Ensures no data loss on shutdown.
        """
        self.logger.info("parquet_writer.stop_requested", symbol=self.symbol)
        self._shutdown.set()

        if self._task is not None:
            await self._task
            self._task = None

        # Flush any remaining records
        if self._batch:
            self.logger.info(
                "parquet_writer.flushing_on_shutdown",
                symbol=self.symbol,
                batch_size=len(self._batch),
            )
            await self._write_batch()

        self.logger.info(
            "parquet_writer.stopped",
            symbol=self.symbol,
            total_records=self._total_written,
            total_batches=self._total_batches,
        )

    async def _consume_and_write(self) -> None:
        """Main loop: consume from queue and write batches."""
        try:
            while not self._shutdown.is_set():
                try:
                    # Wait for either:
                    # - a record from queue with timeout
                    # - shutdown event
                    record = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.batch_timeout_seconds,
                    )
                    
                    # Filter by symbol (for multi-symbol queue)
                    if record.get("symbol") != self.symbol:
                        continue
                    
                    self._batch.append(record)
                    queue_size = self.queue.qsize()

                    # Check if we should write
                    if len(self._batch) >= self.batch_size:
                        self.logger.debug(
                            "parquet_writer.batch_full",
                            symbol=self.symbol,
                            batch_size=len(self._batch),
                            queue_size=queue_size,
                        )
                        await self._write_batch()

                except asyncio.TimeoutError:
                    # Timeout waiting for queue - flush if we have records
                    if self._batch:
                        elapsed = time.time() - self._batch_start_time
                        self.logger.debug(
                            "parquet_writer.batch_timeout",
                            symbol=self.symbol,
                            batch_size=len(self._batch),
                            elapsed_seconds=elapsed,
                        )
                        await self._write_batch()

        except asyncio.CancelledError:
            self.logger.info("parquet_writer.cancelled", symbol=self.symbol)
            raise
        except Exception as error:
            self.logger.error(
                "parquet_writer.error",
                symbol=self.symbol,
                error=str(error),
                batch_size=len(self._batch),
            )
            raise

    async def _write_batch(self) -> None:
        """Write current batch to parquet file."""
        if not self._batch:
            return

        batch_records = len(self._batch)
        try:
            # Convert records to table
            table = self._records_to_table(self._batch)

            # Generate filename based on first record timestamp
            filename = self._generate_filename(self._batch[0])
            filepath = self.output_dir / filename

            # Write to parquet
            pq.write_table(table, str(filepath))

            self.logger.info(
                "parquet_write_success",
                filename=filename,
                record_count=batch_records,
                file_path=str(filepath),
            )

            self._total_written += batch_records
            self._total_batches += 1
            self._batch = []
            self._batch_start_time = time.time()

        except Exception as error:
            self.logger.error(
                "parquet_write_error",
                error=str(error),
                batch_size=batch_records,
            )
            # Re-raise to signal critical failure
            raise

    def _records_to_table(self, records: list[dict[str, Any]]) -> pa.Table:
        """Convert list of records to PyArrow table."""
        # Extract columns
        data = {
            "exchange": [r["exchange"] for r in records],
            "symbol": [r["symbol"] for r in records],
            "event_type": [r["event_type"] for r in records],
            "price": [r["price"] for r in records],
            "quantity": [r["quantity"] for r in records],
            "side": [r["side"] for r in records],
            "trade_time": [r["trade_time"] for r in records],
            "received_time": [r["received_time"] for r in records],
            "latency_ms": [r["latency_ms"] for r in records],
            "agg_trade_id": [r["agg_trade_id"] for r in records],
        }

        # Define schema
        schema = pa.schema([
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("event_type", pa.string()),
            ("price", pa.float64()),
            ("quantity", pa.float64()),
            ("side", pa.string()),
            ("trade_time", pa.int64()),
            ("received_time", pa.int64()),
            ("latency_ms", pa.float64()),
            ("agg_trade_id", pa.int64()),
        ])

        return pa.table(data, schema=schema)

    def _generate_filename(self, record: dict[str, Any]) -> str:
        """
        Generate filename based on record timestamp.
        Format: part-YYYYMMDD-HH-MM.parquet
        """
        # Use received_time (milliseconds since epoch)
        received_time_ms = record["received_time"]
        dt = datetime.fromtimestamp(received_time_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("part-%Y%m%d-%H-%M.parquet")
