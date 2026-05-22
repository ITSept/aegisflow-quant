"""Tests for parquet writer and persistence layer."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from storage.parquet_writer import ParquetWriter


@pytest.mark.asyncio
async def test_parquet_writer_writes_batch() -> None:
    """Test that parquet writer creates a parquet file with correct records."""
    output_dir = Path("/tmp/test_parquet_output")
    output_dir.mkdir(exist_ok=True)

    queue: asyncio.Queue = asyncio.Queue()
    writer = ParquetWriter(
        queue=queue,
        output_dir=str(output_dir),
        batch_size=2,
        batch_timeout_seconds=10.0,
    )

    await writer.start()

    # Add test records
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    for i in range(3):
        record = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "event_type": "aggTrade",
            "price": 27845.1 + i,
            "quantity": 0.001 * (i + 1),
            "side": "BUY" if i % 2 == 0 else "SELL",
            "trade_time": now_ms - 100,
            "received_time": now_ms,
            "latency_ms": 100.0,
            "agg_trade_id": 1000 + i,
        }
        queue.put_nowait(record)

    # Give writer time to process
    await asyncio.sleep(0.5)

    # Stop writer (will flush remaining records)
    await writer.stop()

    # Verify files were created
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files found in {output_dir}"

    # Cleanup
    for f in parquet_files:
        f.unlink()
    output_dir.rmdir()


@pytest.mark.asyncio
async def test_parquet_writer_flushes_on_shutdown() -> None:
    """Test that parquet writer flushes remaining records on shutdown."""
    output_dir = Path("/tmp/test_parquet_shutdown")
    output_dir.mkdir(exist_ok=True)

    queue: asyncio.Queue = asyncio.Queue()
    writer = ParquetWriter(
        queue=queue,
        output_dir=str(output_dir),
        batch_size=100,  # Large batch size so shutdown flush is needed
        batch_timeout_seconds=5.0,  # Reasonable timeout
    )

    await writer.start()

    # Add just one record
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    record = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_type": "aggTrade",
        "price": 27845.1,
        "quantity": 0.001,
        "side": "BUY",
        "trade_time": now_ms - 100,
        "received_time": now_ms,
        "latency_ms": 100.0,
        "agg_trade_id": 1000,
    }
    queue.put_nowait(record)

    # Give consumer time to pick up the record
    await asyncio.sleep(0.1)

    # Stop writer (should flush the single record)
    await writer.stop()

    # Verify file was created
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) == 1, f"Expected 1 file, got {len(parquet_files)}"

    # Cleanup
    for f in parquet_files:
        f.unlink()
    output_dir.rmdir()


@pytest.mark.asyncio
async def test_parquet_writer_timeout_flush() -> None:
    """Test that parquet writer flushes on timeout even with partial batch."""
    output_dir = Path("/tmp/test_parquet_timeout")
    output_dir.mkdir(exist_ok=True)

    queue: asyncio.Queue = asyncio.Queue()
    writer = ParquetWriter(
        queue=queue,
        output_dir=str(output_dir),
        batch_size=100,  # Large batch size
        batch_timeout_seconds=0.5,  # Short timeout for testing
    )

    await writer.start()

    # Add a few records
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    for i in range(3):
        record = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "event_type": "aggTrade",
            "price": 27845.1 + i,
            "quantity": 0.001,
            "side": "BUY",
            "trade_time": now_ms - 100,
            "received_time": now_ms,
            "latency_ms": 100.0,
            "agg_trade_id": 1000 + i,
        }
        queue.put_nowait(record)

    # Wait for timeout to trigger flush
    await asyncio.sleep(1.0)

    # Stop writer
    await writer.stop()

    # Verify file was created
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files found in {output_dir}"

    # Cleanup
    for f in parquet_files:
        f.unlink()
    output_dir.rmdir()


@pytest.mark.asyncio
async def test_parquet_writer_creates_correct_schema() -> None:
    """Test that parquet file has correct schema."""
    import pyarrow.parquet as pq

    output_dir = Path("/tmp/test_parquet_schema")
    output_dir.mkdir(exist_ok=True)

    queue: asyncio.Queue = asyncio.Queue()
    writer = ParquetWriter(
        queue=queue,
        output_dir=str(output_dir),
        batch_size=1,
    )

    await writer.start()

    # Add one record
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    record = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_type": "aggTrade",
        "price": 27845.1,
        "quantity": 0.001,
        "side": "BUY",
        "trade_time": now_ms - 100,
        "received_time": now_ms,
        "latency_ms": 100.0,
        "agg_trade_id": 1000,
    }
    queue.put_nowait(record)

    # Give writer time to process
    await asyncio.sleep(0.2)

    await writer.stop()

    # Read parquet file and verify schema
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) == 1

    table = pq.read_table(str(parquet_files[0]))
    column_names = table.column_names

    expected_columns = [
        "exchange",
        "symbol",
        "event_type",
        "price",
        "quantity",
        "side",
        "trade_time",
        "received_time",
        "latency_ms",
        "agg_trade_id",
    ]
    assert column_names == expected_columns

    # Cleanup
    for f in parquet_files:
        f.unlink()
    output_dir.rmdir()


@pytest.mark.asyncio
async def test_collector_queue_persistence() -> None:
    """Test that BinanceTradeCollector queues records without blocking."""
    from collectors.binance.trade_collector import BinanceTradeCollector

    collector = BinanceTradeCollector(enable_persistence=True, queue_maxsize=10)

    # The queue should exist and be usable
    assert collector._trade_queue is not None
    assert collector._trade_queue.maxsize == 10

    # Test queueing a record
    record = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_type": "aggTrade",
        "price": 27845.1,
        "quantity": 0.001,
        "side": "BUY",
        "trade_time": 1000,
        "received_time": 2000,
        "latency_ms": 1000.0,
        "agg_trade_id": 1000,
    }

    # This should not block
    collector._queue_trade(record)
    assert collector._trade_queue.qsize() == 1


@pytest.mark.asyncio
async def test_collector_queue_overflow_handling() -> None:
    """Test that collector handles queue overflow gracefully."""
    from collectors.binance.trade_collector import BinanceTradeCollector

    collector = BinanceTradeCollector(enable_persistence=True, queue_maxsize=2)

    # Fill the queue
    for i in range(5):
        record = {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "event_type": "aggTrade",
            "price": 27845.1 + i,
            "quantity": 0.001,
            "side": "BUY",
            "trade_time": 1000,
            "received_time": 2000 + i,
            "latency_ms": 1000.0,
            "agg_trade_id": 1000 + i,
        }
        collector._queue_trade(record)

    # Queue should only hold maxsize items
    assert collector._trade_queue.qsize() <= collector._trade_queue.maxsize
    # Should have dropped some items
    assert collector._queue_dropped_count > 0
