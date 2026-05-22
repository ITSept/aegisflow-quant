# Data Persistence Layer - Phase 2A (Minimal Stable Version)

## Overview

This document describes the simple, stable data persistence layer added to the BinanceTradeStream collector. The system follows a minimal design focused on **reliability** and **stability** over performance optimization.

## Architecture

```
WebSocket Stream
       ↓
BinanceTradeCollector
       ↓
asyncio.Queue (5000 max)
       ↓
ParquetWriter (Background Task)
       ↓
Parquet Files (data/binance/aggtrade/BTCUSDT/)
```

## Components

### 1. Queue Buffer (`asyncio.Queue`)

- **Location**: `BinanceTradeCollector._trade_queue`
- **Max Size**: 5000 records (configurable)
- **Strategy**: Non-blocking, drops oldest item on overflow
- **Key Feature**: **NEVER blocks the websocket loop**

#### Queue Overflow Handling

If the queue fills up:
1. Drop the oldest item from the queue
2. Insert the new item
3. Log warning: `queue.full_drop`
4. Track dropped count in metrics

This ensures the websocket collector can continue streaming without backpressure.

### 2. Parquet Writer (`storage/parquet_writer.py`)

Background async task that:
- Consumes records from the queue
- Batches records
- Writes to parquet files

#### Batching Strategy

Writes a batch when **either**:
- 100 records accumulated, **OR**
- 5 seconds elapsed (whichever comes first)

#### File Output

Files are written to:
```
/data/binance/aggtrade/BTCUSDT/part-YYYYMMDD-HH-MM.parquet
```

Filename format includes timestamp of first record in batch.

### 3. Record Schema

Each record contains:

```python
{
    "exchange": "binance",          # str
    "symbol": "BTCUSDT",            # str
    "event_type": "aggTrade",       # str
    "price": 27845.1,               # float
    "quantity": 0.001,              # float
    "side": "BUY",                  # str ("BUY" or "SELL")
    "trade_time": 1716140199000,    # int (ms since epoch)
    "received_time": 1716140200000, # int (ms since epoch)
    "latency_ms": 1000.0,           # float
    "agg_trade_id": 123456,         # int
}
```

## Usage

### Basic Usage (Persistence Enabled by Default)

```python
from collectors.binance.trade_collector import BinanceTradeCollector
import asyncio

async def main():
    # Create collector (persistence enabled by default)
    collector = BinanceTradeCollector()
    
    # Start the collector and parquet writer
    await collector.start()
    
    # Let it run for a while
    await asyncio.sleep(60)
    
    # Stop gracefully (flushes remaining records)
    await collector.stop()

asyncio.run(main())
```

### Disable Persistence (Optional)

```python
collector = BinanceTradeCollector(enable_persistence=False)
```

### Configure Queue Size

```python
collector = BinanceTradeCollector(queue_maxsize=2000)
```

### Use as Context Manager

```python
async with BinanceTradeCollector() as collector:
    await asyncio.sleep(60)
```

## Logging

### Structured Logs Emitted

The system logs these events:

**Startup**:
```
collector.starting persistence_enabled=true
parquet_writer.starting batch_size=100 batch_timeout_seconds=5.0
```

**Trade Events** (every trade):
```
trade.received exchange=binance symbol=BTCUSDT price=27845.1 ...
```

**Queue Metrics** (every ~5 seconds):
```
queue_size current_size=150 max_size=5000 dropped_count=0
```

**Queue Overflow**:
```
queue.full_drop dropped_total=5 queue_size=5000
```

**Batch Writes**:
```
parquet_write_success filename=part-20260522-06-08.parquet record_count=100
```

**Shutdown**:
```
collector.stop_requested queue_dropped_count=0
parquet_writer.flushing_on_shutdown batch_size=25
parquet_writer.stopped total_records=1000 total_batches=10
```

## Stability Guarantees

### No Data Loss on Shutdown

1. When `collector.stop()` is called:
   - WebSocket closes gracefully
   - Collector task terminates
   - Parquet writer flushes all remaining records to disk
   - Final parquet file is written

### No Blocking

- Queue push is **always** non-blocking (`put_nowait`)
- If queue is full, oldest item is dropped (not websocket loop)
- Trade collection never stalls

### Graceful Degradation

- If parquet write fails: error is logged, next batch is attempted
- If queue overflows: oldest item is dropped, metrics logged
- If directory doesn't exist: created automatically

## Validation Criteria

✓ Websocket continues streaming normally  
✓ Queue receives data without blocking websocket  
✓ Parquet files are generated successfully  
✓ Shutdown flush works (no data loss)  
✓ No crashes under normal load  
✓ Memory usage stays stable  
✓ Queue drop handling is correct  

## Non-Goals (Not Implemented Yet)

These features are intentionally **NOT** included in Phase 2A:

- ❌ Partition by date/hour directory structure
- ❌ Compression tuning or optimization
- ❌ Multi-symbol support
- ❌ Multi-threading
- ❌ High-performance batching optimization
- ❌ Complex backpressure strategies
- ❌ Partitioning by any dimension
- ❌ Index creation or query optimization

**Reason**: Keep it simple first. Phase 2A focuses on **minimal stability** - prove the basic pipeline works reliably before adding complexity.

## File Locations

```
/workspaces/aegisflow-quant/
├── storage/
│   ├── __init__.py
│   └── parquet_writer.py          ← New: Parquet writer
├── collectors/
│   └── binance/
│       └── trade_collector.py     ← Modified: Added queue + persistence
├── data/
│   └── binance/
│       └── aggtrade/
│           └── BTCUSDT/           ← New: Output directory
├── tests/
│   ├── test_trade_collector.py
│   └── test_persistence.py        ← New: Persistence tests
└── requirements.txt               ← Modified: Added pyarrow
```

## Testing

Run persistence tests:

```bash
pytest tests/test_persistence.py -v
```

Run all tests:

```bash
pytest tests/ -v
```

## Key Implementation Details

### Queue Management

The queue uses a non-blocking FIFO strategy:
```python
try:
    self._trade_queue.put_nowait(event)
except asyncio.QueueFull:
    # Drop oldest, insert new
    self._trade_queue.get_nowait()
    self._trade_queue.put_nowait(event)
```

### Batch Writing

Timeout-based flush ensures no records wait indefinitely:
```python
try:
    record = await asyncio.wait_for(
        self.queue.get(),
        timeout=self.batch_timeout_seconds,
    )
except asyncio.TimeoutError:
    # Flush partial batch
    await self._write_batch()
```

### Graceful Shutdown

On stop, remaining records are flushed:
```python
await self._shutdown.set()
# ... close websocket ...
# Parquet writer stops consuming
# Then flushes any remaining records in __write_batch
```

## Performance Notes

### Expected Throughput

With typical Binance BTCUSDT aggTrade stream (~10 trades/sec):
- Queue never fills (5000 max, ~50 needed per batch)
- Batch writes every ~10-20 seconds (after 100 records)
- Parquet file ~10KB per 100 trades
- No backpressure, no queuing delay

### Memory Usage

- Queue: ~5000 records × ~500 bytes = ~2.5MB
- Batch buffer: 100 records × ~500 bytes = ~50KB  
- Parquet writer task: ~1MB
- **Total**: ~3.5MB stable footprint

## Troubleshooting

### "queue.full_drop" Warnings

This means records are arriving faster than they're being written. Check:
1. Disk I/O performance
2. Network latency to data source
3. Batch size settings

### No Parquet Files Created

Check:
1. Directory exists: `/data/binance/aggtrade/BTCUSDT/`
2. Disk space available
3. File permissions
4. Check logs for `parquet_write_error`

### High Memory Usage

- Queue shouldn't grow beyond 100-200 items in normal operation
- If queue is consistently full, batch_timeout or batch_size may need tuning
- Check for stuck writer task

## Future Enhancements (Phase 2B+)

After validating Phase 2A stability:

1. **Phase 2B**: Add partition by date/hour structure
2. **Phase 2C**: Add compression and tuning
3. **Phase 3**: Multi-symbol support
4. **Phase 4**: Performance optimization

## Summary

Phase 2A provides a **simple, stable foundation** for data persistence:

- ✓ Non-blocking queue buffer
- ✓ Reliable batch parquet writer
- ✓ Graceful shutdown with flush
- ✓ Comprehensive logging
- ✓ Zero data loss on normal shutdown
- ✓ No websocket loop blocking

The system prioritizes **reliability** over performance, making it safe for production use.
