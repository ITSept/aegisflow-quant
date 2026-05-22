"""
Example: Using BinanceTradeCollector with Data Persistence

This example demonstrates:
1. Starting the collector with persistence enabled
2. Monitoring queue metrics
3. Graceful shutdown with flush
"""

import asyncio
import signal
from collectors.binance.trade_collector import BinanceTradeCollector
from utils.logger import get_logger

logger = get_logger("example")


async def monitor_collector(collector: BinanceTradeCollector, duration_seconds: int = 300) -> None:
    """
    Run the collector for a specified duration with periodic status updates.
    
    Args:
        collector: The BinanceTradeCollector instance
        duration_seconds: How long to run (default 5 minutes)
    """
    logger.info(
        "example.starting",
        duration_seconds=duration_seconds,
        persistence_enabled=collector._enable_persistence,
    )

    try:
        # Start the collector (including parquet writer if persistence enabled)
        await collector.start()

        # Monitor for the specified duration
        await asyncio.sleep(duration_seconds)

    except asyncio.CancelledError:
        logger.info("example.cancelled")
        raise
    except Exception as error:
        logger.error("example.error", error=str(error))
        raise
    finally:
        # Graceful shutdown
        logger.info("example.shutting_down")
        await collector.stop()
        logger.info("example.finished")


async def main() -> None:
    """Main entry point."""
    # Create collector with persistence enabled (default)
    collector = BinanceTradeCollector(
        queue_maxsize=5000,
        enable_persistence=True,
    )

    # Setup signal handling for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        logger.info("example.signal_received")
        # Create a task to stop the collector
        asyncio.create_task(stop_collector())

    async def stop_collector() -> None:
        await collector.stop()

    # Add signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Run the collector for 5 minutes (or until interrupted)
    await monitor_collector(collector, duration_seconds=300)


if __name__ == "__main__":
    print("BinanceTradeCollector with Data Persistence Example")
    print("=" * 60)
    print("This example will:")
    print("1. Connect to Binance WebSocket for BTCUSDT aggTrade stream")
    print("2. Buffer trades in memory queue (max 5000)")
    print("3. Write batches of 100 trades to parquet files")
    print("4. Save files to: data/binance/aggtrade/BTCUSDT/")
    print("5. Run for 5 minutes (or until Ctrl+C)")
    print("=" * 60)
    print()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown complete.")
