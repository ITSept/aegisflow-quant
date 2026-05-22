from __future__ import annotations

import asyncio
import sys
from typing import List

import uvloop

from collectors.base import Collector
from collectors.binance.trade_collector import BinanceTradeCollector
from configs.settings import AppSettings
from utils.logger import configure_logger, get_logger
from utils.signals import ShutdownManager


class Application:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.logger = get_logger(self.settings.app_name)
        self.shutdown_manager = ShutdownManager()
        self.collectors: List[Collector] = [
            BinanceTradeCollector(
                connect_timeout=self.settings.ws_connect_timeout_seconds,
            )
        ]

    async def startup(self) -> None:
        self.logger.info(
            "app.startup",
            environment=self.settings.environment,
            app_name=self.settings.app_name,
        )
        self.shutdown_manager.install()

        for collector in self.collectors:
            await collector.start()
        self.logger.info("app.ready", collector_count=len(self.collectors))

    async def shutdown(self) -> None:
        self.logger.info("app.shutdown_started")
        for collector in reversed(self.collectors):
            await collector.stop()
        self.logger.info("app.shutdown_complete")

    async def run(self) -> int:
        await self.startup()
        await self.shutdown_manager.wait()
        await self.shutdown()
        return 0


def main() -> int:
    uvloop.install()
    settings = AppSettings()
    configure_logger(settings.log_level)
    logger = get_logger("bootstrap")
    logger.info("bootstrap.started")

    return asyncio.run(Application(settings).run())


if __name__ == "__main__":
    raise SystemExit(main())
