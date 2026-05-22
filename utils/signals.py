from __future__ import annotations

import asyncio
import signal
from typing import Callable

from .logger import get_logger

logger = get_logger(__name__)


class ShutdownManager:
    """Handles graceful shutdown signals for async applications."""

    def __init__(self) -> None:
        self._event: asyncio.Event[None] = asyncio.Event()
        self._signals = (signal.SIGINT, signal.SIGTERM)

    def install(self) -> None:
        """Register signal handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()
        for sig in self._signals:
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except NotImplementedError:
                signal.signal(sig, self._legacy_handler(sig))

        logger.info("shutdown.installed", signals=[sig.name for sig in self._signals])

    def _legacy_handler(self, sig: signal.Signals) -> Callable[[int, object], None]:
        def handler(*_: object) -> None:
            self._on_signal(sig)

        return handler

    def _on_signal(self, sig: signal.Signals) -> None:
        logger.info("shutdown.signal_received", signal=sig.name)
        self._event.set()

    async def wait(self) -> None:
        """Wait until one of the configured shutdown signals is received."""
        await self._event.wait()
