import asyncio
import signal

import pytest

from utils.signals import ShutdownManager


@pytest.mark.asyncio
async def test_shutdown_manager_sets_event_and_waits() -> None:
    manager = ShutdownManager()
    manager._on_signal(signal.Signals.SIGTERM)

    await asyncio.wait_for(manager.wait(), timeout=1)
    assert manager._event.is_set()
