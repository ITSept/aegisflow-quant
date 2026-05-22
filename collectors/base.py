from __future__ import annotations

import abc
from types import TracebackType
from typing import Optional, Type


class Collector(abc.ABC):
    """Base class for async data collectors across exchanges."""

    def __init__(self, name: str) -> None:
        self.name = name

    @abc.abstractmethod
    async def start(self) -> None:
        """Start the collector and any underlying async tasks."""
        raise NotImplementedError

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop the collector and cleanup resources."""
        raise NotImplementedError

    async def __aenter__(self) -> "Collector":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        await self.stop()
        return False
