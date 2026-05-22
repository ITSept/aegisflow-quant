"""Symbol configuration and registry for multi-symbol ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class SymbolConfig:
    """Configuration for a trading symbol."""
    
    symbol: str  # e.g., "BTCUSDT"
    enabled: bool = True
    queue_maxsize: int = 5000
    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    
    def __post_init__(self) -> None:
        """Normalize symbol to uppercase."""
        self.symbol = self.symbol.upper()


class SymbolRegistry:
    """Registry of supported trading symbols."""
    
    # Default supported symbols
    DEFAULT_SYMBOLS = [
        SymbolConfig(symbol="BTCUSDT"),
        SymbolConfig(symbol="ETHUSDT"),
    ]
    
    def __init__(self, symbols: Optional[List[SymbolConfig]] = None) -> None:
        """
        Initialize symbol registry.
        
        Args:
            symbols: List of SymbolConfig objects. If None, uses DEFAULT_SYMBOLS.
        """
        self._symbols: Dict[str, SymbolConfig] = {}
        
        configs = symbols or self.DEFAULT_SYMBOLS
        for config in configs:
            self.register(config)
    
    def register(self, config: SymbolConfig) -> None:
        """Register a symbol configuration."""
        symbol_upper = config.symbol.upper()
        self._symbols[symbol_upper] = config
    
    def get(self, symbol: str) -> Optional[SymbolConfig]:
        """Get configuration for a symbol."""
        return self._symbols.get(symbol.upper())
    
    def get_all(self) -> List[SymbolConfig]:
        """Get all registered symbol configurations."""
        return list(self._symbols.values())
    
    def get_enabled(self) -> List[SymbolConfig]:
        """Get all enabled symbols."""
        return [cfg for cfg in self._symbols.values() if cfg.enabled]
    
    def is_registered(self, symbol: str) -> bool:
        """Check if symbol is registered."""
        return symbol.upper() in self._symbols
    
    def __contains__(self, symbol: str) -> bool:
        """Check if symbol is registered (supports 'in' operator)."""
        return self.is_registered(symbol)


# Global registry instance
_global_registry: Optional[SymbolRegistry] = None


def get_registry(symbols: Optional[List[SymbolConfig]] = None) -> SymbolRegistry:
    """Get or create the global symbol registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = SymbolRegistry(symbols)
    return _global_registry


def reset_registry() -> None:
    """Reset the global registry (useful for testing)."""
    global _global_registry
    _global_registry = None
