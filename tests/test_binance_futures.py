from collectors.binance_futures import BinanceFuturesCollector


def test_binance_futures_normalize_event() -> None:
    collector = BinanceFuturesCollector()
    sample_payload = {
        "e": "aggTrade",
        "s": "BTCUSDT",
        "p": "27845.10",
        "q": "0.001",
        "T": 1716140199000,
    }

    normalized = collector._normalize_event(sample_payload)

    assert normalized["exchange"] == "binance"
    assert normalized["symbol"] == "BTCUSDT"
    assert normalized["event_type"] == "aggTrade"
    assert isinstance(normalized["price"], float)
    assert isinstance(normalized["quantity"], float)
    assert normalized["trade_time"] == 1716140199000
    assert normalized["received_time"] >= normalized["trade_time"]
    assert normalized["latency_ms"] >= 0.0
