from collectors.binance.trade_collector import BinanceTradeCollector


def test_normalize_event_parses_value_fields() -> None:
    collector = BinanceTradeCollector()
    payload = {
        "e": "aggTrade",
        "s": "BTCUSDT",
        "p": "27845.10",
        "q": "0.001",
        "T": 1716140199000,
        "a": 123456,
        "m": False,
    }

    normalized = collector._normalize_event(payload)

    assert normalized is not None
    assert normalized["exchange"] == "binance"
    assert normalized["symbol"] == "BTCUSDT"
    assert normalized["event_type"] == "aggTrade"
    assert normalized["price"] == 27845.1
    assert normalized["quantity"] == 0.001
    assert normalized["side"] == "BUY"
    assert normalized["agg_trade_id"] == 123456
    assert isinstance(normalized["latency_ms"], float)
    assert normalized["received_time"] >= normalized["trade_time"]


def test_parse_message_invalid_json_returns_none() -> None:
    collector = BinanceTradeCollector()
    payload = collector._parse_message("{invalid json}")

    assert payload is None


def test_decode_message_bytes() -> None:
    collector = BinanceTradeCollector()
    encoded = b"{\"e\": \"aggTrade\"}"

    assert collector._decode_message(encoded) == '{"e": "aggTrade"}'
