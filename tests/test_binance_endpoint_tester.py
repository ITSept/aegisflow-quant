from collectors.binance_endpoint_tester import BinanceEndpointTester


def test_build_candidates_has_expected_urls() -> None:
    tester = BinanceEndpointTester()
    candidates = tester.build_candidates()

    assert any(candidate.full_url.endswith("/ws/btcusdt@aggTrade") for candidate in candidates)
    assert any("/stream?streams=btcusdt@aggTrade" in candidate.full_url for candidate in candidates)
    assert len(candidates) == 8
    assert len({candidate.full_url for candidate in candidates}) == 6
