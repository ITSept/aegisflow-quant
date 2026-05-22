from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK

from utils.logger import configure_logger, get_logger

ROOT_DIR = Path(__file__).resolve().parent.parent
RUNTIME_CONFIG_PATH = ROOT_DIR / "configs" / "runtime_stream_config.json"
STREAM_NAME = "btcusdt@aggTrade"
BASE_ENDPOINTS = [
    "wss://fstream.binance.com/ws",
    "wss://fstream.binance.com/stream",
    "wss://fstream-auth.binance.com/ws",
    "wss://stream.binance.com:9443",
]
TEST_DURATION_SECONDS = 10.0
MAX_ATTEMPTS = 2
BACKOFF_INITIAL = 1.0
BACKOFF_MAX = 8.0

logger = get_logger("collectors.binance_endpoint_tester")


@dataclass
class EndpointCandidate:
    base_url: str
    mode: str
    full_url: str


@dataclass
class EndpointResult:
    base_url: str
    mode: str
    full_url: str
    connection_success: bool = False
    first_message_latency_ms: Optional[float] = None
    messages_received: int = 0
    message_rate_per_second: float = 0.0
    disconnect_count: int = 0
    disconnect_rate: float = 0.0
    error_count: int = 0
    raw_error_logs: List[str] = field(default_factory=list)
    total_test_seconds: float = 0.0
    score: float = 0.0
    last_error: Optional[str] = None

    def compute_score(self) -> float:
        latency_ms = self.first_message_latency_ms if self.first_message_latency_ms is not None else 9999.0
        self.score = self.message_rate_per_second - (latency_ms * 0.1) - (self.error_count * 10)
        return self.score

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_url": self.base_url,
            "mode": self.mode,
            "full_url": self.full_url,
            "connection_success": self.connection_success,
            "first_message_latency_ms": self.first_message_latency_ms,
            "messages_received": self.messages_received,
            "message_rate_per_second": self.message_rate_per_second,
            "disconnect_count": self.disconnect_count,
            "disconnect_rate": self.disconnect_rate,
            "error_count": self.error_count,
            "raw_error_logs": self.raw_error_logs,
            "total_test_seconds": self.total_test_seconds,
            "score": self.score,
            "last_error": self.last_error,
        }


class BinanceEndpointTester:
    def __init__(
        self,
        stream_name: str = STREAM_NAME,
        duration_seconds: float = TEST_DURATION_SECONDS,
        max_attempts: int = MAX_ATTEMPTS,
        debug: bool = False,
    ) -> None:
        self.stream_name = stream_name
        self.duration_seconds = duration_seconds
        self.max_attempts = max_attempts
        self.debug = debug
        self._shutdown = asyncio.Event()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signame in ("SIGINT", "SIGTERM"):
            try:
                loop.add_signal_handler(getattr(signal, signame), self._shutdown.set)
            except Exception:
                # Some platforms do not support add_signal_handler
                pass

    @staticmethod
    def _build_candidate(base_url: str, mode: str) -> EndpointCandidate:
        base = base_url.rstrip("/")
        if mode == "raw":
            if base.endswith("/ws"):
                full_url = f"{base}/{STREAM_NAME}"
            elif base.endswith("/stream"):
                full_url = f"{base[:-len('/stream')]}/ws/{STREAM_NAME}"
            else:
                full_url = f"{base}/ws/{STREAM_NAME}"
        else:
            if base.endswith("/stream"):
                full_url = f"{base}?streams={STREAM_NAME}"
            elif base.endswith("/ws"):
                full_url = f"{base[:-len('/ws')]}/stream?streams={STREAM_NAME}"
            else:
                full_url = f"{base}/stream?streams={STREAM_NAME}"

        return EndpointCandidate(base_url=base_url, mode=mode, full_url=full_url)

    def build_candidates(self) -> List[EndpointCandidate]:
        candidates: List[EndpointCandidate] = []
        for base_url in BASE_ENDPOINTS:
            for mode in ("raw", "combined"):
                candidates.append(self._build_candidate(base_url, mode))
        return candidates

    async def test_all(self) -> List[EndpointResult]:
        candidates = self.build_candidates()
        results: List[EndpointResult] = []
        for candidate in candidates:
            if self._shutdown.is_set():
                break
            result = await self._test_candidate(candidate)
            results.append(result)
        return results

    async def _test_candidate(self, candidate: EndpointCandidate) -> EndpointResult:
        metrics = EndpointResult(
            base_url=candidate.base_url,
            mode=candidate.mode,
            full_url=candidate.full_url,
        )

        backoff = BACKOFF_INITIAL
        for attempt in range(1, self.max_attempts + 1):
            if self._shutdown.is_set():
                break

            self.logger_info(
                "endpoint.attempt",
                base_url=candidate.base_url,
                mode=candidate.mode,
                full_url=candidate.full_url,
                attempt=attempt,
                backoff_seconds=backoff,
            )

            attempt_result = await self._attempt_connection(candidate, attempt)
            metrics.error_count += attempt_result.error_count
            metrics.raw_error_logs.extend(attempt_result.raw_error_logs)
            metrics.disconnect_count += attempt_result.disconnect_count
            if attempt_result.connection_success:
                metrics.connection_success = True
                metrics.first_message_latency_ms = (
                    attempt_result.first_message_latency_ms
                    if metrics.first_message_latency_ms is None
                    else min(metrics.first_message_latency_ms, attempt_result.first_message_latency_ms)
                )
                metrics.messages_received += attempt_result.messages_received
                metrics.total_test_seconds += attempt_result.total_test_seconds
                metrics.last_error = attempt_result.last_error

                if attempt_result.messages_received > 0:
                    metrics.message_rate_per_second = (
                        metrics.messages_received / max(metrics.total_test_seconds, 1e-6)
                    )
                    break

            if attempt < self.max_attempts and not self._shutdown.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)

        attempts = max(1, self.max_attempts)
        metrics.disconnect_rate = metrics.disconnect_count / attempts
        metrics.compute_score()
        self.logger_info("endpoint.result", **metrics.to_dict())
        return metrics

    async def _attempt_connection(
        self, candidate: EndpointCandidate, attempt: int
    ) -> EndpointResult:
        metrics = EndpointResult(
            base_url=candidate.base_url,
            mode=candidate.mode,
            full_url=candidate.full_url,
        )
        start_time = time.monotonic()
        connection_time: Optional[float] = None

        try:
            async with websockets.connect(
                candidate.full_url,
                open_timeout=10,
                ping_interval=15,
                ping_timeout=10,
                close_timeout=5,
                max_size=2_000_000,
            ) as websocket:
                metrics.connection_success = True
                connection_time = time.monotonic()
                self.logger_info(
                    "endpoint.connected",
                    base_url=candidate.base_url,
                    mode=candidate.mode,
                    full_url=candidate.full_url,
                    attempt=attempt,
                )

                while (
                    not self._shutdown.is_set()
                    and time.monotonic() - connection_time < self.duration_seconds
                ):
                    remaining = self.duration_seconds - (time.monotonic() - connection_time)
                    timeout = min(1.0, max(0.1, remaining))
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        continue
                    except ConnectionClosedOK:
                        self.logger.info(
                            "endpoint.disconnected_cleanly",
                            base_url=candidate.base_url,
                            mode=candidate.mode,
                            full_url=candidate.full_url,
                            attempt=attempt,
                        )
                        break
                    except ConnectionClosedError as error:
                        metrics.disconnect_count += 1
                        metrics.error_count += 1
                        metrics.raw_error_logs.append(str(error))
                        self.logger.warning(
                            "endpoint.disconnected",
                            error=str(error),
                            base_url=candidate.base_url,
                            mode=candidate.mode,
                            full_url=candidate.full_url,
                            attempt=attempt,
                        )
                        break
                    except Exception as error:
                        metrics.error_count += 1
                        metrics.raw_error_logs.append(str(error))
                        self.logger.warning(
                            "endpoint.receive_error",
                            error=str(error),
                            base_url=candidate.base_url,
                            mode=candidate.mode,
                            full_url=candidate.full_url,
                            attempt=attempt,
                        )
                        break

                    receive_time = time.monotonic()
                    raw_text = (
                        raw_message.decode("utf-8", errors="replace")
                        if isinstance(raw_message, (bytes, bytearray))
                        else str(raw_message)
                    )
                    self.logger_debug(
                        "endpoint.raw_frame",
                        base_url=candidate.base_url,
                        mode=candidate.mode,
                        full_url=candidate.full_url,
                        raw_message=raw_text,
                    )

                    metrics.messages_received += 1
                    if metrics.first_message_latency_ms is None and connection_time is not None:
                        metrics.first_message_latency_ms = (receive_time - connection_time) * 1000.0

                    payload = self._parse_message(raw_text, metrics)
                    if payload is None:
                        continue

                    message_latency = self._extract_payload_latency(payload)
                    self.logger_info(
                        "trade.received",
                        base_url=candidate.base_url,
                        mode=candidate.mode,
                        full_url=candidate.full_url,
                        symbol=payload.get("s"),
                        price=payload.get("p"),
                        quantity=payload.get("q"),
                        message_latency_ms=message_latency,
                    )

                    if self.debug:
                        self.logger_info(
                            "endpoint.parsed_message",
                            payload=payload,
                            message_latency_ms=message_latency,
                        )

                if websocket.closed and metrics.connection_success and not self._shutdown.is_set():
                    metrics.disconnect_count += 1
        except Exception as error:
            metrics.error_count += 1
            metrics.raw_error_logs.append(str(error))
            metrics.last_error = str(error)
            self.logger.warning(
                "endpoint.connect_error",
                error=str(error),
                base_url=candidate.base_url,
                mode=candidate.mode,
                full_url=candidate.full_url,
                attempt=attempt,
            )
        finally:
            metrics.total_test_seconds = time.monotonic() - start_time
            if metrics.total_test_seconds > 0:
                metrics.message_rate_per_second = metrics.messages_received / metrics.total_test_seconds
            return metrics

    def _parse_message(self, raw_text: str, metrics: EndpointResult) -> Optional[Dict[str, Any]]:
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as error:
            metrics.error_count += 1
            metrics.raw_error_logs.append(str(error))
            self.logger.error(
                "endpoint.json_parse_error",
                error=str(error),
                raw_message=raw_text,
            )
            return None

        if not isinstance(payload, dict):
            metrics.error_count += 1
            metrics.raw_error_logs.append("payload_not_object")
            self.logger.warning(
                "endpoint.invalid_payload",
                raw_message=raw_text,
                reason="payload_not_object",
            )
            return None

        return payload

    def _extract_payload_latency(self, payload: Dict[str, Any]) -> Optional[float]:
        if "T" in payload:
            try:
                trade_time = int(payload["T"])
                return max(0.0, float(int(time.time() * 1000) - trade_time))
            except Exception:
                return None
        return None

    def logger_info(self, event: str, **kwargs: Any) -> None:
        logger.info(event, **kwargs)

    def logger_debug(self, event: str, **kwargs: Any) -> None:
        if self.debug:
            logger.debug(event, **kwargs)

    def save_best_config(self, best_result: EndpointResult) -> None:
        payload = {
            "best_endpoint": best_result.full_url,
            "base_url": best_result.base_url,
            "mode": best_result.mode,
            "stream_name": self.stream_name,
            "score": best_result.score,
            "message_rate_per_second": best_result.message_rate_per_second,
            "first_message_latency_ms": best_result.first_message_latency_ms,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        RUNTIME_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(RUNTIME_CONFIG_PATH, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        self.logger_info("runtime.config_saved", path=str(RUNTIME_CONFIG_PATH), **payload)

    async def run(self) -> None:
        self.install_signal_handlers()
        candidates = self.build_candidates()
        self.logger_info("tester.start", candidate_count=len(candidates), duration_seconds=self.duration_seconds)
        results = await self.test_all()
        scored = [result for result in results if result.connection_success]
        scored.sort(key=lambda item: item.score, reverse=True)

        summary: Dict[str, Any] = {
            "tested_at": datetime.now(timezone.utc).isoformat(),
            "results": [result.to_dict() for result in results],
        }

        if scored:
            best = scored[0]
            self.save_best_config(best)
            summary["best_endpoint"] = best.full_url
            summary["best_mode"] = best.mode
            summary["best_score"] = best.score
            summary["best_message_rate_per_second"] = best.message_rate_per_second
            summary["best_first_message_latency_ms"] = best.first_message_latency_ms
        else:
            summary["best_endpoint"] = None
            summary["best_score"] = None

        print(json.dumps(summary, indent=2))


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance USD-M Futures websocket endpoint tester.")
    parser.add_argument("--duration", type=float, default=TEST_DURATION_SECONDS, help="Test duration in seconds for each endpoint.")
    parser.add_argument("--attempts", type=int, default=MAX_ATTEMPTS, help="Number of connection attempts per endpoint.")
    parser.add_argument("--diagnostic", action="store_true", help="Enable diagnostic logging with raw frames and parsed payloads.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logger("INFO")
    tester = BinanceEndpointTester(duration_seconds=args.duration, max_attempts=args.attempts, debug=args.diagnostic)
    try:
        asyncio.run(tester.run())
        return 0
    except KeyboardInterrupt:
        logger.info("tester.cancelled")
        return 1
    except Exception as error:
        logger.error("tester.failure", error=str(error))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
