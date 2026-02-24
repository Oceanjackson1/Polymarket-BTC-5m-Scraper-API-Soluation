#!/usr/bin/env python3
"""Real-time order book recorder for Polymarket BTC 5m markets."""
from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.recorder import OrderBookRecorder, RecorderConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real-time order book recorder for Polymarket BTC 5m markets. "
        "Continuously discovers active markets via Gamma API and records "
        "order book events via CLOB WebSocket to per-market CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output",
        help="Base output directory. Order book data goes to <dir>/orderbook/<slug>/",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30.0,
        help="Seconds between market discovery polls (default: 30).",
    )
    parser.add_argument(
        "--grace-period",
        type=float,
        default=120.0,
        help="Seconds to keep recording after market endDate (default: 120).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout for Gamma API requests (default: 30).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for Gamma API requests (default: 5).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = RecorderConfig(
        output_dir=args.output_dir,
        poll_interval_seconds=args.poll_interval,
        grace_period_seconds=args.grace_period,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        log_level=args.log_level,
    )
    recorder = OrderBookRecorder(config)

    def handle_sigterm(signum: int, frame: object) -> None:
        recorder.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    recorder.run()


if __name__ == "__main__":
    main()
