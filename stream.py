#!/usr/bin/env python3
from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.trade_streamer import TradeStreamConfig, TradeStreamService
from polymarket_btc5m.timeframes import parse_timeframes_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Real-time Polymarket BTC up/down trade stream via Polygon newHeads + OrderFilled logs."
        )
    )
    parser.add_argument(
        "--rpc-url",
        required=True,
        type=str,
        help="Polygon WebSocket RPC URL, e.g. wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output" / "trades",
        help="Directory for trades CSV output (default: output/trades).",
    )
    parser.add_argument(
        "--market-poll-interval",
        type=float,
        default=300.0,
        help="Seconds between Gamma market discovery polls (default: 300).",
    )
    parser.add_argument(
        "--ws-host",
        type=str,
        default="127.0.0.1",
        help="Local WS broadcast host (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--ws-port",
        type=int,
        default=8765,
        help="Local WS broadcast port (default: 8765).",
    )
    parser.add_argument(
        "--grace-period",
        type=float,
        default=120.0,
        help="Seconds to keep a market active after endDate (default: 120).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP RPC timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for HTTP RPC/Gamma calls (default: 5).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO).",
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default="5m,15m,1h,4h",
        help=(
            "Comma-separated timeframes to stream. "
            "Supported: 5m,15m,1h,4h (aliases: hourly->1h,4hour->4h)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        timeframes = parse_timeframes_csv(args.timeframes)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    config = TradeStreamConfig(
        rpc_url=args.rpc_url,
        output_dir=args.output_dir,
        timeframes=timeframes,
        market_poll_interval_seconds=args.market_poll_interval,
        grace_period_seconds=args.grace_period,
        local_ws_host=args.ws_host,
        local_ws_port=args.ws_port,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        log_level=args.log_level,
    )
    service = TradeStreamService(config)

    def handle_sigterm(signum: int, frame: object) -> None:
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    service.run()


if __name__ == "__main__":
    main()
