#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m import PipelineConfig, run_pipeline
from polymarket_btc5m.timeframes import parse_timeframes_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket BTC up/down markets and trades with checkpoint support."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output",
        help="Output directory for csv/json files.",
    )
    parser.add_argument(
        "--include-zero-volume",
        action="store_true",
        help="Also request trades for markets whose volume is 0.",
    )
    parser.add_argument(
        "--market-limit",
        type=int,
        default=None,
        help="Only process first N filtered markets (for testing).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint and start from scratch.",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.10,
        help="Delay between requests to reduce rate limit pressure.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout per request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries on transient errors (429/5xx).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="DEBUG/INFO/WARNING/ERROR",
    )
    parser.add_argument(
        "--rpc-url",
        type=str,
        default=None,
        help=(
            "Optional Polygon RPC URL for millisecond timestamp enrichment. "
            "Accepts http(s)/ws(s); if omitted timestamp_ms is left empty."
        ),
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default="5m,15m,1h,4h",
        help=(
            "Comma-separated timeframes to include. "
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
    config = PipelineConfig(
        output_dir=args.output_dir,
        resume=not args.no_resume,
        include_zero_volume=args.include_zero_volume,
        market_limit=args.market_limit,
        request_delay_seconds=args.request_delay_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        log_level=args.log_level,
        rpc_url=args.rpc_url,
        timeframes=timeframes,
    )
    run_pipeline(config)


if __name__ == "__main__":
    main()
