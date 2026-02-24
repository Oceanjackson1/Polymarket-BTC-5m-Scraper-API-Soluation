#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.client import PolymarketApiClient
from polymarket_btc5m.pipeline import (
    TRADES_HEADERS,
    TradeTimestampEnricher,
    _append_csv,
    _fetch_all_trades_for_market,
    _normalize_market_record,
    _write_csv_header,
    configure_logging,
)
from polymarket_btc5m.timeframes import parse_timeframes_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch trades for one Polymarket BTC-updown market slug."
    )
    parser.add_argument(
        "--slug",
        required=True,
        type=str,
        help="Market slug, e.g. btc-updown-5m-1771211700",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "output" / "trades",
        help="Output directory for single-market CSV.",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.10,
        help="Delay between paginated trade requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for HTTP/RPC calls.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )
    parser.add_argument(
        "--rpc-url",
        type=str,
        default=None,
        help=(
            "Optional Polygon RPC URL for millisecond timestamp enrichment. "
            "If omitted timestamp_ms is left empty."
        ),
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default="5m,15m,1h,4h",
        help=(
            "Comma-separated timeframes to allow for slug validation. "
            "Supported: 5m,15m,1h,4h (aliases: hourly->1h,4hour->4h)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    try:
        enabled_timeframes = parse_timeframes_csv(args.timeframes)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    client = PolymarketApiClient(
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )

    response = client.get_gamma("/markets", {"slug": args.slug})
    if not isinstance(response, list) or not response:
        raise RuntimeError(f"Market not found for slug={args.slug}")

    market = _normalize_market_record(response[0], enabled_timeframes=enabled_timeframes)
    if market is None:
        raise RuntimeError(
            f"Market is not a valid BTC-updown market in timeframes={enabled_timeframes}: "
            f"slug={args.slug}"
        )

    enricher = None
    if args.rpc_url:
        enricher = TradeTimestampEnricher(
            rpc_url=args.rpc_url,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
        )
        logging.info("Timestamp enrichment enabled via rpc_url=%s", args.rpc_url)

    trades = _fetch_all_trades_for_market(
        client=client,
        market=market,
        request_delay_seconds=args.request_delay_seconds,
        receipt_enricher=enricher,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / f"trades_{args.slug}.csv"
    if not output_path.exists() or output_path.stat().st_size == 0:
        _write_csv_header(output_path, TRADES_HEADERS)
    _append_csv(output_path, trades, TRADES_HEADERS)

    logging.info(
        "Single market done: slug=%s trades=%s file=%s",
        args.slug,
        len(trades),
        output_path,
    )


if __name__ == "__main__":
    main()
