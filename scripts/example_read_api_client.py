#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.read_api_client import (  # noqa: E402
    PolymarketReadApiClient,
    ReadApiConfig,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Example business-side client for the Polymarket read-only API.")
    parser.add_argument(
        "--base-url",
        type=str,
        default=os.getenv("POLYMARKET_READ_API_URL", "https://10.193.48.144"),
        help="Read-only API base URL.",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.getenv("POLYMARKET_READ_API_TOKEN", ""),
        help="Optional bearer token for /v1/* endpoints.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification. Useful with the default self-signed cert.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="Check the /health endpoint.")
    subparsers.add_parser("index", help="Fetch /v1/keysets/index.")

    keyset_parser = subparsers.add_parser("keyset", help="Fetch one /v1/keysets/{dt}/{timeframe} manifest.")
    keyset_parser.add_argument("--dt", required=True, help="UTC date partition, e.g. 2026-03-02")
    keyset_parser.add_argument("--timeframe", required=True, help="Timeframe partition, e.g. 5m or 15m")

    meta_parser = subparsers.add_parser("meta", help="Fetch a /v1/meta/* dataset.")
    meta_parser.add_argument("--dataset", required=True, choices=["markets", "files"])
    add_dataset_args(meta_parser)

    curated_parser = subparsers.add_parser("curated", help="Fetch a /v1/curated/* dataset.")
    curated_parser.add_argument("--dataset", required=True, choices=["book_snapshots", "price_changes", "trades"])
    add_dataset_args(curated_parser)

    return parser.parse_args()


def add_dataset_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dt", required=True, help="UTC date partition, e.g. 2026-03-02")
    parser.add_argument("--timeframe", required=True, help="Timeframe partition, e.g. 5m or 15m")
    parser.add_argument("--market-slug", default=None, help="Optional market slug filter.")
    parser.add_argument("--columns", nargs="*", default=None, help="Optional list of columns to return.")
    parser.add_argument("--offset", type=int, default=0, help="Row offset.")
    parser.add_argument("--limit", type=int, default=10, help="Max rows to return.")


def main() -> None:
    args = parse_args()

    with PolymarketReadApiClient(
        ReadApiConfig(
            base_url=args.base_url,
            bearer_token=args.token or "",
            verify_tls=not args.insecure,
        )
    ) as client:
        if args.command == "health":
            payload = client.health()
        elif args.command == "index":
            payload = client.keysets_index()
        elif args.command == "keyset":
            payload = client.keyset_manifest(args.dt, args.timeframe)
        elif args.command == "meta":
            payload = client.meta(
                args.dataset,
                dt=args.dt,
                timeframe=args.timeframe,
                market_slug=args.market_slug,
                columns=args.columns,
                offset=args.offset,
                limit=args.limit,
            )
        elif args.command == "curated":
            payload = client.curated(
                args.dataset,
                dt=args.dt,
                timeframe=args.timeframe,
                market_slug=args.market_slug,
                columns=args.columns,
                offset=args.offset,
                limit=args.limit,
            )
        else:
            raise SystemExit(f"Unsupported command: {args.command}")

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
