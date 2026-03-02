#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import uvicorn

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.data_api import create_app  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Polymarket publish HTTP API.")
    parser.add_argument("--publish-dir", type=Path, required=True, help="Local publish directory root.")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Bind host.")
    parser.add_argument("--port", type=int, default=8080, help="Bind port.")
    parser.add_argument("--default-limit", type=int, default=200, help="Default row limit.")
    parser.add_argument("--max-limit", type=int, default=5000, help="Max row limit.")
    parser.add_argument(
        "--auth-mode",
        type=str,
        default=os.getenv("POLYMARKET_API_AUTH_MODE", "bearer"),
        help="API auth mode for /v1/* endpoints. Supported: bearer, none.",
    )
    parser.add_argument(
        "--bearer-token",
        type=str,
        default=os.getenv("POLYMARKET_API_BEARER_TOKEN", ""),
        help="Bearer token used when --auth-mode=bearer. Defaults to POLYMARKET_API_BEARER_TOKEN.",
    )
    parser.add_argument("--log-level", type=str, default="info", help="Uvicorn log level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app(
        publish_dir=args.publish_dir.expanduser().resolve(),
        default_limit=args.default_limit,
        max_limit=args.max_limit,
        bearer_token=args.bearer_token,
        auth_mode=args.auth_mode,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level.lower())


if __name__ == "__main__":
    main()
