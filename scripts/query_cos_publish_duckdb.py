#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.cos_access import (  # noqa: E402
    CosAccessConfig,
    build_client,
    download_object_to_path,
    list_objects,
)
from polymarket_btc5m.keysets import (  # noqa: E402
    KEYSET_GROUPS,
    PARQUET_KEYSET_GROUPS,
    keyset_group_keys,
    load_keyset_manifest,
    normalize_keyset_group,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download parquet data from Tencent COS and query it with DuckDB."
    )
    parser.add_argument("--bucket", type=str, default=os.getenv("TENCENT_COS_BUCKET", ""))
    parser.add_argument("--region", type=str, default=os.getenv("TENCENT_COS_REGION", ""))
    parser.add_argument(
        "--secret-id",
        type=str,
        default=os.getenv("TENCENTCLOUD_SECRET_ID", os.getenv("TENCENT_COS_SECRET_ID", "")),
    )
    parser.add_argument(
        "--secret-key",
        type=str,
        default=os.getenv("TENCENTCLOUD_SECRET_KEY", os.getenv("TENCENT_COS_SECRET_KEY", "")),
    )
    parser.add_argument("--prefix", type=str, default="", help="Prefix to download parquet files from.")
    parser.add_argument("--max-files", type=int, default=50, help="Max parquet files to download.")
    parser.add_argument(
        "--key",
        action="append",
        default=[],
        help="One or more explicit parquet object keys in COS.",
    )
    parser.add_argument(
        "--keyset-key",
        type=str,
        default="",
        help="Explicit COS key for a keyset manifest JSON under meta/keysets/.",
    )
    parser.add_argument(
        "--keyset-group",
        type=str,
        default="meta_markets",
        help=f"Key group inside the keyset manifest. Supported: {', '.join(sorted(KEYSET_GROUPS))}",
    )
    parser.add_argument(
        "--sql",
        type=str,
        default="select * from dataset limit 10",
        help="DuckDB SQL to execute against the downloaded parquet files.",
    )
    return parser.parse_args()


def require_non_empty(value: str, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise SystemExit(f"Missing required {label}.")
    return cleaned


def parquet_keys(client: Any, bucket: str, prefix: str, max_files: int) -> list[str]:
    keys: list[str] = []
    for item in list_objects(client, bucket, prefix):
        key = str(item.get("Key") or "")
        if key.endswith(".parquet"):
            keys.append(key)
        if len(keys) >= max_files:
            break
    return keys


def print_result(result: duckdb.DuckDBPyConnection) -> None:
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    print("\t".join(columns))
    for row in rows:
        print("\t".join("" if value is None else str(value) for value in row))


def resolve_keyset_keys(client: Any, bucket: str, keyset_key: str, group: str) -> list[str]:
    payload = client.get_object(Bucket=bucket, Key=keyset_key)["Body"].get_raw_stream().read()
    manifest = load_keyset_manifest(payload)
    normalized_group = normalize_keyset_group(group)
    keys = keyset_group_keys(manifest, normalized_group)
    if not keys:
        raise SystemExit(
            f"No keys found in keyset manifest {keyset_key} for group {normalized_group}."
        )
    if normalized_group not in PARQUET_KEYSET_GROUPS:
        raise SystemExit(
            f"Keyset group {normalized_group} is not parquet-backed. Supported parquet groups: "
            f"{', '.join(sorted(PARQUET_KEYSET_GROUPS))}"
        )
    return keys


def main() -> None:
    args = parse_args()
    bucket = require_non_empty(args.bucket, "bucket")
    region = require_non_empty(args.region, "region")
    secret_id = require_non_empty(args.secret_id, "secret id")
    secret_key = require_non_empty(args.secret_key, "secret key")

    client = build_client(
        CosAccessConfig(
            bucket=bucket,
            region=region,
            secret_id=secret_id,
            secret_key=secret_key,
        )
    )

    keys = list(args.key)
    if args.keyset_key:
        keys = resolve_keyset_keys(client, bucket, args.keyset_key, args.keyset_group)
    if not keys:
        if not args.prefix:
            raise SystemExit("Provide one of --prefix, --key, or --keyset-key.")
        keys = parquet_keys(client, bucket, args.prefix, args.max_files)
    if not keys:
        raise SystemExit(f"No parquet files found under prefix: {args.prefix}")

    tempdir = Path(tempfile.mkdtemp(prefix="cos-duckdb-"))
    try:
        for key in keys:
            download_object_to_path(client, bucket, key, tempdir / key)

        db = duckdb.connect()
        parquet_glob = str((tempdir / "**" / "*.parquet").as_posix()).replace("'", "''")
        db.execute(
            f"create or replace view dataset as "
            f"select * from read_parquet('{parquet_glob}', hive_partitioning=true)"
        )
        print(f"downloaded_files={len(keys)}")
        print_result(db.execute(args.sql))
    finally:
        shutil.rmtree(tempdir, ignore_errors=True)


if __name__ == "__main__":
    main()
