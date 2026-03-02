#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.cos_access import (  # noqa: E402
    CosAccessConfig,
    build_client,
    download_object_to_path,
    list_objects,
    read_object_bytes,
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
        description="Read Polymarket publish data from Tencent COS with Python."
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
    parser.add_argument("--prefix", type=str, default="", help="Prefix to scan for parquet/json objects.")
    parser.add_argument(
        "--key",
        action="append",
        default=[],
        help="Read one or more explicit object keys from COS.",
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
        "--print-keys",
        action="store_true",
        help="Print resolved object keys and exit.",
    )
    parser.add_argument("--max-files", type=int, default=20, help="Max parquet files to download for prefix mode.")
    parser.add_argument("--limit", type=int, default=5, help="Number of sample rows to print.")
    return parser.parse_args()


def require_non_empty(value: str, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise SystemExit(f"Missing required {label}.")
    return cleaned


def print_table_preview(table: Any, limit: int) -> None:
    print(f"rows={table.num_rows}")
    print(f"columns={table.column_names}")
    preview = table.slice(0, limit)
    print(preview.to_pydict())


def read_single_object(client: Any, bucket: str, key: str, limit: int) -> None:
    payload = read_object_bytes(client, bucket, key)
    if key.endswith(".json"):
        print(json.dumps(json.loads(payload.decode("utf-8")), indent=2))
        return
    if key.endswith(".parquet"):
        table = pq.read_table(pa.BufferReader(payload))
        print_table_preview(table, limit)
        return
    raise SystemExit(f"Unsupported object type for key: {key}")


def list_matching_parquet_keys(client: Any, bucket: str, prefix: str, max_files: int) -> list[str]:
    keys: list[str] = []
    for item in list_objects(client, bucket, prefix):
        key = str(item.get("Key") or "")
        if key.endswith(".parquet"):
            keys.append(key)
        if len(keys) >= max_files:
            break
    return keys


def download_prefix_to_tempdir(client: Any, bucket: str, keys: list[str]) -> Path:
    root = Path(tempfile.mkdtemp(prefix="cos-publish-"))
    for key in keys:
        target = root / key
        download_object_to_path(client, bucket, key, target)
    return root


def read_dataset_from_keys(client: Any, bucket: str, keys: list[str], limit: int) -> None:
    tempdir = download_prefix_to_tempdir(client, bucket, keys)
    try:
        dataset = ds.dataset(tempdir, format="parquet", partitioning="hive")
        table = dataset.to_table()
        print(f"downloaded_files={len(keys)}")
        print_table_preview(table, limit)
    finally:
        shutil.rmtree(tempdir, ignore_errors=True)


def read_prefix_dataset(client: Any, bucket: str, prefix: str, max_files: int, limit: int) -> None:
    keys = list_matching_parquet_keys(client, bucket, prefix, max_files)
    if not keys:
        raise SystemExit(f"No parquet files found under prefix: {prefix}")
    read_dataset_from_keys(client, bucket, keys, limit)


def resolve_keyset_keys(client: Any, bucket: str, keyset_key: str, group: str) -> list[str]:
    payload = read_object_bytes(client, bucket, keyset_key)
    manifest = load_keyset_manifest(payload)
    normalized_group = normalize_keyset_group(group)
    keys = keyset_group_keys(manifest, normalized_group)
    if not keys:
        raise SystemExit(
            f"No keys found in keyset manifest {keyset_key} for group {normalized_group}."
        )
    return keys


def main() -> None:
    args = parse_args()
    bucket = require_non_empty(args.bucket, "bucket")
    region = require_non_empty(args.region, "region")
    secret_id = require_non_empty(args.secret_id, "secret id")
    secret_key = require_non_empty(args.secret_key, "secret key")
    if not args.key and not args.prefix and not args.keyset_key:
        raise SystemExit("Provide one of --key, --prefix, or --keyset-key.")

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
        normalized_keyset_group = normalize_keyset_group(args.keyset_group)
        keys = resolve_keyset_keys(client, bucket, args.keyset_key, normalized_keyset_group)
        if args.print_keys:
            for key in keys:
                print(key)
            return
    else:
        normalized_keyset_group = ""

    if keys:
        if len(keys) == 1:
            read_single_object(client, bucket, keys[0], args.limit)
            return
        if any(not key.endswith(".parquet") for key in keys):
            raise SystemExit(
                "Multiple-key mode supports parquet objects only. Use --print-keys for raw/json groups."
            )
        if args.keyset_key and normalized_keyset_group not in PARQUET_KEYSET_GROUPS:
            raise SystemExit(
                "The selected keyset group is not parquet-backed. Use --print-keys to inspect object keys."
            )
        read_dataset_from_keys(client, bucket, keys, args.limit)
    else:
        read_prefix_dataset(client, bucket, args.prefix, args.max_files, args.limit)


if __name__ == "__main__":
    main()
