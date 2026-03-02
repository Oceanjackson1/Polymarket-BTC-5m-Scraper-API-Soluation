#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.cos_access import (  # noqa: E402
    CosAccessConfig,
    build_client,
    build_object_url,
    list_objects,
    normalize_prefix,
    read_object_bytes,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the Polymarket publish tree stored in Tencent COS."
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
    parser.add_argument(
        "--prefix",
        type=str,
        default=os.getenv("TENCENT_COS_PREFIX", ""),
        help="Optional base prefix inside the bucket.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Number of sample keys to print per prefix.",
    )
    return parser.parse_args()


def require_non_empty(value: str, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise SystemExit(f"Missing required {label}.")
    return cleaned


def join_prefix(base_prefix: str, suffix: str) -> str:
    base = normalize_prefix(base_prefix)
    tail = normalize_prefix(suffix)
    return f"{base}{tail}"


def inspect_prefix(client: Any, bucket: str, prefix: str, sample_limit: int) -> dict[str, Any]:
    total = 0
    samples: list[dict[str, Any]] = []
    for item in list_objects(client, bucket, prefix):
        total += 1
        if len(samples) < sample_limit:
            samples.append(
                {
                    "key": item.get("Key", ""),
                    "size": item.get("Size", ""),
                    "last_modified": item.get("LastModified", ""),
                }
            )
    return {"count": total, "samples": samples}


def find_first_key(client: Any, bucket: str, prefix: str, suffix: str) -> str | None:
    for item in list_objects(client, bucket, prefix):
        key = str(item.get("Key") or "")
        if key.endswith(suffix):
            return key
    return None


def verify_manifest(client: Any, bucket: str, key: str, region: str) -> dict[str, Any]:
    content = json.loads(read_object_bytes(client, bucket, key).decode("utf-8"))
    return {
        "key": key,
        "url": build_object_url(bucket, region, key),
        "market_slug": content.get("market_slug"),
        "timeframe": content.get("timeframe"),
        "datasets": sorted((content.get("datasets") or {}).keys()),
    }


def verify_parquet(client: Any, bucket: str, key: str, region: str) -> dict[str, Any]:
    table = pq.read_table(pa.BufferReader(read_object_bytes(client, bucket, key)))
    return {
        "key": key,
        "url": build_object_url(bucket, region, key),
        "rows": table.num_rows,
        "columns": table.column_names[:12],
    }


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

    sections = {
        "raw": join_prefix(args.prefix, "raw"),
        "curated": join_prefix(args.prefix, "curated"),
        "meta": join_prefix(args.prefix, "meta"),
    }
    report = {
        name: inspect_prefix(client, bucket, prefix, args.sample_limit)
        for name, prefix in sections.items()
    }

    manifest_key = find_first_key(client, bucket, sections["raw"], "manifest.json")
    parquet_key = find_first_key(client, bucket, sections["curated"], ".parquet")
    meta_key = find_first_key(client, bucket, sections["meta"], ".parquet")

    print(json.dumps(report, indent=2, ensure_ascii=True))

    if manifest_key:
        print("\nManifest sample:")
        print(json.dumps(verify_manifest(client, bucket, manifest_key, region), indent=2))
    if parquet_key:
        print("\nCurated parquet sample:")
        print(json.dumps(verify_parquet(client, bucket, parquet_key, region), indent=2))
    if meta_key:
        print("\nMeta parquet sample:")
        print(json.dumps(verify_parquet(client, bucket, meta_key, region), indent=2))


if __name__ == "__main__":
    main()
