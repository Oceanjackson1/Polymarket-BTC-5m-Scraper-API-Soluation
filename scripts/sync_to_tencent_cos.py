#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

try:
    from qcloud_cos import CosConfig, CosS3Client
except ImportError as exc:
    raise SystemExit(
        "Missing dependency qcloud_cos. Install with: pip install -r requirements-deploy.txt"
    ) from exc


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync closed Polymarket order book CSV files from local disk to Tencent COS."
        )
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        required=True,
        help="Local directory to scan, e.g. /data/polymarket-agent/output/orderbook",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.getenv("TENCENT_COS_BUCKET", ""),
        help="COS bucket name, e.g. polymarket-ai-agent-1328550492",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("TENCENT_COS_REGION", ""),
        help="COS region, e.g. ap-tokyo",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=os.getenv("TENCENT_COS_PREFIX", "orderbook"),
        help="Remote object prefix (default: orderbook).",
    )
    parser.add_argument(
        "--secret-id",
        type=str,
        default=os.getenv("TENCENTCLOUD_SECRET_ID", os.getenv("TENCENT_COS_SECRET_ID", "")),
        help="Tencent Cloud secret id. Defaults to env vars.",
    )
    parser.add_argument(
        "--secret-key",
        type=str,
        default=os.getenv("TENCENTCLOUD_SECRET_KEY", os.getenv("TENCENT_COS_SECRET_KEY", "")),
        help="Tencent Cloud secret key. Defaults to env vars.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=60.0,
        help="Seconds between sync passes when running as a daemon (default: 60).",
    )
    parser.add_argument(
        "--stable-seconds",
        type=float,
        default=180.0,
        help=(
            "Only upload files whose mtime is older than this threshold. "
            "This avoids re-uploading still-active market files."
        ),
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="JSON file used to track successfully uploaded versions.",
    )
    parser.add_argument(
        "--scheme",
        type=str,
        default="https",
        choices=["http", "https"],
        help="COS API scheme (default: https).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sync pass and exit.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def require_non_empty(value: str, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise SystemExit(f"Missing required {label}.")
    return cleaned


def default_state_file(source_dir: Path) -> Path:
    return source_dir.parent / ".cos_sync_state.json"


def load_state(path: Path) -> dict[str, dict[str, int]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        logger.warning("State file unreadable, starting fresh: %s", path, exc_info=True)
        return {}
    if not isinstance(data, dict):
        return {}
    normalized: dict[str, dict[str, int]] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        size = value.get("size")
        mtime_ns = value.get("mtime_ns")
        if isinstance(size, int) and isinstance(mtime_ns, int):
            normalized[key] = {"size": size, "mtime_ns": mtime_ns}
    return normalized


def save_state(path: Path, state: dict[str, dict[str, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
    temp_path.replace(path)


def iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and not path.name.endswith(".tmp")
    )


def build_object_key(prefix: str, relative_path: Path) -> str:
    clean_prefix = prefix.strip("/")
    rel = relative_path.as_posix()
    return rel if not clean_prefix else f"{clean_prefix}/{rel}"


def file_version(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}


def is_stable(path: Path, stable_seconds: float, now: float) -> bool:
    return now - path.stat().st_mtime >= stable_seconds


def upload_file(
    client: CosS3Client,
    bucket: str,
    object_key: str,
    path: Path,
) -> None:
    with path.open("rb") as fh:
        client.put_object(
            Bucket=bucket,
            Body=fh,
            Key=object_key,
        )


def sync_once(
    client: CosS3Client,
    bucket: str,
    source_dir: Path,
    prefix: str,
    stable_seconds: float,
    state: dict[str, dict[str, int]],
) -> tuple[int, int, int]:
    now = time.time()
    uploaded = 0
    skipped_active = 0
    already_synced = 0

    for path in iter_files(source_dir):
        relative_path = path.relative_to(source_dir)
        object_key = build_object_key(prefix, relative_path)
        version = file_version(path)
        if not is_stable(path, stable_seconds, now):
            skipped_active += 1
            continue
        if state.get(object_key) == version:
            already_synced += 1
            continue

        logger.info("Uploading %s -> cos://%s/%s", path, bucket, object_key)
        upload_file(client, bucket, object_key, path)
        state[object_key] = version
        uploaded += 1

    return uploaded, skipped_active, already_synced


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    bucket = require_non_empty(args.bucket, "bucket")
    region = require_non_empty(args.region, "region")
    secret_id = require_non_empty(args.secret_id, "secret id")
    secret_key = require_non_empty(args.secret_key, "secret key")
    source_dir = args.source_dir.expanduser().resolve()
    state_file = (args.state_file or default_state_file(source_dir)).expanduser().resolve()

    config = CosConfig(
        Region=region,
        SecretId=secret_id,
        SecretKey=secret_key,
        Scheme=args.scheme,
    )
    client = CosS3Client(config)
    state = load_state(state_file)

    logger.info(
        "COS sync starting. source_dir=%s bucket=%s region=%s prefix=%s",
        source_dir,
        bucket,
        region,
        args.prefix,
    )

    while True:
        try:
            uploaded, skipped_active, already_synced = sync_once(
                client=client,
                bucket=bucket,
                source_dir=source_dir,
                prefix=args.prefix,
                stable_seconds=args.stable_seconds,
                state=state,
            )
            save_state(state_file, state)
            logger.info(
                "COS sync pass complete. uploaded=%d skipped_active=%d already_synced=%d",
                uploaded,
                skipped_active,
                already_synced,
            )
        except KeyboardInterrupt:
            logger.info("Stopping COS sync daemon")
            break
        except Exception:
            logger.exception("COS sync pass failed")

        if args.once:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
