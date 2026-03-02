#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit(
        "Missing dependency pyarrow. Install with: pip install -r requirements-deploy.txt"
    ) from exc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
import sys

sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.timeframes import parse_market_slug

logger = logging.getLogger(__name__)

EVENT_FILE_PREFIXES = ("book_snapshots", "price_changes", "trades", "recovery_events")
REQUIRED_EVENT_FILE_PREFIXES = ("book_snapshots", "price_changes", "trades")
MATERIALIZER_SCHEMA_VERSION = 3
EVENT_COLUMN_TYPES = {
    "book_snapshots": {
        "snapshot_seq": pa.int64(),
        "server_ts_ms": pa.int64(),
        "receive_ts_ms": pa.int64(),
        "receive_utc": pa.string(),
        "asset_id": pa.string(),
        "outcome": pa.string(),
        "condition_id": pa.string(),
        "side": pa.string(),
        "price": pa.float64(),
        "size": pa.float64(),
        "level_index": pa.int32(),
    },
    "price_changes": {
        "server_ts_ms": pa.int64(),
        "receive_ts_ms": pa.int64(),
        "receive_utc": pa.string(),
        "condition_id": pa.string(),
        "asset_id": pa.string(),
        "outcome": pa.string(),
        "side": pa.string(),
        "price": pa.float64(),
        "size": pa.float64(),
        "best_bid": pa.float64(),
        "best_ask": pa.float64(),
    },
    "trades": {
        "server_ts_ms": pa.int64(),
        "receive_ts_ms": pa.int64(),
        "receive_utc": pa.string(),
        "condition_id": pa.string(),
        "asset_id": pa.string(),
        "outcome": pa.string(),
        "side": pa.string(),
        "price": pa.float64(),
        "size": pa.float64(),
        "fee_rate_bps": pa.float64(),
    },
    "recovery_events": {
        "event_seq": pa.int64(),
        "event_ts_ms": pa.int64(),
        "event_utc": pa.string(),
        "gap_id": pa.string(),
        "gap_started_ms": pa.int64(),
        "gap_started_utc": pa.string(),
        "condition_id": pa.string(),
        "asset_id": pa.string(),
        "outcome": pa.string(),
        "recovery_event": pa.string(),
        "reason": pa.string(),
        "book_server_ts_ms": pa.int64(),
        "bid_levels": pa.int32(),
        "ask_levels": pa.int32(),
        "error": pa.string(),
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize stable per-market order book CSVs into publish-ready raw and curated layouts."
        )
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        required=True,
        help="Recorder output root, e.g. /data/polymarket-agent/output/orderbook",
    )
    parser.add_argument(
        "--publish-dir",
        type=Path,
        required=True,
        help="Destination root for raw/curated/meta outputs.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=60.0,
        help="Seconds between scan passes when running as a daemon (default: 60).",
    )
    parser.add_argument(
        "--stable-seconds",
        type=float,
        default=300.0,
        help="Only process market files older than this threshold (default: 300).",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="JSON state file used to skip unchanged markets.",
    )
    parser.add_argument(
        "--link-mode",
        type=str,
        default="hardlink",
        choices=["hardlink", "copy"],
        help="How to materialize raw files locally (default: hardlink).",
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
        help="Run a single pass and exit.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def default_state_file(publish_dir: Path) -> Path:
    return publish_dir / "state" / "materialize_state.json"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"markets": {}}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        logger.warning("State file unreadable, starting fresh: %s", path, exc_info=True)
        return {"markets": {}}
    if not isinstance(data, dict):
        return {"markets": {}}
    markets = data.get("markets")
    if not isinstance(markets, dict):
        return {"markets": {}}
    return {"markets": markets}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, state)


def iso_utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def unix_to_utc(ts: int) -> str:
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def date_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    temp_path.replace(path)


def list_market_dirs(source_dir: Path) -> list[Path]:
    if not source_dir.exists():
        return []
    return sorted(path for path in source_dir.iterdir() if path.is_dir())


def source_files_for_market(market_dir: Path) -> dict[str, Path]:
    slug = market_dir.name
    return {
        prefix: market_dir / f"{prefix}_{slug}.csv"
        for prefix in EVENT_FILE_PREFIXES
    }


def files_signature(files: dict[str, Path]) -> dict[str, dict[str, int]]:
    signature: dict[str, dict[str, int]] = {}
    for event_type, path in files.items():
        if not path.exists():
            signature[event_type] = {
                "size": -1,
                "mtime_ns": -1,
            }
            continue
        stat = path.stat()
        signature[event_type] = {
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return signature


def files_are_stable(files: dict[str, Path], stable_seconds: float, now: float) -> bool:
    for event_type in REQUIRED_EVENT_FILE_PREFIXES:
        path = files[event_type]
        if not path.exists():
            return False
        if now - path.stat().st_mtime < stable_seconds:
            return False
    return True


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def replace_with_link_or_copy(src: Path, dst: Path, mode: str) -> None:
    ensure_parent(dst)
    temp_path = dst.with_suffix(dst.suffix + ".tmp")
    if temp_path.exists():
        temp_path.unlink()
    if mode == "hardlink":
        try:
            os.link(src, temp_path)
        except OSError:
            shutil.copy2(src, temp_path)
    else:
        shutil.copy2(src, temp_path)
    temp_path.replace(dst)


def build_constant_array(value: Any, length: int) -> pa.Array:
    return pa.array([value] * length)


def csv_convert_options(event_type: str) -> pacsv.ConvertOptions:
    column_types = EVENT_COLUMN_TYPES.get(event_type)
    if column_types is None:
        raise ValueError(f"Unsupported event type for schema conversion: {event_type}")
    return pacsv.ConvertOptions(column_types=column_types)


def csv_headers(event_type: str) -> list[str]:
    column_types = EVENT_COLUMN_TYPES.get(event_type)
    if column_types is None:
        raise ValueError(f"Unsupported event type for headers: {event_type}")
    return list(column_types.keys())


def write_empty_csv(path: Path, event_type: str) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_headers(event_type))
        writer.writeheader()


def empty_event_table(event_type: str) -> pa.Table:
    column_types = EVENT_COLUMN_TYPES.get(event_type)
    if column_types is None:
        raise ValueError(f"Unsupported event type for empty table: {event_type}")
    arrays = [
        pa.array([], type=column_type)
        for column_type in column_types.values()
    ]
    return pa.Table.from_arrays(arrays, names=list(column_types.keys()))


def scalar_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        return value.as_py()
    return value


def min_max_as_dict(table: pa.Table, column_name: str) -> dict[str, Any]:
    if column_name not in table.column_names or table.num_rows == 0:
        return {"min": None, "max": None}
    column = table[column_name]
    return {
        "min": scalar_value(pc.min(column)),
        "max": scalar_value(pc.max(column)),
    }


def enrich_table(
    table: pa.Table,
    *,
    slug: str,
    timeframe: str,
    window_start_ts: int,
    dt: str,
    event_type: str,
) -> pa.Table:
    row_count = table.num_rows
    return (
        table.append_column("market_slug", build_constant_array(slug, row_count))
        .append_column("timeframe", build_constant_array(timeframe, row_count))
        .append_column("window_start_ts", build_constant_array(window_start_ts, row_count))
        .append_column("window_start_utc", build_constant_array(unix_to_utc(window_start_ts), row_count))
        .append_column("market_dt", build_constant_array(dt, row_count))
        .append_column("event_type", build_constant_array(event_type, row_count))
    )


def read_and_write_parquet(
    source_path: Path,
    target_path: Path,
    *,
    slug: str,
    timeframe: str,
    window_start_ts: int,
    dt: str,
    event_type: str,
) -> dict[str, Any]:
    ensure_parent(target_path)
    table = pacsv.read_csv(source_path, convert_options=csv_convert_options(event_type))
    enriched = enrich_table(
        table,
        slug=slug,
        timeframe=timeframe,
        window_start_ts=window_start_ts,
        dt=dt,
        event_type=event_type,
    )
    pq.write_table(enriched, target_path, compression="zstd")
    server_ts_column = "book_server_ts_ms" if event_type == "recovery_events" else "server_ts_ms"
    receive_ts_column = "event_ts_ms" if event_type == "recovery_events" else "receive_ts_ms"
    server_ts = min_max_as_dict(enriched, server_ts_column)
    receive_ts = min_max_as_dict(enriched, receive_ts_column)
    return {
        "row_count": enriched.num_rows,
        "server_ts_ms_min": server_ts["min"],
        "server_ts_ms_max": server_ts["max"],
        "receive_ts_ms_min": receive_ts["min"],
        "receive_ts_ms_max": receive_ts["max"],
        "parquet_path": str(target_path),
    }


def write_empty_parquet(
    target_path: Path,
    *,
    slug: str,
    timeframe: str,
    window_start_ts: int,
    dt: str,
    event_type: str,
) -> dict[str, Any]:
    ensure_parent(target_path)
    table = empty_event_table(event_type)
    enriched = enrich_table(
        table,
        slug=slug,
        timeframe=timeframe,
        window_start_ts=window_start_ts,
        dt=dt,
        event_type=event_type,
    )
    pq.write_table(enriched, target_path, compression="zstd")
    return {
        "row_count": 0,
        "server_ts_ms_min": None,
        "server_ts_ms_max": None,
        "receive_ts_ms_min": None,
        "receive_ts_ms_max": None,
        "parquet_path": str(target_path),
    }


def raw_market_dir(publish_dir: Path, dt: str, timeframe: str, slug: str) -> Path:
    return (
        publish_dir
        / "raw"
        / "orderbook"
        / f"dt={dt}"
        / f"timeframe={timeframe}"
        / f"market_slug={slug}"
    )


def curated_event_path(publish_dir: Path, dt: str, timeframe: str, slug: str, event_type: str) -> Path:
    return (
        publish_dir
        / "curated"
        / event_type
        / f"dt={dt}"
        / f"timeframe={timeframe}"
        / f"{slug}.parquet"
    )


def meta_market_path(publish_dir: Path, dt: str, timeframe: str, slug: str) -> Path:
    return (
        publish_dir
        / "meta"
        / "markets"
        / f"dt={dt}"
        / f"timeframe={timeframe}"
        / f"{slug}.parquet"
    )


def meta_files_path(publish_dir: Path, dt: str, timeframe: str, slug: str) -> Path:
    return (
        publish_dir
        / "meta"
        / "files"
        / f"dt={dt}"
        / f"timeframe={timeframe}"
        / f"{slug}.parquet"
    )


def keyset_manifest_path(publish_dir: Path, dt: str, timeframe: str) -> Path:
    return (
        publish_dir
        / "meta"
        / "keysets"
        / f"dt={dt}"
        / f"timeframe={timeframe}"
        / "manifest.json"
    )


def keyset_index_path(publish_dir: Path) -> Path:
    return publish_dir / "meta" / "keysets" / "index.json"


def publish_relative_key(publish_dir: Path, path: Path) -> str:
    return path.relative_to(publish_dir).as_posix()


def write_single_row_parquet(path: Path, row: dict[str, Any]) -> None:
    ensure_parent(path)
    table = pa.Table.from_pylist([row])
    pq.write_table(table, path, compression="zstd")


def write_file_catalog(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")


def materialize_market(
    market_dir: Path,
    publish_dir: Path,
    link_mode: str,
) -> dict[str, Any]:
    slug = market_dir.name
    parsed = parse_market_slug(slug)
    if parsed is None:
        raise ValueError(f"Unsupported market directory slug: {slug}")
    timeframe, window_start_ts = parsed
    dt = date_from_ts(window_start_ts)
    files = source_files_for_market(market_dir)
    signature = files_signature(files)
    generated_at = iso_utc_now()

    raw_dir = raw_market_dir(publish_dir, dt, timeframe, slug)
    curated_metrics: dict[str, dict[str, Any]] = {}
    file_rows: list[dict[str, Any]] = []

    for event_type, source_path in files.items():
        raw_path = raw_dir / source_path.name
        curated_path = curated_event_path(publish_dir, dt, timeframe, slug, event_type)
        if source_path.exists():
            replace_with_link_or_copy(source_path, raw_path, link_mode)
            metrics = read_and_write_parquet(
                source_path,
                curated_path,
                slug=slug,
                timeframe=timeframe,
                window_start_ts=window_start_ts,
                dt=dt,
                event_type=event_type,
            )
            source_stat = source_path.stat()
            source_size_bytes = int(source_stat.st_size)
            source_mtime_ns = int(source_stat.st_mtime_ns)
        else:
            write_empty_csv(raw_path, event_type)
            metrics = write_empty_parquet(
                curated_path,
                slug=slug,
                timeframe=timeframe,
                window_start_ts=window_start_ts,
                dt=dt,
                event_type=event_type,
            )
            source_size_bytes = 0
            source_mtime_ns = 0
        file_rows.append({
            "market_slug": slug,
            "timeframe": timeframe,
            "window_start_ts": window_start_ts,
            "market_dt": dt,
            "event_type": event_type,
            "source_path": str(source_path),
            "raw_path": str(raw_path),
            "curated_path": str(curated_path),
            "source_size_bytes": source_size_bytes,
            "source_mtime_ns": source_mtime_ns,
            "row_count": metrics["row_count"],
            "server_ts_ms_min": metrics["server_ts_ms_min"],
            "server_ts_ms_max": metrics["server_ts_ms_max"],
            "receive_ts_ms_min": metrics["receive_ts_ms_min"],
            "receive_ts_ms_max": metrics["receive_ts_ms_max"],
            "generated_at": generated_at,
        })
        curated_metrics[event_type] = {
            "source_path": str(source_path),
            "raw_path": str(raw_path),
            "curated_path": str(curated_path),
            "row_count": metrics["row_count"],
            "server_ts_ms_min": metrics["server_ts_ms_min"],
            "server_ts_ms_max": metrics["server_ts_ms_max"],
            "receive_ts_ms_min": metrics["receive_ts_ms_min"],
            "receive_ts_ms_max": metrics["receive_ts_ms_max"],
        }

    manifest = {
        "market_slug": slug,
        "materializer_schema_version": MATERIALIZER_SCHEMA_VERSION,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_start_utc": unix_to_utc(window_start_ts),
        "market_dt": dt,
        "generated_at": generated_at,
        "source_dir": str(market_dir),
        "raw_dir": str(raw_dir),
        "signature": signature,
        "datasets": curated_metrics,
    }
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = raw_dir / "manifest.json"
    write_json_atomic(manifest_path, manifest)

    market_row = {
        "market_slug": slug,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_start_utc": unix_to_utc(window_start_ts),
        "market_dt": dt,
        "source_dir": str(market_dir),
        "raw_dir": str(raw_dir),
        "manifest_path": str(manifest_path),
        "generated_at": generated_at,
        "book_rows": curated_metrics["book_snapshots"]["row_count"],
        "price_change_rows": curated_metrics["price_changes"]["row_count"],
        "trade_rows": curated_metrics["trades"]["row_count"],
        "receive_ts_ms_min": min(
            value
            for value in (
                curated_metrics["book_snapshots"]["receive_ts_ms_min"],
                curated_metrics["price_changes"]["receive_ts_ms_min"],
                curated_metrics["trades"]["receive_ts_ms_min"],
            )
            if value is not None
        )
        if any(
            value is not None
            for value in (
                curated_metrics["book_snapshots"]["receive_ts_ms_min"],
                curated_metrics["price_changes"]["receive_ts_ms_min"],
                curated_metrics["trades"]["receive_ts_ms_min"],
            )
        )
        else None,
        "receive_ts_ms_max": max(
            value
            for value in (
                curated_metrics["book_snapshots"]["receive_ts_ms_max"],
                curated_metrics["price_changes"]["receive_ts_ms_max"],
                curated_metrics["trades"]["receive_ts_ms_max"],
            )
            if value is not None
        )
        if any(
            value is not None
            for value in (
                curated_metrics["book_snapshots"]["receive_ts_ms_max"],
                curated_metrics["price_changes"]["receive_ts_ms_max"],
                curated_metrics["trades"]["receive_ts_ms_max"],
            )
        )
        else None,
    }
    write_single_row_parquet(meta_market_path(publish_dir, dt, timeframe, slug), market_row)
    write_file_catalog(meta_files_path(publish_dir, dt, timeframe, slug), file_rows)
    return {
        "slug": slug,
        "materializer_schema_version": MATERIALIZER_SCHEMA_VERSION,
        "signature": signature,
        "raw_dir": str(raw_dir),
        "manifest_path": str(manifest_path),
        "market_dt": dt,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "generated_at": generated_at,
    }


def state_market_identity(entry: dict[str, Any]) -> tuple[str, str, int]:
    slug = str(entry.get("slug") or "").strip()
    if not slug:
        raise ValueError("State entry is missing market slug.")

    timeframe = str(entry.get("timeframe") or "").strip()
    window_start_ts = entry.get("window_start_ts")
    if timeframe and window_start_ts is not None:
        return timeframe, slug, int(window_start_ts)

    parsed = parse_market_slug(slug)
    if parsed is None:
        raise ValueError(f"Unable to parse market slug from state entry: {slug}")
    parsed_timeframe, parsed_window_start_ts = parsed
    return parsed_timeframe, slug, parsed_window_start_ts


def build_market_key_entry(publish_dir: Path, entry: dict[str, Any]) -> dict[str, Any]:
    timeframe, slug, window_start_ts = state_market_identity(entry)
    dt = str(entry.get("market_dt") or date_from_ts(window_start_ts))

    raw_dir = raw_market_dir(publish_dir, dt, timeframe, slug)
    manifest_path = raw_dir / "manifest.json"
    raw_book_path = raw_dir / f"book_snapshots_{slug}.csv"
    raw_price_path = raw_dir / f"price_changes_{slug}.csv"
    raw_trade_path = raw_dir / f"trades_{slug}.csv"
    raw_recovery_path = raw_dir / f"recovery_events_{slug}.csv"
    curated_book_path = curated_event_path(publish_dir, dt, timeframe, slug, "book_snapshots")
    curated_price_path = curated_event_path(publish_dir, dt, timeframe, slug, "price_changes")
    curated_trade_path = curated_event_path(publish_dir, dt, timeframe, slug, "trades")
    curated_recovery_path = curated_event_path(publish_dir, dt, timeframe, slug, "recovery_events")
    meta_market = meta_market_path(publish_dir, dt, timeframe, slug)
    meta_files = meta_files_path(publish_dir, dt, timeframe, slug)

    return {
        "market_slug": slug,
        "timeframe": timeframe,
        "window_start_ts": window_start_ts,
        "window_start_utc": unix_to_utc(window_start_ts),
        "market_dt": dt,
        "generated_at": entry.get("generated_at"),
        "raw": {
            "prefix": f"{publish_relative_key(publish_dir, raw_dir)}/",
            "manifest": publish_relative_key(publish_dir, manifest_path) if manifest_path.exists() else None,
            "book_snapshots_csv": publish_relative_key(publish_dir, raw_book_path) if raw_book_path.exists() else None,
            "price_changes_csv": publish_relative_key(publish_dir, raw_price_path) if raw_price_path.exists() else None,
            "trades_csv": publish_relative_key(publish_dir, raw_trade_path) if raw_trade_path.exists() else None,
            "recovery_events_csv": publish_relative_key(publish_dir, raw_recovery_path) if raw_recovery_path.exists() else None,
        },
        "curated": {
            "book_snapshots": publish_relative_key(publish_dir, curated_book_path) if curated_book_path.exists() else None,
            "price_changes": publish_relative_key(publish_dir, curated_price_path) if curated_price_path.exists() else None,
            "trades": publish_relative_key(publish_dir, curated_trade_path) if curated_trade_path.exists() else None,
            "recovery_events": publish_relative_key(publish_dir, curated_recovery_path) if curated_recovery_path.exists() else None,
        },
        "meta": {
            "markets": publish_relative_key(publish_dir, meta_market) if meta_market.exists() else None,
            "files": publish_relative_key(publish_dir, meta_files) if meta_files.exists() else None,
        },
    }


def write_keysets(publish_dir: Path, state: dict[str, Any]) -> None:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for entry in state.get("markets", {}).values():
        if not isinstance(entry, dict):
            continue
        try:
            market_entry = build_market_key_entry(publish_dir, entry)
        except ValueError:
            logger.warning("Skipping invalid state entry while building keysets", exc_info=True)
            continue
        grouped[(market_entry["market_dt"], market_entry["timeframe"])].append(market_entry)

    index_rows: list[dict[str, Any]] = []
    latest_by_timeframe: dict[str, dict[str, Any]] = {}

    for (dt, timeframe), markets in sorted(grouped.items()):
        markets_sorted = sorted(markets, key=lambda item: (item["window_start_ts"], item["market_slug"]))
        key_groups: dict[str, list[str]] = {
            "raw_manifests": [],
            "raw_book_snapshots_csv": [],
            "raw_price_changes_csv": [],
            "raw_trades_csv": [],
            "raw_recovery_events_csv": [],
            "curated_book_snapshots": [],
            "curated_price_changes": [],
            "curated_trades": [],
            "curated_recovery_events": [],
            "meta_markets": [],
            "meta_files": [],
        }

        for market in markets_sorted:
            raw = market["raw"]
            curated = market["curated"]
            meta = market["meta"]

            if raw["manifest"]:
                key_groups["raw_manifests"].append(raw["manifest"])
            if raw["book_snapshots_csv"]:
                key_groups["raw_book_snapshots_csv"].append(raw["book_snapshots_csv"])
            if raw["price_changes_csv"]:
                key_groups["raw_price_changes_csv"].append(raw["price_changes_csv"])
            if raw["trades_csv"]:
                key_groups["raw_trades_csv"].append(raw["trades_csv"])
            if raw["recovery_events_csv"]:
                key_groups["raw_recovery_events_csv"].append(raw["recovery_events_csv"])
            if curated["book_snapshots"]:
                key_groups["curated_book_snapshots"].append(curated["book_snapshots"])
            if curated["price_changes"]:
                key_groups["curated_price_changes"].append(curated["price_changes"])
            if curated["trades"]:
                key_groups["curated_trades"].append(curated["trades"])
            if curated["recovery_events"]:
                key_groups["curated_recovery_events"].append(curated["recovery_events"])
            if meta["markets"]:
                key_groups["meta_markets"].append(meta["markets"])
            if meta["files"]:
                key_groups["meta_files"].append(meta["files"])

        manifest_path = keyset_manifest_path(publish_dir, dt, timeframe)
        manifest_key = publish_relative_key(publish_dir, manifest_path)
        manifest_payload = {
            "generated_at": iso_utc_now(),
            "market_dt": dt,
            "timeframe": timeframe,
            "market_count": len(markets_sorted),
            "window_start_ts_min": markets_sorted[0]["window_start_ts"],
            "window_start_ts_max": markets_sorted[-1]["window_start_ts"],
            "window_start_utc_min": markets_sorted[0]["window_start_utc"],
            "window_start_utc_max": markets_sorted[-1]["window_start_utc"],
            "markets": markets_sorted,
            "key_groups": key_groups,
        }
        write_json_atomic(manifest_path, manifest_payload)

        index_row = {
            "market_dt": dt,
            "timeframe": timeframe,
            "market_count": len(markets_sorted),
            "window_start_ts_min": markets_sorted[0]["window_start_ts"],
            "window_start_ts_max": markets_sorted[-1]["window_start_ts"],
            "manifest_key": manifest_key,
        }
        index_rows.append(index_row)

        latest = latest_by_timeframe.get(timeframe)
        if latest is None or (
            index_row["market_dt"],
            index_row["window_start_ts_max"],
        ) > (
            latest["market_dt"],
            latest["window_start_ts_max"],
        ):
            latest_by_timeframe[timeframe] = index_row

    write_json_atomic(
        keyset_index_path(publish_dir),
        {
            "generated_at": iso_utc_now(),
            "dataset_count": len(index_rows),
            "datasets": index_rows,
            "latest": latest_by_timeframe,
        },
    )


def process_once(
    *,
    source_dir: Path,
    publish_dir: Path,
    stable_seconds: float,
    state: dict[str, Any],
    link_mode: str,
) -> tuple[int, int, int]:
    now = time.time()
    processed = 0
    skipped_unstable = 0
    skipped_unchanged = 0

    for market_dir in list_market_dirs(source_dir):
        files = source_files_for_market(market_dir)
        if not all(path.exists() for path in files.values()):
            continue
        if not files_are_stable(files, stable_seconds, now):
            skipped_unstable += 1
            continue

        slug = market_dir.name
        signature = files_signature(files)
        previous = state["markets"].get(slug)
        if (
            isinstance(previous, dict)
            and previous.get("signature") == signature
            and previous.get("materializer_schema_version") == MATERIALIZER_SCHEMA_VERSION
        ):
            skipped_unchanged += 1
            continue

        result = materialize_market(market_dir, publish_dir, link_mode)
        state["markets"][slug] = result
        processed += 1
        logger.info(
            "Materialized %s -> dt=%s timeframe=%s raw_dir=%s",
            result["slug"],
            result["market_dt"],
            result["timeframe"],
            result["raw_dir"],
        )

    return processed, skipped_unstable, skipped_unchanged


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    source_dir = args.source_dir.expanduser().resolve()
    publish_dir = args.publish_dir.expanduser().resolve()
    state_file = (args.state_file or default_state_file(publish_dir)).expanduser().resolve()
    state = load_state(state_file)

    logger.info(
        "Materializer starting. source_dir=%s publish_dir=%s stable_seconds=%s",
        source_dir,
        publish_dir,
        args.stable_seconds,
    )

    while True:
        try:
            processed, skipped_unstable, skipped_unchanged = process_once(
                source_dir=source_dir,
                publish_dir=publish_dir,
                stable_seconds=args.stable_seconds,
                state=state,
                link_mode=args.link_mode,
            )
            write_keysets(publish_dir, state)
            save_state(state_file, state)
            logger.info(
                "Materializer pass complete. processed=%d skipped_unstable=%d skipped_unchanged=%d",
                processed,
                skipped_unstable,
                skipped_unchanged,
            )
        except KeyboardInterrupt:
            logger.info("Stopping materializer")
            break
        except Exception:
            logger.exception("Materializer pass failed")

        if args.once:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
