from __future__ import annotations

import json
from typing import Any

KEYSET_GROUPS: dict[str, str] = {
    "raw_manifests": "Raw manifest JSON objects for each market.",
    "raw_book_snapshots_csv": "Raw book snapshot CSV objects for each market.",
    "raw_price_changes_csv": "Raw price change CSV objects for each market.",
    "raw_recovery_events_csv": "Raw recovery event CSV objects for each market.",
    "raw_trades_csv": "Raw trade CSV objects for each market.",
    "curated_book_snapshots": "Curated book snapshot parquet objects.",
    "curated_price_changes": "Curated price change parquet objects.",
    "curated_recovery_events": "Curated recovery event parquet objects.",
    "curated_trades": "Curated trade parquet objects.",
    "meta_markets": "Per-market metadata parquet objects.",
    "meta_files": "Per-market file catalog parquet objects.",
}

PARQUET_KEYSET_GROUPS = {
    "curated_book_snapshots",
    "curated_price_changes",
    "curated_recovery_events",
    "curated_trades",
    "meta_markets",
    "meta_files",
}


def normalize_keyset_group(group: str) -> str:
    cleaned = str(group or "").strip().lower().replace("-", "_")
    if cleaned not in KEYSET_GROUPS:
        supported = ", ".join(sorted(KEYSET_GROUPS))
        raise ValueError(f"Unsupported keyset group '{group}'. Supported groups: {supported}")
    return cleaned


def load_keyset_manifest(payload: bytes) -> dict[str, Any]:
    parsed = json.loads(payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("Keyset manifest payload must be a JSON object.")
    return parsed


def keyset_group_keys(manifest: dict[str, Any], group: str) -> list[str]:
    normalized = normalize_keyset_group(group)
    key_groups = manifest.get("key_groups")
    if not isinstance(key_groups, dict):
        return []
    raw_keys = key_groups.get(normalized, [])
    if not isinstance(raw_keys, list):
        return []
    return [str(item) for item in raw_keys if str(item).strip()]
