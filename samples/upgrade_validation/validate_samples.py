#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
MANIFEST = json.loads((BASE / "manifest.json").read_text(encoding="utf-8"))

EXPECTED_HEADERS = [
    "market_slug",
    "window_start_ts",
    "condition_id",
    "event_id",
    "trade_timestamp",
    "trade_utc",
    "price",
    "size",
    "notional",
    "side",
    "outcome",
    "asset",
    "proxy_wallet",
    "transaction_hash",
    "dedupe_key",
    "timestamp_ms",
    "server_received_ms",
    "trade_time_ms",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_HEADERS:
            raise AssertionError(f"Header mismatch in {path.name}: {reader.fieldnames}")
        return list(reader)


def ensure_empty(v: str, msg: str) -> None:
    if str(v).strip() != "":
        raise AssertionError(msg)


def ensure_non_empty(v: str, msg: str) -> None:
    if str(v).strip() == "":
        raise AssertionError(msg)


# 1) batch without rpc
batch_no_rpc = read_csv(BASE / "batch_trades_no_rpc.csv")
for row in batch_no_rpc:
    ensure_empty(row["timestamp_ms"], "batch_no_rpc timestamp_ms should be empty")
    ensure_empty(row["server_received_ms"], "batch_no_rpc server_received_ms should be empty")
    ensure_empty(row["trade_time_ms"], "batch_no_rpc trade_time_ms should be empty")

# 2) batch with rpc
expect_map = {
    "0x3333333333333333333333333333333333333333333333333333333333333333": MANIFEST["log_index_expectation"]["0x3333333333333333333333333333333333333333333333333333333333333333"],
    "0x4444444444444444444444444444444444444444444444444444444444444444": MANIFEST["log_index_expectation"]["0x4444444444444444444444444444444444444444444444444444444444444444"],
}
batch_with_rpc = read_csv(BASE / "batch_trades_with_rpc.csv")
for row in batch_with_rpc:
    tx = row["transaction_hash"]
    log_index = int(expect_map[tx])
    ts = int(row["trade_timestamp"])
    ts_ms = int(row["timestamp_ms"])
    expected = ts * 1000 + log_index
    if ts_ms != expected:
        raise AssertionError(f"batch_with_rpc timestamp_ms mismatch: {tx} got={ts_ms} expected={expected}")
    ensure_empty(row["server_received_ms"], "batch_with_rpc server_received_ms should be empty")
    ensure_non_empty(row["trade_time_ms"], "batch_with_rpc trade_time_ms should be populated")

# 3) stream
stream_rows = read_csv(BASE / "stream_trades_live.csv")
for row in stream_rows:
    ts = int(row["trade_timestamp"])
    ts_ms = int(row["timestamp_ms"])
    if not (ts * 1000 <= ts_ms < ts * 1000 + 1000):
        raise AssertionError(f"stream timestamp_ms out of range: {row}")
    ensure_non_empty(row["server_received_ms"], "stream server_received_ms should be populated")
    ensure_non_empty(row["trade_time_ms"], "stream trade_time_ms should be populated")

# 4) ws push sample
ws_lines = (BASE / "stream_ws_push.jsonl").read_text(encoding="utf-8").strip().splitlines()
if len(ws_lines) != len(stream_rows):
    raise AssertionError("stream_ws_push.jsonl line count mismatch")

print("sample_validation_passed")
