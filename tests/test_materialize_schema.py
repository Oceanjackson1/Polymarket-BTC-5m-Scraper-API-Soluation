from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.data_api import normalize_table
from scripts.materialize_orderbook_data import materialize_market, read_and_write_parquet


class MaterializeSchemaTest(unittest.TestCase):
    def test_read_and_write_parquet_preserves_asset_id_as_string(self) -> None:
        asset_id = "12345678901234567890123456789012345678901234567890"
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "trades.csv"
            target_path = Path(tmpdir) / "trades.parquet"
            with source_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "server_ts_ms",
                        "receive_ts_ms",
                        "receive_utc",
                        "condition_id",
                        "asset_id",
                        "outcome",
                        "side",
                        "price",
                        "size",
                        "fee_rate_bps",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "server_ts_ms": "1772438692802",
                        "receive_ts_ms": "1772438692928",
                        "receive_utc": "2026-03-02T08:04:52.928Z",
                        "condition_id": "0xabc",
                        "asset_id": asset_id,
                        "outcome": "Up",
                        "side": "BUY",
                        "price": "0.42",
                        "size": "100.5",
                        "fee_rate_bps": "0",
                    }
                )

            read_and_write_parquet(
                source_path,
                target_path,
                slug="btc-updown-15m-1772438400",
                timeframe="15m",
                window_start_ts=1772438400,
                dt="2026-03-02",
                event_type="trades",
            )
            table = pq.read_table(target_path)
            self.assertTrue(pa.types.is_string(table.schema.field("asset_id").type))
            self.assertEqual(table.column("asset_id").to_pylist()[0], asset_id)

    def test_normalize_table_casts_numeric_asset_id_to_string(self) -> None:
        table = pa.table(
            {
                "asset_id": pa.array([4566240981356727000000000000000000000000000000000000000000000000000000000000.0]),
                "condition_id": pa.array(["0xabc"]),
            }
        )
        normalized = normalize_table(table)
        self.assertTrue(pa.types.is_string(normalized.schema.field("asset_id").type))
        self.assertEqual(normalized.column("asset_id").to_pylist()[0], "4.566240981356727e+75")

    def test_materialize_market_synthesizes_empty_recovery_events_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            market_dir = Path(tmpdir) / "btc-updown-15m-1772438400"
            publish_dir = Path(tmpdir) / "publish"
            market_dir.mkdir(parents=True, exist_ok=True)

            files = {
                "book_snapshots": [
                    {
                        "snapshot_seq": "1",
                        "server_ts_ms": "1772438693018",
                        "receive_ts_ms": "1772438693221",
                        "receive_utc": "2026-03-02T08:04:53.221Z",
                        "asset_id": "123",
                        "outcome": "Up",
                        "condition_id": "0xabc",
                        "side": "bid",
                        "price": "0.01",
                        "size": "10",
                        "level_index": "0",
                    }
                ],
                "price_changes": [
                    {
                        "server_ts_ms": "1772438692804",
                        "receive_ts_ms": "1772438692933",
                        "receive_utc": "2026-03-02T08:04:52.933Z",
                        "condition_id": "0xabc",
                        "asset_id": "123",
                        "outcome": "Up",
                        "side": "BUY",
                        "price": "0.25",
                        "size": "10",
                        "best_bid": "0.13",
                        "best_ask": "0.14",
                    }
                ],
                "trades": [
                    {
                        "server_ts_ms": "1772438692802",
                        "receive_ts_ms": "1772438692928",
                        "receive_utc": "2026-03-02T08:04:52.928Z",
                        "condition_id": "0xabc",
                        "asset_id": "123",
                        "outcome": "Up",
                        "side": "BUY",
                        "price": "0.42",
                        "size": "100.5",
                        "fee_rate_bps": "0",
                    }
                ],
            }
            for prefix, rows in files.items():
                path = market_dir / f"{prefix}_{market_dir.name}.csv"
                with path.open("w", newline="", encoding="utf-8") as fh:
                    writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows)

            materialize_market(market_dir, publish_dir, "copy")
            recovery_path = (
                publish_dir
                / "curated"
                / "recovery_events"
                / "dt=2026-03-02"
                / "timeframe=15m"
                / f"{market_dir.name}.parquet"
            )
            table = pq.read_table(recovery_path)
            self.assertEqual(table.num_rows, 0)
            self.assertTrue(pa.types.is_string(table.schema.field("asset_id").type))


if __name__ == "__main__":
    unittest.main()
