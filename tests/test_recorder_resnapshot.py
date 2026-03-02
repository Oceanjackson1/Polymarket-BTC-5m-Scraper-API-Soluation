from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from polymarket_btc5m.market_tracker import ActiveMarket
from polymarket_btc5m.recorder import OrderBookRecorder, RecorderConfig


class FakeClient:
    def __init__(self, books: dict[str, dict[str, object] | None]) -> None:
        self._books = books

    def get_clob_book(self, token_id: str) -> dict[str, object] | None:
        return self._books.get(token_id)


class FakeTracker:
    def __init__(self, market: ActiveMarket) -> None:
        self._market = market

    def get_all_token_ids(self) -> list[str]:
        return list(self._market.token_ids)

    def get_active_markets(self) -> list[ActiveMarket]:
        return [self._market]

    def resolve_token(self, token_id: str) -> tuple[str, str] | None:
        outcome = self._market.token_to_outcome.get(token_id)
        if outcome is None:
            return None
        return self._market.slug, outcome

    def resolve_token_market(self, token_id: str) -> tuple[ActiveMarket, str] | None:
        outcome = self._market.token_to_outcome.get(token_id)
        if outcome is None:
            return None
        return self._market, outcome

    def active_count(self) -> int:
        return 1


class RecorderResnapshotTest(unittest.TestCase):
    def test_run_resnapshot_writes_book_rows_and_clears_pending_gap(self) -> None:
        market = ActiveMarket(
            slug="btc-updown-5m-1772531400",
            timeframe="5m",
            event_id="evt-1",
            window_start_ts=1772531400,
            condition_id="0xcondition",
            token_ids=["token-up"],
            outcomes=["Up"],
            end_date_utc="2026-03-03T00:05:00Z",
            end_date_ts=1772496300.0,
        )
        book = {
            "market": market.condition_id,
            "asset_id": "token-up",
            "timestamp": "1772445684837",
            "bids": [
                {"price": "0.01", "size": "12"},
                {"price": "0.02", "size": "8"},
            ],
            "asks": [
                {"price": "0.98", "size": "5"},
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            recorder = OrderBookRecorder(RecorderConfig(output_dir=Path(tmpdir)))
            recorder._tracker = FakeTracker(market)  # type: ignore[assignment]
            recorder._client = FakeClient({"token-up": book})  # type: ignore[assignment]
            recorder._create_writers([market])
            recorder._resnapshot_needed = True
            recorder._gap_started_ms = 1772445684000
            recorder._write_recovery_event(
                market=market,
                recovery_event="gap_open",
                reason="unit_test_disconnect",
                gap_started_ms=recorder._gap_started_ms,
            )

            recorder._run_resnapshot("unit_test", recorder._gap_started_ms)
            recorder._shutdown()

            book_path = (
                Path(tmpdir)
                / "orderbook"
                / market.slug
                / f"book_snapshots_{market.slug}.csv"
            )
            with book_path.open("r", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            recovery_path = (
                Path(tmpdir)
                / "orderbook"
                / market.slug
                / f"recovery_events_{market.slug}.csv"
            )
            with recovery_path.open("r", encoding="utf-8") as fh:
                recovery_rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["asset_id"], "token-up")
        self.assertEqual(rows[0]["side"], "bid")
        self.assertEqual(rows[-1]["side"], "ask")
        self.assertEqual(
            [row["recovery_event"] for row in recovery_rows],
            ["gap_open", "resnapshot_success", "gap_recovered"],
        )
        self.assertEqual(recovery_rows[1]["asset_id"], "token-up")
        self.assertFalse(recorder._resnapshot_needed)
        self.assertIsNone(recorder._gap_started_ms)
        self.assertEqual(recorder._resnapshot_count, 1)
        self.assertEqual(recorder._events_book, 0)


if __name__ == "__main__":
    unittest.main()
