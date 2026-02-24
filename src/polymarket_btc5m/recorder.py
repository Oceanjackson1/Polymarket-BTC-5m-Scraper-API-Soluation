from __future__ import annotations

import csv
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import PolymarketApiClient
from .market_tracker import ActiveMarket, MarketTracker
from .pipeline import configure_logging
from .ws_connection import ClobWebSocket

logger = logging.getLogger(__name__)

BOOK_SNAPSHOT_HEADERS = [
    "snapshot_seq",
    "server_ts_ms",
    "receive_ts_ms",
    "receive_utc",
    "asset_id",
    "outcome",
    "condition_id",
    "side",
    "price",
    "size",
    "level_index",
]

PRICE_CHANGE_HEADERS = [
    "server_ts_ms",
    "receive_ts_ms",
    "receive_utc",
    "condition_id",
    "asset_id",
    "outcome",
    "side",
    "price",
    "size",
    "best_bid",
    "best_ask",
]

WS_TRADE_HEADERS = [
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
]


@dataclass
class RecorderConfig:
    output_dir: Path
    poll_interval_seconds: float = 30.0
    grace_period_seconds: float = 120.0
    timeout_seconds: int = 30
    max_retries: int = 5
    log_level: str = "INFO"


class MarketCsvWriter:
    """Manages the three CSV files for a single market."""

    def __init__(self, output_dir: Path, slug: str) -> None:
        self._dir = output_dir / "orderbook" / slug
        self._dir.mkdir(parents=True, exist_ok=True)

        self._book_path = self._dir / f"book_snapshots_{slug}.csv"
        self._changes_path = self._dir / f"price_changes_{slug}.csv"
        self._trades_path = self._dir / f"trades_{slug}.csv"

        self._book_fh, self._book_writer = self._open(self._book_path, BOOK_SNAPSHOT_HEADERS)
        self._changes_fh, self._changes_writer = self._open(self._changes_path, PRICE_CHANGE_HEADERS)
        self._trades_fh, self._trades_writer = self._open(self._trades_path, WS_TRADE_HEADERS)

        self._snapshot_seq = 0

    def next_snapshot_seq(self) -> int:
        self._snapshot_seq += 1
        return self._snapshot_seq

    def write_book_row(self, row: dict[str, Any]) -> None:
        self._book_writer.writerow(row)

    def write_change_row(self, row: dict[str, Any]) -> None:
        self._changes_writer.writerow(row)

    def write_trade_row(self, row: dict[str, Any]) -> None:
        self._trades_writer.writerow(row)

    def flush(self) -> None:
        for fh in (self._book_fh, self._changes_fh, self._trades_fh):
            try:
                fh.flush()
                os.fsync(fh.fileno())
            except Exception:
                pass

    def close(self) -> None:
        for fh in (self._book_fh, self._changes_fh, self._trades_fh):
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass

    @staticmethod
    def _open(path: Path, headers: list[str]) -> tuple[Any, csv.DictWriter]:
        is_new = not path.exists() or path.stat().st_size == 0
        fh = path.open("a", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=headers)
        if is_new:
            writer.writeheader()
            fh.flush()
        return fh, writer


class OrderBookRecorder:
    """Main daemon: discovers markets, manages WS subscriptions, writes CSV."""

    def __init__(self, config: RecorderConfig) -> None:
        self._config = config
        self._client = PolymarketApiClient(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self._tracker = MarketTracker(
            client=self._client,
            grace_period_seconds=config.grace_period_seconds,
            poll_interval_seconds=config.poll_interval_seconds,
        )
        self._ws = ClobWebSocket(
            on_event=self._handle_event,
            on_connected=self._on_ws_connected,
            on_disconnected=self._on_ws_disconnected,
        )
        self._writers: dict[str, MarketCsvWriter] = {}
        self._writers_lock = threading.Lock()
        self._stop_event = threading.Event()

        # Stats
        self._events_book = 0
        self._events_price_change = 0
        self._events_trade = 0

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> None:
        configure_logging(self._config.log_level)
        logger.info("Order book recorder starting. Output: %s", self._config.output_dir)

        # First poll before starting WS, so _on_ws_connected has tokens ready
        try:
            added, _ = self._tracker.poll_once()
            self._create_writers(added)
        except Exception:
            logger.exception("Initial discovery poll error")

        self._ws.start()
        try:
            self._discovery_loop()
        except KeyboardInterrupt:
            logger.info("Shutdown requested (KeyboardInterrupt)")
        finally:
            self._shutdown()

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Discovery loop (main thread)
    # ------------------------------------------------------------------

    def _discovery_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                added, expired = self._tracker.poll_once()
                self._subscribe_markets(added)
                self._unsubscribe_markets(expired)
                self._log_status()
            except Exception:
                logger.exception("Discovery poll error")
            self._stop_event.wait(timeout=self._config.poll_interval_seconds)

    def _create_writers(self, markets: list[ActiveMarket]) -> None:
        """Create CSV writers for new markets (no WS subscription)."""
        if not markets:
            return
        with self._writers_lock:
            for market in markets:
                if market.slug not in self._writers:
                    self._writers[market.slug] = MarketCsvWriter(
                        self._config.output_dir, market.slug
                    )

    def _subscribe_markets(self, markets: list[ActiveMarket]) -> None:
        if not markets:
            return
        self._create_writers(markets)
        all_new_tokens: list[str] = []
        for market in markets:
            all_new_tokens.extend(market.token_ids)

        # Only send incremental subscribe; _on_ws_connected handles initial/reconnect
        if all_new_tokens and self._ws.is_connected:
            self._ws.subscribe(all_new_tokens)

    def _unsubscribe_markets(self, markets: list[ActiveMarket]) -> None:
        if not markets:
            return
        all_old_tokens: list[str] = []
        with self._writers_lock:
            for market in markets:
                all_old_tokens.extend(market.token_ids)
                writer = self._writers.pop(market.slug, None)
                if writer:
                    writer.flush()
                    writer.close()
                    logger.info("Closed CSV writer for %s", market.slug)

        if all_old_tokens:
            self._ws.unsubscribe(all_old_tokens)

    def _log_status(self) -> None:
        logger.info(
            "Status: active_markets=%d writers=%d events(book=%d change=%d trade=%d)",
            self._tracker.active_count(),
            len(self._writers),
            self._events_book,
            self._events_price_change,
            self._events_trade,
        )

    # ------------------------------------------------------------------
    # WS callbacks
    # ------------------------------------------------------------------

    def _on_ws_connected(self) -> None:
        logger.info("WS connected, subscribing all tracked tokens")
        all_tokens = self._tracker.get_all_token_ids()
        if all_tokens:
            self._ws.subscribe_initial(all_tokens)

    def _on_ws_disconnected(self) -> None:
        logger.warning("WS disconnected")

    # ------------------------------------------------------------------
    # Event dispatch (WS thread)
    # ------------------------------------------------------------------

    def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type", "")
        receive_ts_ms = int(time.time() * 1000)

        if event_type == "book":
            self._handle_book(event, receive_ts_ms)
        elif event_type == "price_change":
            self._handle_price_change(event, receive_ts_ms)
        elif event_type == "last_trade_price":
            self._handle_last_trade(event, receive_ts_ms)

    def _handle_book(self, event: dict[str, Any], receive_ts_ms: int) -> None:
        asset_id = event.get("asset_id", "")
        resolved = self._tracker.resolve_token(asset_id)
        if resolved is None:
            return

        slug, outcome = resolved
        with self._writers_lock:
            writer = self._writers.get(slug)
        if writer is None:
            return

        condition_id = event.get("market", "")
        server_ts_ms = event.get("timestamp", "")
        receive_utc = _ms_to_utc(receive_ts_ms)
        seq = writer.next_snapshot_seq()

        for side_key, side_label in (("bids", "bid"), ("asks", "ask")):
            for idx, level in enumerate(event.get(side_key, [])):
                writer.write_book_row({
                    "snapshot_seq": seq,
                    "server_ts_ms": server_ts_ms,
                    "receive_ts_ms": receive_ts_ms,
                    "receive_utc": receive_utc,
                    "asset_id": asset_id,
                    "outcome": outcome,
                    "condition_id": condition_id,
                    "side": side_label,
                    "price": level.get("price", ""),
                    "size": level.get("size", ""),
                    "level_index": idx,
                })

        self._events_book += 1

    def _handle_price_change(self, event: dict[str, Any], receive_ts_ms: int) -> None:
        condition_id = event.get("market", "")
        server_ts_ms = event.get("timestamp", "")
        receive_utc = _ms_to_utc(receive_ts_ms)

        for change in event.get("price_changes", []):
            asset_id = change.get("asset_id", "")
            resolved = self._tracker.resolve_token(asset_id)
            if resolved is None:
                continue

            slug, outcome = resolved
            with self._writers_lock:
                writer = self._writers.get(slug)
            if writer is None:
                continue

            writer.write_change_row({
                "server_ts_ms": server_ts_ms,
                "receive_ts_ms": receive_ts_ms,
                "receive_utc": receive_utc,
                "condition_id": condition_id,
                "asset_id": asset_id,
                "outcome": outcome,
                "side": change.get("side", ""),
                "price": change.get("price", ""),
                "size": change.get("size", ""),
                "best_bid": change.get("best_bid", ""),
                "best_ask": change.get("best_ask", ""),
            })

        self._events_price_change += 1

    def _handle_last_trade(self, event: dict[str, Any], receive_ts_ms: int) -> None:
        asset_id = event.get("asset_id", "")
        resolved = self._tracker.resolve_token(asset_id)
        if resolved is None:
            return

        slug, outcome = resolved
        with self._writers_lock:
            writer = self._writers.get(slug)
        if writer is None:
            return

        writer.write_trade_row({
            "server_ts_ms": event.get("timestamp", ""),
            "receive_ts_ms": receive_ts_ms,
            "receive_utc": _ms_to_utc(receive_ts_ms),
            "condition_id": event.get("market", ""),
            "asset_id": asset_id,
            "outcome": outcome,
            "side": event.get("side", ""),
            "price": event.get("price", ""),
            "size": event.get("size", ""),
            "fee_rate_bps": event.get("fee_rate_bps", ""),
        })

        self._events_trade += 1

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _shutdown(self) -> None:
        logger.info("Shutting down recorder...")
        self._ws.stop()
        with self._writers_lock:
            for slug, writer in self._writers.items():
                writer.flush()
                writer.close()
            self._writers.clear()
        logger.info(
            "Recorder stopped. Total events: book=%d change=%d trade=%d",
            self._events_book,
            self._events_price_change,
            self._events_trade,
        )


def _ms_to_utc(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
