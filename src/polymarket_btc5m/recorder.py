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
from .timeframes import DEFAULT_TIMEFRAMES
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

RECOVERY_EVENT_HEADERS = [
    "event_seq",
    "event_ts_ms",
    "event_utc",
    "gap_id",
    "gap_started_ms",
    "gap_started_utc",
    "condition_id",
    "asset_id",
    "outcome",
    "recovery_event",
    "reason",
    "book_server_ts_ms",
    "bid_levels",
    "ask_levels",
    "error",
]


@dataclass
class RecorderConfig:
    output_dir: Path
    poll_interval_seconds: float = 30.0
    grace_period_seconds: float = 120.0
    timeout_seconds: int = 30
    max_retries: int = 5
    log_level: str = "INFO"
    timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES
    flush_interval_seconds: float = 5.0
    resnapshot_on_reconnect: bool = True


class MarketCsvWriter:
    """Manages the three CSV files for a single market."""

    def __init__(self, output_dir: Path, slug: str) -> None:
        self._dir = output_dir / "orderbook" / slug
        self._dir.mkdir(parents=True, exist_ok=True)

        self._book_path = self._dir / f"book_snapshots_{slug}.csv"
        self._changes_path = self._dir / f"price_changes_{slug}.csv"
        self._trades_path = self._dir / f"trades_{slug}.csv"
        self._recovery_path = self._dir / f"recovery_events_{slug}.csv"

        self._book_fh, self._book_writer = self._open(self._book_path, BOOK_SNAPSHOT_HEADERS)
        self._changes_fh, self._changes_writer = self._open(self._changes_path, PRICE_CHANGE_HEADERS)
        self._trades_fh, self._trades_writer = self._open(self._trades_path, WS_TRADE_HEADERS)
        self._recovery_fh, self._recovery_writer = self._open(
            self._recovery_path,
            RECOVERY_EVENT_HEADERS,
        )

        self._snapshot_seq = 0
        self._recovery_event_seq = 0
        self._lock = threading.Lock()
        self._closed = False

    def next_snapshot_seq(self) -> int:
        with self._lock:
            self._snapshot_seq += 1
            return self._snapshot_seq

    def write_book_row(self, row: dict[str, Any]) -> None:
        with self._lock:
            if self._closed:
                return
            self._book_writer.writerow(row)

    def write_change_row(self, row: dict[str, Any]) -> None:
        with self._lock:
            if self._closed:
                return
            self._changes_writer.writerow(row)

    def write_trade_row(self, row: dict[str, Any]) -> None:
        with self._lock:
            if self._closed:
                return
            self._trades_writer.writerow(row)

    def next_recovery_event_seq(self) -> int:
        with self._lock:
            self._recovery_event_seq += 1
            return self._recovery_event_seq

    def write_recovery_row(self, row: dict[str, Any]) -> None:
        with self._lock:
            if self._closed:
                return
            self._recovery_writer.writerow(row)

    def flush(self) -> None:
        with self._lock:
            if self._closed:
                return
            for fh in (self._book_fh, self._changes_fh, self._trades_fh, self._recovery_fh):
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                except Exception:
                    pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for fh in (self._book_fh, self._changes_fh, self._trades_fh, self._recovery_fh):
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                    fh.close()
                except Exception:
                    pass

    @staticmethod
    def _open(path: Path, headers: list[str]) -> tuple[Any, csv.DictWriter]:
        is_new = not path.exists() or path.stat().st_size == 0
        fh = path.open("a", newline="", encoding="utf-8", buffering=1)
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
            timeframes=config.timeframes,
        )
        self._ws = ClobWebSocket(
            on_event=self._handle_event,
            on_connected=self._on_ws_connected,
            on_disconnected=self._on_ws_disconnected,
        )
        self._writers: dict[str, MarketCsvWriter] = {}
        self._writers_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread: threading.Thread | None = None
        self._resnapshot_lock = threading.Lock()
        self._resnapshot_thread: threading.Thread | None = None
        self._resnapshot_needed = False
        self._gap_started_ms: int | None = None
        self._has_connected_once = False

        # Stats
        self._events_book = 0
        self._events_price_change = 0
        self._events_trade = 0
        self._resnapshot_count = 0
        self._resnapshot_failures = 0

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> None:
        configure_logging(self._config.log_level)
        logger.info(
            "Order book recorder starting. Output: %s timeframes=%s",
            self._config.output_dir,
            ",".join(self._config.timeframes),
        )

        # First poll before starting WS, so _on_ws_connected has tokens ready
        try:
            added, _ = self._tracker.poll_once()
            self._create_writers(added)
        except Exception:
            logger.exception("Initial discovery poll error")

        self._start_flush_thread()
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
                self._schedule_resnapshot_if_needed("pending_gap_retry")
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
            "Status: active_markets=%d writers=%d events(book=%d change=%d trade=%d) resnapshots=%d resnapshot_failures=%d pending_gap=%s",
            self._tracker.active_count(),
            len(self._writers),
            self._events_book,
            self._events_price_change,
            self._events_trade,
            self._resnapshot_count,
            self._resnapshot_failures,
            "yes" if self._resnapshot_needed else "no",
        )

    # ------------------------------------------------------------------
    # WS callbacks
    # ------------------------------------------------------------------

    def _on_ws_connected(self) -> None:
        logger.info("WS connected, subscribing all tracked tokens")
        all_tokens = self._tracker.get_all_token_ids()
        if all_tokens:
            self._ws.subscribe_initial(all_tokens)
        if self._has_connected_once:
            self._schedule_resnapshot_if_needed("ws_reconnect")
        else:
            self._has_connected_once = True

    def _on_ws_disconnected(self) -> None:
        logger.warning("WS disconnected")
        if self._stop_event.is_set():
            return
        if not self._has_connected_once or self._tracker.active_count() == 0:
            return
        with self._resnapshot_lock:
            is_new_gap = self._gap_started_ms is None
            if self._gap_started_ms is None:
                self._gap_started_ms = int(time.time() * 1000)
            self._resnapshot_needed = True
            gap_started_ms = self._gap_started_ms
        if is_new_gap:
            for market in self._tracker.get_active_markets():
                self._write_recovery_event(
                    market=market,
                    recovery_event="gap_open",
                    reason="ws_disconnect",
                    gap_started_ms=gap_started_ms,
                )

    def _start_flush_thread(self) -> None:
        if self._config.flush_interval_seconds <= 0:
            return
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name="orderbook-recorder-flush",
            daemon=True,
        )
        self._flush_thread.start()

    def _flush_loop(self) -> None:
        while not self._stop_event.wait(timeout=self._config.flush_interval_seconds):
            with self._writers_lock:
                writers = list(self._writers.items())
            for slug, writer in writers:
                try:
                    writer.flush()
                except Exception:
                    logger.warning("Periodic flush failed for %s", slug, exc_info=True)

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
        self._record_book_snapshot(event, receive_ts_ms, count_event=True)

    def _record_book_snapshot(
        self,
        event: dict[str, Any],
        receive_ts_ms: int,
        *,
        count_event: bool,
    ) -> None:
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

        self._write_book_rows(
            writer=writer,
            seq=seq,
            server_ts_ms=server_ts_ms,
            receive_ts_ms=receive_ts_ms,
            receive_utc=receive_utc,
            asset_id=asset_id,
            outcome=outcome,
            condition_id=condition_id,
            bids=event.get("bids", []),
            asks=event.get("asks", []),
        )

        if count_event:
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

    def _write_book_rows(
        self,
        *,
        writer: MarketCsvWriter,
        seq: int,
        server_ts_ms: Any,
        receive_ts_ms: int,
        receive_utc: str,
        asset_id: Any,
        outcome: str,
        condition_id: Any,
        bids: list[dict[str, Any]],
        asks: list[dict[str, Any]],
    ) -> None:
        for side_key, side_label in ((bids, "bid"), (asks, "ask")):
            for idx, level in enumerate(side_key):
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

    def _write_recovery_event(
        self,
        *,
        market: ActiveMarket,
        recovery_event: str,
        reason: str,
        gap_started_ms: int | None,
        asset_id: str = "",
        outcome: str = "",
        book_server_ts_ms: Any = "",
        bid_levels: int | str = "",
        ask_levels: int | str = "",
        error: str = "",
    ) -> None:
        with self._writers_lock:
            writer = self._writers.get(market.slug)
        if writer is None:
            return

        event_ts_ms = int(time.time() * 1000)
        writer.write_recovery_row({
            "event_seq": writer.next_recovery_event_seq(),
            "event_ts_ms": event_ts_ms,
            "event_utc": _ms_to_utc(event_ts_ms),
            "gap_id": f"gap-{gap_started_ms}" if gap_started_ms is not None else "",
            "gap_started_ms": gap_started_ms if gap_started_ms is not None else "",
            "gap_started_utc": _ms_to_utc(gap_started_ms) if gap_started_ms is not None else "",
            "condition_id": market.condition_id,
            "asset_id": asset_id,
            "outcome": outcome,
            "recovery_event": recovery_event,
            "reason": reason,
            "book_server_ts_ms": book_server_ts_ms,
            "bid_levels": bid_levels,
            "ask_levels": ask_levels,
            "error": error,
        })

    def _schedule_resnapshot_if_needed(self, reason: str) -> None:
        if not self._config.resnapshot_on_reconnect or not self._ws.is_connected:
            return
        with self._resnapshot_lock:
            if not self._resnapshot_needed:
                return
            if self._resnapshot_thread and self._resnapshot_thread.is_alive():
                return
            gap_started_ms = self._gap_started_ms
            self._resnapshot_thread = threading.Thread(
                target=self._run_resnapshot,
                args=(reason, gap_started_ms),
                name="orderbook-resnapshot",
                daemon=True,
            )
            self._resnapshot_thread.start()

    def _run_resnapshot(self, reason: str, gap_started_ms: int | None) -> None:
        token_ids = self._tracker.get_all_token_ids()
        if not token_ids:
            with self._resnapshot_lock:
                self._resnapshot_needed = False
                self._gap_started_ms = None
            return

        started = time.monotonic()
        restored = 0
        missing = 0
        failures = 0
        logger.warning(
            "Starting order book resnapshot. reason=%s tokens=%d gap_started_ms=%s",
            reason,
            len(token_ids),
            gap_started_ms,
        )

        for token_id in token_ids:
            if self._stop_event.is_set():
                return
            try:
                book = self._client.get_clob_book(token_id)
            except Exception:
                failures += 1
                resolved = self._tracker.resolve_token_market(token_id)
                if resolved is not None:
                    market, outcome = resolved
                    self._write_recovery_event(
                        market=market,
                        recovery_event="resnapshot_failure",
                        reason=reason,
                        gap_started_ms=gap_started_ms,
                        asset_id=token_id,
                        outcome=outcome,
                        error="book_fetch_failed",
                    )
                logger.warning(
                    "Order book resnapshot failed for token_id=%s",
                    token_id,
                    exc_info=True,
                )
                continue

            if not book:
                missing += 1
                resolved = self._tracker.resolve_token_market(token_id)
                if resolved is not None:
                    market, outcome = resolved
                    self._write_recovery_event(
                        market=market,
                        recovery_event="resnapshot_missing",
                        reason=reason,
                        gap_started_ms=gap_started_ms,
                        asset_id=token_id,
                        outcome=outcome,
                    )
                continue

            self._record_book_snapshot(book, int(time.time() * 1000), count_event=False)
            restored += 1
            resolved = self._tracker.resolve_token_market(token_id)
            if resolved is not None:
                market, outcome = resolved
                self._write_recovery_event(
                    market=market,
                    recovery_event="resnapshot_success",
                    reason=reason,
                    gap_started_ms=gap_started_ms,
                    asset_id=token_id,
                    outcome=outcome,
                    book_server_ts_ms=book.get("timestamp", ""),
                    bid_levels=len(book.get("bids", [])),
                    ask_levels=len(book.get("asks", [])),
                )

        duration_ms = int((time.monotonic() - started) * 1000)
        with self._resnapshot_lock:
            if failures == 0:
                self._resnapshot_needed = False
                self._gap_started_ms = None
                self._resnapshot_count += 1
            else:
                self._resnapshot_failures += 1

        if failures == 0 and gap_started_ms is not None:
            for market in self._tracker.get_active_markets():
                self._write_recovery_event(
                    market=market,
                    recovery_event="gap_recovered",
                    reason=reason,
                    gap_started_ms=gap_started_ms,
                )

        logger.warning(
            "Order book resnapshot finished. reason=%s restored=%d missing=%d failures=%d duration_ms=%d gap_started_ms=%s",
            reason,
            restored,
            missing,
            failures,
            duration_ms,
            gap_started_ms,
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _shutdown(self) -> None:
        logger.info("Shutting down recorder...")
        self._stop_event.set()
        self._ws.stop()
        if self._flush_thread:
            self._flush_thread.join(timeout=self._config.flush_interval_seconds + 1.0)
        if self._resnapshot_thread:
            self._resnapshot_thread.join(timeout=self._config.timeout_seconds + 1.0)
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
