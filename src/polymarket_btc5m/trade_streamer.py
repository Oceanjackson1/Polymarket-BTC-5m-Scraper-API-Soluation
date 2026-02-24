from __future__ import annotations

import asyncio
import csv
import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Any

import websocket
import websockets

from .chain import (
    EXCHANGE_ADDRESSES,
    USDC_ASSET_ID,
    decode_order_filled_log,
    ms_to_utc,
    parse_hex_int,
    scaled_amount,
    PolygonRpcClient,
)
from .client import PolymarketApiClient
from .market_tracker import MarketTracker
from .pipeline import TRADES_HEADERS, configure_logging, ts_to_utc
from .timeframes import DEFAULT_TIMEFRAMES, normalize_timeframes, timeframe_file_suffix

logger = logging.getLogger(__name__)

RECONNECT_BASE_DELAY_SECONDS = 1.0
RECONNECT_MAX_DELAY_SECONDS = 60.0
SEEN_LOG_UID_LIMIT = 100_000


@dataclass
class TradeStreamConfig:
    rpc_url: str
    output_dir: Path
    timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES
    market_poll_interval_seconds: float = 300.0
    grace_period_seconds: float = 120.0
    local_ws_host: str = "127.0.0.1"
    local_ws_port: int = 8765
    timeout_seconds: int = 30
    max_retries: int = 5
    log_level: str = "INFO"


class CsvTradeAppender:
    def __init__(self, output_dir: Path, timeframes: tuple[str, ...]) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        suffix = timeframe_file_suffix(timeframes)
        if suffix == "5m":
            file_name = "trades_btc_up_or_down_5m.csv"
        else:
            file_name = f"trades_btc_up_or_down_{suffix}.csv"
        self.path = output_dir / file_name
        is_new = not self.path.exists() or self.path.stat().st_size == 0
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=TRADES_HEADERS)
        self._lock = threading.Lock()
        if is_new:
            self._writer.writeheader()
            self._fh.flush()

    def append(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._writer.writerow(row)
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass


class LocalWsBroadcaster:
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: websockets.server.Serve | None = None
        self._clients: set[Any] = set()
        self._started = threading.Event()
        self._startup_error: Exception | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._started.wait(timeout=10)
        if self._startup_error is not None:
            raise RuntimeError(f"Local WS broadcaster failed: {self._startup_error}")

    def broadcast(self, payload: dict[str, Any]) -> None:
        if self._loop is None:
            return
        message = json.dumps(payload, ensure_ascii=False)
        asyncio.run_coroutine_threadsafe(self._broadcast_message(message), self._loop)

    def stop(self) -> None:
        if self._loop is None:
            return
        close_future = asyncio.run_coroutine_threadsafe(self._close_server(), self._loop)
        try:
            close_future.result(timeout=5)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._start_server())
        except Exception as error:  # noqa: BLE001
            self._startup_error = error
            self._started.set()
            return
        self._started.set()
        loop.run_forever()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    async def _start_server(self) -> None:
        self._server = await websockets.serve(
            self._handle_client,
            self._host,
            self._port,
            ping_interval=20,
            ping_timeout=20,
        )
        logger.info("Local WS broadcaster listening ws://%s:%d", self._host, self._port)

    async def _close_server(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._clients:
            await asyncio.gather(
                *(client.close() for client in list(self._clients)),
                return_exceptions=True,
            )
        self._clients.clear()

    async def _handle_client(self, websocket_conn: Any) -> None:
        self._clients.add(websocket_conn)
        try:
            async for _ in websocket_conn:
                pass
        except Exception:
            pass
        finally:
            self._clients.discard(websocket_conn)

    async def _broadcast_message(self, message: str) -> None:
        if not self._clients:
            return
        stale: list[Any] = []
        for client in list(self._clients):
            try:
                await client.send(message)
            except Exception:
                stale.append(client)
        for client in stale:
            self._clients.discard(client)


class PolygonHeadSubscriber:
    def __init__(
        self,
        rpc_url: str,
        on_head: Any,
        on_connected: Any,
        on_disconnected: Any,
    ) -> None:
        self._rpc_url = rpc_url
        self._on_head = on_head
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected = threading.Event()
        self._send_lock = threading.Lock()
        self._reconnect_delay = RECONNECT_BASE_DELAY_SECONDS

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._connect_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._send_lock:
            if self._ws:
                self._ws.close()
        if self._thread:
            self._thread.join(timeout=10)

    def _connect_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self._rpc_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                logger.exception("Polygon WS connection error")

            if self._stop_event.is_set():
                break

            logger.warning("Polygon WS reconnecting in %.1fs", self._reconnect_delay)
            self._stop_event.wait(timeout=self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                RECONNECT_MAX_DELAY_SECONDS,
            )

    def _on_open(self, ws: Any) -> None:
        self._connected.set()
        self._reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        subscribe_msg = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"],
            }
        )
        self._send(subscribe_msg)
        try:
            self._on_connected()
        except Exception:
            logger.exception("on_connected callback failed")

    def _on_message(self, ws: Any, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        if not isinstance(payload, dict):
            return

        if payload.get("method") != "eth_subscription":
            return

        params = payload.get("params", {})
        if not isinstance(params, dict):
            return

        result = params.get("result")
        if not isinstance(result, dict):
            return

        block_number = result.get("number")
        if block_number is None:
            return

        try:
            self._on_head(result)
        except Exception:
            logger.exception("on_head callback failed")

    def _on_error(self, ws: Any, error: Any) -> None:
        logger.warning("Polygon WS error: %s", error)

    def _on_close(self, ws: Any, status: Any, message: Any) -> None:
        self._connected.clear()
        try:
            self._on_disconnected()
        except Exception:
            logger.exception("on_disconnected callback failed")

    def _send(self, message: str) -> None:
        with self._send_lock:
            if self._ws is None:
                return
            try:
                self._ws.send(message)
            except Exception:
                logger.warning("Polygon WS send failed", exc_info=True)


class TradeStreamService:
    def __init__(self, config: TradeStreamConfig) -> None:
        self._config = config
        self._timeframes = normalize_timeframes(config.timeframes)
        self._api_client = PolymarketApiClient(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self._tracker = MarketTracker(
            client=self._api_client,
            grace_period_seconds=config.grace_period_seconds,
            poll_interval_seconds=config.market_poll_interval_seconds,
            timeframes=self._timeframes,
        )
        self._rpc = PolygonRpcClient(
            rpc_url=config.rpc_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        self._csv = CsvTradeAppender(config.output_dir, self._timeframes)
        self._broadcaster = LocalWsBroadcaster(
            host=config.local_ws_host,
            port=config.local_ws_port,
        )
        self._subscriber = PolygonHeadSubscriber(
            rpc_url=config.rpc_url,
            on_head=self._on_new_head,
            on_connected=self._on_ws_connected,
            on_disconnected=self._on_ws_disconnected,
        )
        self._stop_event = threading.Event()
        self._head_queue: Queue[tuple[int, int, int]] = Queue()
        self._processor_thread = threading.Thread(
            target=self._process_heads_loop,
            daemon=True,
        )
        self._processed_blocks: deque[int] = deque(maxlen=4096)
        self._processed_block_set: set[int] = set()
        self._seen_log_uids: deque[str] = deque(maxlen=SEEN_LOG_UID_LIMIT)
        self._seen_log_uid_set: set[str] = set()
        self._events_processed = 0

    def run(self) -> None:
        configure_logging(self._config.log_level)
        logger.info(
            "Trade stream starting. timeframes=%s output_csv=%s",
            ",".join(self._timeframes),
            self._csv.path,
        )

        try:
            added, _ = self._tracker.poll_once()
            if added:
                logger.info("Initial active markets discovered: %s", [m.slug for m in added])
        except Exception:
            logger.exception("Initial market discovery failed")

        self._broadcaster.start()
        self._processor_thread.start()
        self._subscriber.start()

        try:
            self._discovery_loop()
        except KeyboardInterrupt:
            logger.info("Shutdown requested (KeyboardInterrupt)")
        finally:
            self._shutdown()

    def stop(self) -> None:
        self._stop_event.set()

    def _discovery_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                added, expired = self._tracker.poll_once()
                if added:
                    logger.info("Discovered new markets: %s", [m.slug for m in added])
                if expired:
                    logger.info("Expired markets removed: %s", [m.slug for m in expired])
                logger.info(
                    "Status: active_markets=%d tracked_tokens=%d processed_events=%d",
                    self._tracker.active_count(),
                    len(self._tracker.get_all_token_ids()),
                    self._events_processed,
                )
            except Exception:
                logger.exception("Discovery poll failed")

            self._stop_event.wait(timeout=self._config.market_poll_interval_seconds)

    def _on_ws_connected(self) -> None:
        logger.info("Polygon WS connected: %s", self._config.rpc_url)

    def _on_ws_disconnected(self) -> None:
        logger.warning("Polygon WS disconnected")

    def _on_new_head(self, head: dict[str, Any]) -> None:
        block_number = parse_hex_int(head.get("number"))
        if block_number <= 0:
            return
        block_timestamp = parse_hex_int(head.get("timestamp"))
        receive_ms = int(time.time() * 1000)
        self._head_queue.put((block_number, block_timestamp, receive_ms))

    def _process_heads_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                block_number, block_timestamp, receive_ms = self._head_queue.get(timeout=1)
            except Empty:
                continue

            if self._is_processed_block(block_number):
                continue

            try:
                self._process_block(block_number, block_timestamp, receive_ms)
            except Exception:
                logger.exception("Failed to process block=%s", block_number)

    def _process_block(self, block_number: int, block_timestamp: int, receive_ms: int) -> None:
        if block_timestamp == 0:
            block_timestamp = self._rpc.get_block_timestamp(block_number)
            if block_timestamp == 0:
                logger.warning("Skip block=%s because timestamp is unavailable", block_number)
                return

        logs = self._rpc.get_order_filled_logs(block_number)
        if not logs:
            return

        logs = sorted(logs, key=lambda x: parse_hex_int(x.get("logIndex")))
        for raw in logs:
            decoded = decode_order_filled_log(raw)
            if decoded is None:
                continue
            if decoded.address not in EXCHANGE_ADDRESSES:
                continue

            row = self._normalize_trade(decoded, block_timestamp, receive_ms)
            if row is None:
                continue

            log_uid = f"{decoded.tx_hash}:{decoded.log_index}"
            if self._is_seen_log_uid(log_uid):
                continue

            self._csv.append(row)
            self._broadcaster.broadcast(row)
            self._events_processed += 1

    def _normalize_trade(
        self,
        decoded: Any,
        block_timestamp: int,
        receive_ms: int,
    ) -> dict[str, Any] | None:
        maker_match = self._tracker.resolve_token_market(str(decoded.maker_asset_id))
        taker_match = self._tracker.resolve_token_market(str(decoded.taker_asset_id))

        if maker_match is None and taker_match is None:
            return None

        if maker_match is not None:
            market, outcome = maker_match
            token_asset_id = decoded.maker_asset_id
            side = "SELL"
            size_raw = decoded.maker_amount_raw
            quote_asset_id = decoded.taker_asset_id
            quote_raw = decoded.taker_amount_raw
        else:
            market, outcome = taker_match
            token_asset_id = decoded.taker_asset_id
            side = "BUY"
            size_raw = decoded.taker_amount_raw
            quote_asset_id = decoded.maker_asset_id
            quote_raw = decoded.maker_amount_raw

        size_dec = scaled_amount(size_raw)
        if size_dec <= 0:
            return None

        if quote_asset_id == USDC_ASSET_ID:
            notional_dec = scaled_amount(quote_raw)
            price_dec = notional_dec / size_dec
            notional: float | str = round(float(notional_dec), 10)
            price: float | str = round(float(price_dec), 10)
        else:
            notional = ""
            price = ""

        size_value = round(float(size_dec), 10)
        timestamp_ms = block_timestamp * 1000 + decoded.log_index
        dedupe_key = _build_dedupe_key(
            tx_hash=decoded.tx_hash,
            asset=str(token_asset_id),
            side=side,
            timestamp=block_timestamp,
            price=float(price) if isinstance(price, (float, int)) else 0.0,
            size=size_value,
        )

        return {
            "market_slug": market.slug,
            "window_start_ts": str(market.window_start_ts),
            "condition_id": market.condition_id,
            "event_id": market.event_id,
            "trade_timestamp": block_timestamp,
            "trade_utc": ts_to_utc(block_timestamp),
            "price": price,
            "size": size_value,
            "notional": notional,
            "side": side,
            "outcome": outcome,
            "asset": str(token_asset_id),
            "proxy_wallet": decoded.maker,
            "transaction_hash": decoded.tx_hash,
            "dedupe_key": dedupe_key,
            "timestamp_ms": timestamp_ms,
            "server_received_ms": receive_ms,
            "trade_time_ms": ms_to_utc(timestamp_ms),
        }

    def _is_processed_block(self, block_number: int) -> bool:
        if block_number in self._processed_block_set:
            return True
        if (
            self._processed_blocks.maxlen is not None
            and len(self._processed_blocks) >= self._processed_blocks.maxlen
        ):
            old = self._processed_blocks.popleft()
            self._processed_block_set.discard(old)
        self._processed_blocks.append(block_number)
        self._processed_block_set.add(block_number)
        return False

    def _is_seen_log_uid(self, log_uid: str) -> bool:
        if log_uid in self._seen_log_uid_set:
            return True
        if (
            self._seen_log_uids.maxlen is not None
            and len(self._seen_log_uids) >= self._seen_log_uids.maxlen
        ):
            old = self._seen_log_uids.popleft()
            self._seen_log_uid_set.discard(old)
        self._seen_log_uids.append(log_uid)
        self._seen_log_uid_set.add(log_uid)
        return False

    def _shutdown(self) -> None:
        self._stop_event.set()
        logger.info("Stopping trade stream...")
        self._subscriber.stop()
        self._processor_thread.join(timeout=10)
        self._broadcaster.stop()
        self._csv.close()
        logger.info("Trade stream stopped. processed_events=%d", self._events_processed)


def _build_dedupe_key(
    tx_hash: str,
    asset: str,
    side: str,
    timestamp: int,
    price: float,
    size: float,
) -> str:
    return "|".join(
        [
            tx_hash,
            asset,
            side,
            str(timestamp),
            f"{price:.10f}",
            f"{size:.10f}",
        ]
    )
