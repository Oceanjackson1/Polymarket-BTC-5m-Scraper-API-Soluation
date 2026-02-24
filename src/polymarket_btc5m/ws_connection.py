from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable

import websocket

from .client import CLOB_WS_URL

HEARTBEAT_INTERVAL_SECONDS = 10
RECONNECT_BASE_DELAY_SECONDS = 1.0
RECONNECT_MAX_DELAY_SECONDS = 60.0

logger = logging.getLogger(__name__)


class ClobWebSocket:
    """Manages the Polymarket CLOB WebSocket with heartbeat and auto-reconnect."""

    def __init__(
        self,
        on_event: Callable[[dict[str, Any]], None],
        on_connected: Callable[[], None],
        on_disconnected: Callable[[], None],
    ) -> None:
        self._on_event = on_event
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected = threading.Event()
        self._reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        self._lock = threading.Lock()
        self._heartbeat_timer: threading.Timer | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the WebSocket in a daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._connect_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Shut down the WebSocket connection and thread."""
        self._stop_event.set()
        self._cancel_heartbeat()
        with self._lock:
            if self._ws:
                self._ws.close()
        if self._thread:
            self._thread.join(timeout=10)

    def subscribe_initial(self, token_ids: list[str]) -> None:
        """Send the initial subscription (used on connect/reconnect)."""
        if not token_ids:
            return
        msg = json.dumps({
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        })
        self._send(msg)
        logger.info("WS subscribe_initial: %d tokens", len(token_ids))

    def subscribe(self, token_ids: list[str]) -> None:
        """Incrementally subscribe to additional tokens."""
        if not token_ids:
            return
        msg = json.dumps({
            "assets_ids": token_ids,
            "operation": "subscribe",
            "custom_feature_enabled": True,
        })
        self._send(msg)
        logger.info("WS subscribe: %d tokens", len(token_ids))

    def unsubscribe(self, token_ids: list[str]) -> None:
        """Unsubscribe from tokens."""
        if not token_ids:
            return
        msg = json.dumps({
            "assets_ids": token_ids,
            "operation": "unsubscribe",
        })
        self._send(msg)
        logger.info("WS unsubscribe: %d tokens", len(token_ids))

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _send(self, msg: str) -> None:
        with self._lock:
            if self._ws:
                try:
                    self._ws.send(msg)
                except Exception:
                    logger.warning("WS send failed", exc_info=True)

    def _connect_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    CLOB_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever()
            except Exception:
                logger.exception("WS connection error")

            if self._stop_event.is_set():
                break

            logger.warning("WS reconnecting in %.1fs", self._reconnect_delay)
            self._stop_event.wait(timeout=self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                RECONNECT_MAX_DELAY_SECONDS,
            )

    def _on_open(self, ws: Any) -> None:
        logger.info("WS connected to %s", CLOB_WS_URL)
        self._reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        self._connected.set()
        self._start_heartbeat()
        try:
            self._on_connected()
        except Exception:
            logger.exception("on_connected callback error")

    def _on_message(self, ws: Any, message: str) -> None:
        if message == "PONG":
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("WS non-JSON message: %.100s", message)
            return

        events = data if isinstance(data, list) else [data]
        for event in events:
            if isinstance(event, dict):
                try:
                    self._on_event(event)
                except Exception:
                    logger.exception("Error processing WS event")

    def _on_error(self, ws: Any, error: Any) -> None:
        logger.warning("WS error: %s", error)

    def _on_close(self, ws: Any, close_status_code: Any, close_msg: Any) -> None:
        logger.info("WS closed: status=%s msg=%s", close_status_code, close_msg)
        self._connected.clear()
        self._cancel_heartbeat()
        try:
            self._on_disconnected()
        except Exception:
            logger.exception("on_disconnected callback error")

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _start_heartbeat(self) -> None:
        self._cancel_heartbeat()
        self._heartbeat_timer = threading.Timer(
            HEARTBEAT_INTERVAL_SECONDS, self._send_heartbeat
        )
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _send_heartbeat(self) -> None:
        if self._stop_event.is_set():
            return
        self._send("PING")
        self._start_heartbeat()

    def _cancel_heartbeat(self) -> None:
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None
