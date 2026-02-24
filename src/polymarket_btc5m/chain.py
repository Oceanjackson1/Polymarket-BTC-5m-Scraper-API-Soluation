from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests

CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_CTF_EXCHANGE = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
EXCHANGE_ADDRESSES = (CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE)
ORDER_FILLED_TOPIC0 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

USDC_ASSET_ID = 0
USDC_DECIMALS = 6

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecodedOrderFilledLog:
    address: str
    tx_hash: str
    block_number: int
    log_index: int
    maker: str
    taker: str
    maker_asset_id: int
    taker_asset_id: int
    maker_amount_raw: int
    taker_amount_raw: int
    fee_raw: int


def normalize_rpc_http_url(rpc_url: str) -> str:
    parsed = urlparse(rpc_url)
    scheme = parsed.scheme.lower()
    if scheme == "ws":
        return urlunparse(parsed._replace(scheme="http"))
    if scheme == "wss":
        return urlunparse(parsed._replace(scheme="https"))
    return rpc_url


def parse_hex_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if value is None:
        return 0
    text = str(value)
    try:
        if text.startswith("0x"):
            return int(text, 16)
        return int(text)
    except (TypeError, ValueError):
        return 0


def ms_to_utc(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def decode_order_filled_log(log: dict[str, Any]) -> DecodedOrderFilledLog | None:
    topics = log.get("topics", [])
    if not isinstance(topics, list) or len(topics) < 4:
        return None

    topic0 = str(topics[0]).lower()
    if topic0 != ORDER_FILLED_TOPIC0:
        return None

    data = str(log.get("data") or "")
    if not data.startswith("0x"):
        return None

    payload = data[2:]
    if len(payload) < 64 * 5:
        return None

    try:
        maker_asset_id = int(payload[0:64], 16)
        taker_asset_id = int(payload[64:128], 16)
        maker_amount_raw = int(payload[128:192], 16)
        taker_amount_raw = int(payload[192:256], 16)
        fee_raw = int(payload[256:320], 16)

        return DecodedOrderFilledLog(
            address=str(log.get("address") or "").lower(),
            tx_hash=str(log.get("transactionHash") or "").lower(),
            block_number=parse_hex_int(log.get("blockNumber")),
            log_index=parse_hex_int(log.get("logIndex")),
            maker=topic_to_address(str(topics[2])),
            taker=topic_to_address(str(topics[3])),
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount_raw=maker_amount_raw,
            taker_amount_raw=taker_amount_raw,
            fee_raw=fee_raw,
        )
    except Exception:
        return None


def topic_to_address(topic_hex: str) -> str:
    clean = topic_hex.lower().removeprefix("0x")
    if len(clean) != 64:
        raise ValueError(f"Invalid topic length: {topic_hex}")
    return "0x" + clean[-40:]


def scaled_amount(raw: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** USDC_DECIMALS)


class PolygonRpcClient:
    def __init__(
        self,
        rpc_url: str,
        timeout_seconds: int = 15,
        max_retries: int = 4,
        backoff_seconds: float = 1.0,
    ) -> None:
        self.rpc_url = normalize_rpc_http_url(rpc_url)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "polymarket-btc-updown-chain-rpc/1.1 (+https://polymarket.com)"}
        )
        self._request_id = 1

    def call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            payload = {
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params,
            }
            self._request_id += 1

            try:
                response = self.session.post(
                    self.rpc_url,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                body = response.json()
            except Exception as error:  # noqa: BLE001
                last_error = error
                self._sleep_backoff(attempt)
                continue

            if "error" in body:
                last_error = RuntimeError(f"RPC {method} error: {body['error']}")
                self._sleep_backoff(attempt)
                continue

            return body.get("result")

        raise RuntimeError(
            f"RPC call failed method={method} url={self.rpc_url} last_error={last_error}"
        )

    def get_transaction_receipt(self, tx_hash: str) -> dict[str, Any] | None:
        result = self.call("eth_getTransactionReceipt", [tx_hash])
        if isinstance(result, dict):
            return result
        return None

    def get_block_by_number(self, block_number: int) -> dict[str, Any] | None:
        result = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if isinstance(result, dict):
            return result
        return None

    def get_block_timestamp(
        self,
        block_number: int,
        cache: dict[int, int] | None = None,
    ) -> int:
        if cache is not None and block_number in cache:
            return cache[block_number]

        block = self.get_block_by_number(block_number)
        if not isinstance(block, dict):
            return 0

        timestamp = parse_hex_int(block.get("timestamp"))
        if cache is not None:
            cache[block_number] = timestamp
        return timestamp

    def get_order_filled_logs(self, block_number: int) -> list[dict[str, Any]]:
        params = [
            {
                "fromBlock": hex(block_number),
                "toBlock": hex(block_number),
                "address": list(EXCHANGE_ADDRESSES),
                "topics": [ORDER_FILLED_TOPIC0],
            }
        ]
        result = self.call("eth_getLogs", params)
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
        return []

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self.backoff_seconds * (2**attempt), 4.0)
        time.sleep(delay)
