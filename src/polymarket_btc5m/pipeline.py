from __future__ import annotations

import csv
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import PolymarketApiClient
from .chain import (
    EXCHANGE_ADDRESSES,
    PolygonRpcClient,
    decode_order_filled_log,
)
from .timeframes import (
    DEFAULT_TIMEFRAMES,
    match_btc_updown_market,
    normalize_timeframes,
    timeframe_file_suffix,
)

BTC_TAG_ID = 235
MARKET_PAGE_LIMIT = 500
TRADE_PAGE_LIMIT = 1000

MARKETS_HEADERS = [
    "market_id",
    "event_id",
    "condition_id",
    "slug",
    "clob_token_ids",
    "outcomes",
    "window_start_ts",
    "window_start_utc",
    "market_end_utc",
    "volume",
    "market_url",
    "market_url_zh",
]

TRADES_HEADERS = [
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


@dataclass
class PipelineConfig:
    output_dir: Path
    resume: bool = True
    include_zero_volume: bool = False
    market_limit: int | None = None
    request_delay_seconds: float = 0.10
    timeout_seconds: int = 30
    max_retries: int = 5
    log_level: str = "INFO"
    rpc_url: str | None = None
    timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES


class TradeTimestampEnricher:
    """Optionally enrich API trades with log-index based millisecond timestamps."""

    def __init__(self, rpc_url: str, timeout_seconds: int, max_retries: int) -> None:
        self._rpc = PolygonRpcClient(
            rpc_url=rpc_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        self._block_ts_cache: dict[int, int] = {}
        self._receipt_cache: dict[str, dict[str, Any] | None] = {}
        self._usage_counter: dict[tuple[str, str], int] = defaultdict(int)

    def enrich_rows(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            tx_hash = str(row.get("transaction_hash") or "").lower()
            asset = str(row.get("asset") or "")
            if not tx_hash or not asset:
                continue

            parsed = self._get_parsed_receipt(tx_hash)
            if parsed is None:
                continue

            log_indexes = parsed["asset_to_log_indexes"].get(asset)
            if not log_indexes:
                continue

            usage_key = (tx_hash, asset)
            usage_index = self._usage_counter[usage_key]
            log_index = (
                log_indexes[usage_index]
                if usage_index < len(log_indexes)
                else log_indexes[-1]
            )
            self._usage_counter[usage_key] += 1

            block_ts = int(parsed["block_timestamp"])
            timestamp_ms = block_ts * 1000 + int(log_index)
            row["timestamp_ms"] = timestamp_ms
            row["server_received_ms"] = ""
            row["trade_time_ms"] = _ms_to_utc(timestamp_ms)

    def _get_parsed_receipt(self, tx_hash: str) -> dict[str, Any] | None:
        if tx_hash in self._receipt_cache:
            return self._receipt_cache[tx_hash]

        receipt = self._rpc.get_transaction_receipt(tx_hash)
        if not isinstance(receipt, dict):
            self._receipt_cache[tx_hash] = None
            return None

        block_number = _parse_rpc_int(receipt.get("blockNumber"))
        block_timestamp = self._rpc.get_block_timestamp(block_number, self._block_ts_cache)
        if block_timestamp == 0:
            self._receipt_cache[tx_hash] = None
            return None

        asset_to_log_indexes: dict[str, list[int]] = defaultdict(list)
        logs = receipt.get("logs", [])
        if isinstance(logs, list):
            for log in logs:
                if not isinstance(log, dict):
                    continue
                decoded = decode_order_filled_log(log)
                if decoded is None:
                    continue
                if decoded.address not in EXCHANGE_ADDRESSES:
                    continue
                # Include both sides so matching can work for BUY/SELL rows.
                asset_to_log_indexes[str(decoded.maker_asset_id)].append(decoded.log_index)
                asset_to_log_indexes[str(decoded.taker_asset_id)].append(decoded.log_index)

        for asset in asset_to_log_indexes:
            asset_to_log_indexes[asset].sort()

        parsed = {
            "block_timestamp": block_timestamp,
            "asset_to_log_indexes": dict(asset_to_log_indexes),
        }
        self._receipt_cache[tx_hash] = parsed
        return parsed


def run_pipeline(config: PipelineConfig) -> None:
    _configure_logging(config.log_level)
    enabled_timeframes = normalize_timeframes(config.timeframes)
    output_paths = _prepare_output_paths(config.output_dir, enabled_timeframes)
    checkpoint = _load_checkpoint(output_paths["checkpoint"])
    logging.info("Pipeline timeframes=%s", ",".join(enabled_timeframes))

    client = PolymarketApiClient(
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_retries,
    )
    receipt_enricher: TradeTimestampEnricher | None = None
    if config.rpc_url:
        receipt_enricher = TradeTimestampEnricher(
            rpc_url=config.rpc_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
        logging.info("Stage 2 timestamp enrichment enabled via rpc_url=%s", config.rpc_url)
    else:
        logging.info("Stage 2 timestamp enrichment disabled (no rpc_url provided).")

    _run_market_index_stage(
        client=client,
        config=config,
        checkpoint=checkpoint,
        checkpoint_path=output_paths["checkpoint"],
        markets_path=output_paths["markets_csv"],
        enabled_timeframes=enabled_timeframes,
    )

    markets = _read_csv(output_paths["markets_csv"])
    if not markets:
        raise RuntimeError("Market index is empty. Stop to avoid running trades on no input.")

    _run_trades_stage(
        client=client,
        config=config,
        checkpoint=checkpoint,
        checkpoint_path=output_paths["checkpoint"],
        markets=markets,
        trades_path=output_paths["trades_csv"],
        receipt_enricher=receipt_enricher,
    )

    _run_validation_stage(
        checkpoint=checkpoint,
        checkpoint_path=output_paths["checkpoint"],
        markets=markets,
        trades_path=output_paths["trades_csv"],
        report_path=output_paths["validation_report"],
    )


def _run_market_index_stage(
    client: PolymarketApiClient,
    config: PipelineConfig,
    checkpoint: dict[str, Any],
    checkpoint_path: Path,
    markets_path: Path,
    enabled_timeframes: tuple[str, ...],
) -> None:
    state = checkpoint["market_index"]
    if not config.resume:
        state.update(
            {
                "completed": False,
                "next_offset": 0,
                "pages_fetched": 0,
                "records_written": 0,
            }
        )
    if state.get("completed") and markets_path.exists():
        logging.info("Stage 1 skipped: market index already completed.")
        return

    next_offset = int(state.get("next_offset", 0))
    if next_offset == 0 or not markets_path.exists():
        _write_csv_header(markets_path, MARKETS_HEADERS)
        next_offset = 0
        state["next_offset"] = 0
        state["pages_fetched"] = 0
        state["records_written"] = 0

    logging.info("Stage 1 start: indexing markets from offset=%s", next_offset)
    while True:
        params = {
            "tag_id": BTC_TAG_ID,
            "closed": "true",
            "limit": MARKET_PAGE_LIMIT,
            "offset": next_offset,
            "order": "id",
            "ascending": "false",
        }
        page = client.get_gamma("/markets", params=params)
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected /markets response type: {type(page)}")
        if not page:
            break

        records = []
        for market in page:
            record = _normalize_market_record(market, enabled_timeframes)
            if record is not None:
                records.append(record)

        _append_csv(markets_path, records, MARKETS_HEADERS)

        next_offset += len(page)
        state["next_offset"] = next_offset
        state["pages_fetched"] = int(state.get("pages_fetched", 0)) + 1
        state["records_written"] = int(state.get("records_written", 0)) + len(records)
        _save_checkpoint(checkpoint_path, checkpoint)

        logging.info(
            "Stage 1 progress: pages=%s records=%s next_offset=%s",
            state["pages_fetched"],
            state["records_written"],
            next_offset,
        )

        if len(page) < MARKET_PAGE_LIMIT:
            break
        time.sleep(config.request_delay_seconds)

    state["completed"] = True
    state["completed_at"] = _utc_now()
    _save_checkpoint(checkpoint_path, checkpoint)
    logging.info(
        "Stage 1 done: markets records=%s file=%s",
        state["records_written"],
        markets_path,
    )


def _run_trades_stage(
    client: PolymarketApiClient,
    config: PipelineConfig,
    checkpoint: dict[str, Any],
    checkpoint_path: Path,
    markets: list[dict[str, Any]],
    trades_path: Path,
    receipt_enricher: TradeTimestampEnricher | None,
) -> None:
    state = checkpoint["trades_backfill"]
    selected_markets = [
        market
        for market in sorted(markets, key=lambda x: int(x["window_start_ts"]))
        if config.include_zero_volume or _safe_float(market.get("volume"), 0.0) > 0
    ]
    if config.market_limit is not None:
        selected_markets = selected_markets[: config.market_limit]

    if not config.resume:
        state.update(
            {
                "completed": False,
                "next_market_index": 0,
                "markets_processed": 0,
                "trades_written": 0,
            }
        )

    if state.get("completed") and trades_path.exists():
        logging.info("Stage 2 skipped: trades backfill already completed.")
        return

    next_market_index = int(state.get("next_market_index", 0))
    if next_market_index == 0 or not trades_path.exists():
        _write_csv_header(trades_path, TRADES_HEADERS)
        next_market_index = 0
        state["next_market_index"] = 0
        state["markets_processed"] = 0
        state["trades_written"] = 0

    logging.info(
        "Stage 2 start: markets_to_process=%s start_index=%s",
        len(selected_markets),
        next_market_index,
    )
    for index in range(next_market_index, len(selected_markets)):
        market = selected_markets[index]
        trades = _fetch_all_trades_for_market(
            client=client,
            market=market,
            request_delay_seconds=config.request_delay_seconds,
            receipt_enricher=receipt_enricher,
        )
        _append_csv(trades_path, trades, TRADES_HEADERS)

        state["next_market_index"] = index + 1
        state["markets_processed"] = index + 1
        state["trades_written"] = int(state.get("trades_written", 0)) + len(trades)
        state["last_market_slug"] = market["slug"]
        _save_checkpoint(checkpoint_path, checkpoint)

        logging.info(
            "Stage 2 progress: market=%s (%s/%s) trades_in_market=%s total_trades=%s",
            market["slug"],
            index + 1,
            len(selected_markets),
            len(trades),
            state["trades_written"],
        )
        time.sleep(config.request_delay_seconds)

    state["completed"] = True
    state["completed_at"] = _utc_now()
    _save_checkpoint(checkpoint_path, checkpoint)
    logging.info("Stage 2 done: total trades=%s file=%s", state["trades_written"], trades_path)


def _run_validation_stage(
    checkpoint: dict[str, Any],
    checkpoint_path: Path,
    markets: list[dict[str, Any]],
    trades_path: Path,
    report_path: Path,
) -> None:
    state = checkpoint["validation"]
    trades_rows = _read_csv(trades_path) if trades_path.exists() else []
    traded_markets = {row.get("market_slug", "") for row in trades_rows}

    report = {
        "generated_at": _utc_now(),
        "markets_total": len(markets),
        "markets_with_volume_gt_zero": sum(
            1 for m in markets if _safe_float(m.get("volume"), 0.0) > 0
        ),
        "trades_total": len(trades_rows),
        "markets_with_trades": len(traded_markets),
        "markets_without_trades": max(len(markets) - len(traded_markets), 0),
        "trades_file": str(trades_path),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    state["completed"] = True
    state["completed_at"] = _utc_now()
    state["report_path"] = str(report_path)
    _save_checkpoint(checkpoint_path, checkpoint)
    logging.info("Stage 4 done: validation report=%s", report_path)


def _fetch_all_trades_for_market(
    client: PolymarketApiClient,
    market: dict[str, Any],
    request_delay_seconds: float,
    receipt_enricher: TradeTimestampEnricher | None,
) -> list[dict[str, Any]]:
    all_trades: list[dict[str, Any]] = []
    offset = 0

    while True:
        params = {"market": market["condition_id"], "limit": TRADE_PAGE_LIMIT, "offset": offset}
        page = client.get_data("/trades", params=params)
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected /trades response type: {type(page)}")
        if not page:
            break

        for raw_trade in page:
            normalized = _normalize_trade_record(raw_trade, market)
            if normalized is not None:
                all_trades.append(normalized)

        if len(page) < TRADE_PAGE_LIMIT:
            break
        offset += len(page)
        time.sleep(request_delay_seconds)

    if receipt_enricher is not None and all_trades:
        receipt_enricher.enrich_rows(all_trades)

    return all_trades


def _normalize_market_record(
    market: Any,
    enabled_timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES,
) -> dict[str, Any] | None:
    if not isinstance(market, dict):
        return None

    match = match_btc_updown_market(market, enabled_timeframes)
    if match is None:
        return None

    _, window_start_ts = match
    slug = str(market.get("slug") or "")
    events = [e for e in market.get("events", []) if isinstance(e, dict)]
    event_id = str(events[0].get("id", "")) if events else ""
    condition_id = str(market.get("conditionId") or "")
    if not condition_id:
        return None

    return {
        "market_id": str(market.get("id") or ""),
        "event_id": event_id,
        "condition_id": condition_id,
        "slug": slug,
        "clob_token_ids": str(market.get("clobTokenIds") or ""),
        "outcomes": str(market.get("outcomes") or ""),
        "window_start_ts": window_start_ts,
        "window_start_utc": _ts_to_utc(window_start_ts),
        "market_end_utc": str(market.get("endDate") or ""),
        "volume": _safe_float(market.get("volume"), 0.0),
        "market_url": f"https://polymarket.com/event/{slug}",
        "market_url_zh": f"https://polymarket.com/zh/event/{slug}",
    }


def _normalize_trade_record(raw_trade: Any, market: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw_trade, dict):
        return None

    timestamp = raw_trade.get("timestamp")
    timestamp_int = int(timestamp) if timestamp is not None else 0
    size = _safe_float(raw_trade.get("size"), 0.0)
    price = _safe_float(raw_trade.get("price"), 0.0)
    notional = size * price

    transaction_hash = str(raw_trade.get("transactionHash") or "")
    side = str(raw_trade.get("side") or "")
    asset = str(raw_trade.get("asset") or "")
    dedupe_key = "|".join(
        [
            transaction_hash,
            asset,
            side,
            str(timestamp_int),
            f"{price:.10f}",
            f"{size:.10f}",
        ]
    )

    return {
        "market_slug": str(market["slug"]),
        "window_start_ts": str(market["window_start_ts"]),
        "condition_id": str(market["condition_id"]),
        "event_id": str(market["event_id"]),
        "trade_timestamp": timestamp_int,
        "trade_utc": _ts_to_utc(timestamp_int) if timestamp_int else "",
        "price": price,
        "size": size,
        "notional": round(notional, 10),
        "side": side,
        "outcome": str(raw_trade.get("outcome") or ""),
        "asset": asset,
        "proxy_wallet": str(raw_trade.get("proxyWallet") or ""),
        "transaction_hash": transaction_hash,
        "dedupe_key": dedupe_key,
        "timestamp_ms": "",
        "server_received_ms": "",
        "trade_time_ms": "",
    }


def _prepare_output_paths(base_dir: Path, enabled_timeframes: tuple[str, ...]) -> dict[str, Path]:
    indexes_dir = base_dir / "indexes"
    trades_dir = base_dir / "trades"
    reports_dir = base_dir / "reports"
    indexes_dir.mkdir(parents=True, exist_ok=True)
    trades_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    suffix = timeframe_file_suffix(enabled_timeframes)
    if suffix == "5m":
        checkpoint_name = "run_state.json"
        markets_name = "markets_btc_up_or_down_5m.csv"
        trades_name = "trades_btc_up_or_down_5m.csv"
        report_name = "validation_summary.json"
    else:
        checkpoint_name = f"run_state_{suffix}.json"
        markets_name = f"markets_btc_up_or_down_{suffix}.csv"
        trades_name = f"trades_btc_up_or_down_{suffix}.csv"
        report_name = f"validation_summary_{suffix}.json"

    return {
        "checkpoint": base_dir / checkpoint_name,
        "markets_csv": indexes_dir / markets_name,
        "trades_csv": trades_dir / trades_name,
        "validation_report": reports_dir / report_name,
    }


def _load_checkpoint(path: Path) -> dict[str, Any]:
    default_state: dict[str, Any] = {
        "market_index": {
            "completed": False,
            "next_offset": 0,
            "pages_fetched": 0,
            "records_written": 0,
        },
        "trades_backfill": {
            "completed": False,
            "next_market_index": 0,
            "markets_processed": 0,
            "trades_written": 0,
        },
        "validation": {"completed": False},
    }
    if not path.exists():
        return default_state

    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return default_state
    for key, value in default_state.items():
        if key not in loaded:
            loaded[key] = value
    return loaded


def _save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv_header(path: Path, headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()


def _append_csv(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    if not rows:
        return
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_rpc_int(value: Any) -> int:
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


def ts_to_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


_ts_to_utc = ts_to_utc  # backward compat


def _ms_to_utc(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


_configure_logging = configure_logging  # backward compat
