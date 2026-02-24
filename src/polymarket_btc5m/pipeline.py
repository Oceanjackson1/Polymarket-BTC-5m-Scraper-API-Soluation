from __future__ import annotations

import csv
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import PolymarketApiClient

SERIES_SLUG = "btc-up-or-down-5m"
BTC_TAG_ID = 235
MARKET_SLUG_PATTERN = re.compile(r"^btc-updown-5m-(\d+)$")
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


def run_pipeline(config: PipelineConfig) -> None:
    _configure_logging(config.log_level)
    output_paths = _prepare_output_paths(config.output_dir)
    checkpoint = _load_checkpoint(output_paths["checkpoint"])

    client = PolymarketApiClient(
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_retries,
    )

    _run_market_index_stage(
        client=client,
        config=config,
        checkpoint=checkpoint,
        checkpoint_path=output_paths["checkpoint"],
        markets_path=output_paths["markets_csv"],
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
            record = _normalize_market_record(market)
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

    return all_trades


def _normalize_market_record(market: Any) -> dict[str, Any] | None:
    if not isinstance(market, dict):
        return None

    events = [e for e in market.get("events", []) if isinstance(e, dict)]
    in_target_series = any(event.get("seriesSlug") == SERIES_SLUG for event in events)
    if not in_target_series:
        return None

    slug = str(market.get("slug") or "")
    match = MARKET_SLUG_PATTERN.match(slug)
    if match is None:
        return None

    window_start_ts = int(match.group(1))
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
    }


def _prepare_output_paths(base_dir: Path) -> dict[str, Path]:
    indexes_dir = base_dir / "indexes"
    trades_dir = base_dir / "trades"
    reports_dir = base_dir / "reports"
    indexes_dir.mkdir(parents=True, exist_ok=True)
    trades_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    return {
        "checkpoint": base_dir / "run_state.json",
        "markets_csv": indexes_dir / "markets_btc_up_or_down_5m.csv",
        "trades_csv": trades_dir / "trades_btc_up_or_down_5m.csv",
        "validation_report": reports_dir / "validation_summary.json",
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


def ts_to_utc(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


_ts_to_utc = ts_to_utc  # backward compat


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


_configure_logging = configure_logging  # backward compat
