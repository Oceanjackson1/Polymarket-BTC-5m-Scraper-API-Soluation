#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
POLYGON_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
ORDER_FILLED_TOPIC0 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
USDC_ASSET_ID = 0
USDC_DECIMALS = 6
DEFAULT_RPCS = [
    "https://polygon.drpc.org",
    "https://polygon-rpc.com",
]

CSV_HEADERS = [
    "market_slug",
    "condition_id",
    "tx_hash",
    "log_index",
    "log_uid",
    "block_number",
    "trade_timestamp",
    "trade_utc",
    "proxy_wallet",
    "side",
    "outcome",
    "asset",
    "size",
    "price",
    "notional",
    "fee",
    "maker_asset_id",
    "taker_asset_id",
    "maker_amount_raw",
    "taker_amount_raw",
    "fee_raw",
    "rpc_url",
]


@dataclass
class RpcRange:
    start_block: int
    end_block: int
    logs_count: int


class RpcClient:
    def __init__(self, urls: list[str], timeout_seconds: int, max_retries: int) -> None:
        self.urls = urls
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.url_idx = 0
        self.session = requests.Session()

    @property
    def current_url(self) -> str:
        return self.urls[self.url_idx]

    def _rotate(self) -> None:
        self.url_idx = (self.url_idx + 1) % len(self.urls)

    def call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            try:
                response = self.session.post(
                    self.current_url,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                body = response.json()
            except Exception as error:  # noqa: BLE001
                last_error = error
                self._rotate()
                time.sleep(min(1.2 * (attempt + 1), 4))
                continue

            if "error" in body:
                message = str(body["error"])
                if _is_retryable_rpc_error(message):
                    last_error = RuntimeError(message)
                    self._rotate()
                    time.sleep(min(1.2 * (attempt + 1), 4))
                    continue
                raise RuntimeError(f"RPC error: {message}")

            return body.get("result")

        raise RuntimeError(
            f"RPC call failed after {self.max_retries} retries on method={method}. "
            f"last_error={last_error}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Strict full backfill for one Polymarket market using on-chain OrderFilled logs "
            "filtered by taker=Exchange."
        )
    )
    parser.add_argument("--slug", required=True, help="Market slug, e.g. btc-updown-5m-1771211700")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output_strict"),
        help="Output directory for CSV and validation report.",
    )
    parser.add_argument(
        "--rpc-url",
        action="append",
        dest="rpc_urls",
        help="Polygon RPC URL. Can be provided multiple times.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=12,
        help="Timeout for each RPC/HTTP request.",
    )
    parser.add_argument(
        "--max-rpc-retries",
        type=int,
        default=4,
        help="Retries per RPC call.",
    )
    parser.add_argument(
        "--initial-span-blocks",
        type=int,
        default=100,
        help="Initial block span for each eth_getLogs call.",
    )
    parser.add_argument(
        "--min-span-blocks",
        type=int,
        default=1,
        help="Minimum block span when auto-shrinking on provider limits.",
    )
    parser.add_argument(
        "--buffer-before-seconds",
        type=int,
        default=300,
        help="Seconds before createdAt to include in scan window.",
    )
    parser.add_argument(
        "--buffer-after-seconds",
        type=int,
        default=7200,
        help="Seconds after closedTime/endDate to include in scan window.",
    )
    parser.add_argument(
        "--compare-csv",
        type=Path,
        default=None,
        help="Optional existing CSV for overlap comparison.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rpc_urls = args.rpc_urls if args.rpc_urls else DEFAULT_RPCS
    rpc = RpcClient(
        urls=rpc_urls,
        timeout_seconds=args.request_timeout_seconds,
        max_retries=args.max_rpc_retries,
    )

    market = _fetch_market(args.slug, timeout_seconds=args.request_timeout_seconds)
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    outcomes = _parse_json_list(market.get("outcomes"))
    if len(token_ids) != len(outcomes):
        raise RuntimeError(
            f"token/outcome length mismatch: {len(token_ids)} != {len(outcomes)} for {args.slug}"
        )
    token_to_outcome = {int(token_ids[i]): outcomes[i] for i in range(len(token_ids))}

    created_ts = _to_unix(market.get("createdAt"))
    end_ts = max(_to_unix(market.get("closedTime")), _to_unix(market.get("endDate")))
    if created_ts == 0 or end_ts == 0:
        raise RuntimeError("Cannot resolve created/end timestamps from market metadata")

    scan_start_ts = max(1, created_ts - args.buffer_before_seconds)
    scan_end_ts = end_ts + args.buffer_after_seconds

    latest_block = int(rpc.call("eth_blockNumber", []), 16)
    block_ts_cache: dict[int, int] = {}
    start_block = _find_first_block_ge_ts(rpc, scan_start_ts, latest_block, block_ts_cache)
    end_block = _find_last_block_le_ts(rpc, scan_end_ts, latest_block, block_ts_cache)
    if end_block < start_block:
        raise RuntimeError(f"Invalid block range: {start_block}..{end_block}")

    print(
        f"[info] market={market['slug']} condition={market['conditionId']} "
        f"scan_ts={scan_start_ts}..{scan_end_ts} blocks={start_block}..{end_block}"
    )

    rows, ranges, scan_stats = _scan_market_trades(
        rpc=rpc,
        market_slug=market["slug"],
        condition_id=market["conditionId"],
        token_to_outcome=token_to_outcome,
        start_block=start_block,
        end_block=end_block,
        initial_span=args.initial_span_blocks,
        min_span=args.min_span_blocks,
        block_ts_cache=block_ts_cache,
    )

    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        deduped[row["log_uid"]] = row
    rows = sorted(
        deduped.values(),
        key=lambda x: (int(x["trade_timestamp"]), int(x["block_number"]), int(x["log_index"])),
    )

    output_dir = args.output_dir / market["slug"]
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_csv = output_dir / f"strict_trades_{market['slug']}.csv"
    validation_json = output_dir / f"strict_validation_{market['slug']}.json"

    _write_csv(trades_csv, rows, CSV_HEADERS)

    compare_report = _compare_with_existing_csv(args.compare_csv, rows)
    report = _build_validation_report(
        market=market,
        token_to_outcome=token_to_outcome,
        scan_start_ts=scan_start_ts,
        scan_end_ts=scan_end_ts,
        start_block=start_block,
        end_block=end_block,
        ranges=ranges,
        scan_stats=scan_stats,
        rows=rows,
        compare_report=compare_report,
        rpc_urls=rpc_urls,
    )
    validation_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[done] rows={} csv={} validation={}".format(
            len(rows),
            trades_csv,
            validation_json,
        )
    )


def _scan_market_trades(
    rpc: RpcClient,
    market_slug: str,
    condition_id: str,
    token_to_outcome: dict[int, str],
    start_block: int,
    end_block: int,
    initial_span: int,
    min_span: int,
    block_ts_cache: dict[int, int],
) -> tuple[list[dict[str, Any]], list[RpcRange], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    ranges: list[RpcRange] = []
    span = max(1, initial_span)
    current = start_block
    exchange_topic = "0x" + "0" * 24 + POLYGON_EXCHANGE[2:]

    total_logs_scanned = 0
    matched_logs = 0

    started_at = time.time()
    while current <= end_block:
        to_block = min(current + span - 1, end_block)
        params = [
            {
                "address": POLYGON_EXCHANGE,
                "fromBlock": hex(current),
                "toBlock": hex(to_block),
                "topics": [ORDER_FILLED_TOPIC0, None, None, exchange_topic],
            }
        ]

        try:
            logs = rpc.call("eth_getLogs", params)
            if not isinstance(logs, list):
                raise RuntimeError(f"Unexpected eth_getLogs response type: {type(logs)}")
        except Exception as error:  # noqa: BLE001
            message = str(error).lower()
            if _is_span_too_wide_error(message) and span > min_span:
                span = max(min_span, span // 2)
                continue
            raise

        total_logs_scanned += len(logs)
        ranges.append(RpcRange(start_block=current, end_block=to_block, logs_count=len(logs)))

        for log in logs:
            row = _normalize_trade_from_log(
                log=log,
                market_slug=market_slug,
                condition_id=condition_id,
                token_to_outcome=token_to_outcome,
                rpc=rpc,
                block_ts_cache=block_ts_cache,
            )
            if row is None:
                continue
            matched_logs += 1
            rows.append(row)

        current = to_block + 1

        # If the provider forced us to shrink earlier, recover span on lighter slices.
        if span < initial_span and len(logs) < 2000:
            span = min(initial_span, span * 2)

        if len(ranges) % 20 == 0:
            elapsed = round(time.time() - started_at, 1)
            print(
                f"[scan] ranges={len(ranges)} block={to_block}/{end_block} "
                f"orderfilled_taker_logs={total_logs_scanned} matched_market_logs={matched_logs} "
                f"span={span} elapsed_s={elapsed}"
            )

    return rows, ranges, {
        "orderfilled_taker_logs_scanned": total_logs_scanned,
        "matched_market_logs": matched_logs,
    }


def _normalize_trade_from_log(
    log: dict[str, Any],
    market_slug: str,
    condition_id: str,
    token_to_outcome: dict[int, str],
    rpc: RpcClient,
    block_ts_cache: dict[int, int],
) -> dict[str, Any] | None:
    topics = log.get("topics", [])
    if not isinstance(topics, list) or len(topics) < 4:
        return None

    try:
        maker = _topic_to_address(str(topics[2]))
        taker = _topic_to_address(str(topics[3]))
    except Exception:  # noqa: BLE001
        return None

    if taker != POLYGON_EXCHANGE:
        return None

    data = str(log.get("data", ""))
    if not data.startswith("0x"):
        return None
    body = data[2:]
    if len(body) < 64 * 5:
        return None

    maker_asset_id = int(body[0:64], 16)
    taker_asset_id = int(body[64:128], 16)
    maker_amount_raw = int(body[128:192], 16)
    taker_amount_raw = int(body[192:256], 16)
    fee_raw = int(body[256:320], 16)

    side = None
    token_id = None
    size_raw = 0
    notional_raw = 0

    if maker_asset_id in token_to_outcome and taker_asset_id == USDC_ASSET_ID:
        side = "SELL"
        token_id = maker_asset_id
        size_raw = maker_amount_raw
        notional_raw = taker_amount_raw
    elif taker_asset_id in token_to_outcome and maker_asset_id == USDC_ASSET_ID:
        side = "BUY"
        token_id = taker_asset_id
        size_raw = taker_amount_raw
        notional_raw = maker_amount_raw
    else:
        return None

    block_number = int(str(log.get("blockNumber", "0x0")), 16)
    block_ts = _extract_block_timestamp(log)
    if block_ts == 0:
        if block_number not in block_ts_cache:
            block_ts_cache[block_number] = _get_block_timestamp(rpc, block_number)
        block_ts = block_ts_cache[block_number]

    size_dec = Decimal(size_raw) / (Decimal(10) ** USDC_DECIMALS)
    notional_dec = Decimal(notional_raw) / (Decimal(10) ** USDC_DECIMALS)
    price_dec = Decimal("0") if size_dec == 0 else (notional_dec / size_dec)

    tx_hash = str(log.get("transactionHash", "")).lower()
    log_index = int(str(log.get("logIndex", "0x0")), 16)

    return {
        "market_slug": market_slug,
        "condition_id": condition_id,
        "tx_hash": tx_hash,
        "log_index": log_index,
        "log_uid": f"{tx_hash}:{log_index}",
        "block_number": block_number,
        "trade_timestamp": block_ts,
        "trade_utc": _fmt_utc(block_ts),
        "proxy_wallet": maker,
        "side": side,
        "outcome": token_to_outcome[token_id],
        "asset": token_id,
        "size": _decimal_to_str(size_dec),
        "price": _decimal_to_str(price_dec),
        "notional": _decimal_to_str(notional_dec),
        "fee": _decimal_to_str(Decimal(fee_raw) / (Decimal(10) ** USDC_DECIMALS)),
        "maker_asset_id": maker_asset_id,
        "taker_asset_id": taker_asset_id,
        "maker_amount_raw": maker_amount_raw,
        "taker_amount_raw": taker_amount_raw,
        "fee_raw": fee_raw,
        "rpc_url": str(log.get("rpc_url", "")),
    }


def _fetch_market(slug: str, timeout_seconds: int) -> dict[str, Any]:
    response = requests.get(
        f"{GAMMA_BASE}/markets",
        params={"slug": slug},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"Market not found: {slug}")
    market = data[0]
    if not isinstance(market, dict):
        raise RuntimeError(f"Unexpected market payload type: {type(market)}")
    for required in ("slug", "conditionId", "clobTokenIds", "outcomes"):
        if required not in market:
            raise RuntimeError(f"Missing market field '{required}'")
    return market


def _find_first_block_ge_ts(
    rpc: RpcClient,
    target_ts: int,
    hi: int,
    cache: dict[int, int],
) -> int:
    lo = 1
    while lo < hi:
        mid = (lo + hi) // 2
        ts = _cached_block_ts(rpc, mid, cache)
        if ts < target_ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _find_last_block_le_ts(
    rpc: RpcClient,
    target_ts: int,
    hi: int,
    cache: dict[int, int],
) -> int:
    lo = 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        ts = _cached_block_ts(rpc, mid, cache)
        if ts > target_ts:
            hi = mid - 1
        else:
            lo = mid
    return lo


def _cached_block_ts(rpc: RpcClient, block_number: int, cache: dict[int, int]) -> int:
    if block_number not in cache:
        cache[block_number] = _get_block_timestamp(rpc, block_number)
    return cache[block_number]


def _get_block_timestamp(rpc: RpcClient, block_number: int) -> int:
    block = rpc.call("eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(block, dict) or "timestamp" not in block:
        raise RuntimeError(f"Unexpected block payload for block={block_number}")
    return int(str(block["timestamp"]), 16)


def _extract_block_timestamp(log: dict[str, Any]) -> int:
    ts = log.get("blockTimestamp")
    if isinstance(ts, str):
        return int(ts, 16)
    if isinstance(ts, int):
        return ts
    return 0


def _topic_to_address(topic_hex: str) -> str:
    clean = topic_hex.lower().removeprefix("0x")
    if len(clean) != 64:
        raise RuntimeError(f"Invalid topic: {topic_hex}")
    return "0x" + clean[-40:]


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        return [value]
    return []


def _to_unix(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return 0


def _fmt_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _decimal_to_str(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".")


def _write_csv(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _compare_with_existing_csv(
    csv_path: Path | None,
    new_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if csv_path is None:
        return None
    if not csv_path.exists():
        return {
            "path": str(csv_path),
            "exists": False,
            "error": "file_not_found",
        }

    old_rows = 0
    old_tx: set[str] = set()
    old_ts: list[int] = []
    with csv_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            old_rows += 1
            tx = str(row.get("transaction_hash") or row.get("tx_hash") or "").lower()
            if tx:
                old_tx.add(tx)
            ts_raw = row.get("trade_timestamp") or row.get("timestamp")
            try:
                if ts_raw is not None:
                    old_ts.append(int(float(str(ts_raw))))
            except ValueError:
                pass

    new_tx = {str(row["tx_hash"]).lower() for row in new_rows if row.get("tx_hash")}
    new_ts = [int(row["trade_timestamp"]) for row in new_rows]

    return {
        "path": str(csv_path),
        "exists": True,
        "rows_existing": old_rows,
        "rows_new": len(new_rows),
        "unique_tx_existing": len(old_tx),
        "unique_tx_new": len(new_tx),
        "tx_only_in_new": len(new_tx - old_tx),
        "tx_only_in_existing": len(old_tx - new_tx),
        "existing_first_ts": min(old_ts) if old_ts else None,
        "existing_last_ts": max(old_ts) if old_ts else None,
        "new_first_ts": min(new_ts) if new_ts else None,
        "new_last_ts": max(new_ts) if new_ts else None,
    }


def _build_validation_report(
    market: dict[str, Any],
    token_to_outcome: dict[int, str],
    scan_start_ts: int,
    scan_end_ts: int,
    start_block: int,
    end_block: int,
    ranges: list[RpcRange],
    scan_stats: dict[str, int],
    rows: list[dict[str, Any]],
    compare_report: dict[str, Any] | None,
    rpc_urls: list[str],
) -> dict[str, Any]:
    row_uids = [row["log_uid"] for row in rows]
    row_ts = [int(row["trade_timestamp"]) for row in rows]

    continuity_ok = True
    prev_end = None
    for r in ranges:
        if prev_end is not None and r.start_block != prev_end + 1:
            continuity_ok = False
            break
        prev_end = r.end_block

    side_breakdown: dict[str, int] = {}
    outcome_breakdown: dict[str, int] = {}
    notional_sum = Decimal("0")
    for row in rows:
        side = str(row.get("side", ""))
        outcome = str(row.get("outcome", ""))
        side_breakdown[side] = side_breakdown.get(side, 0) + 1
        outcome_breakdown[outcome] = outcome_breakdown.get(outcome, 0) + 1
        notional_sum += Decimal(str(row.get("notional", "0")))

    market_volume = Decimal(str(market.get("volume") or "0"))
    double_notional = notional_sum * 2
    volume_ratio = None
    if market_volume > 0:
        volume_ratio = _decimal_to_str(double_notional / market_volume)

    return {
        "generated_at": _fmt_utc(int(time.time())),
        "market": {
            "slug": market.get("slug"),
            "condition_id": market.get("conditionId"),
            "created_at": market.get("createdAt"),
            "end_date": market.get("endDate"),
            "closed_time": market.get("closedTime"),
            "event_start_time": market.get("eventStartTime"),
            "volume": market.get("volume"),
            "volume_clob": market.get("volumeClob"),
            "token_to_outcome": {str(k): v for k, v in token_to_outcome.items()},
        },
        "rpc": {"urls": rpc_urls},
        "scan": {
            "scan_start_ts": scan_start_ts,
            "scan_start_utc": _fmt_utc(scan_start_ts),
            "scan_end_ts": scan_end_ts,
            "scan_end_utc": _fmt_utc(scan_end_ts),
            "start_block": start_block,
            "end_block": end_block,
            "ranges_total": len(ranges),
            "ranges_contiguous": continuity_ok,
            "total_blocks_scanned": sum(r.end_block - r.start_block + 1 for r in ranges),
            "orderfilled_taker_logs_scanned": scan_stats["orderfilled_taker_logs_scanned"],
            "matched_market_logs": scan_stats["matched_market_logs"],
        },
        "strict_trades": {
            "rows": len(rows),
            "unique_log_uids": len(set(row_uids)),
            "duplicates_removed": len(rows) - len(set(row_uids)),
            "unique_tx_hashes": len({row["tx_hash"] for row in rows}),
            "first_trade_ts": min(row_ts) if row_ts else None,
            "last_trade_ts": max(row_ts) if row_ts else None,
            "first_trade_utc": _fmt_utc(min(row_ts)) if row_ts else None,
            "last_trade_utc": _fmt_utc(max(row_ts)) if row_ts else None,
            "side_breakdown": side_breakdown,
            "outcome_breakdown": outcome_breakdown,
            "notional_sum": _decimal_to_str(notional_sum),
            "double_notional_sum": _decimal_to_str(double_notional),
            "double_notional_vs_market_volume_ratio": volume_ratio,
        },
        "compare_with_existing_csv": compare_report,
    }


def _is_retryable_rpc_error(message: str) -> bool:
    msg = message.lower()
    keywords = [
        "timeout",
        "timed out",
        "too many requests",
        "429",
        "connection reset",
        "service unavailable",
        "gateway timeout",
        "internal error",
        "unauthorized",
        "forbidden",
        "api key",
    ]
    return any(k in msg for k in keywords)


def _is_span_too_wide_error(message: str) -> bool:
    msg = message.lower()
    keywords = [
        "block range is too large",
        "too many results",
        "query timeout",
        "response size exceeded",
        "limit exceeded",
        "more than",
    ]
    return any(k in msg for k in keywords)


if __name__ == "__main__":
    main()
