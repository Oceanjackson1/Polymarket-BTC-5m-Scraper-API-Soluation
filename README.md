# Polymarket BTC Up/Down Multi-Timeframe Trade Scraper

A comprehensive data collection toolkit for **Polymarket BTC prediction markets**, supporting historical backfill, single-market extraction, real-time blockchain streaming, and live order book recording across **4 timeframes** (`5m`, `15m`, `1h`, `4h`).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Supported Markets](#supported-markets)
- [Data Sources & APIs](#data-sources--apis)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Batch Historical Backfill](#1-batch-historical-backfill)
  - [Single Market Fetch](#2-single-market-fetch)
  - [Real-Time Streaming](#3-real-time-streaming)
  - [Order Book Recorder](#4-order-book-recorder)
- [CLI Reference](#cli-reference)
- [Output Schema](#output-schema)
  - [Trades CSV (Realized Data)](#1-trades-csv-realized-data)
  - [Book Snapshots CSV (L2 Order Book)](#2-book-snapshots-csv-l2-order-book)
  - [Price Changes CSV](#3-price-changes-csv)
  - [WS Trades CSV](#4-ws-trades-csv)
  - [Markets Index CSV](#5-markets-index-csv)
- [Output Directory Structure](#output-directory-structure)
- [Millisecond Timestamp Enrichment](#millisecond-timestamp-enrichment)
- [On-Chain Event Monitoring](#on-chain-event-monitoring)
- [Key Concepts](#key-concepts)
- [Project Structure](#project-structure)
- [Module Reference](#module-reference)
- [Validation Tests](#validation-tests)
- [FAQ](#faq)
- [Disclaimer](#disclaimer)

---

## Overview

Polymarket runs BTC Up/Down prediction markets where traders bet on whether Bitcoin's price will go up or down within a fixed time window. These markets operate on the **Polygon blockchain** and are accessible via Polymarket's APIs and CLOB (Central Limit Order Book) WebSocket.

This project provides **5 data collection modes**:

| Mode | Entry Point | Data Source | Data Type | Retroactive? |
|------|-------------|-------------|-----------|:------------:|
| **Batch Backfill** | `main.py` | Data API | Historical trades | Yes |
| **Single Market** | `fetch_single_market.py` | Data API | Historical trades | Yes |
| **Real-Time Stream** | `stream.py` | Polygon RPC | Live trades | No |
| **Order Book Recorder** | `scripts/run_recorder.py` | CLOB WebSocket | L2 order book + trades | No |
| **Strict On-Chain Export** | `scripts/export_market_trades_chain.py` | Polygon RPC | On-chain trades | Yes |

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              Polymarket Platform             │
                    ├──────────┬──────────┬───────────────────────┤
                    │ Gamma API│ Data API │     CLOB WebSocket    │
                    │ (市场索引)│ (历史成交) │  (实时订单簿 & 成交)   │
                    └────┬─────┴────┬─────┴──────────┬────────────┘
                         │          │                 │
                    ┌────▼──────────▼────┐   ┌───────▼──────────┐
                    │  main.py           │   │ run_recorder.py   │
                    │  fetch_single.py   │   │  (WS subscriber)  │
                    │  (HTTP pagination) │   └───────┬──────────┘
                    └────────┬──────────┘            │
                             │                       │
    ┌────────────────────────┤               ┌───────▼──────────┐
    │ Polygon Blockchain     │               │ Order Book CSVs  │
    │ (OrderFilled events)   │               │ - book_snapshots  │
    │         │              │               │ - price_changes   │
    │    ┌────▼────┐         │               │ - ws_trades       │
    │    │stream.py│         │               └──────────────────┘
    │    └────┬────┘         │
    │         │              │
    │    ┌────▼──────────────▼──┐
    │    │     Trade CSVs       │
    │    │  (18 fields/row)     │
    │    └──────────────────────┘
    └────────────────────────────
```

---

## Supported Markets

### Polymarket Pages

| Timeframe | Market Page | Slug Pattern |
|-----------|------------|--------------|
| **5 min** | https://polymarket.com/crypto/5M | `btc-updown-5m-{timestamp}` |
| **15 min** | https://polymarket.com/crypto/15M | `btc-updown-15m-{timestamp}` |
| **1 hour** | https://polymarket.com/crypto/hourly | `btc-updown-1h-{timestamp}` |
| **4 hour** | https://polymarket.com/crypto/4hour | `btc-updown-4h-{timestamp}` |

### Market Identification

- **Slug pattern**: `btc-updown-<timeframe>-<window_start_ts>`
- **Series slug**: `btc-up-or-down-<timeframe>`
- **Tag ID**: `235` (BTC markets on Polymarket)
- **Timeframe aliases**: `hourly`/`60m` → `1h`, `4hour`/`240m` → `4h`

Each market has **two outcomes**: `Up` (BTC price goes up) and `Down` (BTC price goes down), each represented by a separate on-chain token (asset).

---

## Data Sources & APIs

| Source | Base URL | Purpose | Auth Required |
|--------|----------|---------|:-------------:|
| **Gamma API** | `https://gamma-api.polymarket.com` | Market discovery & indexing | No |
| **Data API** | `https://data-api.polymarket.com` | Historical trade records | No |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Real-time order book & trades | No |
| **Polygon RPC** | User-provided (e.g., Alchemy, Infura) | Blockchain events & timestamps | Depends |

### API Endpoints Used

```
GET /markets?tag_id=235&closed=false&active=true    # Gamma: discover markets
GET /trades?market={condition_id}&limit=1000         # Data: paginate trades
WSS /ws/market                                       # CLOB: real-time events
eth_subscribe("newHeads")                            # Polygon: new blocks
eth_getLogs(topics=[OrderFilled])                     # Polygon: trade events
eth_getTransactionReceipt(tx_hash)                   # Polygon: tx details
```

---

## Installation

### Requirements

- **Python 3.9+**
- **pip** (Python package manager)

### Setup

```bash
git clone https://github.com/Oceanjackson1/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper.git
cd Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper

# Create virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | ≥2.32.0 | HTTP client for Gamma/Data APIs |
| `websocket-client` | ≥1.7.0 | CLOB WebSocket connection |
| `websockets` | ≥12.0 | Local WebSocket broadcast server (stream mode) |

---

## Quick Start

### 1. Batch Historical Backfill

Fetch all historical trades from closed BTC markets across all timeframes:

```bash
# All 4 timeframes (default)
python3 main.py --output-dir output

# Only 5-minute markets
python3 main.py --timeframes 5m --output-dir output

# With millisecond timestamp enrichment via RPC
python3 main.py --timeframes 5m,15m,1h,4h \
  --rpc-url https://polygon-rpc.com \
  --output-dir output
```

The pipeline runs in 3 stages:
1. **Market Index** — Discovers all closed BTC markets from Gamma API
2. **Trade Backfill** — Paginates Data API `/trades` for each market
3. **Validation** — Generates coverage/quality report

Supports **checkpoint-based resumption**: safe to interrupt and restart.

### 2. Single Market Fetch

Extract all trades for a specific market:

```bash
python3 fetch_single_market.py \
  --slug btc-updown-5m-1772089200 \
  --output-dir output/trades

# With millisecond enrichment
python3 fetch_single_market.py \
  --slug btc-updown-4h-1772082000 \
  --rpc-url https://polygon-rpc.com \
  --output-dir output/trades
```

Output: `output/trades/trades_{slug}.csv`

### 3. Real-Time Streaming

Monitor live trades by subscribing to Polygon blockchain events:

```bash
python3 stream.py \
  --rpc-url wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --timeframes 5m,15m,1h,4h \
  --output-dir output/trades
```

This will:
- Subscribe to Polygon `newHeads` (new blocks)
- Decode `OrderFilled` events from CTF Exchange contracts
- Write trades to CSV in real-time
- Broadcast JSON via local WebSocket at `ws://127.0.0.1:8765`

**Requires a Polygon WebSocket RPC endpoint** (Alchemy, Infura, QuickNode, etc.)

### 4. Order Book Recorder

Record real-time L2 order book snapshots, price changes, and trades:

```bash
python3 scripts/run_recorder.py \
  --output-dir output \
  --poll-interval 30 \
  --grace-period 120
```

This will:
- Auto-discover all active BTC markets via Gamma API
- Subscribe to CLOB WebSocket for real-time events
- Write 3 CSV files per market: `book_snapshots`, `price_changes`, `trades`
- Output to `output/orderbook/{market_slug}/`

**No RPC required** — uses Polymarket's CLOB WebSocket directly.

---

## CLI Reference

### `main.py` — Batch Pipeline

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-dir` | `output/` | Base output directory |
| `--timeframes` | `5m,15m,1h,4h` | Comma-separated timeframes |
| `--rpc-url` | _(none)_ | Polygon RPC URL for ms timestamp enrichment |
| `--market-limit` | _(all)_ | Limit markets processed (for testing) |
| `--no-resume` | `false` | Ignore checkpoint, start fresh |
| `--include-zero-volume` | `false` | Include zero-volume markets |
| `--request-delay-seconds` | `0.10` | Delay between API requests |
| `--timeout-seconds` | `30` | HTTP timeout |
| `--max-retries` | `5` | Max retries on failure |
| `--log-level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

### `fetch_single_market.py` — Single Market

| Argument | Default | Description |
|----------|---------|-------------|
| `--slug` | _(required)_ | Market slug, e.g. `btc-updown-5m-1772089200` |
| `--output-dir` | `output/trades/` | Output directory |
| `--rpc-url` | _(none)_ | Polygon RPC URL for ms enrichment |
| `--timeframes` | `5m,15m,1h,4h` | For slug validation |
| `--request-delay-seconds` | `0.10` | Delay between API requests |
| `--timeout-seconds` | `30` | HTTP timeout |
| `--max-retries` | `5` | Max retries on failure |
| `--log-level` | `INFO` | Log level |

### `stream.py` — Real-Time Stream

| Argument | Default | Description |
|----------|---------|-------------|
| `--rpc-url` | _(required)_ | Polygon WebSocket RPC endpoint |
| `--output-dir` | `output/trades/` | Output directory |
| `--timeframes` | `5m,15m,1h,4h` | Timeframes to monitor |
| `--market-poll-interval` | `300.0` | Seconds between market discovery polls |
| `--ws-host` | `127.0.0.1` | Local WebSocket broadcast host |
| `--ws-port` | `8765` | Local WebSocket broadcast port |
| `--grace-period` | `120.0` | Seconds to keep tracking after market end |
| `--timeout-seconds` | `30` | HTTP timeout |
| `--max-retries` | `5` | Max retries |
| `--log-level` | `INFO` | Log level |

### `scripts/run_recorder.py` — Order Book Recorder

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-dir` | `output/` | Base output directory |
| `--poll-interval` | `30.0` | Market discovery poll interval (seconds) |
| `--grace-period` | `120.0` | Seconds to keep recording after market end |
| `--timeout-seconds` | `30` | HTTP timeout |
| `--max-retries` | `5` | Max retries |
| `--log-level` | `INFO` | Log level |

### `scripts/export_market_trades_chain.py` — Strict On-Chain Export

| Argument | Default | Description |
|----------|---------|-------------|
| `--slug` | _(required)_ | Market slug |
| `--output-dir` | `output_strict/` | Output directory |
| `--rpc-url` | _(repeatable)_ | One or more Polygon RPC URLs |
| `--request-timeout-seconds` | `12` | RPC timeout |
| `--max-rpc-retries` | `4` | RPC retries |
| `--initial-span-blocks` | `100` | Initial block range for scanning |
| `--min-span-blocks` | `1` | Minimum block range |
| `--buffer-before-seconds` | `300` | Scan buffer before market start |
| `--buffer-after-seconds` | `7200` | Scan buffer after market end |
| `--compare-csv` | _(none)_ | CSV file for overlap analysis |

---

## Output Schema

### 1. Trades CSV (Realized Data)

**Source**: Data API (historical) or Polygon RPC (real-time)
**File pattern**: `trades/trades_btc_up_or_down_{suffix}.csv` or `trades/trades_{slug}.csv`

Each row = one **confirmed on-chain trade**.

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `market_slug` | string | Market identifier, e.g. `btc-updown-5m-1772089200` |
| 2 | `window_start_ts` | int | Market window start (Unix seconds) |
| 3 | `condition_id` | string | Market condition contract address (0x hash) |
| 4 | `event_id` | int | Polymarket platform event number |
| 5 | `trade_timestamp` | int | Trade time (Unix seconds) |
| 6 | `trade_utc` | string | Trade time in UTC ISO 8601 format |
| 7 | `price` | float | Trade price [0, 1], representing probability |
| 8 | `size` | float | Contract quantity |
| 9 | `notional` | float | Notional value in USDC = `price × size` |
| 10 | `side` | string | `BUY` or `SELL` (taker direction) |
| 11 | `outcome` | string | `Up` or `Down` |
| 12 | `asset` | string | Token ID (256-bit integer) |
| 13 | `proxy_wallet` | string | Trader's proxy wallet address |
| 14 | `transaction_hash` | string | On-chain tx hash (verifiable on Polygonscan) |
| 15 | `dedupe_key` | string | Composite deduplication key |
| 16 | `timestamp_ms` | int/empty | Millisecond timestamp (RPC enrichment only) |
| 17 | `server_received_ms` | int/empty | Server receive time in ms (stream mode only) |
| 18 | `trade_time_ms` | string/empty | ISO 8601 with ms precision (RPC enrichment only) |

### 2. Book Snapshots CSV (L2 Order Book)

**Source**: CLOB WebSocket `book` events
**File pattern**: `orderbook/{slug}/book_snapshots_{slug}.csv`

Each row = one **price level** in an order book snapshot. All rows with the same `snapshot_seq` form a complete L2 order book state.

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `snapshot_seq` | int | Snapshot sequence number (incremental) |
| 2 | `server_ts_ms` | int | Server-side millisecond timestamp |
| 3 | `receive_ts_ms` | int | Local receive millisecond timestamp |
| 4 | `receive_utc` | string | Local receive time in UTC ISO 8601 |
| 5 | `asset_id` | string | Token ID (256-bit integer) |
| 6 | `outcome` | string | `Up` or `Down` |
| 7 | `condition_id` | string | Market condition contract address |
| 8 | `side` | string | `bid` (buy orders) or `ask` (sell orders) |
| 9 | `price` | float | Price level [0, 1] |
| 10 | `size` | float | Total size at this price level |
| 11 | `level_index` | int | Price level index (0 = best price) |

### 3. Price Changes CSV

**Source**: CLOB WebSocket `price_change` events
**File pattern**: `orderbook/{slug}/price_changes_{slug}.csv`

Each row = one **incremental change** to the order book (new order, cancellation, partial fill).

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `server_ts_ms` | int | Server-side millisecond timestamp |
| 2 | `receive_ts_ms` | int | Local receive millisecond timestamp |
| 3 | `receive_utc` | string | Local receive time in UTC ISO 8601 |
| 4 | `condition_id` | string | Market condition contract address |
| 5 | `asset_id` | string | Token ID |
| 6 | `outcome` | string | `Up` or `Down` |
| 7 | `side` | string | `BUY` or `SELL` |
| 8 | `price` | float | Changed price level |
| 9 | `size` | float | New total size at this level (0 = level cleared) |
| 10 | `best_bid` | float | Best bid price after change (BBO) |
| 11 | `best_ask` | float | Best ask price after change (BBO) |

### 4. WS Trades CSV

**Source**: CLOB WebSocket `last_trade_price` events
**File pattern**: `orderbook/{slug}/trades_{slug}.csv`

Each row = one **real-time confirmed trade** (realized data).

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `server_ts_ms` | int | Server-side millisecond timestamp |
| 2 | `receive_ts_ms` | int | Local receive millisecond timestamp |
| 3 | `receive_utc` | string | Local receive time in UTC ISO 8601 |
| 4 | `condition_id` | string | Market condition contract address |
| 5 | `asset_id` | string | Token ID |
| 6 | `outcome` | string | `Up` or `Down` |
| 7 | `side` | string | `BUY` or `SELL` (taker direction) |
| 8 | `price` | float | Trade price [0, 1] |
| 9 | `size` | float | Trade size (contract quantity) |
| 10 | `fee_rate_bps` | int | Fee rate in basis points (1 bps = 0.01%) |

### 5. Markets Index CSV

**Source**: Gamma API market discovery
**File pattern**: `indexes/markets_btc_up_or_down_{suffix}.csv`

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | `market_id` | string | Polymarket market ID |
| 2 | `event_id` | int | Platform event number |
| 3 | `condition_id` | string | Condition contract address |
| 4 | `slug` | string | Market slug identifier |
| 5 | `clob_token_ids` | string | Token IDs (pipe-separated) |
| 6 | `outcomes` | string | Outcome labels (pipe-separated) |
| 7 | `window_start_ts` | int | Market window start (Unix seconds) |
| 8 | `window_start_utc` | string | Window start in UTC ISO 8601 |
| 9 | `market_end_utc` | string | Market end time in UTC ISO 8601 |
| 10 | `volume` | float | Total trading volume |
| 11 | `market_url` | string | Market URL (English) |
| 12 | `market_url_zh` | string | Market URL (Chinese) |

---

## Output Directory Structure

### Batch Pipeline Output

```
output/
├── run_state_{suffix}.json              # Checkpoint for resume
├── indexes/
│   └── markets_btc_up_or_down_{suffix}.csv
├── trades/
│   └── trades_btc_up_or_down_{suffix}.csv
└── reports/
    └── validation_summary_{suffix}.json
```

File naming adapts to `--timeframes`:

| Timeframes | Suffix | Example |
|------------|--------|---------|
| `5m` only | `5m` | `trades_btc_up_or_down_5m.csv` |
| All four | `all_timeframes` | `trades_btc_up_or_down_all_timeframes.csv` |
| Subset | Joined | `trades_btc_up_or_down_5m_15m.csv` |

### Order Book Recorder Output

```
output/orderbook/{market_slug}/
├── book_snapshots_{slug}.csv    # L2 order book snapshots
├── price_changes_{slug}.csv     # Incremental order book changes
└── trades_{slug}.csv            # Real-time trades
```

---

## Millisecond Timestamp Enrichment

### How It Works

The Data API returns trade timestamps at **second-level precision**. For higher granularity, this tool can enrich trades with **millisecond timestamps** using Polygon RPC:

```
timestamp_ms = block_timestamp × 1000 + log_index
```

Where `log_index` is the position of the `OrderFilled` event within the transaction receipt.

### By Collection Mode

| Mode | `timestamp_ms` | `server_received_ms` | `trade_time_ms` |
|------|:-:|:-:|:-:|
| **Batch** (no `--rpc-url`) | empty | empty | empty |
| **Batch** (with `--rpc-url`) | computed | empty | UTC format |
| **Stream** (always has RPC) | computed | `time.time() × 1000` | UTC format |
| **Order Book Recorder** | N/A | native `receive_ts_ms` | N/A |

The Order Book Recorder natively provides millisecond timestamps from the CLOB WebSocket (`server_ts_ms` from server, `receive_ts_ms` from local clock).

---

## On-Chain Event Monitoring

### Monitored Contracts

| Contract | Address | Purpose |
|----------|---------|---------|
| CTF Exchange | `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e` | Standard market trades |
| NegRisk CTF Exchange | `0xc5d563a36ae78145c45a50134d48a1215220f80a` | Negative risk market trades |

### Event Signature

```
OrderFilled (topic0): 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
```

### Trade Direction Logic

- `maker_asset_id` ∈ market tokens AND `taker_asset_id` = USDC → **SELL**
- `taker_asset_id` ∈ market tokens AND `maker_asset_id` = USDC → **BUY**

---

## Key Concepts

### Realized Data vs Order Book Data

| | Realized Data (Trades) | Order Book Data |
|---|---|---|
| **What** | Confirmed on-chain transactions | Unfilled pending orders |
| **Tables** | Trades CSV, WS Trades CSV | Book Snapshots, Price Changes |
| **Retroactive** | Yes (Data API retains history) | No (must record in real-time) |
| **Contains** | Price, size, wallet, tx hash | Bid/ask levels, depth |

### Order Book Levels

| Level | Description | Supported |
|-------|-------------|:---------:|
| **L1** | Best Bid/Offer only (BBO) | Yes (via `best_bid`/`best_ask` in price changes) |
| **L2** | Aggregated price levels | **Yes** (book snapshots) |
| **L3** | Individual orders | No |

### Price Interpretation

Prices on Polymarket represent **probabilities**:
- `price = 0.85` on `Up` → 85% probability that BTC goes up
- `price = 0.15` on `Down` → 15% probability that BTC goes down
- Complementary: `Up price + Down price ≈ 1.00`

---

## Project Structure

```
.
├── main.py                       # Batch pipeline entry point
├── fetch_single_market.py        # Single market fetcher
├── stream.py                     # Real-time blockchain stream
├── requirements.txt              # Python dependencies
│
├── src/polymarket_btc5m/         # Core library
│   ├── __init__.py               # Package exports
│   ├── client.py                 # HTTP client (Gamma + Data APIs)
│   ├── timeframes.py             # Timeframe parsing & validation
│   ├── chain.py                  # Polygon RPC & OrderFilled decoder
│   ├── market_tracker.py         # Market discovery & lifecycle tracking
│   ├── pipeline.py               # 3-stage batch pipeline with checkpoints
│   ├── trade_streamer.py         # Real-time trade stream service
│   ├── recorder.py               # Order book recorder (CLOB WS)
│   └── ws_connection.py          # CLOB WebSocket connection manager
│
├── scripts/
│   ├── run_pipeline.py           # Alternative pipeline entry point
│   ├── run_recorder.py           # Order book recording daemon
│   └── export_market_trades_chain.py  # Strict on-chain trade export
│
└── samples/
    ├── upgrade_validation/       # Timestamp enrichment tests
    └── timeframe_expansion_validation/  # Multi-timeframe tests
```

---

## Module Reference

### `client.py` — Polymarket API Client

HTTP abstraction layer with automatic retry, exponential backoff, and rate limit handling.

- **`PolymarketApiClient`**: Main client class
  - `get_gamma(path, params)` — Query Gamma API
  - `get_data(path, params)` — Query Data API
- **Features**: Auto-retry on 429/5xx, Retry-After header support, control char sanitization

### `timeframes.py` — Timeframe Management

Parsing, validation, and normalization for the 4 supported timeframes.

- `normalize_timeframe(value)` — Convert alias (`hourly` → `1h`)
- `parse_timeframes_csv(raw)` — Parse CLI input `"5m,15m,1h,4h"`
- `match_btc_updown_market(market, enabled_timeframes)` — Match market to timeframe
- `timeframe_file_suffix(enabled_timeframes)` — Generate output filename suffix

### `chain.py` — Polygon Blockchain Integration

RPC interaction and `OrderFilled` event decoding.

- **`PolygonRpcClient`**: JSON-RPC client with retry logic
  - `get_transaction_receipt(tx_hash)` — Fetch tx receipt
  - `get_block_timestamp(block_number)` — Get block timestamp (cached)
  - `get_order_filled_logs(block_number)` — Query OrderFilled events
- **`decode_order_filled_log(log)`** — Parse Solidity-encoded event data

### `market_tracker.py` — Market Discovery

Thread-safe market lifecycle tracker using Gamma API.

- **`MarketTracker`**: Discovers and tracks active BTC markets
  - `poll_once()` → `(newly_added, newly_expired)` markets
  - `resolve_token(token_id)` → `(slug, outcome)` mapping
  - `get_all_token_ids()` — List all tracked token IDs

### `pipeline.py` — Batch Data Pipeline

3-stage ETL pipeline with checkpoint-based resumption.

- **Stage 1**: Market index (Gamma API pagination)
- **Stage 2**: Trade backfill (Data API pagination per market)
- **Stage 3**: Validation report
- **`TradeTimestampEnricher`**: Optional RPC-based millisecond enrichment

### `trade_streamer.py` — Real-Time Trade Stream

Blockchain event listener with local WebSocket broadcast.

- **`TradeStreamService`**: Main service
  - Polygon `newHeads` subscription → block queue → trade extraction
  - CSV output + `ws://127.0.0.1:8765` JSON broadcast
  - LRU deduplication (4096 blocks, 100K log UIDs)

### `recorder.py` — Order Book Recorder

CLOB WebSocket event recorder with per-market CSV output.

- **`OrderBookRecorder`**: Main recorder daemon
  - Handles `book`, `price_change`, `last_trade_price` events
  - Auto-discovers markets, manages subscriptions
  - 3 CSV files per market: snapshots, changes, trades

### `ws_connection.py` — WebSocket Manager

Persistent CLOB WebSocket connection with auto-reconnect.

- **`ClobWebSocket`**: Connection manager
  - Exponential backoff reconnection (1s base, 60s max)
  - 10-second heartbeat ping
  - Thread-safe subscribe/unsubscribe

---

## Validation Tests

### Timestamp Enrichment Validation

```bash
python3 samples/upgrade_validation/validate_samples.py
# Expected: sample_validation_passed
```

Validates:
- Batch trades without RPC → ms fields empty
- Batch trades with RPC → `timestamp_ms` = `trade_timestamp × 1000 + log_index`
- Stream trades → all 3 ms fields populated

### Multi-Timeframe Validation

```bash
python3 samples/timeframe_expansion_validation/validate_logic.py
# Expected: timeframe_expansion_validation_passed
```

Validates:
- Timeframe alias normalization
- Market slug matching for all 4 timeframes
- Output path generation logic

---

## FAQ

### Q: Can I get historical order book data for closed markets?

**No.** Order book data only exists while a market is active. Once a market settles, all pending orders are cleared. You must run the Order Book Recorder **during the market's active period** to capture this data.

### Q: Is a Polygon RPC required?

| Mode | RPC Required? |
|------|:---:|
| Batch backfill | Optional (for ms enrichment) |
| Single market fetch | Optional (for ms enrichment) |
| Real-time stream | **Yes** (WebSocket RPC) |
| Order book recorder | No |

### Q: Why does the Data API return a 400 error at offset=4000?

The Data API has a pagination limit. High-volume markets may exceed this. For complete data, use `scripts/export_market_trades_chain.py` which scans on-chain events directly.

### Q: What's the difference between Trades CSV and WS Trades CSV?

| | Trades CSV (Table 1) | WS Trades CSV (Table 4) |
|---|---|---|
| Source | Data API (on-chain confirmed) | CLOB WebSocket (real-time push) |
| Time precision | Seconds (ms optional via RPC) | Native milliseconds |
| Retroactive | Yes | No (must record live) |
| Wallet address | Yes (`proxy_wallet`) | No |
| Transaction hash | Yes | No |
| Fee rate | No | Yes (`fee_rate_bps`) |

### Q: How do I reconstruct a full L2 order book from snapshots?

Filter by `snapshot_seq`, group by `outcome` + `side`, sort by `level_index`:

```python
import pandas as pd

df = pd.read_csv("book_snapshots_btc-updown-4h-1772082000.csv")
snapshot = df[df["snapshot_seq"] == 1]

# Up token buy orders (bids), best price first
up_bids = snapshot[(snapshot["outcome"] == "Up") & (snapshot["side"] == "bid")]
up_bids = up_bids.sort_values("level_index")
```

---

## Disclaimer

This project is for **data research and engineering learning purposes only**. Please comply with Polymarket's Terms of Service and all applicable API/RPC provider terms. The authors are not responsible for any misuse of this tool.

---

## License

MIT
