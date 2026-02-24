# Polymarket BTC 5-Minute Market Data Pipeline

A comprehensive data collection toolkit for [Polymarket](https://polymarket.com) **BTC Up/Down 5-Minute** prediction markets. This project provides two independent data collection systems:

1. **Realized Trade Pipeline** — Batch backfill of historical executed trades from closed markets
2. **Real-Time Order Book Recorder** — Live WebSocket-based L2 order book streaming from active markets

Together, they capture the complete trading lifecycle of BTC 5-minute prediction markets.

---

## Table of Contents

- [Data Definitions](#data-definitions)
  - [Realized Trades](#realized-trades)
  - [Full Order Book (L2)](#full-order-book-l2)
  - [Key Differences](#key-differences)
- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [1. Realized Trade Pipeline](#1-realized-trade-pipeline)
  - [How It Works](#how-it-works)
  - [CLI Usage](#cli-usage)
  - [Output Files](#output-files)
  - [CSV Schemas](#csv-schemas-trades)
- [2. Real-Time Order Book Recorder](#2-real-time-order-book-recorder)
  - [How It Works](#how-it-works-1)
  - [CLI Usage](#cli-usage-1)
  - [Output Files](#output-files-1)
  - [CSV Schemas](#csv-schemas-order-book)
- [3. On-Chain Trade Verification](#3-on-chain-trade-verification)
- [API Sources](#api-sources)
- [Project Structure](#project-structure)
- [Requirements](#requirements)

---

## Data Definitions

### Realized Trades

**Definition:** Realized trades are **executed (filled) transactions** — orders that have been matched and settled on the Polymarket CLOB (Central Limit Order Book). Each trade record represents a completed exchange between a buyer and seller at a specific price and size.

**Data Scope:**
- **Time range:** Historical closed markets only (markets that have already resolved)
- **Source API:** `Data API /trades` (off-chain trade history) and optionally Polygon on-chain `OrderFilled` logs for strict verification
- **What it captures:**
  - Every executed trade: price, size, notional value (`price × size`), side (BUY/SELL), outcome (Up/Down)
  - Transaction metadata: timestamp (unix seconds), proxy wallet address, transaction hash
  - Market context: condition ID, event ID, market slug, window start timestamp
- **What it does NOT capture:**
  - Unexecuted (resting) limit orders
  - Order book depth or liquidity at any point in time
  - Order cancellations or modifications
  - Bid-ask spreads
- **Limitations:**
  - Data API `/trades` has an offset limit (~3000 trades per market). For high-volume markets, on-chain verification (`export_market_trades_chain.py`) provides complete coverage
  - Only available for **closed** markets — the Data API returns trade history after market resolution

### Full Order Book (L2)

**Definition:** The L2 (Level 2) order book is an **aggregated depth snapshot** showing the total resting order size at each price level. "L2" means orders are grouped by price — you see "100 shares at $0.55" but NOT who placed those 100 shares or how many individual orders compose them (that would be L3).

**Data Scope:**
- **Time range:** From the moment the recorder starts running onwards (prospective only, no historical backfill possible)
- **Source:** CLOB WebSocket `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **What it captures (3 event streams):**
  1. **Book Snapshots** (`book` events) — Complete L2 depth after each trade execution. Contains all bid and ask price levels with their aggregated sizes
  2. **Price Changes** (`price_change` events) — Incremental L2 delta updates from order placements, cancellations, and partial fills. `size=0` means a price level was removed
  3. **WebSocket Trades** (`last_trade_price` events) — Trade execution notifications received in real-time via WebSocket, with millisecond-precision receive timestamps
- **What it does NOT capture:**
  - Individual order IDs or L3 data (Polymarket public API does not expose this)
  - Historical order book states before the recorder was started
  - Order book of **closed** markets (Polymarket destroys the CLOB book once a market resolves; REST `/book` returns 404)
- **Timestamp precision:** Millisecond-level. Each record includes:
  - `server_ts_ms` — Server-side timestamp from Polymarket
  - `receive_ts_ms` — Local receive timestamp (when the WebSocket message was processed)
  - `receive_utc` — Human-readable ISO 8601 UTC timestamp

### Key Differences

| Aspect | Realized Trades | Order Book (L2) |
|--------|----------------|-----------------|
| **Data type** | Executed transactions | Resting order depth + deltas |
| **Time direction** | Historical (backward-looking) | Real-time (forward-looking) |
| **Market state** | Closed markets only | Active markets only |
| **Collection method** | REST API batch polling | WebSocket streaming |
| **Granularity** | Per-trade | Per-price-level, millisecond events |
| **Completeness** | All trades (with on-chain fallback) | All events from recorder start |
| **Shows liquidity?** | No (only what traded) | Yes (full bid/ask depth) |
| **Shows spread?** | No | Yes (`best_bid`, `best_ask`) |

> **Why can't we get historical order books?** Polymarket destroys the CLOB order book when a market closes and resolves. The REST endpoint `GET /book?token_id=...` returns HTTP 404 for closed markets. This is why the Order Book Recorder must run continuously to capture depth data while markets are still live.

---

## Architecture Overview

```
Polymarket APIs
├── Gamma API ─────────── Market metadata (discovery, lifecycle)
├── Data API ──────────── Historical trade records (closed markets)
└── CLOB WebSocket ────── Real-time order book events (active markets)

This Project
├── run_pipeline.py ───── Batch pipeline: index markets → backfill trades → validate
├── run_recorder.py ───── Daemon: discover markets → WS subscribe → stream to CSV
└── export_market_trades_chain.py ── On-chain verification for single markets
```

### Realized Trade Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    run_pipeline.py (CLI)                          │
└───────────────────────┬──────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                 pipeline.py (4-Stage Pipeline)                    │
│                                                                   │
│  Stage 1: Market Index ── Gamma /markets (tag_id=235, closed)    │
│      ↓                                                            │
│  Stage 2: Trade Backfill ── Data /trades (per conditionId)       │
│      ↓                                                            │
│  Stage 3: Validation ── Cross-check counts and consistency       │
│                                                                   │
│  Checkpoint: run_state.json (resume on crash)                    │
└──────────────────────────────────────────────────────────────────┘
```

### Order Book Recorder Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                   run_recorder.py (CLI)                           │
└───────────────────────┬──────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│               recorder.py (OrderBookRecorder)                     │
│  Main thread: market discovery loop (Gamma API poll every 30s)   │
│  WS thread:   receive events → dispatch → write CSV              │
├──────────────────────────────┬───────────────────────────────────┤
│  market_tracker.py           │  ws_connection.py                 │
│  • Gamma API active market   │  • WebSocket connection manager   │
│    discovery & filtering     │  • Heartbeat (PING/PONG, 10s)    │
│  • token_id ↔ market map     │  • Auto-reconnect (exp. backoff) │
│  • Expiry-based lifecycle    │  • Subscribe / unsubscribe       │
└──────────────────────────────┴───────────────────────────────────┘
                        │
                        ▼
         output/orderbook/<market-slug>/
           ├── book_snapshots_<slug>.csv
           ├── price_changes_<slug>.csv
           └── trades_<slug>.csv
```

---

## Quick Start

```bash
# Clone
git clone https://github.com/Oceanjackson1/Polymarket-BTC-5m-Scraper-API-Soluation.git
cd Polymarket-BTC-5m-Scraper-API-Soluation

# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Option A: Backfill historical trades (batch, runs to completion)
python scripts/run_pipeline.py

# Option B: Record live order books (daemon, runs until Ctrl-C)
python scripts/run_recorder.py

# Option C: Run both (in separate terminals)
```

---

## 1. Realized Trade Pipeline

### How It Works

The pipeline runs in 3 stages with checkpoint-based resume:

1. **Market Index** — Queries Gamma API for all closed BTC 5-minute markets (`tag_id=235`, `seriesSlug=btc-up-or-down-5m`). Paginates through all results and writes a market index CSV.

2. **Trade Backfill** — For each market in the index, fetches all executed trades from Data API `/trades` endpoint (paginated, `limit=1000`). Computes `notional = price × size` for each trade. Writes all trades to a single consolidated CSV.

3. **Validation** — Cross-checks market count vs. trade count, generates a summary report.

The pipeline saves progress to `run_state.json` after each page/market, allowing crash recovery via `--resume` (default behavior).

### CLI Usage

```bash
# Default run (with auto-resume from last checkpoint)
python scripts/run_pipeline.py

# Fresh start (ignore existing checkpoint)
python scripts/run_pipeline.py --no-resume

# Debug: only process first 20 markets
python scripts/run_pipeline.py --market-limit 20

# Include markets with zero volume
python scripts/run_pipeline.py --include-zero-volume

# All options
python scripts/run_pipeline.py \
  --output-dir output \
  --market-limit 100 \
  --include-zero-volume \
  --request-delay 0.15 \
  --timeout-seconds 30 \
  --max-retries 5 \
  --log-level DEBUG
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `output/` | Base output directory |
| `--no-resume` | (resume) | Start fresh, ignore checkpoint |
| `--market-limit` | None (all) | Max markets to process (for debugging) |
| `--include-zero-volume` | False | Include markets with volume=0 |
| `--request-delay` | 0.10s | Delay between API requests |
| `--timeout-seconds` | 30 | HTTP request timeout |
| `--max-retries` | 5 | Max retries per request (429/5xx auto-retry) |
| `--log-level` | INFO | Logging level (DEBUG/INFO/WARNING/ERROR) |

### Output Files

```
output/
├── indexes/
│   └── markets_btc_up_or_down_5m.csv    # Market index (all discovered markets)
├── trades/
│   └── trades_btc_up_or_down_5m.csv     # Consolidated trade history
├── reports/
│   └── validation_summary.json           # Validation report
└── run_state.json                         # Checkpoint for resume
```

### CSV Schemas (Trades)

**`markets_btc_up_or_down_5m.csv`**

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Gamma API market ID |
| `event_id` | string | Associated event ID |
| `condition_id` | string | CLOB condition ID (used for trade queries) |
| `slug` | string | Market slug, e.g. `btc-updown-5m-1771211700` |
| `clob_token_ids` | string | JSON array of CLOB token IDs (for order book subscription) |
| `outcomes` | string | JSON array of outcome labels, e.g. `["Up","Down"]` |
| `window_start_ts` | int | Unix timestamp of the 5-min window start |
| `window_start_utc` | string | ISO 8601 UTC of window start |
| `market_end_utc` | string | Market end date (ISO 8601) |
| `volume` | float | Total market volume (USD) |
| `market_url` | string | Polymarket URL (English) |
| `market_url_zh` | string | Polymarket URL (Chinese) |

**`trades_btc_up_or_down_5m.csv`**

| Column | Type | Description |
|--------|------|-------------|
| `market_slug` | string | Market slug |
| `window_start_ts` | int | Window start unix timestamp |
| `condition_id` | string | CLOB condition ID |
| `event_id` | string | Event ID |
| `trade_timestamp` | int | Trade execution time (unix seconds) |
| `trade_utc` | string | Trade time (ISO 8601 UTC) |
| `price` | float | Execution price (0.0–1.0) |
| `size` | float | Number of shares traded |
| `notional` | float | `price × size` (USD value) |
| `side` | string | `BUY` or `SELL` |
| `outcome` | string | `Up` or `Down` |
| `asset` | string | Token ID of the traded asset |
| `proxy_wallet` | string | Trader's proxy wallet address |
| `transaction_hash` | string | Polygon transaction hash |
| `dedupe_key` | string | Composite key for deduplication |

---

## 2. Real-Time Order Book Recorder

### How It Works

The recorder runs as a **long-lived daemon process** that continuously:

1. **Discovers markets** — Polls Gamma API every 30 seconds for active (open) BTC 5-minute markets
2. **Subscribes via WebSocket** — Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market` and subscribes to all discovered token IDs
3. **Streams events to CSV** — Receives 3 types of WebSocket events and writes them to per-market CSV files in real-time
4. **Manages lifecycle** — Automatically subscribes to newly created markets and unsubscribes from expired ones (with 120s grace period)

**Resilience features:**
- **Auto-reconnect:** Exponential backoff (1s → 2s → 4s → ... → max 60s) on WebSocket disconnection
- **Full re-subscribe on reconnect:** All active token IDs are re-subscribed after reconnection
- **Append-mode CSV:** Files opened in append mode — safe to restart without losing prior data
- **Heartbeat:** PING every 10 seconds to keep the connection alive
- **Graceful shutdown:** `Ctrl-C` or `SIGTERM` flushes all CSV buffers before exit

### CLI Usage

```bash
# Default: record to output/orderbook/
python scripts/run_recorder.py

# Custom output directory
python scripts/run_recorder.py --output-dir /data/polymarket

# Fine-tune timing
python scripts/run_recorder.py \
  --poll-interval 15 \
  --grace-period 180

# All options
python scripts/run_recorder.py \
  --output-dir output \
  --poll-interval 30 \
  --grace-period 120 \
  --timeout-seconds 30 \
  --max-retries 5 \
  --log-level DEBUG
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `output/` | Base output directory |
| `--poll-interval` | 30.0s | Seconds between Gamma API discovery polls |
| `--grace-period` | 120.0s | Seconds to keep recording after market endDate |
| `--timeout-seconds` | 30 | HTTP timeout for Gamma API requests |
| `--max-retries` | 5 | Max retries for Gamma API requests |
| `--log-level` | INFO | Logging level |

### Output Files

Each active market gets its own directory with 3 CSV files:

```
output/orderbook/
├── btc-updown-5m-1771211700/
│   ├── book_snapshots_btc-updown-5m-1771211700.csv
│   ├── price_changes_btc-updown-5m-1771211700.csv
│   └── trades_btc-updown-5m-1771211700.csv
├── btc-updown-5m-1771212000/
│   ├── book_snapshots_btc-updown-5m-1771212000.csv
│   ├── price_changes_btc-updown-5m-1771212000.csv
│   └── trades_btc-updown-5m-1771212000.csv
└── ... (one directory per active market)
```

### CSV Schemas (Order Book)

**`book_snapshots_<slug>.csv`** — Full L2 depth snapshots

Triggered by `book` events (sent after each trade). Each snapshot expands into multiple rows (one per price level).

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_seq` | int | Auto-incrementing snapshot counter (per market) |
| `server_ts_ms` | int | Server timestamp (milliseconds since epoch) |
| `receive_ts_ms` | int | Local receive timestamp (ms) |
| `receive_utc` | string | Receive time (ISO 8601 UTC, ms precision) |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` or `Down` |
| `condition_id` | string | Market condition ID |
| `side` | string | `bid` or `ask` |
| `price` | string | Price level (0.0–1.0) |
| `size` | string | Total resting size at this price level |
| `level_index` | int | Position in the price ladder (0 = best) |

**`price_changes_<slug>.csv`** — L2 incremental updates

Triggered by `price_change` events (order place, cancel, or partial fill). Captures the delta — `size=0` means the price level was completely removed.

| Column | Type | Description |
|--------|------|-------------|
| `server_ts_ms` | int | Server timestamp (ms) |
| `receive_ts_ms` | int | Local receive timestamp (ms) |
| `receive_utc` | string | Receive time (ISO 8601 UTC) |
| `condition_id` | string | Market condition ID |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` or `Down` |
| `side` | string | `BUY` or `SELL` |
| `price` | string | Affected price level |
| `size` | string | New total size at this level (`0` = removed) |
| `best_bid` | string | Current best bid price after this change |
| `best_ask` | string | Current best ask price after this change |

**`trades_<slug>.csv`** — Real-time trade executions

Triggered by `last_trade_price` events. Similar data to the realized trade pipeline but with millisecond-precision timestamps and real-time delivery.

| Column | Type | Description |
|--------|------|-------------|
| `server_ts_ms` | int | Server timestamp (ms) |
| `receive_ts_ms` | int | Local receive timestamp (ms) |
| `receive_utc` | string | Receive time (ISO 8601 UTC) |
| `condition_id` | string | Market condition ID |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` or `Down` |
| `side` | string | `BUY` or `SELL` |
| `price` | string | Execution price |
| `size` | string | Trade size |
| `fee_rate_bps` | string | Fee rate in basis points |

---

## 3. On-Chain Trade Verification

For high-volume markets where Data API `/trades` may hit the ~3000 offset limit, you can verify trades directly from the Polygon blockchain:

```bash
python scripts/export_market_trades_chain.py \
  --slug btc-updown-5m-1771211700 \
  --output-dir output_strict \
  --compare-csv output/trades/trades_btc-updown-5m-1771211700.csv
```

This scans Polygon `OrderFilled` event logs for the specified market, providing a ground-truth trade record that bypasses any API pagination limits.

**Output:**
- `output_strict/<slug>/strict_trades_<slug>.csv` — Complete on-chain trade history
- `output_strict/<slug>/strict_validation_<slug>.json` — Validation report with coverage analysis

---

## API Sources

This project uses three Polymarket API endpoints:

| API | Base URL | Purpose | Auth Required |
|-----|----------|---------|---------------|
| **Gamma API** | `https://gamma-api.polymarket.com` | Market metadata, discovery, lifecycle | No |
| **Data API** | `https://data-api.polymarket.com` | Historical trade records | No |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Real-time order book events | No |

**Gamma API** is used by both systems for market discovery. Key parameters:
- `tag_id=235` — Bitcoin markets
- `seriesSlug=btc-up-or-down-5m` — 5-minute series filter
- `slug` pattern: `btc-updown-5m-<unix_timestamp>`

**WebSocket Protocol:**
- Subscribe: `{"assets_ids": [...], "type": "market", "custom_feature_enabled": true}`
- Heartbeat: Send text `"PING"` every 10 seconds, receive `"PONG"`
- Events arrive as JSON arrays; each element has an `event_type` field (`book`, `price_change`, `last_trade_price`)

---

## Project Structure

```
├── src/polymarket_btc5m/
│   ├── __init__.py          # Package exports
│   ├── client.py            # HTTP client with retry logic (Gamma + Data APIs)
│   ├── pipeline.py          # Batch trade pipeline (stages 1-3)
│   ├── market_tracker.py    # Active market discovery and lifecycle management
│   ├── ws_connection.py     # WebSocket connection with heartbeat and reconnect
│   └── recorder.py          # Order book recorder (event dispatch + CSV writing)
├── scripts/
│   ├── run_pipeline.py      # CLI: batch trade backfill
│   ├── run_recorder.py      # CLI: real-time order book recording
│   └── export_market_trades_chain.py  # CLI: on-chain trade verification
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Requirements

- Python 3.10+
- Dependencies (auto-installed via `pip install -r requirements.txt`):
  - `requests>=2.32.0` — HTTP client for REST APIs
  - `websocket-client>=1.7.0` — WebSocket client for real-time streaming

No API keys required. All Polymarket endpoints used are public.
