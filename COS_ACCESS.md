# COS Access

This project now publishes stable market data into Tencent COS under three top-level prefixes:

```text
raw/
curated/
meta/
```

It also publishes deterministic key manifests under:

```text
meta/keysets/index.json
meta/keysets/dt=YYYY-MM-DD/timeframe=TIMEFRAME/manifest.json
```

## What Each Prefix Means

- `raw/`: original CSV output plus `manifest.json` for each market.
- `curated/`: Parquet files partitioned by `dt` and `timeframe`.
- `meta/`: market-level and file-level metadata tables in Parquet.
- `meta/keysets`: explicit object-key manifests for each `dt + timeframe`, designed for environments without `ListBucket`.

## Fastest Ways To Get Data

1. COS console:
   Open the bucket and browse `raw/`, `curated/`, or `meta/`.
2. VM local publish tree:
   Read `/data/polymarket-agent/publish` directly on the recorder VM.
3. Python SDK:
   Use the scripts below to list, inspect, and download objects from COS.
4. DuckDB:
   Download a COS prefix to a temp cache and query it with SQL.

## Environment Variables

These scripts default to the same variables used by the VM:

```bash
export TENCENT_COS_BUCKET=polymarket-ai-agent-1328550492
export TENCENT_COS_REGION=ap-tokyo
export TENCENTCLOUD_SECRET_ID=...
export TENCENTCLOUD_SECRET_KEY=...
```

Install the read-side dependencies with:

```bash
pip install -r requirements-analytics.txt
```

## Bucket Inspection

Check whether `raw`, `curated`, and `meta` are present in COS:

```bash
python3 scripts/inspect_cos_publish.py
```

Note:
`inspect_cos_publish.py` and any `--prefix` based reads require COS bucket listing permission. If your credentials only have write or direct object read permission, use explicit `--key` reads instead.

## Keyset-First Reads Without `ListBucket`

Read the latest keyset index:

```bash
python3 scripts/read_cos_publish_python.py \
  --key meta/keysets/index.json
```

Print the curated trade object keys for one `dt + timeframe` manifest:

```bash
python3 scripts/read_cos_publish_python.py \
  --keyset-key meta/keysets/dt=2026-03-02/timeframe=15m/manifest.json \
  --keyset-group curated_trades \
  --print-keys
```

Query `meta_markets` straight from a keyset manifest:

```bash
python3 scripts/query_cos_publish_duckdb.py \
  --keyset-key meta/keysets/dt=2026-03-02/timeframe=15m/manifest.json \
  --keyset-group meta_markets \
  --sql "select market_slug, trade_rows, price_change_rows from dataset order by price_change_rows desc limit 10"
```

## Python Examples

Read one manifest:

```bash
python3 scripts/read_cos_publish_python.py \
  --key raw/orderbook/dt=2026-03-02/timeframe=15m/market_slug=btc-updown-15m-1772448300/manifest.json
```

Read a Parquet slice from curated trades:

```bash
python3 scripts/read_cos_publish_python.py \
  --prefix curated/trades/dt=2026-03-02/timeframe=5m/ \
  --max-files 10 \
  --limit 5
```

Read a small explicit parquet set when `ListBucket` is not allowed:

```bash
python3 scripts/read_cos_publish_python.py \
  --key curated/price_changes/dt=2026-03-02/timeframe=15m/btc-updown-15m-1772448300.parquet \
  --key curated/price_changes/dt=2026-03-02/timeframe=15m/btc-updown-15m-1772449200.parquet \
  --limit 5
```

## DuckDB Example

Query curated trades from COS:

```bash
python3 scripts/query_cos_publish_duckdb.py \
  --prefix curated/trades/dt=2026-03-02/timeframe=5m/ \
  --max-files 20 \
  --sql "select market_slug, count(*) as trades from dataset group by 1 order by trades desc limit 10"
```

Query the market metadata layer:

```bash
python3 scripts/query_cos_publish_duckdb.py \
  --prefix meta/markets/dt=2026-03-02/timeframe=15m/ \
  --sql "select market_slug, trade_rows, receive_ts_ms_min, receive_ts_ms_max from dataset order by trade_rows desc limit 10"
```

If your COS credentials do not have bucket listing permission, pass known keys directly:

```bash
python3 scripts/query_cos_publish_duckdb.py \
  --key meta/markets/dt=2026-03-02/timeframe=15m/btc-updown-15m-1772448300.parquet \
  --key meta/markets/dt=2026-03-02/timeframe=15m/btc-updown-15m-1772449200.parquet \
  --sql "select market_slug, trade_rows from dataset order by trade_rows desc"
```

The new `meta/keysets/.../manifest.json` files remove the need to hand-maintain these key lists.

## HTTP API

The VM can also expose the local publish tree over HTTP.

`/health` stays open for basic liveness checks.
By default, all `/v1/*` endpoints require:

```text
Authorization: Bearer <POLYMARKET_API_BEARER_TOKEN>
```

If the API is intentionally private behind Feilian/VPN and `POLYMARKET_API_AUTH_MODE=none` is enabled, `/v1/*` can be accessed without a token from the private network.

Examples:

```bash
curl http://127.0.0.1:8080/health
curl -H "Authorization: Bearer $POLYMARKET_API_BEARER_TOKEN" "http://127.0.0.1:8080/v1/keysets/index"
curl -H "Authorization: Bearer $POLYMARKET_API_BEARER_TOKEN" "http://127.0.0.1:8080/v1/keysets/2026-03-02/15m"
curl -H "Authorization: Bearer $POLYMARKET_API_BEARER_TOKEN" "http://127.0.0.1:8080/v1/meta/markets?dt=2026-03-02&timeframe=15m&limit=5"
curl -H "Authorization: Bearer $POLYMARKET_API_BEARER_TOKEN" "http://127.0.0.1:8080/v1/curated/price_changes?dt=2026-03-02&timeframe=15m&market_slug=btc-updown-15m-1772448300&limit=10"
```

If Nginx HTTPS is installed, use:

```bash
TOKEN=$(cat /root/polymarket_api_bearer_token.txt)
curl -k https://127.0.0.1/health
curl -k -H "Authorization: Bearer $TOKEN" "https://127.0.0.1/v1/keysets/index"
```

The default TLS certificate is self-signed, so `curl` examples use `-k`. Replace it with a trusted cert later if you want browser-safe HTTPS.

For downstream business consumers, see:

- [BUSINESS_GATEWAY.md](/Users/niyutong/Desktop/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper-main/BUSINESS_GATEWAY.md)
- [read_api_client.py](/Users/niyutong/Desktop/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper-main/src/polymarket_btc5m/read_api_client.py)
- [example_read_api_client.py](/Users/niyutong/Desktop/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper-main/scripts/example_read_api_client.py)

## Recommended Read Path

1. Start with `meta/markets` to locate the market and date.
2. Read `curated/trades` for fills and realized activity.
3. Read `curated/price_changes` for incremental order book changes.
4. Read `curated/recovery_events` to audit websocket gaps, resnapshots, and recovery windows.
5. Read `curated/book_snapshots` only when you need full L2 reconstruction.
6. Use `raw/` only for auditing or parser debugging.
