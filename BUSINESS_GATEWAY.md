# Business Read-Only Gateway

This document is for downstream teams that only need to consume published data.

## What This Gateway Is

The recorder VM now exposes a read-only HTTPS API in front of the local publish tree.

- Base URL: `https://10.193.48.144`
- Health endpoint: `GET /health`
- Data endpoints: `GET /v1/...`
- Auth model: default `Bearer` token on every `/v1/*` request, optional private-only mode with no token

The default TLS certificate is self-signed. Until you replace it with a trusted cert, clients must either:

- trust the server cert explicitly, or
- disable verification for internal testing only

## Authentication

By default, all `/v1/*` endpoints require:

```text
Authorization: Bearer <your_token>
```

If the operator enables `POLYMARKET_API_AUTH_MODE=none` for a Feilian/VPN-only deployment, `/v1/*` can be used without a token inside the private network. That mode should not be exposed to the public internet.

Typical `curl` pattern:

```bash
TOKEN=...
curl -k -H "Authorization: Bearer $TOKEN" "https://10.193.48.144/v1/keysets/index"
```

## Recommended Read Flow

Business-side consumers should read in this order:

1. `GET /v1/keysets/index`
   Use this to discover the available `dt + timeframe` partitions.
2. `GET /v1/keysets/{dt}/{timeframe}`
   Use this to discover the exact object groups and market list for one partition.
3. `GET /v1/meta/markets`
   Use this to filter markets, inspect row counts, and select the exact `market_slug`.
4. `GET /v1/curated/trades` or `GET /v1/curated/price_changes`
   Use these for business logic, monitoring, or analytics.
5. `GET /v1/curated/recovery_events`
   Use this when you need to audit websocket gaps, resnapshots, and recovery windows.
6. `GET /v1/meta/files`
   Use this when you need per-file lineage or completeness checks.

Only use `book_snapshots` when you truly need order book reconstruction. Most business workflows should start from `trades` or `price_changes`. Use `recovery_events` to explain data quality windows around reconnects and resnapshots.

## Endpoint Summary

### `GET /health`

No auth required.

Example:

```bash
curl -k "https://10.193.48.144/health"
```

### `GET /v1/keysets/index`

Auth required.

Returns all known `dt + timeframe` datasets and the latest manifest for each timeframe.

Example:

```bash
curl -k \
  -H "Authorization: Bearer $TOKEN" \
  "https://10.193.48.144/v1/keysets/index"
```

### `GET /v1/keysets/{dt}/{timeframe}`

Auth required.

Returns:

- `market_count`
- `markets`
- `key_groups`

Example:

```bash
curl -k \
  -H "Authorization: Bearer $TOKEN" \
  "https://10.193.48.144/v1/keysets/2026-03-02/15m"
```

### `GET /v1/meta/markets`

Auth required.

Query params:

- `dt`
- `timeframe`
- `market_slug` optional
- `limit`
- `offset`
- `columns` optional comma-separated field list

Example:

```bash
curl -k \
  -H "Authorization: Bearer $TOKEN" \
  "https://10.193.48.144/v1/meta/markets?dt=2026-03-02&timeframe=15m&limit=5"
```

### `GET /v1/meta/files`

Auth required.

Same query shape as `meta/markets`.

### `GET /v1/curated/{dataset}`

Auth required.

Supported datasets:

- `trades`
- `price_changes`
- `book_snapshots`
- `recovery_events`

Example:

```bash
curl -k \
  -H "Authorization: Bearer $TOKEN" \
  "https://10.193.48.144/v1/curated/price_changes?dt=2026-03-02&timeframe=15m&market_slug=btc-updown-15m-1772448300&limit=10"
```

## Python SDK Example

The repository now includes a small reusable Python client:

- [read_api_client.py](/Users/niyutong/Desktop/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper-main/src/polymarket_btc5m/read_api_client.py)
- [example_read_api_client.py](/Users/niyutong/Desktop/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper-main/scripts/example_read_api_client.py)

Minimal usage:

```python
from polymarket_btc5m.read_api_client import PolymarketReadApiClient, ReadApiConfig

with PolymarketReadApiClient(
    ReadApiConfig(
        base_url="https://10.193.48.144",
        bearer_token="YOUR_TOKEN",
        verify_tls=False,  # self-signed cert by default
    )
) as client:
    index = client.keysets_index()
    manifest = client.keyset_manifest("2026-03-02", "15m")
    markets = client.meta("markets", dt="2026-03-02", timeframe="15m", limit=5)
    changes = client.curated(
        "price_changes",
        dt="2026-03-02",
        timeframe="15m",
        market_slug="btc-updown-15m-1772448300",
        limit=10,
    )
```

CLI example:

```bash
export POLYMARKET_READ_API_URL=https://10.193.48.144
export POLYMARKET_READ_API_TOKEN=...

python3 scripts/example_read_api_client.py --insecure health
python3 scripts/example_read_api_client.py --insecure index
python3 scripts/example_read_api_client.py --insecure keyset --dt 2026-03-02 --timeframe 15m
python3 scripts/example_read_api_client.py --insecure meta --dataset markets --dt 2026-03-02 --timeframe 15m --limit 5
python3 scripts/example_read_api_client.py --insecure curated --dataset price_changes --dt 2026-03-02 --timeframe 15m --market-slug btc-updown-15m-1772448300 --limit 10
```

## Error Model

Common HTTP responses:

- `200`: success
- `401`: missing or invalid bearer token
- `404`: dataset partition or market file not found
- `500`: internal API or file-read failure

For retry behavior:

- retry `502/503/504`
- do not blindly retry `401` or `404`

## Data Contract Notes

- `dt` is a UTC partition date.
- `timeframe` is one of `5m`, `15m`, `1h`, `4h`.
- `market_slug` identifies a single market window.
- responses from `meta/*` and `curated/*` contain:
  - `file_count`
  - `total_rows`
  - `returned_rows`
  - `rows`

## Production Recommendation

For business-side production integration, the next improvements should be:

1. replace the self-signed cert with a trusted TLS cert
2. issue a dedicated token per consumer or team
3. add access logging and request rate limits in Nginx
4. if usage grows, move consumers to a separate read gateway or cache layer
