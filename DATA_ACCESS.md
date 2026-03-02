# Data Access Plan

## Recommended Layout

Use a two-layer model:

1. `raw`: immutable market-level snapshots copied from the recorder output.
2. `curated`: Parquet files optimized for analytical reads.

The publish tree is:

```text
publish/
  raw/
    orderbook/
      dt=YYYY-MM-DD/
        timeframe=5m/
          market_slug=btc-updown-.../
            book_snapshots_<slug>.csv
            price_changes_<slug>.csv
            trades_<slug>.csv
            manifest.json
  curated/
    book_snapshots/
      dt=YYYY-MM-DD/
        timeframe=5m/
          <slug>.parquet
    price_changes/
      dt=YYYY-MM-DD/
        timeframe=5m/
          <slug>.parquet
    trades/
      dt=YYYY-MM-DD/
        timeframe=5m/
          <slug>.parquet
  meta/
    markets/
      dt=YYYY-MM-DD/
        timeframe=5m/
          <slug>.parquet
    files/
      dt=YYYY-MM-DD/
        timeframe=5m/
          <slug>.parquet
```

## Why This Is The Best Read Pattern

- `raw` preserves exact recorder output for replay and debugging.
- `curated` avoids scanning many CSV files during analysis.
- Partitioning by `dt` and `timeframe` matches the common query path.
- `market_slug` stays as a column in Parquet, so queries stay simple.
- `meta/markets` is the first table to read when locating a market.
- `meta/files` provides file-level lineage and completeness checks.

## Default Read Workflow

1. Find the market or date range from `meta/markets`.
2. Read `curated/trades` for trade analytics.
3. Read `curated/price_changes` for incremental order book changes.
4. Read `curated/book_snapshots` only when a full L2 reconstruction is needed.
5. Fall back to `raw` only for audits or parser debugging.

## Suggested Query Tools

- `DuckDB` for ad hoc queries directly against COS or local Parquet.
- `PyArrow` for Python batch jobs.
- `Spark` only if the dataset grows beyond single-node workflows.

## Example DuckDB Queries

```sql
SELECT market_slug, timeframe, trade_rows
FROM read_parquet('publish/meta/markets/dt=2026-03-02/timeframe=5m/*.parquet')
ORDER BY trade_rows DESC;
```

```sql
SELECT market_slug, receive_ts_ms, side, price, size
FROM read_parquet('publish/curated/trades/dt=2026-03-02/timeframe=5m/*.parquet')
WHERE market_slug = 'btc-updown-5m-1772524200'
ORDER BY receive_ts_ms;
```

```sql
SELECT market_slug, receive_ts_ms, side, price, best_bid, best_ask
FROM read_parquet('publish/curated/price_changes/dt=2026-03-02/timeframe=15m/*.parquet')
WHERE receive_ts_ms BETWEEN 1772524200000 AND 1772524500000;
```
