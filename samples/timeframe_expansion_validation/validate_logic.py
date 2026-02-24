#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / 'src'
sys.path.insert(0, str(SRC))

from polymarket_btc5m.pipeline import _normalize_market_record, _prepare_output_paths
from polymarket_btc5m.timeframes import match_btc_updown_market, parse_timeframes_csv

BASE = Path(__file__).resolve().parent


def load_json(name: str):
    return json.loads((BASE / name).read_text(encoding='utf-8'))


def read_csv(name: str):
    with (BASE / name).open('r', newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def validate_timeframe_parsing() -> None:
    parsed = parse_timeframes_csv('5m,15m,hourly,4hour')
    expected = ('5m', '15m', '1h', '4h')
    if parsed != expected:
        raise AssertionError(f'parse_timeframes_csv mismatch: got={parsed} expected={expected}')


def validate_market_matching() -> None:
    markets = load_json('input_markets.json')
    enabled = parse_timeframes_csv('5m,15m,1h,4h')

    matched = []
    for m in markets:
        hit = match_btc_updown_market(m, enabled)
        if hit is not None:
            matched.append((m['slug'], hit[0], hit[1]))

    got_slugs = sorted([x[0] for x in matched])
    expected_slugs = sorted([
        'btc-updown-5m-1773024000',
        'btc-updown-15m-1773024300',
        'btc-updown-hourly-1773027600',
        'btc-updown-4hour-1773038400',
    ])
    if got_slugs != expected_slugs:
        raise AssertionError(f'match_btc_updown_market slugs mismatch: got={got_slugs} expected={expected_slugs}')


def validate_market_normalization() -> None:
    markets = load_json('input_markets.json')
    enabled = parse_timeframes_csv('5m,15m,1h,4h')

    normalized = []
    for m in markets:
        row = _normalize_market_record(m, enabled_timeframes=enabled)
        if row is not None:
            normalized.append(row)

    expected_rows = read_csv('expected_markets_all_timeframes.csv')

    got_pairs = sorted((str(x['slug']), str(x['window_start_ts'])) for x in normalized)
    exp_pairs = sorted((str(x['slug']), str(x['window_start_ts'])) for x in expected_rows)
    if got_pairs != exp_pairs:
        raise AssertionError(f'_normalize_market_record mismatch: got={got_pairs} expected={exp_pairs}')


def validate_output_path_suffixes() -> None:
    payload = load_json('expected_output_paths.json')
    tmp = ROOT / 'samples' / 'timeframe_expansion_validation' / '.tmp_output_paths'

    for case in payload.values():
        timeframes = tuple(case['timeframes'])
        expected = case['expected']
        paths = _prepare_output_paths(tmp, timeframes)

        if paths['checkpoint'].name != expected['checkpoint']:
            raise AssertionError('checkpoint filename mismatch')
        if paths['markets_csv'].name != expected['markets_csv']:
            raise AssertionError('markets_csv filename mismatch')
        if paths['trades_csv'].name != expected['trades_csv']:
            raise AssertionError('trades_csv filename mismatch')
        if paths['validation_report'].name != expected['validation_report']:
            raise AssertionError('validation_report filename mismatch')


def validate_trade_timestamp_samples() -> None:
    rows = read_csv('sample_trades_multi_timeframes.csv')
    for row in rows:
        ts = int(row['trade_timestamp'])
        ts_ms = int(row['timestamp_ms'])
        if not (ts * 1000 <= ts_ms < ts * 1000 + 1000):
            raise AssertionError(f'timestamp_ms out of range for row={row}')
        if not row['server_received_ms'].strip():
            raise AssertionError('server_received_ms should be non-empty for stream sample row')
        if not row['trade_time_ms'].strip():
            raise AssertionError('trade_time_ms should be non-empty for stream sample row')


def main() -> None:
    validate_timeframe_parsing()
    validate_market_matching()
    validate_market_normalization()
    validate_output_path_suffixes()
    validate_trade_timestamp_samples()
    print('timeframe_expansion_validation_passed')


if __name__ == '__main__':
    main()
