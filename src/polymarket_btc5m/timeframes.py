from __future__ import annotations

import re
from typing import Any, Iterable

SUPPORTED_TIMEFRAMES = ("5m", "15m", "1h", "4h")
DEFAULT_TIMEFRAMES = SUPPORTED_TIMEFRAMES
LEGACY_5M_TIMEFRAMES = ("5m",)

_TIMEFRAME_ALIASES = {
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "60m": "1h",
    "hourly": "1h",
    "4h": "4h",
    "4hour": "4h",
    "240m": "4h",
}

_MARKET_SLUG_PATTERN = re.compile(r"^btc-updown-([a-z0-9]+)-(\d+)$")
_SERIES_SLUG_PATTERN = re.compile(r"^btc-up-or-down-([a-z0-9]+)$")


def normalize_timeframe(value: str) -> str | None:
    token = str(value or "").strip().lower()
    return _TIMEFRAME_ALIASES.get(token)


def normalize_timeframes(values: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        token = normalize_timeframe(str(value))
        if token is None:
            raise ValueError(
                f"Unsupported timeframe '{value}'. Supported: {', '.join(SUPPORTED_TIMEFRAMES)}"
            )
        if token not in normalized:
            normalized.append(token)

    if not normalized:
        raise ValueError("At least one timeframe is required.")

    return tuple(tf for tf in SUPPORTED_TIMEFRAMES if tf in normalized)


def parse_timeframes_csv(raw: str | None, default: tuple[str, ...] = DEFAULT_TIMEFRAMES) -> tuple[str, ...]:
    if raw is None or not str(raw).strip():
        return tuple(default)
    items = [x.strip() for x in str(raw).split(",") if x.strip()]
    return normalize_timeframes(items)


def match_btc_updown_market(
    market: dict[str, Any],
    enabled_timeframes: tuple[str, ...],
) -> tuple[str, int] | None:
    slug = str(market.get("slug") or "").lower()
    slug_match = _MARKET_SLUG_PATTERN.match(slug)
    if slug_match is None:
        return None

    slug_tf = normalize_timeframe(slug_match.group(1))
    if slug_tf is None or slug_tf not in enabled_timeframes:
        return None

    events = [e for e in market.get("events", []) if isinstance(e, dict)]
    series_has_same_tf = False
    for event in events:
        series_slug = str(event.get("seriesSlug") or "").lower()
        series_match = _SERIES_SLUG_PATTERN.match(series_slug)
        if series_match is None:
            continue
        series_tf = normalize_timeframe(series_match.group(1))
        if series_tf == slug_tf:
            series_has_same_tf = True
            break

    if not series_has_same_tf:
        return None

    window_start_ts = int(slug_match.group(2))
    return slug_tf, window_start_ts


def timeframe_file_suffix(enabled_timeframes: tuple[str, ...]) -> str:
    ordered = tuple(tf for tf in SUPPORTED_TIMEFRAMES if tf in set(enabled_timeframes))
    if ordered == ("5m",):
        return "5m"
    if ordered == DEFAULT_TIMEFRAMES:
        return "all_timeframes"
    return "_".join(ordered)
