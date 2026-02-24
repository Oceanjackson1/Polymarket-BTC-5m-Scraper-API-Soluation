from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .client import PolymarketApiClient
from .timeframes import LEGACY_5M_TIMEFRAMES, match_btc_updown_market, normalize_timeframes

BTC_TAG_ID = 235

logger = logging.getLogger(__name__)


@dataclass
class ActiveMarket:
    """A currently live BTC up/down market."""

    slug: str
    timeframe: str
    event_id: str
    window_start_ts: int
    condition_id: str
    token_ids: list[str]
    outcomes: list[str]
    end_date_utc: str
    end_date_ts: float
    token_to_outcome: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.token_to_outcome:
            self.token_to_outcome = {
                tid: outcome
                for tid, outcome in zip(self.token_ids, self.outcomes)
            }


class MarketTracker:
    """Discovers active BTC up/down markets and tracks their lifecycle."""

    def __init__(
        self,
        client: PolymarketApiClient,
        grace_period_seconds: float = 120.0,
        poll_interval_seconds: float = 30.0,
        timeframes: tuple[str, ...] = LEGACY_5M_TIMEFRAMES,
    ) -> None:
        self._client = client
        self._grace_period_seconds = grace_period_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self._timeframes = normalize_timeframes(timeframes)
        self._active: dict[str, ActiveMarket] = {}  # slug -> market
        self._token_index: dict[str, str] = {}  # token_id -> slug
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def poll_once(self) -> tuple[list[ActiveMarket], list[ActiveMarket]]:
        """Query Gamma API and return (newly_added, newly_expired) markets."""
        discovered = self._fetch_active_markets()
        added = self._process_new(discovered)
        expired = self._check_expired()
        return added, expired

    def resolve_token(self, token_id: str) -> tuple[str, str] | None:
        """Return (slug, outcome) for a token_id, or None."""
        with self._lock:
            slug = self._token_index.get(token_id)
            if slug is None:
                return None
            market = self._active.get(slug)
            if market is None:
                return None
            outcome = market.token_to_outcome.get(token_id, "")
            return slug, outcome

    def resolve_token_market(self, token_id: str) -> tuple[ActiveMarket, str] | None:
        """Return (market, outcome) for token_id, or None."""
        with self._lock:
            slug = self._token_index.get(token_id)
            if slug is None:
                return None
            market = self._active.get(slug)
            if market is None:
                return None
            return market, market.token_to_outcome.get(token_id, "")

    def get_all_token_ids(self) -> list[str]:
        """Return all currently tracked token IDs."""
        with self._lock:
            return list(self._token_index.keys())

    def get_market(self, slug: str) -> ActiveMarket | None:
        with self._lock:
            return self._active.get(slug)

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_active_markets(self) -> list[dict[str, Any]]:
        """Fetch open BTC up/down markets from Gamma API."""
        all_markets: list[dict[str, Any]] = []
        offset = 0
        while True:
            params: dict[str, Any] = {
                "tag_id": BTC_TAG_ID,
                "closed": "false",
                "active": "true",
                "limit": 500,
                "offset": offset,
                "order": "id",
                "ascending": "false",
            }
            try:
                page = self._client.get_gamma("/markets", params=params)
            except Exception:
                logger.exception("Gamma API fetch error")
                break

            if not isinstance(page, list) or not page:
                break

            for market in page:
                if self._is_target_market(market):
                    all_markets.append(market)

            if len(page) < 500:
                break
            offset += len(page)

        return all_markets

    def _is_target_market(self, market: dict[str, Any]) -> bool:
        if not isinstance(market, dict):
            return False
        return match_btc_updown_market(market, self._timeframes) is not None

    def _process_new(self, discovered: list[dict[str, Any]]) -> list[ActiveMarket]:
        added: list[ActiveMarket] = []
        now = time.time()
        with self._lock:
            for raw in discovered:
                slug = str(raw.get("slug") or "")
                if slug in self._active:
                    continue

                timeframe_match = match_btc_updown_market(raw, self._timeframes)
                if timeframe_match is None:
                    continue
                timeframe, window_start_ts = timeframe_match

                condition_id = str(raw.get("conditionId") or "")
                clob_token_ids_str = str(raw.get("clobTokenIds") or "")
                outcomes_str = str(raw.get("outcomes") or "")
                events = [e for e in raw.get("events", []) if isinstance(e, dict)]
                event_id = str(events[0].get("id") or "") if events else ""

                try:
                    token_ids = json.loads(clob_token_ids_str)
                except (json.JSONDecodeError, TypeError):
                    token_ids = []
                try:
                    outcomes = json.loads(outcomes_str)
                except (json.JSONDecodeError, TypeError):
                    outcomes = []

                if not token_ids or not condition_id:
                    logger.warning("Skipping market %s: missing token_ids or condition_id", slug)
                    continue

                end_date_utc = str(raw.get("endDate") or "")
                end_date_ts = self._parse_end_date(end_date_utc)

                # Skip markets already past expiry
                if end_date_ts > 0 and now > end_date_ts + self._grace_period_seconds:
                    continue

                market = ActiveMarket(
                    slug=slug,
                    timeframe=timeframe,
                    event_id=event_id,
                    window_start_ts=window_start_ts,
                    condition_id=condition_id,
                    token_ids=token_ids,
                    outcomes=outcomes,
                    end_date_utc=end_date_utc,
                    end_date_ts=end_date_ts,
                )
                self._active[slug] = market
                for tid in token_ids:
                    self._token_index[tid] = slug
                added.append(market)

        if added:
            logger.info("Discovered %d new markets: %s", len(added), [m.slug for m in added])
        return added

    def _check_expired(self) -> list[ActiveMarket]:
        now = time.time()
        expired: list[ActiveMarket] = []
        with self._lock:
            for slug in list(self._active.keys()):
                market = self._active[slug]
                if market.end_date_ts > 0 and now > market.end_date_ts + self._grace_period_seconds:
                    expired.append(market)
                    del self._active[slug]
                    for tid in market.token_ids:
                        self._token_index.pop(tid, None)

        if expired:
            logger.info("Expired %d markets: %s", len(expired), [m.slug for m in expired])
        return expired

    @staticmethod
    def _parse_end_date(end_date_str: str) -> float:
        if not end_date_str:
            return 0.0
        try:
            dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, TypeError):
            return 0.0
