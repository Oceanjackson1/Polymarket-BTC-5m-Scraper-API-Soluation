from __future__ import annotations

import json
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DATA_BASE_URL = "https://data-api.polymarket.com"
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolymarketApiClient:
    def __init__(
        self,
        timeout_seconds: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 0.8,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            allowed_methods=["GET"],
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=backoff_factor,
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {"User-Agent": "polymarket-btc-5m-pipeline/1.0 (+https://polymarket.com)"}
        )

    def get_gamma(self, path: str, params: dict[str, Any]) -> Any:
        return self._get_json(f"{GAMMA_BASE_URL}{path}", params=params)

    def get_data(self, path: str, params: dict[str, Any]) -> Any:
        return self._get_json(f"{DATA_BASE_URL}{path}", params=params)

    def _get_json(self, url: str, params: dict[str, Any]) -> Any:
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        # Some responses may contain control chars in long text fields.
        return json.loads(response.text, strict=False)
