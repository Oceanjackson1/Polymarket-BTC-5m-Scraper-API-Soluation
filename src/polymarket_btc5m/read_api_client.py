from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class ReadApiConfig:
    base_url: str
    bearer_token: str = ""
    verify_tls: bool = True
    timeout_seconds: float = 30.0


class PolymarketReadApiClient:
    def __init__(self, config: ReadApiConfig) -> None:
        base_url = str(config.base_url or "").strip().rstrip("/")
        bearer_token = str(config.bearer_token or "").strip()
        if not base_url:
            raise ValueError("base_url is required")

        self._base_url = base_url
        self._verify_tls = config.verify_tls
        self._timeout_seconds = config.timeout_seconds
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        if bearer_token:
            self._session.headers["Authorization"] = f"Bearer {bearer_token}"

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "PolymarketReadApiClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health", authenticated=False)

    def keysets_index(self) -> dict[str, Any]:
        return self._request("GET", "/v1/keysets/index")

    def keyset_manifest(self, dt: str, timeframe: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/keysets/{dt}/{timeframe}")

    def meta(
        self,
        dataset_name: str,
        *,
        dt: str,
        timeframe: str,
        market_slug: str | None = None,
        columns: list[str] | None = None,
        offset: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        return self._dataset_request(
            f"/v1/meta/{dataset_name}",
            dt=dt,
            timeframe=timeframe,
            market_slug=market_slug,
            columns=columns,
            offset=offset,
            limit=limit,
        )

    def curated(
        self,
        dataset_name: str,
        *,
        dt: str,
        timeframe: str,
        market_slug: str | None = None,
        columns: list[str] | None = None,
        offset: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        return self._dataset_request(
            f"/v1/curated/{dataset_name}",
            dt=dt,
            timeframe=timeframe,
            market_slug=market_slug,
            columns=columns,
            offset=offset,
            limit=limit,
        )

    def _dataset_request(
        self,
        path: str,
        *,
        dt: str,
        timeframe: str,
        market_slug: str | None,
        columns: list[str] | None,
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "dt": dt,
            "timeframe": timeframe,
            "offset": offset,
            "limit": limit,
        }
        if market_slug:
            params["market_slug"] = market_slug
        if columns:
            params["columns"] = ",".join(columns)
        return self._request("GET", path, params=params)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        headers = {}
        if not authenticated:
            headers["Authorization"] = ""

        response = self._session.request(
            method=method,
            url=f"{self._base_url}{path}",
            params=params,
            timeout=self._timeout_seconds,
            verify=self._verify_tls,
            headers=headers or None,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected response payload for {path}")
        return payload
