from __future__ import annotations

import json
from pathlib import Path
import secrets
from typing import Any

import pyarrow as pa
from fastapi import FastAPI, Header, HTTPException, Query
import pyarrow.parquet as pq

CURATED_DATASETS = {
    "book_snapshots": ("curated", "book_snapshots"),
    "price_changes": ("curated", "price_changes"),
    "recovery_events": ("curated", "recovery_events"),
    "trades": ("curated", "trades"),
}

META_DATASETS = {
    "markets": ("meta", "markets"),
    "files": ("meta", "files"),
}
STRING_ID_FIELDS = {"asset_id", "condition_id"}
API_AUTH_MODES = {"bearer", "none"}


def keyset_manifest_path(publish_dir: Path, dt: str, timeframe: str) -> Path:
    return publish_dir / "meta" / "keysets" / f"dt={dt}" / f"timeframe={timeframe}" / "manifest.json"


def keyset_index_path(publish_dir: Path) -> Path:
    return publish_dir / "meta" / "keysets" / "index.json"


def load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON at {path}") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail=f"Unexpected JSON payload at {path}")
    return payload


def parse_columns(columns: str | None) -> list[str] | None:
    if not columns:
        return None
    parsed = [item.strip() for item in columns.split(",") if item.strip()]
    return parsed or None


def partition_dir(publish_dir: Path, root: tuple[str, str], dt: str, timeframe: str) -> Path:
    top_level, dataset_name = root
    return publish_dir / top_level / dataset_name / f"dt={dt}" / f"timeframe={timeframe}"


def dataset_files(
    publish_dir: Path,
    root: tuple[str, str],
    dt: str,
    timeframe: str,
    market_slug: str | None,
) -> list[Path]:
    base_dir = partition_dir(publish_dir, root, dt, timeframe)
    if not base_dir.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Dataset partition not found for dt={dt} timeframe={timeframe}",
        )

    if market_slug:
        target = base_dir / f"{market_slug}.parquet"
        if not target.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Market file not found for slug={market_slug}",
            )
        return [target]

    files = sorted(base_dir.glob("*.parquet"))
    if not files:
        raise HTTPException(
            status_code=404,
            detail=f"No parquet files found for dt={dt} timeframe={timeframe}",
        )
    return files


def read_parquet_rows(
    files: list[Path],
    *,
    limit: int,
    offset: int,
    columns: list[str] | None,
) -> dict[str, Any]:
    try:
        tables = [normalize_table(pq.ParquetFile(str(path)).read(columns=columns)) for path in files]
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    total_rows = table.num_rows
    if offset:
        table = table.slice(offset)
    if limit:
        table = table.slice(0, limit)

    return {
        "file_count": len(files),
        "total_rows": total_rows,
        "returned_rows": table.num_rows,
        "rows": table.to_pylist(),
    }


def normalize_table(table: pa.Table) -> pa.Table:
    fields: list[pa.Field] = []
    arrays: list[pa.ChunkedArray] = []

    for field, column in zip(table.schema, table.columns):
        normalized_field = field
        normalized_chunks = []
        if field.name in STRING_ID_FIELDS and not pa.types.is_string(field.type):
            target_type = pa.string()
            normalized_field = pa.field(field.name, target_type, field.nullable, field.metadata)
            for chunk in column.chunks:
                normalized_chunks.append(chunk.cast(target_type))
            arrays.append(pa.chunked_array(normalized_chunks, type=target_type))
        elif pa.types.is_dictionary(field.type):
            value_type = field.type.value_type
            normalized_field = pa.field(field.name, value_type, field.nullable, field.metadata)
            for chunk in column.chunks:
                normalized_chunks.append(chunk.cast(value_type))
            arrays.append(pa.chunked_array(normalized_chunks, type=value_type))
        else:
            arrays.append(column)
        fields.append(normalized_field)

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def unauthorized() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail="Missing or invalid bearer token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def require_bearer_token(
    *,
    expected_token: str,
    authorization: str | None,
) -> None:
    if not expected_token:
        raise HTTPException(status_code=500, detail="API bearer token is not configured.")

    raw_header = str(authorization or "").strip()
    if not raw_header:
        raise unauthorized()

    scheme, _, token = raw_header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise unauthorized()
    if not secrets.compare_digest(token, expected_token):
        raise unauthorized()


def normalize_auth_mode(value: str | None) -> str:
    auth_mode = str(value or "bearer").strip().lower()
    if auth_mode not in API_AUTH_MODES:
        supported = ", ".join(sorted(API_AUTH_MODES))
        raise ValueError(f"Unsupported API auth mode '{value}'. Supported: {supported}")
    return auth_mode


def require_request_auth(
    *,
    auth_mode: str,
    expected_token: str,
    authorization: str | None,
) -> None:
    if auth_mode == "none":
        return
    require_bearer_token(
        expected_token=expected_token,
        authorization=authorization,
    )


def create_app(
    *,
    publish_dir: Path,
    default_limit: int = 200,
    max_limit: int = 5000,
    bearer_token: str,
    auth_mode: str = "bearer",
) -> FastAPI:
    app = FastAPI(
        title="Polymarket Publish API",
        version="1.0.0",
        description="HTTP access to local publish/keyset/meta/curated data.",
    )
    app.state.publish_dir = publish_dir
    app.state.default_limit = default_limit
    app.state.max_limit = max_limit
    app.state.auth_mode = normalize_auth_mode(auth_mode)
    app.state.bearer_token = str(bearer_token or "").strip()

    if app.state.auth_mode == "bearer" and not app.state.bearer_token:
        raise ValueError("A non-empty bearer token is required for the data API.")

    @app.get("/health")
    def health() -> dict[str, Any]:
        index_path = keyset_index_path(app.state.publish_dir)
        return {
            "status": "ok",
            "publish_dir": str(app.state.publish_dir),
            "keyset_index_exists": index_path.exists(),
            "auth_mode": app.state.auth_mode,
        }

    @app.get("/v1/keysets/index")
    def get_keyset_index(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        require_request_auth(
            auth_mode=app.state.auth_mode,
            expected_token=app.state.bearer_token,
            authorization=authorization,
        )
        return load_json(keyset_index_path(app.state.publish_dir))

    @app.get("/v1/keysets/{dt}/{timeframe}")
    def get_keyset_manifest(
        dt: str,
        timeframe: str,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        require_request_auth(
            auth_mode=app.state.auth_mode,
            expected_token=app.state.bearer_token,
            authorization=authorization,
        )
        return load_json(keyset_manifest_path(app.state.publish_dir, dt, timeframe))

    @app.get("/v1/meta/{dataset_name}")
    def get_meta_dataset(
        dataset_name: str,
        dt: str,
        timeframe: str,
        market_slug: str | None = None,
        columns: str | None = None,
        offset: int = Query(0, ge=0),
        limit: int = Query(default_limit, ge=1, le=max_limit),
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        require_request_auth(
            auth_mode=app.state.auth_mode,
            expected_token=app.state.bearer_token,
            authorization=authorization,
        )
        if dataset_name not in META_DATASETS:
            supported = ", ".join(sorted(META_DATASETS))
            raise HTTPException(status_code=404, detail=f"Unsupported meta dataset: {dataset_name}. Supported: {supported}")

        files = dataset_files(app.state.publish_dir, META_DATASETS[dataset_name], dt, timeframe, market_slug)
        result = read_parquet_rows(
            files,
            limit=limit,
            offset=offset,
            columns=parse_columns(columns),
        )
        result.update(
            {
                "dataset": f"meta/{dataset_name}",
                "dt": dt,
                "timeframe": timeframe,
                "market_slug": market_slug,
            }
        )
        return result

    @app.get("/v1/curated/{dataset_name}")
    def get_curated_dataset(
        dataset_name: str,
        dt: str,
        timeframe: str,
        market_slug: str | None = None,
        columns: str | None = None,
        offset: int = Query(0, ge=0),
        limit: int = Query(default_limit, ge=1, le=max_limit),
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        require_request_auth(
            auth_mode=app.state.auth_mode,
            expected_token=app.state.bearer_token,
            authorization=authorization,
        )
        if dataset_name not in CURATED_DATASETS:
            supported = ", ".join(sorted(CURATED_DATASETS))
            raise HTTPException(status_code=404, detail=f"Unsupported curated dataset: {dataset_name}. Supported: {supported}")

        files = dataset_files(app.state.publish_dir, CURATED_DATASETS[dataset_name], dt, timeframe, market_slug)
        result = read_parquet_rows(
            files,
            limit=limit,
            offset=offset,
            columns=parse_columns(columns),
        )
        result.update(
            {
                "dataset": f"curated/{dataset_name}",
                "dt": dt,
                "timeframe": timeframe,
                "market_slug": market_slug,
            }
        )
        return result

    return app
