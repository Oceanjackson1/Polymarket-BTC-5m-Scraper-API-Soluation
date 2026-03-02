from __future__ import annotations

__all__ = [
    "OrderBookRecorder",
    "PipelineConfig",
    "PolymarketReadApiClient",
    "ReadApiConfig",
    "RecorderConfig",
    "run_pipeline",
]


def __getattr__(name: str) -> object:
    if name in {"PipelineConfig", "run_pipeline"}:
        from .pipeline import PipelineConfig, run_pipeline

        exports = {
            "PipelineConfig": PipelineConfig,
            "run_pipeline": run_pipeline,
        }
        return exports[name]

    if name in {"OrderBookRecorder", "RecorderConfig"}:
        from .recorder import OrderBookRecorder, RecorderConfig

        exports = {
            "OrderBookRecorder": OrderBookRecorder,
            "RecorderConfig": RecorderConfig,
        }
        return exports[name]

    if name in {"PolymarketReadApiClient", "ReadApiConfig"}:
        from .read_api_client import PolymarketReadApiClient, ReadApiConfig

        exports = {
            "PolymarketReadApiClient": PolymarketReadApiClient,
            "ReadApiConfig": ReadApiConfig,
        }
        return exports[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
