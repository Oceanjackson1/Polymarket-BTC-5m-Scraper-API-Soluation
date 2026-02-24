# Upgrade Validation Samples

该目录提供用于验证以下升级需求的数据样本：
- 升级 A：实时 stream 模式（CSV + 本地 WS 推送）
- 升级 B：毫秒级时间戳字段

## 文件说明
- `batch_trades_no_rpc.csv`：模拟 `main.py/fetch_single_market.py` 未传 `--rpc-url`
- `batch_trades_with_rpc.csv`：模拟批量/单市场传入 `--rpc-url` 后的 enrich 输出
- `stream_trades_live.csv`：模拟 `stream.py` 实时采集输出
- `stream_ws_push.jsonl`：模拟本地 WebSocket (`ws://127.0.0.1:8765`) 推送 JSON
- `manifest.json`：测试断言参考（含预期 log_index）
- `validate_samples.py`：一键校验脚本

## 关键字段断言
1. CSV 列顺序保持原字段不变，新增三列在末尾：`timestamp_ms`、`server_received_ms`、`trade_time_ms`
2. 批量无 RPC：三列为空
3. 批量有 RPC：`timestamp_ms = trade_timestamp * 1000 + log_index`，`server_received_ms` 为空
4. 实时 stream：`timestamp_ms` 与 `server_received_ms` 都有值

## 一键校验
```bash
python3 samples/upgrade_validation/validate_samples.py
```

预期输出：`sample_validation_passed`
