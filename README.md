# Polymarket BTC Up/Down 多周期数据采集工具

一个面向 Polymarket BTC 预测市场的数据采集项目，支持：
- 历史成交回填（Gamma API + Data API）
- 单市场成交抓取
- 链上实时成交流（Polygon `newHeads` + `OrderFilled` 日志）
- 毫秒级时间戳增强

本版本已从原先的仅 `5m` 扩展到 `5m / 15m / 1h / 4h` 四个时间周期，且保持原有采集逻辑不变（只扩展时间范畴）。

---

## 1. 支持市场范围

### Polymarket 页面
- `5m`: https://polymarket.com/zh/crypto/5M
- `15m`: https://polymarket.com/zh/crypto/15M
- `1h`: https://polymarket.com/zh/crypto/hourly
- `4h`: https://polymarket.com/zh/crypto/4hour

### 市场识别规则（代码层）
- `slug`: `btc-updown-<timeframe>-<window_start_ts>`
- `event.seriesSlug`: `btc-up-or-down-<timeframe>`
- 支持时间别名：
  - `hourly` / `60m` -> `1h`
  - `4hour` / `240m` -> `4h`

---

## 2. 核心能力

### A. 批量历史回填（`main.py` / `scripts/run_pipeline.py`）
- 阶段 1：Gamma API 索引市场
- 阶段 2：Data API `/trades` 分页拉取成交
- 阶段 3：校验报告输出
- 支持 checkpoint 断点续跑
- 支持可选链上毫秒时间戳 enrich（传 `--rpc-url`）

### B. 单市场抓取（`fetch_single_market.py`）
- 通过 `--slug` 抓一个市场的全部成交
- 输出单市场 CSV
- 支持可选链上毫秒时间戳 enrich

### C. 实时成交流（`stream.py`）
- 市场发现层：按固定间隔轮询 Gamma API，发现活跃市场
- 链上监听层：
  - 订阅 Polygon `newHeads`
  - 每个新区块查询两套合约的 `OrderFilled` 日志
  - 仅保留 maker/taker asset 命中当前活跃 BTC 市场 token 的日志
- 实时写入 CSV，并通过本地 WebSocket 广播 JSON
- 支持断线自动重连（指数退避，最大 60 秒）

### D. 兼容旧能力
- 订单簿录制器仍可用：`scripts/run_recorder.py`
- 严格链上单市场扫描脚本仍可用：`scripts/export_market_trades_chain.py`

---

## 3. 技术栈与要求

- Python `3.9+`
- 依赖：
  - `requests`
  - `websocket-client`
  - `websockets`

安装：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 4. 项目结构

```text
.
├── main.py
├── fetch_single_market.py
├── stream.py
├── requirements.txt
├── scripts
│   ├── run_pipeline.py
│   ├── run_recorder.py
│   └── export_market_trades_chain.py
├── src/polymarket_btc5m
│   ├── client.py
│   ├── chain.py
│   ├── timeframes.py
│   ├── market_tracker.py
│   ├── pipeline.py
│   ├── trade_streamer.py
│   ├── recorder.py
│   └── ws_connection.py
└── samples
    ├── upgrade_validation
    └── timeframe_expansion_validation
```

---

## 5. 快速开始

### 5.1 批量回填（默认四周期）

```bash
python3 main.py --output-dir output
```

或：

```bash
python3 scripts/run_pipeline.py --output-dir output
```

### 5.2 只跑 5m（兼容旧行为）

```bash
python3 main.py --timeframes 5m --output-dir output
```

### 5.3 批量回填 + 毫秒 enrich（可选）

```bash
python3 main.py \
  --timeframes 5m,15m,1h,4h \
  --rpc-url https://polygon-rpc.com \
  --output-dir output
```

### 5.4 单市场抓取

```bash
python3 fetch_single_market.py \
  --slug btc-updown-15m-1773024300 \
  --output-dir output/trades
```

带链上毫秒 enrich：

```bash
python3 fetch_single_market.py \
  --slug btc-updown-15m-1773024300 \
  --rpc-url https://polygon-rpc.com \
  --output-dir output/trades
```

### 5.5 实时 stream

```bash
python3 stream.py \
  --rpc-url wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --timeframes 5m,15m,1h,4h \
  --output-dir output/trades
```

---

## 6. 关键参数

### 6.1 `--timeframes`

支持：`5m,15m,1h,4h`

也支持别名：
- `hourly` -> `1h`
- `4hour` -> `4h`

示例：

```bash
--timeframes 5m,15m,hourly,4hour
```

### 6.2 `--rpc-url`

- 批量/单市场：可选，用于计算 `timestamp_ms`
- 实时 stream：必填（用于订阅 `newHeads`）

---

## 7. 输出规则

### 7.1 批量输出目录

默认在 `output/` 下生成：
- `indexes/`（市场索引）
- `trades/`（成交）
- `reports/`（校验）
- checkpoint 文件

文件命名随 `--timeframes` 变化：

1) `--timeframes 5m`（兼容旧版）
- `run_state.json`
- `indexes/markets_btc_up_or_down_5m.csv`
- `trades/trades_btc_up_or_down_5m.csv`
- `reports/validation_summary.json`

2) `--timeframes 5m,15m,1h,4h`
- `run_state_all_timeframes.json`
- `indexes/markets_btc_up_or_down_all_timeframes.csv`
- `trades/trades_btc_up_or_down_all_timeframes.csv`
- `reports/validation_summary_all_timeframes.json`

3) 子集（例如 `5m,15m`）
- `run_state_5m_15m.json`
- `indexes/markets_btc_up_or_down_5m_15m.csv`
- `trades/trades_btc_up_or_down_5m_15m.csv`
- `reports/validation_summary_5m_15m.json`

### 7.2 实时 stream 输出

- 写入 `--output-dir` 下：`trades_btc_up_or_down_<suffix>.csv`
- 默认四周期：`trades_btc_up_or_down_all_timeframes.csv`
- 同时在本地 WS 推送 JSON（默认 `ws://127.0.0.1:8765`）

### 7.3 CSV 字段（成交）

保持原字段顺序不变，末尾新增三列：
- `timestamp_ms`
- `server_received_ms`
- `trade_time_ms`

完整列顺序：

```text
market_slug,window_start_ts,condition_id,event_id,trade_timestamp,trade_utc,
price,size,notional,side,outcome,asset,proxy_wallet,transaction_hash,dedupe_key,
timestamp_ms,server_received_ms,trade_time_ms
```

---

## 8. 毫秒时间戳规则

### 实时 stream
- `timestamp_ms = block.timestamp * 1000 + log_index`
- `server_received_ms = int(time.time() * 1000)`
- `trade_time_ms` 为 `timestamp_ms` 的 UTC 可读格式

### 批量/单市场
- 传 `--rpc-url`：尝试按交易 receipt + logIndex 计算 `timestamp_ms`
- 不传 `--rpc-url`：`timestamp_ms/server_received_ms/trade_time_ms` 为空

---

## 9. 链上事件监听说明（stream）

监听合约：
- CTF Exchange: `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`
- NegRisk CTF Exchange: `0xc5d563a36ae78145c45a50134d48a1215220f80a`

事件 topic0：
- `OrderFilled`: `0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6`

过滤逻辑：
- 仅保留 makerAssetId 或 takerAssetId 属于当前活跃 BTC 市场 token 的事件

---

## 10. 校验样本

### A. 升级 A/B 综合样本

```bash
python3 samples/upgrade_validation/validate_samples.py
```

预期输出：

```text
sample_validation_passed
```

### B. 四周期扩展专项样本

```bash
python3 samples/timeframe_expansion_validation/validate_logic.py
```

预期输出：

```text
timeframe_expansion_validation_passed
```

---

## 11. 常见问题

### Q1：为什么历史成交覆盖率不是 100%？
Data API `/trades` 存在分页/offset 侧限制，高成交市场可能受影响。可用 `scripts/export_market_trades_chain.py` 做严格链上补齐。

### Q2：1h 市场为什么有时匹配不到？
Polymarket 侧命名可能使用 `hourly` 形式。代码已支持 `hourly/1h/60m` 别名归一。

### Q3：是否必须链上节点？
- 批量/单市场：不必须（仅 enrich 时需要）
- 实时 stream：必须提供可用 Polygon WS RPC

---

## 12. 免责声明

本项目仅用于数据研究与工程学习，请遵守 Polymarket 与相关 API/RPC 服务条款。
