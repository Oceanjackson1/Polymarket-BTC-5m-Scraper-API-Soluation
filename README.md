# Polymarket BTC Up/Down 多周期采集与发布工具

面向 Polymarket BTC Up/Down 市场的完整数据链路，覆盖 `5m`、`15m`、`1h`、`4h` 四个周期，支持：

- 历史成交回填
- 单市场抓取
- 链上实时成交监听
- CLOB 订单簿录制
- 本地物化为 Parquet 发布目录
- 只读 HTTP API 暴露
- 同步到腾讯云 COS

项目现在不只是“抓 CSV”，而是一个从采集、沉淀、发布到下游读取的完整工作流。

## 核心能力

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 历史批量回填 | `main.py` | 通过 Gamma + Data API 拉取已关闭 BTC 市场历史成交，支持断点续跑 |
| 单市场历史抓取 | `fetch_single_market.py` | 按 `slug` 抓取单个市场成交 |
| 严格链上导出 | `scripts/export_market_trades_chain.py` | 基于 Polygon `OrderFilled` 日志回放单市场成交 |
| 实时成交流 | `stream.py` | 监听 Polygon `newHeads`，解析链上成交并写入 CSV，同时本地 WS 广播 |
| 订单簿录制 | `scripts/run_recorder.py` | 订阅 Polymarket CLOB WebSocket，记录 L2 快照、价格变化、成交、恢复事件 |
| 数据物化 | `scripts/materialize_orderbook_data.py` | 将录制结果整理为 `raw` / `curated` / `meta` 发布目录 |
| 只读数据 API | `scripts/run_data_api.py` | 对外提供 `/health`、`/v1/keysets/*`、`/v1/meta/*`、`/v1/curated/*` |
| COS 同步 | `scripts/sync_to_tencent_cos.py` | 将稳定文件同步到腾讯云 COS |
| 下游读取 | `src/polymarket_btc5m/read_api_client.py` | 提供业务侧 Python 读取客户端 |

## 支持的市场

仅处理 Polymarket BTC Up/Down 系列，识别规则来自市场 `slug` 与 `seriesSlug`：

- `btc-updown-5m-{window_start_ts}`
- `btc-updown-15m-{window_start_ts}`
- `btc-updown-1h-{window_start_ts}`
- `btc-updown-4h-{window_start_ts}`

时间别名也已兼容：

- `hourly` / `60m` -> `1h`
- `4hour` / `240m` -> `4h`

## 系统链路

```text
Gamma API          Data API           CLOB WebSocket           Polygon RPC
   |                  |                      |                      |
   |                  |                      |                      |
   +---- 市场发现 -----+                      |                      |
   |                  +---- 历史成交 --------+                      |
   |                                         +---- 实时订单簿 ------+
   |                                                                |
   +--> main.py / fetch_single_market.py                             |
   +--> scripts/run_recorder.py                                      |
   +--> stream.py / export_market_trades_chain.py -------------------+
                  |
                  v
         output/orderbook / output/trades
                  |
                  v
   scripts/materialize_orderbook_data.py
                  |
                  v
     publish/raw + publish/curated + publish/meta
                  |
          +-------+--------+
          |                |
          v                v
  scripts/run_data_api.py  scripts/sync_to_tencent_cos.py
```

## 项目结构

```text
.
├── main.py
├── fetch_single_market.py
├── stream.py
├── scripts/
│   ├── run_recorder.py
│   ├── materialize_orderbook_data.py
│   ├── run_data_api.py
│   ├── sync_to_tencent_cos.py
│   ├── export_market_trades_chain.py
│   ├── example_read_api_client.py
│   └── query_cos_publish_duckdb.py
├── src/polymarket_btc5m/
├── deploy/
├── tests/
└── samples/
```

## 环境要求

- Python 3.9+
- Linux / macOS
- 可选 Polygon RPC
- 可选腾讯云 COS 凭证

依赖分层如下：

- `requirements.txt`: 基础抓取与实时流
- `requirements-deploy.txt`: 部署侧依赖，包含 `pyarrow`、`fastapi`、`uvicorn`、`cos-python-sdk-v5`
- `requirements-analytics.txt`: 分析侧依赖，额外包含 `duckdb`

## 安装

```bash
git clone https://github.com/Oceanjackson1/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper.git
cd Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

如果你要跑物化、API、COS 同步：

```bash
pip install -r requirements-deploy.txt
```

如果你还要本地用 DuckDB 查 Parquet：

```bash
pip install -r requirements-analytics.txt
```

## 快速开始

### 1. 批量回填历史成交

```bash
python3 main.py --output-dir output

python3 main.py \
  --timeframes 5m,15m,1h,4h \
  --rpc-url https://polygon-rpc.com \
  --output-dir output
```

说明：

- Stage 1: 发现已关闭 BTC 市场
- Stage 2: 拉取成交并可选做毫秒时间戳增强
- Stage 3: 生成校验报告
- 默认支持 checkpoint 断点续跑

### 2. 抓取单个市场

```bash
python3 fetch_single_market.py \
  --slug btc-updown-15m-1772438400 \
  --output-dir output/trades
```

### 3. 严格链上导出单市场成交

适合做链上校验或 Data API 对账。

```bash
python3 scripts/export_market_trades_chain.py \
  --slug btc-updown-15m-1772438400 \
  --rpc-url https://polygon-rpc.com \
  --output-dir output_strict
```

### 4. 实时链上成交流

```bash
python3 stream.py \
  --rpc-url wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --timeframes 5m,15m,1h,4h \
  --output-dir output/trades
```

运行后会：

- 订阅 Polygon `newHeads`
- 解码 `OrderFilled`
- 追加写入成交 CSV
- 在 `ws://127.0.0.1:8765` 广播 JSON

### 5. 录制订单簿

```bash
python3 scripts/run_recorder.py \
  --output-dir output \
  --timeframes 5m,15m,1h,4h \
  --poll-interval 30 \
  --grace-period 120 \
  --flush-interval 5
```

每个市场目录下会产出四类文件：

- `book_snapshots_<slug>.csv`
- `price_changes_<slug>.csv`
- `trades_<slug>.csv`
- `recovery_events_<slug>.csv`

其中 `recovery_events` 用来记录断线、补快照、恢复窗口，便于下游判断数据质量。

### 6. 物化为发布目录

将 `output/orderbook` 中已稳定的市场数据转换为适合下游读取的 `publish` 树。

```bash
python3 scripts/materialize_orderbook_data.py \
  --source-dir output/orderbook \
  --publish-dir publish \
  --once
```

默认行为：

- 仅处理超过稳定时间阈值的市场
- 将原始 CSV 组织到 `raw`
- 转换为 Parquet 到 `curated`
- 生成 `meta/markets`、`meta/files`
- 生成 `meta/keysets/index.json` 和分区级 `manifest.json`

### 7. 启动只读 HTTP API

```bash
python3 scripts/run_data_api.py \
  --publish-dir publish \
  --host 0.0.0.0 \
  --port 8080 \
  --auth-mode bearer \
  --bearer-token YOUR_TOKEN
```

可选鉴权模式：

- `bearer`: 默认，所有 `/v1/*` 需要 Bearer Token
- `none`: 仅适合内网或 VPN 场景

### 8. 同步到腾讯云 COS

```bash
export TENCENT_COS_BUCKET=your-bucket
export TENCENT_COS_REGION=ap-tokyo
export TENCENTCLOUD_SECRET_ID=xxx
export TENCENTCLOUD_SECRET_KEY=xxx

python3 scripts/sync_to_tencent_cos.py \
  --source-dir publish \
  --prefix polymarket-btc \
  --once
```

### 9. 下游读取示例

Python SDK：

```python
from polymarket_btc5m.read_api_client import PolymarketReadApiClient, ReadApiConfig

with PolymarketReadApiClient(
    ReadApiConfig(
        base_url="https://your-host",
        bearer_token="YOUR_TOKEN",
        verify_tls=False,
    )
) as client:
    index = client.keysets_index()
    manifest = client.keyset_manifest("2026-03-02", "15m")
    markets = client.meta("markets", dt="2026-03-02", timeframe="15m", limit=5)
    trades = client.curated("trades", dt="2026-03-02", timeframe="15m", limit=5)
```

CLI 示例：

```bash
python3 scripts/example_read_api_client.py --base-url https://your-host --token YOUR_TOKEN index
python3 scripts/example_read_api_client.py --base-url https://your-host --token YOUR_TOKEN keyset --dt 2026-03-02 --timeframe 15m
```

## 输出目录

### 采集输出

```text
output/
├── trades/
│   ├── trades_btc_up_or_down_5m.csv
│   └── trades_btc_up_or_down_all_timeframes.csv
└── orderbook/
    └── btc-updown-15m-1772438400/
        ├── book_snapshots_<slug>.csv
        ├── price_changes_<slug>.csv
        ├── trades_<slug>.csv
        └── recovery_events_<slug>.csv
```

### 发布输出

```text
publish/
├── raw/
│   └── orderbook/dt=YYYY-MM-DD/timeframe=15m/market_slug=<slug>/
├── curated/
│   ├── book_snapshots/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
│   ├── price_changes/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
│   ├── trades/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
│   └── recovery_events/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
└── meta/
    ├── markets/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
    ├── files/dt=YYYY-MM-DD/timeframe=15m/<slug>.parquet
    └── keysets/
        ├── index.json
        └── dt=YYYY-MM-DD/timeframe=15m/manifest.json
```

## HTTP API

常用接口：

- `GET /health`
- `GET /v1/keysets/index`
- `GET /v1/keysets/{dt}/{timeframe}`
- `GET /v1/meta/markets`
- `GET /v1/meta/files`
- `GET /v1/curated/book_snapshots`
- `GET /v1/curated/price_changes`
- `GET /v1/curated/trades`
- `GET /v1/curated/recovery_events`

查询参数通用字段：

- `dt`
- `timeframe`
- `market_slug`
- `columns`
- `offset`
- `limit`

推荐读取顺序：

1. 先读 `keysets/index`
2. 再读分区 `manifest`
3. 用 `meta/markets` 找目标市场
4. 用 `curated/trades` 或 `curated/price_changes` 做主业务查询
5. 需要排查断线与恢复时再读 `curated/recovery_events`

## 常用命令

```bash
# 运行测试
python3 -m unittest discover -s tests

# 启动本地 API 后做健康检查
curl http://127.0.0.1:8080/health

# 读取受保护接口
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "http://127.0.0.1:8080/v1/keysets/index"
```

## 部署

仓库已经提供部署脚本和 systemd service：

- `deploy/run_orderbook_recorder.sh`
- `deploy/run_materialize.sh`
- `deploy/run_cos_sync.sh`
- `deploy/run_data_api.sh`
- `deploy/systemd/*.service`
- `deploy/nginx/polymarket-data-api.conf`
- `deploy/polymarket-agent.env.example`

生产部署建议直接看这些文档：

- [DEPLOYMENT.md](DEPLOYMENT.md)
- [DATA_ACCESS.md](DATA_ACCESS.md)
- [BUSINESS_GATEWAY.md](BUSINESS_GATEWAY.md)
- [COS_ACCESS.md](COS_ACCESS.md)

## 测试与样例

- `tests/test_data_api_auth.py`: 验证 API 鉴权模式
- `tests/test_materialize_schema.py`: 验证物化 schema 与字符串 ID 保持
- `tests/test_recorder_resnapshot.py`: 验证重连补快照与恢复事件写入
- `samples/`: 保留升级和时间周期扩展校验样本

## 注意事项

- `stream.py` 和严格链上导出依赖 Polygon RPC，推荐使用稳定的专有节点。
- `run_recorder.py` 直接依赖 Polymarket CLOB WebSocket，不需要 RPC。
- 若暴露公网，`run_data_api.py` 不应使用 `auth-mode=none`。
- `materialize_orderbook_data.py` 默认只处理“稳定”文件，避免活跃市场反复重写。
- COS 拉取场景如果没有 `ListBucket` 权限，应通过 `keysets manifest` 或显式对象 key 读取。

## 免责声明

本项目用于数据采集、研究和内部分析，不构成投资建议。请自行评估第三方 API、链上节点和云服务的稳定性与成本。
