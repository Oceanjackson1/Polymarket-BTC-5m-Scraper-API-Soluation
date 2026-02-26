# Polymarket BTC Up/Down 多周期数据采集工具

一个面向 **Polymarket BTC 预测市场**的完整数据采集工具包，支持历史成交回填、单市场抓取、链上实时流式采集、实时订单簿录制，覆盖 **4 个时间周期**（`5m`、`15m`、`1h`、`4h`）。

---

## 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [支持的市场范围](#支持的市场范围)
- [数据来源与 API](#数据来源与-api)
- [安装部署](#安装部署)
- [快速开始](#快速开始)
  - [批量历史回填](#1-批量历史回填)
  - [单市场抓取](#2-单市场抓取)
  - [实时流式采集](#3-实时流式采集)
  - [订单簿录制](#4-订单簿录制)
- [命令行参数参考](#命令行参数参考)
- [输出数据表结构](#输出数据表结构)
  - [表一：Trades 历史成交表（已实现交易）](#表一trades-历史成交表已实现交易)
  - [表二：Book Snapshots 订单簿快照表（L2）](#表二book-snapshots-订单簿快照表l2)
  - [表三：Price Changes 价格变动表](#表三price-changes-价格变动表)
  - [表四：WS Trades 实时成交表](#表四ws-trades-实时成交表)
  - [表五：Markets Index 市场索引表](#表五markets-index-市场索引表)
- [输出目录结构](#输出目录结构)
- [毫秒时间戳增强](#毫秒时间戳增强)
- [链上事件监听](#链上事件监听)
- [核心概念](#核心概念)
- [项目结构](#项目结构)
- [模块说明](#模块说明)
- [验证测试](#验证测试)
- [常见问题](#常见问题)
- [免责声明](#免责声明)

---

## 项目概述

Polymarket 运行 BTC Up/Down 预测市场，交易者在固定时间窗口内押注比特币价格是涨（Up）还是跌（Down）。这些市场运行在 **Polygon 区块链**上，可通过 Polymarket 的 API 和 CLOB（中央限价订单簿）WebSocket 访问。

本项目提供 **5 种数据采集模式**：

| 模式 | 入口文件 | 数据来源 | 数据类型 | 可否回溯历史？ |
|------|----------|----------|----------|:--------------:|
| **批量历史回填** | `main.py` | Data API | 历史成交 | 可以 |
| **单市场抓取** | `fetch_single_market.py` | Data API | 历史成交 | 可以 |
| **实时流式采集** | `stream.py` | Polygon RPC | 实时成交 | 不可以 |
| **订单簿录制** | `scripts/run_recorder.py` | CLOB WebSocket | L2 订单簿 + 成交 | 不可以 |
| **严格链上导出** | `scripts/export_market_trades_chain.py` | Polygon RPC | 链上成交 | 可以 |

---

## 系统架构

```
                    ┌─────────────────────────────────────────────┐
                    │              Polymarket 平台                 │
                    ├──────────┬──────────┬───────────────────────┤
                    │ Gamma API│ Data API │     CLOB WebSocket    │
                    │ (市场索引) │ (历史成交) │  (实时订单簿 & 成交)   │
                    └────┬─────┴────┬─────┴──────────┬────────────┘
                         │          │                 │
                    ┌────▼──────────▼────┐   ┌───────▼──────────┐
                    │  main.py           │   │ run_recorder.py   │
                    │  fetch_single.py   │   │  (WS 订阅器)      │
                    │  (HTTP 分页拉取)    │   └───────┬──────────┘
                    └────────┬──────────┘            │
                             │                       │
    ┌────────────────────────┤               ┌───────▼──────────┐
    │ Polygon 区块链          │               │ 订单簿 CSV 文件    │
    │ (OrderFilled 事件)      │               │ - book_snapshots  │
    │         │              │               │ - price_changes   │
    │    ┌────▼────┐         │               │ - ws_trades       │
    │    │stream.py│         │               └──────────────────┘
    │    └────┬────┘         │
    │         │              │
    │    ┌────▼──────────────▼──┐
    │    │     成交 CSV 文件     │
    │    │   (每行 18 个字段)    │
    │    └──────────────────────┘
    └────────────────────────────
```

---

## 支持的市场范围

### Polymarket 页面

| 时间周期 | 市场页面 | Slug 模式 |
|----------|----------|-----------|
| **5 分钟** | https://polymarket.com/crypto/5M | `btc-updown-5m-{时间戳}` |
| **15 分钟** | https://polymarket.com/crypto/15M | `btc-updown-15m-{时间戳}` |
| **1 小时** | https://polymarket.com/crypto/hourly | `btc-updown-1h-{时间戳}` |
| **4 小时** | https://polymarket.com/crypto/4hour | `btc-updown-4h-{时间戳}` |

### 市场识别规则

- **Slug 格式**：`btc-updown-<时间周期>-<窗口开始时间戳>`
- **系列 Slug**：`btc-up-or-down-<时间周期>`
- **标签 ID**：`235`（Polymarket 上的 BTC 市场标签）
- **时间别名**：`hourly` / `60m` → `1h`，`4hour` / `240m` → `4h`

每个市场有**两个结果**：`Up`（BTC 价格上涨）和 `Down`（BTC 价格下跌），各自对应一个链上 Token（资产）。

---

## 数据来源与 API

| 数据来源 | 地址 | 用途 | 是否需要认证 |
|----------|------|------|:------------:|
| **Gamma API** | `https://gamma-api.polymarket.com` | 市场发现与索引 | 否 |
| **Data API** | `https://data-api.polymarket.com` | 历史成交记录 | 否 |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | 实时订单簿与成交 | 否 |
| **Polygon RPC** | 用户自行提供（如 Alchemy、Infura） | 区块链事件与时间戳 | 视服务商 |

### 使用到的 API 端点

```
GET /markets?tag_id=235&closed=false&active=true    # Gamma：发现市场
GET /trades?market={condition_id}&limit=1000         # Data：分页拉取成交
WSS /ws/market                                       # CLOB：实时事件推送
eth_subscribe("newHeads")                            # Polygon：新区块订阅
eth_getLogs(topics=[OrderFilled])                     # Polygon：成交事件查询
eth_getTransactionReceipt(tx_hash)                   # Polygon：交易详情
```

---

## 安装部署

### 环境要求

- **Python 3.9+**
- **pip**（Python 包管理器）

### 安装步骤

```bash
git clone https://github.com/Oceanjackson1/Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper.git
cd Polymarket-BTC-UpDown-MultiTimeframe-Trade-Scraper

# 创建虚拟环境（推荐）
python3 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 依赖项

| 包名 | 版本要求 | 用途 |
|------|----------|------|
| `requests` | ≥2.32.0 | Gamma / Data API 的 HTTP 客户端 |
| `websocket-client` | ≥1.7.0 | CLOB WebSocket 连接 |
| `websockets` | ≥12.0 | 本地 WebSocket 广播服务器（流式模式） |

---

## 快速开始

### 1. 批量历史回填

从已关闭的 BTC 市场中拉取所有历史成交记录：

```bash
# 全部 4 个时间周期（默认）
python3 main.py --output-dir output

# 只跑 5 分钟市场
python3 main.py --timeframes 5m --output-dir output

# 带毫秒时间戳增强（需要 RPC）
python3 main.py --timeframes 5m,15m,1h,4h \
  --rpc-url https://polygon-rpc.com \
  --output-dir output
```

流水线分 3 个阶段运行：
1. **市场索引** — 从 Gamma API 发现所有已关闭的 BTC 市场
2. **成交回填** — 对每个市场分页拉取 Data API `/trades`
3. **校验报告** — 生成覆盖率/质量报告

支持 **断点续跑（Checkpoint）**：可随时中断并恢复。

### 2. 单市场抓取

抓取指定市场的全部成交记录：

```bash
python3 fetch_single_market.py \
  --slug btc-updown-5m-1772089200 \
  --output-dir output/trades

# 带毫秒时间戳增强
python3 fetch_single_market.py \
  --slug btc-updown-4h-1772082000 \
  --rpc-url https://polygon-rpc.com \
  --output-dir output/trades
```

输出文件：`output/trades/trades_{slug}.csv`

### 3. 实时流式采集

通过订阅 Polygon 区块链事件监听实时成交：

```bash
python3 stream.py \
  --rpc-url wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --timeframes 5m,15m,1h,4h \
  --output-dir output/trades
```

运行后将会：
- 订阅 Polygon `newHeads`（新区块通知）
- 解码 CTF Exchange 合约上的 `OrderFilled` 事件
- 实时写入 CSV 文件
- 通过本地 WebSocket `ws://127.0.0.1:8765` 广播 JSON 数据

**需要 Polygon WebSocket RPC 端点**（Alchemy、Infura、QuickNode 等）

### 4. 订单簿录制

录制实时 L2 订单簿快照、价格变动和成交：

```bash
python3 scripts/run_recorder.py \
  --output-dir output \
  --poll-interval 30 \
  --grace-period 120
```

运行后将会：
- 通过 Gamma API 自动发现所有活跃的 BTC 市场
- 订阅 CLOB WebSocket 获取实时事件
- 每个市场生成 3 个 CSV 文件：`book_snapshots`、`price_changes`、`trades`
- 输出到 `output/orderbook/{市场slug}/`

**无需 RPC** — 直接使用 Polymarket 的 CLOB WebSocket。

---

## 命令行参数参考

### `main.py` — 批量回填流水线

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | `output/` | 输出根目录 |
| `--timeframes` | `5m,15m,1h,4h` | 逗号分隔的时间周期 |
| `--rpc-url` | _(无)_ | Polygon RPC 地址，用于毫秒时间戳增强 |
| `--market-limit` | _(全部)_ | 限制处理的市场数（测试用） |
| `--no-resume` | `false` | 忽略断点，从头开始 |
| `--include-zero-volume` | `false` | 包含零成交量的市场 |
| `--request-delay-seconds` | `0.10` | API 请求间隔（秒） |
| `--timeout-seconds` | `30` | HTTP 超时时间 |
| `--max-retries` | `5` | 最大重试次数 |
| `--log-level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

### `fetch_single_market.py` — 单市场抓取

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--slug` | _(必填)_ | 市场 slug，如 `btc-updown-5m-1772089200` |
| `--output-dir` | `output/trades/` | 输出目录 |
| `--rpc-url` | _(无)_ | Polygon RPC 地址（可选） |
| `--timeframes` | `5m,15m,1h,4h` | 用于 slug 校验 |
| `--request-delay-seconds` | `0.10` | API 请求间隔 |
| `--timeout-seconds` | `30` | HTTP 超时时间 |
| `--max-retries` | `5` | 最大重试次数 |
| `--log-level` | `INFO` | 日志级别 |

### `stream.py` — 实时流式采集

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--rpc-url` | _(必填)_ | Polygon WebSocket RPC 端点 |
| `--output-dir` | `output/trades/` | 输出目录 |
| `--timeframes` | `5m,15m,1h,4h` | 监听的时间周期 |
| `--market-poll-interval` | `300.0` | 市场发现轮询间隔（秒） |
| `--ws-host` | `127.0.0.1` | 本地 WebSocket 广播地址 |
| `--ws-port` | `8765` | 本地 WebSocket 广播端口 |
| `--grace-period` | `120.0` | 市场结束后继续录制时间（秒） |
| `--timeout-seconds` | `30` | HTTP 超时时间 |
| `--max-retries` | `5` | 最大重试次数 |
| `--log-level` | `INFO` | 日志级别 |

### `scripts/run_recorder.py` — 订单簿录制器

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | `output/` | 输出根目录 |
| `--poll-interval` | `30.0` | 市场发现轮询间隔（秒） |
| `--grace-period` | `120.0` | 市场结束后继续录制时间（秒） |
| `--timeout-seconds` | `30` | HTTP 超时时间 |
| `--max-retries` | `5` | 最大重试次数 |
| `--log-level` | `INFO` | 日志级别 |

### `scripts/export_market_trades_chain.py` — 严格链上导出

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--slug` | _(必填)_ | 市场 slug |
| `--output-dir` | `output_strict/` | 输出目录 |
| `--rpc-url` | _(可重复)_ | 一个或多个 Polygon RPC 地址 |
| `--request-timeout-seconds` | `12` | RPC 超时时间 |
| `--max-rpc-retries` | `4` | RPC 重试次数 |
| `--initial-span-blocks` | `100` | 初始扫描区块范围 |
| `--min-span-blocks` | `1` | 最小扫描区块范围 |
| `--buffer-before-seconds` | `300` | 市场开始前的扫描缓冲（秒） |
| `--buffer-after-seconds` | `7200` | 市场结束后的扫描缓冲（秒） |
| `--compare-csv` | _(无)_ | 用于交叉比对的 CSV 文件 |

---

## 输出数据表结构

### 表一：Trades 历史成交表（已实现交易）

**来源**：Data API（历史回溯）或 Polygon RPC（实时流）
**文件**：`trades/trades_btc_up_or_down_{后缀}.csv` 或 `trades/trades_{slug}.csv`

每一行 = 一笔**链上确认的真实成交**。

| # | 字段 | 类型 | 说明 |
|---|------|------|------|
| 1 | `market_slug` | string | 市场标识符，如 `btc-updown-5m-1772089200` |
| 2 | `window_start_ts` | int | 市场窗口开始时间（Unix 秒） |
| 3 | `condition_id` | string | 市场条件合约地址（0x 哈希），每个市场唯一 |
| 4 | `event_id` | int | Polymarket 平台事件编号 |
| 5 | `trade_timestamp` | int | 成交时间（Unix 秒） |
| 6 | `trade_utc` | string | 成交时间的 UTC ISO 8601 格式 |
| 7 | `price` | float | 成交价格 [0, 1]，代表该结果发生的概率 |
| 8 | `size` | float | 成交数量（合约数量） |
| 9 | `notional` | float | 名义金额（USDC）= `price × size` |
| 10 | `side` | string | `BUY`（买入）或 `SELL`（卖出），Taker 方向 |
| 11 | `outcome` | string | `Up`（BTC 涨）或 `Down`（BTC 跌） |
| 12 | `asset` | string | Token ID（256 位整数） |
| 13 | `proxy_wallet` | string | 交易者的代理钱包地址 |
| 14 | `transaction_hash` | string | 链上交易哈希，可在 Polygonscan 查询 |
| 15 | `dedupe_key` | string | 去重键，由 `tx_hash|asset|side|timestamp|price|size` 组合 |
| 16 | `timestamp_ms` | int/空 | 毫秒时间戳（需 RPC 增强） |
| 17 | `server_received_ms` | int/空 | 服务器接收毫秒时间戳（仅流式模式） |
| 18 | `trade_time_ms` | string/空 | 毫秒精度的 ISO 8601 格式（需 RPC 增强） |

### 表二：Book Snapshots 订单簿快照表（L2）

**来源**：CLOB WebSocket `book` 事件
**文件**：`orderbook/{slug}/book_snapshots_{slug}.csv`

每一行 = 订单簿中**某个价格档位的挂单状态**。同一 `snapshot_seq` 下的所有行组成一次完整的 L2 订单簿快照。

| # | 字段 | 类型 | 说明 |
|---|------|------|------|
| 1 | `snapshot_seq` | int | 快照序号（递增），同一序号 = 一次完整订单簿状态 |
| 2 | `server_ts_ms` | int | 服务器端毫秒时间戳（Polymarket 生成） |
| 3 | `receive_ts_ms` | int | 本地接收毫秒时间戳 |
| 4 | `receive_utc` | string | 本地接收时间 UTC ISO 8601 格式 |
| 5 | `asset_id` | string | Token ID（256 位整数），区分 Up/Down |
| 6 | `outcome` | string | `Up` 或 `Down` |
| 7 | `condition_id` | string | 市场条件合约地址 |
| 8 | `side` | string | `bid`（买盘挂单）或 `ask`（卖盘挂单） |
| 9 | `price` | float | 价格档位 [0, 1] |
| 10 | `size` | float | 该价格的**总挂单量** |
| 11 | `level_index` | int | 档位索引，0 = 最优价格（最靠近市场价） |

> **还原完整订单簿**：筛选同一 `snapshot_seq`，按 `outcome` + `side` 分组，再按 `level_index` 排序

### 表三：Price Changes 价格变动表

**来源**：CLOB WebSocket `price_change` 事件
**文件**：`orderbook/{slug}/price_changes_{slug}.csv`

每一行 = 订单簿中**某个价格档位发生了变化**（新增挂单、撤单、部分成交）。

| # | 字段 | 类型 | 说明 |
|---|------|------|------|
| 1 | `server_ts_ms` | int | 服务器端毫秒时间戳 |
| 2 | `receive_ts_ms` | int | 本地接收毫秒时间戳 |
| 3 | `receive_utc` | string | 本地接收时间 UTC ISO 8601 格式 |
| 4 | `condition_id` | string | 市场条件合约地址 |
| 5 | `asset_id` | string | Token ID |
| 6 | `outcome` | string | `Up` 或 `Down` |
| 7 | `side` | string | `BUY`（买盘变动）或 `SELL`（卖盘变动） |
| 8 | `price` | float | 发生变动的价格档位 |
| 9 | `size` | float | 变动后的**新挂单量**（`0` = 该档位已清空） |
| 10 | `best_bid` | float | 变动后的**最优买价**（BBO 买一价） |
| 11 | `best_ask` | float | 变动后的**最优卖价**（BBO 卖一价） |

> **用途**：增量数据，可在 Book Snapshot 基础上实时更新订单簿状态。`best_bid` / `best_ask` 可直接用于计算买卖价差（spread）

### 表四：WS Trades 实时成交表

**来源**：CLOB WebSocket `last_trade_price` 事件
**文件**：`orderbook/{slug}/trades_{slug}.csv`

每一行 = 一笔**实时确认的成交**，属于已实现交易（Realized Data）。

| # | 字段 | 类型 | 说明 |
|---|------|------|------|
| 1 | `server_ts_ms` | int | 服务器端毫秒时间戳 |
| 2 | `receive_ts_ms` | int | 本地接收毫秒时间戳 |
| 3 | `receive_utc` | string | 本地接收时间 UTC ISO 8601 格式 |
| 4 | `condition_id` | string | 市场条件合约地址 |
| 5 | `asset_id` | string | Token ID |
| 6 | `outcome` | string | `Up` 或 `Down` |
| 7 | `side` | string | `BUY` 或 `SELL`（Taker 方向） |
| 8 | `price` | float | 成交价格 [0, 1] |
| 9 | `size` | float | 成交数量 |
| 10 | `fee_rate_bps` | int | 手续费率（基点，1 bps = 0.01%），`0` = 免手续费 |

### 表五：Markets Index 市场索引表

**来源**：Gamma API 市场发现
**文件**：`indexes/markets_btc_up_or_down_{后缀}.csv`

| # | 字段 | 类型 | 说明 |
|---|------|------|------|
| 1 | `market_id` | string | Polymarket 市场 ID |
| 2 | `event_id` | int | 平台事件编号 |
| 3 | `condition_id` | string | 条件合约地址 |
| 4 | `slug` | string | 市场 slug 标识符 |
| 5 | `clob_token_ids` | string | Token ID（竖线分隔） |
| 6 | `outcomes` | string | 结果标签（竖线分隔） |
| 7 | `window_start_ts` | int | 市场窗口开始时间（Unix 秒） |
| 8 | `window_start_utc` | string | 窗口开始时间 UTC ISO 8601 格式 |
| 9 | `market_end_utc` | string | 市场结束时间 UTC ISO 8601 格式 |
| 10 | `volume` | float | 总成交量 |
| 11 | `market_url` | string | 市场链接（英文） |
| 12 | `market_url_zh` | string | 市场链接（中文） |

---

## 输出目录结构

### 批量流水线输出

```
output/
├── run_state_{后缀}.json              # 断点文件（用于续跑）
├── indexes/
│   └── markets_btc_up_or_down_{后缀}.csv
├── trades/
│   └── trades_btc_up_or_down_{后缀}.csv
└── reports/
    └── validation_summary_{后缀}.json
```

文件命名随 `--timeframes` 参数变化：

| 时间周期 | 后缀 | 示例 |
|----------|------|------|
| 仅 `5m` | `5m` | `trades_btc_up_or_down_5m.csv` |
| 全部四个 | `all_timeframes` | `trades_btc_up_or_down_all_timeframes.csv` |
| 子集 | 拼接 | `trades_btc_up_or_down_5m_15m.csv` |

### 订单簿录制输出

```
output/orderbook/{市场slug}/
├── book_snapshots_{slug}.csv    # L2 订单簿快照
├── price_changes_{slug}.csv     # 订单簿增量变动
└── trades_{slug}.csv            # 实时成交
```

---

## 毫秒时间戳增强

### 原理

Data API 返回的成交时间戳是**秒级精度**。如需更高精度，本工具可通过 Polygon RPC 增强至**毫秒级**：

```
timestamp_ms = 区块时间戳 × 1000 + log_index
```

其中 `log_index` 是 `OrderFilled` 事件在交易回执中的位置索引。

### 各采集模式对比

| 模式 | `timestamp_ms` | `server_received_ms` | `trade_time_ms` |
|------|:-:|:-:|:-:|
| **批量回填**（无 `--rpc-url`） | 空 | 空 | 空 |
| **批量回填**（有 `--rpc-url`） | 计算值 | 空 | UTC 格式 |
| **流式采集**（必须有 RPC） | 计算值 | `time.time() × 1000` | UTC 格式 |
| **订单簿录制** | 不适用 | 原生 `receive_ts_ms` | 不适用 |

订单簿录制器原生提供毫秒时间戳：`server_ts_ms`（服务器端）和 `receive_ts_ms`（本地时钟）。

---

## 链上事件监听

### 监听的合约

| 合约 | 地址 | 用途 |
|------|------|------|
| CTF Exchange | `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e` | 标准市场交易 |
| NegRisk CTF Exchange | `0xc5d563a36ae78145c45a50134d48a1215220f80a` | 负风险市场交易 |

### 事件签名

```
OrderFilled (topic0): 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
```

### 交易方向判断逻辑

- `maker_asset_id` ∈ 市场 Token 且 `taker_asset_id` = USDC → **SELL（卖出）**
- `taker_asset_id` ∈ 市场 Token 且 `maker_asset_id` = USDC → **BUY（买入）**

---

## 核心概念

### 已实现数据（Realized Data）vs 订单簿数据（Order Book Data）

| | 已实现数据（成交记录） | 订单簿数据 |
|---|---|---|
| **定义** | 链上确认的撮合成交 | 未成交的挂单状态 |
| **对应表** | 表一（Trades）、表四（WS Trades） | 表二（Book Snapshots）、表三（Price Changes） |
| **可否回溯** | 可以（Data API 保留历史） | 不可以（必须实时录制） |
| **包含内容** | 价格、数量、钱包地址、交易哈希 | 买卖盘深度、各档位挂单量 |

### 订单簿层级

| 层级 | 说明 | 本项目是否支持 |
|------|------|:--------------:|
| **L1** | 仅最优买卖价（BBO） | 支持（通过 price_changes 的 `best_bid`/`best_ask`） |
| **L2** | 按价格聚合的各档位 | **支持**（book_snapshots） |
| **L3** | 每笔独立挂单明细 | 不支持 |

### 价格的含义

Polymarket 上的价格代表**概率**：
- `Up` 的 `price = 0.85` → 85% 概率 BTC 上涨
- `Down` 的 `price = 0.15` → 15% 概率 BTC 下跌
- 互补关系：`Up 价格 + Down 价格 ≈ 1.00`

### 表一 vs 表四的区别

| 对比项 | 表一：Trades 历史成交 | 表四：WS Trades 实时成交 |
|--------|----------------------|--------------------------|
| 数据来源 | Data API（链上确认） | CLOB WebSocket（实时推送） |
| 时间精度 | 秒级（可 RPC 补毫秒） | 原生毫秒级 |
| 是否可回溯 | 可以（市场关闭后仍可获取） | 不可以（必须实时录制） |
| 钱包地址 | 有 `proxy_wallet` | 无 |
| 交易哈希 | 有 `transaction_hash` | 无 |
| 手续费 | 无 | 有 `fee_rate_bps` |
| 名义金额 | 有 `notional` 字段 | 无（需自行计算 `price × size`） |

---

## 项目结构

```
.
├── main.py                       # 批量回填入口
├── fetch_single_market.py        # 单市场抓取入口
├── stream.py                     # 实时流式采集入口
├── requirements.txt              # Python 依赖
│
├── src/polymarket_btc5m/         # 核心库
│   ├── __init__.py               # 包导出
│   ├── client.py                 # HTTP 客户端（Gamma + Data API）
│   ├── timeframes.py             # 时间周期解析与校验
│   ├── chain.py                  # Polygon RPC 与 OrderFilled 事件解码
│   ├── market_tracker.py         # 市场发现与生命周期追踪
│   ├── pipeline.py               # 三阶段批量流水线（含断点续跑）
│   ├── trade_streamer.py         # 实时成交流服务
│   ├── recorder.py               # 订单簿录制器（CLOB WS）
│   └── ws_connection.py          # CLOB WebSocket 连接管理
│
├── scripts/
│   ├── run_pipeline.py           # 批量流水线的替代入口
│   ├── run_recorder.py           # 订单簿录制守护进程
│   └── export_market_trades_chain.py  # 严格链上成交导出
│
└── samples/
    ├── upgrade_validation/       # 时间戳增强验证测试
    └── timeframe_expansion_validation/  # 多周期验证测试
```

---

## 模块说明

### `client.py` — Polymarket API 客户端

HTTP 抽象层，支持自动重试、指数退避和速率限制处理。

- **`PolymarketApiClient`**：主客户端类
  - `get_gamma(path, params)` — 查询 Gamma API
  - `get_data(path, params)` — 查询 Data API
- **特性**：429/5xx 自动重试、Retry-After 头支持、控制字符清洗

### `timeframes.py` — 时间周期管理

4 个支持周期的解析、校验和标准化。

- `normalize_timeframe(value)` — 别名转换（`hourly` → `1h`）
- `parse_timeframes_csv(raw)` — 解析 CLI 输入 `"5m,15m,1h,4h"`
- `match_btc_updown_market(market, enabled_timeframes)` — 匹配市场到时间周期
- `timeframe_file_suffix(enabled_timeframes)` — 生成输出文件名后缀

### `chain.py` — Polygon 区块链集成

RPC 交互与 `OrderFilled` 事件解码。

- **`PolygonRpcClient`**：JSON-RPC 客户端（含重试）
  - `get_transaction_receipt(tx_hash)` — 获取交易回执
  - `get_block_timestamp(block_number)` — 获取区块时间戳（带缓存）
  - `get_order_filled_logs(block_number)` — 查询 OrderFilled 事件
- **`decode_order_filled_log(log)`** — 解析 Solidity 编码的事件数据

### `market_tracker.py` — 市场发现

线程安全的市场生命周期追踪器。

- **`MarketTracker`**：发现并追踪活跃的 BTC 市场
  - `poll_once()` → `(新增市场, 过期市场)`
  - `resolve_token(token_id)` → `(slug, outcome)` 映射
  - `get_all_token_ids()` — 列出所有已追踪的 Token ID

### `pipeline.py` — 批量数据流水线

三阶段 ETL 流水线，支持断点续跑。

- **阶段 1**：市场索引（Gamma API 分页）
- **阶段 2**：成交回填（Data API 逐市场分页）
- **阶段 3**：校验报告
- **`TradeTimestampEnricher`**：可选的 RPC 毫秒时间戳增强

### `trade_streamer.py` — 实时成交流

区块链事件监听器 + 本地 WebSocket 广播。

- **`TradeStreamService`**：主服务
  - Polygon `newHeads` 订阅 → 区块队列 → 成交提取
  - CSV 输出 + `ws://127.0.0.1:8765` JSON 广播
  - LRU 去重（4096 个区块、100K 条日志 UID）

### `recorder.py` — 订单簿录制器

CLOB WebSocket 事件录制，按市场输出 CSV。

- **`OrderBookRecorder`**：主录制守护进程
  - 处理 `book`、`price_change`、`last_trade_price` 事件
  - 自动发现市场、管理订阅
  - 每个市场 3 个 CSV 文件：快照、变动、成交

### `ws_connection.py` — WebSocket 管理器

持久化 CLOB WebSocket 连接，支持自动重连。

- **`ClobWebSocket`**：连接管理器
  - 指数退避重连（基数 1 秒，最大 60 秒）
  - 10 秒心跳 ping
  - 线程安全的订阅/取消订阅

---

## 验证测试

### 时间戳增强验证

```bash
python3 samples/upgrade_validation/validate_samples.py
# 预期输出：sample_validation_passed
```

验证内容：
- 无 RPC 的批量成交 → 毫秒字段为空
- 有 RPC 的批量成交 → `timestamp_ms` = `trade_timestamp × 1000 + log_index`
- 流式成交 → 全部 3 个毫秒字段均有值

### 多周期扩展验证

```bash
python3 samples/timeframe_expansion_validation/validate_logic.py
# 预期输出：timeframe_expansion_validation_passed
```

验证内容：
- 时间别名标准化
- 4 个周期的市场 slug 匹配
- 输出文件路径生成逻辑

---

## 常见问题

### Q：已关闭的市场还能获取订单簿数据吗？

**不能。** 订单簿数据只在市场活跃期间存在。市场结算后，所有挂单被清除。必须在**市场活跃期间**运行订单簿录制器才能采集。

### Q：是否必须有 Polygon RPC？

| 模式 | 是否需要 RPC？ |
|------|:-:|
| 批量历史回填 | 可选（仅用于毫秒增强） |
| 单市场抓取 | 可选（仅用于毫秒增强） |
| 实时流式采集 | **必须**（WebSocket RPC） |
| 订单簿录制 | 不需要 |

### Q：Data API 在 offset=4000 时返回 400 错误？

Data API 存在分页上限。高成交量市场可能超出此限制。如需完整数据，可使用 `scripts/export_market_trades_chain.py` 直接扫描链上事件。

### Q：如何用快照数据还原完整的 L2 订单簿？

筛选 `snapshot_seq`，按 `outcome` + `side` 分组，按 `level_index` 排序：

```python
import pandas as pd

df = pd.read_csv("book_snapshots_btc-updown-4h-1772082000.csv")
snapshot = df[df["snapshot_seq"] == 1]

# Up Token 的买盘（bid），按最优价格排序
up_bids = snapshot[(snapshot["outcome"] == "Up") & (snapshot["side"] == "bid")]
up_bids = up_bids.sort_values("level_index")
```

### Q：为什么历史成交覆盖率不是 100%？

Data API `/trades` 存在分页/offset 限制，高成交量市场可能受影响。可用 `scripts/export_market_trades_chain.py` 做严格链上补齐。

---

## 免责声明

本项目仅用于**数据研究与工程学习**。请遵守 Polymarket 服务条款及相关 API/RPC 服务商的使用条款。作者不对本工具的任何滥用行为承担责任。

---

## 许可证

MIT
