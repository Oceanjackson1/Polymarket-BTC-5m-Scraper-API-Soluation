# Polymarket BTC 5 分钟预测市场数据采集工具

一套完整的 [Polymarket](https://polymarket.com) **BTC Up/Down 5 分钟** 预测市场数据采集工具包。本项目提供两个独立的数据采集系统：

1. **已成交交易管道 (Realized Trade Pipeline)** — 批量回填已关闭市场的历史成交数据
2. **实时订单簿录制器 (Order Book Recorder)** — 基于 WebSocket 的 L2 订单簿实时流式采集

两者结合，可以覆盖 BTC 5 分钟预测市场的完整交易生命周期。

---

## 目录

- [数据定义](#数据定义)
  - [已成交交易 (Realized Trades)](#已成交交易-realized-trades)
  - [完整订单簿 L2 (Full Order Book)](#完整订单簿-l2-full-order-book)
  - [核心差异对比](#核心差异对比)
- [架构概览](#架构概览)
- [快速开始](#快速开始)
- [一、已成交交易管道](#一已成交交易管道)
  - [工作原理](#工作原理)
  - [命令行用法](#命令行用法)
  - [输出文件](#输出文件)
  - [CSV 字段说明](#csv-字段说明交易)
- [二、实时订单簿录制器](#二实时订单簿录制器)
  - [工作原理](#工作原理-1)
  - [命令行用法](#命令行用法-1)
  - [输出文件](#输出文件-1)
  - [CSV 字段说明](#csv-字段说明订单簿)
- [三、链上交易验证](#三链上交易验证)
- [API 数据源](#api-数据源)
- [项目结构](#项目结构)
- [环境要求](#环境要求)

---

## 数据定义

### 已成交交易 (Realized Trades)

**定义：** 已成交交易是指**已经撮合成功并结算的交易记录** — 买卖双方在 Polymarket CLOB（中央限价订单簿）上以特定价格和数量完成的交易。每条记录代表一次真实发生的成交。

**数据范畴：**
- **时间范围：** 仅限已关闭（已结算）的历史市场
- **数据来源：** `Data API /trades`（链下成交记录），以及可选的 Polygon 链上 `OrderFilled` 事件日志用于严格验证
- **包含的数据：**
  - 每笔已执行的交易：成交价格 (price)、成交数量 (size)、名义金额 (`price × size`)、方向 (BUY/SELL)、结果 (Up/Down)
  - 交易元数据：时间戳（Unix 秒级）、代理钱包地址、Polygon 交易哈希
  - 市场上下文：condition ID、event ID、市场 slug、5 分钟窗口起始时间戳
- **不包含的数据：**
  - 未成交的挂单（resting limit orders）
  - 任何时间点的订单簿深度或流动性
  - 订单的撤销或修改记录
  - 买卖价差（bid-ask spread）
- **限制：**
  - Data API `/trades` 存在偏移量限制（每个市场约 3000 条）。对于高交易量市场，可使用链上验证脚本 (`export_market_trades_chain.py`) 获取完整数据
  - 仅可获取**已关闭**市场的数据 — Data API 在市场结算后才提供完整成交历史

### 完整订单簿 L2 (Full Order Book)

**定义：** L2（Level 2）订单簿是**按价格聚合的深度快照**，显示每个价格档位上的总挂单量。"L2" 意味着订单按价格分组 — 你能看到"在 $0.55 价位有 100 股挂单"，但**看不到**这 100 股由多少个独立订单组成、分别是谁下的（那是 L3 数据）。

**数据范畴：**
- **时间范围：** 从录制器启动时刻开始，仅可向前采集（无法回填历史数据）
- **数据来源：** CLOB WebSocket `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- **包含的数据（3 个事件流）：**
  1. **订单簿快照** (`book` 事件) — 每次成交后推送的完整 L2 深度快照，包含所有买盘 (bid) 和卖盘 (ask) 价格档位及其聚合数量
  2. **价格变动** (`price_change` 事件) — L2 增量更新，由挂单、撤单、部分成交触发。`size=0` 表示该价格档位已被完全移除
  3. **实时成交** (`last_trade_price` 事件) — 通过 WebSocket 实时推送的成交通知，具有毫秒级接收时间戳
- **不包含的数据：**
  - 单个订单 ID 或 L3 级别数据（Polymarket 公开 API 不提供）
  - 录制器启动之前的历史订单簿状态
  - **已关闭**市场的订单簿（Polymarket 在市场结算后会销毁 CLOB 订单簿；REST `/book` 接口返回 404）
- **时间戳精度：** 毫秒级。每条记录包含：
  - `server_ts_ms` — Polymarket 服务端时间戳
  - `receive_ts_ms` — 本地接收时间戳（WebSocket 消息被处理的时刻）
  - `receive_utc` — 人类可读的 ISO 8601 UTC 时间戳

### 核心差异对比

| 维度 | 已成交交易 (Realized Trades) | 订单簿 L2 (Order Book) |
|------|---------------------------|----------------------|
| **数据类型** | 已执行的成交记录 | 挂单深度 + 增量变动 |
| **时间方向** | 历史回溯（backward-looking） | 实时前瞻（forward-looking） |
| **市场状态** | 仅已关闭市场 | 仅活跃市场 |
| **采集方式** | REST API 批量拉取 | WebSocket 流式推送 |
| **数据粒度** | 逐笔成交 | 逐价格档位，毫秒级事件 |
| **数据完整性** | 全部成交（可链上兜底） | 录制器启动后的全部事件 |
| **是否反映流动性？** | 否（只有已成交的） | 是（完整买卖盘深度） |
| **是否反映价差？** | 否 | 是（`best_bid`、`best_ask`） |

> **为什么无法获取历史订单簿？** Polymarket 在市场关闭并结算后会销毁 CLOB 订单簿。REST 接口 `GET /book?token_id=...` 对已关闭市场返回 HTTP 404。因此，订单簿录制器必须在市场仍然活跃时持续运行，才能捕获深度数据。

---

## 架构概览

```
Polymarket API 层
├── Gamma API ─────────── 市场元数据（发现、生命周期）
├── Data API ──────────── 历史成交记录（已关闭市场）
└── CLOB WebSocket ────── 实时订单簿事件（活跃市场）

本项目
├── run_pipeline.py ───── 批量管道：索引市场 → 回填成交 → 数据校验
├── run_recorder.py ───── 守护进程：发现市场 → WS 订阅 → 流式写入 CSV
└── export_market_trades_chain.py ── 单市场链上验证
```

### 已成交交易管道架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    run_pipeline.py (CLI 入口)                     │
└───────────────────────┬──────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                pipeline.py (三阶段管道)                            │
│                                                                   │
│  阶段 1：市场索引 ── Gamma /markets (tag_id=235, closed)         │
│      ↓                                                            │
│  阶段 2：成交回填 ── Data /trades (按 conditionId 分页)          │
│      ↓                                                            │
│  阶段 3：数据校验 ── 交叉验证计数与一致性                         │
│                                                                   │
│  断点续跑：run_state.json（崩溃后可恢复进度）                     │
└──────────────────────────────────────────────────────────────────┘
```

### 订单簿录制器架构

```
┌──────────────────────────────────────────────────────────────────┐
│                   run_recorder.py (CLI 入口)                      │
└───────────────────────┬──────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│              recorder.py (OrderBookRecorder)                      │
│  主线程：市场发现循环（每 30 秒轮询 Gamma API）                   │
│  WS 线程：接收事件 → 分发处理 → 写入 CSV                         │
├──────────────────────────────┬───────────────────────────────────┤
│  market_tracker.py           │  ws_connection.py                 │
│  • Gamma API 活跃市场发现     │  • WebSocket 连接管理             │
│    及过滤                    │  • 心跳保活（PING/PONG，10秒）    │
│  • token_id ↔ 市场映射       │  • 断线自动重连（指数退避）       │
│  • 基于到期时间的生命周期管理 │  • 动态订阅 / 退订               │
└──────────────────────────────┴───────────────────────────────────┘
                        │
                        ▼
         output/orderbook/<market-slug>/
           ├── book_snapshots_<slug>.csv   （完整深度快照）
           ├── price_changes_<slug>.csv    （L2 增量变动）
           └── trades_<slug>.csv           （实时成交）
```

---

## 快速开始

```bash
# 克隆仓库
git clone https://github.com/Oceanjackson1/Polymarket-BTC-5m-Scraper-API-Soluation.git
cd Polymarket-BTC-5m-Scraper-API-Soluation

# 配置环境
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 方案 A：回填历史成交数据（批量运行，自动完成）
python scripts/run_pipeline.py

# 方案 B：录制实时订单簿（守护进程，Ctrl-C 停止）
python scripts/run_recorder.py

# 方案 C：两者同时运行（在不同终端窗口中）
```

---

## 一、已成交交易管道

### 工作原理

管道分 3 个阶段运行，支持断点续跑：

1. **市场索引** — 查询 Gamma API 获取所有已关闭的 BTC 5 分钟市场（`tag_id=235`，`seriesSlug=btc-up-or-down-5m`），全量分页后写入市场索引 CSV。

2. **成交回填** — 对索引中的每个市场，从 Data API `/trades` 接口分页拉取全部已执行的成交记录（`limit=1000`），计算每笔交易的名义金额 `notional = price × size`，写入统一的成交 CSV 文件。

3. **数据校验** — 交叉验证市场数量与成交数量的一致性，生成校验报告。

管道在每一页/每个市场处理完后都会将进度保存到 `run_state.json`，崩溃后可通过 `--resume`（默认行为）恢复。

### 命令行用法

```bash
# 默认运行（自动从上次断点恢复）
python scripts/run_pipeline.py

# 从头开始（忽略已有断点）
python scripts/run_pipeline.py --no-resume

# 调试模式：只处理前 20 个市场
python scripts/run_pipeline.py --market-limit 20

# 包含零交易量的市场
python scripts/run_pipeline.py --include-zero-volume

# 完整参数示例
python scripts/run_pipeline.py \
  --output-dir output \
  --market-limit 100 \
  --include-zero-volume \
  --request-delay 0.15 \
  --timeout-seconds 30 \
  --max-retries 5 \
  --log-level DEBUG
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | `output/` | 输出基础目录 |
| `--no-resume` | （默认续跑） | 从头开始，忽略断点文件 |
| `--market-limit` | 无（全部） | 最大处理市场数（调试用） |
| `--include-zero-volume` | False | 是否包含交易量为 0 的市场 |
| `--request-delay` | 0.10 秒 | API 请求间隔 |
| `--timeout-seconds` | 30 | HTTP 请求超时时间 |
| `--max-retries` | 5 | 单次请求最大重试次数（429/5xx 自动重试） |
| `--log-level` | INFO | 日志级别 (DEBUG/INFO/WARNING/ERROR) |

### 输出文件

```
output/
├── indexes/
│   └── markets_btc_up_or_down_5m.csv    # 市场索引（所有发现的市场）
├── trades/
│   └── trades_btc_up_or_down_5m.csv     # 合并的成交历史
├── reports/
│   └── validation_summary.json           # 校验报告
└── run_state.json                         # 断点续跑状态
```

### CSV 字段说明（交易）

**`markets_btc_up_or_down_5m.csv`** — 市场索引

| 字段 | 类型 | 说明 |
|------|------|------|
| `market_id` | string | Gamma API 市场 ID |
| `event_id` | string | 关联的事件 ID |
| `condition_id` | string | CLOB condition ID（用于查询成交） |
| `slug` | string | 市场标识符，如 `btc-updown-5m-1771211700` |
| `clob_token_ids` | string | CLOB token ID 的 JSON 数组（用于订单簿订阅） |
| `outcomes` | string | 结果标签的 JSON 数组，如 `["Up","Down"]` |
| `window_start_ts` | int | 5 分钟窗口起始的 Unix 时间戳 |
| `window_start_utc` | string | 窗口起始时间（ISO 8601 UTC） |
| `market_end_utc` | string | 市场结束时间（ISO 8601） |
| `volume` | float | 市场总交易量（USD） |
| `market_url` | string | Polymarket 链接（英文） |
| `market_url_zh` | string | Polymarket 链接（中文） |

**`trades_btc_up_or_down_5m.csv`** — 成交记录

| 字段 | 类型 | 说明 |
|------|------|------|
| `market_slug` | string | 市场标识符 |
| `window_start_ts` | int | 窗口起始 Unix 时间戳 |
| `condition_id` | string | CLOB condition ID |
| `event_id` | string | 事件 ID |
| `trade_timestamp` | int | 成交时间（Unix 秒级） |
| `trade_utc` | string | 成交时间（ISO 8601 UTC） |
| `price` | float | 成交价格（0.0–1.0） |
| `size` | float | 成交数量（股） |
| `notional` | float | 名义金额 `price × size`（USD） |
| `side` | string | `BUY` 或 `SELL` |
| `outcome` | string | `Up` 或 `Down` |
| `asset` | string | 成交资产的 token ID |
| `proxy_wallet` | string | 交易者的代理钱包地址 |
| `transaction_hash` | string | Polygon 交易哈希 |
| `dedupe_key` | string | 用于去重的复合键 |

---

## 二、实时订单簿录制器

### 工作原理

录制器以**长期运行的守护进程**方式持续工作：

1. **发现市场** — 每 30 秒轮询 Gamma API，获取当前活跃（未关闭）的 BTC 5 分钟市场
2. **WebSocket 订阅** — 连接到 `wss://ws-subscriptions-clob.polymarket.com/ws/market`，订阅所有已发现的 token ID
3. **流式写入 CSV** — 接收 3 种 WebSocket 事件，实时写入每个市场对应的 CSV 文件
4. **生命周期管理** — 自动订阅新创建的市场，自动退订已过期的市场（到期后保留 120 秒的宽限期）

**容错特性：**
- **断线自动重连：** 指数退避策略（1s → 2s → 4s → ... → 最大 60s）
- **重连后全量重订阅：** 所有活跃 token ID 在重连后自动重新订阅
- **追加模式 CSV：** 文件以 append 模式打开 — 重启后不会丢失之前的数据
- **心跳保活：** 每 10 秒发送 PING 保持连接
- **优雅关闭：** `Ctrl-C` 或 `SIGTERM` 会先刷新所有 CSV 缓冲区再退出

### 命令行用法

```bash
# 默认运行：录制到 output/orderbook/
python scripts/run_recorder.py

# 自定义输出目录
python scripts/run_recorder.py --output-dir /data/polymarket

# 调整时间参数
python scripts/run_recorder.py \
  --poll-interval 15 \
  --grace-period 180

# 完整参数示例
python scripts/run_recorder.py \
  --output-dir output \
  --poll-interval 30 \
  --grace-period 120 \
  --timeout-seconds 30 \
  --max-retries 5 \
  --log-level DEBUG
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-dir` | `output/` | 输出基础目录 |
| `--poll-interval` | 30.0 秒 | Gamma API 市场发现轮询间隔 |
| `--grace-period` | 120.0 秒 | 市场到期后继续录制的宽限时间 |
| `--timeout-seconds` | 30 | Gamma API 请求的 HTTP 超时 |
| `--max-retries` | 5 | Gamma API 请求最大重试次数 |
| `--log-level` | INFO | 日志级别 |

### 输出文件

每个活跃市场拥有独立的目录，包含 3 个 CSV 文件：

```
output/orderbook/
├── btc-updown-5m-1771211700/
│   ├── book_snapshots_btc-updown-5m-1771211700.csv  （深度快照）
│   ├── price_changes_btc-updown-5m-1771211700.csv   （增量变动）
│   └── trades_btc-updown-5m-1771211700.csv          （实时成交）
├── btc-updown-5m-1771212000/
│   ├── book_snapshots_btc-updown-5m-1771212000.csv
│   ├── price_changes_btc-updown-5m-1771212000.csv
│   └── trades_btc-updown-5m-1771212000.csv
└── ...（每个活跃市场一个目录）
```

### CSV 字段说明（订单簿）

**`book_snapshots_<slug>.csv`** — 完整 L2 深度快照

由 `book` 事件触发（每次成交后推送）。每个快照展开为多行（每个价格档位一行）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `snapshot_seq` | int | 自增快照序号（每市场独立） |
| `server_ts_ms` | int | 服务端时间戳（毫秒） |
| `receive_ts_ms` | int | 本地接收时间戳（毫秒） |
| `receive_utc` | string | 接收时间（ISO 8601 UTC，毫秒精度） |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` 或 `Down` |
| `condition_id` | string | 市场 condition ID |
| `side` | string | `bid`（买盘）或 `ask`（卖盘） |
| `price` | string | 价格档位（0.0–1.0） |
| `size` | string | 该价格档位的总挂单量 |
| `level_index` | int | 价格阶梯中的位置（0 = 最优价） |

**`price_changes_<slug>.csv`** — L2 增量更新

由 `price_change` 事件触发（挂单、撤单或部分成交）。记录变动的增量 — `size=0` 表示该价格档位已被完全移除。

| 字段 | 类型 | 说明 |
|------|------|------|
| `server_ts_ms` | int | 服务端时间戳（毫秒） |
| `receive_ts_ms` | int | 本地接收时间戳（毫秒） |
| `receive_utc` | string | 接收时间（ISO 8601 UTC） |
| `condition_id` | string | 市场 condition ID |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` 或 `Down` |
| `side` | string | `BUY` 或 `SELL` |
| `price` | string | 受影响的价格档位 |
| `size` | string | 该档位的新总量（`0` = 已移除） |
| `best_bid` | string | 变动后的最优买价 |
| `best_ask` | string | 变动后的最优卖价 |

**`trades_<slug>.csv`** — 实时成交

由 `last_trade_price` 事件触发。与已成交交易管道的数据类似，但具有毫秒级时间戳且为实时推送。

| 字段 | 类型 | 说明 |
|------|------|------|
| `server_ts_ms` | int | 服务端时间戳（毫秒） |
| `receive_ts_ms` | int | 本地接收时间戳（毫秒） |
| `receive_utc` | string | 接收时间（ISO 8601 UTC） |
| `condition_id` | string | 市场 condition ID |
| `asset_id` | string | CLOB token ID |
| `outcome` | string | `Up` 或 `Down` |
| `side` | string | `BUY` 或 `SELL` |
| `price` | string | 成交价格 |
| `size` | string | 成交数量 |
| `fee_rate_bps` | string | 手续费率（基点） |

---

## 三、链上交易验证

当 Data API `/trades` 因偏移量限制（~3000 条）无法返回高交易量市场的全部成交时，可以直接从 Polygon 区块链验证：

```bash
python scripts/export_market_trades_chain.py \
  --slug btc-updown-5m-1771211700 \
  --output-dir output_strict \
  --compare-csv output/trades/trades_btc-updown-5m-1771211700.csv
```

该脚本扫描 Polygon 链上的 `OrderFilled` 事件日志，提供不受 API 分页限制的完整成交记录作为地面真值 (ground truth)。

**输出：**
- `output_strict/<slug>/strict_trades_<slug>.csv` — 完整的链上成交历史
- `output_strict/<slug>/strict_validation_<slug>.json` — 校验报告（含覆盖率分析）

---

## API 数据源

本项目使用 Polymarket 的三个 API 端点：

| API | 基础 URL | 用途 | 是否需要认证 |
|-----|----------|------|-------------|
| **Gamma API** | `https://gamma-api.polymarket.com` | 市场元数据、发现、生命周期 | 否 |
| **Data API** | `https://data-api.polymarket.com` | 历史成交记录 | 否 |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | 实时订单簿事件 | 否 |

**Gamma API** 被两个系统共同使用，用于市场发现。关键参数：
- `tag_id=235` — Bitcoin 相关市场
- `seriesSlug=btc-up-or-down-5m` — 5 分钟系列过滤
- `slug` 格式：`btc-updown-5m-<unix_timestamp>`

**WebSocket 协议：**
- 订阅消息：`{"assets_ids": [...], "type": "market", "custom_feature_enabled": true}`
- 心跳保活：每 10 秒发送文本 `"PING"`，接收 `"PONG"`
- 事件以 JSON 数组形式到达；每个元素包含 `event_type` 字段（`book`、`price_change`、`last_trade_price`）

---

## 项目结构

```
├── src/polymarket_btc5m/
│   ├── __init__.py          # 包导出
│   ├── client.py            # HTTP 客户端，带重试逻辑（Gamma + Data API）
│   ├── pipeline.py          # 批量成交管道（阶段 1-3）
│   ├── market_tracker.py    # 活跃市场发现与生命周期管理
│   ├── ws_connection.py     # WebSocket 连接管理（心跳 + 重连）
│   └── recorder.py          # 订单簿录制器（事件分发 + CSV 写入）
├── scripts/
│   ├── run_pipeline.py      # CLI 入口：批量成交回填
│   ├── run_recorder.py      # CLI 入口：实时订单簿录制
│   └── export_market_trades_chain.py  # CLI 入口：链上交易验证
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 环境要求

- Python 3.10+
- 依赖项（通过 `pip install -r requirements.txt` 自动安装）：
  - `requests>=2.32.0` — REST API 的 HTTP 客户端
  - `websocket-client>=1.7.0` — 实时流式推送的 WebSocket 客户端

无需 API 密钥。所有使用的 Polymarket 接口均为公开接口。
