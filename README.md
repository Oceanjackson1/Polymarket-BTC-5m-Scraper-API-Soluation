# Polymarket BTC 5-Minute Price Prediction Scraper

Polymarket 比特币五分钟价格涨跌预测事件 —— 历史数据采集工具。

本工具可以自动化获取 [Polymarket](https://polymarket.com) 上所有 BTC 5 分钟价格预测市场的：
1. **历史事件链接** — 每个 5 分钟窗口对应一个预测市场
2. **完整订单簿成交历史** — 每个市场中每一笔成交的金额、价格、方向、时间戳

### 技术路径说明

本工具采用**纯 API 采集路径**，通过 Polymarket 官方提供的中心化 REST API（Gamma API + Data API）获取数据，**不涉及任何链上数据采集**（不读取 Polygon 链上合约事件、不使用 Subgraph、不依赖 RPC 节点）。

- 优点：无需 API Key、无需链上基础设施、部署简单
- 局限：受限于 Data API 的 offset 上限（单方向最多 4000 条），对高交易量市场的覆盖率约为 97%（通过 BUY+SELL 分拆策略优化后）

---

## 目录

- [背景知识](#背景知识)
- [核心功能](#核心功能)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [使用方式](#使用方式)
- [数据获取逻辑详解](#数据获取逻辑详解)
- [数据字段说明](#数据字段说明)
- [API 限制与应对策略](#api-限制与应对策略)
- [常见问题](#常见问题)

---

## 背景知识

### 什么是 Polymarket BTC 5 分钟预测

Polymarket 提供一种短期二元期权市场：每 5 分钟创建一个新的预测事件，参与者下注 BTC 在该 5 分钟窗口内价格是"涨 (Up)"还是"跌 (Down)"。

**事件链接格式:**

```
https://polymarket.com/event/btc-updown-5m-{unix_timestamp}
```

其中 `{unix_timestamp}` 是该 5 分钟窗口的**起始时间戳** (Unix 秒)。例如：
- `btc-updown-5m-1771211700` → 2026-02-16 03:15:00 UTC 开始的 5 分钟窗口
- 每 300 秒 (5 分钟) 创建一个新的事件

### 涉及的 Polymarket API

本工具使用两个公开 API（均无需认证）：

| API | 端点 | 用途 |
|-----|------|------|
| **Gamma API** | `https://gamma-api.polymarket.com/events` | 获取事件元数据 (slug, conditionId, volume 等) |
| **Data API** | `https://data-api.polymarket.com/trades` | 获取逐笔成交历史 (size, price, timestamp 等) |

---

## 核心功能

### 功能 1: 获取所有历史 BTC 5 分钟预测事件链接

- 从 Gamma API 分页遍历所有标记为 `5M` (tag_id=102892) 的事件
- 通过 slug 前缀 `btc-updown-5m-` 过滤出 BTC 相关事件（排除 ETH/XRP）
- 提取事件的 `slug`、`conditionId`、`volume`、`开始/结束时间` 等关键字段
- 自动生成 Polymarket 页面链接
- 支持断点续传（记录分页 offset，中断后可从上次位置继续）

### 功能 2: 获取每个事件的完整订单簿成交历史

- 对每个事件，通过其 `conditionId` 从 Data API 获取所有成交记录
- 采用 **BUY + SELL 分拆策略**突破 API 的 4000 条查询上限（详见下方）
- 每笔成交记录包含：金额 (size)、价格 (price)、方向 (BUY/SELL)、结果 (Up/Down)、时间戳、交易哈希、钱包地址
- 去重机制：基于 `(transaction_hash, timestamp, size, side, wallet)` 组合键
- 支持断点续传（记录已处理的事件 slug）

---

## 项目结构

```
Polymarket-BTC-5m-Scraper/
├── main.py                    # 主入口脚本 (命令行接口)
├── fetch_single_market.py     # 独立脚本: 获取单个市场的完整交易 (BUY+SELL 策略)
├── requirements.txt           # Python 依赖
├── README.md                  # 本文件
├── PLAN.md                    # 开发计划文档
├── src/
│   ├── __init__.py
│   ├── config.py              # 配置: API 端点、分页参数、数据库路径
│   ├── api_client.py          # HTTP 客户端: 请求重试、速率控制
│   ├── database.py            # SQLite 数据库: 建表、增删改查、进度管理
│   ├── fetch_events.py        # 事件采集模块
│   ├── fetch_trades.py        # 交易采集模块 (含 BUY+SELL 分拆策略)
│   └── exporter.py            # 数据导出模块 (CSV / JSON)
└── data/                      # 运行后自动创建
    ├── polymarket_btc_5m.db   # SQLite 数据库
    ├── events.csv             # 导出的事件列表
    └── trades.csv             # 导出的交易记录
```

---

## 快速开始

### 环境要求

- Python 3.9+
- 网络访问 (需能访问 Polymarket API)

### 安装

```bash
# 克隆仓库
git clone https://github.com/Oceanjackson1/Polymarket-BTC-5m-Scraper.git
cd Polymarket-BTC-5m-Scraper

# 安装依赖
pip install -r requirements.txt
```

### 一键运行完整采集

```bash
python main.py
```

这将依次执行：
1. 采集所有 BTC 5 分钟事件 → 存入 SQLite
2. 采集每个事件的交易数据 → 存入 SQLite
3. 导出 CSV 文件到 `data/` 目录

> **注意**: 完整采集包含数万个事件，每个事件需要多次 API 请求。预计首次完整运行需要 **数小时至数天**。脚本支持断点续传，可以随时中断并重新运行。

---

## 使用方式

### 分步骤执行

```bash
# 只采集事件 (获取所有事件链接和元数据)
python main.py events

# 只采集交易 (需先完成事件采集)
python main.py trades

# 只导出数据 (CSV 格式)
python main.py export

# 导出为 JSON 格式
python main.py export --format json

# 查看数据库摘要统计
python main.py summary
```

### 获取单个市场的完整交易历史

如果只需要某个特定市场的数据，可使用独立脚本 `fetch_single_market.py`:

```bash
# 编辑脚本中的 EVENT_SLUG 和 CONDITION_ID，然后运行:
python fetch_single_market.py
```

该脚本使用 BUY + SELL 分拆策略获取单个市场的全量交易，并直接输出 CSV 文件。

### 在 Python 中调用

```python
from src.database import init_db, get_all_events, get_connection
from src.fetch_events import fetch_all_events
from src.fetch_trades import _fetch_trades_for_event

# 初始化数据库
init_db()

# 采集所有事件
fetch_all_events(resume=True)

# 获取事件列表
events = get_all_events()
print(f"共 {len(events)} 个事件")

# 获取某个事件的交易
event = events[0]
trades = _fetch_trades_for_event(event["condition_id"], event["slug"])
print(f"获取到 {len(trades)} 笔交易")

# 直接查询 SQLite
conn = get_connection()
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM trades WHERE event_slug = ?", ("btc-updown-5m-1771211700",))
print(f"该事件交易数: {cursor.fetchone()[0]}")
conn.close()
```

---

## 数据获取逻辑详解

### 第一步: 事件采集 (`src/fetch_events.py`)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Gamma API 事件采集流程                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 请求 Gamma API /events?tag_id=102892&limit=100&offset=N    │
│     ├─ tag_id=102892 是 "5M" 标签, 返回 BTC/ETH/XRP 5分钟事件   │
│     └─ 每页最多 100 个事件                                       │
│                                                                  │
│  2. 客户端过滤: 只保留 slug 以 "btc-updown-5m-" 开头的事件       │
│     ├─ btc-updown-5m-1771211700 ✓ (保留)                        │
│     ├─ eth-updown-5m-1771211700 ✗ (丢弃)                        │
│     └─ xrp-updown-5m-1771211700 ✗ (丢弃)                        │
│                                                                  │
│  3. 提取字段:                                                    │
│     ├─ id, slug, title, condition_id                             │
│     ├─ start_time, end_time, volume                              │
│     ├─ outcome_prices, closed                                    │
│     └─ polymarket_url (自动拼接)                                 │
│                                                                  │
│  4. INSERT OR IGNORE 到 SQLite events 表                        │
│                                                                  │
│  5. offset += 100, 重复步骤 1-4 直到无更多数据                    │
│                                                                  │
│  断点续传: fetch_progress 表记录 last_offset                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 第二步: 交易采集 (`src/fetch_trades.py`)

```
┌─────────────────────────────────────────────────────────────────┐
│               Data API 交易采集流程 (每个事件)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  对数据库中的每个事件 (condition_id):                              │
│                                                                  │
│  ▶ 阶段 1: 普通获取                                              │
│    GET /trades?market={conditionId}&limit=1000&offset=N          │
│    ├─ offset=0    → 最多 1000 条                                │
│    ├─ offset=1000 → 最多 1000 条                                │
│    ├─ offset=2000 → 最多 1000 条                                │
│    ├─ offset=3000 → 最多 1000 条                                │
│    └─ offset=4000 → 400 错误 (API 上限)                         │
│    合计: 最多 4000 条                                            │
│                                                                  │
│    如果未触及上限 → 直接保存, 完成                                 │
│    如果触及上限   → 进入阶段 2                                     │
│                                                                  │
│  ▶ 阶段 2: BUY + SELL 分拆策略                                   │
│    GET /trades?market={conditionId}&limit=1000&offset=N&side=BUY │
│    ├─ 最多获取 4000 条 BUY 交易                                  │
│    GET /trades?market={conditionId}&limit=1000&offset=N&side=SELL│
│    ├─ 最多获取 4000 条 SELL 交易                                  │
│    └─ 合并去重 → 最多 ~8000 条唯一交易                            │
│                                                                  │
│  ▶ 去重键: (transaction_hash, timestamp, size, side, wallet)     │
│                                                                  │
│  ▶ INSERT OR IGNORE 到 SQLite trades 表                          │
│                                                                  │
│  断点续传: fetch_progress 表记录 last_event_slug                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 为什么需要 BUY + SELL 分拆?

Polymarket Data API 对单次查询有约 **4000 条记录**的硬上限 (`offset + limit < 4000`)。对于热门 5 分钟市场，交易笔数可能超过 4000。

我们发现 `side` 参数可以有效过滤：
- `side=BUY` → 返回的全部是 BUY 方向的交易, 最多 4000 条
- `side=SELL` → 返回的全部是 SELL 方向的交易, 最多 4000 条

分别查询后合并去重，可以获得最多 ~8000 条记录。实测覆盖率:

| 策略 | 获取数量 | 覆盖率 |
|------|----------|--------|
| 普通分页 (无 side 过滤) | 4,000 | ~73% |
| **BUY + SELL 分拆** | **6,883** | **~97%** |

> 剩余 ~3% 的缺失来自 BUY 方向的 4000 条上限（最早的少量 BUY 交易无法获取）。

---

## 数据字段说明

### events 表 (事件)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | Polymarket 事件 ID |
| `slug` | TEXT | 事件标识符, 如 `btc-updown-5m-1771211700` |
| `title` | TEXT | 事件标题, 如 "Bitcoin Up or Down - February 15, 10:15PM-10:20PM ET" |
| `condition_id` | TEXT | 合约条件 ID (用于查询交易数据) |
| `start_time` | TEXT | 事件开始时间 (ISO 8601) |
| `end_time` | TEXT | 事件结束时间 (ISO 8601) |
| `volume` | REAL | Polymarket 报告的交易量 (USDC) |
| `outcome_prices` | TEXT | 结算价格, 如 `["0","1"]` 表示 Down 胜 |
| `closed` | INTEGER | 是否已结算 (1=已结算, 0=未结算) |
| `polymarket_url` | TEXT | Polymarket 页面链接 |
| `created_at` | TEXT | 事件创建时间 |
| `fetched_at` | TEXT | 数据采集时间 |

### trades 表 (交易)

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INTEGER | 自增主键 |
| `event_slug` | TEXT | 所属事件 slug |
| `condition_id` | TEXT | 合约条件 ID |
| `trade_timestamp` | INTEGER | 成交时间 (Unix 时间戳, 秒) |
| `side` | TEXT | 交易方向: `BUY` 或 `SELL` |
| `outcome` | TEXT | 预测结果: `Up` 或 `Down` |
| `size` | REAL | **成交金额** (USDC) — 这是订单的成交额 |
| `price` | REAL | 成交价格 (0~1 之间, 代表概率) |
| `proxy_wallet` | TEXT | 交易者的代理钱包地址 (Polygon) |
| `transaction_hash` | TEXT | Polygon 链上交易哈希 |
| `fetched_at` | TEXT | 数据采集时间 |

#### 字段补充说明

- **`size` (成交额)**: 该笔交易投入的 USDC 金额。例如 `size=100` 表示这笔交易花了 100 USDC。
- **`price` (价格)**: 表示市场对某个结果的概率估计。`price=0.55` 表示市场认为该结果有 55% 的概率发生。买入 100 USDC 在 price=0.55 时，如果预测正确，可获得 100/0.55 ≈ 181.8 USDC。
- **`side`**: `BUY` 表示买入该 outcome 的份额，`SELL` 表示卖出。
- **`outcome`**: `Up` 表示看涨，`Down` 表示看跌。

---

## API 限制与应对策略

| 限制 | 详情 | 应对策略 |
|------|------|----------|
| **Gamma API 分页** | 每页最多 100 条 | 自动分页, offset 递增 |
| **Data API 每页上限** | 每次请求最多返回 1000 条 (无论 limit 设多大) | 固定 limit=1000, offset 分页 |
| **Data API offset 上限** | offset + limit >= 4000 时返回 400 错误 | BUY + SELL 分拆策略 |
| **速率限制** | 过于频繁会收到 429 错误 | 请求间隔 0.3s + 429 时指数退避重试 |
| **网络超时** | API 偶尔响应缓慢 | 30s 超时 + 最多 3 次重试 |

### 速率控制参数 (可在 `src/config.py` 中调整)

```python
REQUEST_DELAY = 0.3     # 请求间隔 (秒)
MAX_RETRIES = 3         # 最大重试次数
RETRY_DELAY = 2.0       # 重试间隔 (秒), 每次翻倍
```

---

## 常见问题

### Q: 首次采集需要多长时间?

- **事件采集**: 约 15-30 分钟 (12,000+ 个事件)
- **交易采集**: 取决于事件数量和每个事件的交易量。每个事件 1-10 秒不等，总计可能需要数小时到数天。
- 支持断点续传，可以分多次运行。

### Q: 可以只获取特定时间段的数据吗?

事件采集会获取所有历史事件。交易采集时可以通过修改 `src/database.py` 的查询条件过滤特定时间段。导出时支持日期过滤:

```python
from src.exporter import export_trades_csv
export_trades_csv(start_date=1771200000, end_date=1771300000)
```

### Q: 为什么覆盖率不是 100%?

Data API 的 offset 上限 (4000 条) 是硬限制。BUY+SELL 分拆策略可覆盖 ~97%，剩余 ~3% 是因为 BUY 方向交易数超过 4000 时，最早的少量记录无法获取。对于交易量较小的市场（< 4000 笔交易），可以获取 100% 的数据。

### Q: 数据存在哪里?

数据存储在 `data/polymarket_btc_5m.db` (SQLite 数据库)。可以直接用任何 SQLite 工具查看，或使用导出功能生成 CSV/JSON。

### Q: 如何增量更新?

```bash
# 采集新增的事件
python main.py events

# 只采集还没有交易数据的事件
python main.py trades
```

脚本会自动跳过已采集的事件和已有交易数据的事件。

### Q: 为什么不从链上采集数据?

本工具选择**纯 API 路径**而非链上采集，原因如下：

| 维度 | 纯 API 路径 (当前方案) | 链上采集路径 |
|------|----------------------|-------------|
| 部署复杂度 | 低 — 只需 Python + pip install | 高 — 需要 RPC 节点 / Subgraph / 合约 ABI 解析 |
| 认证要求 | 无需 API Key | 可能需要 RPC 提供商 Key |
| 数据覆盖率 | ~97% (受 offset 上限限制) | 100% (链上数据完整) |
| 开发成本 | 低 | 高 — 需理解 CTF Exchange 合约、token ID 映射等 |
| 数据延迟 | 接近实时 | 取决于索引速度 |

对于绝大多数分析场景，97% 的覆盖率已足够。如需 100% 完整数据，可考虑扩展链上采集方案（查询 Polygon 上的 CTF Exchange 合约事件日志或 Polymarket Subgraph）。

---

## 技术栈

- **Python 3.9+**
- **requests** — HTTP 请求
- **tqdm** — 进度条
- **pandas** — 数据处理 (可选)
- **SQLite** — 本地数据存储

## License

MIT
