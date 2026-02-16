# Polymarket BTC 5分钟价格预测数据采集脚本 - 开发计划

## 项目概述
采集 Polymarket 上所有 BTC 五分钟价格涨跌预测事件的数据，包括：
1. 所有历史事件链接
2. 每个事件中的逐笔交易数据（订单额 + 时间戳）

## 开发进度

- [x] **Step 1 - 环境搭建** ✅ 已完成
  - ✅ 创建项目目录结构 (src/, data/)
  - ✅ 创建 requirements.txt 依赖文件
  - ✅ 创建配置模块 (src/config.py)
  - ✅ 创建数据库模块 (src/database.py)
  - ✅ 创建 API 客户端模块 (src/api_client.py)

- [x] **Step 2 - 事件采集模块** ✅ 已完成
  - ✅ 分页遍历 Gamma API `/events?tag_id=102892` (src/fetch_events.py)
  - ✅ 客户端过滤 slug 前缀 `btc-updown-5m-`
  - ✅ 提取关键字段存储到 SQLite events 表
  - ✅ 首次运行验证通过，成功采集 10,000+ BTC 5分钟事件

- [x] **Step 3 - 交易采集模块** ✅ 已完成
  - ✅ 遍历每个事件的 conditionId (src/fetch_trades.py)
  - ✅ 调用 Data API `/trades?market={conditionId}` 分页获取
  - ✅ 提取关键字段存储到 SQLite trades 表

- [x] **Step 4 - 断点续传 & 增量更新** ✅ 已完成
  - ✅ 记录采集进度到 SQLite fetch_progress 表
  - ✅ 支持中断后续传 (resume=True)
  - ✅ 支持增量获取新事件和新交易 (only_missing=True)

- [x] **Step 5 - 数据导出模块** ✅ 已完成
  - ✅ 支持导出 events.csv / trades.csv (src/exporter.py)
  - ✅ 支持导出 JSON 格式
  - ✅ 支持按日期范围筛选
  - ✅ 支持数据摘要统计 (print_summary)

- [x] **Step 6 - 主入口 & README** ✅ 已完成
  - ✅ 创建 main.py 主入口（命令行参数）
  - ✅ 创建 README.md 使用说明

## API 信息

| API | 基础 URL | 用途 |
|-----|---------|------|
| Gamma API | `https://gamma-api.polymarket.com` | 获取事件/市场元数据 |
| Data API | `https://data-api.polymarket.com` | 获取交易历史（无需认证）|

### 关键端点
- 事件列表: `GET /events?tag_id=102892&limit=100&offset={n}`
- 单个事件: `GET /events/slug/{slug}`
- 交易历史: `GET /trades?market={conditionId}&limit=10000&offset={n}`

### 数据关系
- Series slug: `btc-up-or-down-5m` (ID: 10684)
- Event slug 格式: `btc-updown-5m-{unix_timestamp}`
- 每个 Event 包含 1 个 Market，Market 有 `conditionId`
- Trade 通过 `conditionId` 关联到 Market/Event
