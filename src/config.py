"""
配置模块 - 存放所有 API 端点、常量和可调参数
"""
import os

# ── API 端点 ──────────────────────────────────────────────
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

# 事件列表端点
EVENTS_URL = f"{GAMMA_API_BASE}/events"
# 单个事件端点（按 slug）
EVENT_BY_SLUG_URL = f"{GAMMA_API_BASE}/events/slug"
# 交易历史端点
TRADES_URL = f"{DATA_API_BASE}/trades"

# ── 过滤参数 ──────────────────────────────────────────────
TAG_ID_5M = 102892          # "5M" 标签 ID
SLUG_PREFIX = "btc-updown-5m-"  # BTC 5分钟事件 slug 前缀
SERIES_SLUG = "btc-up-or-down-5m"

# ── 分页与速率控制 ────────────────────────────────────────
EVENTS_PAGE_SIZE = 100      # Gamma API 每页事件数
TRADES_PAGE_SIZE = 1000     # Data API 每次实际最多返回 1000 条（无论 limit 设多大）
TRADES_MAX_OFFSET = 3000    # Data API offset 上限（offset+limit >= 4000 会返回 400）
REQUEST_DELAY = 0.3         # 请求间隔（秒），防止被限流
MAX_RETRIES = 3             # 请求失败重试次数
RETRY_DELAY = 2.0           # 重试间隔（秒）

# ── 数据库 ────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "polymarket_btc_5m.db")

# ── Polymarket 页面链接模板 ───────────────────────────────
POLYMARKET_EVENT_URL_TEMPLATE = "https://polymarket.com/event/{slug}"
