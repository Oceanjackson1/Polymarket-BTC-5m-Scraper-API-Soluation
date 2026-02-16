"""
数据库模块 - SQLite 数据库初始化和操作
"""
import sqlite3
import os
from typing import Optional, List, Dict
from src.config import DB_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表结构"""
    conn = get_connection()
    cursor = conn.cursor()

    # 事件表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id              INTEGER PRIMARY KEY,
            slug            TEXT UNIQUE NOT NULL,
            title           TEXT,
            condition_id    TEXT,
            start_time      TEXT,
            end_time        TEXT,
            volume          REAL,
            outcome_prices  TEXT,
            closed          INTEGER DEFAULT 0,
            polymarket_url  TEXT,
            created_at      TEXT,
            fetched_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # 交易表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            event_slug          TEXT NOT NULL,
            condition_id        TEXT NOT NULL,
            trade_timestamp     INTEGER NOT NULL,
            side                TEXT,
            outcome             TEXT,
            size                REAL,
            price               REAL,
            proxy_wallet        TEXT,
            transaction_hash    TEXT,
            fetched_at          TEXT DEFAULT (datetime('now')),
            UNIQUE(condition_id, transaction_hash, proxy_wallet, trade_timestamp, size)
        )
    """)

    # 采集进度表（用于断点续传）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fetch_progress (
            task_name       TEXT PRIMARY KEY,
            last_offset     INTEGER DEFAULT 0,
            last_event_slug TEXT,
            updated_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # 索引
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_event_slug
        ON trades(event_slug)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_condition_id
        ON trades(condition_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp
        ON trades(trade_timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_slug
        ON events(slug)
    """)

    conn.commit()
    conn.close()
    print(f"[DB] 数据库已初始化: {DB_PATH}")


def save_events(events: List[Dict]):
    """批量保存事件到数据库"""
    if not events:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    for event in events:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO events
                (id, slug, title, condition_id, start_time, end_time,
                 volume, outcome_prices, closed, polymarket_url, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event["id"],
                event["slug"],
                event["title"],
                event["condition_id"],
                event["start_time"],
                event["end_time"],
                event["volume"],
                event["outcome_prices"],
                event["closed"],
                event["polymarket_url"],
                event["created_at"],
            ))
            if cursor.rowcount > 0:
                inserted += 1
        except sqlite3.Error as e:
            print(f"[DB] 保存事件 {event.get('slug')} 失败: {e}")

    conn.commit()
    conn.close()
    return inserted


def save_trades(trades: List[Dict]):
    """批量保存交易到数据库"""
    if not trades:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    for trade in trades:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO trades
                (event_slug, condition_id, trade_timestamp, side, outcome,
                 size, price, proxy_wallet, transaction_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade["event_slug"],
                trade["condition_id"],
                trade["trade_timestamp"],
                trade["side"],
                trade["outcome"],
                trade["size"],
                trade["price"],
                trade["proxy_wallet"],
                trade["transaction_hash"],
            ))
            if cursor.rowcount > 0:
                inserted += 1
        except sqlite3.Error as e:
            pass  # 重复记录静默跳过

    conn.commit()
    conn.close()
    return inserted


def get_progress(task_name: str) -> Optional[Dict]:
    """获取采集进度"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM fetch_progress WHERE task_name = ?",
        (task_name,)
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None


def save_progress(task_name: str, last_offset: int = 0, last_event_slug: str = ""):
    """保存采集进度"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO fetch_progress (task_name, last_offset, last_event_slug, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(task_name) DO UPDATE SET
            last_offset = excluded.last_offset,
            last_event_slug = excluded.last_event_slug,
            updated_at = datetime('now')
    """, (task_name, last_offset, last_event_slug))
    conn.commit()
    conn.close()


def get_all_events() -> List[Dict]:
    """获取数据库中所有事件"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM events ORDER BY start_time ASC")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_events_without_trades() -> List[Dict]:
    """获取还没有采集交易数据的事件"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT e.* FROM events e
        LEFT JOIN (
            SELECT DISTINCT event_slug FROM trades
        ) t ON e.slug = t.event_slug
        WHERE t.event_slug IS NULL
        ORDER BY e.start_time ASC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_event_count() -> int:
    """获取事件总数"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM events")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_trade_count() -> int:
    """获取交易总数"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM trades")
    count = cursor.fetchone()[0]
    conn.close()
    return count
