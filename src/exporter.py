"""
数据导出模块 - 将 SQLite 数据导出为 CSV / JSON
"""
import os
import json
import sqlite3
from typing import Optional
from src.config import DB_PATH, DATA_DIR
from src.database import get_connection


def export_events_csv(output_path=None, start_date=None, end_date=None):
    """
    导出事件列表为 CSV。

    参数:
        output_path: 输出文件路径，默认 data/events.csv
        start_date: 起始日期过滤 (ISO 格式, 如 '2025-12-01')
        end_date: 结束日期过滤
    """
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "events.csv")

    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM events WHERE 1=1"
    params = []

    if start_date:
        query += " AND start_time >= ?"
        params.append(start_date)
    if end_date:
        query += " AND start_time <= ?"
        params.append(end_date)

    query += " ORDER BY start_time ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")
        for row in rows:
            values = []
            for v in row:
                v_str = str(v) if v is not None else ""
                if "," in v_str or '"' in v_str or "\n" in v_str:
                    v_str = '"' + v_str.replace('"', '""') + '"'
                values.append(v_str)
            f.write(",".join(values) + "\n")

    conn.close()
    print(f"[Export] 事件已导出: {output_path} ({len(rows)} 条)")
    return output_path


def export_trades_csv(output_path=None, event_slug=None, start_date=None, end_date=None):
    """
    导出交易数据为 CSV。

    参数:
        output_path: 输出文件路径，默认 data/trades.csv
        event_slug: 过滤特定事件的交易
        start_date: 起始时间戳过滤 (Unix timestamp)
        end_date: 结束时间戳过滤
    """
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "trades.csv")

    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM trades WHERE 1=1"
    params = []

    if event_slug:
        query += " AND event_slug = ?"
        params.append(event_slug)
    if start_date:
        query += " AND trade_timestamp >= ?"
        params.append(int(start_date))
    if end_date:
        query += " AND trade_timestamp <= ?"
        params.append(int(end_date))

    query += " ORDER BY trade_timestamp ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")
        for row in rows:
            values = []
            for v in row:
                v_str = str(v) if v is not None else ""
                if "," in v_str or '"' in v_str or "\n" in v_str:
                    v_str = '"' + v_str.replace('"', '""') + '"'
                values.append(v_str)
            f.write(",".join(values) + "\n")

    conn.close()
    print(f"[Export] 交易已导出: {output_path} ({len(rows)} 条)")
    return output_path


def export_events_json(output_path=None, start_date=None, end_date=None):
    """导出事件列表为 JSON"""
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "events.json")

    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM events WHERE 1=1"
    params = []

    if start_date:
        query += " AND start_time >= ?"
        params.append(start_date)
    if end_date:
        query += " AND start_time <= ?"
        params.append(end_date)

    query += " ORDER BY start_time ASC"

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[Export] 事件已导出: {output_path} ({len(rows)} 条)")
    return output_path


def export_trades_json(output_path=None, event_slug=None, start_date=None, end_date=None):
    """导出交易数据为 JSON"""
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "trades.json")

    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM trades WHERE 1=1"
    params = []

    if event_slug:
        query += " AND event_slug = ?"
        params.append(event_slug)
    if start_date:
        query += " AND trade_timestamp >= ?"
        params.append(int(start_date))
    if end_date:
        query += " AND trade_timestamp <= ?"
        params.append(int(end_date))

    query += " ORDER BY trade_timestamp ASC"

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[Export] 交易已导出: {output_path} ({len(rows)} 条)")
    return output_path


def print_summary():
    """打印数据库摘要统计"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM events")
    event_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM trades")
    trade_count = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(start_time), MAX(start_time) FROM events")
    row = cursor.fetchone()
    earliest = row[0] or "N/A"
    latest = row[1] or "N/A"

    cursor.execute("SELECT SUM(volume) FROM events")
    total_volume = cursor.fetchone()[0] or 0

    cursor.execute("""
        SELECT event_slug, COUNT(*) as cnt
        FROM trades
        GROUP BY event_slug
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top_events = cursor.fetchall()

    conn.close()

    print("\n" + "=" * 60)
    print("  Polymarket BTC 5分钟预测 - 数据摘要")
    print("=" * 60)
    print(f"  事件总数:     {event_count:,}")
    print(f"  交易总数:     {trade_count:,}")
    print(f"  最早事件:     {earliest}")
    print(f"  最新事件:     {latest}")
    print(f"  总交易量:     ${total_volume:,.2f}")

    if top_events:
        print("\n  交易最多的前5个事件:")
        for row in top_events:
            print(f"    {row[0]}: {row[1]:,} 笔")

    print("=" * 60 + "\n")
