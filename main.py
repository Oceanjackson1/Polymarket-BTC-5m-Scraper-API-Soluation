#!/usr/bin/env python3
"""
Polymarket BTC 5分钟价格预测数据采集脚本 - 主入口

用法:
    python main.py                    # 完整采集流程（事件 + 交易 + 导出）
    python main.py events             # 只采集事件
    python main.py trades             # 只采集交易（需要先采集事件）
    python main.py export             # 只导出数据
    python main.py summary            # 打印数据摘要
    python main.py export --format json  # 导出为 JSON 格式
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database import init_db
from src.fetch_events import fetch_all_events
from src.fetch_trades import fetch_all_trades
from src.exporter import (
    export_events_csv,
    export_trades_csv,
    export_events_json,
    export_trades_json,
    print_summary,
)


def run_full():
    """完整采集流程"""
    print("=" * 60)
    print("  Polymarket BTC 5分钟预测 - 全量采集")
    print("=" * 60)

    init_db()

    # Step 1: 采集事件
    print("\n>>> Step 1/3: 采集事件...")
    fetch_all_events(resume=True)

    # Step 2: 采集交易
    print("\n>>> Step 2/3: 采集交易...")
    fetch_all_trades(only_missing=True)

    # Step 3: 导出
    print("\n>>> Step 3/3: 导出数据...")
    export_events_csv()
    export_trades_csv()

    # 摘要
    print_summary()


def run_events():
    """只采集事件"""
    print(">>> 采集事件...")
    fetch_all_events(resume=True)


def run_trades():
    """只采集交易"""
    print(">>> 采集交易...")
    fetch_all_trades(only_missing=True)


def run_export(fmt="csv"):
    """导出数据"""
    print(f">>> 导出数据 (格式: {fmt})...")
    if fmt == "json":
        export_events_json()
        export_trades_json()
    else:
        export_events_csv()
        export_trades_csv()


def main():
    args = sys.argv[1:]

    if not args:
        run_full()
    elif args[0] == "events":
        run_events()
    elif args[0] == "trades":
        run_trades()
    elif args[0] == "export":
        fmt = "csv"
        if "--format" in args:
            idx = args.index("--format")
            if idx + 1 < len(args):
                fmt = args[idx + 1]
        run_export(fmt)
    elif args[0] == "summary":
        print_summary()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
