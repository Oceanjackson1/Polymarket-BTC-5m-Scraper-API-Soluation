#!/usr/bin/env python3
"""
获取 Polymarket 单个市场的完整订单簿成交历史
目标市场: btc-updown-5m-1771211700

策略: 分别按 side=BUY 和 side=SELL 获取交易,
突破 Data API 单次查询 4000 条的限制,
然后合并去重获得完整数据集。
"""
import csv
import time
import requests
import sys
from datetime import datetime

EVENT_SLUG = "btc-updown-5m-1771211700"
CONDITION_ID = "0x87d2a99961ada5716db13ad8cb520d2a8d5a751b9e8c2db93e569e1bde521468"
DATA_API_URL = "https://data-api.polymarket.com/trades"
OUTPUT_PATH = "/Users/ocean/Desktop/btc-updown-5m-1771211700_trades.csv"

PAGE_SIZE = 1000
REQUEST_DELAY = 0.35
MAX_RETRIES = 3


def api_get(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 5 * attempt
                print(f"    限流(429), 等待 {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 400:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            return None
        except Exception as e:
            print(f"    请求失败({attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
    return None


def make_trade_key(t):
    """生成唯一交易标识"""
    return (
        f"{t.get('transactionHash', '')}"
        f"|{t.get('timestamp', '')}"
        f"|{t.get('size', '')}"
        f"|{t.get('price', '')}"
        f"|{t.get('side', '')}"
        f"|{t.get('outcome', '')}"
        f"|{t.get('proxyWallet', '')}"
    )


def fetch_trades_by_side(side):
    """按 BUY/SELL 获取全部交易"""
    trades = []
    seen = set()
    offset = 0

    print(f"\n  [{side}] 开始获取...")

    while True:
        params = {
            "market": CONDITION_ID,
            "limit": PAGE_SIZE,
            "offset": offset,
            "side": side,
        }

        time.sleep(REQUEST_DELAY)
        data = api_get(DATA_API_URL, params)

        if data is None:
            print(f"    offset={offset}: 到达 API 上限")
            break
        if not data:
            break

        new_count = 0
        for t in data:
            key = make_trade_key(t)
            if key not in seen:
                seen.add(key)
                trades.append(t)
                new_count += 1

        actual_side = sum(1 for t in data if t.get("side") == side)
        ts_range = f"[{min(t['timestamp'] for t in data)}, {max(t['timestamp'] for t in data)}]"
        print(f"    offset={offset}: 返回 {len(data)} 笔 (匹配 {actual_side}), 新增 {new_count}, 累计 {len(trades)}, ts={ts_range}")

        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"  [{side}] 完成: {len(trades)} 笔")
    return trades, seen


def main():
    print("=" * 64)
    print(f"  Polymarket 完整交易历史获取")
    print(f"  市场: {EVENT_SLUG}")
    print(f"  策略: BUY + SELL 分别获取并合并")
    print("=" * 64)

    # 阶段1: 获取 BUY 交易
    buy_trades, buy_seen = fetch_trades_by_side("BUY")

    # 阶段2: 获取 SELL 交易
    sell_trades, sell_seen = fetch_trades_by_side("SELL")

    # 合并去重
    all_trades = []
    global_seen = set()

    for t in buy_trades:
        key = make_trade_key(t)
        if key not in global_seen:
            global_seen.add(key)
            all_trades.append(t)

    for t in sell_trades:
        key = make_trade_key(t)
        if key not in global_seen:
            global_seen.add(key)
            all_trades.append(t)

    print(f"\n  [合并] BUY={len(buy_trades)} + SELL={len(sell_trades)} => 去重后 {len(all_trades)} 笔")

    if not all_trades:
        print("\n未获取到任何交易数据!")
        sys.exit(1)

    # 按时间戳排序 (升序)
    all_trades.sort(key=lambda t: (t.get("timestamp", 0), t.get("side", ""), t.get("outcome", "")))

    # 统计
    total_size = sum(float(t.get("size", 0)) for t in all_trades)
    total_notional = sum(float(t.get("size", 0)) * float(t.get("price", 0)) for t in all_trades)
    buy_count = sum(1 for t in all_trades if t.get("side") == "BUY")
    sell_count = sum(1 for t in all_trades if t.get("side") == "SELL")
    up_count = sum(1 for t in all_trades if t.get("outcome") == "Up")
    down_count = sum(1 for t in all_trades if t.get("outcome") == "Down")
    unique_wallets = len(set(t.get("proxyWallet", "") for t in all_trades if t.get("proxyWallet")))
    unique_hashes = len(set(t.get("transactionHash", "") for t in all_trades if t.get("transactionHash")))

    earliest = all_trades[0].get("timestamp", 0)
    latest = all_trades[-1].get("timestamp", 0)
    earliest_dt = datetime.utcfromtimestamp(earliest).strftime("%Y-%m-%d %H:%M:%S UTC")
    latest_dt = datetime.utcfromtimestamp(latest).strftime("%Y-%m-%d %H:%M:%S UTC")

    reported_volume = 120551.52

    print(f"\n{'=' * 64}")
    print(f"  完整交易统计")
    print(f"{'=' * 64}")
    print(f"  总交易笔数:        {len(all_trades)}")
    print(f"  唯一交易哈希:      {unique_hashes}")
    print(f"  唯一钱包地址:      {unique_wallets}")
    print(f"  总成交量(size):    ${total_size:,.2f} USDC")
    print(f"  总名义额(size*p): ${total_notional:,.2f} USDC")
    print(f"  买入(BUY):         {buy_count}")
    print(f"  卖出(SELL):        {sell_count}")
    print(f"  Up outcome:        {up_count}")
    print(f"  Down outcome:      {down_count}")
    print(f"  最早交易:          {earliest_dt}")
    print(f"  最晚交易:          {latest_dt}")
    print(f"  Polymarket报告量:  ${reported_volume:,.2f} USDC")
    print(f"  覆盖率(size):      {total_size/reported_volume*100:.1f}%")
    print(f"{'=' * 64}")

    # 保存 CSV
    fieldnames = [
        "trade_no",
        "timestamp",
        "datetime_utc",
        "side",
        "outcome",
        "size_usdc",
        "price",
        "proxy_wallet",
        "trader_name",
        "transaction_hash",
        "event_slug",
        "condition_id",
    ]

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, t in enumerate(all_trades, 1):
            ts = t.get("timestamp", 0)
            try:
                dt_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError):
                dt_str = ""

            writer.writerow({
                "trade_no": i,
                "timestamp": ts,
                "datetime_utc": dt_str,
                "side": t.get("side", ""),
                "outcome": t.get("outcome", ""),
                "size_usdc": t.get("size", 0),
                "price": t.get("price", 0),
                "proxy_wallet": t.get("proxyWallet", ""),
                "trader_name": t.get("name", "") or t.get("pseudonym", ""),
                "transaction_hash": t.get("transactionHash", ""),
                "event_slug": t.get("eventSlug", EVENT_SLUG),
                "condition_id": t.get("conditionId", CONDITION_ID),
            })

    print(f"\n[CSV] 已保存到: {OUTPUT_PATH}")
    print(f"  总行数: {len(all_trades)}")
    print(f"\n完成!")


if __name__ == "__main__":
    main()
