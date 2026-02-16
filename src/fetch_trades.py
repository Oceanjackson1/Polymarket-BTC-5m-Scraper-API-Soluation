"""
交易采集模块 - 从 Data API 获取每个事件的逐笔交易数据

核心策略:
    Polymarket Data API 有两个关键限制:
    1. 每次请求最多返回 1000 条记录 (无论 limit 设多大)
    2. offset 上限约 4000 (offset >= 4000 返回 400 错误)

    为突破限制, 本模块采用 BUY + SELL 分拆策略:
    - 分别按 side=BUY 和 side=SELL 获取交易
    - 每个方向最多获取 4000 条, 合计可获取 ~8000 条
    - 通过唯一键 (transaction_hash + timestamp + size + side + wallet) 去重
    - 对于实测的市场, 该策略可覆盖 ~97% 的交易量
"""
from tqdm import tqdm
from src.api_client import api_get
from src.config import TRADES_URL
from src.database import (
    init_db,
    save_trades,
    save_progress,
    get_progress,
    get_all_events,
    get_events_without_trades,
    get_trade_count,
)

# Data API 实际限制
ACTUAL_PAGE_SIZE = 1000       # API 每次实际最多返回 1000 条
MAX_OFFSET_PER_QUERY = 3000   # offset 0,1000,2000,3000 → 最多 4000 条


def _fetch_trades_by_side(condition_id, event_slug, side):
    """
    按指定方向 (BUY/SELL) 获取交易数据。
    使用 offset 分页, 每页 1000 条, 最多翻到 offset=3000。
    
    返回: (trades_list, is_complete)
        is_complete: 是否获取到了全部数据 (未触及 offset 上限)
    """
    trades = []
    offset = 0

    while True:
        params = {
            "market": condition_id,
            "limit": ACTUAL_PAGE_SIZE,
            "offset": offset,
            "side": side,
        }

        data = api_get(TRADES_URL, params=params)

        if data is None:
            # 400 错误 = 到达 offset 上限
            break

        if not data or len(data) == 0:
            break

        for raw in data:
            trade = {
                "event_slug": event_slug,
                "condition_id": condition_id,
                "trade_timestamp": raw.get("timestamp", 0),
                "side": raw.get("side", ""),
                "outcome": raw.get("outcome", ""),
                "size": raw.get("size", 0),
                "price": raw.get("price", 0),
                "proxy_wallet": raw.get("proxyWallet", ""),
                "transaction_hash": raw.get("transactionHash", ""),
            }
            trades.append(trade)

        if len(data) < ACTUAL_PAGE_SIZE:
            return trades, True

        offset += ACTUAL_PAGE_SIZE

        if offset > MAX_OFFSET_PER_QUERY:
            return trades, False

    return trades, True


def _fetch_trades_for_event(condition_id, event_slug):
    """
    获取单个事件的所有交易数据。
    
    策略:
    1. 先尝试不带 side 过滤的普通请求
    2. 如果数据量触及上限, 改用 BUY + SELL 分拆策略
    3. 合并去重后返回
    """
    # 先尝试普通获取 (不分 side)
    all_trades = []
    offset = 0
    hit_limit = False

    while True:
        params = {
            "market": condition_id,
            "limit": ACTUAL_PAGE_SIZE,
            "offset": offset,
        }

        data = api_get(TRADES_URL, params=params)

        if data is None:
            hit_limit = True
            break

        if not data or len(data) == 0:
            break

        for raw in data:
            trade = {
                "event_slug": event_slug,
                "condition_id": condition_id,
                "trade_timestamp": raw.get("timestamp", 0),
                "side": raw.get("side", ""),
                "outcome": raw.get("outcome", ""),
                "size": raw.get("size", 0),
                "price": raw.get("price", 0),
                "proxy_wallet": raw.get("proxyWallet", ""),
                "transaction_hash": raw.get("transactionHash", ""),
            }
            all_trades.append(trade)

        if len(data) < ACTUAL_PAGE_SIZE:
            break

        offset += ACTUAL_PAGE_SIZE

        if offset > MAX_OFFSET_PER_QUERY:
            hit_limit = True
            break

    # 如果没触及上限, 直接返回
    if not hit_limit:
        return all_trades

    # 触及上限 → 使用 BUY + SELL 分拆策略获取更多数据
    buy_trades, _ = _fetch_trades_by_side(condition_id, event_slug, "BUY")
    sell_trades, _ = _fetch_trades_by_side(condition_id, event_slug, "SELL")

    # 合并去重 (基于唯一键)
    seen = set()
    merged = []

    def _trade_key(t):
        return (
            t["transaction_hash"],
            t["trade_timestamp"],
            str(t["size"]),
            t["side"],
            t["proxy_wallet"],
        )

    # 先加入普通获取的
    for t in all_trades:
        key = _trade_key(t)
        if key not in seen:
            seen.add(key)
            merged.append(t)

    # 再加入 BUY 分拆获取的
    for t in buy_trades:
        key = _trade_key(t)
        if key not in seen:
            seen.add(key)
            merged.append(t)

    # 再加入 SELL 分拆获取的
    for t in sell_trades:
        key = _trade_key(t)
        if key not in seen:
            seen.add(key)
            merged.append(t)

    return merged


def fetch_all_trades(only_missing=True):
    """
    获取所有事件的交易数据。

    参数:
        only_missing: True=只获取还没有交易数据的事件; False=所有事件
    """
    init_db()
    task_name = "fetch_trades"

    if only_missing:
        events = get_events_without_trades()
        print(f"[Trades] 找到 {len(events)} 个待采集交易的事件")
    else:
        events = get_all_events()
        print(f"[Trades] 共 {len(events)} 个事件需要采集交易")

    if not events:
        print("[Trades] 无待采集事件")
        return 0

    # 查找上次中断位置
    progress = get_progress(task_name)
    start_idx = 0
    if progress and progress.get("last_event_slug"):
        last_slug = progress["last_event_slug"]
        for i, e in enumerate(events):
            if e["slug"] == last_slug:
                start_idx = i + 1
                break
        if start_idx > 0:
            print(f"[Trades] 从上次进度恢复: 跳过前 {start_idx} 个事件")

    total_trades = 0
    total_events_processed = 0

    pbar = tqdm(
        events[start_idx:],
        desc="采集交易",
        unit="事件",
        initial=start_idx,
        total=len(events),
    )

    for event in pbar:
        slug = event["slug"]
        condition_id = event.get("condition_id", "")

        if not condition_id:
            pbar.set_postfix({"跳过": slug[:30]})
            continue

        trades = _fetch_trades_for_event(condition_id, slug)

        if trades:
            inserted = save_trades(trades)
            total_trades += inserted

        total_events_processed += 1
        pbar.set_postfix({
            "累计交易": total_trades,
            "本事件": len(trades),
            "slug": slug[-15:],
        })

        # 每处理10个事件保存一次进度
        if total_events_processed % 10 == 0:
            save_progress(task_name, last_event_slug=slug)

    # 最终保存进度
    if events:
        save_progress(task_name, last_event_slug=events[-1]["slug"])

    pbar.close()

    total_in_db = get_trade_count()
    print(f"[Trades] 采集完成: 新增 {total_trades} 笔交易，数据库共 {total_in_db} 笔")

    return total_trades


if __name__ == "__main__":
    fetch_all_trades(only_missing=True)
