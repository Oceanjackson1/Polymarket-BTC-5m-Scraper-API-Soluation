"""
事件采集模块 - 从 Gamma API 获取所有 BTC 5分钟价格预测事件
"""
from tqdm import tqdm
from src.api_client import api_get
from src.config import (
    EVENTS_URL,
    TAG_ID_5M,
    SLUG_PREFIX,
    EVENTS_PAGE_SIZE,
    POLYMARKET_EVENT_URL_TEMPLATE,
)
from src.database import (
    init_db,
    save_events,
    save_progress,
    get_progress,
    get_event_count,
)


def _parse_event(raw):
    """
    从 Gamma API 返回的原始事件数据中提取关键字段。
    只保留 slug 以 'btc-updown-5m-' 开头的事件。
    """
    slug = raw.get("slug", "")
    if not slug.startswith(SLUG_PREFIX):
        return None

    markets = raw.get("markets", [])
    market = markets[0] if markets else {}

    return {
        "id": int(raw.get("id", 0)),
        "slug": slug,
        "title": raw.get("title", ""),
        "condition_id": market.get("conditionId", ""),
        "start_time": raw.get("startTime", raw.get("startDate", "")),
        "end_time": raw.get("endDate", ""),
        "volume": raw.get("volume", 0) or market.get("volumeNum", 0) or 0,
        "outcome_prices": market.get("outcomePrices", ""),
        "closed": 1 if raw.get("closed") else 0,
        "polymarket_url": POLYMARKET_EVENT_URL_TEMPLATE.format(slug=slug),
        "created_at": raw.get("createdAt", ""),
    }


def fetch_all_events(resume: bool = True) -> int:
    """
    分页获取所有 BTC 5分钟事件并存入数据库。

    参数:
        resume: 是否从上次中断的位置继续

    返回:
        新增事件数
    """
    init_db()
    task_name = "fetch_events"

    start_offset = 0
    if resume:
        progress = get_progress(task_name)
        if progress:
            start_offset = progress["last_offset"]
            print(f"[Events] 从上次进度恢复: offset={start_offset}")

    offset = start_offset
    total_new = 0
    consecutive_empty = 0
    max_consecutive_empty = 3  # 连续3页无新事件则停止

    print(f"[Events] 开始采集 BTC 5分钟事件 (offset={offset})...")

    pbar = tqdm(desc="采集事件", unit="页")

    while True:
        params = {
            "tag_id": TAG_ID_5M,
            "limit": EVENTS_PAGE_SIZE,
            "offset": offset,
            "order": "id",
            "ascending": "true",
        }

        data = api_get(EVENTS_URL, params=params)

        if data is None:
            print(f"[Events] 请求失败，offset={offset}，稍后可续传")
            save_progress(task_name, last_offset=offset)
            break

        if not data or len(data) == 0:
            print(f"[Events] 无更多数据，offset={offset}")
            break

        # 过滤出 BTC 5分钟事件
        btc_events = []
        for raw_event in data:
            parsed = _parse_event(raw_event)
            if parsed:
                btc_events.append(parsed)

        # 保存到数据库
        if btc_events:
            inserted = save_events(btc_events)
            total_new += inserted
            if inserted > 0:
                consecutive_empty = 0
            else:
                consecutive_empty += 1
        else:
            consecutive_empty += 1

        pbar.update(1)
        pbar.set_postfix({"新增": total_new, "offset": offset, "本页BTC": len(btc_events)})

        # 保存进度
        save_progress(task_name, last_offset=offset + EVENTS_PAGE_SIZE)

        # 如果返回的数据少于 page_size，说明已到最后一页
        if len(data) < EVENTS_PAGE_SIZE:
            print(f"[Events] 已到最后一页 (返回 {len(data)} 条)")
            break

        # 连续多页无新事件但数据还在返回，继续翻页（可能是 ETH/XRP 事件）
        if consecutive_empty >= max_consecutive_empty * 10:
            print(f"[Events] 连续 {consecutive_empty} 页无新 BTC 事件，但仍有数据，继续...")

        offset += EVENTS_PAGE_SIZE

    pbar.close()

    total_events = get_event_count()
    print(f"[Events] 采集完成: 新增 {total_new} 个事件，数据库共 {total_events} 个事件")

    # 完成后重置进度（下次运行可重新增量扫描）
    save_progress(task_name, last_offset=0)

    return total_new


if __name__ == "__main__":
    fetch_all_events(resume=False)
