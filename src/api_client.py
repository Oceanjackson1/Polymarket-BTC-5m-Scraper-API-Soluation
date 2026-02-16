"""
API 客户端模块 - 封装 HTTP 请求，含重试和速率控制
"""
import time
import requests
from src.config import MAX_RETRIES, RETRY_DELAY, REQUEST_DELAY


_last_request_time = 0.0


def _rate_limit():
    """速率控制：确保请求间隔不小于 REQUEST_DELAY"""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()


def api_get(url: str, params=None):
    """
    发起 GET 请求，含重试机制和速率控制。
    返回 JSON 解析后的数据，失败返回 None。
    """
    _rate_limit()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = RETRY_DELAY * attempt * 2
                print(f"[API] 被限流 (429)，等待 {wait:.1f}s 后重试...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.Timeout:
            print(f"[API] 请求超时 (尝试 {attempt}/{MAX_RETRIES}): {url}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
        except requests.exceptions.HTTPError as e:
            print(f"[API] HTTP 错误 (尝试 {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
        except requests.exceptions.RequestException as e:
            print(f"[API] 请求异常 (尝试 {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
        except ValueError:
            print(f"[API] JSON 解析失败 (尝试 {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    print(f"[API] 请求最终失败: {url}")
    return None
