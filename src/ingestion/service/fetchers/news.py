"""On-demand news fetchers — Sina rolling news + East Money stock news.

Each function is a standalone callable that fetches data from source.
They are used by DataService when cache misses and data needs to be fetched.

CLS (财联社) removed 2026-05: all API endpoints blocked by signature verification.
Replaced with Sina Finance rolling news for market-wide coverage.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 新浪财经滚动新闻（替代财联社快讯）
# ---------------------------------------------------------------------------

_SINA_ROLL_URL = "https://feed.mix.sina.com.cn/api/roll/get"


async def fetch_cls_telegram(page_size: int = 50) -> list[dict[str, Any]]:
    """Fetch latest market rolling news from Sina Finance (新浪财经).

    Replaces the defunct CLS telegraph API.
    Returns list of {id, title, content, created_at}.
    """
    params = {
        "pageid": "153",
        "lid": "2509",          # 滚动新闻
        "num": page_size,
        "page": 1,
        "r": str(time.time() % 1),
        "callback": "feedShow",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(_SINA_ROLL_URL, params=params, headers=headers)
        resp.raise_for_status()
        raw = resp.text

    # Parse: try{feedShow({...})}catch(e){}
    try:
        start = raw.index("feedShow(") + 9
        depth = 0
        end = start
        for i in range(start, len(raw)):
            if raw[i] == "{":
                depth += 1
            elif raw[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        data = json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError):
        logger.warning("Failed to parse Sina roll news response")
        return []

    items = []
    for item in data.get("result", {}).get("data", []):
        if not isinstance(item, dict):
            continue
        ctime_str = item.get("ctime", "")
        try:
            created = datetime.fromtimestamp(int(ctime_str))
        except (ValueError, TypeError, OSError):
            created = datetime.now()
        items.append({
            "id": item.get("docId", item.get("id", "")),
            "title": item.get("title", ""),
            "content": item.get("intro", item.get("summary", "")) or "",
            "created_at": created,
        })
    return items


# ---------------------------------------------------------------------------
# 东方财富个股新闻
# ---------------------------------------------------------------------------


async def fetch_stock_news(symbol: str, page_size: int = 20) -> list[dict[str, Any]]:
    """Fetch stock-specific news from East Money via akshare.

    Returns list of {symbol, id, title, content, source, time, url}.
    """
    try:
        import akshare as ak
    except ImportError:
        logger.warning("akshare not installed, stock_news unavailable")
        return []

    try:
        df = _safe_stock_news_em(ak, symbol)
    except Exception as e:
        logger.warning("stock_news fetch failed for %s: %s", symbol, e)
        return []

    if df is None or df.empty:
        return []

    items = []
    for _, row in df.iterrows():
        items.append({
            "symbol": symbol,
            "id": str(row.get("新闻代码", row.get("id", ""))),
            "title": str(row.get("新闻标题", row.get("title", ""))),
            "content": str(row.get("新闻内容", row.get("content", "")))[:500],
            "source": str(row.get("文章来源", row.get("source", ""))),
            "time": _parse_time(row.get("发布时间", row.get("time", ""))),
            "url": str(row.get("新闻链接", row.get("url", ""))),
        })
    return items


def _safe_stock_news_em(ak, symbol: str) -> pd.DataFrame:
    """Call akshare stock_news_em with workaround for pandas 3.0 pyarrow bug.

    akshare uses str.replace(r\"\\u3000\", ...) which crashes on
    pandas 3.0 + pyarrow backend. We monkey-patch the string accessor
    to fall back to Python strings when this specific regex fails.
    """
    # Temporarily disable pyarrow string backend to avoid the regex bug
    original_option = pd.options.mode.string_storage
    try:
        pd.options.mode.string_storage = "python"
        return ak.stock_news_em(symbol=symbol)
    finally:
        pd.options.mode.string_storage = original_option


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_time(val) -> datetime | None:
    """Parse various time formats."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val / 1000 if val > 1e12 else val)
        except (ValueError, OSError):
            return None
    s = str(val).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None
