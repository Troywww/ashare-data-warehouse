"""巨潮公告 on-demand fetcher — akshare cninfo 接口

使用 akshare.stock_zh_a_disclosure_report_cninfo 获取公告，
追加写入 announcements 表。

用法：
    announcements = await fetch_announcements("603876", days=30)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_ak = None


def _get_ak():
    global _ak
    if _ak is None:
        try:
            import akshare as ak
            _ak = ak
        except ImportError:
            logger.warning("akshare not installed, announcements unavailable")
            return None
    return _ak


async def fetch_announcements(
    symbol: str,
    days: int = 30,
    keyword: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch announcements from cninfo via akshare.

    Parameters
    ----------
    symbol : str
        6-digit stock code.
    days : int
        How many days back to search.
    keyword : str | None
        Optional keyword filter (e.g. "审计", "减持").

    Returns
    -------
    list of {id, symbol, title, announce_type, date, url}.
    """
    ak = _get_ak()
    if ak is None:
        return []

    end = date.today()
    start = end - timedelta(days=days)

    try:
        df = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=symbol,
            market="沪深京",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            keyword=keyword or "",
        )
    except Exception as e:
        logger.warning("announcements fetch failed for %s: %s", symbol, e)
        return []

    if df is None or df.empty:
        return []

    results = []
    for _, row in df.iterrows():
        title = str(row.get("公告标题", row.get("title", "")))
        date_val = row.get("公告时间", row.get("date", row.get("announcementDate", "")))
        url = str(row.get("公告链接", row.get("url", row.get("adjunctUrl", ""))))

        results.append({
            "id": str(row.get("announcementId", row.get("id", _hash(title + str(date_val))))),
            "symbol": symbol,
            "title": title,
            "announce_type": str(row.get("公告类别", row.get("category", ""))),
            "date": _parse_date(date_val),
            "url": url,
        })

    return results


def _parse_date(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        s = str(val).replace("-", "").replace("/", "").strip()
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None


def _hash(s: str) -> str:
    import hashlib
    return hashlib.md5(s.encode()).hexdigest()[:12]
