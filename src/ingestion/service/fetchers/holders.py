"""增减持/股东变动 on-demand fetcher — 使用 akshare（同花顺源）

按需拉取单只股票的股东增减持记录，
追加写入 shareholder_changes 表。

注意：akshare 是惰性导入（仅在调用此 fetcher 时加载），
避免增加 MCP 容器的启动依赖。
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

# 在函数内部惰性导入 akshare，避免 MCP 容器启动时就必须装 akshare
_ak = None


def _get_ak():
    global _ak
    if _ak is None:
        try:
            import akshare as ak
            _ak = ak
        except ImportError:
            logger.warning("akshare not installed, shareholder_changes unavailable")
            return None
    return _ak


async def fetch_shareholder_changes(symbol: str) -> list[dict[str, Any]]:
    """Fetch shareholder change records for a stock via akshare (同花顺).

    Returns list of {symbol, announce_date, shareholder, change_type,
                     change_vol, hold_vol}.
    """
    ak = _get_ak()
    if ak is None:
        return []

    try:
        df = ak.stock_shareholder_change_ths(symbol)
    except Exception as e:
        logger.warning("shareholder_changes fetch failed for %s: %s", symbol, e)
        return []

    if df is None or df.empty:
        return []

    # Map Chinese column names to English
    results = []
    for _, row in df.iterrows():
        record = {"symbol": symbol}
        for i, col in enumerate(df.columns):
            val = row.iloc[i]
            if "公告" in col:
                record["announce_date"] = _parse_date(val)
            elif "变动股东" in col or "股东" in col:
                record["shareholder"] = str(val) if val else ""
            elif "变动" in col and ("方向" in col or "类型" in col):
                record["change_type"] = str(val) if val else ""
            elif "交易" in col:
                record["avg_price"] = _parse_vol(val)
            elif "剩余" in col:
                record["hold_vol"] = _parse_vol(val)
            elif "变动期间" in col:
                record["change_period"] = str(val) if val else ""
            elif "途径" in col or "方式" in col:
                record["change_method"] = str(val) if val else ""
        # Defaults for fields not in 同花顺 data
        record.setdefault("announce_date", None)
        record.setdefault("shareholder", "")
        record.setdefault("change_type", "")
        record.setdefault("change_vol", None)
        record.setdefault("hold_vol", None)
        results.append(record)

    return results


def _parse_date(val) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_vol(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").replace("亿", "").replace("万", ""))
    except (ValueError, TypeError):
        return None
