"""On-demand real-time quote fetcher — uses easy_tdx.

Each function is standalone, knows nothing about cache.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from easy_tdx import TdxClient
from easy_tdx.models.enums import Market

logger = logging.getLogger(__name__)


def _parse_market(symbol: str) -> tuple[Market, str]:
    """Determine market from symbol prefix."""
    if symbol.startswith(("6", "68")):
        return Market.SH, symbol
    elif symbol.startswith("92"):
        return Market.BJ, symbol
    else:
        return Market.SZ, symbol


async def fetch_realtime_quote(symbol: str) -> dict[str, Any] | None:
    """Fetch real-time quote for a single stock via easy_tdx.

    Returns dict with keys: open, high, low, close, pre_close, vol, amount,
    bid1~5, ask1~5, etc.
    """
    market, code = _parse_market(symbol)
    try:
        with TdxClient.from_best_host() as client:
            df = client.get_security_quotes([(market, code)])
            if not df.empty:
                return df.iloc[0].to_dict()
        return None
    except Exception as e:
        logger.warning("realtime_quote fetch failed for %s: %s", symbol, e)
        return None


async def fetch_realtime_quotes(symbols: list[str]) -> list[dict[str, Any]]:
    """Fetch real-time quotes for multiple stocks via easy_tdx.

    Batches all symbols into a single call (max 80 per batch).
    """
    all_results: list[dict[str, Any]] = []
    BATCH_SIZE = 80

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        pairs = []
        for s in batch:
            market, code = _parse_market(s)
            pairs.append((market, code))

        try:
            with TdxClient.from_best_host() as client:
                df = client.get_security_quotes(pairs)
                if not df.empty:
                    all_results.extend(df.to_dict("records"))
        except Exception as e:
            logger.warning("realtime_quotes batch fetch failed: %s", e)

    return all_results


async def fetch_intraday_kline(
    symbol: str,
    period: str = "1min",
    count: int = 240,
) -> list[dict[str, Any]]:
    """Fetch intraday minute K-line via easy_tdx MacClient.

    Uses MacClient for minute-level kline data.
    """
    from easy_tdx.mac.client import MacClient
    from easy_tdx.mac.enums import Period as MacPeriod

    period_map = {
        "1min": MacPeriod.MIN_1,
        "5min": MacPeriod.MIN_5,
        "15min": MacPeriod.MIN_15,
        "30min": MacPeriod.MIN_30,
        "60min": MacPeriod.MIN_60,
    }
    p = period_map.get(period, MacPeriod.MIN_1)
    mkt_code = _parse_market(symbol)

    try:
        with MacClient("121.36.248.138", timeout=10) as client:
            df = client.get_stock_kline(mkt_code[0], mkt_code[1], period=p, count=count)
            if df.empty:
                return []
            return df.to_dict("records")
    except Exception as e:
        logger.warning("intraday_kline fetch failed for %s: %s", symbol, e)
        return []


async def fetch_limit_up_ladder() -> list[dict[str, Any]]:
    """Fetch today's limit-up stock ladder (涨停梯队) via akshare.

    Returns list of limit-up stocks with: symbol, name, limit_up_count (连板数),
    first_limit_up_time (首次封板时间), last_limit_up_time (最后封板时间),
    turnover_rate, etc.
    """
    import akshare as ak

    try:
        df = ak.stock_zt_pool_em(date=datetime.now().strftime("%Y%m%d"))
    except Exception:
        # Try previous trading day if today's data not available
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        try:
            df = ak.stock_zt_pool_em(date=yesterday)
        except Exception as e:
            logger.warning("limit_up_ladder fetch failed: %s", e)
            return []

    if df is None or df.empty:
        return []

    field_map = {
        "symbol": "代码",
        "name": "名称",
        "close": "最新价",
        "change_pct": "涨跌幅",
        "limit_up_count": "连板数",
        "first_limit_up_time": "首次封板时间",
        "last_limit_up_time": "最后封板时间",
        "open_count": "炸板次数",
        "limit_up_strength": "封单量",
        "turnover_rate": "换手率",
        "volume": "成交额",
        "float_mv": "流通市值",
        "total_mv": "总市值",
        "industry": "所属行业",
    }
    rev = {v: k for k, v in field_map.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(field_map.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    return df.to_dict(orient="records")
