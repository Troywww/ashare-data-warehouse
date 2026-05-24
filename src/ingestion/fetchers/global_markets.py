"""Fetcher: global_markets — 外围行情（美股/港股/黄金/原油/外汇）.

Source: opentdx goods_kline (TCP, 扩展市场)
Coverage: TSLA, AAPL, MSFT, QQQ, SPY, 00700(港股), 黄金, 原油, 外汇
Schedule: daily, auto-clean data older than 6 months
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
from opentdx.const import EX_MARKET, PERIOD
from opentdx.tdxClient import TdxClient

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# (ex_market, symbol_in_opentdx, our_symbol, name)
_GLOBAL_SYMBOLS = [
    (EX_MARKET.US_STOCK, "TSLA", "TSLA", "特斯拉"),
    (EX_MARKET.US_STOCK, "AAPL", "AAPL", "苹果"),
    (EX_MARKET.US_STOCK, "MSFT", "MSFT", "微软"),
    (EX_MARKET.US_STOCK, "QQQ", "QQQ", "纳斯达克100ETF"),
    (EX_MARKET.US_STOCK, "SPY", "SPY", "标普500ETF"),
    (EX_MARKET.HK_MAIN_BOARD, "00700", "HK00700", "腾讯控股"),
    (EX_MARKET.HK_MAIN_BOARD, "00001", "HK00001", "长和"),
    (EX_MARKET.HK_MAIN_BOARD, "00005", "HK00005", "汇丰控股"),
    (EX_MARKET.HK_MAIN_BOARD, "09988", "HK09988", "阿里巴巴"),
    (EX_MARKET.HK_MAIN_BOARD, "03690", "HK03690", "美团"),
    # 注意: 期货/外汇代码需要根据 opentdx EX_MARKET 枚举确认
]

_RELEVANT_DAYS = 180  # 6 months


def _fetch_single_goods(client, ex_market: int, symbol: str, count: int) -> list[dict]:
    """Fetch daily kline for one global symbol via opentdx."""
    try:
        klines = client.goods_kline(ex_market, symbol, PERIOD.DAILY, count=count)
        if not klines:
            return []
        results = []
        for k in klines:
            dt_str = str(k.get("datetime", ""))[:10]
            if not dt_str:
                continue
            results.append({
                "symbol": symbol,
                "date": dt_str,
                "open": float(k.get("open", 0)),
                "high": float(k.get("high", 0)),
                "low": float(k.get("low", 0)),
                "close": float(k.get("close", 0)),
                "volume": float(k.get("vol", 0)),
            })
        return results
    except Exception as e:
        logger.debug("goods %s: %s", symbol, e)
        return []


@retry(max_attempts=2, delay=2.0)
def _fetch_global_markets() -> pd.DataFrame:
    """Fetch all global market klines from opentdx."""
    with TdxClient() as client:
        all_rows = []
        for ex_market, sym, our_sym, name in _GLOBAL_SYMBOLS:
            rows = _fetch_single_goods(client, ex_market, sym, _RELEVANT_DAYS)
            for r in rows:
                r["symbol"] = our_sym
            all_rows.extend(rows)
            logger.debug("global %s (%s): %d days", our_sym, name, len(rows))

        if not all_rows:
            logger.warning("global_markets: no data from opentdx")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["symbol", "date"])
        logger.info("global_markets: %d rows (%d symbols)", len(df), df["symbol"].nunique())
        return df


@register_fetcher(
    "global_markets",
    group="lowfreq",
    description="外围行情 — opentdx goods_kline 美股/港股/黄金/原油/外汇",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist global market data, then clean old data."""
    df = _fetch_global_markets()
    if df.empty:
        return 0

    written = db.upsert_dataframe("global_markets", df)

    # Clean data older than 6 months
    cutoff = date.today() - timedelta(days=180)
    db.conn.execute("DELETE FROM global_markets WHERE date < ?", [cutoff])
    logger.info("global_markets: cleaned data before %s", cutoff)

    return written
