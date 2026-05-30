"""Fetcher: global_markets — 外围行情（美股/港股/黄金/原油/外汇）.

Source: easy_tdx MacExClient.goods_kline (TCP, MAC 扩展市场协议)
Coverage: TSLA, AAPL, MSFT, QQQ, SPY, 00700(港股), etc.
Schedule: daily, auto-clean data older than 6 months
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
from easy_tdx.ex.mac_client import MacExClient
from easy_tdx.mac.enums import ExMarket

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# (ex_market, symbol_in_tdx, our_symbol, name)
_GLOBAL_SYMBOLS = [
    (ExMarket.US_STOCK, "TSLA", "TSLA", "特斯拉"),
    (ExMarket.US_STOCK, "AAPL", "AAPL", "苹果"),
    (ExMarket.US_STOCK, "MSFT", "MSFT", "微软"),
    (ExMarket.US_STOCK, "QQQ", "QQQ", "纳斯达克100ETF"),
    (ExMarket.US_STOCK, "SPY", "SPY", "标普500ETF"),
    (ExMarket.HK_MAIN_BOARD, "00700", "HK00700", "腾讯控股"),
    (ExMarket.HK_MAIN_BOARD, "00001", "HK00001", "长和"),
    (ExMarket.HK_MAIN_BOARD, "00005", "HK00005", "汇丰控股"),
    (ExMarket.HK_MAIN_BOARD, "09988", "HK09988", "阿里巴巴"),
    (ExMarket.HK_MAIN_BOARD, "03690", "HK03690", "美团"),
]

_RELEVANT_DAYS = 180  # 6 months


@retry(max_attempts=2, delay=2.0)
def _fetch_global_markets() -> pd.DataFrame:
    """Fetch all global market klines from easy_tdx MacExClient."""
    with MacExClient.from_best_host() as client:
        all_rows = []
        for ex_market, sym, our_sym, name in _GLOBAL_SYMBOLS:
            try:
                df = client.goods_kline(ex_market, sym, count=_RELEVANT_DAYS)
                if df.empty:
                    logger.debug("global %s (%s): empty", our_sym, name)
                    continue
                for _, row in df.iterrows():
                    dt = row.get("datetime")
                    if dt is None:
                        continue
                    dt_str = str(dt)[:10] if hasattr(dt, "strftime") else str(dt)[:10]
                    all_rows.append({
                        "symbol": our_sym,
                        "date": dt_str,
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": float(row.get("vol", 0)),
                    })
            except Exception as e:
                logger.debug("global %s (%s): %s", our_sym, name, e)

        if not all_rows:
            logger.warning("global_markets: no data from easy_tdx")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["symbol", "date"])
        logger.info("global_markets: %d rows (%d symbols)", len(df), df["symbol"].nunique())
        return df


@register_fetcher(
    "global_markets",
    group="lowfreq",
    description="外围行情 — easy_tdx MacExClient goods_kline",
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
