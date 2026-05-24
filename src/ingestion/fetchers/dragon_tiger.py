"""Fetcher: dragon_tiger — 龙虎榜.

Source: akshare stock_lhb_detail_em() (HTTP, 东财)
Fields: 19 fields (全量存储 akshare 返回字段)
Schedule: daily incremental, sliding window 7 days
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_AKSHARE_FIELD_MAP = {
    "symbol": "代码",
    "date": "上榜日",
    "reason": "解读",
    "close": "收盘价",
    "change_pct": "涨跌幅",
    "net_buy": "龙虎榜净买额",
    "buy_amount": "龙虎榜买入额",
    "sell_amount": "龙虎榜卖出额",
    "total_amount": "龙虎榜成交额",
    "market_total_amount": "市场总成交额",
    "net_buy_ratio": "净买额占总成交比",
    "amount_ratio": "成交额占总成交比",
    "turnover_rate": "换手率",
    "float_mv": "流通市值",
    "perf_1d": "上榜后1日",
    "perf_2d": "上榜后2日",
    "perf_5d": "上榜后5日",
    "perf_10d": "上榜后10日",
    "comment": "解读",
}


@retry(max_attempts=3, delay=2.0)
def _fetch_dragon_tiger(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch dragon tiger data from akshare."""
    try:
        import akshare as ak
        # akshare expects YYYYMMDD, not YYYY-MM-DD
        df = ak.stock_lhb_detail_em(
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
        )
        if df is None or df.empty:
            logger.warning("dragon_tiger: akshare returned empty data for %s ~ %s", start_date, end_date)
            return pd.DataFrame()

        # Rename columns to match schema
        reverse_map = {v: k for k, v in _AKSHARE_FIELD_MAP.items() if v in df.columns}
        df = df.rename(columns=reverse_map)

        # Keep only known columns
        known = list(_AKSHARE_FIELD_MAP.keys())
        cols = [c for c in known if c in df.columns]
        df = df[cols]

        # Ensure date is date type
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        # Ensure symbol is 6-digit string
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.zfill(6)

        return df
    except ImportError:
        logger.error("akshare not installed — install with: pip install akshare")
        raise


@register_fetcher(
    "dragon_tiger",
    group="signals",
    description="龙虎榜 — akshare stock_lhb_detail_em() 19字段全量",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist dragon tiger data.

    Uses sliding window: last 7 days from trade_date.
    INSERT OR REPLACE handles dedup via PK (symbol, date, reason).
    """
    start = (trade_date - timedelta(days=7)).isoformat()
    end = trade_date.isoformat()

    logger.info("dragon_tiger: fetching %s ~ %s", start, end)
    df = _fetch_dragon_tiger(start, end)

    if df.empty:
        logger.info("dragon_tiger: no data for %s ~ %s", start, end)
        return 0

    return db.upsert_dataframe("dragon_tiger", df)
