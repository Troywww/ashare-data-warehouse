"""Fetcher: hot_stocks — 雪球关注热度.

Source: akshare stock_hot_follow_xq() (HTTP, 雪球)
Fields: rank, symbol, stock_name, follow_count, price
Schedule: daily incremental
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_FIELD_MAP = {
    "symbol": "股票代码",
    "stock_name": "股票简称",
    "follow_count": "关注",
    "price": "最新价",
}


@retry(max_attempts=3, delay=2.0)
def _fetch_hot_stocks() -> pd.DataFrame:
    """Fetch hot stocks ranking from akshare (雪球)."""
    import akshare as ak
    df = ak.stock_hot_follow_xq()
    if df is None or df.empty:
        return pd.DataFrame()

    rev = {v: k for k, v in _FIELD_MAP.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(_FIELD_MAP.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    # Strip SH/SZ prefix from symbol (akshare returns "SH600519")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].str.replace(r"^(SH|SZ|BJ)", "", regex=True)

    # Add sequential rank (API doesn't provide it)
    df["rank"] = range(1, len(df) + 1)

    return df


@register_fetcher(
    "hot_stocks",
    group="signals",
    description="雪球关注热度 — akshare stock_hot_follow_xq()",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist hot stocks ranking."""
    df = _fetch_hot_stocks()
    if df.empty:
        logger.warning("hot_stocks: no data from akshare")
        return 0

    df["date"] = trade_date
    return db.upsert_dataframe("hot_stocks", df)
