"""Fetcher: margin_trading — 融资融券.

Source: akshare stock_margin_detail_sse() + stock_margin_detail_szse() (HTTP)
Fields: symbol, date, rzye, rzye_buy, rzye_repay, rqyl, rqyl_sell, rqyl_repay, rqyl_amt, rzrqye
Schedule: daily incremental
Note: 北交所非两融标的
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_FIELD_MAP_SSE = {
    "symbol": "标的证券代码",
    "date": "交易日",
    "rzye": "融资余额",
    "rzye_buy": "融资买入额",
    "rzye_repay": "融资偿还额",
    "rqyl": "融券余量",
    "rqyl_sell": "融券卖出量",
    "rqyl_repay": "融券偿还量",
}

_FIELD_MAP_SZSE = {
    "symbol": "证券代码",
    "date": "信用交易日期",
    "rzye": "融资余额",
    "rqyl": "融券余量",
    "rqyl_amt": "融券余额",
    "rzrqye": "融资融券余额",
}


@retry(max_attempts=3, delay=2.0)
def _fetch_sse(date_str: str) -> pd.DataFrame:
    """Fetch SSE margin trading data from akshare."""
    import akshare as ak
    try:
        df = ak.stock_margin_detail_sse(date=date_str)
    except Exception:
        # akshare may fail on empty data days; try default date
        try:
            df = ak.stock_margin_detail_sse()
        except Exception:
            return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    rev = {v: k for k, v in _FIELD_MAP_SSE.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(_FIELD_MAP_SSE.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    return df


@retry(max_attempts=3, delay=2.0)
def _fetch_szse(date_str: str) -> pd.DataFrame:
    """Fetch SZSE margin trading data from akshare."""
    import akshare as ak
    try:
        df = ak.stock_margin_detail_szse(date=date_str)
    except Exception:
        try:
            df = ak.stock_margin_detail_szse()
        except Exception:
            return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    rev = {v: k for k, v in _FIELD_MAP_SZSE.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(_FIELD_MAP_SZSE.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    return df


@register_fetcher(
    "margin_trading",
    group="signals",
    description="融资融券 — akshare SSE+SZSE 双接口",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist margin trading data."""
    date_str = trade_date.isoformat()
    total = 0

    # SSE
    df_sse = _fetch_sse(date_str)
    if not df_sse.empty:
        df_sse["date"] = trade_date
        total += db.upsert_dataframe("margin_trading", df_sse)
        logger.info("margin_trading: SSE → %d rows", len(df_sse))

    # SZSE
    df_szse = _fetch_szse(date_str)
    if not df_szse.empty:
        df_szse["date"] = trade_date
        total += db.upsert_dataframe("margin_trading", df_szse)
        logger.info("margin_trading: SZSE → %d rows", len(df_szse))

    if total == 0:
        logger.warning("margin_trading: no data for %s", date_str)

    return total
