"""Fetcher: fundamentals — 季度财务数据.

Source: akshare stock_yjbb_em() (HTTP, 东财)
Fields: eps, roe, revenue, profit, revenue_yoy, profit_yoy, bvps, operating_cashflow, gross_margin
Schedule: quarterly (Mar/Jun/Sep/Dec)
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_FIELD_MAP = {
    "symbol": "股票代码",
    "end_date": "期末日期",
    "publ_date": "最新公告日期",
    "eps": "每股收益",
    "roe": "净资产收益率",
    "revenue": "营业总收入",
    "profit": "净利润",
    "revenue_yoy": "营业总收入同比增长率",
    "profit_yoy": "净利润增长率",
    "bvps": "每股净资产",
    "operating_cashflow": "每股经营现金流量",
    "gross_margin": "销售毛利率",
}


def _quarter_end(trade_date: date) -> str:
    """Calculate the previous quarter-end date string (YYYY-MM-DD).

    e.g. May 2025 → "2025-03-31", Aug 2025 → "2025-06-30"
    """
    m = trade_date.month
    y = trade_date.year
    if m <= 3:
        qm, qy = 12, y - 1  # 去年Q4
    elif m <= 6:
        qm, qy = 3, y
    elif m <= 9:
        qm, qy = 6, y
    else:
        qm, qy = 9, y

    # Last day of the quarter month
    import calendar
    last_day = calendar.monthrange(qy, qm)[1]
    return f"{qy}-{qm:02d}-{last_day:02d}"


@retry(max_attempts=3, delay=2.0)
def _fetch_fundamentals(end_date_str: str) -> pd.DataFrame:
    """Fetch quarterly fundamentals from akshare."""
    import akshare as ak

    # akshare expects YYYYMMDD format
    date_code = end_date_str.replace("-", "")
    df = ak.stock_yjbb_em(date=date_code)
    if df is None or df.empty:
        return pd.DataFrame()

    rev = {v: k for k, v in _FIELD_MAP.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(_FIELD_MAP.keys())
    cols = [c for c in known if c in df.columns]

    # Only keep mapped columns, add end_date
    df = df[cols]
    df["end_date"] = end_date_str[:10]

    # Remove duplicates on symbol
    df = df.drop_duplicates(subset=["symbol"])

    # Convert types
    if "publ_date" in df.columns and df["publ_date"].dtype == object:
        df["publ_date"] = pd.to_datetime(df["publ_date"]).dt.date
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    return df


@register_fetcher(
    "fundamentals",
    group="lowfreq",
    description="季度财务数据 — akshare stock_yjbb_em() 全市场含北交所",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist quarterly fundamentals.

    Skips if we already have data for the most recent quarter.
    """
    end_date_str = _quarter_end(trade_date)

    # Check if we already have this quarter
    max_end = db.get_max_date("fundamentals")
    if max_end is not None:
        # Convert end_date_str to date for comparison
        q_end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        if max_end >= q_end:
            logger.info("fundamentals: quarter %s already loaded (max=%s), skipping", end_date_str, max_end)
            return 0

    df = _fetch_fundamentals(end_date_str)
    if df.empty:
        logger.warning("fundamentals: no data for quarter %s", end_date_str)
        return 0

    return db.upsert_dataframe("fundamentals", df)
