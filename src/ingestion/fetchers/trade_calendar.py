"""Fetcher: trade_calendar — 交易日历.

Source: baostock query_trade_dates()
Schedule: yearly (Dec), full replace
"""
from __future__ import annotations

import logging
from datetime import date

import baostock as bs
import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# 查询范围（从 1990 年至今足够覆盖 A 股全部历史）
_START_DATE = "1990-01-01"


@retry(max_attempts=3, delay=2.0)
def _fetch_trade_calendar() -> pd.DataFrame:
    """Pull full trade calendar from baostock."""
    login_ok = bs.login()
    if login_ok.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login_ok.error_msg}")

    try:
        end_date = date.today().isoformat()
        rs = bs.query_trade_dates(start_date=_START_DATE, end_date=end_date)
        if rs.error_code != "0":
            raise RuntimeError(f"query_trade_dates failed: {rs.error_msg}")

        rows = []
        while rs.next():
            row = rs.get_row_data()
            rows.append({"date": row[0], "is_trading": row[1] == "1"})

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        logger.info("trade_calendar: %d dates from baostock", len(df))
        return df
    finally:
        bs.logout()


@register_fetcher(
    "trade_calendar",
    group="core",
    description="交易日历 — baostock 全量覆盖",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist trade calendar.

    Only runs when the existing data is from a previous year (yearly refresh).
    """
    max_date = db.get_max_date("trade_calendar")
    current_year = date.today().year

    # Skip if we already have data for this year
    if max_date is not None and max_date.year >= current_year:
        logger.info("trade_calendar: up-to-date (max=%s), skipping", max_date)
        return 0

    df = _fetch_trade_calendar()
    if df.empty:
        return 0

    return db.upsert_dataframe("trade_calendar", df)
