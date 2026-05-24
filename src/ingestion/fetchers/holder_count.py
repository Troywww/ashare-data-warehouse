"""Fetcher: holder_count — 股东户数.

Source: 东财 datacenter RPT_HOLDERNUMLATEST (HTTP, 已加分页)
Fields: stock_code, end_date, holder_count, change_qoq, avg_shares
Schedule: monthly
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# 注意: 东财 API 列名已变更 (verified 2026-05):
# 可用: SECURITY_CODE, END_DATE, HOLDER_NUM, AVG_MARKET_CAP
# CHANGE_QOQ / AVG_SHARES 等已不可用，对应字段置 NULL
_DATACENTER_API = (
    "https://datacenter-web.eastmoney.com/api/data/v1/get"
    "?reportName=RPT_HOLDERNUMLATEST"
    "&columns=SECURITY_CODE,END_DATE,HOLDER_NUM,AVG_MARKET_CAP"
    "&pageNumber={page}&pageSize=500"
    "&sortTypes=-1&sortColumns=END_DATE"
)


@retry(max_attempts=3, delay=2.0)
def _fetch_holder_count() -> pd.DataFrame:
    """Fetch holder count data from 东财 datacenter (paginated)."""
    all_rows = []
    page = 1

    while True:
        url = _DATACENTER_API.format(page=page)
        resp = requests.get(url, timeout=15)
        data = resp.json()

        if data.get("code") != 0:
            logger.warning("holder_count: API returned code %s", data.get("code"))
            break

        items = data.get("result", {}).get("data", [])
        if not items:
            break

        for item in items:
            all_rows.append({
                "stock_code": item.get("SECURITY_CODE", ""),
                "end_date": item.get("END_DATE", ""),
                "holder_count": item.get("HOLDER_NUM"),
                "change_qoq": None,  # 东财 API 已不再提供
                "avg_shares": None,  # 东财 API 已不再提供
            })

        total_pages = data.get("result", {}).get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    if not all_rows:
        logger.info("holder_count: no data")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if "end_date" in df.columns:
        df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
    logger.info("holder_count: %d rows from %d pages", len(df), page)
    return df


@register_fetcher(
    "holder_count",
    group="lowfreq",
    description="股东户数 — 东财 datacenter RPT_HOLDERNUMLATEST 分页全量",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist holder count data (monthly refresh)."""
    max_end = db.get_max_date("holder_count")
    current_month = date.today().month

    # Only run once per month
    if max_end is not None:
        last_month = max_end.month
        months_diff = (date.today().year - max_end.year) * 12 + (current_month - last_month)
        if months_diff < 1:
            logger.info(
                "holder_count: last refresh was %s (<1 month), skipping", max_end
            )
            return 0

    df = _fetch_holder_count()
    if df.empty:
        return 0
    return db.upsert_dataframe("holder_count", df)
