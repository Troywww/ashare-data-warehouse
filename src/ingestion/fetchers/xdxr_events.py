"""Fetcher: xdxr_events — 除权除息事件.

Source: 东财 datacenter RPT_SHAREBONUS_DET (HTTP)
Schedule: daily, detect events for today only (no historical backfill)
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# 注意: 东财 API 列名已变更，当前可用: SECURITY_CODE, EX_DIVIDEND_DATE, BONUS_RATIO, NOTICE_DATE
# CASH_DIVIDEND / TRANSFER_RATIO / CATEGORY 已不可用，对应字段置 NULL
_DATACENTER_URL = (
    "https://datacenter-web.eastmoney.com/api/data/v1/get"
    "?reportName=RPT_SHAREBONUS_DET"
    "&columns=SECURITY_CODE,EX_DIVIDEND_DATE,BONUS_RATIO,NOTICE_DATE"
    "&filter=(EX_DIVIDEND_DATE='{today}')"
    "&pageNumber=1&pageSize=500&sortTypes=-1&sortColumns=EX_DIVIDEND_DATE"
)


@retry(max_attempts=3, delay=2.0)
def _fetch_xdxr(today_str: str) -> pd.DataFrame:
    """Fetch today's xdxr events from 东财 datacenter."""
    url = _DATACENTER_URL.format(today=today_str)
    resp = requests.get(url, timeout=15)
    data = resp.json()

    if data.get("code") != 0 or not data.get("result", {}).get("data"):
        logger.info("xdxr_events: no events for %s", today_str)
        return pd.DataFrame()

    rows = []
    for item in data["result"]["data"]:
        ex_date_str = item.get("EX_DIVIDEND_DATE", today_str)
        rows.append({
            "stock_code": item.get("SECURITY_CODE", ""),
            "ex_date": datetime.strptime(ex_date_str[:10], "%Y-%m-%d").date(),
            "cash_dividend": None,  # 东财 API 已不再提供此字段
            "bonus_ratio": item.get("BONUS_RATIO"),
            "transfer_ratio": None,  # 东财 API 已不再提供此字段
            "category": None,  # 东财 API 已不再提供此字段
        })

    return pd.DataFrame(rows)


@register_fetcher(
    "xdxr_events",
    depends_on=["stock_universe"],
    group="core",
    description="除权除息事件 — 东财 datacenter RPT_SHAREBONUS_DET（仅当天）",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist today's xdxr events (daily incremental only)."""
    df = _fetch_xdxr(trade_date.isoformat())
    if df.empty:
        return 0
    return db.upsert_dataframe("xdxr_events", df)
