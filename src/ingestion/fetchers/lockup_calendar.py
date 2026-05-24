"""Fetcher: lockup_calendar — 限售解禁.

Source: 东财 datacenter RPT_LIFT_STAGE (HTTP)
Fields: stock_code, unlock_date, unlock_vol, unlock_ratio, status
Schedule: daily, 30 days history + 90 days future

API column mapping (verified 2026-05):
  SECURITY_CODE, FREE_DATE, FREE_RATIO
  UNLOCK_VOL / UNLOCK_DATE / STATUS 等不可用，对应字段置 NULL
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_DATACENTER_API = (
    "https://datacenter-web.eastmoney.com/api/data/v1/get"
    "?reportName=RPT_LIFT_STAGE"
    "&columns=SECURITY_CODE,FREE_DATE,FREE_RATIO"
    "&filter=(FREE_DATE>='{start}')(FREE_DATE<='{end}')"
    "&pageNumber=1&pageSize=5000"
    "&sortTypes=-1&sortColumns=FREE_DATE"
)


@retry(max_attempts=3, delay=2.0)
def _fetch_lockup(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch lockup calendar from 东财 datacenter."""
    url = _DATACENTER_API.format(start=start_date, end=end_date)
    resp = requests.get(url, timeout=15)
    data = resp.json()

    if data.get("code") != 0 or not data.get("result", {}).get("data"):
        logger.info("lockup_calendar: no data for %s ~ %s", start_date, end_date)
        return pd.DataFrame()

    rows = []
    for item in data["result"]["data"]:
        rows.append({
            "stock_code": item.get("SECURITY_CODE", ""),
            "unlock_date": item.get("FREE_DATE", ""),
            "unlock_vol": None,   # 东财 API 已不再提供此字段
            "unlock_ratio": item.get("FREE_RATIO"),
            "status": None,       # 东财 API 已不再提供此字段
        })

    df = pd.DataFrame(rows)
    if "unlock_date" in df.columns:
        df["unlock_date"] = pd.to_datetime(df["unlock_date"]).dt.date
    return df


@register_fetcher(
    "lockup_calendar",
    group="signals",
    description="限售解禁 — 东财 datacenter RPT_LIFT_STAGE 30天历史+90天未来",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist lockup calendar (daily, rolling window)."""
    start = (trade_date - timedelta(days=30)).isoformat()
    end = (trade_date + timedelta(days=90)).isoformat()

    df = _fetch_lockup(start, end)
    if df.empty:
        return 0
    return db.upsert_dataframe("lockup_calendar", df)
