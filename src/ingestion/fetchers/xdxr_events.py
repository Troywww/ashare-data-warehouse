"""Fetcher: xdxr_events — 除权除息事件.

Source: 东方财富 datacenter-web API RPT_SHAREBONUS_DET (HTTP, ~1s)
Replaces: easy_tdx TdxClient.get_xdxr_info (TCP, 283s for 5525 stocks)
Schedule: daily, filter by EX_DIVIDEND_DATE

Fields: stock_code, ex_date, cash_dividend, bonus_ratio, transfer_ratio, category
Note: Eastmoney values are per-10-shares; we divide by 10 for per-share.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher

logger = logging.getLogger(__name__)

_XDXR_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_XDXR_COLUMNS = (
    "SECURITY_CODE,SECURITY_NAME_ABBR,EX_DIVIDEND_DATE,"
    "BONUS_RATIO,IT_RATIO,PRETAX_BONUS_RMB,IMPL_PLAN_PROFILE"
)
_PAGE_SIZE = 500  # More than enough for any single day


def _fetch_xdxr_events(trade_date: date) -> pd.DataFrame:
    """Query eastmoney for all stocks with ex-date = trade_date.

    Single HTTP request, returns per-10-share values (divided by 10 below).
    """
    params = {
        "reportName": "RPT_SHAREBONUS_DET",
        "columns": _XDXR_COLUMNS,
        "pageNumber": "1",
        "pageSize": str(_PAGE_SIZE),
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(EX_DIVIDEND_DATE='{trade_date.isoformat()}')",
    }
    resp = requests.get(_XDXR_API, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("result") or {}
    items = result.get("data") or []
    if not items:
        logger.info("xdxr_events: no events for %s", trade_date)
        return pd.DataFrame()

    rows = []
    for item in items:
        code = item.get("SECURITY_CODE", "")
        bonus = item.get("BONUS_RATIO")  # per 10 shares, None → 0
        transfer = item.get("IT_RATIO")  # per 10 shares, None → 0
        cash = item.get("PRETAX_BONUS_RMB")  # per 10 shares, None → 0

        # Convert per-10-shares to per-share (matching easy_tdx convention)
        bonus_ratio = (bonus or 0) / 10.0
        transfer_ratio = (transfer or 0) / 10.0
        cash_dividend = (cash or 0) / 10.0

        # Derive category from field values
        if bonus_ratio > 0 or transfer_ratio > 0:
            category = "除权除息(含送转)"
        else:
            category = "除权除息"

        rows.append({
            "stock_code": code,
            "ex_date": trade_date,
            "cash_dividend": cash_dividend,
            "bonus_ratio": bonus_ratio,
            "transfer_ratio": transfer_ratio,
            "category": category,
        })

    logger.info("xdxr_events: %d events for %s", len(rows), trade_date)
    return pd.DataFrame(rows)


@register_fetcher(
    "xdxr_events",
    depends_on=["stock_universe"],
    group="core",
    description="除权除息事件 — 东方财富 datacenter API（单次HTTP，~1s）",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist today's xdxr events from eastmoney datacenter API.

    Replaces easy_tdx per-stock query (5525 calls, ~283s) with single HTTP
    request (~1s).
    """
    df = _fetch_xdxr_events(trade_date)
    if df.empty:
        return 0

    return db.upsert_dataframe("xdxr_events", df)
