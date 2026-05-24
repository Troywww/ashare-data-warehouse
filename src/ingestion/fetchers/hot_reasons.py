"""Fetcher: hot_reasons — 同花顺题材归因.

Source: 同花顺 zx.10jqka.com.cn (HTTP, 零鉴权)
Fields: date, symbol, stock_name, reason_tags, close, change_amt, change_pct, turnover_rate, amount, volume
Schedule: daily incremental
"""
from __future__ import annotations

import json
import logging
from datetime import date

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_THS_API = "http://zx.10jqka.com.cn/event/api/getharden/date/{date_str}/"


@retry(max_attempts=3, delay=2.0)
def _fetch_hot_reasons(trade_date_str: str) -> pd.DataFrame:
    """Fetch hot reasons from 同花顺."""
    url = _THS_API.format(date_str=trade_date_str)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    resp = requests.get(url, timeout=15, headers=headers)
    data = resp.json()

    if not data.get("data"):
        logger.info("hot_reasons: no data for %s", trade_date_str)
        return pd.DataFrame(columns=[
            "symbol", "stock_name", "reason_tags", "close",
            "change_amt", "change_pct", "turnover_rate", "amount", "volume",
        ])

    rows = []
    for item in data["data"]:
        rows.append({
            "symbol": str(item.get("code", "")).zfill(6),
            "stock_name": item.get("name", ""),
            "reason_tags": item.get("reason", "") or item.get("reason_tags", ""),
            "close": item.get("close"),
            "change_amt": item.get("change_amt"),
            "change_pct": item.get("change_pct") or item.get("zhangfu"),
            "turnover_rate": item.get("turnover_rate"),
            "amount": item.get("amount"),
            "volume": item.get("volume"),
        })

    return pd.DataFrame(rows)


@register_fetcher(
    "hot_reasons",
    group="signals",
    description="同花顺题材归因 — zx.10jqka.com.cn 人工运营标签",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist hot reasons data."""
    df = _fetch_hot_reasons(trade_date.isoformat())
    if df.empty:
        return 0

    df["date"] = trade_date
    return db.upsert_dataframe("hot_reasons", df)
