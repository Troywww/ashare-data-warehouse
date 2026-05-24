"""Fetcher: block_trades — 大宗交易.

Source: 东财 datacenter RPT_DATA_BLOCKTRADE (HTTP)
Fields: stock_code, trade_date, price, volume, amount, premium_ratio, buyer_broker, seller_broker
Schedule: daily incremental, rolling 30-day window

API column mapping (verified 2026-05):
  SECURITY_CODE, TRADE_DATE, DEAL_PRICE, DEAL_VOLUME, DEAL_AMT,
  PREMIUM_RATIO, BUYER_NAME, SELLER_NAME, CLOSE_PRICE, SECURITY_NAME_ABBR
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
    "?reportName=RPT_DATA_BLOCKTRADE"
    "&columns=SECURITY_CODE,TRADE_DATE,DEAL_PRICE,PREMIUM_RATIO,DEAL_VOLUME,DEAL_AMT,BUYER_NAME,SELLER_NAME"
    "&filter=(TRADE_DATE>='{start}')"
    "&pageNumber=1&pageSize=5000"
    "&sortTypes=-1&sortColumns=TRADE_DATE"
)


@retry(max_attempts=3, delay=2.0)
def _fetch_block_trades(start_date: str) -> pd.DataFrame:
    """Fetch block trades from 东财 datacenter."""
    url = _DATACENTER_API.format(start=start_date)
    resp = requests.get(url, timeout=15)
    data = resp.json()

    if data.get("code") != 0 or not data.get("result", {}).get("data"):
        logger.info("block_trades: no data since %s", start_date)
        return pd.DataFrame()

    rows = []
    for item in data["result"]["data"]:
        rows.append({
            "stock_code": item.get("SECURITY_CODE", ""),
            "trade_date": item.get("TRADE_DATE", ""),
            "price": item.get("DEAL_PRICE"),
            "volume": item.get("DEAL_VOLUME"),
            "amount": item.get("DEAL_AMT"),
            "premium_ratio": item.get("PREMIUM_RATIO"),
            "buyer_broker": item.get("BUYER_NAME", "") or "",
            "seller_broker": item.get("SELLER_NAME", "") or "",
        })

    df = pd.DataFrame(rows)
    if not df.empty and "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


@register_fetcher(
    "block_trades",
    group="signals",
    description="大宗交易 — 东财 datacenter RPT_DATA_BLOCKTRADE 近30天",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist block trades (rolling 30-day window)."""
    start = (trade_date - timedelta(days=30)).isoformat()
    df = _fetch_block_trades(start)

    if df.empty:
        return 0

    return db.upsert_dataframe("block_trades", df)
