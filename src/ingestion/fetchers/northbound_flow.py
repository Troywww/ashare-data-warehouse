"""Fetcher: northbound_flow — 北向资金.

Source: akshare stock_hsgt_fund_flow_summary_em() (HTTP, 东财)
Fields: trade_date, market(sh/sz), net_buy
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

# akshare 返回的字段中，北向对应的值
_NORTHBOUND_DIRECTION = "北向"
_MARKET_MAP = {"沪股通": "sh", "深股通": "sz"}


@retry(max_attempts=3, delay=2.0)
def _fetch_northbound() -> pd.DataFrame:
    """Fetch northbound flow summary from akshare."""
    import akshare as ak
    df = ak.stock_hsgt_fund_flow_summary_em()
    if df is None or df.empty:
        return pd.DataFrame()

    # 过滤出北向（沪股通+深股通）
    north = df[df["资金方向"] == _NORTHBOUND_DIRECTION].copy()
    if north.empty:
        logger.warning("northbound_flow: no northbound data in akshare response")
        return pd.DataFrame()

    rows = []
    for _, row in north.iterrows():
        market_label = row.get("板块", "")
        mkt = _MARKET_MAP.get(market_label)
        if mkt is None:
            continue
        rows.append({
            "trade_date": row.get("交易日"),
            "market": mkt,
            "net_buy": row.get("成交净买额", 0) or 0,
        })

    if not rows:
        return pd.DataFrame()

    df_result = pd.DataFrame(rows)
    if "trade_date" in df_result.columns:
        df_result["trade_date"] = pd.to_datetime(df_result["trade_date"]).dt.date
    return df_result


@register_fetcher(
    "northbound_flow",
    group="core",
    description="北向资金 — akshare stock_hsgt_fund_flow_summary_em 每日汇总",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist northbound flow data."""
    df = _fetch_northbound()
    if df.empty:
        logger.warning("northbound_flow: no data")
        return 0
    return db.upsert_dataframe("northbound_flow", df)
