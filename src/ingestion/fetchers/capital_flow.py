"""Fetcher: capital_flow — 个股资金流向.

Source: opentdx stock_capital_flow (TCP, 通达信)
Fields: net_main(当日主力), net_super_5d/large_5d/medium_5d/small_5d(5日累计)
"""
from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
from opentdx.const import MARKET
from opentdx.tdxClient import TdxClient

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)


def _market(symbol: str) -> int:
    if symbol.startswith(("6", "68")):
        return MARKET.SH
    if symbol.startswith("92"):
        return MARKET.BJ
    return MARKET.SZ


def _fetch_capital_flow(symbols: list[str]) -> pd.DataFrame:
    """Fetch capital flow for all stocks, reusing TdxClient."""
    rows = []
    client = None

    for i, sym in enumerate(symbols):
        for attempt in range(2):
            try:
                if client is None:
                    client = TdxClient()
                    client.__enter__()
                result = client.stock_capital_flow(_market(sym), sym)
                data = result.get("data", {})
                if data:
                    rows.append({
                        "symbol": sym,
                        "net_main": float(data.get("今日主力净流入", 0) or 0),
                        "net_super_5d": float(data.get("5日超大单净额", 0) or 0),
                        "net_large_5d": float(data.get("5日大单净额", 0) or 0),
                        "net_medium_5d": float(data.get("5日中单净额", 0) or 0),
                        "net_small_5d": float(data.get("5日小单净额", 0) or 0),
                    })
                break
            except Exception as e:
                if client is not None:
                    try:
                        client.__exit__(None, None, None)
                    except Exception:
                        pass
                    client = None
                if attempt < 1:
                    continue
                logger.debug("capital_flow %s failed: %s", sym, e)

        if (i + 1) % 1000 == 0:
            logger.info("capital_flow: %d/%d done", i + 1, len(symbols))

    if client is not None:
        try:
            client.__exit__(None, None, None)
        except Exception:
            pass

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@register_fetcher(
    "capital_flow",
    depends_on=["stock_universe"],
    group="core",
    description="个股资金流向 — opentdx stock_capital_flow 复用连接",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily capital flow data."""
    symbols = db.conn.execute(
        "SELECT symbol FROM stock_universe ORDER BY symbol"
    ).fetchdf()["symbol"].tolist()
    if not symbols:
        return 0

    df = _fetch_capital_flow(symbols)
    if df.empty:
        return 0

    df["date"] = trade_date
    return db.upsert_dataframe("capital_flow", df)
