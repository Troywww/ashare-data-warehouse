"""Fetcher: stock_universe — 全品种索引.

Source: opentdx stock_list (TCP, 通达信)
Filter: 代码前缀白名单 (60/68/00/30/92)
Schedule: daily, full replace
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from opentdx.const import MARKET
from opentdx.tdxClient import TdxClient

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# 代码前缀白名单（每个市场只取实际 A 股）
# SH: 60xxxx 沪主板, 68xxxx 科创板
# SZ: 00xxxx 深主板, 30xxxx 创业板
# BJ: 92xxxx 北交所 A 股（opentdx 独立市场）
_SH_PREFIXES = ("60", "68")
_SZ_PREFIXES = ("00", "30")
_BJ_PREFIXES = ("92",)
_INDEX_PREFIXES = ("39", "000", "880", "881")  # 指数代码

# 需要排除的元数据代码（非交易品种，如市场统计条目）
_META_CODES = {
    "395001", "395002", "395003", "395004", "395005", "395006",
    "395011", "395012", "395013", "395014", "395015", "395032",
    "395033", "395034", "395035", "395036", "395037", "395041",
    "395051", "395052", "395053", "395054", "395061", "395062",
    "395071", "395072", "395073", "395074", "395081", "395082",
    "395083", "395091", "395092", "395093", "395101", "395102",
    "899050",  # 北证50（指数）
}

# 需要排除的债券/转债前缀（非股票，但出现在 stock_list 中）
_BOND_PREFIXES = ("81", "82", "83", "84", "85", "86", "87", "88", "89")


def _is_valid_stock(code: str) -> bool:
    """Check if code is a valid A-share stock or index."""
    if len(code) != 6:
        return False
    if code in _META_CODES:
        return False
    if code.startswith(_BOND_PREFIXES):
        return False
    return any(code.startswith(p) for p in _SH_PREFIXES + _SZ_PREFIXES + _INDEX_PREFIXES + _BJ_PREFIXES)


@retry(max_attempts=2, delay=2.0)
def _fetch_stock_universe() -> pd.DataFrame:
    """Fetch complete stock universe from opentdx (SH + SZ + BJ)."""
    with TdxClient() as client:
        all_stocks = []

        # SH market: 60xxxx 沪主板, 68xxxx 科创板
        sh_list = client.stock_list(MARKET.SH)
        for s in sh_list:
            code = s.get("code", "")
            if _is_valid_stock(code):
                all_stocks.append({
                    "symbol": code,
                    "name": s.get("name", ""),
                    "market": "sh",
                })

        # SZ market: 00xxxx 深主板, 30xxxx 创业板
        sz_list = client.stock_list(MARKET.SZ)
        for s in sz_list:
            code = s.get("code", "")
            if _is_valid_stock(code):
                all_stocks.append({
                    "symbol": code,
                    "name": s.get("name", ""),
                    "market": "sz",
                })

        # BJ market: 92xxxx 北交所 A 股（opentdx 独立市场）
        bj_list = client.stock_list(MARKET.BJ)
        for s in bj_list:
            code = s.get("code", "")
            if _is_valid_stock(code):
                all_stocks.append({
                    "symbol": code,
                    "name": s.get("name", ""),
                    "market": "bj",
                })

        df = pd.DataFrame(all_stocks)
        if df.empty:
            logger.warning("stock_universe: empty result from opentdx")
            return df

        df = df.drop_duplicates(subset=["symbol"], keep="last")
        logger.info(
            "stock_universe: %d stocks (sh=%d, sz=%d, bj=%d)",
            len(df),
            (df["market"] == "sh").sum(),
            (df["market"] == "sz").sum(),
            (df["market"] == "bj").sum(),
        )
        return df


@register_fetcher(
    "stock_universe",
    group="core",
    description="全品种索引 — opentdx stock_list 全量覆盖",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist stock universe (daily full replace)."""
    df = _fetch_stock_universe()
    if df.empty:
        return 0
    return db.upsert_dataframe("stock_universe", df)
