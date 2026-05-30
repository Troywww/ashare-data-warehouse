"""Fetcher: stock_universe — 全品种索引.

Source: akshare stock_info_a_code_name() (HTTP, ~13s, SH+SZ+BJ 全量)
        + baostock query_stock_basic() (TCP, ~12s, SH+SZ 上市日期)
        + akshare stock_info_bj_name_code() (HTTP, ~7s, BJ 上市日期)
Filter: A 股代码前缀白名单 (60/68/00/30/92/83/87/4)
Schedule: daily, full replace
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import baostock as bs
import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# SH: 60xxxx 沪主板, 68xxxx 科创板
# SZ: 00xxxx 深主板, 30xxxx 创业板
# BJ: 92xxxx 北交所
_SH_PREFIXES = ("60", "68")
_SZ_PREFIXES = ("00", "30")
_BJ_PREFIXES = ("92", "83", "87", "4")
_INDEX_PREFIXES = ("39", "000", "880", "881")


def _is_valid_stock(code: str) -> bool:
    """Check if code is a valid A-share stock."""
    if len(code) != 6:
        return False
    return any(code.startswith(p) for p in _SH_PREFIXES + _SZ_PREFIXES + _BJ_PREFIXES)


def _fetch_listing_dates_from_baostock() -> dict[str, date] | None:
    """Query baostock for stock listing dates."""
    try:
        bs.login()
        rs = bs.query_stock_basic()
        if rs.error_code != "0":
            logger.warning("baostock query_stock_basic failed: %s", rs.error_msg)
            return None

        dates: dict[str, date] = {}
        while rs.next():
            row = rs.get_row_data()
            if row[4] != "1":
                continue
            code = row[0]
            ipo_date_str = row[2]
            if not ipo_date_str:
                continue
            symbol = code.split(".")[-1] if "." in code else code
            if len(symbol) != 6:
                continue
            try:
                dates[symbol] = datetime.strptime(ipo_date_str, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue

        logger.info("stock_universe: got %d listing dates from baostock", len(dates))
        return dates
    except Exception as e:
        logger.warning("baostock listing dates failed: %s", e)
        return None
    finally:
        try:
            bs.logout()
        except Exception:
            pass


def _fetch_bj_listing_dates() -> dict[str, date] | None:
    """Query akshare for BJ stock listing dates (baostock doesn't cover BJ)."""
    import akshare as ak

    try:
        df = ak.stock_info_bj_name_code()
        # columns: [证券代码, 证券简称, 总股本, 流通股本, 上市日期, 所属行业, 省份, 更新日期]
        code_col = df.columns[0]
        date_col = df.columns[4]

        dates: dict[str, date] = {}
        for _, row in df.iterrows():
            code = str(row[code_col])
            ipo_str = row[date_col]
            if not ipo_str or pd.isna(ipo_str):
                continue
            try:
                dates[code] = datetime.strptime(str(ipo_str)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue

        logger.info("stock_universe: got %d BJ listing dates from akshare", len(dates))
        return dates
    except Exception as e:
        logger.warning("akshare BJ listing dates failed: %s", e)
        return None


@retry(max_attempts=2, delay=2.0)
def _fetch_stock_universe() -> pd.DataFrame:
    """Fetch complete stock universe from akshare (HTTP, ~13s, SH+SZ+BJ)."""
    import akshare as ak

    all_stocks: list[dict] = []

    df_raw = ak.stock_info_a_code_name()
    # columns: ["code", "name"]

    for _, row in df_raw.iterrows():
        code = str(row["code"])
        if not _is_valid_stock(code):
            continue

        # 根据代码前缀推断市场
        if code.startswith(_SH_PREFIXES):
            market = "sh"
        elif code.startswith(_SZ_PREFIXES):
            market = "sz"
        elif code.startswith(_BJ_PREFIXES):
            market = "bj"
        else:
            continue

        all_stocks.append({
            "symbol": code,
            "name": str(row["name"]),
            "market": market,
        })

    df = pd.DataFrame(all_stocks)
    if df.empty:
        logger.warning("stock_universe: empty result from akshare")
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
    description="全品种索引 — akshare(全量) + baostock(SH/SZ上市日期) + akshare(BJ上市日期)",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist stock universe.

    - backfill/init: akshare(~13s) + baostock(~12s) + akshare BJ(~7s) = ~32s total
    - daily update:  skip if data exists (stock list rarely changes daily)
    """
    is_backfill = getattr(config, "_backfill", False)

    # Daily mode: skip if universe already populated (avoids 15-min TDX pagination)
    if not is_backfill:
        existing = db.count("stock_universe")
        if existing > 0:
            logger.info("stock_universe: %d stocks exist, skipping (use backfill to refresh)", existing)
            return 0

    df = _fetch_stock_universe()
    if df.empty:
        return 0

    # Enrich with listing dates: baostock (SH+SZ) + akshare (BJ)
    listing_dates: dict[str, date] = {}

    bs_dates = _fetch_listing_dates_from_baostock()
    if bs_dates:
        listing_dates.update(bs_dates)

    bj_dates = _fetch_bj_listing_dates()
    if bj_dates:
        listing_dates.update(bj_dates)  # BJ overwrites any dupes, but there shouldn't be any

    if listing_dates:
        df["list_date"] = df["symbol"].map(listing_dates)
        found = df["list_date"].notna().sum()
        logger.info("stock_universe: list_date filled for %d/%d stocks", found, len(df))
    else:
        df["list_date"] = None
        logger.warning("stock_universe: list_date not available, backfill will use fallback")

    return db.upsert_dataframe("stock_universe", df)
