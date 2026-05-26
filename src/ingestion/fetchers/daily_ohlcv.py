"""Fetcher: daily_ohlcv — 日K线（主行情表）.

Source:
  - Primary: opentdx stock_kline(adjust=QFQ) — TCP, 前复权, 含北交所
  - Backup:  baostock query_history_k_data_plus(adjustflag=2) — TCP, 仅 sh/sz
Schedule: daily incremental (last 5 days), backfill count=800
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
import baostock as bs
from opentdx.const import MARKET, PERIOD, ADJUST
from opentdx.tdxClient import TdxClient

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

# Date range for backfill
BACKFILL_START = date(2010, 1, 1)


def _market(symbol: str) -> int:
    """Map 6-digit symbol to opentdx market code."""
    if symbol.startswith(("6", "68")):
        return MARKET.SH
    if symbol.startswith("92"):
        return MARKET.BJ
    return MARKET.SZ


# ---------------------------------------------------------------------------
# opentdx TCP 批量拉取
# ---------------------------------------------------------------------------


def _fetch_batch_opentdx(
    symbols: list[str],
    count: int,
    *,
    max_workers: int = 8,
    market_map: dict[str, int] | None = None,
    write_db: IngestionDB | None = None,
) -> pd.DataFrame:
    """Fetch kline data from opentdx in parallel.

    Parameters
    ----------
    symbols : list of 6-digit codes
    count : number of bars per symbol (max 800)
    max_workers : thread pool size
    market_map : optional pre-built {symbol: market_code} dict
    write_db : if provided, results are written per-worker to DB (for large backfills)

    Returns
    -------
    pd.DataFrame of all fetched rows (if write_db is None), else empty DataFrame.
    """
    if not symbols:
        return pd.DataFrame()

    if market_map is None:
        market_map = {s: _market(s) for s in symbols}

    all_rows: list[pd.DataFrame] = []
    _err = [0]

    def _worker(chunk: list[str]) -> pd.DataFrame | None:
        chunk_rows = []
        with TdxClient() as client:
            for sym in chunk:
                try:
                    market = market_map.get(sym, MARKET.SH)
                    bars = client.stock_kline(market, sym, PERIOD.DAILY, 0, min(800, count), adjust=ADJUST.QFQ)
                except Exception:
                    _err[0] += 1
                    continue
                if not bars:
                    continue
                for b in bars:
                    dt = b.get("datetime")
                    if dt is None:
                        continue
                    chunk_rows.append({
                        "symbol": sym,
                        "date": dt.date() if hasattr(dt, "date") else dt,
                        "open": b.get("open"),
                        "high": b.get("high"),
                        "low": b.get("low"),
                        "close": b.get("close"),
                        "volume": int(b.get("vol", 0)) if b.get("vol") else 0,
                        "amount": b.get("amount"),
                        "pct_chg": None,
                        "turnover_rate": b.get("turnover"),
                    })
        if chunk_rows and write_db:
            df_chunk = pd.DataFrame(chunk_rows)
            write_db.upsert_dataframe("daily_ohlcv", df_chunk)
            return None
        elif chunk_rows:
            return pd.DataFrame(chunk_rows)
        return None

    # Split symbols into chunks for workers
    chunk_size = max(1, len(symbols) // max_workers) if max_workers > 1 else len(symbols)
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_worker, c) for c in chunks]
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result is not None:
                    all_rows.append(result)
            except Exception:
                _err[0] += 1

    if _err[0]:
        logger.warning("ohlcv: %d/%d stocks failed via opentdx", _err[0], len(symbols))

    if all_rows:
        return pd.concat(all_rows, ignore_index=True)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# opentdx 多轮智能回补
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


@register_fetcher(
    "daily_ohlcv",
    depends_on=["stock_universe", "xdxr_events"],
    group="core",
    description="日K线(前复权) — opentdx QFQ 主源 / baostock 备源",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily OHLCV data.

    - Backfill: opentdx 多轮智能回补，只补稀疏股票 (< 500行)，并发 3 worker
    - Incremental: fetch last 2 days from opentdx (8 threads)
    - On xdxr event: re-fetch affected stocks from opentdx (full history)
    """
    # Get stock list from universe (with market mapping)
    df_syms = db.conn.execute(
        "SELECT symbol, market FROM stock_universe ORDER BY symbol"
    ).fetchdf()
    symbols = df_syms["symbol"].tolist()
    market_map = {
        row["symbol"]: MARKET.SH if row["market"] == "sh" else MARKET.BJ if row["market"] == "bj" else MARKET.SZ
        for _, row in df_syms.iterrows()
    }

    if getattr(config, "_backfill", False):
        # opentdx 多轮智能回补：只补稀疏股票 (< 500行)，并发 3 worker
        total = 0
        max_rounds = 5
        for round_num in range(1, max_rounds + 1):
            sparse = db.conn.execute(
                "SELECT symbol FROM daily_ohlcv GROUP BY symbol HAVING COUNT(*) < 500"
            ).fetchdf()["symbol"].tolist()
            if not sparse:
                logger.info("ohlcv: backfill complete — all stocks have 500+ days")
                break
            logger.info("ohlcv: backfill round %d/%d — %d/%d stocks need refetch",
                        round_num, max_rounds, len(sparse), len(symbols))
            backfill_workers = min(config.thread_pool, 3)
            _fetch_batch_opentdx(sparse, 5000, max_workers=backfill_workers,
                                 market_map=market_map, write_db=db)
            if round_num < max_rounds:
                remaining = db.conn.execute(
                    "SELECT COUNT(*) FROM daily_ohlcv GROUP BY symbol HAVING COUNT(*) < 500"
                ).fetchdf().shape[0]
                if remaining > 0:
                    logger.info("ohlcv: backfill round %d done, %d stocks still sparse — cooling 30s",
                                round_num, remaining)
                    time.sleep(30)
        rows_written = db.count("daily_ohlcv")
        return rows_written
    else:
        count = 2  # incremental — last 2 days

    if not symbols:
        logger.warning("ohlcv: stock_universe is empty, cannot fetch")
        return 0

    # Fetch from opentdx (primary - incremental mode only, backfill handled above)
    logger.info(
        "ohlcv: fetching %d stocks from opentdx (count=%d, workers=%d, mode=incremental)",
        len(symbols), count, config.thread_pool,
    )

    if True:
        df = _fetch_batch_opentdx(symbols, count, max_workers=config.thread_pool, market_map=market_map)
        rows_written = 0
        if not df.empty:
            rows_written = db.upsert_dataframe("daily_ohlcv", df)

    # Check xdxr events — re-fetch those stocks from opentdx (updated 前复权)
    xdxr_stocks = db.conn.execute(
        "SELECT DISTINCT stock_code FROM xdxr_events WHERE ex_date = ?",
        [trade_date],
    ).fetchdf()["stock_code"].tolist()
    if xdxr_stocks:
        logger.info("ohlcv: re-fetching %d stocks (xdxr events)", len(xdxr_stocks))
        df_xdxr = _fetch_batch_opentdx(
            xdxr_stocks, 5000, max_workers=min(config.thread_pool, 3), market_map=market_map,
        )
        if not df_xdxr.empty:
            rows_written += db.upsert_dataframe("daily_ohlcv", df_xdxr)

    return rows_written
