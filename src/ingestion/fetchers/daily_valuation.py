"""Fetcher: daily_valuation — 估值数据（多线程 baostock 回补）.

Source:
  - History:  baostock query_history_k_data_plus (多线程, ~10-15min for 5991 stocks)
  - Daily:    腾讯API qt.gtimg.cn
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import pandas as pd
import requests

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

_TENCENT_API = "https://qt.gtimg.cn/q={batch_str}"
_BATCH_SIZE = 80

_PUSH2_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
    "Origin": "https://quote.eastmoney.com",
}


def _tencent_batch(codes: list[str]) -> pd.DataFrame:
    """Fetch valuation data for a batch of stocks from 腾讯API."""
    batch_str = ",".join(codes)
    url = _TENCENT_API.format(batch_str=batch_str)
    resp = requests.get(url, timeout=15, headers=_PUSH2_HEADERS)
    resp.encoding = "gbk"
    text = resp.text

    rows = []
    for line in text.strip().split(";\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        try:
            parts = line.split("=", 1)[1].strip('"').split("~")
            if len(parts) < 46:
                continue
            key = line.split("=", 1)[0].strip()
            symbol = key[4:] if key.startswith(("v_sh", "v_sz", "v_bj")) else key
            pe_str = parts[39] if len(parts) > 39 else ""
            total_mv_str = parts[44] if len(parts) > 44 else ""
            circ_mv_str = parts[45] if len(parts) > 45 else ""
            pb_str = parts[46] if len(parts) > 46 else ""

            rows.append({
                "symbol": symbol,
                "pe_ttm": float(pe_str) if pe_str and pe_str != "-" else None,
                "pb": float(pb_str) if pb_str and pb_str != "-" else None,
                "total_mv": float(total_mv_str) if total_mv_str and total_mv_str != "-" else None,
                "circ_mv": float(circ_mv_str) if circ_mv_str and circ_mv_str != "-" else None,
            })
        except (IndexError, ValueError):
            continue

    return pd.DataFrame(rows)


def _fetch_tencent_incremental(symbols: list[str]) -> pd.DataFrame:
    """Fetch today's valuation from 腾讯API in batches."""
    all_rows = []
    for i in range(0, len(symbols), _BATCH_SIZE):
        batch = symbols[i:i + _BATCH_SIZE]
        prefixed = []
        for sym in batch:
            if sym.startswith(("6", "68")):
                prefixed.append(f"sh{sym}")
            elif sym.startswith("8"):
                prefixed.append(f"bj{sym}")
            else:
                prefixed.append(f"sz{sym}")
        df = _tencent_batch(prefixed)
        if not df.empty:
            all_rows.append(df)

    if not all_rows:
        return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)


_bs_lock = threading.Lock()


def _baostock_single(symbols: list[str], db_path: str, start: int, end: int) -> int:
    """Fetch valuation history for symbols[start:end] via baostock (sequential, single-threaded).

    Writes to DB every 50K rows to avoid memory buildup and data loss on timeout.
    """
    import baostock as bs

    with _bs_lock:
        lg = bs.login()
        if lg.error_code != "0":
            return 0

    try:
        total = 0
        batch_rows = []
        end_date = date.today().isoformat()

        for idx in range(start, end):
            sym = str(symbols[idx]).zfill(6)
            market = "sh" if sym.startswith(("6", "68")) else "sz"
            code = f"{market}.{sym}"

            with _bs_lock:
                rs = bs.query_history_k_data_plus(
                    code,
                    fields="date,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                    start_date="2015-01-01",
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2",
                )

            if rs.error_code == "0":
                while rs.next():
                    row = rs.get_row_data()
                    d = row[0] if row else ""
                    if not d:
                        continue
                    batch_rows.append({
                        "symbol": sym, "date": datetime.strptime(str(d)[:10], "%Y-%m-%d").date(),
                        "pe_ttm": float(row[1]) if row[1] else None,
                        "pb": float(row[2]) if row[2] else None,
                        "ps_ttm": float(row[3]) if row[3] else None,
                        "pcf_ncf_ttm": float(row[4]) if row[4] else None,
                        "total_mv": None, "circ_mv": None,
                    })

            if len(batch_rows) >= 50000 and db_path:
                df = pd.DataFrame(batch_rows)
                _db = IngestionDB(db_path)
                total += _db.upsert_dataframe("daily_valuation", df)
                _db.close()
                batch_rows = []

        # Final flush
        if batch_rows:
            df = pd.DataFrame(batch_rows)
            _db = IngestionDB(db_path)
            total += _db.upsert_dataframe("daily_valuation", df)
            _db.close()

        return total
    finally:
        bs.logout()


def _fetch_baostock_history(symbols: list[str], db_path: str, thread_pool: int = 1) -> int:
    """Fetch valuation history via baostock (sequential, incremental DB writes).

    baostock uses a global session, so queries run sequentially with a lock.
    Writes to DB every 50K rows — safe to interrupt and resume.
    """
    total = 0
    done = set()
    # Check which stocks already have data
    import os as _os
    if _os.path.exists(db_path):
        try:
            _db = IngestionDB(db_path)
            done = set(_db.conn.execute("SELECT DISTINCT symbol FROM daily_valuation").fetchdf()['symbol'].tolist())
            _db.close()
        except Exception:
            pass

    to_fetch = [s for s in symbols if s not in done]
    if not to_fetch:
        logger.info("valuation backfill: all stocks already have data, skip")
        _db = IngestionDB(db_path)
        total = _db.count("daily_valuation")
        _db.close()
        return total

    logger.info("valuation backfill: %d/%d stocks to fetch", len(to_fetch), len(symbols))

    # Parallel: split into batches, run in thread pool (baostock uses per-thread login)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    batch_size = 100
    batches = []
    for start in range(0, len(to_fetch), batch_size):
        end = min(start + batch_size, len(to_fetch))
        batches.append((to_fetch, db_path, start, end))

    n_workers = min(config.thread_pool, len(batches), 4)
    logger.info("valuation backfill: %d batches, %d workers", len(batches), n_workers)

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        fut_map = {pool.submit(_baostock_single, *b): b for b in batches}
        done_batches = 0
        for fut in as_completed(fut_map):
            done_batches += 1
            try:
                n = fut.result()
                total += n
                logger.info("valuation backfill: batch %d/%d done (%d rows total, %d rows this batch)",
                            done_batches, len(batches), total, n)
            except Exception as e:
                logger.error("valuation backfill: batch failed: %s", e)

    return total


@register_fetcher(
    "daily_valuation",
    depends_on=["stock_universe"],
    group="core",
    description="估值数据 — 腾讯API增量(pe/pb/市值) + baostock历史回补(peTTM/pbMRQ/psTTM)",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily valuation.

    - Backfill: baostock historical data (multi-threaded)
    - Incremental: 腾讯API daily snapshot
    """
    max_date = db.get_max_date("daily_valuation")
    symbols = db.conn.execute(
        "SELECT symbol FROM stock_universe ORDER BY symbol"
    ).fetchdf()["symbol"].tolist()

    if not symbols:
        logger.warning("valuation: stock_universe is empty")
        return 0

    total = 0

    # Backfill path — multi-threaded baostock with incremental DB writes
    need_backfill = getattr(config, "_backfill", False)
    if need_backfill:
        logger.info("valuation: backfill from baostock (%d stocks)", len(symbols))
        total += _fetch_baostock_history(symbols, db.db_path, thread_pool=config.thread_pool)
        logger.info("valuation: backfill done — %d rows", total)

    # Daily incremental from 腾讯API
    logger.info("valuation: fetching incremental from 腾讯API (%d stocks)", len(symbols))
    df_incr = _fetch_tencent_incremental(symbols)
    if not df_incr.empty:
        df_incr["date"] = trade_date
        total += db.upsert_dataframe("daily_valuation", df_incr)
        logger.info("valuation: incremental %d rows from 腾讯API", len(df_incr))

    return total
