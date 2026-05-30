"""Fetcher: capital_flow — 个股资金流向（主力净流入/超大单/大单/中单/小单）.

Source: easy_tdx TdxClient.get_fund_flow (TCP, 通达信标准协议)
Optimization: ONE persistent TdxClient per worker thread (vs old per-30-stock reconnect).
Schedule: daily, incremental (today only)
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
from easy_tdx import TdxClient
from easy_tdx.models.enums import Market

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher

logger = logging.getLogger(__name__)

# Each worker thread gets a chunk of stocks to query with its own TdxClient.
# BATCH_SIZE is informational for progress reporting only.
_CHUNK_SIZE = 500


def _market(symbol: str) -> Market:
    """Map 6-digit symbol to easy_tdx Market enum."""
    if symbol.startswith(("6", "68")):
        return Market.SH
    if symbol.startswith("92"):
        return Market.BJ
    return Market.SZ


def _fetch_capital_flow(symbols: list[str], max_workers: int = 4) -> pd.DataFrame:
    """Fetch capital flow for all stocks with persistent TdxClient per worker.

    Each worker gets its own TdxClient connection and processes a chunk of
    stocks sequentially. This avoids the old pattern of creating a new client
    per 30-stock batch (185 connection handshakes → ~4 handshakes).
    """
    if not symbols:
        return pd.DataFrame()

    n = len(symbols)
    chunk_size = max(_CHUNK_SIZE, (n + max_workers - 1) // max_workers)
    chunks = [symbols[i:i + chunk_size] for i in range(0, n, chunk_size)]
    total_chunks = len(chunks)

    all_rows: list[dict] = []
    _lock = threading.Lock()
    _done_count = [0]
    _err_count = [0]

    def _fetch_chunk(chunk: list[str], worker_id: int) -> list[dict]:
        """Fetch one chunk with a single persistent TdxClient connection."""
        rows: list[dict] = []
        try:
            with TdxClient.from_best_host() as client:
                for sym in chunk:
                    try:
                        mkt = _market(sym)
                        flow = client.get_fund_flow(mkt, sym)
                        if flow.empty:
                            continue
                        row = flow.iloc[0]
                        rows.append({
                            "symbol": sym,
                            "net_main": (
                                (row["super_in"] + row["large_in"])
                                - (row["super_out"] + row["large_out"])
                            ),
                            "net_super": row["super_in"] - row["super_out"],
                            "net_large": row["large_in"] - row["large_out"],
                            "net_medium": row["medium_in"] - row["medium_out"],
                            "net_small": row["small_in"] - row["small_out"],
                        })
                    except Exception:
                        _err_count[0] += 1
            return rows
        except Exception:
            _err_count[0] += 1
            return rows

    if max_workers <= 1:
        for ci, chunk in enumerate(chunks):
            rows = _fetch_chunk(chunk, 0)
            all_rows.extend(rows)
            logger.info("capital_flow: chunk %d/%d done (%d rows, %d errors)",
                        ci + 1, total_chunks, len(all_rows), _err_count[0])
    else:
        def _worker(idx: int) -> None:
            rows = _fetch_chunk(chunks[idx], idx)
            with _lock:
                all_rows.extend(rows)
                _done_count[0] += 1
                logger.info("capital_flow: chunk %d/%d done (%d rows, %d errors)",
                            _done_count[0], total_chunks, len(all_rows), _err_count[0])

        with ThreadPoolExecutor(max_workers=min(max_workers, total_chunks)) as pool:
            futures = [pool.submit(_worker, i) for i in range(total_chunks)]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    _err_count[0] += 1

    if _err_count[0]:
        logger.warning("capital_flow: %d per-stock errors", _err_count[0])

    if not all_rows:
        return pd.DataFrame()
    return pd.DataFrame(all_rows)


@register_fetcher(
    "capital_flow",
    depends_on=["stock_universe"],
    group="core",
    description="个股资金流向 — easy_tdx get_fund_flow（持久连接优化）",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily capital flow data."""
    symbols = db.conn.execute(
        "SELECT symbol FROM stock_universe ORDER BY symbol"
    ).fetchdf()["symbol"].tolist()
    if not symbols:
        return 0

    df = _fetch_capital_flow(symbols, max_workers=config.thread_pool)
    if df.empty:
        return 0

    df["date"] = trade_date
    return db.upsert_dataframe("capital_flow", df)
