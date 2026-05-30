"""Fetcher: daily_ohlcv — 日K线（主行情表）.

Source:
  - Primary: easy_tdx MacClient.get_stock_kline(adjust=QFQ) — TCP, 前复权, MAC协议
  - Backup:  baostock query_history_k_data_plus(adjustflag=2) — TCP, 仅 sh/sz
Schedule: daily incremental (last 2 days), backfill count=800
"""
from __future__ import annotations

import bisect
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import pandas as pd
from easy_tdx.mac.client import MacClient
from easy_tdx.mac.enums import Adjust, Period

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher

logger = logging.getLogger(__name__)

# Date range for backfill
BACKFILL_START = date(2016, 1, 1)

# Known good MAC host (same as easy_tdx mac_hosts)
MAC_HOST = "121.36.248.138"


def _market(symbol: str) -> int:
    """Map 6-digit symbol to market code (0=SZ, 1=SH, 2=BJ)."""
    if symbol.startswith(("6", "68")):
        return 1  # SH
    if symbol.startswith("92"):
        return 2  # BJ
    return 0      # SZ


# ---------------------------------------------------------------------------
# easy_tdx MacClient 批量拉取 (QFQ 前复权)
# ---------------------------------------------------------------------------

BATCH_SIZE = 30


def _fetch_batch_easy_tdx(
    symbols: list[str],
    count: int,
    *,
    max_workers: int = 4,
    market_map: dict[str, int] | None = None,
    write_db: IngestionDB | None = None,
    start: int = 0,
) -> pd.DataFrame:
    """Fetch QFQ kline from easy_tdx MacClient in parallel.

    Parameters
    ----------
    symbols : list of 6-digit codes
    count : number of bars per symbol (max ~800 per call, auto-paginated)
    max_workers : thread pool size
    market_map : optional pre-built {symbol: market_code} dict
    write_db : if provided, results are written per-batch to DB (for large backfills)
    start : starting position (0=most recent). Used for pagination.
    """
    if not symbols:
        return pd.DataFrame()

    if market_map is None:
        market_map = {s: _market(s) for s in symbols}

    all_rows: list[pd.DataFrame] = []
    _err = [0]       # per-stock errors
    _conn_err = [0]  # batch-level connection failures
    _empty = [0]     # count of symbols returning empty bars

    def _do_chunk(
        chunk: list[str],
        db: IngestionDB | None = None,
    ) -> pd.DataFrame | None:
        """Process a chunk of symbols — one fresh MacClient per chunk."""
        chunk_rows: list[dict] = []
        try:
            with MacClient(MAC_HOST, timeout=15) as client:
                for sym in chunk:
                    try:
                        mkt = market_map.get(sym, 1)
                        bars = client.get_stock_kline(
                            mkt, sym, period=Period.DAILY,
                            start=start, count=min(800, count),
                            adjust=Adjust.QFQ,
                        )
                    except Exception:
                        _err[0] += 1
                        continue
                    if bars.empty:
                        _empty[0] += 1
                        continue
                    for _, b in bars.iterrows():
                        dt = b.get("datetime")
                        if dt is None:
                            continue
                        d = dt.date() if hasattr(dt, "date") else pd.Timestamp(dt).date()
                        chunk_rows.append({
                            "symbol": sym,
                            "date": d,
                            "open": b.get("open"),
                            "high": b.get("high"),
                            "low": b.get("low"),
                            "close": b.get("close"),
                            "volume": int(b.get("vol", 0)) if pd.notna(b.get("vol")) else 0,
                            "amount": b.get("amount"),
                            "pct_chg": None,
                            "turnover_rate": None,
                        })
        except Exception:
            _conn_err[0] += 1

        if chunk_rows and db:
            df_chunk = pd.DataFrame(chunk_rows)
            db.upsert_dataframe("daily_ohlcv", df_chunk)
            return None
        elif chunk_rows:
            return pd.DataFrame(chunk_rows)
        return None

    sub_batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    total_batches = len(sub_batches)

    if max_workers <= 1:
        for bi, c in enumerate(sub_batches):
            try:
                result = _do_chunk(c, write_db)
                if result is not None:
                    all_rows.append(result)
                if total_batches > 1 and (bi + 1) % 10 == 0:
                    logger.info("ohlcv: batch %d/%d complete (%d errors, %d empty)",
                                bi + 1, total_batches, _err[0], _empty[0])
            except Exception:
                _err[0] += 1
    else:
        _db_path = write_db.db_path if write_db else None
        _batch_idx = [0]
        _lock = threading.Lock()

        def _worker() -> None:
            db = IngestionDB(_db_path, ensure_schema=False) if _db_path else None
            while True:
                with _lock:
                    idx = _batch_idx[0]
                    if idx >= total_batches:
                        break
                    _batch_idx[0] = idx + 1
                try:
                    result = _do_chunk(sub_batches[idx], db)
                    if result is not None and not result.empty:
                        with _lock:
                            all_rows.append(result)
                    if (idx + 1) % 50 == 0:
                        logger.info("ohlcv: batch %d/%d complete (%d errors, %d empty)",
                                    idx + 1, total_batches, _err[0], _empty[0])
                except Exception:
                    _err[0] += 1

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_worker) for _ in range(max_workers)]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    _err[0] += 1

    if _err[0] or _conn_err[0]:
        logger.warning("ohlcv: %d per-stock errors, %d batch connection failures (up to %d stocks/batch)",
                       _err[0], _conn_err[0], BATCH_SIZE)
    if _empty[0]:
        logger.debug("ohlcv: %d/%d stocks returned empty bars", _empty[0], len(symbols))

    if all_rows:
        return pd.concat(all_rows, ignore_index=True)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


@register_fetcher(
    "daily_ohlcv",
    depends_on=["stock_universe", "xdxr_events"],
    group="core",
    description="日K线(前复权) — easy_tdx MacClient QFQ",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily OHLCV data.

    - Backfill: uses listing_date from stock_universe + trade_calendar to compute
      expected row counts; only refetches stocks where actual < 85% of expected.
      Multi-round (max 3), drops stocks that get no new data per round.
    - Incremental: fetch last 2 days from MacClient (QFQ)
    - On xdxr event: re-fetch affected stocks (full history, only for stocks
      with >= 100 existing bars)
    """
    # Get stock list from universe
    df_syms = db.conn.execute(
        "SELECT symbol, market FROM stock_universe ORDER BY symbol"
    ).fetchdf()
    symbols = df_syms["symbol"].tolist()
    market_map = {
        row["symbol"]: 1 if row["market"] == "sh" else 2 if row["market"] == "bj" else 0
        for _, row in df_syms.iterrows()
    }

    if getattr(config, "_backfill", False):
        # Load all trading days for expected-row calculation
        trading_days_raw = db.conn.execute(
            "SELECT date FROM trade_calendar WHERE is_trading ORDER BY date"
        ).fetchdf()["date"].tolist()
        trading_days = [d.date() if hasattr(d, "date") else d for d in trading_days_raw]

        if not trading_days:
            logger.warning("ohlcv: trade_calendar empty, falling back to count-based backfill")
            sparse = db.conn.execute(
                "SELECT symbol FROM daily_ohlcv GROUP BY symbol HAVING COUNT(*) < 500"
            ).fetchdf()["symbol"].tolist()
            if not sparse:
                sparse = symbols
                logger.info("ohlcv: daily_ohlcv is empty, fetching all %d stocks", len(sparse))
            if sparse:
                _fetch_batch_easy_tdx(sparse, 5000, max_workers=config.thread_pool,
                                      market_map=market_map, write_db=db)
            return db.count("daily_ohlcv")

        # Compute expected vs actual rows per stock
        df_stats = db.conn.execute("""
            SELECT
                su.symbol,
                su.list_date,
                (SELECT MIN(date) FROM daily_ohlcv WHERE symbol = su.symbol) AS first_date,
                (SELECT COUNT(*) FROM daily_ohlcv WHERE symbol = su.symbol) AS actual_rows
            FROM stock_universe su
        """).fetchdf()

        null_dates = df_stats["list_date"].isna().sum()
        if null_dates > 0:
            logger.info("ohlcv: %d stocks have NULL list_date", null_dates)

        need_backfill: list[str] = []
        fallback_count = 0
        for _, row in df_stats.iterrows():
            list_date = row["list_date"]
            first_date = row["first_date"]
            effective = None
            if not pd.isna(list_date):
                effective = list_date.date() if hasattr(list_date, "date") else list_date
            elif not pd.isna(first_date):
                effective = first_date.date() if hasattr(first_date, "date") else first_date
            if effective is None:
                effective = BACKFILL_START
                fallback_count += 1
            effective = max(effective, BACKFILL_START)
            idx = bisect.bisect_left(trading_days, effective)
            expected = len(trading_days) - idx
            actual = int(row["actual_rows"]) if not pd.isna(row["actual_rows"]) else 0
            if expected > 0 and actual < expected * 0.85:
                need_backfill.append(row["symbol"])

        if fallback_count > 0:
            logger.info("ohlcv: %d stocks using BACKFILL_START (%s) as effective date",
                        fallback_count, BACKFILL_START)

        if not need_backfill:
            logger.info("ohlcv: backfill — 0/%d stocks need refetch", len(df_stats))
            return db.count("daily_ohlcv")

        logger.info("ohlcv: backfill — %d/%d stocks need refetch (expected vs actual gap)",
                    len(need_backfill), len(df_stats))

        # Multi-round fetch (max 3 rounds, 800 bars each)
        max_rounds = 3
        BARS_PER_ROUND = 800
        round_counts: dict[str, int] = {}

        for round_num in range(1, max_rounds + 1):
            if round_num > 1:
                min_bars = (round_num - 1) * BARS_PER_ROUND
                need_backfill = [
                    s for s in need_backfill
                    if round_counts.get(s, 0) >= min_bars
                ]

            if not need_backfill:
                logger.info("ohlcv: backfill complete — round %d, no stocks need more data", round_num)
                break

            offset = (round_num - 1) * BARS_PER_ROUND
            logger.info("ohlcv: backfill round %d/%d — %d stocks (offset=%d)",
                        round_num, max_rounds, len(need_backfill), offset)

            _fetch_batch_easy_tdx(need_backfill, BARS_PER_ROUND, max_workers=config.thread_pool,
                                  market_map=market_map, write_db=db, start=offset)

            # Record post-round counts
            placeholders = ",".join(["?" for _ in need_backfill])
            if placeholders:
                df_new = db.conn.execute(
                    f"SELECT symbol, COUNT(*) AS cnt FROM daily_ohlcv WHERE symbol IN ({placeholders}) GROUP BY symbol",
                    need_backfill,
                ).fetchdf()
                for _, row in df_new.iterrows():
                    round_counts[row["symbol"]] = int(row["cnt"])

            if round_num < max_rounds:
                time.sleep(2)

        return db.count("daily_ohlcv")
    else:
        count = 2  # incremental — last 2 days

    if not symbols:
        logger.warning("ohlcv: stock_universe is empty, cannot fetch")
        return 0

    # Incremental fetch from MacClient (QFQ)
    logger.info(
        "ohlcv: fetching %d stocks from easy_tdx MacClient (count=%d, workers=%d, mode=incremental)",
        len(symbols), count, config.thread_pool,
    )

    df = _fetch_batch_easy_tdx(symbols, count, max_workers=config.thread_pool, market_map=market_map)
    rows_written = 0
    if not df.empty:
        rows_written = db.upsert_dataframe("daily_ohlcv", df)

    # Check xdxr events — re-fetch QFQ history for stocks with existing data.
    # Skip stocks with < 100 bars (first deploy guard).
    xdxr_stocks = db.conn.execute(
        "SELECT DISTINCT stock_code FROM xdxr_events WHERE ex_date = ?",
        [trade_date],
    ).fetchdf()["stock_code"].tolist()
    if xdxr_stocks:
        placeholders = ",".join(["?" for _ in xdxr_stocks])
        df_has_history = db.conn.execute(
            f"SELECT symbol FROM daily_ohlcv WHERE symbol IN ({placeholders}) GROUP BY symbol HAVING COUNT(*) >= 100",
            xdxr_stocks,
        ).fetchdf()
        xdxr_stocks = df_has_history["symbol"].tolist() if not df_has_history.empty else []

    if xdxr_stocks:
        logger.info("ohlcv: re-fetching %d stocks (xdxr events)", len(xdxr_stocks))
        BARS_PER_ROUND = 800
        placeholders = ",".join(["?" for _ in xdxr_stocks])
        df_counts = db.conn.execute(
            f"SELECT symbol, COUNT(*) AS cnt FROM daily_ohlcv WHERE symbol IN ({placeholders}) GROUP BY symbol",
            xdxr_stocks,
        ).fetchdf()
        stock_counts: dict[str, int] = dict(zip(df_counts["symbol"], df_counts["cnt"]))
        max_bars = max(stock_counts.values()) if stock_counts else 0
        total_rounds = (max_bars + BARS_PER_ROUND - 1) // BARS_PER_ROUND

        logger.info("ohlcv: xdxr — max %d bars → %d rounds", max_bars, total_rounds)

        for rnd in range(total_rounds):
            offset = rnd * BARS_PER_ROUND
            to_fetch = [s for s in xdxr_stocks if stock_counts.get(s, 0) > offset]

            logger.info("ohlcv: xdxr round %d/%d — %d stocks (offset=%d)",
                        rnd + 1, total_rounds, len(to_fetch), offset)

            _fetch_batch_easy_tdx(
                to_fetch, BARS_PER_ROUND,
                max_workers=config.thread_pool,
                market_map=market_map, write_db=db,
                start=offset,
            )
            if rnd < total_rounds - 1:
                time.sleep(2)

    return rows_written
