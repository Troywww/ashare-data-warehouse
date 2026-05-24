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


def _fetch_single_opentdx(symbol: str, count: int, market_override: int | None = None) -> list[dict]:
    """Fetch daily kline for one symbol from opentdx (with retry)."""
    last_error = None
    for attempt in range(3):
        try:
            with TdxClient() as client:
                mkt = _market(symbol) if market_override is None else market_override
                klines = client.stock_kline(
                    mkt, symbol, PERIOD.DAILY,
                    count=count, adjust=ADJUST.QFQ,
                )
                if not klines:
                    return []

                results = []
                for k in klines:
                    dt_str = str(k.get("datetime", ""))[:10]
                    try:
                        d = datetime.strptime(dt_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    if d < BACKFILL_START:
                        continue

                    close = float(k.get("close", 0))
                    pre_close = None
                    if results:
                        pre_close = results[-1]["close"]

                    results.append({
                        "symbol": symbol, "date": d,
                        "open": float(k.get("open", 0)),
                        "high": float(k.get("high", 0)),
                        "low": float(k.get("low", 0)),
                        "close": close,
                        "volume": int(k.get("vol", 0)),
                        "amount": float(k.get("amount", 0)),
                        "pct_chg": ((close / pre_close) - 1) * 100 if pre_close and pre_close > 0 else None,
                        "turnover_rate": k.get("turnover"),
                    })
                return results
        except Exception as e:
            last_error = e
            if attempt < 2:
                continue  # immediate retry
    logger.debug("opentdx %s failed after 3 retries: %s", symbol, last_error)
    return []


def _fetch_single_baostock(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch daily kline for one symbol from baostock (backup)."""
    market_prefix = "sh." if symbol.startswith(("6", "68")) else "sz."
    code = f"{market_prefix}{symbol}"

    rs = bs.query_history_k_data_plus(
        code,
        fields="date,open,high,low,close,volume,amount,pctChg,turn",
        start_date=start_date,
        end_date=end_date,
        adjustflag=2,  # 前复权
    )
    if rs.error_code != "0":
        logger.debug("baostock %s: %s", symbol, rs.error_msg)
        return []

    results = []
    while rs.next():
        row = rs.get_row_data()
        d = datetime.strptime(row[0], "%Y-%m-%d").date()
        results.append({
            "symbol": symbol,
            "date": d,
            "open": float(row[1]) if row[1] else 0,
            "high": float(row[2]) if row[2] else 0,
            "low": float(row[3]) if row[3] else 0,
            "close": float(row[4]) if row[4] else 0,
            "volume": int(float(row[5])) if row[5] else 0,
            "amount": float(row[6]) if row[6] else 0,
            "pct_chg": float(row[7]) if row[7] else None,
            "turnover_rate": float(row[8]) if row[8] else None,
        })
    return results


def _ensure_client(client_holder: list, max_retries: int = 3) -> object:
    """Get or create a TdxClient. Returns the client.

    Retries up to max_retries times with exponential backoff,
    since opentdx servers may throttle under heavy concurrent load.
    """
    if client_holder[0] is not None:
        return client_holder[0]
    last_err = None
    for attempt in range(max_retries):
        try:
            c = TdxClient()
            c.__enter__()
            client_holder[0] = c
            return c
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            logger.warning("opentdx connect attempt %d/%d failed: %s, retry in %ds", attempt + 1, max_retries, e, wait)
            time.sleep(wait)
    raise ConnectionError("opentdx connect failed after %d retries: %s" % (max_retries, last_err))


def _close_client(client_holder: list) -> None:
    """Safely close TdxClient if open."""
    if client_holder[0] is not None:
        try:
            client_holder[0].__exit__(None, None, None)
        except Exception:
            pass
        client_holder[0] = None


def _process_klines(sym: str, klines: list, prev_close: dict, results: list) -> None:
    """Parse opentdx kline data and append to results."""
    for k in sorted(klines, key=lambda x: str(x.get("datetime", ""))):
        dt_str = str(k.get("datetime", ""))[:10]
        try:
            d = datetime.strptime(dt_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < BACKFILL_START:
            continue
        close = float(k.get("close", 0))
        pc = prev_close.get(sym)
        results.append({
            "symbol": sym, "date": d,
            "open": float(k.get("open", 0)), "high": float(k.get("high", 0)),
            "low": float(k.get("low", 0)), "close": close,
            "volume": int(k.get("vol", 0)), "amount": float(k.get("amount", 0)),
            "pct_chg": ((close / pc) - 1) * 100 if pc and pc > 0 else None,
            "turnover_rate": k.get("turnover"),
        })
        prev_close[sym] = close


def _fetch_worker(stocks: list[tuple[str, int]], count: int) -> list[dict]:
    """Fetch klines for a chunk of stocks.

    Reliability design:
    - One TCP connection reused across stocks (performance)
    - On ANY exception: close broken connection, open fresh one, retry same stock
    - Each stock gets up to 3 attempts with independent connections
    - Connection is always cleaned up at the end
    """
    results = []
    prev_close: dict[str, float] = {}
    client_holder: list = [None]  # mutable holder for closure-like access

    try:
        for sym, mkt in stocks:
            last_error = None
            for attempt in range(3):
                try:
                    _ensure_client(client_holder)
                    klines = client_holder[0].stock_kline(
                        mkt, sym, PERIOD.DAILY, count=count, adjust=ADJUST.QFQ,
                    )
                    if klines:
                        _process_klines(sym, klines, prev_close, results)
                    break  # success, move to next stock
                except Exception as e:
                    last_error = e
                    # Connection broken — close it, next attempt creates new one
                    _close_client(client_holder)
                    if attempt < 2:
                        time.sleep(1)
                        continue
                    # 3 attempts exhausted
                    logger.warning("opentdx %s failed after 2 retries: %s", sym, e)
    finally:
        _close_client(client_holder)

    return results


def _fetch_batch_opentdx(symbols: list[str], count: int, max_workers: int = 8,
                         market_map: dict[str, int] | None = None,
                         write_db: IngestionDB | None = None) -> pd.DataFrame:
    """Fetch klines in parallel — each thread reuses one TdxClient for many stocks.

    If write_db is provided, results are written per-worker to DB (for large backfills)
    instead of accumulating all in memory.
    """
    stock_list = [(sym, market_map.get(sym, 0)) for sym in symbols]
    chunk_size = max(1, len(stock_list) // max_workers)
    chunks = [stock_list[i:i + chunk_size] for i in range(0, len(stock_list), chunk_size)]

    all_rows: list[dict] = [] if write_db is None else None  # skip accumulation when writing
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as pool:
        futs = [pool.submit(_fetch_worker, chunk, count) for chunk in chunks]
        for i, fut in enumerate(as_completed(futs)):
            try:
                rows = fut.result()
                if write_db and rows:
                    df = pd.DataFrame(rows)
                    # Each worker uses its own DB connection (thread-safe)
                    from src.ingestion.db import IngestionDB
                    wdb = IngestionDB(write_db.db_path)
                    n = wdb.upsert_dataframe("daily_ohlcv", df)
                    wdb.close()
                    logger.info("ohlcv: worker %d/%d done — %d rows written (%d stocks, %.0fs)",
                               i + 1, len(futs), n, len(chunks[i]), time.time() - t0)
                elif all_rows is not None:
                    all_rows.extend(rows)
                    logger.info("ohlcv: worker %d/%d done (%d stocks, %.0fs)",
                               i + 1, len(futs), len(chunks[i]), time.time() - t0)
            except Exception as e:
                logger.warning("ohlcv worker failed: %s", e)

    if all_rows is not None:
        if not all_rows:
            return pd.DataFrame()
        return pd.DataFrame(all_rows)
    return pd.DataFrame()


def _fetch_batch_baostock(symbols: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch klines for multiple symbols from baostock (sequential)."""
    bs.login()
    try:
        all_rows = []
        for sym in symbols:
            try:
                rows = _fetch_single_baostock(sym, start_date, end_date)
                all_rows.extend(rows)
            except Exception as e:
                logger.debug("baostock %s: %s", sym, e)
        if not all_rows:
            return pd.DataFrame()
        return pd.DataFrame(all_rows)
    finally:
        bs.logout()


@register_fetcher(
    "daily_ohlcv",
    depends_on=["stock_universe"],
    group="core",
    description="日K线前复权 — opentdx QFQ 主源 / baostock 备源",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily OHLCV data.

    Strategy:
      - Daily incremental: fetch last 5 days from opentdx (8 threads)
      - Backfill (trade_date=1970-01-01): fetch 800 bars per stock
      - On xdxr event: re-fetch affected stocks from baostock (full history)
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
        # Smart backfill: auto-retry until all stocks have sufficient history
        # (opentdx may throttle under concurrency, so we retry failed stocks)
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
            # Cooldown between rounds to avoid opentdx server throttling
            if round_num < max_rounds:
                remaining = db.conn.execute(
                    "SELECT COUNT(*) FROM daily_ohlcv GROUP BY symbol HAVING COUNT(*) < 500"
                ).fetchdf().shape[0]
                if remaining > 0:
                    logger.info("ohlcv: backfill round %d done, %d stocks still sparse — cooling 30s", round_num, remaining)
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
        logger.info("ohlcv: %d stocks with xdxr events, re-fetching from opentdx", len(xdxr_stocks))
        xdxr_map = {s: market_map.get(s, 0) for s in xdxr_stocks if s in market_map}
        if xdxr_map:
            df_x = _fetch_batch_opentdx(list(xdxr_map.keys()), 5000,
                                        max_workers=4, market_map=xdxr_map, write_db=db)
            logger.info("ohlcv: xdxr re-fetch done")

    return rows_written
