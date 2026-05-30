"""DataService — unified data access layer.

Routes data requests through:
1. Cache (market-aware TTL + request coalescing)
2. DuckDB (persistent data)
3. On-demand fetcher (real-time sources)
4. Compute engine (technical indicators)

Usage::

    ds = DataService(db_path="./data/ingestion/stock_research.duckdb")
    result = await ds.fetch("realtime_quote", {"symbol": "000001"})
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any, Optional

import pandas as pd

from src.ingestion.db import IngestionDB
from .cache import MarketAwareCache
from .policy import POLICIES, CachePolicy, is_trading_time
from .fetchers.realtime_quote import (
    fetch_realtime_quote,
    fetch_realtime_quotes,
    fetch_intraday_kline,
    fetch_limit_up_ladder,
)
from .fetchers.news import fetch_cls_telegram, fetch_stock_news
from .fetchers.announcements import fetch_announcements
from .fetchers.holders import fetch_shareholder_changes
from .fetchers.consensus import fetch_eps_consensus, fetch_research_reports
from .fetchers.on_demand import (
    fetch_board_daily,
    fetch_block_trades,
    fetch_dragon_tiger,
    fetch_dragon_tiger_seats,
    fetch_global_markets,
    fetch_hot_reasons,
    fetch_hot_stocks,
    fetch_lockup_calendar,
    fetch_margin_trading,
)
from .writers.news_writer import persist_cls_telegram, persist_stock_news
from .compute.indicators import signal_scan, params_hash

logger = logging.getLogger(__name__)


class DataService:
    """Unified data access layer with caching, persistence, and compute."""

    def __init__(
        self,
        db_path: str,
        cache: MarketAwareCache | None = None,
    ):
        self.db_path = db_path
        self.cache = cache or MarketAwareCache()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch(self, data_type: str, params: dict | None = None) -> Any:
        """Fetch data by type, with caching and auto-routing.

        Parameters
        ----------
        data_type : str
            One of the keys in POLICIES (e.g. 'realtime_quote', 'cls_telegram').
        params : dict | None
            Parameters for the data type (e.g. {"symbol": "000001"}).

        Returns
        -------
        Any
            Fetched or computed data.
        """
        policy = POLICIES.get(data_type)
        if policy is None:
            raise ValueError(f"Unknown data_type: {data_type}. Available: {list(POLICIES.keys())}")

        # Build cache key
        cache_key = self._build_key(data_type, params)

        # Is this a compute type?
        if policy.compute_fn:
            return await self._compute_and_cache(cache_key, policy, params)

        # Is it persistent (check DuckDB first)?
        if policy.check_db_first and policy.db_table:
            db_result = await self._query_db(policy.db_table, params)
            if db_result is not None and len(db_result) > 0:
                return db_result

        # Cache + on-demand fetch
        return await self.cache.get_or_fetch(
            key=cache_key,
            policy=policy,
            fetcher=lambda: self._do_fetch(data_type, policy, params),
        )

    async def compute(self, indicator: str, params: dict | None = None) -> Any:
        """Compute a technical indicator on demand.

        This is a convenience wrapper around fetch() for compute types.

        Parameters
        ----------
        indicator : str
            One of 'macd', 'kdj', 'signal_scan'.
        params : dict
            Parameters including data source info (symbol, period, etc.)
        """
        # Route to the right policy
        if indicator == "macd":
            return await self.fetch("compute_macd", params)
        elif indicator == "kdj":
            return await self.fetch("compute_kdj", params)
        elif indicator == "rsi":
            return await self.fetch("compute_rsi", params)
        elif indicator == "boll":
            return await self.fetch("compute_boll", params)
        elif indicator == "signal_scan":
            return await self.fetch("compute_signal_scan", params)
        else:
            raise ValueError(f"Unknown indicator: {indicator}")

    def fetch_sync(self, data_type: str, params: dict | None = None) -> Any:
        """Sync version of fetch — for use in non-async contexts (e.g. MCP).

        WARNING: Uses asyncio.run() internally. Do NOT call this from within an
        already-running event loop (it will raise RuntimeError). Use `await ds.fetch()`
        instead in async contexts.
        """
        return asyncio.run(self.fetch(data_type, params))

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def invalidate_all(self) -> int:
        """Clear all cache. Call before market open."""
        return self.cache.invalidate_all()

    def invalidate_by_type(self, data_type: str) -> int:
        """Clear cache for a specific data type."""
        prefix = f"{data_type}:"
        return self.cache.invalidate_by_prefix(prefix)

    def cache_stats(self) -> dict:
        """Get cache statistics."""
        return self.cache.stats()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_key(self, data_type: str, params: dict | None) -> str:
        """Build a deterministic cache key from data type and params."""
        if not params:
            return data_type
        # Sort params for deterministic keys
        sorted_params = json.dumps(params, sort_keys=True)
        return f"{data_type}:{sorted_params}"

    async def _do_fetch(self, data_type: str, policy: CachePolicy, params: dict) -> Any:
        """Execute the actual fetch from source."""
        source = policy.source
        result = None

        # Route to the correct fetcher
        if data_type == "realtime_quote":
            symbol = params.get("symbol", "")
            result = await fetch_realtime_quote(symbol)

        elif data_type == "realtime_quotes":
            symbols = params.get("symbols", [])
            result = await fetch_realtime_quotes(symbols)

        elif data_type == "intraday_kline_1min":
            symbol = params.get("symbol", "")
            count = params.get("count", 240)
            result = await fetch_intraday_kline(symbol, "1min", count)

        elif data_type == "intraday_kline_5min":
            symbol = params.get("symbol", "")
            count = params.get("count", 96)
            result = await fetch_intraday_kline(symbol, "5min", count)

        elif data_type == "limit_up_ladder":
            result = await fetch_limit_up_ladder()

        elif data_type == "cls_telegram":
            count = params.get("count", 50)
            result = await fetch_cls_telegram(count)
            # Persist to DuckDB
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        persist_cls_telegram(db, result)
                except Exception as e:
                    logger.warning("Failed to persist cls_telegram: %s", e)

        elif data_type == "stock_news":
            symbol = params.get("symbol", "")
            result = await fetch_stock_news(symbol)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        persist_stock_news(db, result)
                except Exception as e:
                    logger.warning("Failed to persist stock_news: %s", e)

        elif data_type == "announcements":
            symbol = params.get("symbol", "")
            days = params.get("days", 30)
            result = await fetch_announcements(symbol)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.append_dataframe("announcements", df)
                except Exception as e:
                    logger.warning("Failed to persist announcements: %s", e)

        elif data_type == "research_reports":
            symbol = params.get("symbol", "")
            result = await fetch_research_reports(symbol)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("research_reports", df)
                except Exception as e:
                    logger.warning("Failed to persist research_reports: %s", e)

        elif data_type == "eps_consensus":
            symbol = params.get("symbol", "")
            result = await fetch_eps_consensus(symbol)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("eps_consensus", df)
                except Exception as e:
                    logger.warning("Failed to persist eps_consensus: %s", e)

        elif data_type == "shareholder_changes":
            symbol = params.get("symbol", "")
            result = await fetch_shareholder_changes(symbol)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("shareholder_changes", df)
                except Exception as e:
                    logger.warning("Failed to persist shareholder_changes: %s", e)

        # === on-demand tables (formerly pipeline Wave 2) ===
        elif data_type == "dragon_tiger":
            date_str = params.get("date_str", "")
            result = await fetch_dragon_tiger(date_str)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("dragon_tiger", df)
                except Exception as e:
                    logger.warning("Failed to persist dragon_tiger: %s", e)

        elif data_type == "dragon_tiger_seats":
            symbol = params.get("symbol", "")
            date_str = params.get("date_str", "")
            result = await fetch_dragon_tiger_seats(symbol, date_str)
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("dragon_tiger_seats", df)
                except Exception as e:
                    logger.warning("Failed to persist dragon_tiger_seats: %s", e)

        elif data_type == "board_daily":
            result = await fetch_board_daily()
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("board_daily", df)
                except Exception as e:
                    logger.warning("Failed to persist board_daily: %s", e)

        elif data_type == "hot_stocks":
            result = await fetch_hot_stocks()
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("hot_stocks", df)
                except Exception as e:
                    logger.warning("Failed to persist hot_stocks: %s", e)

        elif data_type == "hot_reasons":
            result = await fetch_hot_reasons()
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("hot_reasons", df)
                except Exception as e:
                    logger.warning("Failed to persist hot_reasons: %s", e)

        elif data_type == "margin_trading":
            date_str = params.get("date_str", "")
            symbol = params.get("symbol", "")
            result = await fetch_margin_trading(date_str)
            if symbol and result:
                result = [r for r in result if r.get("symbol") == symbol]
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("margin_trading", df)
                except Exception as e:
                    logger.warning("Failed to persist margin_trading: %s", e)

        elif data_type == "block_trades":
            date_str = params.get("date_str", "")
            symbol = params.get("symbol", "")
            result = await fetch_block_trades(date_str)
            if symbol and result:
                result = [r for r in result if r.get("stock_code") == symbol]
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("block_trades", df)
                except Exception as e:
                    logger.warning("Failed to persist block_trades: %s", e)

        elif data_type == "lockup_calendar":
            result = await fetch_lockup_calendar()
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("lockup_calendar", df)
                except Exception as e:
                    logger.warning("Failed to persist lockup_calendar: %s", e)

        elif data_type == "global_markets":
            result = await fetch_global_markets()
            if policy.persist and result:
                try:
                    with IngestionDB(self.db_path) as db:
                        df = pd.DataFrame(result)
                        db.upsert_dataframe("global_markets", df)
                except Exception as e:
                    logger.warning("Failed to persist global_markets: %s", e)

        else:
            raise ValueError(f"No fetcher for data_type: {data_type}")

        return result

    async def _query_db(self, table: str, params: dict | None) -> Any:
        """Query DuckDB for existing data."""
        try:
            with IngestionDB(self.db_path) as db:
                if table == "cls_telegram":
                    days = (params or {}).get("days", 1)
                    df = db.conn.execute(f"""
                        SELECT * FROM cls_telegram
                        WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '{days} DAY'
                        ORDER BY created_at DESC LIMIT 50
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "stock_news":
                    symbol = (params or {}).get("symbol", "")
                    df = db.conn.execute("""
                        SELECT * FROM stock_news WHERE symbol = ?
                        ORDER BY time DESC LIMIT 50
                    """, [symbol]).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                # On-demand tables: check if data exists for today
                elif table == "dragon_tiger":
                    df = db.conn.execute("""
                        SELECT * FROM dragon_tiger
                        WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
                        ORDER BY date DESC, net_buy DESC
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "dragon_tiger_seats":
                    symbol = (params or {}).get("symbol", "")
                    date_str = (params or {}).get("date_str", "")
                    if symbol and date_str:
                        df = db.conn.execute("""
                            SELECT * FROM dragon_tiger_seats
                            WHERE symbol = ? AND date = ?
                            ORDER BY side, net_amount DESC
                        """, [symbol, date_str]).fetchdf()
                    elif symbol:
                        df = db.conn.execute("""
                            SELECT * FROM dragon_tiger_seats
                            WHERE symbol = ?
                            ORDER BY date DESC, side, net_amount DESC
                        """, [symbol]).fetchdf()
                    else:
                        df = db.conn.execute("""
                            SELECT * FROM dragon_tiger_seats
                            WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
                            ORDER BY date DESC, symbol, side, net_amount DESC
                        """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "board_daily":
                    df = db.conn.execute("""
                        SELECT * FROM board_daily
                        WHERE date = (SELECT MAX(date) FROM board_daily)
                        ORDER BY rank
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "hot_stocks":
                    df = db.conn.execute("""
                        SELECT * FROM hot_stocks
                        WHERE date = (SELECT MAX(date) FROM hot_stocks)
                        ORDER BY rank LIMIT 50
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "hot_reasons":
                    df = db.conn.execute("""
                        SELECT * FROM hot_reasons
                        WHERE date = (SELECT MAX(date) FROM hot_reasons)
                        ORDER BY change_pct DESC LIMIT 50
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "margin_trading":
                    symbol = (params or {}).get("symbol", "")
                    if symbol:
                        df = db.conn.execute("""
                            SELECT * FROM margin_trading WHERE symbol = ?
                            ORDER BY date DESC LIMIT 60
                        """, [symbol]).fetchdf()
                    else:
                        df = db.conn.execute("""
                            SELECT * FROM margin_trading
                            WHERE date = (SELECT MAX(date) FROM margin_trading)
                        """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "block_trades":
                    symbol = (params or {}).get("symbol", "")
                    if symbol:
                        df = db.conn.execute("""
                            SELECT * FROM block_trades WHERE stock_code = ?
                            ORDER BY trade_date DESC LIMIT 50
                        """, [symbol]).fetchdf()
                    else:
                        df = db.conn.execute("""
                            SELECT * FROM block_trades
                            ORDER BY trade_date DESC LIMIT 50
                        """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "lockup_calendar":
                    df = db.conn.execute("""
                        SELECT * FROM lockup_calendar
                        WHERE unlock_date >= CURRENT_DATE
                        ORDER BY unlock_date
                    """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

                elif table == "global_markets":
                    symbol = (params or {}).get("symbol", "")
                    if symbol:
                        df = db.conn.execute("""
                            SELECT * FROM global_markets WHERE symbol = ?
                            ORDER BY date DESC LIMIT 60
                        """, [symbol]).fetchdf()
                    else:
                        df = db.conn.execute("""
                            SELECT * FROM global_markets
                            WHERE date = (SELECT MAX(date) FROM global_markets)
                            ORDER BY symbol
                        """).fetchdf()
                    return df.to_dict(orient="records") if not df.empty else None

            return None
        except Exception as e:
            logger.debug("DB query failed for %s: %s", table, e)
            return None

    async def _compute_and_cache(
        self, cache_key: str, policy: CachePolicy, params: dict | None
    ) -> Any:
        """Execute a compute function with caching."""
        return await self.cache.get_or_fetch(
            key=cache_key,
            policy=policy,
            fetcher=lambda: self._run_compute(policy.compute_fn, params),
        )

    async def _run_compute(self, compute_fn: str | None, params: dict | None) -> Any:
        """Run a compute function."""
        if compute_fn == "signal_scan":
            return await self._compute_signal_scan(params or {})
        elif compute_fn == "macd":
            return await self._compute_single_indicator("macd", params or {})
        elif compute_fn == "kdj":
            return await self._compute_single_indicator("kdj", params or {})
        elif compute_fn == "rsi":
            return await self._compute_single_indicator("rsi", params or {})
        elif compute_fn == "boll":
            return await self._compute_single_indicator("boll", params or {})
        else:
            raise ValueError(f"Unknown compute function: {compute_fn}")

    async def _compute_signal_scan(self, params: dict) -> list[dict]:
        """Scan stocks for a technical signal — reads from indicator_values.

        Daily signals are derived from indicator_values (fast SQL query).
        Weekly signals fall back to full computation from OHLCV.
        """
        indicator = params.get("indicator", "macd")
        signal = params.get("signal", "golden_cross")
        period = params.get("period", "daily")
        lookback = params.get("lookback", 120)

        # Weekly: indicator_values is daily-only, fall back to full compute
        if period == "weekly":
            return await self._compute_signal_scan_from_ohlcv(params)

        # Daily: read from indicator_values and derive signals
        try:
            with IngestionDB(self.db_path) as db:
                return self._scan_indicator_values(db, indicator, signal)
        except Exception as e:
            logger.error("Failed to scan indicator_values: %s", e)
            return []

    async def _compute_signal_scan_from_ohlcv(self, params: dict) -> list[dict]:
        """Fallback: compute signals from OHLCV (used for weekly)."""
        indicator = params.get("indicator", "macd")
        signal = params.get("signal", "golden_cross")
        period = params.get("period", "daily")
        lookback = params.get("lookback", 120)

        try:
            with IngestionDB(self.db_path) as db:
                df = db.conn.execute(f"""
                    SELECT symbol, date, open, high, low, close, volume
                    FROM daily_ohlcv
                    WHERE date >= CURRENT_DATE - INTERVAL '{lookback} DAY'
                    ORDER BY symbol, date
                """).fetchdf()
        except Exception as e:
            logger.error("Failed to read K-line for signal scan: %s", e)
            return []

        if df.empty:
            return []

        raw_params = {
            "fast": params.get("fast", 12),
            "slow": params.get("slow", 26),
            "signal": params.get("signal_period", 9),
            "n": params.get("n", 9),
            "period": params.get("rsi_period", 14),
        }
        return signal_scan(df, indicator, signal, period, raw_params)

    def _scan_indicator_values(
        self, db: IngestionDB, indicator: str, signal: str
    ) -> list[dict]:
        """Derive signals from indicator_values table (fast, no recompute)."""
        # Map signal → indicator_values column + condition
        SIGNAL_SQL = {
            # MACD cross-over: today DIF>DEA, yesterday DIF<=DEA
            # MACD cross pre-computed at indicator time (MACD_CROSS: 1=golden, -1=dead)
            "golden_cross": """
                SELECT symbol, date::VARCHAR AS date,
                       'golden_cross' AS signal, MACD_DIF AS value
                FROM indicator_values
                WHERE freq = 'D' AND MACD_CROSS = 1
            """,
            "dead_cross": """
                SELECT symbol, date::VARCHAR AS date,
                       'dead_cross' AS signal, MACD_DIF AS value
                FROM indicator_values
                WHERE freq = 'D' AND MACD_CROSS = -1
            """,
            # RSI thresholds: check latest daily value (easy_tdx default N=24)
            "oversold": """
                SELECT symbol, date::VARCHAR AS date,
                       'oversold' AS signal, RSI AS value
                FROM indicator_values
                WHERE freq = 'D'
                  AND date = (SELECT MAX(date) FROM indicator_values WHERE freq = 'D')
                  AND RSI < 30
            """,
            "overbought": """
                SELECT symbol, date::VARCHAR AS date,
                       'overbought' AS signal, RSI AS value
                FROM indicator_values
                WHERE freq = 'D'
                  AND date = (SELECT MAX(date) FROM indicator_values WHERE freq = 'D')
                  AND RSI > 70
            """,
            # BOLL breakouts: check latest daily value
            "upper_break": """
                SELECT iv.symbol, iv.date::VARCHAR AS date,
                       'upper_break' AS signal, (iv.BOLL_UPPER - iv.BOLL_LOWER) / iv.BOLL_MID AS value
                FROM indicator_values iv
                JOIN daily_ohlcv ohlcv
                  ON iv.symbol = ohlcv.symbol AND iv.date = ohlcv.date
                WHERE iv.freq = 'D'
                  AND iv.date = (SELECT MAX(date) FROM indicator_values WHERE freq = 'D')
                  AND ohlcv.close > iv.BOLL_UPPER
            """,
            "lower_break": """
                SELECT iv.symbol, iv.date::VARCHAR AS date,
                       'lower_break' AS signal, (iv.BOLL_UPPER - iv.BOLL_LOWER) / iv.BOLL_MID AS value
                FROM indicator_values iv
                JOIN daily_ohlcv ohlcv
                  ON iv.symbol = ohlcv.symbol AND iv.date = ohlcv.date
                WHERE iv.freq = 'D'
                  AND iv.date = (SELECT MAX(date) FROM indicator_values WHERE freq = 'D')
                  AND ohlcv.close < iv.BOLL_LOWER
            """,
        }

        sql = SIGNAL_SQL.get(signal)
        if not sql:
            logger.warning("Unknown signal: %s/%s", indicator, signal)
            return []

        try:
            rows = db.conn.execute(sql).fetchall()
        except Exception as e:
            logger.warning("Signal scan SQL failed: %s", e)
            return []

        results = []
        for row in rows:
            results.append({
                "symbol": row[0],
                "date": row[1],
                "signal": row[2],
                "value": row[3] if row[3] is not None else 0.0,
                "extra": {"indicator": indicator, "period": "daily"},
            })
        return results

    async def _compute_single_indicator(self, indicator: str, params: dict) -> dict:
        """Compute a single indicator for one stock."""
        symbol = params.get("symbol", "")
        lookback = params.get("lookback", 120)

        # Read K-line
        try:
            with IngestionDB(self.db_path) as db:
                df = db.conn.execute(f"""
                    SELECT date, open, high, low, close, volume
                    FROM daily_ohlcv
                    WHERE symbol = ? AND date >= CURRENT_DATE - INTERVAL '{lookback} DAY'
                    ORDER BY date
                """, [symbol]).fetchdf()
        except Exception as e:
            logger.error("Failed to read K-line for %s: %s", symbol, e)
            return {}

        if df.empty:
            return {}

        from easy_tdx.indicator import compute_indicators as easy_indicators

        if indicator in ("macd", "kdj", "rsi", "boll"):
            result = easy_indicators(df, [indicator.upper()], keep_ohlcv=False)
            if result.empty:
                return {}
            # Flatten latest row to dict
            return result.iloc[-1].to_dict()
        return {}

    # _persist_signals removed — signals are now derived on-the-fly
    # from indicator_values via _scan_indicator_values, no separate table needed.
