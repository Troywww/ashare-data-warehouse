"""Ingestion persistence layer - DuckDB with upsert."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = os.getenv("INGESTION_DB_PATH", "./data/ingestion/stock_research.duckdb")


class IngestionDB:
    """DuckDB persistence layer. 19 tables, all INSERT OR REPLACE."""

    TABLES = {
        "trade_calendar": """
            CREATE TABLE IF NOT EXISTS trade_calendar (
                date    DATE PRIMARY KEY,
                is_trading BOOLEAN
            )""",

        "stock_universe": """
            CREATE TABLE IF NOT EXISTS stock_universe (
                symbol      VARCHAR PRIMARY KEY,
                name        VARCHAR,
                market      VARCHAR
            )""",

        "stock_classification": """
            CREATE TABLE IF NOT EXISTS stock_classification (
                symbol      VARCHAR PRIMARY KEY,
                industry    VARCHAR,
                region      VARCHAR
            )""",

        "concept_blocks": """
            CREATE TABLE IF NOT EXISTS concept_blocks (
                symbol       VARCHAR NOT NULL,
                concept_name VARCHAR NOT NULL,
                board_code   VARCHAR,
                PRIMARY KEY (symbol, concept_name)
            )""",

        "daily_ohlcv": """
            CREATE TABLE IF NOT EXISTS daily_ohlcv (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                open        DOUBLE,
                high        DOUBLE,
                low         DOUBLE,
                close       DOUBLE,
                volume      BIGINT,
                amount      DOUBLE,
                pct_chg     DOUBLE,
                turnover_rate DOUBLE,
                PRIMARY KEY (symbol, date)
            )""",

        "daily_valuation": """
            CREATE TABLE IF NOT EXISTS daily_valuation (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                pe_ttm      DOUBLE,
                pb          DOUBLE,
                ps_ttm      DOUBLE,
                pcf_ncf_ttm DOUBLE,
                total_mv    DOUBLE,
                circ_mv     DOUBLE,
                PRIMARY KEY (symbol, date)
            )""",

        "xdxr_events": """
            CREATE TABLE IF NOT EXISTS xdxr_events (
                stock_code   VARCHAR NOT NULL,
                ex_date      DATE NOT NULL,
                cash_dividend DOUBLE,
                bonus_ratio  DOUBLE,
                transfer_ratio DOUBLE,
                category     VARCHAR,
                PRIMARY KEY (stock_code, ex_date)
            )""",

        "dragon_tiger": """
            CREATE TABLE IF NOT EXISTS dragon_tiger (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                reason      VARCHAR DEFAULT '',
                close       DOUBLE,
                change_pct  DOUBLE,
                net_buy     DOUBLE,
                buy_amount  DOUBLE,
                sell_amount DOUBLE,
                total_amount DOUBLE,
                market_total_amount DOUBLE,
                net_buy_ratio DOUBLE,
                amount_ratio DOUBLE,
                turnover_rate DOUBLE,
                float_mv    DOUBLE,
                perf_1d     DOUBLE,
                perf_2d     DOUBLE,
                perf_5d     DOUBLE,
                perf_10d    DOUBLE,
                comment     VARCHAR,
                PRIMARY KEY (symbol, date, reason)
            )""",

        "capital_flow": """
            CREATE TABLE IF NOT EXISTS capital_flow (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                net_main    DOUBLE,
                net_super_5d   DOUBLE,
                net_large_5d   DOUBLE,
                net_medium_5d  DOUBLE,
                net_small_5d   DOUBLE,
                PRIMARY KEY (symbol, date)
            )""",

        "board_daily": """
            CREATE TABLE IF NOT EXISTS board_daily (
                date        DATE NOT NULL,
                board_name  VARCHAR NOT NULL,
                board_type  VARCHAR,
                change_pct  DOUBLE,
                rank        INTEGER,
                total_mv    DOUBLE,
                turnover_rate DOUBLE,
                up_count    INTEGER,
                down_count  INTEGER,
                leader_name VARCHAR,
                leader_pct  DOUBLE,
                PRIMARY KEY (date, board_name)
            )""",

        "hot_stocks": """
            CREATE TABLE IF NOT EXISTS hot_stocks (
                date        DATE NOT NULL,
                rank        INTEGER NOT NULL,
                symbol      VARCHAR,
                stock_name  VARCHAR,
                follow_count DOUBLE,
                price       DOUBLE,
                PRIMARY KEY (date, rank)
            )""",

        "hot_reasons": """
            CREATE TABLE IF NOT EXISTS hot_reasons (
                date        DATE NOT NULL,
                symbol      VARCHAR NOT NULL,
                stock_name  VARCHAR,
                reason_tags VARCHAR,
                close       DOUBLE,
                change_amt  DOUBLE,
                change_pct  DOUBLE,
                turnover_rate DOUBLE,
                amount      DOUBLE,
                volume      DOUBLE,
                PRIMARY KEY (date, symbol)
            )""",

        "northbound_flow": """
            CREATE TABLE IF NOT EXISTS northbound_flow (
                trade_date  DATE NOT NULL,
                market      VARCHAR NOT NULL,
                net_buy     DOUBLE,
                PRIMARY KEY (trade_date, market)
            )""",

        "margin_trading": """
            CREATE TABLE IF NOT EXISTS margin_trading (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                rzye        DOUBLE,
                rzye_buy    DOUBLE,
                rzye_repay  DOUBLE,
                rqyl        DOUBLE,
                rqyl_sell   DOUBLE,
                rqyl_repay  DOUBLE,
                rqyl_amt    DOUBLE,
                rzrqye      DOUBLE,
                PRIMARY KEY (symbol, date)
            )""",

        "block_trades": """
            CREATE TABLE IF NOT EXISTS block_trades (
                stock_code   VARCHAR NOT NULL,
                trade_date   DATE NOT NULL,
                price        DOUBLE,
                volume       BIGINT,
                amount       DOUBLE,
                premium_ratio DOUBLE,
                buyer_broker VARCHAR,
                seller_broker VARCHAR,
                PRIMARY KEY (stock_code, trade_date, price)
            )""",

        "holder_count": """
            CREATE TABLE IF NOT EXISTS holder_count (
                stock_code   VARCHAR NOT NULL,
                end_date     DATE NOT NULL,
                holder_count BIGINT,
                change_qoq   DOUBLE,
                avg_shares   DOUBLE,
                PRIMARY KEY (stock_code, end_date)
            )""",

        "lockup_calendar": """
            CREATE TABLE IF NOT EXISTS lockup_calendar (
                stock_code   VARCHAR NOT NULL,
                unlock_date  DATE NOT NULL,
                unlock_vol   BIGINT,
                unlock_ratio DOUBLE,
                status       VARCHAR,
                PRIMARY KEY (stock_code, unlock_date)
            )""",

        "fundamentals": """
            CREATE TABLE IF NOT EXISTS fundamentals (
                symbol      VARCHAR NOT NULL,
                end_date    DATE NOT NULL,
                publ_date   DATE,
                eps         DOUBLE,
                roe         DOUBLE,
                revenue     DOUBLE,
                profit      DOUBLE,
                revenue_yoy DOUBLE,
                profit_yoy  DOUBLE,
                bvps        DOUBLE,
                operating_cashflow DOUBLE,
                gross_margin DOUBLE,
                industry    VARCHAR,
                PRIMARY KEY (symbol, end_date)
            )""",

        "global_markets": """
            CREATE TABLE IF NOT EXISTS global_markets (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                open        DOUBLE,
                high        DOUBLE,
                low         DOUBLE,
                close       DOUBLE,
                volume      DOUBLE,
                PRIMARY KEY (symbol, date)
            )""",
    }

    # 每张表用于判断"最后更新日期"的列名
    TABLE_DATE_COLUMNS: Dict[str, str] = {
        "trade_calendar": "date",
        "daily_ohlcv": "date",
        "daily_valuation": "date",
        "capital_flow": "date",
        "dragon_tiger": "date",
        "margin_trading": "date",
        "fundamentals": "end_date",
        "board_daily": "date",
        "hot_stocks": "date",
        "northbound_flow": "trade_date",
        "hot_reasons": "date",
        "lockup_calendar": "unlock_date",
        "block_trades": "trade_date",
        "holder_count": "end_date",
        "global_markets": "date",
        "xdxr_events": "ex_date",
    }

    def __init__(self, db_path: Optional[str] = None, *, ensure_schema: bool = True):
        self.db_path = str(Path(db_path or _DEFAULT_DB_PATH).expanduser().resolve())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        if ensure_schema: self.ensure_schema()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def ensure_schema(self) -> None:
        for ddl in self.TABLES.values():
            self.conn.execute(ddl)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_date ON daily_ohlcv (symbol, date DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_val_sym_date ON daily_valuation (symbol, date DESC)")

    def get_db_size(self) -> int:
        return Path(self.db_path).stat().st_size

    def get_max_date(self, table: str) -> Optional[date]:
        col = self.TABLE_DATE_COLUMNS.get(table)
        if not col:
            return None
        row = self.conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
        return row[0] if row and row[0] else None

    def upsert_dataframe(self, table: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        df = df.copy()
        for col in df.select_dtypes(include=["datetime64[ns]"]):
            df[col] = df[col].dt.date
        self.conn.register("_ingest_df", df)
        cols = ", ".join(df.columns)
        self.conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {cols} FROM _ingest_df")
        self.conn.unregister("_ingest_df")
        return len(df)

    def count(self, table: str, symbol: Optional[str] = None) -> int:
        if symbol:
            row = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE symbol = ?", [symbol]).fetchone()
        else:
            row = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return row[0] if row else 0

    def table_stats(self) -> Dict[str, int]:
        stats = {}
        for name in self.TABLES:
            try:
                stats[name] = self.conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            except Exception:
                stats[name] = 0
        return stats
