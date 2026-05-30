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

    # Schema version for migration tracking
    SCHEMA_VERSION = 3

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
                market      VARCHAR,
                list_date   DATE
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
                net_super   DOUBLE,
                net_large   DOUBLE,
                net_medium  DOUBLE,
                net_small   DOUBLE,
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

        "cls_telegram": """
            CREATE TABLE IF NOT EXISTS cls_telegram (
                id          VARCHAR PRIMARY KEY,
                title       VARCHAR,
                content     TEXT,
                created_at  TIMESTAMP
            )""",

        "stock_news": """
            CREATE TABLE IF NOT EXISTS stock_news (
                symbol      VARCHAR NOT NULL,
                id          VARCHAR NOT NULL,
                title       VARCHAR,
                content     TEXT,
                source      VARCHAR,
                time        TIMESTAMP,
                url         VARCHAR,
                PRIMARY KEY (symbol, id)
            )""",

        "announcements": """
            CREATE TABLE IF NOT EXISTS announcements (
                id          VARCHAR PRIMARY KEY,
                symbol      VARCHAR NOT NULL,
                title       VARCHAR,
                announce_type VARCHAR,
                date        DATE,
                url         VARCHAR
            )""",

        "indicator_values": """
            CREATE TABLE IF NOT EXISTS indicator_values (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                freq        VARCHAR NOT NULL DEFAULT 'D',
                -- MACD (12,26,9)
                MACD_DIF    DOUBLE,
                MACD_DEA    DOUBLE,
                MACD_HIST   DOUBLE,
                MACD_CROSS  TINYINT DEFAULT 0,  -- 1=golden cross, -1=dead cross, 0=none
                -- KDJ (9,3,3)
                KDJ_K       DOUBLE,
                KDJ_D       DOUBLE,
                KDJ_J       DOUBLE,
                -- RSI (24) — easy_tdx default
                RSI         DOUBLE,
                -- BOLL (20,2)
                BOLL_UPPER  DOUBLE,
                BOLL_MID    DOUBLE,
                BOLL_LOWER  DOUBLE,
                -- BIAS (6,12,24)
                BIAS1       DOUBLE,
                BIAS2       DOUBLE,
                BIAS3       DOUBLE,
                -- PSY (12,6)
                PSY         DOUBLE,
                PSY_MA      DOUBLE,
                -- TRIX (12,20)
                TRIX        DOUBLE,
                TRIX_MA     DOUBLE,
                -- DPO (20,10,6)
                DPO         DOUBLE,
                DPO_MA      DOUBLE,
                -- MTM (12,6)
                MTM         DOUBLE,
                MTM_MA      DOUBLE,
                -- ROC (12,6)
                ROC         DOUBLE,
                ROC_MA      DOUBLE,
                -- EXPMA (12,50)
                EXPMA_12    DOUBLE,
                EXPMA_50    DOUBLE,
                -- BBI (3,6,12,20)
                BBI         DOUBLE,
                -- DFMA (10,50,10)
                DFMA_DIF    DOUBLE,
                DFMA_DMA    DOUBLE,
                -- DMI (14,6)
                DMI_PDI     DOUBLE,
                DMI_MDI     DOUBLE,
                DMI_ADX     DOUBLE,
                DMI_ADXR    DOUBLE,
                -- ATR (20)
                ATR         DOUBLE,
                -- WR (10,6)
                WR1         DOUBLE,
                WR2         DOUBLE,
                -- CCI (14)
                CCI         DOUBLE,
                -- CR (20)
                CR          DOUBLE,
                -- KTN 肯特纳通道 (20,10)
                KTN_UPPER   DOUBLE,
                KTN_MID     DOUBLE,
                KTN_LOWER   DOUBLE,
                -- XSII 薛斯通道II (102,7)
                XSII_TD1    DOUBLE,
                XSII_TD2    DOUBLE,
                XSII_TD3    DOUBLE,
                XSII_TD4    DOUBLE,
                -- OBV 能量潮
                OBV         DOUBLE,
                -- VR (26)
                VR          DOUBLE,
                -- EMV (14,9)
                EMV         DOUBLE,
                EMV_MA      DOUBLE,
                -- MASS (9,25,6)
                MASS        DOUBLE,
                MASS_MA     DOUBLE,
                -- MFI (14)
                MFI         DOUBLE,
                -- BRAR (26)
                AR          DOUBLE,
                BR          DOUBLE,
                -- ASI (26,10)
                ASI         DOUBLE,
                ASI_MA      DOUBLE,
                -- 捉妖大师 (120,60,20,10)
                ZY_LONG     DOUBLE,
                ZY_MID      DOUBLE,
                ZY_SHORT    DOUBLE,
                ZY_TREND    DOUBLE,
                -- BIAS_SIGNAL 乖离率信号 (10,30)
                BS_X        DOUBLE,
                BS_SMA      DOUBLE,
                BS_LMA      DOUBLE,
                -- TAQ 唐安奇通道 (20)
                TAQ_UP      DOUBLE,
                TAQ_MID     DOUBLE,
                TAQ_DOWN    DOUBLE,
                PRIMARY KEY (symbol, date, freq)
            )""",

        "research_reports": """
            CREATE TABLE IF NOT EXISTS research_reports (
                id          VARCHAR PRIMARY KEY,
                symbol      VARCHAR NOT NULL,
                title       VARCHAR,
                org_name    VARCHAR,
                publish_date DATE,
                rating      VARCHAR,
                target_price DOUBLE,
                eps_this_yr DOUBLE,
                eps_next_yr DOUBLE,
                eps_next2_yr DOUBLE,
                url         VARCHAR
            )""",

        "eps_consensus": """
            CREATE TABLE IF NOT EXISTS eps_consensus (
                symbol      VARCHAR NOT NULL,
                year        INTEGER NOT NULL,
                analyst_count INTEGER,
                eps_min     DOUBLE,
                eps_avg     DOUBLE,
                eps_max     DOUBLE,
                PRIMARY KEY (symbol, year)
            )""",

        "shareholder_changes": """
            CREATE TABLE IF NOT EXISTS shareholder_changes (
                symbol        VARCHAR NOT NULL,
                announce_date DATE NOT NULL,
                shareholder   VARCHAR,
                change_type   VARCHAR,
                change_vol    BIGINT,
                avg_price     DOUBLE,
                hold_vol      BIGINT,
                change_period VARCHAR,
                change_method VARCHAR,
                PRIMARY KEY (symbol, announce_date, shareholder)
            )""",

        "dragon_tiger_seats": """
            CREATE TABLE IF NOT EXISTS dragon_tiger_seats (
                symbol      VARCHAR NOT NULL,
                date        DATE NOT NULL,
                seat_name   VARCHAR NOT NULL,
                buy_amount  DOUBLE,
                sell_amount DOUBLE,
                net_amount  DOUBLE,
                buy_ratio   DOUBLE,
                sell_ratio  DOUBLE,
                reason      VARCHAR,
                side        VARCHAR NOT NULL,
                PRIMARY KEY (symbol, date, seat_name, side)
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
        "indicator_values": "date",
        "announcements": "date",
        "research_reports": "publish_date",
        "eps_consensus": "year",
        "shareholder_changes": "announce_date",
        "dragon_tiger_seats": "date",
    }

    def __init__(
        self,
        db_path: Optional[str] = None,
        *,
        ensure_schema: bool = True,
        read_only: bool = False,
    ):
        self.db_path = str(Path(db_path or _DEFAULT_DB_PATH).expanduser().resolve())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._read_only = read_only
        if ensure_schema and not read_only:
            self.ensure_schema()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path), read_only=self._read_only)
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

        # Migration v3: add list_date to stock_universe
        try:
            self.conn.execute(
                "ALTER TABLE stock_universe ADD COLUMN list_date DATE"
            )
            logger.info("Migration v3: added list_date column to stock_universe")
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

        # Migration v4: add freq column to indicator_values + update PK
        try:
            has_freq = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = 'indicator_values' AND column_name = 'freq'"
            ).fetchone()[0]
            if not has_freq:
                logger.info("Migration v4: rebuilding indicator_values with freq column")
                self.conn.execute("ALTER TABLE indicator_values RENAME TO indicator_values_old")
                # Re-build table with new schema (freq + updated PK)
                self.conn.execute(self.TABLES["indicator_values"])
                old_cols = [r[0] for r in self.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'indicator_values_old'"
                ).fetchall()]
                old_col_str = ", ".join(old_cols)
                self.conn.execute(
                    f"INSERT INTO indicator_values ({old_col_str}, freq) "
                    f"SELECT {old_col_str}, 'D' FROM indicator_values_old"
                )
                self.conn.execute("DROP TABLE indicator_values_old")
                logger.info("Migration v4: indicator_values rebuilt with freq column")
        except Exception as e:
            if "does not exist" in str(e).lower():
                pass  # table doesn't exist yet — CREATE TABLE IF NOT EXISTS will handle it
            else:
                raise

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

    def append_dataframe(self, table: str, df: pd.DataFrame) -> int:
        """追加写入（用于新闻/快讯等追加型数据），按主键去重。"""
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
