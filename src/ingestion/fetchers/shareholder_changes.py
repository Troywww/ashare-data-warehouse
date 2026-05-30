"""大股东增减持数据 — akshare 同花顺源

Pipeline 低频率拉取（周频），逐只股票拉取后写入。
同时有 on-demand 版本在 service/fetchers/holders.py 中按需拉取。
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher

logger = logging.getLogger(__name__)

_MAX_STOCKS_PER_RUN = 200

# 惰性导入 akshare
_ak = None


def _get_ak():
    global _ak
    if _ak is None:
        try:
            import akshare as ak
            _ak = ak
        except ImportError:
            return None
    return _ak


@register_fetcher(
    "shareholder_changes",
    depends_on=["stock_universe"],
    group="signals",
    description="大股东增减持（akshare 同花顺源）",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch shareholder changes for recently active stocks.

    每次 pipeline 最多处理 _MAX_STOCKS_PER_RUN 只股票。
    完整的按需拉取由 service/fetchers/holders.py 提供。
    """
    ak = _get_ak()
    if ak is None:
        logger.warning("akshare not installed, skipping shareholder_changes")
        return 0

    stocks = db.conn.execute("""
        SELECT symbol FROM stock_universe LIMIT ?
    """, [_MAX_STOCKS_PER_RUN]).fetchdf()

    if stocks.empty:
        return 0

    total_rows = 0
    for symbol in stocks["symbol"]:
        try:
            df = ak.stock_shareholder_change_ths(symbol)
            if df is not None and not df.empty:
                rows = []
                for _, row in df.iterrows():
                    rows.append({
                        "symbol": symbol,
                        "announce_date": row.iloc[0] if len(row) > 0 else None,
                        "change_date": None,
                        "shareholder": str(row.iloc[1]) if len(row) > 1 else "",
                        "change_type": str(row.iloc[2]) if len(row) > 2 else "",
                        "change_vol": _parse_vol(row.iloc[3]) if len(row) > 3 else None,
                        "hold_vol": _parse_vol(row.iloc[4]) if len(row) > 4 else None,
                        "change_ratio": None,
                        "hold_ratio": None,
                    })
                if rows:
                    df_out = pd.DataFrame(rows)
                    db.upsert_dataframe("shareholder_changes", df_out)
                    total_rows += len(rows)
        except Exception as e:
            logger.debug("shareholder_changes skip %s: %s", symbol, e)

    logger.info("shareholder_changes: %d rows (%d stocks checked)", total_rows, len(stocks))
    return total_rows


def _parse_vol(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").replace("亿", "").replace("万", ""))
    except (ValueError, TypeError):
        return None
