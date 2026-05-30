"""Fetcher: board_daily — 板块涨跌排名.

Source: easy_tdx MacClient.get_board_list (TCP, MAC 协议)
板块指数自带价格和领涨股，无需遍历成分股。
Schedule: daily
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from easy_tdx.mac.client import MacClient
from easy_tdx.mac.enums import BoardType

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)

MAC_HOST = "121.36.248.138"


def _parse_boards(board_type: BoardType, type_name: str) -> pd.DataFrame:
    """Fetch board list with index prices from easy_tdx MacClient."""
    with MacClient(MAC_HOST, timeout=15) as client:
        boards_df = client.get_board_list(board_type)
        if boards_df.empty:
            logger.warning("board_daily: no %s boards", type_name)
            return pd.DataFrame()

        rows = []
        for i, (_, board) in enumerate(boards_df.iterrows()):
            price = board.get("price", 0) or 0
            pre_close = board.get("pre_close", 0) or 0
            change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close else 0

            rows.append({
                "board_name": board.get("name", ""),
                "board_type": type_name,
                "change_pct": change_pct,
                "rank": i + 1,
                "total_mv": None,
                "turnover_rate": None,
                "up_count": None,
                "down_count": None,
                "leader_name": board.get("symbol_name", ""),
                "leader_pct": None,
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        logger.info("  %s: %d boards", type_name, len(df))
        return df


@register_fetcher(
    "board_daily",
    group="signals",
    description="板块涨跌排名 — easy_tdx MacClient get_board_list",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily board rankings."""
    total = 0

    df_ind = _parse_boards(BoardType.HY, "industry")
    if not df_ind.empty:
        df_ind["date"] = trade_date
        total += db.upsert_dataframe("board_daily", df_ind)
        logger.info("board_daily: industry → %d rows", len(df_ind))

    df_con = _parse_boards(BoardType.GN, "concept")
    if not df_con.empty:
        df_con["date"] = trade_date
        total += db.upsert_dataframe("board_daily", df_con)
        logger.info("board_daily: concept → %d rows", len(df_con))

    if total == 0:
        logger.warning("board_daily: no data")

    return total
