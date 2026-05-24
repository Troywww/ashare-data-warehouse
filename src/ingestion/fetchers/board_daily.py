"""Fetcher: board_daily — 板块涨跌排名.

Source: opentdx stock_board_list (TCP, 通达信)
板块指数自带价格和领涨股，无需遍历成分股。
Schedule: daily 14:30（盘中有实时数据）
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from opentdx.const import BOARD_TYPE
from opentdx.tdxClient import TdxClient

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher, retry

logger = logging.getLogger(__name__)


def _parse_boards(board_type: BOARD_TYPE, type_name: str) -> pd.DataFrame:
    """Fetch board list with index prices from opentdx.

    stock_board_list returns per-board:
      name, price(板块指数), pre_close, 
      symbol_name(领涨股), symbol_price(领涨股价)
    """
    with TdxClient() as client:
        boards = client.stock_board_list(board_type)
        if not boards:
            logger.warning("board_daily: no %s boards", type_name)
            return pd.DataFrame()

        rows = []
        for i, board in enumerate(boards):
            price = board.get("price", 0) or 0
            pre_close = board.get("pre_close", 0) or 0

            # Calculate change_pct from board index price
            change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close else 0

            rows.append({
                "board_name": board.get("name", ""),
                "board_type": type_name,
                "change_pct": change_pct,
                "rank": i + 1,
                "total_mv": None,  # not available from this API
                "turnover_rate": None,
                "up_count": None,  # not available from this API
                "down_count": None,
                "leader_name": board.get("symbol_name", ""),
                "leader_pct": None,  # need member data for precise leader change
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        # 按涨跌幅排序，重新编号
        df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        logger.info("  %s: %d boards", type_name, len(df))
        return df


@register_fetcher(
    "board_daily",
    group="signals",
    description="板块涨跌排名 — opentdx stock_board_list 板块指数价格",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist daily board rankings.

    使用 opentdx stock_board_list 获取板块指数价格和前收盘价，
    计算涨跌幅。单次 API 调用，极快。
    """
    total = 0

    df_ind = _parse_boards(BOARD_TYPE.HY, "industry")
    if not df_ind.empty:
        df_ind["date"] = trade_date
        total += db.upsert_dataframe("board_daily", df_ind)
        logger.info("board_daily: industry → %d rows", len(df_ind))

    df_con = _parse_boards(BOARD_TYPE.GN, "concept")
    if not df_con.empty:
        df_con["date"] = trade_date
        total += db.upsert_dataframe("board_daily", df_con)
        logger.info("board_daily: concept → %d rows", len(df_con))

    if total == 0:
        logger.warning("board_daily: no data")

    return total
