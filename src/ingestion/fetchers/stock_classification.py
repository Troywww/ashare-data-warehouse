"""Fetcher: stock_classification — 行业/地域索引.

Source: opentdx stock_board_list (HY/DQ) + stock_board_members
Schedule: daily, full replace
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


def _fetch_board_data(board_type: BOARD_TYPE, type_name: str) -> pd.DataFrame:
    """Fetch board members for all boards of a given type."""
    with TdxClient() as client:
        boards = client.stock_board_list(board_type)
        if not boards:
            logger.warning("stock_classification: no %s boards", type_name)
            return pd.DataFrame()

        rows = []
        for board in boards:
            board_code = board.get("code", "")
            board_name = board.get("name", "")

            try:
                members = client.stock_board_members(board_code, count=5000)
                if not members:
                    continue
                for m in members:
                    code = str(m.get("code", ""))
                    if len(code) != 6:
                        continue
                    rows.append({
                        "symbol": code,
                        type_name: board_name,
                    })
            except Exception as e:
                logger.debug("board %s (%s): %s", board_code, board_name, e)
                continue

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        logger.info("  %s: %d boards, %d mappings", type_name, len(boards), len(df))
        return df


@retry(max_attempts=2, delay=2.0)
def _fetch_classification() -> pd.DataFrame:
    """Fetch industry (HY) + region (DQ) classification."""
    df_hy = _fetch_board_data(BOARD_TYPE.HY, "industry")
    df_dq = _fetch_board_data(BOARD_TYPE.DQ, "region")

    if df_hy.empty and df_dq.empty:
        logger.warning("classification: empty result from opentdx")
        return pd.DataFrame(columns=["symbol", "industry", "region"])

    merged = pd.merge(
        df_hy if not df_hy.empty else pd.DataFrame(columns=["symbol"]),
        df_dq if not df_dq.empty else pd.DataFrame(columns=["symbol"]),
        on="symbol", how="outer",
    )
    merged = merged.drop_duplicates(subset=["symbol"])
    logger.info("classification: %d stocks total", len(merged))
    return merged


@register_fetcher(
    "stock_classification",
    depends_on=["stock_universe"],
    group="core",
    description="行业/地域索引 — opentdx HY+DQ 全量覆盖",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist stock classification (daily full replace)."""
    df = _fetch_classification()
    if df.empty:
        return 0
    return db.upsert_dataframe("stock_classification", df)
