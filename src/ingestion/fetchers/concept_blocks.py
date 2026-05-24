"""Fetcher: concept_blocks — 概念板块索引.

Source: opentdx stock_board_list(GN) + stock_board_members
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


@retry(max_attempts=2, delay=2.0)
def _fetch_concept_blocks() -> pd.DataFrame:
    """Fetch concept blocks (GN) with N:N stock mapping."""
    with TdxClient() as client:
        boards = client.stock_board_list(BOARD_TYPE.GN, count=500)
        if not boards:
            logger.warning("concept_blocks: no GN boards from opentdx")
            return pd.DataFrame(columns=["symbol", "concept_name", "board_code"])

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
                        "concept_name": board_name,
                        "board_code": board_code,
                    })
            except Exception as e:
                logger.debug("concept board %s (%s): %s", board_code, board_name, e)
                continue

        df = pd.DataFrame(rows)
        if df.empty:
            logger.warning("concept_blocks: empty result")
            return df

        logger.info(
            "concept_blocks: %d mappings (%d boards, %d stocks)",
            len(df), df["board_code"].nunique(), df["symbol"].nunique(),
        )
        return df


@register_fetcher(
    "concept_blocks",
    depends_on=["stock_universe"],
    group="core",
    description="概念板块索引 — opentdx GN 概念板块 N:N 关联",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Fetch and persist concept blocks (daily full replace)."""
    df = _fetch_concept_blocks()
    if df.empty:
        return 0
    return db.upsert_dataframe("concept_blocks", df)
