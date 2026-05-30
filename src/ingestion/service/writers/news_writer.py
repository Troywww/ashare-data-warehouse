"""Writers for append-only data — persist fetched news/telegrams to DuckDB."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.ingestion.db import IngestionDB

logger = logging.getLogger(__name__)


def persist_cls_telegram(db: IngestionDB, items: list[dict[str, Any]]) -> int:
    """Write CLS telegraph items to DuckDB (append, dedup by id)."""
    if not items:
        return 0
    df = pd.DataFrame(items)
    if "id" not in df.columns:
        logger.warning("CLS telegram items missing 'id' field, skipping persist")
        return 0
    return db.append_dataframe("cls_telegram", df)


def persist_stock_news(db: IngestionDB, items: list[dict[str, Any]]) -> int:
    """Write stock news items to DuckDB (append, dedup by symbol+id)."""
    if not items:
        return 0
    df = pd.DataFrame(items)
    if "id" not in df.columns or "symbol" not in df.columns:
        logger.warning("Stock news items missing required fields, skipping persist")
        return 0
    return db.append_dataframe("stock_news", df)


def persist_announcements(db: IngestionDB, items: list[dict[str, Any]]) -> int:
    """Write announcement items to DuckDB."""
    if not items:
        return 0
    df = pd.DataFrame(items)
    return db.append_dataframe("announcements", df)
