"""Fetcher registry, type aliases, and shared utilities.

Every fetcher module under ``fetchers/`` exports a ``fetch()`` function
with the signature defined by ``FetcherFn``, then registers itself via
``register_fetcher()`` at module level.

Usage::

    from src.ingestion.fetchers import FETCHER_REGISTRY, run_fetcher

    for name, (fn, deps, group, desc) in FETCHER_REGISTRY.items():
        count = await run_fetcher(db, config, trade_date, name)
"""
from __future__ import annotations

import functools
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from typing import Callable

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB

logger = logging.getLogger(__name__)

# Type alias for all fetcher functions
FetcherFn = Callable[["IngestionDB", "Config", date], int]


@dataclass
class FetcherEntry:
    fn: FetcherFn
    depends_on: list[str] = None       # fetchers that must run first
    group: str = "core"                # core / signals / auxiliary / lowfreq
    description: str = ""
    enabled: bool = True               # toggled by config


# Global registry — ordered by insertion (which respects dependency order)
FETCHER_REGISTRY: OrderedDict[str, FetcherEntry] = OrderedDict()


def register_fetcher(
    name: str,
    *,
    depends_on: list[str] = None,
    group: str = "core",
    description: str = "",
) -> Callable[[FetcherFn], FetcherFn]:
    """Decorator that registers a fetcher function in the global registry."""
    def wrapper(fn: FetcherFn) -> FetcherFn:
        FETCHER_REGISTRY[name] = FetcherEntry(
            fn=fn,
            depends_on=depends_on or [],
            group=group,
            description=description or fn.__doc__ or name,
        )
        return fn
    return wrapper


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def retry(max_attempts: int = 3, delay: float = 1.0):
    """Simple retry decorator with exponential backoff."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        sleep = delay * (2 ** (attempt - 1))
                        logger.warning(
                            "%s attempt %d/%d failed: %s. Retrying in %.0fs…",
                            fn.__name__, attempt, max_attempts, exc, sleep,
                        )
                        time.sleep(sleep)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


def timed(fn: FetcherFn) -> FetcherFn:
    """Decorator that logs elapsed time for a fetcher."""
    @functools.wraps(fn)
    def wrapper(db: IngestionDB, config: Config, trade_date: date) -> int:
        t0 = time.perf_counter()
        try:
            count = fn(db, config, trade_date)
            elapsed = time.perf_counter() - t0
            logger.info("  ✓ %s → %s rows (%.1fs)", fn.__name__, count, elapsed)
            return count
        except Exception as e:
            elapsed = time.perf_counter() - t0
            logger.error("  ✗ %s failed after %.1fs: %s", fn.__name__, elapsed, e)
            raise
    return wrapper


def run_fetcher(
    db: IngestionDB,
    config: Config,
    trade_date: date,
    name: str,
) -> int:
    """Run a single named fetcher with timing + error capture."""
    entry = FETCHER_REGISTRY.get(name)
    if entry is None:
        raise KeyError(f"Unknown fetcher: {name}")
    if not entry.enabled:
        logger.debug("Skipping %s (disabled)", name)
        return 0
    return timed(entry.fn)(db, config, trade_date)
