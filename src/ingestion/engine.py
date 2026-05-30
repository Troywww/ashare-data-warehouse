"""Daily update engine — orchestrates all fetchers in dependency-ordered waves.

Wave 0: Independent fetchers (no deps) — run in parallel
Wave 1: Depends on stock_universe — run after Wave 0, independent items parallel
Wave 2: All remaining independent fetchers — run in parallel
"""
from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Optional

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.em_auth import patch_requests_session
from src.ingestion.fetchers import FETCHER_REGISTRY, FetcherEntry, run_fetcher

# Apply EastMoney NID auth patch globally
patch_requests_session()

# Auto-discover and register all fetcher modules
import importlib
import os as _os
_fetcher_dir = _os.path.join(_os.path.dirname(__file__), "fetchers")
for _f in sorted(_os.listdir(_fetcher_dir)):
    if _f.endswith(".py") and _f != "__init__.py":
        importlib.import_module(f"src.ingestion.fetchers.{_f[:-3]}")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dependency graph — execution waves
# ---------------------------------------------------------------------------

# Wave 0: No dependencies — can start immediately (parallel)
_WAVE_0 = [
    "stock_universe",
    "trade_calendar",
    "global_markets",
    "northbound_flow",
    "board_daily",
    "dragon_tiger",
    "hot_stocks",
    "hot_reasons",
    "margin_trading",
    "block_trades",
    "lockup_calendar",
    "holder_count",
    "fundamentals",
]

# Wave 1: xdxr → ohlcv chain (sequential — ohlcv adjusts prices with xdxr data)
# Both depend on stock_universe which is guaranteed done after Wave 0
_WAVE_1_SEQ = [
    "xdxr_events",
    "daily_ohlcv",
]

# Wave 2: Depends on Wave 0+1 — parallel
#   stock_universe → daily_valuation, capital_flow, stock_classification,
#                    concept_blocks, shareholder_changes
#   daily_ohlcv   → indicator_values
_WAVE_2 = [
    "daily_valuation",
    "capital_flow",
    "stock_classification",
    "concept_blocks",
    "indicator_values",
]
# Note: shareholder_changes excluded — hard 200-stock limit, use DataService on-demand.

# Wave 3: Reserved for future post-processing.
_WAVE_3: list[str] = []

# All fetchers in execution order (for display / backfill compatibility)
_FETCHER_ORDER = _WAVE_0 + _WAVE_1_SEQ + _WAVE_2 + _WAVE_3

# Map fetcher registry name -> config source toggle
_FETCHER_SOURCE_MAP = {
    "trade_calendar": "baostock",
    "stock_universe": "easy_tdx",
    "stock_classification": "easy_tdx",
    "concept_blocks": "easy_tdx",
    "daily_ohlcv": "easy_tdx",
    "daily_valuation": "tencent_api",
    "capital_flow": "easy_tdx",
    "northbound_flow": "eastmoney",
    "dragon_tiger": "akshare",
    "board_daily": "easy_tdx",
    "hot_stocks": "akshare",
    "hot_reasons": "ths",
    "margin_trading": "akshare",
    "block_trades": "eastmoney",
    "lockup_calendar": "eastmoney",
    "holder_count": "eastmoney",
    "fundamentals": "akshare",
    "global_markets": "easy_tdx",
    "xdxr_events": "eastmoney",
    "indicator_values": "easy_tdx",
    "shareholder_changes": "akshare",
}

# Max parallel workers for network fetch
_MAX_PARALLEL = 4


@dataclass
class FetcherResult:
    name: str
    rows: int
    elapsed: float
    error: Optional[str] = None
    skipped: bool = False


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class DailyUpdateEngine:
    """Orchestrates the daily data ingestion pipeline.

    Usage::

        engine = DailyUpdateEngine(config)
        results = engine.run_daily_update(trade_date)
        for r in results:
            print(f"{r.name}: {r.rows} rows ({r.elapsed:.1f}s)")
    """

    def __init__(self, config: Config):
        self.config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_daily_update(self, trade_date: Optional[date] = None, backfill: bool = False,
                         tables: list[str] | None = None,
                         progress_callback: Callable[[FetcherResult], None] | None = None,
                         ) -> list[FetcherResult]:
        """Run the full daily update pipeline.

        Parameters
        ----------
        trade_date : date, optional
            Trading date to fetch data for. Defaults to today.
        backfill : bool
            If True, fetch full history instead of incremental.
        tables : list[str], optional
            Only run these fetchers (by registry name). None = all.
        progress_callback : callable, optional
            Called after each fetcher completes with its FetcherResult.
            Useful for live progress reporting in web UIs.

        Returns
        -------
        list[FetcherResult]
            Results for each fetcher in execution order.
        """
        trade_date = trade_date or date.today()
        results: list[FetcherResult] = []

        logger.info(
            "=== Daily update %s — %d fetchers (parallel) ===",
            trade_date.isoformat(), len(_FETCHER_ORDER),
        )
        t_start = time.perf_counter()

        # Ensure all tables exist before parallel execution (avoids write-write conflict)
        with IngestionDB(self.config.db_path) as db:
            pass  # ensure_schema runs in __init__

        # Set backfill flag on config for fetchers to check
        self.config._backfill = backfill

        if tables:
            # --- Targeted mode: run only specified tables in parallel ---
            # Used by scheduler per-table triggers and frontend manual runs.
            # Dependencies are assumed to be already satisfied in DB from previous runs.
            logger.info("--- Targeted: %s ---", ", ".join(tables))
            targeted = self._run_wave(tables, trade_date, parallel=True,
                                      progress_callback=progress_callback)
            results.extend(targeted)
        else:
            # --- Full mode: wave-based execution respecting dependency order ---
            # --- Wave 0: Independent fetchers (parallel) ---
            logger.info("--- Wave 0: %s ---", ", ".join(_WAVE_0))
            wave0_results = self._run_wave(_WAVE_0, trade_date,
                                           progress_callback=progress_callback)
            results.extend(wave0_results)

            # --- Wave 1: Depends on stock_universe (sequential) ---
            logger.info("--- Wave 1: %s ---", ", ".join(_WAVE_1_SEQ))
            wave1_results = self._run_wave(_WAVE_1_SEQ, trade_date, parallel=False,
                                           progress_callback=progress_callback)
            results.extend(wave1_results)

            # --- Wave 2: Independent signal/low-freq (parallel) ---
            logger.info("--- Wave 2: %s ---", ", ".join(_WAVE_2))
            wave2_results = self._run_wave(_WAVE_2, trade_date,
                                           progress_callback=progress_callback)
            results.extend(wave2_results)

            # --- Wave 3: Pre-compute indicators (after data ready) ---
            if not backfill:
                logger.info("--- Wave 3: %s ---", ", ".join(_WAVE_3))
                wave3_results = self._run_wave(_WAVE_3, trade_date, parallel=False,
                                               progress_callback=progress_callback)
                results.extend(wave3_results)

                # After data ingestion, clear DataService cache so agents get fresh data
                self._clear_data_service_cache()

        total_elapsed = time.perf_counter() - t_start
        ok = sum(1 for r in results if r.error is None and not r.skipped)
        failed = sum(1 for r in results if r.error is not None)
        skipped = sum(1 for r in results if r.skipped)
        total_rows = sum(r.rows for r in results)

        logger.info(
            "=== Done — %d ok, %d failed, %d skipped — %d rows in %.1fs ===",
            ok, failed, skipped, total_rows, total_elapsed,
        )

        # Persist progress to .progress.json
        self._save_progress(results, trade_date)

        return results

    def run_backfill(self, tables: list[str] | None = None,
                     progress_callback: Callable[[FetcherResult], None] | None = None,
                     ) -> list[FetcherResult]:
        """Run a full backfill for specified tables (or all if None)."""
        logger.info("=== Backfill start === tables=%s", tables or "all")
        return self.run_daily_update(backfill=True, tables=tables,
                                     progress_callback=progress_callback)

    def status(self) -> dict[str, int]:
        """Return row counts for all tables."""
        with IngestionDB(self.config.db_path) as db:
            return db.table_stats()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _run_wave(self, names: list[str], trade_date: date,
                  parallel: bool = True,
                  progress_callback: Callable[[FetcherResult], None] | None = None,
                  ) -> list[FetcherResult]:
        """Run a wave of fetchers — parallel or sequential."""
        # Filter and check
        tasks: list[tuple[str, FetcherEntry]] = []
        skipped: list[FetcherResult] = []

        for name in names:
            if name not in FETCHER_REGISTRY:
                logger.warning("Unknown fetcher: %s", name)
                r = FetcherResult(name=name, rows=0, elapsed=0, skipped=True)
                skipped.append(r)
                if progress_callback:
                    progress_callback(r)
                continue
            entry = FETCHER_REGISTRY[name]

            if not self._source_enabled(name):
                r = FetcherResult(name=name, rows=0, elapsed=0, skipped=True)
                skipped.append(r)
                if progress_callback:
                    progress_callback(r)
                continue

            tasks.append((name, entry))

        if not tasks:
            return skipped

        if parallel and len(tasks) > 1:
            results = self._run_parallel(tasks, trade_date,
                                         progress_callback=progress_callback)
        else:
            results = self._run_sequential(tasks, trade_date,
                                           progress_callback=progress_callback)

        return skipped + results

    def _run_sequential(self, tasks: list[tuple[str, FetcherEntry]],
                        trade_date: date,
                        progress_callback: Callable[[FetcherResult], None] | None = None,
                        ) -> list[FetcherResult]:
        """Run fetchers one by one."""
        results = []
        with IngestionDB(self.config.db_path) as db:
            for name, entry in tasks:
                t0 = time.perf_counter()
                try:
                    rows = run_fetcher(db, self.config, trade_date, name)
                    elapsed = time.perf_counter() - t0
                    r = FetcherResult(name=name, rows=rows, elapsed=elapsed)
                except Exception as e:
                    elapsed = time.perf_counter() - t0
                    logger.error("  x %s failed after %.1fs: %s", name, elapsed, e)
                    r = FetcherResult(name=name, rows=0, elapsed=elapsed, error=str(e))
                results.append(r)
                if progress_callback:
                    progress_callback(r)
        return results

    def _run_parallel(self, tasks: list[tuple[str, FetcherEntry]],
                      trade_date: date,
                      progress_callback: Callable[[FetcherResult], None] | None = None,
                      ) -> list[FetcherResult]:
        """Run fetchers in parallel — each gets its own DB connection for writing.

        DuckDB serializes writes internally (single-writer lock), so concurrent
        writes are safe — they just queue briefly. Network fetch (the real
        bottleneck) runs fully in parallel.
        """
        db_path = self.config.db_path
        results: list[FetcherResult] = [None] * len(tasks)  # type: ignore

        def _worker(idx: int, name: str) -> tuple[int, FetcherResult]:
            """Fetch data in thread, write to DB via own connection."""
            t0 = time.perf_counter()
            try:
                with IngestionDB(db_path, ensure_schema=False) as db:
                    rows = run_fetcher(db, self.config, trade_date, name)
                elapsed = time.perf_counter() - t0
                return idx, FetcherResult(name=name, rows=rows, elapsed=elapsed)
            except Exception as e:
                elapsed = time.perf_counter() - t0
                logger.error("  x %s failed after %.1fs: %s", name, elapsed, e)
                return idx, FetcherResult(name=name, rows=0, elapsed=elapsed, error=str(e))

        max_workers = min(_MAX_PARALLEL, len(tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_worker, i, name): i
                for i, (name, _entry) in enumerate(tasks)
            }
            for future in as_completed(futures):
                idx, result = future.result()
                results[idx] = result
                if progress_callback:
                    progress_callback(result)

        return results

    def _resolve_order(self) -> list[tuple[str, FetcherEntry]]:
        """Return (name, entry) in topological order (for compatibility)."""
        by_name = {n: FETCHER_REGISTRY[n] for n in _FETCHER_ORDER if n in FETCHER_REGISTRY}
        for name, entry in FETCHER_REGISTRY.items():
            if name not in by_name:
                by_name[name] = entry
        return [(name, by_name[name]) for name in _FETCHER_ORDER if name in by_name]

    def _source_enabled(self, fetcher_name: str) -> bool:
        """Check whether the fetcher's data source is enabled in config."""
        source = _FETCHER_SOURCE_MAP.get(fetcher_name, "")
        if not source:
            return True
        return getattr(self.config.sources, source, True)

    def _save_progress(self, results: list[FetcherResult], trade_date: date) -> None:
        """Save run results to .progress.json for incremental tracking."""
        progress_path = Path(self.config.data_dir) / ".progress.json"

        progress: dict = {}
        if progress_path.exists():
            try:
                with open(progress_path, encoding="utf-8") as f:
                    progress = json.load(f)
            except (json.JSONDecodeError, OSError):
                progress = {}

        table_progress = progress.setdefault("tables", {})
        for r in results:
            if r.skipped:
                continue
            entry: dict = {
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "rows": r.rows,
                "elapsed_sec": round(r.elapsed, 1),
            }
            if r.error:
                entry["error"] = r.error
            table_progress[r.name] = entry

        progress["last_daily_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        errors = [
            {"table": r.name, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "error": r.error}
            for r in results if r.error
        ]
        if errors:
            progress.setdefault("errors", []).extend(errors)

        progress_path.parent.mkdir(parents=True, exist_ok=True)
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)

        logger.debug("Progress saved to %s", progress_path)

    def _clear_data_service_cache(self) -> None:
        """Clear DataService cache after pipeline ingestion completes.

        Called after Wave 3 to ensure agents always see fresh data.
        """
        try:
            from src.ingestion.service import DataService
            svc = DataService(self.config.db_path)
            cleared = svc.invalidate_all()
            logger.info("DataService cache cleared: %d entries", cleared)
        except Exception as e:
            logger.debug("Cache clear skipped: %s", e)
