"""Scheduler — periodic daily update runner.

Each fetcher/group is scheduled independently per config.
If a run takes longer than its interval, the next run is skipped.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date

import schedule as sched_lib

from src.ingestion.config import Config, load_config
from src.ingestion.engine import DailyUpdateEngine

logger = logging.getLogger(__name__)

_running: set[str] = set()
_running_lock = threading.Lock()


def _run_fetcher_group(engine: DailyUpdateEngine, name: str):
    """Run a scheduled fetcher or group — thread-safe, no overlap."""
    if name in engine.config.schedule_groups:
        targets = engine.config.schedule_groups[name]
    else:
        targets = [name]

    with _running_lock:
        already = [t for t in targets if t in _running]
        if already:
            logger.warning("Skipped %s — %s still running", name, already)
            return
        for t in targets:
            _running.add(t)

    try:
        logger.info("=== Scheduled: %s ===", name)
        engine.run_daily_update(tables=targets)
    finally:
        with _running_lock:
            for t in targets:
                _running.discard(t)


def _parse_time(time_str: str) -> tuple[str, str] | None:
    """Parse schedule time string → (type, hh:mm)."""
    time_str = time_str.strip()
    parts = time_str.split()
    if len(parts) == 1:
        return "daily", parts[0]
    if len(parts) == 2 and parts[0] in ("monthly", "quarterly", "yearly"):
        return parts[0], parts[1]
    logger.warning("Invalid schedule: %s", time_str)
    return None


def start_scheduler(config: Config):
    """Start the background scheduler."""
    engine = DailyUpdateEngine(config)
    registered = 0

    for name, time_str in config.schedule.iter_items():
        parsed = _parse_time(time_str)
        if parsed is None:
            continue
        sched_type, hhmm = parsed

        if sched_type == "daily":
            sched_lib.every().day.at(hhmm).do(_run_fetcher_group, engine, name)
            registered += 1
        elif sched_type == "monthly":
            sched_lib.every().day.at(hhmm).do(_monthly_check, engine, name)
            registered += 1
        elif sched_type == "quarterly":
            sched_lib.every().day.at(hhmm).do(_quarterly_check, engine, name)
            registered += 1
        elif sched_type == "yearly":
            sched_lib.every().day.at(hhmm).do(_yearly_check, engine, name)
            registered += 1

    logger.info("Scheduler started — %d jobs", registered)

    # Main scheduling loop
    logger.info("Entering main loop...")
    while True:
        sched_lib.run_pending()
        time.sleep(30)


def _monthly_check(engine: DailyUpdateEngine, name: str):
    if date.today().day == 1:
        _run_fetcher_group(engine, name)


_quarterly_done: set[str] = set()


def _quarterly_check(engine: DailyUpdateEngine, name: str):
    if date.today().month in (3, 6, 9, 12) and name not in _quarterly_done:
        _quarterly_done.add(name)
        _run_fetcher_group(engine, name)


_yearly_done: set[str] = set()


def _yearly_check(engine: DailyUpdateEngine, name: str):
    if date.today().month == 12 and date.today().day == 31 and name not in _yearly_done:
        _yearly_done.add(name)
        _run_fetcher_group(engine, name)


def run_once(config: Config):
    DailyUpdateEngine(config).run_daily_update()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default="config.yaml")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    cfg = load_config(args.config)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_once(cfg) if args.once else start_scheduler(cfg)
