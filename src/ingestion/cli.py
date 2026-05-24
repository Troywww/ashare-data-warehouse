"""Command-line interface for the ingestion pipeline.

Usage::

    ingestion daily-update          # Run today's incremental update
    ingestion backfill              # Full history backfill
    ingestion status                # Show table row counts
    ingestion schedule              # Start the scheduler (Docker default)
"""
from __future__ import annotations

import argparse
import logging
import sys

from src.ingestion.config import load_config
from src.ingestion.engine import DailyUpdateEngine

logger = logging.getLogger(__name__)


def _setup_logging(level: str = "INFO", log_file: str | None = None):
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%H:%M:%S"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=fmt, datefmt=datefmt, handlers=handlers)


def cmd_daily_update(args):
    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)
    engine = DailyUpdateEngine(cfg)
    tables = args.tables.split(",") if args.tables else None
    results = engine.run_daily_update(tables=tables)
    _print_results(results)
    # Exit code: 0 if all ok, 1 if any failed
    if any(r.error for r in results):
        sys.exit(1)


def cmd_backfill(args):
    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)
    engine = DailyUpdateEngine(cfg)
    tables = args.tables.split(",") if args.tables else None
    results = engine.run_backfill(tables=tables)
    _print_results(results)
    if any(r.error for r in results):
        sys.exit(1)


def cmd_status(args):
    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)
    engine = DailyUpdateEngine(cfg)
    stats = engine.status()
    print(f"{'Table':<25} {'Rows':>10}")
    print("-" * 37)
    for name, count in sorted(stats.items()):
        print(f"{name:<25} {count:>10,}")


def cmd_schedule(args):
    """Start the background scheduler with per-fetcher timing."""
    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)
    from src.ingestion.scheduler import start_scheduler
    start_scheduler(cfg)


def cmd_serve(args):
    """Start the web control panel."""
    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)
    from src.ingestion.web import create_app
    app = create_app()
    app.config["CONFIG_PATH"] = args.config
    host = args.host or "0.0.0.0"
    port = int(args.port or 5000)
    print(f"Web 控制面板启动: http://{host}:{port}")
    app.run(host=host, port=port, debug=args.debug)


def _print_results(results):
    print(f"\n{'Fetcher':<25} {'Rows':>8} {'Time':>8} {'Status'}")
    print("-" * 50)
    for r in results:
        if r.skipped:
            status = "⏭ skipped"
        elif r.error:
            status = f"✗ {r.error}"
        else:
            status = "✓"
        print(f"{r.name:<25} {r.rows:>8,} {r.elapsed:>7.1f}s {status}")
    print()


def main():
    parser = argparse.ArgumentParser(description="A股数据仓库 — 数据摄入工具")
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Config file path (default: config.yaml)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    daily_parser = sub.add_parser("daily-update", help="Run today's incremental update")
    daily_parser.add_argument(
        "--tables", "-t", default=None,
        help="Comma-separated table names, e.g. daily_ohlcv,daily_valuation. Default: all",
    )
    backfill_parser = sub.add_parser("backfill", help="Full history backfill")
    backfill_parser.add_argument(
        "--tables", "-t", default=None,
        help="Comma-separated table names to backfill, e.g. daily_ohlcv,daily_valuation. Default: all",
    )
    sub.add_parser("status", help="Show table row counts")
    sub.add_parser("schedule", help="Start scheduled runner (Docker default)")
    serve_parser = sub.add_parser("serve", help="Start web control panel")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Listen host (default: 0.0.0.0)")
    serve_parser.add_argument("--port", default=5000, type=int, help="Listen port (default: 5000)")
    serve_parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")

    args = parser.parse_args()

    dispatch = {
        "daily-update": cmd_daily_update,
        "backfill": cmd_backfill,
        "status": cmd_status,
        "schedule": cmd_schedule,
        "serve": cmd_serve,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
