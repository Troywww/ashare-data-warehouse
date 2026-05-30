"""Configuration system — YAML + env var override.

Priority (high → low):
  1. Environment variable INGESTION_*
  2. YAML config file
  3. Default values

Usage:
    from src.ingestion.config import load_config
    cfg = load_config("config.yaml")
    cfg.db_path          # "/app/data/stock_research.duckdb"
    cfg.schedule.core    # "16:00"
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

# ---------------------------------------------------------------------------
# Config data classes
# ---------------------------------------------------------------------------





@dataclass
class ScheduleConfig:
    """Per-table/group schedule config.

    Config format::
        schedule:
          core: "16:00"             # group → all fetchers in that group
          daily_ohlcv: "15:30"     # individual fetcher override
          holder_count: "monthly 10:00"  # special: monthly/quarterly/yearly
    """
    data: dict[str, str] = field(default_factory=lambda: {
        "core": "16:00",
        "signals": "17:00",
        "global_markets": "09:00",
        "weekly": "weekly 10:00",
        "trade_calendar": "yearly 10:00",
        "holder_count": "monthly 10:00",
        "fundamentals": "monthly 10:00",
    })
    schedule_groups: dict[str, list[str]] = field(default_factory=dict)

    def get_time(self, fetcher_name: str) -> str | None:
        """Get scheduled time for a fetcher.
        Returns None if no schedule found (won't run automatically).
        """
        # Direct match
        if fetcher_name in self.data:
            return self.data[fetcher_name]
        # Check groups
        for group_name, members in self.schedule_groups.items():
            if fetcher_name in members and group_name in self.data:
                return self.data[group_name]
        return None

    @property
    def core(self) -> str:
        return self.data.get("core", "16:00")

    @property
    def signals(self) -> str:
        return self.data.get("signals", "17:00")

    def iter_items(self) -> list[tuple[str, str]]:
        """Iterate over (fetcher_name_or_group, time_str) pairs."""
        return list(self.data.items())


@dataclass
class SourceToggles:
    easy_tdx: bool = True
    baostock: bool = True
    tencent_api: bool = True
    eastmoney: bool = True
    akshare: bool = True


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: Optional[str] = None  # None = stdout only (Docker default)


@dataclass
class Config:
    # Paths
    db_path: str = "./data/ingestion/stock_research.duckdb"
    data_dir: str = "./data/ingestion"

    # Schedules
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)

    # Schedule groups — fetcher grouping for batch scheduling
    schedule_groups: dict[str, list[str]] = field(default_factory=lambda: {
        "core": [
            "stock_universe",
            "xdxr_events", "daily_ohlcv", "daily_valuation",
            "capital_flow", "northbound_flow", "board_daily",
        ],
        "signals": [
            "dragon_tiger", "hot_stocks", "hot_reasons", "margin_trading",
            "block_trades", "lockup_calendar", "indicator_values",
        ],
        "weekly": [
            "stock_classification", "concept_blocks",
        ],
    })

    # Performance
    thread_pool: int = 8

    # Data source toggles
    sources: SourceToggles = field(default_factory=SourceToggles)

    # Logging
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    # DuckDB connection tuning
    duckdb_memory_limit: str = "2GB"
    duckdb_threads: int = 4


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

_ENV_PREFIX = "INGESTION_"


def _env_key(*parts: str) -> str:
    """Build env var key: INGESTION_SCHEDULE_CORE, INGESTION_DB_PATH, etc."""
    return _ENV_PREFIX + "_".join(parts).upper()


def _scalar_overrides(cls, prefix: tuple[str, ...]) -> dict:
    """Read flat env vars that override leaf fields of a dataclass.

    Example: INGESTION_SCHEDULE_CORE="15:30" → {"core": "15:30"}
    """
    overrides = {}
    for fld in cls.__dataclass_fields__:  # type: ignore[attr-defined]
        val = os.environ.get(_env_key(*prefix, fld))
        if val is not None:
            overrides[fld] = val
    return overrides


def _deep_merge(base: dict, overrides: dict) -> dict:
    """Recursive dict merge — overrides win."""
    merged = dict(base)
    for k, v in overrides.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def load_config(yaml_path: Optional[str] = None) -> Config:
    """Load Config from YAML file, then apply env var overrides."""
    raw: dict = {}

    # 1) Load YAML
    if yaml_path:
        path = Path(yaml_path)
        if path.exists():
            with open(path, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        else:
            import logging
            logging.getLogger(__name__).warning("Config file %s not found, using defaults", yaml_path)

    # 2) Env var overrides at top level
    raw.update(_scalar_overrides(Config, ()))

    # 3) Nested overrides
    for section_key, section_cls in [
        ("sources", SourceToggles),
        ("logging", LoggingConfig),
    ]:
        section_raw = raw.get(section_key, {})
        section_raw.update(_scalar_overrides(section_cls, (section_key,)))
        raw[section_key] = section_raw

    # Schedule: wrap flat keys into data dict, apply env overrides
    sched_raw = raw.get("schedule", {})
    if isinstance(sched_raw, dict) and "data" not in sched_raw:
        sched_raw = {"data": sched_raw}
    # Env: INGESTION_SCHEDULE_DAILY_OHLCV="15:00"
    for key in list(os.environ.keys()):
        if key.startswith("INGESTION_SCHEDULE_") and key != "INGESTION_SCHEDULE":
            fetcher_name = key[len("INGESTION_SCHEDULE_"):].lower()
            sched_raw.setdefault("data", {})[fetcher_name] = os.environ[key]
    raw["schedule"] = sched_raw

    # 4) Construct Config from merged dict
    cfg = Config(**{k: v for k, v in raw.items() if k in Config.__dataclass_fields__})

    # Reconstruct nested dataclasses
    for key, cls in [("schedule", ScheduleConfig), ("sources", SourceToggles), ("logging", LoggingConfig)]:
        if key in raw and isinstance(raw[key], dict):
            if key == "schedule":
                sched_dict = dict(raw[key])
                sched_dict.setdefault("schedule_groups", raw.get("schedule_groups", {}))
                setattr(cfg, key, cls(**sched_dict))
            else:
                setattr(cfg, key, cls(**raw[key]))

    return cfg
