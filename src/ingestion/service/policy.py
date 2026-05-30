"""Cache policies — per-data-type TTL definitions with market-aware expiry.

Each policy defines:
- trading_ttl: TTL in seconds during trading hours (09:30-15:00)
- closed_ttl: TTL after market close (None = never expire until cache invalidation)
- source: which data source to fetch from
- persist: whether to write results to DuckDB
- check_db_first: whether to check DuckDB before fetching
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional


# Trading hours
TRADING_START = time(9, 30)
TRADING_END = time(15, 0)


def is_trading_time(now: datetime | None = None) -> bool:
    """Check if current time falls within A-share trading hours."""
    if now is None:
        now = datetime.now()
    t = now.time()
    # Monday to Friday
    if now.weekday() >= 5:
        return False
    return TRADING_START <= t <= TRADING_END


@dataclass
class CachePolicy:
    """Cache policy for a data type.

    Parameters
    ----------
    trading_ttl : int
        TTL in seconds during trading hours.
    closed_ttl : int | None
        TTL after market close. None = never expire.
    source : str
        Data source identifier ('easy_tdx', 'eastmoney', 'cls', 'ths', 'tencent', etc.)
    persist : bool
        Whether to persist fetched data to DuckDB.
    check_db_first : bool
        Whether to check DuckDB before fetching from source.
    compute_fn : str | None
        If set, this data is computed (not fetched), using the named compute function.
    """
    trading_ttl: int = 60
    closed_ttl: int | None = 300
    source: str = "easy_tdx"
    persist: bool = False
    check_db_first: bool = False
    compute_fn: str | None = None
    db_table: str | None = None
    db_dedup_keys: list[str] | None = None

    def current_ttl(self) -> int:
        """Return the effective TTL based on current market status."""
        if is_trading_time():
            return self.trading_ttl
        if self.closed_ttl is not None:
            return self.closed_ttl
        # closed_ttl=None → never expire: return a very large number
        return 999_999_999

    @property
    def never_expire(self) -> bool:
        return self.closed_ttl is None and not is_trading_time()


# ---------------------------------------------------------------------------
# All cache policies
# ---------------------------------------------------------------------------

POLICIES: dict[str, CachePolicy] = {
    # === 实时行情（C 类：纯缓存，不写库） ===
    "realtime_quote": CachePolicy(
        trading_ttl=3, closed_ttl=3600, source="easy_tdx",
    ),
    "realtime_quotes": CachePolicy(
        trading_ttl=3, closed_ttl=3600, source="easy_tdx",
    ),
    "intraday_kline_1min": CachePolicy(
        trading_ttl=30, closed_ttl=None, source="easy_tdx",
    ),
    "intraday_kline_5min": CachePolicy(
        trading_ttl=120, closed_ttl=None, source="easy_tdx",
    ),
    "tick_chart": CachePolicy(
        trading_ttl=30, closed_ttl=None, source="easy_tdx",
    ),
    "limit_up_ladder": CachePolicy(
        trading_ttl=30, closed_ttl=None, source="easy_tdx",
    ),
    "unusual": CachePolicy(
        trading_ttl=10, closed_ttl=None, source="easy_tdx",
    ),
    "capital_flow_minute": CachePolicy(
        trading_ttl=60, closed_ttl=None, source="eastmoney",
    ),
    "northbound_minute": CachePolicy(
        trading_ttl=60, closed_ttl=None, source="ths",
    ),
    "realtime_index": CachePolicy(
        trading_ttl=3, closed_ttl=3600, source="tencent",
    ),

    # === 追加拿取（B 类：写库 + 缓存最新） ===
    "cls_telegram": CachePolicy(
        trading_ttl=300, closed_ttl=1800, source="cls",
        persist=True, db_table="cls_telegram",
    ),
    "stock_news": CachePolicy(
        trading_ttl=300, closed_ttl=1800, source="eastmoney",
        persist=True, db_table="stock_news",
    ),
    "announcements": CachePolicy(
        trading_ttl=1800, closed_ttl=7200, source="cninfo",
        persist=True, db_table="announcements",
    ),
    "research_reports": CachePolicy(
        trading_ttl=3600, closed_ttl=7200, source="eastmoney",
        persist=True, db_table="research_reports",
    ),
    "eps_consensus": CachePolicy(
        trading_ttl=3600, closed_ttl=14400, source="ths",
        persist=True, db_table="eps_consensus",
    ),
    "shareholder_changes": CachePolicy(
        trading_ttl=3600, closed_ttl=14400, source="ths",
        persist=True, db_table="shareholder_changes",
    ),

    # === 按需拉取（A 类：查 DB → 过期/为空 → 拉源 → 写库） ===
    "dragon_tiger": CachePolicy(
        trading_ttl=1800, closed_ttl=None, source="akshare",
        persist=True, check_db_first=True, db_table="dragon_tiger",
    ),
    "dragon_tiger_seats": CachePolicy(
        trading_ttl=1800, closed_ttl=None, source="akshare",
        persist=True, check_db_first=True, db_table="dragon_tiger_seats",
    ),
    "board_daily": CachePolicy(
        trading_ttl=1800, closed_ttl=None, source="easy_tdx",
        persist=True, check_db_first=True, db_table="board_daily",
    ),
    "hot_stocks": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="akshare",
        persist=True, check_db_first=True, db_table="hot_stocks",
    ),
    "hot_reasons": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="akshare",
        persist=True, check_db_first=True, db_table="hot_reasons",
    ),
    "margin_trading": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="akshare",
        persist=True, check_db_first=True, db_table="margin_trading",
    ),
    "block_trades": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="eastmoney",
        persist=True, check_db_first=True, db_table="block_trades",
    ),
    "lockup_calendar": CachePolicy(
        trading_ttl=14400, closed_ttl=None, source="eastmoney",
        persist=True, check_db_first=True, db_table="lockup_calendar",
    ),
    "global_markets": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="easy_tdx",
        persist=True, check_db_first=True, db_table="global_markets",
    ),

    # === 即时计算（D 类：Compute 引擎） ===
    "compute_macd": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="compute",
        compute_fn="macd",
    ),
    "compute_kdj": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="compute",
        compute_fn="kdj",
    ),
    "compute_rsi": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="compute",
        compute_fn="rsi",
    ),
    "compute_boll": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="compute",
        compute_fn="boll",
    ),
    "compute_signal_scan": CachePolicy(
        trading_ttl=3600, closed_ttl=None, source="compute",
        compute_fn="signal_scan",
    ),
}
