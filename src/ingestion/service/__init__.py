"""DataService — unified data access layer with cache + compute + on-demand fetching."""
from .service import DataService
from .cache import MarketAwareCache
from .policy import CachePolicy, POLICIES, is_trading_time

__all__ = ["DataService", "MarketAwareCache", "CachePolicy", "POLICIES", "is_trading_time"]
