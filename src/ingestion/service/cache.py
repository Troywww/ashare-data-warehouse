"""Market-aware TTL cache with request coalescing.

Features:
- TTL varies by market status (trading hours vs closed)
- Request coalescing: concurrent requests for the same key share one fetch
- Bulk invalidation (by type or full) for pipeline pre-market reset
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Optional

from .policy import CachePolicy, is_trading_time

logger = logging.getLogger(__name__)


class MarketAwareCache:
    """In-memory cache with market-aware TTL and request coalescing.

    Usage::

        cache = MarketAwareCache()
        result = await cache.get_or_fetch(
            key="realtime_quote:000001",
            policy=POLICIES["realtime_quote"],
            fetcher=lambda: fetch_quote("000001"),
        )
    """

    def __init__(self, maxsize: int = 50000):
        self._store: dict[str, tuple[Any, float]] = {}  # key -> (value, expire_at)
        self._pending: dict[str, asyncio.Future] = {}    # key -> Future
        self._lock = asyncio.Lock()
        self._maxsize = maxsize

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_or_fetch(
        self,
        key: str,
        policy: CachePolicy,
        fetcher: Callable,
    ) -> Any:
        """Get from cache or fetch and cache.

        Parameters
        ----------
        key : str
            Cache key (e.g. "realtime_quote:000001").
        policy : CachePolicy
            TTL configuration for this data type.
        fetcher : Callable
            Async function to fetch data if cache misses.

        Returns
        -------
        Any
            Cached or freshly fetched data.
        """
        # 1. Check cache
        hit = self._check(key)
        if hit is not None:
            return hit

        # 2. Request coalescing: someone already fetching?
        async with self._lock:
            if key in self._pending:
                logger.debug("Cache coalescing for %s", key)
                return await self._pending[key]

            future = asyncio.ensure_future(self._do_fetch(key, policy, fetcher))
            self._pending[key] = future

        try:
            return await future
        finally:
            self._pending.pop(key, None)

    async def get_or_fetch_sync(
        self,
        key: str,
        policy: CachePolicy,
        fetcher: Callable,
    ) -> Any:
        """Sync version of get_or_fetch — wraps sync fetcher in thread."""
        import functools

        async def _wrapper():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, fetcher)

        return await self.get_or_fetch(key, policy, _wrapper)

    def get(self, key: str) -> Any | None:
        """Direct cache lookup without fetch fallback."""
        hit = self._check(key)
        return hit

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Manually set a cache entry."""
        if ttl is None:
            ttl = 999_999_999
        self._store[key] = (value, time.time() + ttl)
        self._maybe_evict()

    def invalidate(self, key: str) -> None:
        """Remove a single key from cache."""
        self._store.pop(key, None)
        self._pending.pop(key, None)

    def invalidate_by_prefix(self, prefix: str) -> int:
        """Remove all cache entries with key starting with prefix.

        Returns count of removed entries.
        """
        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            self._store.pop(k, None)
            self._pending.pop(k, None)
        return len(keys)

    def invalidate_all(self) -> int:
        """Clear entire cache. Called by pipeline before market open.

        Returns count of removed entries.
        """
        count = len(self._store)
        self._store.clear()
        self._pending.clear()
        logger.info("Cache invalidated: %d entries cleared", count)
        return count

    def stats(self) -> dict:
        """Return cache statistics."""
        now = time.time()
        valid = sum(1 for v, e in self._store.values() if now < e)
        expired = len(self._store) - valid
        return {
            "total_entries": len(self._store),
            "valid": valid,
            "expired": expired,
            "pending_requests": len(self._pending),
            "maxsize": self._maxsize,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _check(self, key: str) -> Any | None:
        """Check cache without triggering fetch."""
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expire_at = entry
        if time.time() < expire_at:
            return value
        # Expired
        del self._store[key]
        return None

    async def _do_fetch(self, key: str, policy: CachePolicy, fetcher: Callable) -> Any:
        """Fetch data and cache it."""
        try:
            value = await fetcher()
            ttl = policy.current_ttl()
            self._store[key] = (value, time.time() + ttl)
            self._maybe_evict()
            return value
        except Exception as e:
            logger.warning("Cache fetch failed for %s: %s", key, e)
            raise

    def _maybe_evict(self) -> None:
        """Evict oldest entries if over maxsize."""
        if len(self._store) <= self._maxsize:
            return
        # Remove expired first
        now = time.time()
        expired_keys = [k for k, (_, e) in self._store.items() if now >= e]
        for k in expired_keys:
            del self._store[k]
        # If still over, remove oldest 10%
        if len(self._store) > self._maxsize:
            sorted_items = sorted(self._store.items(), key=lambda x: x[1][1])
            for k, _ in sorted_items[:len(self._store) // 10]:
                del self._store[k]
