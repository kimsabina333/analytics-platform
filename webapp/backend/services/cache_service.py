"""
Three-layer cache:

  L1  in-memory dict  (asyncio-safe, sub-ms, lost on restart)
  L2  Redis           (fast, optional, lost on restart)
  L3  SQLite          (persistent, 24 h TTL, survives restarts)

get(key)  → L1 → L2 → L3 → None
set(key)  → L1 + L2 + L3  (L3 always persists with PERSIST_TTL)

PredictionService / DataService call .get/.set with the same short Redis TTL
they always used (300 s).  The L3 write is a side-effect they don't see.
"""

import asyncio
import fnmatch
import time
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from backend.services.db_service import DBService

# SQLite entries live 24 hours.  Keys that include date_max auto-invalidate
# naturally when the date advances (different key → L3 miss → fresh compute).
PERSIST_TTL = 86_400  # 24 h


class CacheService:
    def __init__(self, redis_url: str, ttl: int = 300, db: "Optional[DBService]" = None):
        self.ttl = ttl
        self._db = db
        self._client = None
        self._mem: dict = {}           # {key: (value_str, expires_monotonic)}
        self._mem_lock = asyncio.Lock()

        try:
            import redis.asyncio as aioredis
            self._client = aioredis.from_url(
                redis_url, decode_responses=True, socket_connect_timeout=1
            )
            print(f"Cache: Redis configured at {redis_url}")
        except Exception as e:
            print(f"Cache: Redis unavailable ({e}), using memory + SQLite")

    # ── helpers ───────────────────────────────────────────────────────────────

    async def _redis_ok(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            return False

    # ── public API ────────────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[str]:
        # L1: memory
        async with self._mem_lock:
            entry = self._mem.get(key)
            if entry:
                val, exp = entry
                if time.monotonic() < exp:
                    return val
                del self._mem[key]

        # L2: Redis
        if await self._redis_ok():
            try:
                val = await self._client.get(key)
                if val is not None:
                    # Warm L1
                    async with self._mem_lock:
                        self._mem[key] = (val, time.monotonic() + self.ttl)
                    return val
            except Exception:
                pass

        # L3: SQLite (persistent)
        if self._db is not None:
            val = self._db.cache_get(key)
            if val is not None:
                # Warm L1 so the next request in this process is instant
                async with self._mem_lock:
                    self._mem[key] = (val, time.monotonic() + self.ttl)
                return val

        return None

    async def set(self, key: str, value: str, ex: Optional[int] = None) -> None:
        ttl = ex or self.ttl

        # L1: memory
        async with self._mem_lock:
            self._mem[key] = (value, time.monotonic() + ttl)

        # L2: Redis
        if await self._redis_ok():
            try:
                await self._client.set(key, value, ex=ttl)
            except Exception:
                pass

        # L3: SQLite — always persist, regardless of Redis availability.
        # Running synchronous SQLite here is acceptable: the call is fast
        # (< 5 ms for a 1 MB compressed write) and this is an internal tool.
        if self._db is not None:
            self._db.cache_set(key, value, ttl=PERSIST_TTL)

    async def delete_pattern(self, pattern: str) -> None:
        # L1
        async with self._mem_lock:
            to_del = [k for k in self._mem if fnmatch.fnmatch(k, pattern)]
            for k in to_del:
                del self._mem[k]

        # L2
        if await self._redis_ok():
            try:
                keys = await self._client.keys(pattern)
                if keys:
                    await self._client.delete(*keys)
            except Exception:
                pass

        # L3
        if self._db is not None:
            self._db.cache_delete_pattern(pattern)
