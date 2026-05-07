import asyncio
import time
from typing import List, Optional

import pandas as pd


AVAILABLE_DIMENSIONS = [
    "offer", "geo", "utm_source", "channel",
    "gender", "age", "payment_method", "card_type", "card_brand",
]

TABLE = "analytics_draft.ltv_ml_fast"


def _bq_query(bq_client, sql: str) -> list:
    rows = bq_client.query(sql).result()
    return [dict(r) for r in rows]


class LTVService:
    def __init__(self, bq_client):
        self.bq_client = bq_client
        self._overview_cache: Optional[dict] = None
        self._overview_time: float = 0
        self._seg_cache: dict = {}
        self._seg_time: dict = {}
        self.CACHE_TTL = 3600
        self._lock = asyncio.Lock()
        self._seg_locks: dict = {d: asyncio.Lock() for d in AVAILABLE_DIMENSIONS}

    async def get_overview(self) -> dict:
        async with self._lock:
            if self._overview_cache is not None and (time.time() - self._overview_time) < self.CACHE_TTL:
                return self._overview_cache
            sql = f"""
            SELECT
                AVG(CAST(ltv AS FLOAT64))           AS avg_ltv,
                AVG(CAST(arppu AS FLOAT64))         AS avg_arppu,
                AVG(CAST(ltv_recurring AS FLOAT64)) AS avg_ltv_recurring,
                AVG(CAST(churned AS FLOAT64))       AS churn_rate,
                COUNT(*)                            AS customer_count
            FROM `{TABLE}`
            WHERE ltv IS NOT NULL AND arppu IS NOT NULL
            """
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(None, lambda: _bq_query(self.bq_client, sql))
            row = rows[0] if rows else {}
            self._overview_cache = {
                "avg_ltv": float(row.get("avg_ltv") or 0),
                "avg_arppu": float(row.get("avg_arppu") or 0),
                "avg_ltv_recurring": float(row.get("avg_ltv_recurring") or 0),
                "churn_rate": float(row.get("churn_rate") or 0),
                "customer_count": int(row.get("customer_count") or 0),
            }
            self._overview_time = time.time()
            print(f"LTV overview loaded: {self._overview_cache['customer_count']} customers")
            return self._overview_cache

    async def get_by_dimension(self, dimension: str) -> List[dict]:
        lock = self._seg_locks.get(dimension, self._lock)
        async with lock:
            cached_time = self._seg_time.get(dimension, 0)
            if dimension in self._seg_cache and (time.time() - cached_time) < self.CACHE_TTL:
                return self._seg_cache[dimension]
            sql = f"""
            SELECT
                CAST({dimension} AS STRING)         AS value,
                AVG(CAST(ltv AS FLOAT64))           AS avg_ltv,
                AVG(CAST(arppu AS FLOAT64))         AS avg_arppu,
                AVG(CAST(ltv_recurring AS FLOAT64)) AS avg_ltv_recurring,
                AVG(CAST(churned AS FLOAT64))       AS churn_rate,
                COUNT(*)                            AS count
            FROM `{TABLE}`
            WHERE ltv IS NOT NULL AND arppu IS NOT NULL
              AND {dimension} IS NOT NULL
            GROUP BY {dimension}
            ORDER BY avg_ltv DESC
            """
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(None, lambda: _bq_query(self.bq_client, sql))
            result = [
                {
                    "value": str(r.get("value") or ""),
                    "avg_ltv": float(r.get("avg_ltv") or 0),
                    "avg_arppu": float(r.get("avg_arppu") or 0),
                    "avg_ltv_recurring": float(r.get("avg_ltv_recurring") or 0),
                    "churn_rate": float(r.get("churn_rate") or 0),
                    "count": int(r.get("count") or 0),
                }
                for r in rows
            ]
            self._seg_cache[dimension] = result
            self._seg_time[dimension] = time.time()
            return result
