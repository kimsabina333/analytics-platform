import asyncio
import time
from typing import List, Optional

import numpy as np
import pandas as pd


AVAILABLE_DIMENSIONS = [
    "offer", "geo", "utm_source", "channel",
    "gender", "age", "payment_method", "card_type", "card_brand",
]

TABLE = "analytics_draft.ltv_ml_fast"


class LTVService:
    def __init__(self, bq_client):
        self.bq_client = bq_client
        self._cache_df: Optional[pd.DataFrame] = None
        self._cache_time: float = 0
        self.CACHE_TTL = 3600
        self._lock = asyncio.Lock()

    async def get_data(self) -> pd.DataFrame:
        async with self._lock:
            if self._cache_df is not None and (time.time() - self._cache_time) < self.CACHE_TTL:
                return self._cache_df
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, self._query)
            self._cache_df = df
            self._cache_time = time.time()
            print(f"LTV: loaded {len(df)} rows from {TABLE}")
            return df

    def _query(self) -> pd.DataFrame:
        sql = f"""
        SELECT
            offer, geo, utm_source, channel, gender, age,
            payment_method, card_type, card_brand,
            CAST(ltv AS FLOAT64) AS ltv,
            CAST(arppu AS FLOAT64) AS arppu,
            CAST(ltv_recurring AS FLOAT64) AS ltv_recurring,
            CAST(churned AS INT64) AS churned,
            paid_count,
            subscription_cohort_date
        FROM `{TABLE}`
        WHERE ltv IS NOT NULL AND arppu IS NOT NULL
        """
        rows = self.bq_client.query(sql).result()
        return pd.DataFrame([dict(row) for row in rows])

    async def get_overview(self) -> dict:
        df = await self.get_data()
        return {
            "avg_ltv": float(df["ltv"].mean()),
            "avg_arppu": float(df["arppu"].mean()),
            "avg_ltv_recurring": float(df["ltv_recurring"].mean()),
            "churn_rate": float(df["churned"].mean()) if "churned" in df.columns else None,
            "customer_count": int(len(df)),
        }

    async def get_by_dimension(self, dimension: str) -> List[dict]:
        df = await self.get_data()
        if dimension not in df.columns:
            return []
        agg = (
            df.groupby(dimension)
            .agg(
                avg_ltv=("ltv", "mean"),
                avg_arppu=("arppu", "mean"),
                avg_ltv_recurring=("ltv_recurring", "mean"),
                churn_rate=("churned", "mean"),
                count=("ltv", "count"),
            )
            .reset_index()
            .rename(columns={dimension: "value"})
        )
        agg = agg.replace({np.nan: None})
        return agg.to_dict("records")
