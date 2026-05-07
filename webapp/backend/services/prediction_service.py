import asyncio
import hashlib
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backend.models.prediction import (
    DailyPrediction,
    DeclineCategoryStat,
    SegmentPredictionResult,
)
from backend.services.cache_service import CacheService
from backend.services.data_service import DataService
from backend.services.db_service import DBService
from backend.services.model_service import FEATURES, ModelService

MIN_CNT = 50
MIN_DAYS = 3
MAX_CI_WIDTH = 0.5
DECLINE_CATEGORIES = [
    "INSUFFICIENT_FUNDS",
    "FRAUD_RISK",
    "DO_NOT_HONOR",
    "CARD_ISSUE",
    "BANK_DECLINE",
    "TECH_ERROR",
    "OTHER",
]
DECLINE_COLS = {f"decline_{cat.lower()}": cat for cat in DECLINE_CATEGORIES}


class PredictionService:
    """
    Orchestrates data loading → model inference → SR computation.

    Key design decisions:
    - _data_lock: ensures model runs (expensive) happen once per date_max.
    - _data_cache: keeps numpy sample arrays in memory (not serializable to Redis efficiently).
    - Segment-level results are cached in Redis (JSON-serializable Pydantic models).
    - Logic faithfully mirrors compute_sr() / compute_sr_combo() from sr_conversion_alert.py.
    """

    def __init__(
        self,
        model_svc: ModelService,
        data_svc: DataService,
        cache_svc: CacheService,
        cache_prefix: str = "",
        db_svc: Optional[DBService] = None,
        model_key: str = "first",
    ):
        self.model = model_svc
        self.data = data_svc
        self.cache = cache_svc
        self.db = db_svc
        self.model_key = model_key
        self._prefix = cache_prefix  # e.g. "first:" or "recur:" — isolates cache keys per model
        self._segment_locks: Dict[str, asyncio.Lock] = {}
        self._data_lock = asyncio.Lock()
        # (date_max_str, data_df, dates_array, samples_list)
        self._data_cache: Optional[Tuple[str, pd.DataFrame, np.ndarray, List[np.ndarray]]] = None

    def _single_key(self, dimension: str, value: str, q: float) -> str:
        return f"single:{dimension}={value}:q={q:g}"

    def _combo_key(self, filters: Dict[str, str], q: float) -> str:
        filter_str = "&".join(f"{k}={v}" for k, v in sorted(filters.items()))
        return f"combo:{filter_str}:q={q:g}"

    def _cache_value_key(self, value: str) -> str:
        return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]

    def _materialized_get(self, segment_key: str) -> Optional[SegmentPredictionResult]:
        if self.db is None:
            return None
        cached = self.db.segment_prediction_get_latest(self.model_key, segment_key)
        if not cached:
            return None
        result = SegmentPredictionResult.model_validate_json(cached)
        if self._looks_like_legacy_decline_payload(result):
            return None
        return result

    def _looks_like_legacy_decline_payload(self, result: SegmentPredictionResult) -> bool:
        """Old materialized rows were created before decline categories existed."""
        if not result.daily:
            return False
        has_decline_breakdown = any(day.declines or day.decline_count for day in result.daily)
        has_failed_attempts = any(
            day.actual_sr is not None and day.actual_sr < 0.999 and day.count > 0
            for day in result.daily
        )
        return has_failed_attempts and not has_decline_breakdown

    def _materialized_set(
        self,
        segment_key: str,
        date_max: str,
        result: SegmentPredictionResult,
    ) -> None:
        if self.db is None or not result.daily:
            return
        latest = result.daily[-1]
        self.db.segment_prediction_set(
            model=self.model_key,
            segment_key=segment_key,
            date_max=date_max,
            dimension=result.dimension,
            value=result.value,
            q_threshold=result.q_threshold,
            is_alert=result.is_alert,
            latest_sr=latest.actual_sr,
            latest_count=latest.count,
            value_str=result.model_dump_json(),
        )

    def _get_segment_lock(self, key: str) -> asyncio.Lock:
        if key not in self._segment_locks:
            self._segment_locks[key] = asyncio.Lock()
        return self._segment_locks[key]

    async def _get_data_and_samples(self) -> Tuple[pd.DataFrame, np.ndarray, List[np.ndarray]]:
        """Load BQ data (Redis-cached 5 min) and run posterior predictive for all rows per date."""
        raw_key = f"{self._prefix}raw_df:v2_declines"
        cached_json = await self.cache.get(raw_key)
        if cached_json:
            df = pd.read_json(cached_json, orient="records")
            df["date"] = pd.to_datetime(df["date"])
        else:
            df = await self.data.load_data()
            await self.cache.set(raw_key, df.to_json(orient="records", date_format="iso"))

        decline_cols = [col for col in DECLINE_COLS if col in df.columns]
        agg_spec = {
            "success": ("success", "sum"),
            "cnt": ("cnt", "sum"),
            **{col: (col, "sum") for col in decline_cols},
        }
        data = (
            df.groupby(["date"] + FEATURES, as_index=False)
            .agg(**agg_spec)
            .sort_values(["date", "success"])
        )
        data["date"] = pd.to_datetime(data["date"])
        data.set_index("date", inplace=True)
        dates = np.unique(data.index)

        if len(dates) == 0:
            return data, dates, []

        date_max_str = str(dates[-1])[:10]

        async with self._data_lock:
            if self._data_cache and self._data_cache[0] == date_max_str:
                return self._data_cache[1], self._data_cache[2], self._data_cache[3]

            samples: List[np.ndarray] = []
            for date in dates:
                daily = data.loc[date]
                if isinstance(daily, pd.Series):
                    daily = daily.to_frame().T
                X_enc = self.model.encode(daily)
                cnt_arr = np.array(daily["cnt"])
                pred = await self.model.predict(cnt_arr, X_enc)
                samples.append(pred[::8])  # thin 4000→500 samples to reduce RAM

            self._data_cache = (date_max_str, data, dates, samples)

        return data, dates, samples

    # ─── single-dimension ────────────────────────────────────────────────────

    async def compute_segment_sr(
        self, dimension: str, value: str, q: float = 0.05, force_refresh: bool = False
    ) -> Optional[SegmentPredictionResult]:
        segment_key = self._single_key(dimension, value, q)
        if not force_refresh:
            materialized = self._materialized_get(segment_key)
            if materialized is not None:
                return materialized

        data, dates, samples = await self._get_data_and_samples()
        if len(dates) == 0:
            return None

        date_max = str(dates[-1])[:10]
        cache_val = self._cache_value_key(f"{dimension}:{value}:{q:g}")
        cache_key = f"{self._prefix}pred:v2_declines:{cache_val}:{date_max}"
        lock = self._get_segment_lock(cache_key)

        async with lock:
            if not force_refresh:
                cached = await self.cache.get(cache_key)
                if cached:
                    result = SegmentPredictionResult.model_validate_json(cached)
                    self._materialized_set(segment_key, date_max, result)
                    return result

            result = self._compute_single(data, dates, samples, dimension, value, q)
            if result:
                await self.cache.set(cache_key, result.model_dump_json())
                self._materialized_set(segment_key, date_max, result)
            return result

    def _compute_single(
        self,
        data: pd.DataFrame,
        dates: np.ndarray,
        samples: List[np.ndarray],
        col: str,
        val: str,
        q: float,
    ) -> Optional[SegmentPredictionResult]:
        masks = []
        for date in dates:
            d = data.loc[date]
            if isinstance(d, pd.Series):
                d = d.to_frame().T
            masks.append((d[col] == val).values)

        if not any(m.any() for m in masks):
            return None

        valid_idx = [i for i, m in enumerate(masks) if m.any()]
        valid_dates = dates[valid_idx]
        valid_masks = [masks[i] for i in valid_idx]
        valid_samples = [samples[i] for i in valid_idx]

        query_str = f"{col}=='{val}'"
        cnt_segment = data.query(query_str).groupby("date")["cnt"].sum().sort_index()

        if len(cnt_segment) < MIN_DAYS or cnt_segment.mean() < MIN_CNT:
            return None

        success_dist = np.array(
            [valid_samples[i][:, valid_masks[i]].sum(1) for i in range(len(valid_dates))]
        )
        cnt_np = cnt_segment.to_numpy().reshape(-1, 1)

        avg_ci_width = (
            np.quantile(success_dist / cnt_np, 0.99, axis=1)
            - np.quantile(success_dist / cnt_np, 0.01, axis=1)
        ).mean()
        if avg_ci_width > MAX_CI_WIDTH:
            return None

        sr_dist = success_dist / cnt_np
        sr_fact = (
            data.query(query_str)
            .groupby("date")
            .apply(lambda x: x["success"].sum() / x["cnt"].sum())
            .sort_index()
        )

        decline_by_date = self._build_decline_breakdown(
            data.query(query_str), cnt_segment
        )
        return self._build_result(
            col, val, q, valid_dates, sr_dist, sr_fact, cnt_segment, decline_by_date
        )

    # ─── multi-dimension combo ────────────────────────────────────────────────

    async def compute_segment_sr_combo(
        self, filters: Dict[str, str], q: float = 0.05, force_refresh: bool = False
    ) -> Optional[SegmentPredictionResult]:
        segment_key = self._combo_key(filters, q)
        if not force_refresh:
            materialized = self._materialized_get(segment_key)
            if materialized is not None:
                return materialized

        data, dates, samples = await self._get_data_and_samples()
        if len(dates) == 0:
            return None

        date_max = str(dates[-1])[:10]
        filter_str = "&".join(f"{k}={v}" for k, v in sorted(filters.items()))
        cache_val = self._cache_value_key(f"{filter_str}:{q:g}")
        cache_key = f"{self._prefix}combo:v2_declines:{cache_val}:{date_max}"
        lock = self._get_segment_lock(cache_key)

        async with lock:
            if not force_refresh:
                cached = await self.cache.get(cache_key)
                if cached:
                    result = SegmentPredictionResult.model_validate_json(cached)
                    self._materialized_set(segment_key, date_max, result)
                    return result

            result = self._compute_combo(data, dates, samples, filters, q)
            if result:
                await self.cache.set(cache_key, result.model_dump_json())
                self._materialized_set(segment_key, date_max, result)
            return result

    def _compute_combo(
        self,
        data: pd.DataFrame,
        dates: np.ndarray,
        samples: List[np.ndarray],
        filters: Dict[str, str],
        q: float,
    ) -> Optional[SegmentPredictionResult]:
        query_parts = [f"{col}=='{val}'" for col, val in filters.items()]
        query_str = " and ".join(query_parts)

        masks = []
        for date in dates:
            d = data.loc[date]
            if isinstance(d, pd.Series):
                d = d.to_frame().T
            mask = np.ones(len(d), dtype=bool)
            for col, val in filters.items():
                mask &= (d[col] == val).values
            masks.append(mask)

        if not any(m.any() for m in masks):
            return None

        valid_idx = [i for i, m in enumerate(masks) if m.any()]
        valid_dates = dates[valid_idx]
        valid_masks = [masks[i] for i in valid_idx]
        valid_samples = [samples[i] for i in valid_idx]

        cnt_segment = data.query(query_str).groupby("date")["cnt"].sum().sort_index()

        if len(cnt_segment) < MIN_DAYS or cnt_segment.mean() < MIN_CNT:
            return None

        success_dist = np.array(
            [valid_samples[i][:, valid_masks[i]].sum(1) for i in range(len(valid_dates))]
        )
        cnt_np = cnt_segment.to_numpy().reshape(-1, 1)

        avg_ci_width = (
            np.quantile(success_dist / cnt_np, 0.99, axis=1)
            - np.quantile(success_dist / cnt_np, 0.01, axis=1)
        ).mean()
        if avg_ci_width > MAX_CI_WIDTH:
            return None

        sr_dist = success_dist / cnt_np
        sr_fact = (
            data.query(query_str)
            .groupby("date")
            .apply(lambda x: x["success"].sum() / x["cnt"].sum())
            .sort_index()
        )

        decline_by_date = self._build_decline_breakdown(
            data.query(query_str), cnt_segment
        )
        dim_val = "&".join(f"{k}={v}" for k, v in sorted(filters.items()))
        return self._build_result(
            "combo", dim_val, q, valid_dates, sr_dist, sr_fact, cnt_segment, decline_by_date
        )

    # ─── shared result builder ────────────────────────────────────────────────

    def _build_result(
        self,
        dimension: str,
        value: str,
        q: float,
        valid_dates: np.ndarray,
        sr_dist: np.ndarray,
        sr_fact: pd.Series,
        cnt_segment: pd.Series,
        decline_by_date: Dict[str, List[DeclineCategoryStat]],
    ) -> SegmentPredictionResult:
        daily_preds: List[DailyPrediction] = []
        for i, date in enumerate(valid_dates):
            date_key = str(date)[:10]
            dist_day = sr_dist[i]
            fact_val = float(sr_fact.get(date, np.nan))
            ci_low = float(np.quantile(dist_day, q))
            ci_high = float(np.quantile(dist_day, 1 - q))
            is_alert = (not np.isnan(fact_val)) and (fact_val < ci_low)
            declines = decline_by_date.get(date_key, [])
            decline_count = sum(d.count for d in declines)
            top_decline_category = declines[0].category if declines else None
            daily_preds.append(
                DailyPrediction(
                    date=date_key,
                    mean=float(dist_day.mean()),
                    ci_low=ci_low,
                    ci_high=ci_high,
                    actual_sr=None if np.isnan(fact_val) else fact_val,
                    count=int(cnt_segment.get(date, 0)),
                    decline_count=decline_count,
                    declines=declines,
                    top_decline_category=top_decline_category,
                    is_alert=is_alert,
                )
            )

        last = daily_preds[-1]
        return SegmentPredictionResult(
            dimension=dimension,
            value=value,
            q_threshold=q,
            is_alert=last.is_alert,
            ci_width=float(np.mean([d.ci_high - d.ci_low for d in daily_preds])),
            daily=daily_preds,
        )

    def _build_decline_breakdown(
        self,
        segment_data: pd.DataFrame,
        cnt_segment: pd.Series,
    ) -> Dict[str, List[DeclineCategoryStat]]:
        available_cols = [col for col in DECLINE_COLS if col in segment_data.columns]
        if not available_cols or segment_data.empty:
            return {}

        grouped = segment_data.groupby(level=0)[available_cols].sum().sort_index()
        result: Dict[str, List[DeclineCategoryStat]] = {}

        for date, row in grouped.iterrows():
            total_declines = int(row.sum())
            attempts = int(cnt_segment.get(date, 0))
            if total_declines <= 0 or attempts <= 0:
                continue
            stats = []
            for col, category in DECLINE_COLS.items():
                if col not in row.index:
                    continue
                count = int(row[col])
                if count <= 0:
                    continue
                stats.append(
                    DeclineCategoryStat(
                        category=category,
                        count=count,
                        share_of_declines=count / total_declines,
                        share_of_attempts=count / attempts,
                    )
                )
            result[str(date)[:10]] = sorted(
                stats, key=lambda item: item.count, reverse=True
            )
        return result

    async def get_decline_explanation(
        self, dimension: str, value: str, q: float = 0.05
    ) -> Optional[Dict]:
        result = await self.compute_segment_sr(dimension, value, q)
        if result is None or not result.daily:
            return None

        latest = result.daily[-1]
        previous = result.daily[-2] if len(result.daily) > 1 else None
        prev_by_category = {
            item.category: item
            for item in (previous.declines if previous else [])
        }

        drivers = []
        for item in latest.declines:
            prev = prev_by_category.get(item.category)
            prev_share = prev.share_of_attempts if prev else 0.0
            drivers.append({
                "category": item.category,
                "count": item.count,
                "share_of_declines": item.share_of_declines,
                "share_of_attempts": item.share_of_attempts,
                "attempt_share_delta_vs_previous_day": item.share_of_attempts - prev_share,
            })

        return {
            "dimension": result.dimension,
            "value": result.value,
            "date": latest.date,
            "is_alert": result.is_alert,
            "actual_sr": latest.actual_sr,
            "expected_mean_sr": latest.mean,
            "bayesian_ci_low": latest.ci_low,
            "attempts": latest.count,
            "decline_count": latest.decline_count,
            "top_decline_category": latest.top_decline_category,
            "decline_drivers": drivers,
        }

    # ─── bulk helpers ─────────────────────────────────────────────────────────

    async def get_overview(
        self,
        force_refresh: bool = False,
        allow_partial: bool = True,
    ) -> List[SegmentPredictionResult]:
        tasks = []
        materialized_results: List[SegmentPredictionResult] = []
        for key, q in self.model.q_map.items():
            q_float = float(q)
            parts = key.split("&")
            if len(parts) == 1:
                dim, val = parts[0].split("=", 1)
                segment_key = self._single_key(dim, val, q_float)
                if not force_refresh:
                    materialized = self._materialized_get(segment_key)
                    if materialized is not None:
                        materialized_results.append(materialized)
                        continue
                tasks.append(self.compute_segment_sr(dim, val, q_float, force_refresh))
            else:
                filters = dict(p.split("=", 1) for p in parts)
                segment_key = self._combo_key(filters, q_float)
                if not force_refresh:
                    materialized = self._materialized_get(segment_key)
                    if materialized is not None:
                        materialized_results.append(materialized)
                        continue
                tasks.append(self.compute_segment_sr_combo(filters, q_float, force_refresh))

        if materialized_results and not force_refresh and allow_partial:
            return materialized_results

        raw = await asyncio.gather(*tasks, return_exceptions=True)
        computed = [r for r in raw if isinstance(r, SegmentPredictionResult)]
        return materialized_results + computed

    async def get_top_segments(
        self, dimension: str, n: int = 5, order: str = "best"
    ) -> List[Dict]:
        categories = self.model.categories.get(dimension, [])
        tasks = [self.compute_segment_sr(dimension, val) for val in categories]
        raw = await asyncio.gather(*tasks, return_exceptions=True)
        valid = [r for r in raw if isinstance(r, SegmentPredictionResult) and r.daily]
        ranked = sorted(
            valid,
            key=lambda r: (r.daily[-1].actual_sr or 0) if r.daily else 0,
            reverse=(order == "best"),
        )
        return [
            {
                "dimension": r.dimension,
                "value": r.value,
                "latest_sr": r.daily[-1].actual_sr,
                "mean_sr": r.daily[-1].mean,
                "is_alert": r.is_alert,
                "count": r.daily[-1].count,
            }
            for r in ranked[:n]
        ]
