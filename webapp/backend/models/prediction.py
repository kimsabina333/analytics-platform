from typing import List, Optional
from pydantic import BaseModel, Field


class DeclineCategoryStat(BaseModel):
    category: str
    count: int
    share_of_declines: float
    share_of_attempts: float


class DailyPrediction(BaseModel):
    date: str
    mean: float
    ci_low: float
    ci_high: float
    actual_sr: Optional[float]
    count: int
    decline_count: int = 0
    declines: List[DeclineCategoryStat] = Field(default_factory=list)
    top_decline_category: Optional[str] = None
    is_alert: bool


class SegmentPredictionResult(BaseModel):
    dimension: str
    value: str
    q_threshold: float
    is_alert: bool
    ci_width: float
    daily: List[DailyPrediction]


class OverviewResponse(BaseModel):
    segments: List[SegmentPredictionResult]
    poll_interval_seconds: int
    last_updated: str
