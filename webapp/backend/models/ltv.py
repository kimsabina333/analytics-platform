from typing import List, Optional
from pydantic import BaseModel


class LTVSegmentStat(BaseModel):
    value: str
    avg_ltv: float
    avg_arppu: float
    avg_ltv_recurring: float
    churn_rate: Optional[float] = None
    count: int


class LTVOverview(BaseModel):
    avg_ltv: float
    avg_arppu: float
    avg_ltv_recurring: float
    churn_rate: Optional[float] = None
    customer_count: int
