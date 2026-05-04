from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Query

from backend.core.config import settings
from backend.core.lifespan import get_prediction_service
from backend.models.prediction import OverviewResponse, SegmentPredictionResult

router = APIRouter()


@router.get("/overview", response_model=OverviewResponse)
async def get_overview(model: str = Query("first")):
    pred_svc = get_prediction_service(model)
    segments = await pred_svc.get_overview()
    return OverviewResponse(
        segments=segments,
        poll_interval_seconds=settings.poll_interval_seconds,
        last_updated=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/alerts", response_model=List[SegmentPredictionResult])
async def get_alerts(model: str = Query("first")):
    pred_svc = get_prediction_service(model)
    segments = await pred_svc.get_overview()
    return [s for s in segments if s.is_alert]
