from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.core.lifespan import get_prediction_service
from backend.models.prediction import SegmentPredictionResult
from backend.services.model_service import FEATURES

router = APIRouter()


class ComboRequest(BaseModel):
    filters: Dict[str, str]
    q: float = 0.05
    model: str = "first"


@router.get("/dimensions")
async def get_dimensions(model: str = Query("first")):
    pred_svc = get_prediction_service(model)
    return {
        "dimensions": FEATURES,
        "categories": pred_svc.model.categories,
    }


@router.get("/sr", response_model=Optional[SegmentPredictionResult])
async def get_segment_sr(
    dimension: str = Query(...),
    value: str = Query(...),
    q: float = Query(0.05),
    model: str = Query("first"),
):
    pred_svc = get_prediction_service(model)
    result = await pred_svc.compute_segment_sr(dimension, value, q)
    if result is None:
        raise HTTPException(status_code=404, detail="Insufficient data for this segment")
    return result


@router.post("/sr/combo", response_model=Optional[SegmentPredictionResult])
async def get_combo_sr(body: ComboRequest):
    pred_svc = get_prediction_service(body.model)
    result = await pred_svc.compute_segment_sr_combo(body.filters, body.q)
    if result is None:
        raise HTTPException(status_code=404, detail="Insufficient data for this combination")
    return result


@router.get("/top")
async def get_top_segments(
    dimension: str = Query(...),
    n: int = Query(5),
    order: str = Query("best"),
    model: str = Query("first"),
):
    pred_svc = get_prediction_service(model)
    return await pred_svc.get_top_segments(dimension, n, order)
