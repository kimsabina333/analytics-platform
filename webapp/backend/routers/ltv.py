from fastapi import APIRouter, HTTPException
from typing import List

from backend.core.lifespan import get_ltv_service
from backend.models.ltv import LTVOverview, LTVSegmentStat
from backend.services.ltv_service import AVAILABLE_DIMENSIONS

router = APIRouter()


@router.get("/overview", response_model=LTVOverview)
async def get_overview():
    svc = get_ltv_service()
    if svc is None:
        raise HTTPException(503, "LTV service not available")
    data = await svc.get_overview()
    return LTVOverview(**data)


@router.get("/by-segment", response_model=List[LTVSegmentStat])
async def get_by_segment(dimension: str = "offer"):
    svc = get_ltv_service()
    if svc is None:
        raise HTTPException(503, "LTV service not available")
    if dimension not in AVAILABLE_DIMENSIONS:
        raise HTTPException(400, f"Unknown dimension '{dimension}'. Available: {AVAILABLE_DIMENSIONS}")
    rows = await svc.get_by_dimension(dimension)
    return [LTVSegmentStat(**r) for r in rows]


@router.get("/dimensions")
async def get_dimensions():
    return {"dimensions": AVAILABLE_DIMENSIONS}
