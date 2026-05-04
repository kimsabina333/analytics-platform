import json

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from backend.core.lifespan import get_risk_service, get_ai_risk_service
from backend.models.chat import ChatRequest

router = APIRouter()


def _svc():
    svc = get_risk_service()
    if svc is None:
        from fastapi import HTTPException
        raise HTTPException(503, "Risk service not available (BigQuery not configured)")
    return svc


@router.get("/mids")
async def get_mids():
    return await _svc().get_mids()


@router.get("/summary")
async def get_summary():
    return await _svc().get_summary()


@router.get("/trends")
async def get_trends(mid: str = Query(None)):
    return await _svc().get_trends(mid)


@router.get("/anomalies")
async def get_anomalies():
    return await _svc().get_anomalies()


@router.get("/cor")
async def get_cor(
    breakdown_type: str = Query(None),
    merchant_account: str = Query(None),
):
    return await _svc().get_cor_breakdown(breakdown_type, merchant_account)


@router.get("/cor/summary")
async def get_cor_summary():
    return await _svc().get_cor_summary()


@router.get("/revenue")
async def get_revenue(
    mid: str = Query(None),
    event_type: str = Query(None),
):
    return await _svc().get_revenue(mid, event_type)


@router.get("/cor-revenue")
async def get_cor_revenue(
    mid: str = Query(None),
    event_type: str = Query("Settlement"),
):
    return await _svc().get_cor_revenue_ratio(mid, event_type)


@router.post("/chat")
async def risk_chat(request: ChatRequest):
    ai_svc = get_ai_risk_service()
    if ai_svc is None:
        from fastapi import HTTPException
        raise HTTPException(503, "Risk AI service not available (BigQuery not configured)")

    async def event_generator():
        async for event in ai_svc.stream_response(
            [m.model_dump() for m in request.messages]
        ):
            yield f"data: {json.dumps(event, default=str)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
