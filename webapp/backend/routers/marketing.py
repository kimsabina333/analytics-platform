import json

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse

from backend.core.lifespan import get_marketing_service, get_ai_marketing_service
from backend.models.chat import ChatRequest

router = APIRouter()


def _svc():
    svc = get_marketing_service()
    if svc is None:
        raise HTTPException(503, "Marketing service not available (BigQuery not configured)")
    return svc


@router.get("/roi")
async def get_roi(source: str = Query(None)):
    try:
        return await _svc().get_roi(source)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(502, f"Marketing ROI query failed: {e}")


@router.get("/sources")
async def get_sources():
    try:
        return await _svc().get_sources()
    except Exception as e:
        raise HTTPException(502, f"Marketing sources query failed: {e}")


@router.post("/chat")
async def marketing_chat(request: ChatRequest):
    ai_svc = get_ai_marketing_service()
    if ai_svc is None:
        raise HTTPException(503, "Marketing AI service not available (BigQuery not configured)")

    async def event_generator():
        async for event in ai_svc.stream_response([m.model_dump() for m in request.messages]):
            yield f"data: {json.dumps(event, default=str)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
