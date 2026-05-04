import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.core.lifespan import get_ai_service
from backend.models.chat import ChatRequest

router = APIRouter()


@router.post("")
async def chat(request: ChatRequest):
    ai_svc = get_ai_service(request.model)
    if ai_svc is None:
        raise HTTPException(status_code=503, detail=f"Model '{request.model}' is not available")

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
