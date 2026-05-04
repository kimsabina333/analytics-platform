import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.core.lifespan import get_ai_service
from backend.models.chat import ChatRequest

router = APIRouter()


@router.post("")
async def chat(request: ChatRequest):
    ai_svc = get_ai_service(request.model)

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
