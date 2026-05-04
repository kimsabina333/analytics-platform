from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import os

from backend.core.lifespan import lifespan, is_ready
from backend.routers import chat, dashboard, segments, ltv, risk, marketing
from backend.routers.auth import decode_token
import backend.routers.auth as auth_router
import backend.routers.admin as admin_router

app = FastAPI(title="SR Monitoring Platform", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    # Protect all /api/ routes except /api/auth/*
    if path.startswith("/api/") and not path.startswith("/api/auth/"):
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        try:
            decode_token(header[7:])
        except Exception as e:
            return JSONResponse({"detail": str(e)}, status_code=401)
    return await call_next(request)


app.include_router(auth_router.router, prefix="/api/auth",  tags=["auth"])
app.include_router(admin_router.router, prefix="/api/admin", tags=["admin"])
app.include_router(dashboard.router,   prefix="/api/dashboard", tags=["dashboard"])
app.include_router(segments.router, prefix="/api/segments", tags=["segments"])
app.include_router(chat.router, prefix="/api/chat", tags=["chat"])
app.include_router(ltv.router,  prefix="/api/ltv",  tags=["ltv"])
app.include_router(risk.router, prefix="/api/risk", tags=["risk"])
app.include_router(marketing.router, prefix="/api/marketing", tags=["marketing"])


@app.get("/health")
async def health():
    return {"status": "ok", "ready": is_ready()}


@app.get("/")
async def ui():
    ui_path = os.path.join(os.path.dirname(__file__), "..", "test_ui.html")
    return FileResponse(os.path.abspath(ui_path))
