import time
import uuid

from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from backend.core.lifespan import get_prediction_service
from backend.services.db_service import DBService, DB_PATH

router = APIRouter()
_db = DBService(DB_PATH)
_segment_jobs: dict[str, dict] = {}


class SegmentWarmupRequest(BaseModel):
    model: str = "first"
    force_refresh: bool = False
    background: bool = True


async def _run_segment_materialize(job_id: str, model: str, force_refresh: bool) -> None:
    _segment_jobs[job_id].update({"status": "running", "started_at": time.time()})
    try:
        pred_svc = get_prediction_service(model)
        segments = await pred_svc.get_overview(
            force_refresh=force_refresh,
            allow_partial=False,
        )
        _segment_jobs[job_id].update({
            "status": "completed",
            "completed_at": time.time(),
            "materialized": len(segments),
        })
    except Exception as e:
        _segment_jobs[job_id].update({
            "status": "failed",
            "completed_at": time.time(),
            "error": str(e),
        })


@router.get("/cache")
def cache_stats():
    """Show what is currently stored in the persistent SQLite cache."""
    return _db.cache_stats()


@router.delete("/cache")
def cache_clear(prefix: str = ""):
    """
    Delete cache entries.
    - No prefix → clear everything.
    - prefix=first:  → clear first-model data.
    - prefix=recur:  → clear recurring data.
    - prefix=ltv:    → clear LTV data.
    """
    if prefix:
        removed = _db.cache_delete_pattern(prefix + "*")
    else:
        removed = _db.cache_delete_pattern("*")
    return {"deleted": removed}


@router.post("/cache/evict")
def cache_evict():
    """Manually remove expired rows (runs automatically on startup too)."""
    removed = _db.cache_evict_expired()
    return {"evicted": removed}


@router.get("/segments/materialized")
def segment_materialized_stats():
    """Show ready-to-serve materialized segment prediction payloads."""
    return _db.segment_prediction_stats()


@router.post("/segments/materialize")
async def segment_materialize(body: SegmentWarmupRequest, background_tasks: BackgroundTasks):
    """
    Build or refresh the segment prediction table for dashboard q_map segments.
    Normal dashboard requests read this table first, so this endpoint is the
    explicit warmup after raw data or model changes.
    """
    job_id = uuid.uuid4().hex
    _segment_jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "model": body.model,
        "force_refresh": body.force_refresh,
        "created_at": time.time(),
        "materialized": 0,
    }
    if body.background:
        background_tasks.add_task(
            _run_segment_materialize, job_id, body.model, body.force_refresh
        )
        return _segment_jobs[job_id]

    await _run_segment_materialize(job_id, body.model, body.force_refresh)
    return _segment_jobs[job_id]


@router.get("/segments/materialize/{job_id}")
def segment_materialize_job(job_id: str):
    return _segment_jobs.get(job_id, {"job_id": job_id, "status": "not_found"})


@router.get("/segments/materialize")
def segment_materialize_jobs():
    return {
        "jobs": sorted(
            _segment_jobs.values(),
            key=lambda item: item.get("created_at", 0),
            reverse=True,
        )[:20]
    }
