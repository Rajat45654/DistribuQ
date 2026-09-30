import asyncio
import logging
import random
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.config import settings
from api.db import (
    init_db_pool,
    close_db_pool,
    create_job,
    get_job,
    get_db_pool,
    get_workers,
    get_dead_letters,
    replay_dead_letter,
    get_job_counts,
    get_active_worker_count,
)
from api.events import (
    emit_job_submitted,
    emit_stats_snapshot,
    EVENTS_CHANNEL,
)
from api.models import (
    JobCreateRequest,
    JobCreateResponse,
    JobDetailResponse,
    JobStatus,
    WorkerDetailResponse,
    DeadLetterResponse,
    ReplayResponse,
)
from api.redis_client import init_redis, close_redis, enqueue_job, schedule_job, get_redis
from api.ws_manager import manager, redis_subscriber_loop

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("distribuq.api")

DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting DistribuQ API service...")
    await init_db_pool()
    await init_redis()

    # Start the Redis -> WebSocket fan-out subscriber as a background task
    subscriber_task = asyncio.create_task(
        redis_subscriber_loop(settings.REDIS_URL),
        name="redis-ws-subscriber",
    )

    # Periodic stats broadcaster (every 5s) so the dashboard gets counters even
    # when there is no job activity
    stats_task = asyncio.create_task(
        _stats_broadcast_loop(),
        name="stats-broadcaster",
    )

    yield

    logger.info("Shutting down DistribuQ API service...")
    subscriber_task.cancel()
    stats_task.cancel()
    try:
        await subscriber_task
    except asyncio.CancelledError:
        pass
    try:
        await stats_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    await close_db_pool()


async def _stats_broadcast_loop() -> None:
    """Broadcasts a stats.snapshot event every 5 seconds to all WS clients."""
    while True:
        try:
            await asyncio.sleep(5)
            if manager.count > 0:
                counts = await get_job_counts()
                active = await get_active_worker_count()
                await emit_stats_snapshot(get_redis(), counts, active)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Stats broadcast error: %s", exc)


app = FastAPI(
    title="DistribuQ API",
    description="Distributed Task Queue & Worker Orchestration REST API",
    version="1.0.0",
    lifespan=lifespan,
)

# Serve dashboard static files
if DASHBOARD_DIR.exists():
    app.mount("/dashboard", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboard")


# ---------------------------------------------------------------------------
# Core routes
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to dashboard if it exists, otherwise Swagger."""
    if (DASHBOARD_DIR / "index.html").exists():
        return RedirectResponse(url="/dashboard/")
    return RedirectResponse(url="/docs")


@app.get("/healthz", tags=["System"])
async def health_check():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/jobs",
    response_model=JobCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Jobs"],
    summary="Submit a new background job",
)
async def submit_job(job_req: JobCreateRequest):
    job_record = await create_job(
        job_type=job_req.type,
        payload=job_req.payload,
        priority=job_req.priority,
        max_attempts=job_req.max_attempts,
        scheduled_for=job_req.scheduled_for,
        recurrence_rule=job_req.recurrence_rule,
    )
    job_id = job_record["id"]

    if job_req.scheduled_for is None:
        await enqueue_job(job_id=job_id, priority=job_req.priority)
        logger.info("Submitted and enqueued job %s (type: %s, priority: %d)", job_id, job_req.type, job_req.priority)
    else:
        await schedule_job(job_id=job_id, scheduled_for=job_req.scheduled_for)
        logger.info("Scheduled job %s for %s", job_id, job_req.scheduled_for)

    # Broadcast job.submitted event to dashboard
    try:
        await emit_job_submitted(
            get_redis(),
            job_id=str(job_id),
            job_type=job_req.type,
            priority=job_req.priority,
            scheduled_for=job_req.scheduled_for.isoformat() if job_req.scheduled_for else None,
        )
    except Exception:
        pass  # non-critical

    return JobCreateResponse(job_id=job_id, status=JobStatus.PENDING)


@app.get(
    "/api/v1/jobs/{job_id}",
    response_model=JobDetailResponse,
    tags=["Jobs"],
    summary="Get full job record by ID",
)
async def get_job_by_id(job_id: UUID):
    job = await get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found",
        )
    return JobDetailResponse(**job)


@app.get(
    "/api/v1/jobs",
    response_model=List[JobDetailResponse],
    tags=["Jobs"],
    summary="List and filter jobs",
)
async def list_jobs(
    status_filter: Optional[JobStatus] = Query(None, alias="status"),
    job_type: Optional[str] = Query(None, alias="type"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    conditions = []
    params = []

    if status_filter:
        conditions.append("status = %s")
        params.append(status_filter.value)
    if job_type:
        conditions.append("type = %s")
        params.append(job_type)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT id, type, payload, status, priority, attempts, max_attempts,
               next_retry_at, scheduled_for, recurrence_rule, result, error,
               locked_by, locked_at, created_at, updated_at
        FROM jobs
        {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
            return [JobDetailResponse(**row) for row in rows]


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------

@app.get(
    "/api/v1/workers",
    response_model=List[WorkerDetailResponse],
    tags=["Workers"],
    summary="List all registered workers and their health state",
)
async def list_registered_workers():
    workers = await get_workers()
    return [WorkerDetailResponse(**w) for w in workers]


# ---------------------------------------------------------------------------
# Dead Letters
# ---------------------------------------------------------------------------

@app.get(
    "/api/v1/dead-letters",
    response_model=List[DeadLetterResponse],
    tags=["Dead Letters"],
    summary="List dead-lettered jobs",
)
async def list_dead_letters(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    records = await get_dead_letters(limit=limit, offset=offset)
    return [DeadLetterResponse(**r) for r in records]


@app.post(
    "/api/v1/dead-letters/{id}/replay",
    response_model=ReplayResponse,
    status_code=status.HTTP_200_OK,
    tags=["Dead Letters"],
    summary="Replay a dead-lettered job",
)
async def replay_dead_letter_job(id: UUID):
    result = await replay_dead_letter(id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dead letter record '{id}' not found",
        )
    job_id = result["job_id"]
    await enqueue_job(job_id=job_id)
    logger.info("Replayed dead letter %s (job %s) - re-enqueued as PENDING", result["dead_letter_id"], job_id)
    return ReplayResponse(
        message="Job successfully replayed and enqueued for execution",
        job_id=job_id,
        dead_letter_id=result["dead_letter_id"],
        status=JobStatus.PENDING,
    )


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@app.get("/api/v1/stats", tags=["System"], summary="Current job status counts and worker summary")
async def get_stats():
    counts = await get_job_counts()
    active = await get_active_worker_count()
    workers = await get_workers()
    return {
        "job_counts": counts,
        "active_workers": active,
        "workers": [WorkerDetailResponse(**w) for w in workers],
    }


# ---------------------------------------------------------------------------
# Chaos endpoint (dev only)
# ---------------------------------------------------------------------------

@app.post(
    "/api/v1/dev/kill-worker",
    tags=["Dev / Chaos"],
    summary="Kill a random alive worker (for demo/chaos testing)",
)
async def kill_random_worker():
    """
    Publishes a kill command to a Redis channel that a random alive worker
    subscribes to. The worker receives it and initiates graceful shutdown.
    """
    redis = get_redis()
    workers = await get_workers()
    alive = [w for w in workers if w.get("status") == "ALIVE"]
    if not alive:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No alive workers found")

    target = random.choice(alive)
    worker_id = target["id"]
    channel = f"distribuq:kill:{worker_id}"
    await redis.publish(channel, "kill")
    logger.info("Sent kill signal to worker %s via channel %s", worker_id, channel)
    return {"killed_worker_id": worker_id, "channel": channel}


# ---------------------------------------------------------------------------
# WebSocket - live dashboard
# ---------------------------------------------------------------------------

@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send a fresh stats snapshot immediately on connect
        try:
            counts = await get_job_counts()
            active = await get_active_worker_count()
            await emit_stats_snapshot(get_redis(), counts, active)
        except Exception:
            pass

        # Keep alive - wait for client to disconnect
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
