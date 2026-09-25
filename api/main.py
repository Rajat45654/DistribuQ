import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from typing import List, Optional
from uuid import UUID
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse

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
from api.redis_client import init_redis, close_redis, enqueue_job, schedule_job

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("distribuq.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting DistribuQ API service...")
    await init_db_pool()
    await init_redis()
    yield
    logger.info("Shutting down DistribuQ API service...")
    await close_redis()
    await close_db_pool()


app = FastAPI(
    title="DistribuQ API",
    description="Distributed Task Queue & Worker Orchestration REST API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    """Redirect root path to interactive Swagger documentation."""
    return RedirectResponse(url="/docs")


@app.get("/healthz", tags=["System"])
async def health_check():
    return {"status": "ok"}


@app.post(
    "/api/v1/jobs",
    response_model=JobCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Jobs"],
    summary="Submit a new background job",
)
async def submit_job(job_req: JobCreateRequest):
    # 1. Write initial job record to Postgres
    job_record = await create_job(
        job_type=job_req.type,
        payload=job_req.payload,
        priority=job_req.priority,
        max_attempts=job_req.max_attempts,
        scheduled_for=job_req.scheduled_for,
        recurrence_rule=job_req.recurrence_rule,
    )
    job_id = job_record["id"]

    # 2. If it's not a delayed job, push onto Redis immediately (FIFO)
    # If scheduled_for is set, register into Redis scheduled sorted set
    if job_req.scheduled_for is None:
        await enqueue_job(job_id=job_id)
        logger.info("Submitted and enqueued job %s (type: %s)", job_id, job_req.type)
    else:
        await schedule_job(job_id=job_id, scheduled_for=job_req.scheduled_for)
        logger.info("Scheduled job %s for %s", job_id, job_req.scheduled_for)

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


@app.get(
    "/api/v1/workers",
    response_model=List[WorkerDetailResponse],
    tags=["Workers"],
    summary="List all registered workers and their health state",
)
async def list_registered_workers():
    workers = await get_workers()
    return [WorkerDetailResponse(**w) for w in workers]


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


