import logging
from typing import Any, Dict, Optional
from uuid import UUID
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from api.config import settings

logger = logging.getLogger("distribuq.worker.db")

worker_db_pool: Optional[AsyncConnectionPool] = None


async def init_worker_db_pool() -> AsyncConnectionPool:
    global worker_db_pool
    if worker_db_pool is None:
        worker_db_pool = AsyncConnectionPool(
            conninfo=settings.DATABASE_URL,
            min_size=1,
            max_size=5,
            open=False,
            kwargs={"row_factory": dict_row}
        )
        await worker_db_pool.open()
        logger.info("Worker Postgres connection pool initialized.")
    return worker_db_pool


async def close_worker_db_pool() -> None:
    global worker_db_pool
    if worker_db_pool is not None:
        await worker_db_pool.close()
        worker_db_pool = None
        logger.info("Worker Postgres connection pool closed.")


def get_worker_db_pool() -> AsyncConnectionPool:
    if worker_db_pool is None:
        raise RuntimeError("Worker database connection pool is not initialized.")
    return worker_db_pool


async def fetch_and_lock_job(job_id: UUID, worker_id: str) -> Optional[Dict[str, Any]]:
    """
    Attempts to lock a job for execution. Sets status to RUNNING, records worker ID,
    locks at current timestamp, and increments attempts.
    """
    query = """
        UPDATE jobs
        SET status = 'RUNNING',
            locked_by = %s,
            locked_at = NOW(),
            attempts = attempts + 1
        WHERE id = %s AND status IN ('PENDING', 'RETRYING')
        RETURNING id, type, payload, attempts, max_attempts;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (worker_id, job_id))
            row = await cur.fetchone()
            await conn.commit()
            return row


async def mark_job_success(job_id: UUID, result: Any) -> None:
    """Marks a job as SUCCESS, storing the result and releasing lock."""
    query = """
        UPDATE jobs
        SET status = 'SUCCESS',
            result = %s,
            error = NULL,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (Jsonb(result) if result is not None else None, job_id))
            await conn.commit()


async def mark_job_failed(job_id: UUID, error_message: str) -> None:
    """Marks a job as FAILED, storing the error message and releasing lock."""
    query = """
        UPDATE jobs
        SET status = 'FAILED',
            error = %s,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (error_message, job_id))
            await conn.commit()


async def mark_job_retrying(job_id: UUID, error_message: str, next_retry_at: Any) -> None:
    """Marks a job as RETRYING, setting next_retry_at timestamp, error message, and releasing lock."""
    query = """
        UPDATE jobs
        SET status = 'RETRYING',
            error = %s,
            next_retry_at = %s,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (error_message, next_retry_at, job_id))
            await conn.commit()


async def move_to_dead_letter(job_id: UUID, final_error: str, attempts_made: int) -> None:
    """Marks a job as DEAD_LETTER in jobs table and archives an audit row into dead_letters table."""
    query_jobs = """
        UPDATE jobs
        SET status = 'DEAD_LETTER',
            error = %s,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s;
    """
    query_dlq = """
        INSERT INTO dead_letters (job_id, final_error, attempts_made, moved_at)
        VALUES (%s, %s, %s, NOW());
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query_jobs, (final_error, job_id))
            await cur.execute(query_dlq, (job_id, final_error, attempts_made))
            await conn.commit()


async def get_dead_letter(job_id: UUID) -> Optional[dict]:
    """Retrieves the dead letter audit record for a given job_id."""
    query = """
        SELECT id, job_id, final_error, attempts_made, moved_at
        FROM dead_letters
        WHERE job_id = %s;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (job_id,))
            return await cur.fetchone()


async def get_pending_scheduled_jobs() -> list[dict]:
    """Retrieves all PENDING jobs that have a scheduled_for timestamp."""
    query = """
        SELECT id, scheduled_for, priority
        FROM jobs
        WHERE status = 'PENDING' AND scheduled_for IS NOT NULL;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            return await cur.fetchall()





async def upsert_worker_heartbeat(worker_id: str, jobs_processed: int = 0) -> None:
    """Upserts worker liveness status and processed jobs count into the workers table."""
    query = """
        INSERT INTO workers (id, status, last_heartbeat, jobs_processed, started_at)
        VALUES (%s, 'ALIVE', NOW(), %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            status = 'ALIVE',
            last_heartbeat = NOW(),
            jobs_processed = EXCLUDED.jobs_processed;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (worker_id, jobs_processed))
            await conn.commit()


async def mark_worker_dead(worker_id: str) -> None:
    """Marks a specific worker as DEAD (e.g., during graceful shutdown)."""
    query = """
        UPDATE workers
        SET status = 'DEAD'
        WHERE id = %s;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (worker_id,))
            await conn.commit()


async def mark_expired_workers_dead(timeout_seconds: int = 10) -> list[str]:
    """Marks workers as DEAD if their last heartbeat is older than timeout_seconds."""
    query = """
        UPDATE workers
        SET status = 'DEAD'
        WHERE status = 'ALIVE'
          AND last_heartbeat < NOW() - make_interval(secs => %s)
        RETURNING id;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (timeout_seconds,))
            rows = await cur.fetchall()
            await conn.commit()
            return [row["id"] for row in rows]


async def reclaim_stale_jobs(visibility_timeout_seconds: int = 15) -> list[dict]:
    """
    Reclaims jobs that are still RUNNING but:
    1) locked_at is older than visibility_timeout_seconds, OR
    2) locked_by worker is DEAD or has timed-out heartbeats.
    Resets status back to PENDING and returns the reclaimed job records.
    """
    query = """
        UPDATE jobs
        SET status = 'PENDING',
            locked_by = NULL,
            locked_at = NULL
        WHERE status = 'RUNNING'
          AND (
            locked_at < NOW() - make_interval(secs => %s)
            OR locked_by IN (
                SELECT id FROM workers 
                WHERE status = 'DEAD' 
                   OR last_heartbeat < NOW() - make_interval(secs => %s)
            )
          )
        RETURNING id, type, locked_by;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (visibility_timeout_seconds, visibility_timeout_seconds))
            rows = await cur.fetchall()
            await conn.commit()
            return rows


async def list_all_workers() -> list[dict]:
    """Lists all registered workers and their current heartbeat/health state."""
    query = """
        SELECT id, status, last_heartbeat, jobs_processed, started_at
        FROM workers
        ORDER BY started_at DESC;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            return await cur.fetchall()


async def reclaim_due_retry_jobs() -> list[dict]:
    """
    Finds jobs in RETRYING status whose next_retry_at is due (next_retry_at <= NOW()).
    Resets status to PENDING, clears lock fields, and returns the records so they can be
    re-enqueued into Redis.
    """
    query = """
        UPDATE jobs
        SET status = 'PENDING',
            locked_by = NULL,
            locked_at = NULL
        WHERE status = 'RETRYING'
          AND next_retry_at <= NOW()
        RETURNING id, type, attempts, max_attempts;
    """
    pool = get_worker_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            await conn.commit()
            return rows


