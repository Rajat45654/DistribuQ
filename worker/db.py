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
