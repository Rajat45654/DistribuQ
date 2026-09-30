import logging
from typing import Any, Dict, Optional
from uuid import UUID
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from api.config import settings

logger = logging.getLogger("distribuq.api.db")

db_pool: Optional[AsyncConnectionPool] = None


async def init_db_pool() -> AsyncConnectionPool:
    global db_pool
    if db_pool is None:
        logger.info("Initializing Postgres connection pool...")
        db_pool = AsyncConnectionPool(
            conninfo=settings.DATABASE_URL,
            min_size=2,
            max_size=10,
            open=False,
            kwargs={"row_factory": dict_row}
        )
        await db_pool.open()
        logger.info("Postgres connection pool opened successfully.")
    return db_pool


async def close_db_pool() -> None:
    global db_pool
    if db_pool is not None:
        logger.info("Closing Postgres connection pool...")
        await db_pool.close()
        db_pool = None
        logger.info("Postgres connection pool closed.")


def get_db_pool() -> AsyncConnectionPool:
    if db_pool is None:
        raise RuntimeError("Database connection pool is not initialized.")
    return db_pool


async def create_job(
    job_type: str,
    payload: Dict[str, Any],
    priority: int = 0,
    max_attempts: int = 3,
    scheduled_for: Optional[Any] = None,
    recurrence_rule: Optional[str] = None,
) -> Dict[str, Any]:
    query = """
        INSERT INTO jobs (type, payload, priority, max_attempts, scheduled_for, recurrence_rule, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'PENDING')
        RETURNING id, type, payload, status, priority, attempts, max_attempts, 
                  next_retry_at, scheduled_for, recurrence_rule, result, error, 
                  locked_by, locked_at, created_at, updated_at;
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                query,
                (
                    job_type,
                    Jsonb(payload),
                    priority,
                    max_attempts,
                    scheduled_for,
                    recurrence_rule,
                ),
            )
            row = await cur.fetchone()
            await conn.commit()
            return row


async def get_job(job_id: UUID) -> Optional[Dict[str, Any]]:
    query = """
        SELECT id, type, payload, status, priority, attempts, max_attempts,
               next_retry_at, scheduled_for, recurrence_rule, result, error,
               locked_by, locked_at, created_at, updated_at
        FROM jobs
        WHERE id = %s;
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (job_id,))
            return await cur.fetchone()


async def get_workers() -> list[Dict[str, Any]]:
    query = """
        SELECT id, status, last_heartbeat, jobs_processed, started_at
        FROM workers
        ORDER BY started_at DESC;
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            return await cur.fetchall()


async def get_dead_letters(limit: int = 50, offset: int = 0) -> list[Dict[str, Any]]:
    """Retrieves all dead-lettered jobs with pagination, joining job metadata."""
    query = """
        SELECT dl.id, dl.job_id, dl.final_error, dl.attempts_made, dl.moved_at,
               j.type AS job_type, j.payload AS payload
        FROM dead_letters dl
        LEFT JOIN jobs j ON dl.job_id = j.id
        ORDER BY dl.moved_at DESC
        LIMIT %s OFFSET %s;
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (limit, offset))
            return await cur.fetchall()


async def replay_dead_letter(identifier: UUID) -> Optional[Dict[str, Any]]:
    """
    Looks up a dead letter by either dead_letters.id or jobs.id.
    Deletes the dead letter entry and resets the job to PENDING with 0 attempts.
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT dl.id AS dead_letter_id, dl.job_id
                FROM dead_letters dl
                WHERE dl.id = %s OR dl.job_id = %s;
                """,
                (identifier, identifier),
            )
            record = await cur.fetchone()
            if not record:
                return None

            dl_id = record["dead_letter_id"]
            job_id = record["job_id"]

            await cur.execute("DELETE FROM dead_letters WHERE id = %s;", (dl_id,))
            await cur.execute(
                """
                UPDATE jobs
                SET status = 'PENDING',
                    attempts = 0,
                    error = NULL,
                    result = NULL,
                    next_retry_at = NULL,
                    locked_by = NULL,
                    locked_at = NULL
                WHERE id = %s;
                """,
                (job_id,),
            )
            await conn.commit()
            return {"dead_letter_id": dl_id, "job_id": job_id}


async def get_job_counts() -> Dict[str, int]:
    """Returns a dict of job status -> count for the dashboard stats snapshot."""
    query = """
        SELECT status, COUNT(*) AS cnt
        FROM jobs
        GROUP BY status;
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
    base = {"PENDING": 0, "RUNNING": 0, "SUCCESS": 0, "FAILED": 0, "RETRYING": 0, "DEAD_LETTER": 0}
    for row in rows:
        base[row["status"]] = row["cnt"]
    return base


async def get_active_worker_count() -> int:
    """Returns the number of workers with status = 'ALIVE'."""
    query = "SELECT COUNT(*) AS cnt FROM workers WHERE status = 'ALIVE';"
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            row = await cur.fetchone()
    return row["cnt"] if row else 0


async def cancel_job(job_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Cancels a job if it is currently in PENDING state.
    If the job does not exist, returns None.
    If the job is already RUNNING, SUCCESS, FAILED, RETRYING, or DEAD_LETTER,
    returns the record without modifying it (no-op as per spec).
    """
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, status, type, priority FROM jobs WHERE id = %s;", (job_id,))
            job = await cur.fetchone()
            if not job:
                return None

            if job["status"] == "PENDING":
                await cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'FAILED',
                        error = 'Job cancelled by user',
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE id = %s AND status = 'PENDING'
                    RETURNING id, status, type, priority;
                    """,
                    (job_id,),
                )
                updated = await cur.fetchone()
                await conn.commit()
                return updated or job

            return job


