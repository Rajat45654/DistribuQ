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
