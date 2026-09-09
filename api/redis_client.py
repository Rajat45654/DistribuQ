import logging
from typing import Optional
from uuid import UUID
import redis.asyncio as aioredis

from api.config import settings

logger = logging.getLogger("distribuq.api.redis")

redis_client: Optional[aioredis.Redis] = None


async def init_redis() -> aioredis.Redis:
    global redis_client
    if redis_client is None:
        logger.info("Initializing async Redis client...")
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True
        )
        await redis_client.ping()
        logger.info("Redis connected successfully.")
    return redis_client


async def close_redis() -> None:
    global redis_client
    if redis_client is not None:
        logger.info("Closing Redis connection...")
        await redis_client.aclose()
        redis_client = None
        logger.info("Redis connection closed.")


def get_redis() -> aioredis.Redis:
    if redis_client is None:
        raise RuntimeError("Redis client is not initialized.")
    return redis_client


async def enqueue_job(job_id: UUID, queue: str = settings.DEFAULT_QUEUE, client: Optional[aioredis.Redis] = None) -> int:
    """
    Pushes a job UUID reference onto the specified Redis queue using LPUSH.
    Workers dequeue from the right side via BRPOP or BRPOPLPUSH (FIFO).
    """
    c = client or get_redis()
    # LPUSH puts the new item at head; BRPOPLPUSH pops from tail -> FIFO
    count = await c.lpush(queue, str(job_id))
    logger.debug("Enqueued job %s onto %s (queue length: %d)", job_id, queue, count)
    return count


async def atomic_reserve_job(
    source_queue: str = settings.DEFAULT_QUEUE,
    processing_queue: str = settings.DEFAULT_PROCESSING_QUEUE,
    timeout: int = 1,
    client: Optional[aioredis.Redis] = None,
) -> Optional[UUID]:
    """
    Atomically moves a job UUID from source_queue to processing_queue using BRPOPLPUSH.
    Prevents job loss if a worker crashes before finishing execution.
    Returns the UUID of the popped job or None if timeout expired.
    """
    c = client or get_redis()
    job_id_str = await c.brpoplpush(source_queue, processing_queue, timeout=timeout)
    if job_id_str:
        return UUID(job_id_str)
    return None


async def ack_job(
    job_id: UUID,
    processing_queue: str = settings.DEFAULT_PROCESSING_QUEUE,
    client: Optional[aioredis.Redis] = None,
) -> int:
    """
    Removes a completed or failed job from the processing list.
    """
    c = client or get_redis()
    removed = await c.lrem(processing_queue, 1, str(job_id))
    logger.debug("Acknowledged and removed job %s from %s (removed: %d)", job_id, processing_queue, removed)
    return removed


async def requeue_job(
    job_id: UUID,
    processing_queue: str = settings.DEFAULT_PROCESSING_QUEUE,
    target_queue: str = settings.DEFAULT_QUEUE,
    client: Optional[aioredis.Redis] = None,
) -> None:
    """
    Removes an unacknowledged job from processing_queue and prepends it back to target_queue.
    """
    c = client or get_redis()
    await c.lrem(processing_queue, 1, str(job_id))
    await c.rpush(target_queue, str(job_id))
    logger.info("Requeued stale job %s from %s back to %s", job_id, processing_queue, target_queue)

