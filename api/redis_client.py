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


async def enqueue_job(job_id: UUID, queue: str = settings.DEFAULT_QUEUE) -> int:
    """
    Pushes a job UUID reference onto the specified Redis queue using LPUSH.
    Workers dequeue from the right side via BRPOP (FIFO).
    """
    client = get_redis()
    # LPUSH puts the new item at head; BRPOP pops from tail -> FIFO
    count = await client.lpush(queue, str(job_id))
    logger.debug("Enqueued job %s onto %s (queue length: %d)", job_id, queue, count)
    return count
