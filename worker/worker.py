import asyncio
import logging
import os
import signal
import sys
from uuid import UUID, uuid4
import redis.asyncio as aioredis

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.config import settings
from worker.db import (
    init_worker_db_pool,
    close_worker_db_pool,
    fetch_and_lock_job,
    mark_job_success,
    mark_job_failed,
)
from worker.handlers.registry import get_handler, execute_handler
import worker.handlers.default_handlers  # ensure default handlers are registered

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("distribuq.worker")


class Worker:
    def __init__(self, queue: str = settings.DEFAULT_QUEUE, worker_id: str | None = None):
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid4().hex[:6]}"
        self.queue = queue
        self.running = False
        self.redis_client: aioredis.Redis | None = None

    def handle_signal(self, signum, frame):
        logger.info("[%s] Received shutdown signal (%s). Initiating graceful shutdown...", self.worker_id, signum)
        self.running = False

    async def start(self):
        self.running = True
        logger.info("[%s] Worker starting up. Listening on queue '%s'...", self.worker_id, self.queue)

        # Setup OS signal handling
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self.handle_signal)
            except Exception:
                pass

        await init_worker_db_pool()
        self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await self.redis_client.ping()

        try:
            while self.running:
                try:
                    # Pop from queue (blocks up to 1 second to allow checking self.running)
                    pop_result = await self.redis_client.brpop(self.queue, timeout=1)
                    if not pop_result:
                        continue

                    _, job_id_str = pop_result
                    job_id = UUID(job_id_str)
                    await self.process_job(job_id)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self.running:
                        logger.error("[%s] Unexpected error in worker loop: %s", self.worker_id, e, exc_info=True)
                        await asyncio.sleep(0.5)

        finally:
            await self.shutdown()

    async def process_job(self, job_id: UUID):
        logger.info("[%s] Picked up job %s from Redis", self.worker_id, job_id)
        job = await fetch_and_lock_job(job_id, self.worker_id)
        if not job:
            logger.warning("[%s] Job %s could not be locked (may be cancelled or already processed)", self.worker_id, job_id)
            return

        job_type = job["type"]
        payload = job["payload"] or {}
        logger.info("[%s] Processing job %s (type: %s, attempt %d/%d)", self.worker_id, job_id, job_type, job["attempts"], job["max_attempts"])

        try:
            handler = get_handler(job_type)
            result = await execute_handler(handler, payload)
            await mark_job_success(job_id, result)
            logger.info("[%s] Job %s COMPLETED successfully with result: %s", self.worker_id, job_id, result)
        except Exception as err:
            logger.error("[%s] Job %s FAILED with error: %s", self.worker_id, job_id, err)
            await mark_job_failed(job_id, str(err))

    async def shutdown(self):
        logger.info("[%s] Shutting down worker...", self.worker_id)
        if self.redis_client:
            await self.redis_client.aclose()
        await close_worker_db_pool()
        logger.info("[%s] Worker shutdown complete.", self.worker_id)


if __name__ == "__main__":
    import selectors
    worker = Worker()
    try:
        if sys.platform == "win32":
            asyncio.run(worker.start(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        else:
            asyncio.run(worker.start())
    except KeyboardInterrupt:
        pass
