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
from api.redis_client import atomic_reserve_job, ack_job
from worker.db import (
    init_worker_db_pool,
    close_worker_db_pool,
    fetch_and_lock_job,
    mark_job_success,
    mark_job_failed,
    mark_job_retrying,
)
from worker.handlers.registry import get_handler, execute_handler
import worker.handlers.default_handlers  # ensure default handlers are registered
from worker.heartbeat import HeartbeatManager
from worker.retry import compute_next_retry_at


logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("distribuq.worker")


class Worker:
    def __init__(
        self,
        queue: str = settings.DEFAULT_QUEUE,
        processing_queue: str = settings.DEFAULT_PROCESSING_QUEUE,
        worker_id: str | None = None,
        heartbeat_interval_sec: int = settings.HEARTBEAT_INTERVAL_SEC,
    ):
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid4().hex[:6]}"
        self.queue = queue
        self.processing_queue = processing_queue
        self.running = False
        self.jobs_processed = 0
        self.redis_client: aioredis.Redis | None = None
        self.heartbeat = HeartbeatManager(
            worker_id=self.worker_id,
            get_jobs_processed=lambda: self.jobs_processed,
            interval_sec=heartbeat_interval_sec,
        )

    def handle_signal(self, signum, frame):
        logger.info("[%s] Received shutdown signal (%s). Initiating graceful shutdown...", self.worker_id, signum)
        self.running = False

    async def start(self):
        self.running = True
        logger.info(
            "[%s] Worker starting up. Listening on '%s' (processing: '%s')...",
            self.worker_id,
            self.queue,
            self.processing_queue,
        )

        # Setup OS signal handling
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self.handle_signal)
            except Exception:
                pass

        await init_worker_db_pool()
        self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await self.redis_client.ping()

        # Start periodic background heartbeats
        await self.heartbeat.start()

        try:
            while self.running:
                try:
                    # Atomically reserve job from source queue into processing queue
                    job_id = await atomic_reserve_job(
                        source_queue=self.queue,
                        processing_queue=self.processing_queue,
                        timeout=1,
                        client=self.redis_client,
                    )
                    if not job_id:
                        continue

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
        logger.info("[%s] Reserved job %s in %s", self.worker_id, job_id, self.processing_queue)
        job = await fetch_and_lock_job(job_id, self.worker_id)
        if not job:
            logger.warning("[%s] Job %s could not be locked (may be cancelled or already processed)", self.worker_id, job_id)
            await ack_job(job_id, self.processing_queue, self.redis_client)
            return

        job_type = job["type"]
        payload = job["payload"] or {}
        logger.info(
            "[%s] Processing job %s (type: %s, attempt %d/%d)",
            self.worker_id,
            job_id,
            job_type,
            job["attempts"],
            job["max_attempts"],
        )

        try:
            handler = get_handler(job_type)
            result = await execute_handler(handler, payload)
            await mark_job_success(job_id, result)
            self.jobs_processed += 1
            logger.info("[%s] Job %s COMPLETED successfully with result: %s", self.worker_id, job_id, result)
        except Exception as err:
            logger.error("[%s] Job %s FAILED with error: %s", self.worker_id, job_id, err)
            current_attempts = job.get("attempts", 1)
            max_attempts = job.get("max_attempts", 3)

            if current_attempts < max_attempts:
                next_retry = compute_next_retry_at(attempt=current_attempts)
                await mark_job_retrying(job_id, str(err), next_retry)
                logger.warning(
                    "[%s] Job %s scheduled for retry (attempt %d/%d) at %s",
                    self.worker_id,
                    job_id,
                    current_attempts,
                    max_attempts,
                    next_retry,
                )
            else:
                await mark_job_failed(job_id, str(err))

            self.jobs_processed += 1
        finally:

            # Acknowledge and remove from the processing list once persisted to Postgres
            await ack_job(job_id, self.processing_queue, self.redis_client)

    async def shutdown(self):
        logger.info("[%s] Shutting down worker...", self.worker_id)
        if self.heartbeat:
            await self.heartbeat.stop()
        if self.redis_client:
            await self.redis_client.aclose()
            self.redis_client = None
        logger.info("[%s] Worker shutdown complete.", self.worker_id)


if __name__ == "__main__":
    import selectors
    worker = Worker()

    async def run_worker():
        try:
            await worker.start()
        finally:
            await close_worker_db_pool()

    try:
        if sys.platform == "win32":
            asyncio.run(run_worker(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
        else:
            asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass

