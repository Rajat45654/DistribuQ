import asyncio
import logging
from typing import Optional
from uuid import UUID
import redis.asyncio as aioredis

from api.config import settings
from api.redis_client import requeue_job, get_redis
from worker.db import (
    get_worker_db_pool,
    init_worker_db_pool,
    close_worker_db_pool,
    mark_expired_workers_dead,
    reclaim_stale_jobs,
    reclaim_due_retry_jobs,
)

logger = logging.getLogger("distribuq.worker.reaper")


class Reaper:
    """
    Monitors worker health and in-flight job locks.
    If a worker fails to send heartbeats or a job exceeds its visibility timeout,
    the Reaper marks the worker DEAD, resets the job to PENDING in Postgres,
    and returns it to the main Redis queue for another worker to process.
    """

    def __init__(
        self,
        visibility_timeout_sec: int = settings.DEFAULT_VISIBILITY_TIMEOUT_SEC,
        heartbeat_timeout_sec: int = settings.WORKER_TIMEOUT_SEC,
        check_interval_sec: float = 5.0,
        source_queue: str = settings.DEFAULT_QUEUE,
        processing_queue: str = settings.DEFAULT_PROCESSING_QUEUE,
        redis_client: Optional[aioredis.Redis] = None,
    ):
        self.visibility_timeout_sec = visibility_timeout_sec
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.check_interval_sec = check_interval_sec
        self.source_queue = source_queue
        self.processing_queue = processing_queue
        self.redis_client = redis_client
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def run_once(self) -> list[dict]:
        """
        Runs a single pass of worker liveness check and stale job reclamation.
        Returns the list of reclaimed job records.
        """
        # 1. Mark expired workers as DEAD
        dead_workers = await mark_expired_workers_dead(self.heartbeat_timeout_sec)
        if dead_workers:
            logger.warning("[Reaper] Detected %d timed-out workers and marked them DEAD: %s", len(dead_workers), dead_workers)

        # 2. Reclaim stale RUNNING jobs in Postgres
        reclaimed_jobs = await reclaim_stale_jobs(self.visibility_timeout_sec)
        if reclaimed_jobs:
            logger.warning("[Reaper] Reclaimed %d stranded jobs: %s", len(reclaimed_jobs), [j["id"] for j in reclaimed_jobs])

            # 3. Ensure reclaimed jobs are moved in Redis back to the source queue
            for job in reclaimed_jobs:
                job_id = UUID(str(job["id"]))
                try:
                    await requeue_job(
                        job_id=job_id,
                        processing_queue=self.processing_queue,
                        target_queue=self.source_queue,
                        client=self.redis_client,
                    )
                except Exception as e:
                    logger.error("[Reaper] Failed to requeue job %s in Redis: %s", job_id, e)

        # 4. Reclaim due RETRYING jobs and push them back into Redis source queue
        due_retries = await reclaim_due_retry_jobs()
        if due_retries:
            logger.info("[Reaper] Found %d due retry jobs to re-enqueue", len(due_retries))
            c = self.redis_client or get_redis()
            for rjob in due_retries:
                r_id = UUID(str(rjob["id"]))
                try:
                    await c.lpush(self.source_queue, str(r_id))
                    logger.info(
                        "[Reaper] Re-enqueued retry job %s (attempt %d/%d) onto %s",
                        r_id,
                        rjob["attempts"],
                        rjob["max_attempts"],
                        self.source_queue,
                    )
                except Exception as e:
                    logger.error("[Reaper] Failed to re-enqueue retry job %s: %s", r_id, e)

        return reclaimed_jobs


    async def start(self) -> None:
        """Starts the periodic reaper loop."""
        self.running = True
        logger.info(
            "[Reaper] Starting reaper loop (interval: %.1fs, visibility_timeout: %ds, heartbeat_timeout: %ds)...",
            self.check_interval_sec,
            self.visibility_timeout_sec,
            self.heartbeat_timeout_sec,
        )

        if self.redis_client is None:
            self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while self.running:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Reaper] Error during reaper execution cycle: %s", e, exc_info=True)

            try:
                await asyncio.sleep(self.check_interval_sec)
            except asyncio.CancelledError:
                break

    async def stop(self) -> None:
        """Stops the reaper loop."""
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Reaper] Reaper loop stopped.")


if __name__ == "__main__":
    import selectors
    import sys

    async def main():
        await init_worker_db_pool()
        reaper = Reaper()
        await reaper.start()
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await reaper.stop()
            await close_worker_db_pool()

    if sys.platform == "win32":
        asyncio.run(main(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
    else:
        asyncio.run(main())
