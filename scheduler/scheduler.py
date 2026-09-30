import asyncio
import logging
import signal
import sys
import time
from typing import List, Optional
from uuid import UUID
import redis.asyncio as aioredis

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.config import settings
from api.redis_client import (
    promote_due_scheduled_jobs,
    schedule_job,
    enqueue_job,
    requeue_job,
)
from worker.db import (
    init_worker_db_pool,
    close_worker_db_pool,
    get_pending_scheduled_jobs,
    reclaim_due_retry_jobs,
    reclaim_stale_jobs,
    mark_expired_workers_dead,
)

logger = logging.getLogger("distribuq.scheduler")


class Scheduler:
    """
    Dedicated scheduler process that manages:
    1. Delayed & recurring jobs: monitors Redis scheduled sorted set (zset:scheduled).
    2. Retry backoff promotion: monitors Postgres jobs where next_retry_at <= NOW().
    3. Stale job reclamation: recovers jobs stranded by crashed/dead workers.
    """

    def __init__(
        self,
        poll_interval_sec: float = 0.5,
        target_queue: str = settings.DEFAULT_QUEUE,
        scheduled_set: str = settings.DEFAULT_SCHEDULED_SET,
        redis_client: Optional[aioredis.Redis] = None,
    ):
        self.poll_interval_sec = poll_interval_sec
        self.target_queue = target_queue
        self.scheduled_set = scheduled_set
        self.redis_client = redis_client
        self.running = False
        self._last_reaper_run = 0.0

    def handle_signal(self, signum, frame):
        logger.info("[Scheduler] Received shutdown signal (%s). Stopping scheduler...", signum)
        self.running = False

    async def sync_from_db(self) -> int:
        """Syncs all PENDING scheduled jobs from Postgres into Redis sorted set on startup."""
        try:
            pending_jobs = await get_pending_scheduled_jobs()
            count = 0
            for j in pending_jobs:
                if j.get("scheduled_for"):
                    await schedule_job(
                        job_id=j["id"],
                        scheduled_for=j["scheduled_for"],
                        scheduled_set=self.scheduled_set,
                        client=self.redis_client,
                    )
                    count += 1
            if count > 0:
                logger.info("[Scheduler] Synced %d scheduled jobs from PostgreSQL into %s", count, self.scheduled_set)
            return count
        except Exception as err:
            logger.error("[Scheduler] Error syncing scheduled jobs from database: %s", err)
            return 0

    async def run_once(self) -> List[UUID]:
        """Promotes due jobs from the scheduled set to the target queue."""
        promoted = await promote_due_scheduled_jobs(
            target_queue=self.target_queue,
            scheduled_set=self.scheduled_set,
            client=self.redis_client,
        )
        return promoted

    async def promote_retries(self) -> int:
        """Promotes jobs in RETRYING status whose next_retry_at is due back into Redis."""
        try:
            due_retries = await reclaim_due_retry_jobs()
            for rjob in due_retries:
                job_id = UUID(str(rjob["id"]))
                await enqueue_job(job_id=job_id, queue=self.target_queue, client=self.redis_client)
                logger.info(
                    "[Scheduler] Promoted retry job %s (attempt %d/%d) to %s",
                    job_id, rjob["attempts"], rjob["max_attempts"], self.target_queue,
                )
            return len(due_retries)
        except Exception as e:
            logger.error("[Scheduler] Error promoting retry jobs: %s", e)
            return 0

    async def run_reaper(self) -> None:
        """Recovers stranded jobs and marks dead workers."""
        try:
            await mark_expired_workers_dead(settings.WORKER_TIMEOUT_SEC)
            stale_jobs = await reclaim_stale_jobs(settings.DEFAULT_VISIBILITY_TIMEOUT_SEC)
            for sjob in stale_jobs:
                job_id = UUID(str(sjob["id"]))
                await requeue_job(
                    job_id=job_id,
                    processing_queue=settings.DEFAULT_PROCESSING_QUEUE,
                    target_queue=self.target_queue,
                    client=self.redis_client,
                )
                logger.warning("[Scheduler] Reclaimed stranded job %s back to %s", job_id, self.target_queue)
        except Exception as e:
            logger.error("[Scheduler] Error in reaper cycle: %s", e)

    async def start(self):
        self.running = True
        logger.info(
            "[Scheduler] Scheduler process started (poll_interval: %ss, target: %s, set: %s)",
            self.poll_interval_sec,
            self.target_queue,
            self.scheduled_set,
        )

        if not self.redis_client:
            self.redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

        await self.sync_from_db()

        while self.running:
            try:
                # 1. Promote delayed jobs
                await self.run_once()

                # 2. Promote due retry jobs
                await self.promote_retries()

                # 3. Periodic reaper pass (every 5 seconds)
                now = time.monotonic()
                if now - self._last_reaper_run >= 5.0:
                    self._last_reaper_run = now
                    await self.run_reaper()

                await asyncio.sleep(self.poll_interval_sec)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self.running:
                    logger.error("[Scheduler] Error in scheduler loop: %s", e, exc_info=True)
                    await asyncio.sleep(self.poll_interval_sec)

        await self.shutdown()

    async def shutdown(self):
        logger.info("[Scheduler] Scheduler shutting down...")
        if self.redis_client:
            await self.redis_client.aclose()
            self.redis_client = None
        logger.info("[Scheduler] Scheduler shutdown complete.")


async def main():
    await init_worker_db_pool()
    scheduler = Scheduler()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, scheduler.handle_signal)
        except (ValueError, AttributeError):
            pass

    try:
        await scheduler.start()
    finally:
        await close_worker_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
