import asyncio
import logging
import signal
import sys
from typing import List, Optional
from uuid import UUID
import redis.asyncio as aioredis

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.config import settings
from api.redis_client import promote_due_scheduled_jobs, schedule_job
from worker.db import init_worker_db_pool, close_worker_db_pool, get_pending_scheduled_jobs

logger = logging.getLogger("distribuq.scheduler")


class Scheduler:
    """
    Dedicated scheduler process that manages delayed and recurring jobs.
    Monitors the Redis scheduled sorted set (zset:scheduled) and promotes
    due jobs into the ready queue (queue:default) once scheduled_for <= NOW().
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
                await self.run_once()
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
