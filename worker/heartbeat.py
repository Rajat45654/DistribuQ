import asyncio
import logging
from typing import Callable, Optional

from api.config import settings
from worker.db import upsert_worker_heartbeat, mark_worker_dead

logger = logging.getLogger("distribuq.worker.heartbeat")


class HeartbeatManager:
    def __init__(
        self,
        worker_id: str,
        get_jobs_processed: Callable[[], int],
        interval_sec: int = settings.HEARTBEAT_INTERVAL_SEC,
    ):
        self.worker_id = worker_id
        self.get_jobs_processed = get_jobs_processed
        self.interval_sec = interval_sec
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the background heartbeat loop."""
        self.running = True
        # Send initial heartbeat immediately
        try:
            await upsert_worker_heartbeat(self.worker_id, self.get_jobs_processed())
            logger.info("[%s] Registered initial heartbeat (interval: %ds).", self.worker_id, self.interval_sec)
        except Exception as e:
            logger.error("[%s] Failed to send initial heartbeat: %s", self.worker_id, e)

        self._task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while self.running:
            try:
                await asyncio.sleep(self.interval_sec)
                if not self.running:
                    break
                await upsert_worker_heartbeat(self.worker_id, self.get_jobs_processed())
                logger.debug("[%s] Heartbeat sent (jobs_processed=%d).", self.worker_id, self.get_jobs_processed())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("[%s] Error sending heartbeat: %s", self.worker_id, e)

    async def stop(self) -> None:
        """Gracefully stops heartbeats and marks the worker DEAD in Postgres."""
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        try:
            await mark_worker_dead(self.worker_id)
            logger.info("[%s] Worker marked DEAD in registry.", self.worker_id)
        except Exception as e:
            logger.error("[%s] Failed to mark worker DEAD on shutdown: %s", self.worker_id, e)
