import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID
import pytest
from httpx import AsyncClient

from api.db import get_job
from api.redis_client import get_redis
from scheduler.scheduler import Scheduler
from worker.worker import Worker


@pytest.mark.asyncio
async def test_delayed_job_does_not_execute_before_scheduled_time(client: AsyncClient):
    """
    Phase 4 Acceptance Criterion:
    A job scheduled in the future stays out of the ready queue and is NOT executed
    by active workers before its scheduled_for timestamp.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing", "zset:scheduled")

    scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=3.0)
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "math.add",
            "payload": {"a": 10, "b": 20},
            "scheduled_for": scheduled_time.isoformat(),
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # Start an active worker
    worker = Worker(worker_id="test-worker-delayed-early", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    # Wait 1 second (well before the 3-second schedule)
    await asyncio.sleep(1.0)

    # Verify job is still PENDING and has not been executed
    job = await get_job(job_id)
    assert job["status"] == "PENDING"
    assert job["attempts"] == 0
    assert job["result"] is None

    # Verify job is not in the ready queue
    queue_len = await redis.llen("queue:default")
    assert queue_len == 0

    # Verify job is registered in the scheduled sorted set
    score = await redis.zscore("zset:scheduled", str(job_id))
    assert score is not None
    assert abs(score - scheduled_time.timestamp()) < 1.0

    # Clean shutdown of worker
    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_scheduler_promotes_and_worker_executes_when_due(client: AsyncClient):
    """
    Phase 4 Acceptance Criterion:
    When a scheduled job becomes due (scheduled_for <= now), the Scheduler promotes
    it into the ready queue, and a worker executes it to SUCCESS.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing", "zset:scheduled")

    scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=1.0)
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "echo",
            "payload": {"message": "Hello from the future!"},
            "scheduled_for": scheduled_time.isoformat(),
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    scheduler = Scheduler(poll_interval_sec=0.1)
    worker = Worker(worker_id="test-worker-delayed-exec", heartbeat_interval_sec=1)

    scheduler_task = asyncio.create_task(scheduler.start())
    worker_task = asyncio.create_task(worker.start())

    succeeded = False
    for _ in range(40):
        await asyncio.sleep(0.15)
        j = await get_job(job_id)
        if j and j["status"] == "SUCCESS":
            succeeded = True
            assert j["result"] == {"echo": "Hello from the future!"}
            assert j["attempts"] == 1
            # Execution must happen at or after the scheduled time
            assert j["updated_at"] >= j["scheduled_for"]
            break

    scheduler.running = False
    worker.running = False
    scheduler_task.cancel()
    worker_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    assert succeeded, "Scheduled job was not promoted and executed to SUCCESS"

    # Verify job removed from scheduled set upon promotion
    remaining = await redis.zscore("zset:scheduled", str(job_id))
    assert remaining is None


@pytest.mark.asyncio
async def test_scheduler_sync_from_database(client: AsyncClient):
    """
    Verifies that the Scheduler syncs pending scheduled jobs from PostgreSQL
    into the Redis sorted set upon startup for disaster recovery.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing", "zset:scheduled")

    scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=10.0)
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "math.multiply",
            "payload": {"a": 5, "b": 5},
            "scheduled_for": scheduled_time.isoformat(),
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # Simulate Redis loss of the scheduled set
    await redis.delete("zset:scheduled")
    assert await redis.zscore("zset:scheduled", str(job_id)) is None

    # Scheduler sync restores it from PostgreSQL
    scheduler = Scheduler(poll_interval_sec=0.1)
    synced_count = await scheduler.sync_from_db()
    assert synced_count >= 1

    score = await redis.zscore("zset:scheduled", str(job_id))
    assert score is not None
