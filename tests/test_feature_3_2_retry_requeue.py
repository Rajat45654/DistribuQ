import asyncio
import os
import sys
from datetime import datetime, timezone
from uuid import UUID
import pytest
from httpx import AsyncClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.db import get_job, get_db_pool
from api.redis_client import get_redis
from worker.reaper import Reaper
from worker.worker import Worker


@pytest.mark.asyncio
async def test_reaper_reclaims_due_retry_jobs(client: AsyncClient):
    """
    Verifies that Reaper.run_once finds jobs in RETRYING state where
    next_retry_at <= NOW(), resets their status to PENDING, and pushes
    them back to the main Redis queue.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    # 1. Submit a job
    resp = await client.post(
        "/api/v1/jobs",
        json={"type": "echo", "payload": {"message": "retry me"}},
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # 2. Clean old RETRYING jobs from prior tests and set our job to RETRYING
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE jobs SET status = 'FAILED' WHERE status = 'RETRYING';")
            await cur.execute(
                """
                UPDATE jobs
                SET status = 'RETRYING',
                    next_retry_at = NOW() - INTERVAL '5 seconds',
                    locked_by = NULL,
                    locked_at = NULL
                WHERE id = %s;
                """,
                (job_id,),
            )
            await conn.commit()

    # Clear queue so it only contains re-enqueued jobs
    await redis.delete("queue:default", "queue:processing")

    # 3. Run Reaper once
    reaper = Reaper()
    await reaper.run_once()

    # 4. Verify job in Postgres is now PENDING and next_retry_at was cleared/due
    j = await get_job(job_id)
    assert j["status"] == "PENDING"
    assert j["locked_by"] is None

    # 5. Verify job is present in Redis queue
    queued_items = await redis.lrange("queue:default", 0, -1)
    assert str(job_id) in queued_items



@pytest.mark.asyncio
async def test_end_to_end_flaky_job_retries_and_succeeds(client: AsyncClient):
    """
    End-to-end retry cycle:
    1. Submits a job that fails on attempt 1.
    2. Worker marks it RETRYING.
    3. Manually advance or wait for next_retry_at to mature.
    4. Reaper re-enqueues the job.
    5. Worker picks up attempt 2 and succeeds!
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    task_key = f"flaky_test_{int(datetime.now(timezone.utc).timestamp())}"
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "flaky",
            "payload": {"task_key": task_key, "fail_until_attempt": 2},
            "max_attempts": 3,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="worker-flaky-test", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    # Step A: Wait for worker to fail attempt 1 and enter RETRYING
    retried = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "RETRYING":
            retried = True
            assert j["attempts"] == 1
            break
    assert retried, "Job did not enter RETRYING status after attempt 1"

    # Step B: Fast-forward next_retry_at to NOW() so Reaper picks it up immediately
    pool = get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE jobs SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = %s;",
                (job_id,),
            )
            await conn.commit()

    # Step C: Run Reaper pass to promote the due retry job back to Redis
    reaper = Reaper()
    await reaper.run_once()

    # Step D: Wait for worker to pick up attempt 2 and succeed
    succeeded = False
    for _ in range(35):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "SUCCESS":
            succeeded = True
            assert j["attempts"] == 2
            assert j["result"] == {"recovered_at_attempt": 2, "task_key": task_key}
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    assert succeeded, "Job did not reach SUCCESS on attempt 2 after retry re-enqueue"
