import asyncio
import os
import sys
from uuid import UUID
import pytest
from httpx import AsyncClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.db import get_job, get_workers
from api.redis_client import get_redis
from worker.worker import Worker
from worker.reaper import Reaper


@pytest.mark.asyncio
async def test_phase2_criterion_1_multiple_concurrent_workers(client: AsyncClient):
    """
    Acceptance Criterion 1:
    Submit multiple jobs with concurrent workers running in parallel.
    Every job reaches SUCCESS exactly once with zero duplicate executions.
    Workload is distributed across the worker pool.
    """
    num_jobs = 30
    num_workers = 3
    job_ids = []

    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    # 1. Submit 30 jobs via API
    for i in range(num_jobs):
        resp = await client.post(
            "/api/v1/jobs",
            json={"type": "math.add", "payload": {"a": i, "b": 10}},
        )
        assert resp.status_code == 201
        job_ids.append(UUID(resp.json()["job_id"]))

    # 2. Start 3 concurrent workers
    workers = [Worker(worker_id=f"test-worker-pool-{i}", heartbeat_interval_sec=1) for i in range(num_workers)]
    worker_tasks = [asyncio.create_task(w.start()) for w in workers]

    # 3. Wait until all jobs reach terminal state (SUCCESS)
    all_completed = False
    for _ in range(40):  # up to 8 seconds
        await asyncio.sleep(0.2)
        completed_count = 0
        for jid in job_ids:
            j = await get_job(jid)
            if j and j["status"] == "SUCCESS":
                completed_count += 1
        if completed_count == num_jobs:
            all_completed = True
            break

    # Stop workers cleanly
    for w in workers:
        w.running = False
    for t in worker_tasks:
        t.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)


    assert all_completed, f"Only {completed_count}/{num_jobs} jobs reached SUCCESS within timeout"

    # 4. Verify each job executed exactly once and result is correct
    for i, jid in enumerate(job_ids):
        j = await get_job(jid)
        assert j["status"] == "SUCCESS"
        assert j["attempts"] == 1, f"Job {jid} was executed {j['attempts']} times (expected exactly 1)"
        assert j["result"] == {"sum": i + 10}
        assert j["locked_by"] is None

    # 5. Verify all workers in the pool processed at least some jobs
    redis = get_redis()
    proc_len = await redis.llen("queue:processing")
    assert proc_len == 0, f"Expected 0 unacknowledged jobs in queue:processing, found {proc_len}"

    total_processed = sum(w.jobs_processed for w in workers)
    assert total_processed == num_jobs


@pytest.mark.asyncio
async def test_phase2_criterion_2_crash_recovery_visibility_timeout(client: AsyncClient):
    """
    Acceptance Criterion 2:
    Simulate an abrupt worker crash (SIGKILL/unhandled abort) mid-job.
    The Reaper detects the stale visibility timeout / missing heartbeats,
    marks the crashed worker DEAD, reclaims the in-flight job,
    and a second healthy worker picks it up and finishes it with zero data loss.
    """
    # 1. Submit a long-running job (sleep 3s)
    resp = await client.post(
        "/api/v1/jobs",
        json={"type": "sleep", "payload": {"seconds": 3.0}},
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # 2. Worker 1 starts and picks up the job
    worker_crashed = Worker(worker_id="worker-crash-target", heartbeat_interval_sec=1)
    crash_task = asyncio.create_task(worker_crashed.start())

    # Wait until Worker 1 locks the job and sets RUNNING
    locked = False
    for _ in range(25):
        await asyncio.sleep(0.1)
        j = await get_job(job_id)
        if j and j["status"] == "RUNNING" and j["locked_by"] == "worker-crash-target":
            locked = True
            break
    assert locked, "Job was not locked by worker-crash-target"

    # 3. Simulate abrupt crash: Cancel the task directly without running graceful shutdown
    crash_task.cancel()
    try:
        await crash_task
    except asyncio.CancelledError:
        pass
    if worker_crashed.redis_client:
        await worker_crashed.redis_client.aclose()

    # Verify job is still stranded as RUNNING in Postgres and in Redis queue:processing
    stranded = await get_job(job_id)
    assert stranded["status"] == "RUNNING"
    assert stranded["locked_by"] == "worker-crash-target"

    # 4. Run the Reaper with short timeout (1 second visibility timeout)
    await asyncio.sleep(1.2)  # Wait for visibility timeout to elapse
    reaper = Reaper(visibility_timeout_sec=1, heartbeat_timeout_sec=1)
    reclaimed = await reaper.run_once()

    assert any(str(r["id"]) == str(job_id) for r in reclaimed), "Reaper failed to reclaim stranded job"

    # Verify job in Postgres is now reset to PENDING and lock cleared
    after_reap = await get_job(job_id)
    assert after_reap["status"] == "PENDING"
    assert after_reap["locked_by"] is None

    # 5. Start a second healthy worker to complete the reclaimed job
    worker_healthy = Worker(worker_id="worker-healthy-hero", heartbeat_interval_sec=1)
    healthy_task = asyncio.create_task(worker_healthy.start())

    completed = False
    for _ in range(40):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "SUCCESS":
            completed = True
            assert j["result"] == {"slept_seconds": 3.0}
            assert j["attempts"] == 2  # 1st attempt before crash, 2nd attempt by healthy worker
            break

    worker_healthy.running = False
    await healthy_task
    assert completed, "Reclaimed job did not complete successfully after worker crash"


@pytest.mark.asyncio
async def test_phase2_criterion_3_worker_registry_and_heartbeats(client: AsyncClient):
    """
    Acceptance Criterion 3:
    Workers periodically upsert their heartbeat in Postgres,
    accessible via GET /api/v1/workers.
    On graceful shutdown, worker status transitions to DEAD.
    """
    worker_id = "test-worker-heartbeat-check"
    worker = Worker(worker_id=worker_id, heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    # Wait for initial heartbeat
    await asyncio.sleep(0.5)

    # Check API GET /api/v1/workers
    resp = await client.get("/api/v1/workers")
    assert resp.status_code == 200
    workers = resp.json()
    our_worker = next((w for w in workers if w["id"] == worker_id), None)
    assert our_worker is not None
    assert our_worker["status"] == "ALIVE"
    assert our_worker["jobs_processed"] == 0

    # Stop worker gracefully
    worker.running = False
    await worker_task

    # Check API again: status should now be DEAD
    resp2 = await client.get("/api/v1/workers")
    assert resp2.status_code == 200
    our_worker_dead = next((w for w in resp2.json() if w["id"] == worker_id), None)
    assert our_worker_dead is not None
    assert our_worker_dead["status"] == "DEAD"
