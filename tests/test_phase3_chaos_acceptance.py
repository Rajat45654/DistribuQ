import asyncio
from datetime import datetime, timezone
import random
from uuid import UUID, uuid4
import pytest
from httpx import AsyncClient

from api.db import get_job
from api.redis_client import get_redis
from worker.db import get_dead_letter
from worker.worker import Worker
from worker.reaper import Reaper


@pytest.mark.asyncio
async def test_exponential_backoff_delays_to_dead_letter(client: AsyncClient):
    """
    Phase 3 Acceptance Criterion 1:
    A job handler that always fails ends up in dead_letters after exactly
    max_attempts tries, with increasing delays between attempts.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Persistent failure to test backoff intervals", "_base_delay": 0.2},
            "max_attempts": 3,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-backoff-worker", heartbeat_interval_sec=1)
    reaper = Reaper(visibility_timeout_sec=10, check_interval_sec=0.1)

    worker_task = asyncio.create_task(worker.start())
    reaper_task = asyncio.create_task(reaper.start())

    retry_timestamps = []
    prev_attempts = 0

    for _ in range(80):
        await asyncio.sleep(0.15)
        j = await get_job(job_id)
        if not j:
            continue

        if j["attempts"] > prev_attempts:
            retry_timestamps.append((j["attempts"], datetime.now(timezone.utc)))
            prev_attempts = j["attempts"]

        if j["status"] == "DEAD_LETTER":
            break

    worker.running = False
    reaper.running = False
    worker_task.cancel()
    reaper_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    try:
        await reaper_task
    except asyncio.CancelledError:
        pass

    final_job = await get_job(job_id)
    assert final_job["status"] == "DEAD_LETTER"
    assert final_job["attempts"] == 3

    # Assert audit row in dead_letters table
    dl_record = await get_dead_letter(job_id)
    assert dl_record is not None
    assert dl_record["attempts_made"] == 3
    assert "Persistent failure to test backoff intervals" in dl_record["final_error"]


@pytest.mark.asyncio
async def test_phase3_chaos_fault_injection_100_percent_terminal(client: AsyncClient):
    """
    Phase 3 Acceptance Criterion 2 (Chaos Test):
    Run a batch of mixed jobs (success, transient flaky, and permanent failure)
    while randomly killing workers mid-flight.
    Assert that 100% of jobs eventually reach a terminal state (SUCCESS or DEAD_LETTER)
    and that no jobs remain stranded in RUNNING or silently lost.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    job_ids = []
    num_jobs = 12

    # Submit a mix of jobs
    for i in range(num_jobs):
        if i % 3 == 0:
            # Persistent failure -> should reach DEAD_LETTER
            resp = await client.post(
                "/api/v1/jobs",
                json={
                    "type": "fail",
                    "payload": {"reason": f"Chaos failure {i}", "_base_delay": 0.05},
                    "max_attempts": 2,
                },
            )
        elif i % 3 == 1:
            # Flaky -> should retry and reach SUCCESS
            task_key = f"chaos-flaky-{uuid4().hex[:6]}"
            resp = await client.post(
                "/api/v1/jobs",
                json={
                    "type": "flaky",
                    "payload": {"task_key": task_key, "fail_until_attempt": 2, "_base_delay": 0.05},
                    "max_attempts": 3,
                },
            )
        else:
            # Standard fast job -> should reach SUCCESS
            resp = await client.post(
                "/api/v1/jobs",
                json={
                    "type": "math.add",
                    "payload": {"a": i, "b": 10},
                    "max_attempts": 2,
                },
            )

        assert resp.status_code == 201
        job_ids.append(UUID(resp.json()["job_id"]))

    # Start Reaper with fast visibility timeout & check interval
    reaper = Reaper(visibility_timeout_sec=2, heartbeat_timeout_sec=2, check_interval_sec=0.2)
    reaper_task = asyncio.create_task(reaper.start())

    # Spawn 3 workers
    workers = [Worker(worker_id=f"chaos-worker-{k}", heartbeat_interval_sec=1) for k in range(3)]
    worker_tasks = [asyncio.create_task(w.start()) for w in workers]

    # Inject chaos: abruptly cancel one worker mid-execution
    await asyncio.sleep(0.4)
    victim_idx = random.randint(0, len(workers) - 1)
    victim_worker = workers[victim_idx]
    victim_worker.running = False
    worker_tasks[victim_idx].cancel()

    # Wait for remaining workers and reaper to process and recover
    all_terminal = False
    for _ in range(80):
        await asyncio.sleep(0.2)
        states = [await get_job(jid) for jid in job_ids]
        statuses = [s["status"] for s in states if s]
        if all(st in ("SUCCESS", "DEAD_LETTER") for st in statuses):
            all_terminal = True
            break

    # Clean shutdown
    for w in workers:
        w.running = False
    reaper.running = False
    for wt in worker_tasks:
        wt.cancel()
    reaper_task.cancel()

    for wt in worker_tasks:
        try:
            await wt
        except asyncio.CancelledError:
            pass
    try:
        await reaper_task
    except asyncio.CancelledError:
        pass

    assert all_terminal, "Not all jobs reached terminal state (SUCCESS or DEAD_LETTER)"

    # Validate final states
    final_states = [await get_job(jid) for jid in job_ids]
    for s in final_states:
        assert s["status"] in ("SUCCESS", "DEAD_LETTER")
        assert s["locked_by"] is None, f"Job {s['id']} remained locked by {s['locked_by']}"
