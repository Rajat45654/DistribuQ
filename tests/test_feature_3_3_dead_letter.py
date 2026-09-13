import asyncio
from datetime import datetime, timezone
from uuid import UUID
import pytest
from httpx import AsyncClient

from api.db import get_job
from api.redis_client import get_redis
from worker.db import get_dead_letter
from worker.worker import Worker
from worker.reaper import Reaper


@pytest.mark.asyncio
async def test_job_moves_to_dead_letter_on_single_attempt_exhaustion(client: AsyncClient):
    """
    Feature 3.3 Acceptance Criterion:
    When a job with max_attempts=1 fails, it immediately transitions to DEAD_LETTER
    and creates an audit record in the dead_letters table.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Immediate fatal crash"},
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-dlq-worker-1", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    dead_lettered = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "DEAD_LETTER":
            dead_lettered = True
            assert j["attempts"] == 1
            assert "Immediate fatal crash" in (j["error"] or "")
            assert j["locked_by"] is None
            assert j["locked_at"] is None
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    assert dead_lettered, "Job failed to transition to DEAD_LETTER after 1 attempt"

    # Verify audit row in dead_letters table
    dlq_record = await get_dead_letter(job_id)
    assert dlq_record is not None
    assert dlq_record["job_id"] == job_id
    assert dlq_record["attempts_made"] == 1
    assert "Immediate fatal crash" in dlq_record["final_error"]
    assert isinstance(dlq_record["moved_at"], datetime)


@pytest.mark.asyncio
async def test_job_retries_and_lands_in_dead_letter_queue(client: AsyncClient):
    """
    Feature 3.3 Acceptance Criterion:
    A job configured with max_attempts=3 that always fails retries through exponential
    backoff and, upon exhausting all 3 attempts, is moved to the dead_letters table.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {
                "reason": "Persistent hardware failure",
                "_base_delay": 0.05,  # fast backoff for testing
            },
            "max_attempts": 3,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-dlq-worker-multi", heartbeat_interval_sec=1)
    reaper = Reaper(visibility_timeout_sec=10, check_interval_sec=0.1)

    worker_task = asyncio.create_task(worker.start())
    reaper_task = asyncio.create_task(reaper.start())

    dead_lettered = False
    for _ in range(60):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "DEAD_LETTER":
            dead_lettered = True
            assert j["attempts"] == 3
            assert "Persistent hardware failure" in (j["error"] or "")
            assert j["locked_by"] is None
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

    assert dead_lettered, "Job failed to reach DEAD_LETTER status after 3 attempts"

    # Verify dead_letters table row
    dlq_record = await get_dead_letter(job_id)
    assert dlq_record is not None
    assert dlq_record["job_id"] == job_id
    assert dlq_record["attempts_made"] == 3
    assert "Persistent hardware failure" in dlq_record["final_error"]
    assert isinstance(dlq_record["moved_at"], datetime)
