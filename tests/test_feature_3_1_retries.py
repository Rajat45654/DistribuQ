import asyncio
import os
import sys
from datetime import datetime, timezone
from uuid import UUID
import pytest
from httpx import AsyncClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.db import get_job
from api.redis_client import get_redis
from worker.retry import compute_backoff_delay, compute_next_retry_at
from worker.worker import Worker


def test_exponential_backoff_calculation():
    """Verifies that exponential backoff doubles the delay per attempt and obeys max_delay."""
    # Test deterministic delays without jitter
    assert compute_backoff_delay(attempt=1, base_delay=1.0, jitter=False) == 1.0
    assert compute_backoff_delay(attempt=2, base_delay=1.0, jitter=False) == 2.0
    assert compute_backoff_delay(attempt=3, base_delay=1.0, jitter=False) == 4.0
    assert compute_backoff_delay(attempt=4, base_delay=1.0, jitter=False) == 8.0

    # Test cap at max_delay
    assert compute_backoff_delay(attempt=10, base_delay=1.0, max_delay=30.0, jitter=False) == 30.0

    # Test with jitter (delay <= delay * 1.5)
    for att in (1, 2, 3):
        delay_with_jitter = compute_backoff_delay(attempt=att, base_delay=2.0, jitter=True)
        base = 2.0 * (2 ** (att - 1))
        assert base <= delay_with_jitter <= base * 1.5


def test_next_retry_at_timestamp():
    """Verifies that compute_next_retry_at returns a future UTC timestamp."""
    now = datetime.now(timezone.utc)
    next_retry = compute_next_retry_at(attempt=1, base_delay=5.0, jitter=False)
    diff = (next_retry - now).total_seconds()
    assert 4.5 <= diff <= 6.0


@pytest.mark.asyncio
async def test_worker_transitions_failed_job_to_retrying(client: AsyncClient):
    """
    Verifies that when a job fails and attempts < max_attempts,
    the worker marks the job RETRYING, sets next_retry_at, stores error,
    and releases the lock.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Transient network timeout"},
            "max_attempts": 3,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-retry-worker", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    # Wait for worker to pick up and process attempt 1
    retried = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "RETRYING":
            retried = True
            assert j["attempts"] == 1
            assert j["max_attempts"] == 3
            assert "Transient network timeout" in j["error"]
            assert j["next_retry_at"] is not None
            # Verify next_retry_at is in the future
            now_utc = datetime.now(timezone.utc)
            assert j["next_retry_at"] > now_utc
            assert j["locked_by"] is None
            assert j["locked_at"] is None
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    assert retried, "Job did not transition to RETRYING status on failure"


@pytest.mark.asyncio
async def test_worker_fails_job_when_max_attempts_exhausted(client: AsyncClient):
    """
    Verifies that when max_attempts == 1, failure immediately transitions
    to FAILED because no more attempts remain.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Unrecoverable single-attempt error"},
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-fail-single-worker", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    failed = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] in ("FAILED", "DEAD_LETTER"):
            failed = True
            assert j["attempts"] == 1
            assert "Unrecoverable single-attempt error" in j["error"]
            assert j["next_retry_at"] is None
            assert j["locked_by"] is None
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    assert failed, "Single-attempt job did not transition to FAILED"
