import asyncio
import os
import sys
from uuid import UUID
import pytest
from httpx import AsyncClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.db import get_job
from api.redis_client import get_redis
from worker.worker import Worker


@pytest.mark.asyncio
async def test_criterion_1_submit_job_returns_job_id(client: AsyncClient):
    """
    Acceptance Criterion 1:
    Submitting a job via POST /jobs returns a job_id immediately with status PENDING.
    """
    response = await client.post(
        "/api/v1/jobs",
        json={
            "type": "math.add",
            "payload": {"a": 10, "b": 20},
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert "job_id" in data
    assert data["status"] == "PENDING"
    # Validate that job_id is a valid UUID
    assert UUID(data["job_id"])


@pytest.mark.asyncio
async def test_criterion_2_job_executes_to_success(client: AsyncClient):
    """
    Acceptance Criterion 2:
    Within a few seconds, GET /jobs/{id} shows status: SUCCESS with correct computation.
    """
    response = await client.post(
        "/api/v1/jobs",
        json={
            "type": "math.add",
            "payload": {"a": 15, "b": 27},
        },
    )
    assert response.status_code == 201
    job_id = UUID(response.json()["job_id"])

    # Execute via worker
    worker = Worker(worker_id="test-worker-c2")
    worker_task = asyncio.create_task(worker.start())

    # Poll for completion up to 5 seconds
    success = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        job = await get_job(job_id)
        if job and job["status"] == "SUCCESS":
            success = True
            assert job["result"] == {"sum": 42}
            assert job["attempts"] == 1
            assert job["locked_by"] is None
            break

    worker.running = False
    await worker_task
    assert success, "Job did not reach SUCCESS status within timeout"

    # Verify via API GET endpoint
    api_resp = await client.get(f"/api/v1/jobs/{job_id}")
    assert api_resp.status_code == 200
    data = api_resp.json()
    assert data["status"] == "SUCCESS"
    assert data["result"] == {"sum": 42}


@pytest.mark.asyncio
async def test_criterion_3_worker_restart_durability(client: AsyncClient):
    """
    Acceptance Criterion 3:
    Restarting the worker does not lose in-flight/pending jobs from before restart.
    They remain PENDING in Postgres and on Redis queue if not yet popped.
    """
    # 1. Submit job while NO worker is running
    response = await client.post(
        "/api/v1/jobs",
        json={
            "type": "math.multiply",
            "payload": {"a": 6, "b": 7},
        },
    )
    assert response.status_code == 201
    job_id = UUID(response.json()["job_id"])

    # 2. Verify job is PENDING in Postgres and still present on Redis queue
    job_before = await get_job(job_id)
    assert job_before["status"] == "PENDING"
    assert job_before["attempts"] == 0

    redis = get_redis()
    queue_len = await redis.llen("queue:default")
    assert queue_len >= 1

    # 3. Simulate "worker start / restart" after the submission
    worker = Worker(worker_id="test-worker-restart")
    worker_task = asyncio.create_task(worker.start())

    # Poll until worker picks up the pre-existing pending job and finishes it
    success = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        job = await get_job(job_id)
        if job and job["status"] == "SUCCESS":
            success = True
            assert job["result"] == {"product": 42}
            break

    worker.running = False
    await worker_task
    assert success, "Pre-existing pending job was not picked up after worker start"


@pytest.mark.asyncio
async def test_job_failure_handling(client: AsyncClient):
    """
    Verifies that a failing handler cleanly transitions job status to FAILED
    and records the error message without crashing the worker.
    """
    response = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Test fault condition"},
            "max_attempts": 1,
        },
    )
    assert response.status_code == 201
    job_id = UUID(response.json()["job_id"])


    worker = Worker(worker_id="test-worker-fail")
    worker_task = asyncio.create_task(worker.start())

    failed = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        job = await get_job(job_id)
        if job and job["status"] in ("FAILED", "DEAD_LETTER"):
            failed = True
            assert "Test fault condition" in job["error"]
            assert job["locked_by"] is None
            break

    worker.running = False
    await worker_task
    assert failed, "Failing job did not transition to FAILED"
