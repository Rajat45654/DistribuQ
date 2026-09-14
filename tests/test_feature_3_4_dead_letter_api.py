import asyncio
from uuid import UUID, uuid4
import pytest
from httpx import AsyncClient

from api.db import get_job
from api.redis_client import get_redis
from worker.worker import Worker


@pytest.mark.asyncio
async def test_get_dead_letters_and_replay_endpoint(client: AsyncClient):
    """
    Feature 3.4 Acceptance:
    1. GET /api/v1/dead-letters lists jobs that landed in DLQ.
    2. POST /api/v1/dead-letters/{id}/replay resets job to PENDING and pushes to Redis queue.
    3. The replayed job is removed from the dead_letters table.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    # 1. Submit a job configured to immediately fail into DLQ (max_attempts=1)
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Test fatal crash for DLQ API"},
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # 2. Run worker to process and move to DLQ
    worker = Worker(worker_id="test-dlq-api-worker", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "DEAD_LETTER":
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    # 3. Query GET /api/v1/dead-letters
    dlq_resp = await client.get("/api/v1/dead-letters")
    assert dlq_resp.status_code == 200
    dlq_list = dlq_resp.json()
    assert len(dlq_list) >= 1

    matched = [d for d in dlq_list if d["job_id"] == str(job_id)]
    assert len(matched) == 1
    dl_record = matched[0]
    dl_id = UUID(dl_record["id"])
    assert dl_record["attempts_made"] == 1
    assert "Test fatal crash for DLQ API" in dl_record["final_error"]
    assert dl_record["job_type"] == "fail"

    # 4. Replay the dead letter using its dead letter ID
    replay_resp = await client.post(f"/api/v1/dead-letters/{dl_id}/replay")
    assert replay_resp.status_code == 200
    replay_data = replay_resp.json()
    assert replay_data["job_id"] == str(job_id)
    assert replay_data["status"] == "PENDING"
    assert replay_data["dead_letter_id"] == str(dl_id)

    # 5. Verify job is now PENDING in DB with 0 attempts and cleared errors
    job_after = await get_job(job_id)
    assert job_after["status"] == "PENDING"
    assert job_after["attempts"] == 0
    assert job_after["error"] is None
    assert job_after["locked_by"] is None
    assert job_after["locked_at"] is None

    # 6. Verify job is present in Redis queue
    queue_len = await redis.llen("queue:default")
    assert queue_len >= 1

    # 7. Verify the dead letter is removed from dead_letters table
    dlq_resp_after = await client.get("/api/v1/dead-letters")
    assert dlq_resp_after.status_code == 200
    matched_after = [d for d in dlq_resp_after.json() if d["id"] == str(dl_id)]
    assert len(matched_after) == 0


@pytest.mark.asyncio
async def test_replay_dead_letter_not_found(client: AsyncClient):
    """
    Replaying a non-existent dead letter ID returns 404 Not Found.
    """
    non_existent = uuid4()
    resp = await client.post(f"/api/v1/dead-letters/{non_existent}/replay")
    assert resp.status_code == 404
    assert f"Dead letter record '{non_existent}' not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_replay_by_job_id(client: AsyncClient):
    """
    Verifies that POST /api/v1/dead-letters/{id}/replay also accepts the job_id
    for maximum operator convenience.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "fail",
            "payload": {"reason": "Crash to test job_id lookup replay"},
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    worker = Worker(worker_id="test-dlq-api-worker-2", heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "DEAD_LETTER":
            break

    worker.running = False
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    # Replay directly by job_id
    replay_resp = await client.post(f"/api/v1/dead-letters/{job_id}/replay")
    assert replay_resp.status_code == 200
    assert replay_resp.json()["job_id"] == str(job_id)
    assert replay_resp.json()["status"] == "PENDING"


@pytest.mark.asyncio
async def test_replayed_job_executes_successfully_by_worker(client: AsyncClient):
    """
    Verifies the entire replay lifecycle end-to-end:
    1. A flaky job fails on attempt 1 with max_attempts=1 and enters DLQ.
    2. The job is replayed via API back into PENDING state.
    3. An active worker picks up the replayed job and processes it to SUCCESS.
    """
    redis = get_redis()
    await redis.delete("queue:default", "queue:processing")

    task_key = f"replay-test-{uuid4().hex[:6]}"
    resp = await client.post(
        "/api/v1/jobs",
        json={
            "type": "flaky",
            "payload": {"task_key": task_key, "fail_until_attempt": 2},
            "max_attempts": 1,
        },
    )
    assert resp.status_code == 201
    job_id = UUID(resp.json()["job_id"])

    # Worker 1 runs and fails job into DLQ
    worker1 = Worker(worker_id="test-dlq-exec-w1", heartbeat_interval_sec=1)
    w1_task = asyncio.create_task(worker1.start())

    for _ in range(25):
        await asyncio.sleep(0.2)
        j = await get_job(job_id)
        if j and j["status"] == "DEAD_LETTER":
            break

    worker1.running = False
    w1_task.cancel()
    try:
        await w1_task
    except asyncio.CancelledError:
        pass

    assert j["status"] == "DEAD_LETTER"

    # Replay the job
    replay_resp = await client.post(f"/api/v1/dead-letters/{job_id}/replay")
    assert replay_resp.status_code == 200

    # Worker 2 starts and should process the replayed job to SUCCESS
    worker2 = Worker(worker_id="test-dlq-exec-w2", heartbeat_interval_sec=1)
    w2_task = asyncio.create_task(worker2.start())

    succeeded = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        j_replayed = await get_job(job_id)
        if j_replayed and j_replayed["status"] == "SUCCESS":
            succeeded = True
            assert j_replayed["result"] == {"recovered_at_attempt": 2, "task_key": task_key}
            assert j_replayed["attempts"] == 1
            assert j_replayed["error"] is None
            break

    worker2.running = False
    w2_task.cancel()
    try:
        await w2_task
    except asyncio.CancelledError:
        pass

    assert succeeded, "Replayed job was not executed to SUCCESS by worker"

