import asyncio
import json
from uuid import uuid4
import pytest
from httpx import AsyncClient
from fastapi.testclient import TestClient

from api.main import app
from api.db import create_job, get_job, get_db_pool
from api.redis_client import get_redis
from api.events import (
    make_event,
    publish_event,
    emit_job_submitted,
    emit_job_status_changed,
    emit_worker_heartbeat,
    emit_worker_dead,
    emit_stats_snapshot,
    EVENTS_CHANNEL,
)
from worker.db import upsert_worker_heartbeat
from worker.worker import Worker


@pytest.mark.asyncio
async def test_dashboard_static_assets(client: AsyncClient):
    """
    AC: Static files for dashboard are mounted and accessible.
    Root path / redirects to /dashboard/.
    """
    # 1. Root redirect
    root_resp = await client.get("/", follow_redirects=False)
    assert root_resp.status_code == 307
    assert root_resp.headers["location"] == "/dashboard/"

    # 2. index.html
    dash_resp = await client.get("/dashboard/")
    assert dash_resp.status_code == 200
    assert "DistribuQ — Live Dashboard" in dash_resp.text
    assert '<canvas id="throughput-chart"' in dash_resp.text
    assert 'id="event-feed"' in dash_resp.text
    assert 'id="btn-chaos"' in dash_resp.text

    # 3. style.css
    css_resp = await client.get("/dashboard/style.css")
    assert css_resp.status_code == 200
    assert "--bg-primary:" in css_resp.text

    # 4. app.js
    js_resp = await client.get("/dashboard/app.js")
    assert js_resp.status_code == 200
    assert "handleIncomingEvent" in js_resp.text


@pytest.mark.asyncio
async def test_stats_endpoint(client: AsyncClient):
    """
    AC: GET /api/v1/stats returns all job status counts and worker details.
    """
    resp = await client.get("/api/v1/stats")
    assert resp.status_code == 200
    data = resp.json()

    assert "job_counts" in data
    assert "active_workers" in data
    assert "workers" in data

    counts = data["job_counts"]
    for expected_key in ["PENDING", "RUNNING", "SUCCESS", "FAILED", "RETRYING", "DEAD_LETTER"]:
        assert expected_key in counts
        assert isinstance(counts[expected_key], int)


@pytest.mark.asyncio
async def test_chaos_kill_worker_endpoint(client: AsyncClient):
    """
    AC: POST /api/v1/dev/kill-worker publishes kill signal to an alive worker.
    If no alive workers, returns 404.
    """
    pool = get_db_pool()
    worker_id = f"test-chaos-{uuid4()}"

    # First mark all existing workers to DEAD to test 404
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE workers SET status = 'DEAD';")

    res_404 = await client.post("/api/v1/dev/kill-worker")
    assert res_404.status_code == 404
    assert "No alive workers" in res_404.json()["detail"]

    # Now register an alive worker
    await upsert_worker_heartbeat(worker_id, jobs_processed=5)

    # Test kill endpoint succeeds
    res_kill = await client.post("/api/v1/dev/kill-worker")
    assert res_kill.status_code == 200
    kill_data = res_kill.json()
    assert kill_data["killed_worker_id"] == worker_id
    assert kill_data["channel"] == f"distribuq:kill:{worker_id}"


@pytest.mark.asyncio
async def test_events_emission_helpers(client: AsyncClient):
    """
    AC: Event helpers correctly format and publish events to Redis pub/sub.
    """
    redis = get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)

    # Allow subscription to register
    await asyncio.sleep(0.05)

    # 1. Job submitted event
    await emit_job_submitted(redis, job_id="job-123", job_type="test_echo", priority=1)

    # 2. Job status changed
    await emit_job_status_changed(redis, job_id="job-123", job_type="test_echo", status="RUNNING", worker_id="w-1")

    # 3. Worker heartbeat
    await emit_worker_heartbeat(redis, worker_id="w-1", status="ALIVE", jobs_processed=10)

    # 4. Worker dead
    await emit_worker_dead(redis, worker_id="w-1")

    # 5. Stats snapshot
    await emit_stats_snapshot(redis, counts={"PENDING": 1, "RUNNING": 0}, active_workers=1)

    received_events = []
    # Collect messages from pubsub
    for _ in range(15):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg and msg["type"] == "message":
            received_events.append(json.loads(msg["data"]))
        if len(received_events) >= 5:
            break

    await pubsub.unsubscribe(EVENTS_CHANNEL)
    await pubsub.aclose()

    assert len(received_events) >= 5
    event_names = [e["event"] for e in received_events]
    assert "job.submitted" in event_names
    assert "job.status_changed" in event_names
    assert "worker.heartbeat" in event_names
    assert "worker.dead" in event_names
    assert "stats.snapshot" in event_names


@pytest.mark.asyncio
async def test_worker_kill_subscriber(client: AsyncClient):
    """
    AC: A running worker listening on its kill channel shuts down cleanly upon receiving 'kill'.
    """
    worker_id = f"test-worker-kill-{uuid4()}"
    worker = Worker(worker_id=worker_id, heartbeat_interval_sec=1)
    worker_task = asyncio.create_task(worker.start())

    # Wait for worker to start and subscribe
    for _ in range(25):
        await asyncio.sleep(0.1)
        if worker.running and worker.redis_client:
            break

    assert worker.running is True

    # Send kill command to worker's personal kill channel
    redis = get_redis()
    channel = f"distribuq:kill:{worker_id}"
    await redis.publish(channel, "kill")

    # Worker should terminate on its own within 2 seconds
    for _ in range(30):
        await asyncio.sleep(0.1)
        if not worker.running:
            break

    assert worker.running is False

    try:
        await asyncio.wait_for(worker_task, timeout=2.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass


def test_websocket_dashboard_broadcast():
    """
    AC: /ws/dashboard connects and receives real-time broadcast messages.
    """
    with TestClient(app) as test_client:
        with test_client.websocket_connect("/ws/dashboard") as ws:
            # First message sent automatically on connection is a stats.snapshot
            data = ws.receive_json()
            assert data["event"] == "stats.snapshot"
            assert "counts" in data["data"]
            assert "active_workers" in data["data"]
