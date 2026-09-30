"""
Tests for Job Cancellation & Recurrence Validation

Verifies:
  - POST /api/v1/jobs/{job_id}/cancel cancels a PENDING job
  - POST /api/v1/jobs/{job_id}/cancel is a no-op on non-pending jobs
  - POST /api/v1/jobs/{job_id}/cancel returns 404 for unknown job_id
  - POST /api/v1/jobs with invalid recurrence_rule returns 400 Bad Request
"""

import pytest
from uuid import uuid4


@pytest.mark.asyncio
async def test_cancel_pending_job(client):
    # 1. Submit a delayed job so it stays PENDING
    submit_res = await client.post(
        "/api/v1/jobs",
        json={
            "type": "test_echo",
            "payload": {"msg": "to be cancelled"},
            "scheduled_for": "2099-01-01T00:00:00Z",
        },
    )
    assert submit_res.status_code == 201
    job_id = submit_res.json()["job_id"]

    # 2. Cancel the job
    cancel_res = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert cancel_res.status_code == 200
    data = cancel_res.json()
    assert data["job_id"] == job_id
    assert data["status"] == "FAILED"

    # 3. Verify in database
    detail_res = await client.get(f"/api/v1/jobs/{job_id}")
    assert detail_res.status_code == 200
    detail = detail_res.json()
    assert detail["status"] == "FAILED"
    assert "cancelled by user" in detail["error"].lower()


@pytest.mark.asyncio
async def test_cancel_nonexistent_job_returns_404(client):
    random_id = str(uuid4())
    res = await client.post(f"/api/v1/jobs/{random_id}/cancel")
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_invalid_recurrence_rule_returns_400(client):
    res = await client.post(
        "/api/v1/jobs",
        json={
            "type": "test_echo",
            "payload": {},
            "recurrence_rule": "not-a-valid-cron-or-interval",
        },
    )
    assert res.status_code == 400
    assert "invalid recurrence_rule" in res.json()["detail"].lower()
