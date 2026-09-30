"""
Feature 4.2 - Recurring Jobs Tests

Verifies:
  - @every interval rule parsing (positive cases + edge cases)
  - cron rule parsing via croniter
  - is_valid_recurrence_rule helper
  - API accepts recurrence_rule when submitting a job
  - Worker: after success, next occurrence is created and registered in the scheduled set
"""

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --------------------------------------------------------------------------- #
# 1. Unit tests for worker/recurrence.py                                       #
# --------------------------------------------------------------------------- #

from worker.recurrence import (
    compute_next_run,
    parse_interval,
    is_valid_recurrence_rule,
    next_run_from_interval,
)


class TestParseInterval:
    def test_seconds(self):
        td = parse_interval("@every 30s")
        assert td == timedelta(seconds=30)

    def test_minutes(self):
        td = parse_interval("@every 5m")
        assert td == timedelta(minutes=5)

    def test_hours(self):
        td = parse_interval("@every 2h")
        assert td == timedelta(hours=2)

    def test_days(self):
        td = parse_interval("@every 1d")
        assert td == timedelta(days=1)

    def test_composite(self):
        td = parse_interval("@every 1h30m")
        assert td == timedelta(hours=1, minutes=30)

    def test_not_interval_returns_none(self):
        assert parse_interval("*/5 * * * *") is None

    def test_case_insensitive(self):
        td = parse_interval("@every 10S")
        assert td == timedelta(seconds=10)


class TestNextRunFromInterval:
    def test_adds_delta_to_base(self):
        base = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = next_run_from_interval("@every 30s", after=base)
        assert result == datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc)

    def test_uses_now_when_no_base(self):
        before = datetime.now(timezone.utc)
        result = next_run_from_interval("@every 10m")
        after = datetime.now(timezone.utc)
        assert before + timedelta(minutes=10) <= result <= after + timedelta(minutes=10)


class TestComputeNextRun:
    def test_interval_dispatch(self):
        base = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("@every 1h", after=base)
        assert result == datetime(2024, 6, 1, 1, 0, 0, tzinfo=timezone.utc)

    def test_cron_dispatch(self):
        # "every minute" - next run should be ~1 minute after base
        base = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run("* * * * *", after=base)
        assert result == datetime(2024, 6, 1, 0, 1, 0, tzinfo=timezone.utc)

    def test_invalid_raises(self):
        with pytest.raises(Exception):
            compute_next_run("not-a-valid-rule")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            compute_next_run("")


class TestIsValidRecurrenceRule:
    def test_interval_is_valid(self):
        assert is_valid_recurrence_rule("@every 5m") is True

    def test_cron_is_valid(self):
        assert is_valid_recurrence_rule("*/5 * * * *") is True

    def test_garbage_is_invalid(self):
        assert is_valid_recurrence_rule("blah blah") is False


# --------------------------------------------------------------------------- #
# 2. Integration smoke test - API accepts recurrence_rule field                #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_api_accepts_recurrence_rule():
    """POST /api/v1/jobs with a recurrence_rule should not raise a validation error."""
    import httpx
    from api.main import app

    # Fake all side effects so we can test routing + validation only
    fake_job_id = uuid4()
    fake_record = {
        "id": fake_job_id,
        "type": "test_echo",
        "status": "PENDING",
        "priority": 0,
        "attempts": 0,
        "max_attempts": 3,
        "payload": {},
        "scheduled_for": None,
        "recurrence_rule": "@every 10m",
        "result": None,
        "error": None,
        "locked_by": None,
        "locked_at": None,
        "next_retry_at": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

    with (
        patch("api.main.create_job", new=AsyncMock(return_value=fake_record)),
        patch("api.main.enqueue_job", new=AsyncMock(return_value=1)),
        patch("api.main.init_db_pool", new=AsyncMock()),
        patch("api.main.close_db_pool", new=AsyncMock()),
        patch("api.main.init_redis", new=AsyncMock()),
        patch("api.main.close_redis", new=AsyncMock()),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/jobs",
                json={
                    "type": "test_echo",
                    "payload": {"msg": "hello"},
                    "recurrence_rule": "@every 10m",
                },
            )
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "PENDING"


# --------------------------------------------------------------------------- #
# 3. Unit test - worker schedules next occurrence on success                   #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_worker_schedules_next_occurrence_on_success():
    """
    When a job with a recurrence_rule completes successfully, the worker must
    create a new job row (schedule_next_occurrence) and register it in Redis
    (schedule_job). This test mocks the DB and Redis calls.
    """
    from uuid import uuid4 as _uuid4

    job_id = _uuid4()
    next_job_id = _uuid4()

    completed_job = {
        "id": job_id,
        "type": "test_echo",
        "payload": {"msg": "recurring"},
        "recurrence_rule": "@every 30s",
        "attempts": 1,
        "max_attempts": 3,
        "priority": 0,
    }

    next_job_record = {
        "id": next_job_id,
        "type": "test_echo",
        "payload": {"msg": "recurring"},
        "recurrence_rule": "@every 30s",
        "status": "PENDING",
    }

    with (
        patch("worker.worker.fetch_and_lock_job", new=AsyncMock(return_value=completed_job)),
        patch("worker.worker.mark_job_success", new=AsyncMock()),
        patch("worker.worker.ack_job", new=AsyncMock()),
        patch("worker.worker.schedule_next_occurrence", new=AsyncMock(return_value=next_job_record)),
        patch("worker.worker.schedule_job", new=AsyncMock()),
        patch("worker.worker.execute_handler", new=AsyncMock(return_value={"ok": True})),
        patch("worker.worker.get_handler", return_value=AsyncMock()),
    ):
        from worker.worker import Worker

        w = Worker.__new__(Worker)
        w.worker_id = "test-worker"
        w.processing_queue = "queue:processing"
        w.jobs_processed = 0
        w.redis_client = AsyncMock()

        await w.process_job(job_id)

        # Verify next occurrence was created and registered
        import worker.worker as ww
        ww.schedule_next_occurrence.assert_called_once()
        call_kwargs = ww.schedule_next_occurrence.call_args
        assert call_kwargs.kwargs["job_type"] == "test_echo"
        assert call_kwargs.kwargs["recurrence_rule"] == "@every 30s"

        ww.schedule_job.assert_called_once()
        sj_kwargs = ww.schedule_job.call_args
        assert sj_kwargs.kwargs["job_id"] == next_job_id
