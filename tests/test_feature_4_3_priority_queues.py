"""
Feature 4.3 - Priority Queues Tests

Verifies:
  - enqueue_job routes to queue:high / queue:default / queue:low by priority
  - priority_reserve_job picks jobs from high before default before low
  - API passes priority through when submitting a job
"""

import asyncio
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call
from uuid import uuid4

import pytest

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from api.config import settings


# --------------------------------------------------------------------------- #
# 1. enqueue_job routes to the correct queue                                   #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_enqueue_high_priority():
    from api.redis_client import enqueue_job

    mock_redis = AsyncMock()
    mock_redis.lpush = AsyncMock(return_value=1)

    job_id = uuid4()
    await enqueue_job(job_id=job_id, priority=5, client=mock_redis)

    mock_redis.lpush.assert_called_once_with(settings.HIGH_QUEUE, str(job_id))


@pytest.mark.asyncio
async def test_enqueue_default_priority():
    from api.redis_client import enqueue_job

    mock_redis = AsyncMock()
    mock_redis.lpush = AsyncMock(return_value=1)

    job_id = uuid4()
    await enqueue_job(job_id=job_id, priority=0, client=mock_redis)

    mock_redis.lpush.assert_called_once_with(settings.DEFAULT_QUEUE, str(job_id))


@pytest.mark.asyncio
async def test_enqueue_low_priority():
    from api.redis_client import enqueue_job

    mock_redis = AsyncMock()
    mock_redis.lpush = AsyncMock(return_value=1)

    job_id = uuid4()
    await enqueue_job(job_id=job_id, priority=-1, client=mock_redis)

    mock_redis.lpush.assert_called_once_with(settings.LOW_QUEUE, str(job_id))


@pytest.mark.asyncio
async def test_enqueue_explicit_queue_overrides_priority():
    """An explicit queue= kwarg should bypass priority-based routing."""
    from api.redis_client import enqueue_job

    mock_redis = AsyncMock()
    mock_redis.lpush = AsyncMock(return_value=1)

    job_id = uuid4()
    await enqueue_job(job_id=job_id, priority=99, queue="queue:custom", client=mock_redis)

    mock_redis.lpush.assert_called_once_with("queue:custom", str(job_id))


# --------------------------------------------------------------------------- #
# 2. priority_reserve_job picks high before default before low                 #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_priority_reserve_picks_high_first():
    """When queue:high has a job, it should be picked even if default also has one."""
    from api.redis_client import priority_reserve_job

    high_job_id = uuid4()
    mock_redis = AsyncMock()

    # rpoplpush returns the high-queue job immediately
    mock_redis.rpoplpush = AsyncMock(return_value=str(high_job_id))

    result = await priority_reserve_job(client=mock_redis)

    assert result == high_job_id
    # Only the high queue call should have happened
    mock_redis.rpoplpush.assert_called_once_with(settings.HIGH_QUEUE, settings.DEFAULT_PROCESSING_QUEUE)


@pytest.mark.asyncio
async def test_priority_reserve_falls_through_to_default():
    """When queue:high is empty, default queue job should be picked."""
    from api.redis_client import priority_reserve_job

    default_job_id = uuid4()
    mock_redis = AsyncMock()

    # High queue is empty, default has a job
    def rpoplpush_side(src, dst):
        if src == settings.HIGH_QUEUE:
            return None
        return str(default_job_id)

    mock_redis.rpoplpush = AsyncMock(side_effect=rpoplpush_side)

    result = await priority_reserve_job(client=mock_redis)

    assert result == default_job_id


@pytest.mark.asyncio
async def test_priority_reserve_falls_through_to_low():
    """When high and default are empty, low queue job should be picked."""
    from api.redis_client import priority_reserve_job

    low_job_id = uuid4()
    mock_redis = AsyncMock()

    def rpoplpush_side(src, dst):
        if src == settings.LOW_QUEUE:
            return str(low_job_id)
        return None

    mock_redis.rpoplpush = AsyncMock(side_effect=rpoplpush_side)

    result = await priority_reserve_job(client=mock_redis)

    assert result == low_job_id


@pytest.mark.asyncio
async def test_priority_reserve_blocks_when_all_empty():
    """When all queues are empty, falls back to brpop and returns None on timeout."""
    from api.redis_client import priority_reserve_job

    mock_redis = AsyncMock()
    mock_redis.rpoplpush = AsyncMock(return_value=None)
    mock_redis.brpop = AsyncMock(return_value=None)  # timeout expired

    result = await priority_reserve_job(client=mock_redis)

    assert result is None
    mock_redis.brpop.assert_called_once()


@pytest.mark.asyncio
async def test_priority_reserve_brpop_fallback_enqueues_to_processing():
    """When brpop fires, the job is moved into the processing queue."""
    from api.redis_client import priority_reserve_job

    low_job_id = uuid4()
    mock_redis = AsyncMock()
    mock_redis.rpoplpush = AsyncMock(return_value=None)
    mock_redis.brpop = AsyncMock(return_value=(settings.LOW_QUEUE, str(low_job_id)))
    mock_redis.lpush = AsyncMock(return_value=1)

    result = await priority_reserve_job(client=mock_redis)

    assert result == low_job_id
    mock_redis.lpush.assert_called_once_with(settings.DEFAULT_PROCESSING_QUEUE, str(low_job_id))


# --------------------------------------------------------------------------- #
# 3. Config sanity                                                              #
# --------------------------------------------------------------------------- #

def test_queue_names_are_distinct():
    assert settings.HIGH_QUEUE != settings.DEFAULT_QUEUE
    assert settings.DEFAULT_QUEUE != settings.LOW_QUEUE
    assert settings.HIGH_QUEUE != settings.LOW_QUEUE


def test_queue_names_correct():
    assert settings.HIGH_QUEUE == "queue:high"
    assert settings.DEFAULT_QUEUE == "queue:default"
    assert settings.LOW_QUEUE == "queue:low"
