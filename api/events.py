"""
Event bus for DistribuQ - Redis pub/sub based.

Publishers (workers, reaper, API) call publish_event() to broadcast a JSON
event on the `distribuq:events` channel.

The API's WebSocket manager subscribes to this channel and fans every event
out to all connected browser clients.

Event envelope:
    {
        "event":     str,          # e.g. "job.status_changed", "worker.heartbeat"
        "timestamp": ISO-8601 UTC,
        "data":      dict          # event-specific payload
    }

Defined event types:
    job.status_changed   - { job_id, type, status, worker_id? }
    job.submitted        - { job_id, type, priority, scheduled_for? }
    worker.heartbeat     - { worker_id, status, jobs_processed, last_heartbeat }
    worker.dead          - { worker_id }
    stats.snapshot       - { counts: {PENDING, RUNNING, SUCCESS, FAILED, RETRYING, DEAD_LETTER}, active_workers }
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

EVENTS_CHANNEL = "distribuq:events"

logger = logging.getLogger("distribuq.events")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(event_type: str, data: Dict[str, Any]) -> str:
    """Serialises an event envelope to a JSON string."""
    return json.dumps({
        "event": event_type,
        "timestamp": _now_iso(),
        "data": data,
    })


async def publish_event(
    event_type: str,
    data: Dict[str, Any],
    client: aioredis.Redis,
) -> None:
    """Publishes a single event to the Redis pub/sub channel."""
    try:
        payload = make_event(event_type, data)
        await client.publish(EVENTS_CHANNEL, payload)
        logger.debug("Published event %s: %s", event_type, payload[:120])
    except Exception as exc:
        # Publishing is best-effort - never let it break the caller
        logger.warning("Failed to publish event %s: %s", event_type, exc)


# ---------------------------------------------------------------------------
# Convenience helpers - one function per event type so callers stay clean
# ---------------------------------------------------------------------------

async def emit_job_status_changed(
    client: aioredis.Redis,
    job_id: str,
    job_type: str,
    status: str,
    worker_id: Optional[str] = None,
) -> None:
    data: Dict[str, Any] = {"job_id": job_id, "type": job_type, "status": status}
    if worker_id:
        data["worker_id"] = worker_id
    await publish_event("job.status_changed", data, client)


async def emit_job_submitted(
    client: aioredis.Redis,
    job_id: str,
    job_type: str,
    priority: int,
    scheduled_for: Optional[str] = None,
) -> None:
    data: Dict[str, Any] = {"job_id": job_id, "type": job_type, "priority": priority}
    if scheduled_for:
        data["scheduled_for"] = scheduled_for
    await publish_event("job.submitted", data, client)


async def emit_worker_heartbeat(
    client: aioredis.Redis,
    worker_id: str,
    status: str,
    jobs_processed: int,
) -> None:
    await publish_event("worker.heartbeat", {
        "worker_id": worker_id,
        "status": status,
        "jobs_processed": jobs_processed,
        "last_heartbeat": _now_iso(),
    }, client)


async def emit_worker_dead(client: aioredis.Redis, worker_id: str) -> None:
    await publish_event("worker.dead", {"worker_id": worker_id}, client)


async def emit_stats_snapshot(
    client: aioredis.Redis,
    counts: Dict[str, int],
    active_workers: int,
) -> None:
    await publish_event("stats.snapshot", {
        "counts": counts,
        "active_workers": active_workers,
    }, client)
