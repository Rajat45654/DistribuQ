#!/usr/bin/env python
"""
Phase 4 Demo Script - Scheduling, Recurring Jobs, and Priority Queues

Demonstrates the three Phase 4 capabilities against a live running DistribuQ stack.

Usage:
    # Start the stack first:
    #   docker-compose up -d
    #   python -m scheduler.scheduler   (in a separate terminal)
    #
    # Then run:
    python scripts/demo_phase4.py

What it shows:
  1. Delayed job - submitted now, should only run in ~10 seconds
  2. Recurring job - fires every 15 seconds automatically, at least 3 times
  3. Priority queues - high priority job beats a batch of low-priority ones
"""

import asyncio
import sys
import time
from datetime import datetime, timezone, timedelta

import httpx

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BASE_URL = "http://localhost:8000/api/v1"
POLL_INTERVAL = 1.0       # seconds between status checks
DEMO_TIMEOUT  = 120       # max seconds to wait per demonstration


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def separator(title: str):
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}\n")


async def poll_job_status(client: httpx.AsyncClient, job_id: str, timeout: int = DEMO_TIMEOUT) -> dict:
    """Poll until job reaches a terminal state (SUCCESS/FAILED/DEAD_LETTER) or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = await client.get(f"{BASE_URL}/jobs/{job_id}")
        r.raise_for_status()
        job = r.json()
        status = job.get("status", "?")
        print(f"  [{ts()}] Job {job_id[:8]}... status: {status}")
        if status in ("SUCCESS", "FAILED", "DEAD_LETTER"):
            return job
        await asyncio.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Job {job_id} did not reach terminal state within {timeout}s")


# --------------------------------------------------------------------------- #
# Demo 1 - Delayed Job                                                         #
# --------------------------------------------------------------------------- #

async def demo_delayed_job(client: httpx.AsyncClient):
    separator("DEMO 1 - Delayed Job (runs in 10 seconds)")

    delay_secs = 10
    run_at = datetime.now(timezone.utc) + timedelta(seconds=delay_secs)
    submitted_at = time.monotonic()

    print(f"  Submitting job scheduled for: {run_at.isoformat()}")
    r = await client.post(f"{BASE_URL}/jobs", json={
        "type": "test_echo",
        "payload": {"msg": "delayed demo"},
        "scheduled_for": run_at.isoformat(),
    })
    r.raise_for_status()
    job_id = r.json()["job_id"]
    print(f"  Job submitted: {job_id}")
    print(f"  Waiting for it to run (should NOT start for ~{delay_secs}s)...\n")

    job = await poll_job_status(client, job_id, timeout=delay_secs + 30)
    elapsed = time.monotonic() - submitted_at

    print(f"\n  Job completed in {elapsed:.1f}s after submission.")
    if elapsed >= delay_secs:
        print("  [PASS] Job respected the delay - did not run early.")
    else:
        print("  [FAIL] Job ran before its scheduled_for time!")

    return job


# --------------------------------------------------------------------------- #
# Demo 2 - Recurring Job                                                       #
# --------------------------------------------------------------------------- #

async def demo_recurring_job(client: httpx.AsyncClient):
    separator("DEMO 2 - Recurring Job (@every 15s - fires 3 times automatically)")

    RECURRENCE = "@every 15s"
    TARGET_RUNS = 3
    completed_ids = []

    print(f"  Submitting first occurrence with recurrence_rule='{RECURRENCE}'")
    r = await client.post(f"{BASE_URL}/jobs", json={
        "type": "test_echo",
        "payload": {"msg": "recurring demo"},
        "recurrence_rule": RECURRENCE,
    })
    r.raise_for_status()
    first_id = r.json()["job_id"]
    print(f"  First job: {first_id}\n")

    # Poll the first job, then look for subsequent recurring jobs
    current_id = first_id
    run_number = 0

    deadline = time.monotonic() + (TARGET_RUNS * 20) + 30  # generous deadline

    while run_number < TARGET_RUNS and time.monotonic() < deadline:
        run_number += 1
        print(f"  --- Run #{run_number} (job {current_id[:8]}...) ---")
        job = await poll_job_status(client, current_id, timeout=30)
        completed_ids.append(current_id)
        print(f"  Run #{run_number} DONE.\n")

        if run_number < TARGET_RUNS:
            # Find the next recurring job - list recent PENDING jobs
            print(f"  Waiting for next occurrence to appear (~15s)...")
            found_next = False
            wait_deadline = time.monotonic() + 25
            while time.monotonic() < wait_deadline and not found_next:
                await asyncio.sleep(2)
                r = await client.get(f"{BASE_URL}/jobs?status=PENDING&limit=20")
                pending = r.json()
                for pj in pending:
                    if pj["id"] not in completed_ids and pj["recurrence_rule"] == RECURRENCE:
                        current_id = pj["id"]
                        found_next = True
                        print(f"  Next occurrence found: {current_id}\n")
                        break

            if not found_next:
                print("  [FAIL] Next recurring occurrence not found within expected window.")
                return

    if run_number >= TARGET_RUNS:
        print(f"  [PASS] Recurring job fired {run_number} times automatically with no manual re-submission.")
    else:
        print(f"  [FAIL] Only {run_number}/{TARGET_RUNS} runs completed.")


# --------------------------------------------------------------------------- #
# Demo 3 - Priority Queues                                                     #
# --------------------------------------------------------------------------- #

async def demo_priority_queues(client: httpx.AsyncClient):
    separator("DEMO 3 - Priority Queues (high beats low)")

    print("  Submitting 5 LOW-priority jobs first...")
    low_ids = []
    for i in range(5):
        r = await client.post(f"{BASE_URL}/jobs", json={
            "type": "test_echo",
            "payload": {"msg": f"low priority job {i}", "_base_delay": 0},
            "priority": -1,
        })
        r.raise_for_status()
        low_ids.append(r.json()["job_id"])

    print(f"  Submitted {len(low_ids)} low-priority jobs: {[j[:8] for j in low_ids]}\n")
    await asyncio.sleep(0.5)

    print("  Now submitting 1 HIGH-priority job...")
    r = await client.post(f"{BASE_URL}/jobs", json={
        "type": "test_echo",
        "payload": {"msg": "high priority - should run first"},
        "priority": 10,
    })
    r.raise_for_status()
    high_id = r.json()["job_id"]
    print(f"  High-priority job: {high_id}\n")

    print("  Polling until high-priority job completes...\n")
    high_job = await poll_job_status(client, high_id, timeout=30)

    # Check: how many low-priority jobs are still PENDING?
    r = await client.get(f"{BASE_URL}/jobs?status=PENDING&limit=20")
    still_pending = [j for j in r.json() if j["id"] in low_ids]

    print(f"\n  High-priority job status: {high_job['status']}")
    print(f"  Low-priority jobs still pending: {len(still_pending)}/{len(low_ids)}")

    if high_job["status"] == "SUCCESS" and len(still_pending) > 0:
        print("  [PASS] High-priority job completed while low-priority jobs were still queued.")
    elif high_job["status"] == "SUCCESS":
        print("  [INFO] High-priority job completed - all low-priority also done (workers were fast).")
    else:
        print("  [FAIL] High-priority job did not succeed.")


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

async def main():
    print("\n" + "=" * 60)
    print("  DistribuQ - Phase 4 Demo")
    print("  Scheduling | Recurring Jobs | Priority Queues")
    print("=" * 60)
    print(f"\n  Target API: {BASE_URL}")
    print("  Make sure docker-compose and scheduler are running.\n")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Health check
        try:
            r = await client.get("http://localhost:8000/healthz")
            r.raise_for_status()
            print(f"  API health check: OK\n")
        except Exception as e:
            print(f"  [ERROR] Cannot reach API: {e}")
            print("  Start the stack with: docker-compose up -d")
            return

        try:
            await demo_delayed_job(client)
        except Exception as e:
            print(f"  Demo 1 error: {e}")

        try:
            await demo_recurring_job(client)
        except Exception as e:
            print(f"  Demo 2 error: {e}")

        try:
            await demo_priority_queues(client)
        except Exception as e:
            print(f"  Demo 3 error: {e}")

    separator("PHASE 4 DEMO COMPLETE")


if __name__ == "__main__":
    asyncio.run(main())
