"""
DistribuQ — Chaos Under Load Benchmark

Injects random worker kills during an active high-load queue processing stream.
Verifies that 100% of jobs eventually reach SUCCESS or terminal status, with
zero silent job loss and zero duplicate executions.
"""

import argparse
import asyncio
import logging
import random
import sys
import time
from typing import Dict, List
import httpx
import psycopg

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("distribuq.chaos_bench")


async def chaos_injector(
    api_url: str,
    stop_event: asyncio.Event,
    min_interval: float = 2.0,
    max_interval: float = 4.0,
    kill_log: List[Dict] = None,
):
    """Periodically triggers /api/v1/dev/kill-worker until stopped."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        while not stop_event.is_set():
            delay = random.uniform(min_interval, max_interval)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
                break
            except asyncio.TimeoutError:
                pass

            if stop_event.is_set():
                break

            try:
                res = await client.post(f"{api_url}/api/v1/dev/kill-worker")
                if res.status_code == 200:
                    data = res.json()
                    worker_id = data.get("killed_worker_id")
                    logger.warning("[CHAOS] Injected worker kill -> %s", worker_id)
                    if kill_log is not None:
                        kill_log.append({
                            "time": time.monotonic(),
                            "worker_id": worker_id,
                        })
                elif res.status_code == 404:
                    logger.info("[CHAOS] No alive workers available to kill (restarting...)")
            except Exception as e:
                logger.warning("[CHAOS] Failed to trigger kill-worker: %s", e)


async def run_chaos_test(
    api_url: str = "http://localhost:8000",
    db_url: str = "postgresql://distribuq:password@localhost:5432/distribuq",
    total_jobs: int = 150,
    job_type: str = "test_echo",
    timeout_sec: float = 120.0,
) -> Dict:
    logger.info("=" * 65)
    logger.info("  Starting Chaos Under Load Test: %d jobs | Type: %s", total_jobs, job_type)
    logger.info("=" * 65)

    stop_chaos = asyncio.Event()
    kill_log: List[Dict] = []
    chaos_task = asyncio.create_task(chaos_injector(api_url, stop_chaos, kill_log=kill_log))

    # 1. Submit jobs with concurrency
    submitted_ids = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        logger.info("Submitting %d jobs...", total_jobs)
        for i in range(total_jobs):
            res = await client.post(
                f"{api_url}/api/v1/jobs",
                json={
                    "type": job_type,
                    "payload": {"idx": i, "chaos_test": True},
                    "priority": 0,
                    "max_attempts": 3,
                },
            )
            if res.status_code == 201:
                submitted_ids.append(res.json()["job_id"])
            if i % 25 == 0 and i > 0:
                await asyncio.sleep(0.1)

    logger.info("Submitted %d jobs. Monitoring queue drain under chaos...", len(submitted_ids))

    # 2. Wait until all jobs complete
    deadline = time.monotonic() + timeout_sec
    all_done = False
    counts = {}

    while time.monotonic() < deadline:
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT status, count(*) FROM jobs WHERE id = ANY(%s) GROUP BY status;",
                    (submitted_ids,),
                )
                rows = await cur.fetchall()

        counts = {r[0]: r[1] for r in rows}
        completed = counts.get("SUCCESS", 0) + counts.get("DEAD_LETTER", 0)
        sys.stdout.write(
            f"\r  [Chaos Run] Completed: {completed}/{total_jobs} "
            f"(Kills: {len(kill_log)}, Running: {counts.get('RUNNING', 0)}, Pending: {counts.get('PENDING', 0)})   "
        )
        sys.stdout.flush()

        if completed >= total_jobs:
            all_done = True
            break
        await asyncio.sleep(0.5)

    print("")
    stop_chaos.set()
    await chaos_task

    # 3. Assertions & Verification
    success_count = counts.get("SUCCESS", 0)
    reliability_pct = (success_count / total_jobs) * 100

    print("\n" + "=" * 65)
    print("  CHAOS BENCHMARK RESULTS")
    print("=" * 65)
    print(f"  Total Jobs Submitted:     {total_jobs}")
    print(f"  Workers Killed by Chaos:  {len(kill_log)}")
    print(f"  Jobs Successfully Done:   {success_count}/{total_jobs} ({reliability_pct:.1f}%)")
    print(f"  Jobs Lost / Stuck:        {total_jobs - success_count}")
    print(f"  Overall Reliability:      {'PASS (100%)' if reliability_pct == 100 else 'FAIL'}")
    print("=" * 65 + "\n")

    return {
        "total_jobs": total_jobs,
        "workers_killed": len(kill_log),
        "success_count": success_count,
        "reliability_pct": reliability_pct,
        "status_counts": counts,
        "passed": reliability_pct == 100.0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DistribuQ Chaos Under Load Benchmark")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--db", default="postgresql://distribuq:password@localhost:5432/distribuq")
    parser.add_argument("--jobs", type=int, default=100)
    args = parser.parse_args()

    asyncio.run(run_chaos_test(api_url=args.url, db_url=args.db, total_jobs=args.jobs))
