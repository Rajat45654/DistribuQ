"""
DistribuQ — Load Generation & Benchmarking Tool

Submits a batch of jobs asynchronously at high throughput, tracks
submission speed, monitors processing until the queue is drained, and calculates
exact end-to-end latency percentiles (p50, p90, p95, p99) by querying Postgres.
"""

import argparse
import asyncio
import json
import logging
import math
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx
import psycopg

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("distribuq.load_test")


def calculate_percentiles(values: List[float]) -> Dict[str, float]:
    """Calculates min, p50, p90, p95, p99, max, mean, and stddev."""
    if not values:
        return {"min": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0, "stddev": 0}
    
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def percentile(p: float) -> float:
        idx = int(math.ceil((p / 100.0) * n)) - 1
        idx = max(0, min(n - 1, idx))
        return sorted_vals[idx]

    mean = sum(sorted_vals) / n
    variance = sum((x - mean) ** 2 for x in sorted_vals) / n if n > 1 else 0
    stddev = math.sqrt(variance)

    return {
        "min": round(sorted_vals[0], 2),
        "p50": round(percentile(50), 2),
        "p90": round(percentile(90), 2),
        "p95": round(percentile(95), 2),
        "p99": round(percentile(99), 2),
        "max": round(sorted_vals[-1], 2),
        "mean": round(mean, 2),
        "stddev": round(stddev, 2),
    }


async def submit_worker(
    client: httpx.AsyncClient,
    api_url: str,
    job_type: str,
    payload: Dict[str, Any],
    priority: int,
    queue: asyncio.Queue,
    results: List[Dict[str, Any]],
):
    """Worker task that consumes from input queue and posts jobs to the API."""
    while not queue.empty():
        try:
            job_idx = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        req_payload = {
            "type": job_type,
            "payload": {**payload, "bench_idx": job_idx},
            "priority": priority,
        }

        t0 = time.monotonic()
        try:
            res = await client.post(f"{api_url}/api/v1/jobs", json=req_payload)
            t1 = time.monotonic()
            if res.status_code == 201:
                data = res.json()
                results.append({
                    "job_id": data["job_id"],
                    "submit_latency_ms": (t1 - t0) * 1000,
                    "success": True,
                })
            else:
                results.append({"success": False, "error": f"HTTP {res.status_code}"})
        except Exception as e:
            results.append({"success": False, "error": str(e)})
        finally:
            queue.task_done()


async def wait_for_queue_drain(
    api_url: str,
    db_url: str,
    job_ids: List[str],
    timeout_sec: float = 120.0,
    poll_interval_sec: float = 0.5,
) -> Dict[str, Any]:
    """Polls until all submitted jobs reach a terminal status (SUCCESS or DEAD_LETTER)."""
    deadline = time.monotonic() + timeout_sec
    target_count = len(job_ids)

    logger.info("Waiting for %d jobs to finish processing (timeout: %.0fs)...", target_count, timeout_sec)

    while time.monotonic() < deadline:
        # Query status breakdown for our specific submitted batch
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT status, count(*) 
                    FROM jobs 
                    WHERE id = ANY(%s) 
                    GROUP BY status;
                    """,
                    (job_ids,),
                )
                rows = await cur.fetchall()

        counts = {r[0]: r[1] for r in rows}
        completed = counts.get("SUCCESS", 0) + counts.get("DEAD_LETTER", 0) + counts.get("FAILED", 0)
        pending = counts.get("PENDING", 0)
        running = counts.get("RUNNING", 0)
        retrying = counts.get("RETRYING", 0)

        sys.stdout.write(
            f"\r  [Progress] Completed: {completed}/{target_count} "
            f"(Success: {counts.get('SUCCESS', 0)}, Running: {running}, Pending: {pending}, Retrying: {retrying})   "
        )
        sys.stdout.flush()

        if completed >= target_count:
            print("")
            return {"counts": counts, "completed_all": True}

        await asyncio.sleep(poll_interval_sec)

    print("")
    logger.warning("Queue drain timed out after %.1f seconds.", timeout_sec)
    return {"counts": counts, "completed_all": False}


async def fetch_latencies_from_db(db_url: str, job_ids: List[str]) -> List[float]:
    """Queries Postgres for end-to-end turnaround latency (updated_at - created_at) in milliseconds."""
    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000 AS duration_ms
                FROM jobs
                WHERE id = ANY(%s) AND status = 'SUCCESS';
                """,
                (job_ids,),
            )
            rows = await cur.fetchall()
            return [float(r[0]) for r in rows if r[0] is not None]


async def run_load_test(
    api_url: str = "http://localhost:8000",
    db_url: str = "postgresql://distribuq:password@localhost:5432/distribuq",
    total_jobs: int = 500,
    concurrency: int = 20,
    job_type: str = "test_echo",
    payload: Optional[Dict[str, Any]] = None,
    priority: int = 0,
    timeout_sec: float = 120.0,
) -> Dict[str, Any]:
    """
    Executes a complete load test benchmark:
    1. Submits total_jobs using concurrency concurrent workers.
    2. Waits until all jobs reach terminal state.
    3. Fetches execution timestamps and calculates throughput + latency percentiles.
    """
    if payload is None:
        payload = {"data": "bench_load"}

    logger.info("=" * 65)
    logger.info("  Starting Load Test: %d jobs | Concurrency: %d | Type: %s", total_jobs, concurrency, job_type)
    logger.info("=" * 65)

    q = asyncio.Queue()
    for i in range(total_jobs):
        q.put_nowait(i)

    results: List[Dict[str, Any]] = []
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)

    # 1. Submission phase
    sub_start = time.monotonic()
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        workers = [
            asyncio.create_task(
                submit_worker(client, api_url, job_type, payload, priority, q, results)
            )
            for _ in range(concurrency)
        ]
        await asyncio.gather(*workers)
    sub_end = time.monotonic()

    sub_duration = max(0.001, sub_end - sub_start)
    successful_submits = [r for r in results if r.get("success")]
    job_ids = [r["job_id"] for r in successful_submits]
    submit_rate = len(successful_submits) / sub_duration

    logger.info(
        "Submitted %d/%d jobs in %.2fs (Submit Throughput: %.1f jobs/sec)",
        len(successful_submits), total_jobs, sub_duration, submit_rate
    )

    if not job_ids:
        logger.error("No jobs were successfully submitted.")
        return {"error": "Submission failed"}

    # 2. Processing / Drain phase
    drain_start = time.monotonic()
    drain_status = await wait_for_queue_drain(api_url, db_url, job_ids, timeout_sec=timeout_sec)
    drain_end = time.monotonic()
    drain_duration = max(0.001, drain_end - drain_start)

    # Total turnaround = from first job submitted to last job completed
    total_elapsed = max(0.001, drain_end - sub_start)
    completed_count = drain_status["counts"].get("SUCCESS", 0) + drain_status["counts"].get("DEAD_LETTER", 0)
    effective_throughput = completed_count / total_elapsed

    # 3. Latency extraction
    turnaround_latencies = await fetch_latencies_from_db(db_url, job_ids)
    turnaround_percentiles = calculate_percentiles(turnaround_latencies)

    submit_latencies = [r["submit_latency_ms"] for r in successful_submits]
    submit_percentiles = calculate_percentiles(submit_latencies)

    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_jobs": total_jobs,
        "concurrency": concurrency,
        "job_type": job_type,
        "submission": {
            "duration_sec": round(sub_duration, 3),
            "throughput_jobs_sec": round(submit_rate, 2),
            "latency_ms": submit_percentiles,
        },
        "processing": {
            "duration_sec": round(drain_duration, 3),
            "effective_throughput_jobs_sec": round(effective_throughput, 2),
            "turnaround_latency_ms": turnaround_percentiles,
            "status_counts": drain_status["counts"],
            "reliability_pct": round((drain_status["counts"].get("SUCCESS", 0) / total_jobs) * 100, 2),
        },
    }

    # Summary Display
    print("\n" + "=" * 65)
    print("  LOAD TEST RESULTS SUMMARY")
    print("=" * 65)
    print(f"  Total Jobs:             {total_jobs}")
    print(f"  Submission Rate:        {metrics['submission']['throughput_jobs_sec']:.1f} jobs/sec (over {sub_duration:.2f}s)")
    print(f"  Effective Throughput:   {metrics['processing']['effective_throughput_jobs_sec']:.1f} jobs/sec")
    print(f"  Success Rate:           {metrics['processing']['reliability_pct']:.1f}%")
    print("-" * 65)
    print("  Turnaround Latency (submit -> worker completion):")
    print(f"    p50:  {turnaround_percentiles['p50']:>7.2f} ms")
    print(f"    p90:  {turnaround_percentiles['p90']:>7.2f} ms")
    print(f"    p95:  {turnaround_percentiles['p95']:>7.2f} ms")
    print(f"    p99:  {turnaround_percentiles['p99']:>7.2f} ms")
    print(f"    min:  {turnaround_percentiles['min']:>7.2f} ms | max: {turnaround_percentiles['max']:>7.2f} ms")
    print(f"    avg:  {turnaround_percentiles['mean']:>7.2f} ms ± {turnaround_percentiles['stddev']:.2f} ms")
    print("=" * 65 + "\n")

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DistribuQ Load Generator")
    parser.add_argument("--url", default="http://localhost:8000", help="API URL")
    parser.add_argument("--db", default="postgresql://distribuq:password@localhost:5432/distribuq", help="DB URL")
    parser.add_argument("--jobs", type=int, default=500, help="Total jobs to submit")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent submitters")
    parser.add_argument("--type", default="test_echo", help="Job type")
    parser.add_argument("--priority", type=int, default=0, help="Job priority")
    parser.add_argument("--timeout", type=float, default=120.0, help="Drain timeout")
    parser.add_argument("--output", default=None, help="Save metrics to JSON file")

    args = parser.parse_args()

    res = asyncio.run(
        run_load_test(
            api_url=args.url,
            db_url=args.db,
            total_jobs=args.jobs,
            concurrency=args.concurrency,
            job_type=args.type,
            priority=args.priority,
            timeout_sec=args.timeout,
        )
    )

    if args.output:
        with open(args.output, "w") as f:
            json.dump(res, f, indent=2)
        print(f"Results saved to {args.output}")
