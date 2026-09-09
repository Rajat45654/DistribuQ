import asyncio
import sys
import httpx
from uuid import UUID

API_URL = "http://localhost:8080"


async def run_phase2_demo():
    print("=" * 70)
    print(" DistribuQ Phase 2 - Multi-Worker Concurrency & Heartbeat Demo")
    print("=" * 70)

    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0) as client:
        # 1. Check API Health
        health = await client.get("/healthz")
        print(f"\n[1] API Health: {health.json()}")

        # 2. Check Active Workers
        workers_resp = await client.get("/api/v1/workers")
        workers = workers_resp.json()
        print(f"\n[2] Registered Workers ({len(workers)} found):")
        for w in workers:
            print(f"    - ID: {w['id']} | Status: {w['status']} | Processed: {w['jobs_processed']} | Last Heartbeat: {w['last_heartbeat']}")

        # 3. Submit 15 Concurrent Math Jobs
        print("\n[3] Submitting 15 concurrent jobs to the queue...")
        submitted = []
        for i in range(1, 16):
            resp = await client.post(
                "/api/v1/jobs",
                json={"type": "math.multiply", "payload": {"a": i, "b": 10}},
            )
            job = resp.json()
            submitted.append(job["job_id"])
            print(f"    -> Job {i:02d} submitted: {job['job_id']} (PENDING)")

        # 4. Wait for processing across the worker pool
        print("\n[4] Waiting 3 seconds for workers to process jobs concurrently...")
        await asyncio.sleep(3.0)

        # 5. Check Completion & Distribution
        print("\n[5] Verifying job results and worker assignment:")
        for i, jid in enumerate(submitted, 1):
            detail = await client.get(f"/api/v1/jobs/{jid}")
            d = detail.json()
            print(f"    Job {i:02d}: Status: {d['status']} | Result: {d['result']} | Attempts: {d['attempts']}")

        # 6. Updated Worker Stats
        workers_resp2 = await client.get("/api/v1/workers")
        print(f"\n[6] Updated Worker Pool Stats:")
        for w in workers_resp2.json():
            print(f"    - ID: {w['id']} | Status: {w['status']} | Total Processed: {w['jobs_processed']}")

        print("\nPhase 2 demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_phase2_demo())
