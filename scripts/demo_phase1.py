import asyncio
import sys
import httpx
from uuid import UUID

API_URL = "http://localhost:8080"


async def run_demo():
    print("=" * 60)
    print(" DistribuQ Phase 1 - Live Verification Demo")
    print("=" * 60)

    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0) as client:
        # Check health
        health = await client.get("/healthz")
        print(f"\n[1] API Health Check: {health.json()}")

        # Submit Job 1: Math Add
        print("\n[2] Submitting Job 1 ('math.add', {'a': 100, 'b': 250})...")
        r1 = await client.post("/api/v1/jobs", json={"type": "math.add", "payload": {"a": 100, "b": 250}})
        job1 = r1.json()
        print(f"    -> Response: {job1}")

        # Submit Job 2: Math Multiply
        print("\n[3] Submitting Job 2 ('math.multiply', {'a': 12, 'b': 12})...")
        r2 = await client.post("/api/v1/jobs", json={"type": "math.multiply", "payload": {"a": 12, "b": 12}})
        job2 = r2.json()
        print(f"    -> Response: {job2}")

        # Submit Job 3: Echo
        print("\n[4] Submitting Job 3 ('echo', {'message': 'Hello DistribuQ Systems Engineering!'})...")
        r3 = await client.post("/api/v1/jobs", json={"type": "echo", "payload": {"message": "Hello DistribuQ Systems Engineering!"}})
        job3 = r3.json()
        print(f"    -> Response: {job3}")

        # Wait for worker to consume
        print("\n[5] Waiting 2 seconds for worker execution...")
        await asyncio.sleep(2.0)

        # Query full job details
        for i, j in enumerate([job1, job2, job3], 1):
            detail = await client.get(f"/api/v1/jobs/{j['job_id']}")
            d = detail.json()
            print(f"\n[Job {i} Detail]")
            print(f"    ID:       {d['id']}")
            print(f"    Type:     {d['type']}")
            print(f"    Status:   {d['status']}")
            print(f"    Attempts: {d['attempts']}")
            print(f"    Result:   {d['result']}")
            print(f"    Created:  {d['created_at']}")
            print(f"    Updated:  {d['updated_at']}")

        # List all jobs
        list_resp = await client.get("/api/v1/jobs?limit=5")
        print(f"\n[6] GET /api/v1/jobs returned {len(list_resp.json())} recent jobs.")
        print("\nPhase 1 live demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_demo())
