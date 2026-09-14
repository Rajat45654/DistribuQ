import asyncio
import sys
from uuid import UUID
import httpx

API_URL = "http://localhost:8080"


async def run_phase3_demo():
    print("=" * 75)
    print(" DistribuQ Phase 3 - Retries, Exponential Backoff & DLQ Replay Demo")
    print("=" * 75)

    async with httpx.AsyncClient(base_url=API_URL, timeout=15.0) as client:
        # 1. Check API Health
        try:
            health = await client.get("/healthz")
            print(f"\n[1] API Health Status: {health.json()}")
        except Exception as err:
            print(f"\n[1] Error: Cannot connect to API at {API_URL}: {err}")
            print("Please ensure the API is running via: uvicorn api.main:app --port 8080")
            return

        # 2. Check Registered Workers
        workers_resp = await client.get("/api/v1/workers")
        workers = workers_resp.json()
        print(f"\n[2] Registered Workers ({len(workers)} online):")
        for w in workers:
            print(f"    - Worker: {w['id']} | Status: {w['status']} | Processed: {w['jobs_processed']}")

        # 3. Submit a Flaky Job (Transient failure recovered via retries)
        print("\n[3] Submitting a transient 'flaky' job (recovers on attempt 2)...")
        flaky_resp = await client.post(
            "/api/v1/jobs",
            json={
                "type": "flaky",
                "payload": {"task_key": "demo-flaky", "fail_until_attempt": 2},
                "max_attempts": 3,
            },
        )
        flaky_job_id = flaky_resp.json()["job_id"]
        print(f"    -> Flaky job submitted: {flaky_job_id} (max_attempts: 3)")

        # 4. Submit an Unrecoverable Job (Lands in Dead-Letter Queue)
        print("\n[4] Submitting a persistently failing job (enters Dead-Letter Queue)...")
        fail_resp = await client.post(
            "/api/v1/jobs",
            json={
                "type": "fail",
                "payload": {"reason": "Non-recoverable external API timeout"},
                "max_attempts": 2,
            },
        )
        fail_job_id = fail_resp.json()["job_id"]
        print(f"    -> Failing job submitted: {fail_job_id} (max_attempts: 2)")

        # 5. Monitor Job Lifecycle
        print("\n[5] Monitoring job state progression (polling for 6 seconds)...")
        for step in range(1, 7):
            await asyncio.sleep(1.0)
            flaky_state = (await client.get(f"/api/v1/jobs/{flaky_job_id}")).json()
            fail_state = (await client.get(f"/api/v1/jobs/{fail_job_id}")).json()

            print(
                f"    [T+{step}s] Flaky: {flaky_state['status']} (attempts: {flaky_state['attempts']}) | "
                f"Fatal: {fail_state['status']} (attempts: {fail_state['attempts']})"
            )
            if flaky_state["status"] == "SUCCESS" and fail_state["status"] == "DEAD_LETTER":
                print("    -> Both jobs have settled into their expected states!")
                break

        # 6. Inspect Dead-Letter Queue
        print("\n[6] Inspecting Dead-Letter Queue via GET /api/v1/dead-letters...")
        dlq_resp = await client.get("/api/v1/dead-letters")
        dlq_items = dlq_resp.json()
        print(f"    Total items in DLQ: {len(dlq_items)}")
        target_dl = None
        for dl in dlq_items:
            print(f"    - DLQ ID: {dl['id']} | Job: {dl['job_id']} | Attempts: {dl['attempts_made']}")
            print(f"      Final Error: {dl['final_error']}")
            if dl["job_id"] == fail_job_id:
                target_dl = dl

        # 7. Replay Dead-Lettered Job
        if target_dl:
            print(f"\n[7] Replaying dead-lettered job {fail_job_id} via POST /api/v1/dead-letters/{target_dl['id']}/replay...")
            replay_resp = await client.post(f"/api/v1/dead-letters/{target_dl['id']}/replay")
            print(f"    -> Replay Response: {replay_resp.json()}")

            replayed_job = (await client.get(f"/api/v1/jobs/{fail_job_id}")).json()
            print(f"    -> Job State after Replay: {replayed_job['status']} (attempts: {replayed_job['attempts']}, error: {replayed_job['error']})")

        print("\n" + "=" * 75)
        print(" Phase 3 demo completed successfully!")
        print("=" * 75)


if __name__ == "__main__":
    asyncio.run(run_phase3_demo())
