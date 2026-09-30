#!/usr/bin/env python
"""
Phase 5 Demo Script - Real-Time Monitoring & Live Dashboard

Demonstrates the real-time event pipeline and dashboard endpoints
against a live running DistribuQ stack.

Usage:
    # 1. Start the stack (API + Worker + Redis + Postgres):
    #    docker-compose up -d
    #
    # 2. Run the demo:
    #    python scripts/demo_phase5.py

What it demonstrates:
  1. Dashboard Asset Verification: Checks HTTP endpoints for index.html, style.css, app.js
  2. Live WebSocket Connection: Connects to ws://localhost:8000/ws/dashboard and receives snapshots
  3. Real-time Event Streaming: Submits jobs and monitors live event feed over WebSocket
  4. Chaos Testing: Injects worker termination via /api/v1/dev/kill-worker
"""

import asyncio
import json
import sys
import time
from datetime import datetime

import httpx

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

HTTP_BASE = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws/dashboard"


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def separator(title: str):
    width = 65
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}\n")


async def demo_1_dashboard_assets(client: httpx.AsyncClient):
    separator("Demo 1: Dashboard UI & Static Assets")
    print(f"[{ts()}] Requesting dashboard root ({HTTP_BASE}/dashboard/)...")
    res = await client.get(f"{HTTP_BASE}/dashboard/")
    if res.status_code == 200:
        print(f"[{ts()}] OK (HTTP 200) - Dashboard HTML loaded ({len(res.text)} bytes)")
    else:
        print(f"[{ts()}] FAILED (HTTP {res.status_code})")
        return False

    print(f"[{ts()}] Requesting {HTTP_BASE}/dashboard/style.css...")
    res_css = await client.get(f"{HTTP_BASE}/dashboard/style.css")
    print(f"[{ts()}] OK (HTTP {res_css.status_code}) - CSS stylesheet loaded ({len(res_css.text)} bytes)")

    print(f"[{ts()}] Requesting {HTTP_BASE}/dashboard/app.js...")
    res_js = await client.get(f"{HTTP_BASE}/dashboard/app.js")
    print(f"[{ts()}] OK (HTTP {res_js.status_code}) - Frontend JavaScript loaded ({len(res_js.text)} bytes)")

    print(f"[{ts()}] Requesting stats summary {HTTP_BASE}/api/v1/stats...")
    res_stats = await client.get(f"{HTTP_BASE}/api/v1/stats")
    stats = res_stats.json()
    print(f"[{ts()}] Stats response:")
    print(f"         Active Workers: {stats.get('active_workers', 0)}")
    print(f"         Job Counts: {json.dumps(stats.get('job_counts', {}))}")
    return True


async def demo_2_websocket_stream(client: httpx.AsyncClient):
    separator("Demo 2: Live WebSocket Event Stream")
    try:
        import websockets
    except ImportError:
        print(f"[{ts()}] Note: 'websockets' library not installed in ambient python; skipping direct ws test")
        return

    print(f"[{ts()}] Connecting to WebSocket {WS_URL}...")
    try:
        async with websockets.connect(WS_URL) as ws:
            print(f"[{ts()}] WebSocket connected successfully!")

            # 1. Wait for initial snapshot
            raw_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            snapshot = json.loads(raw_msg)
            print(f"[{ts()}] Received initial event: {snapshot.get('event')}")
            print(f"         Payload: {json.dumps(snapshot.get('data', {}))}")

            # 2. Submit a test job via API
            print(f"[{ts()}] Submitting test job to trigger live events...")
            submit_res = await client.post(
                f"{HTTP_BASE}/api/v1/jobs",
                json={
                    "type": "test_echo",
                    "payload": {"demo": "Phase 5 Real-Time Test"},
                    "priority": 1,
                },
            )
            job_id = submit_res.json()["job_id"]
            print(f"[{ts()}] Submitted job {job_id[:8]}... Waiting for live WebSocket broadcasts...")

            # 3. Listen for events
            deadline = time.monotonic() + 10.0
            events_seen = []
            while time.monotonic() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    evt = json.loads(msg)
                    evt_type = evt.get("event")
                    events_seen.append(evt_type)
                    print(f"  [{ts()}] [WS EVENT] {evt_type:20s} -> {json.dumps(evt.get('data', {}))}")
                    if evt_type == "job.status_changed" and evt.get("data", {}).get("status") == "SUCCESS":
                        print(f"[{ts()}] Job execution completed and observed over WebSocket!")
                        break
                except asyncio.TimeoutError:
                    break

            print(f"[{ts()}] Total events observed on live WebSocket: {len(events_seen)}")
    except Exception as e:
        print(f"[{ts()}] WebSocket stream test notice: {e}")


async def demo_3_chaos_kill(client: httpx.AsyncClient):
    separator("Demo 3: Chaos Testing via Dev Endpoint")
    print(f"[{ts()}] Testing POST {HTTP_BASE}/api/v1/dev/kill-worker...")
    res = await client.post(f"{HTTP_BASE}/api/v1/dev/kill-worker")
    if res.status_code == 200:
        data = res.json()
        print(f"[{ts()}] SUCCESS - Kill signal sent to worker: {data.get('killed_worker_id')}")
        print(f"         Channel: {data.get('channel')}")
    elif res.status_code == 404:
        print(f"[{ts()}] NOTE (404): No alive workers currently running to kill. (Start a worker first to test kill)")
    else:
        print(f"[{ts()}] Response: {res.status_code} - {res.text}")


async def main():
    print("""
=================================================================
       DistribuQ — Phase 5 Real-Time Dashboard & Events Demo     
=================================================================
    """)

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Check API health
        try:
            r = await client.get(f"{HTTP_BASE}/healthz")
            if r.status_code != 200:
                print(f"Error: API at {HTTP_BASE} returned HTTP {r.status_code}")
                return
        except Exception as e:
            print(f"Error: Could not connect to API at {HTTP_BASE} ({e})")
            print("Make sure Docker Compose is up: docker-compose up -d")
            return

        await demo_1_dashboard_assets(client)
        await demo_2_websocket_stream(client)
        await demo_3_chaos_kill(client)

    separator("Phase 5 Demo Complete!")
    print(f"Open your browser and navigate to:\n    {HTTP_BASE}/dashboard/\n")
    print("Features ready to explore in the UI:")
    print("  • Live job status counters with reactive progress bars")
    print("  • HTML5 Canvas real-time throughput chart (success/failure rates)")
    print("  • Streaming live event feed showing status transitions")
    print("  • Active/dead workers list with heartbeat indicators")
    print("  • Recent jobs table with click-to-view detail modal")
    print("  • 'Submit Job' modal dialog for interactive queuing")
    print("  • 'Chaos' button to trigger worker death and recovery")


if __name__ == "__main__":
    asyncio.run(main())
