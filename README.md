# DistribuQ: Distributed Task Queue & Worker Orchestration System

DistribuQ is a distributed task queue system designed for high concurrency, fault tolerance, and observable background job execution.

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Client /  │─────▶│   REST API   │─────▶│    Queue     │
│  Producer   │      │  (FastAPI)   │      │  (Redis)     │
└─────────────┘      └──────────────┘      └──────┬──────┘
                            │                     │
                            ▼                     ▼
                   ┌──────────────┐      ┌─────────────┐
                   │  Job Store   │      │   Worker    │
                   │  (Postgres)  │◀────▶│    Pool      │
                   └──────────────┘      └──────┬──────┘
                            ▲                     │
                            └── status updates ────┘
                                      │
                                      ▼
                            ┌───────────────────┐
                            │  Live Dashboard    │
                            │  (WebSockets, JS)  │
                            └───────────────────┘
```

## Tech Stack
- **API Framework**: FastAPI, Uvicorn (async-native)
- **Transport Queue**: Redis (FIFO via `LPUSH` / `BRPOP`, priority tiers via `queue:high/default/low`, scheduled set via sorted set)
- **Job Store**: PostgreSQL 16 (single source of truth with UUIDs, JSONB payloads, and indexing)
- **Scheduling**: Dedicated scheduler process + Redis sorted set for delayed/recurring jobs; `croniter` for cron expressions
- **Testing**: Pytest, Pytest-Asyncio, HTTPX

---

## Quickstart

### 1. Start Infrastructure (Postgres & Redis)
```bash
docker compose up -d
```

### 2. Install Dependencies
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run Automated Acceptance Tests
```bash
pytest tests/ -v
```

### 4. Run the API Service
```bash
python -m api.run
```
API Documentation is available interactively at `http://localhost:8080/docs`.

### 5. Run Workers and Reaper
```bash
# In separate terminals:
python -m worker.worker
python -m worker.reaper
```

### 6. Run the Scheduler (for Delayed & Recurring Jobs)
```bash
python -m scheduler.scheduler
```

### 7. Run Live Demos
```bash
# Phase 1: Core single-worker queue
python scripts/demo_phase1.py

# Phase 2: Multi-worker concurrency & crash recovery
python scripts/demo_phase2.py

# Phase 3: Retries, exponential backoff, DLQ & replay
python scripts/demo_phase3.py

# Phase 4: Delayed jobs, recurring jobs, priority queues
python scripts/demo_phase4.py

# Phase 5: Real-time dashboard & live WebSocket events
python scripts/demo_phase5.py
```

---

## Status Roadmap
- [x] **Phase 1: Core Queue, Single Worker** *(Completed)*
- [x] **Phase 2: Multiple Workers, Concurrency Safety** *(Completed)*
- [x] **Phase 3: Retries, Backoff, Dead-Letter Queue** *(Completed)*
- [x] **Phase 4: Scheduling - Delayed Jobs, Recurring Jobs, Priority Queues** *(Completed)*
- [x] **Phase 5: Real-Time Dashboard** *(Completed)*
- [x] **Phase 6: Performance Testing & Scaling Analysis** *(Completed)*

---

## Phase 4 Features

### Delayed Jobs
Submit a job with a `scheduled_for` timestamp - it will not execute until that time.
```json
{ "type": "test_echo", "payload": {}, "scheduled_for": "2024-12-01T12:00:00Z" }
```
The Scheduler daemon (`python -m scheduler.scheduler`) monitors a Redis sorted set and promotes due jobs to the active queue atomically via a Lua script.

### Recurring Jobs
Submit a job with a `recurrence_rule` - the next occurrence is automatically scheduled after each successful run.
```json
{ "type": "test_echo", "payload": {}, "recurrence_rule": "@every 5m" }
```
Supported formats:
- `@every <N>s/m/h/d` - e.g. `@every 30s`, `@every 1h30m`
- Standard 5-field cron - e.g. `*/5 * * * *`, `0 9 * * 1`

### Priority Queues
Jobs route to different Redis lists based on priority:
- `priority > 0` → `queue:high`
- `priority == 0` → `queue:default` (default)
- `priority < 0` → `queue:low`

Workers drain `queue:high` before `queue:default` before `queue:low`.
```json
{ "type": "urgent_task", "payload": {}, "priority": 10 }
```

---

## Phase 5 Features — Real-Time Dashboard

Navigate to `http://localhost:8000/dashboard/` (or `http://localhost:8000/` which redirects there) to open the live web dashboard.

### Key Capabilities
- **Live Status Cards**: Real-time reactive counters for `PENDING`, `RUNNING`, `SUCCESS`, `RETRYING`, `FAILED`, and `DEAD_LETTER` with proportional status bar fills.
- **WebSocket Event Bus (`/ws/dashboard`)**: Redis pub/sub channel `distribuq:events` fans out instant state changes to all connected web clients without page reloads.
- **Real-Time Throughput Chart**: Built with native HTML5 Canvas, tracking rolling completed jobs (success vs. failure rates) in 5-second sampling buckets.
- **Live Event Stream**: Real-time console showing timestamped job transitions, worker heartbeats, and system events with colored status badges.
- **Worker Health Monitor**: Displays registered workers with pulsating green/red status indicators, heartbeat timestamps, and lifetime job counts.
- **Job Inspection & Details**: Interactive table of recent jobs with click-to-view modal displaying JSON payload, execution results, error stack traces, and attempt counts.
- **Interactive Controls**:
  - **Submit Job**: Modal dialog allowing on-the-fly job creation with custom type, priority, max attempts, and payload.
  - **Chaos Injection (`/api/v1/dev/kill-worker`)**: Kills an active worker to demonstrate automatic visibility timeout reclamation and failover.

---

## Phase 6 Features — Performance Testing & Scaling Analysis

Full empirical benchmark documentation and bottleneck analysis is available in [BENCHMARKS.md](file:///d:/DistribuQ/BENCHMARKS.md).

### 1. High-Throughput Async Load Generator
Simulate high-concurrency client traffic with configurable submission rates and execution durations:
```bash
python benchmarks/load_test.py --jobs 500 --concurrency 25 --rate 100
```
Measures:
- Total throughput (jobs/sec submitted and processed)
- Latency percentiles: p50, p90, p95, p99 turnaround times
- Error rate and database verification

### 2. Chaos Engineering Under Heavy Load
Tests system resilience by systematically injecting worker crashes during active pipeline processing:
```bash
python benchmarks/chaos_under_load.py --jobs 100 --kill-interval 3.0
```
Validates zero job loss, automatic visibility timeout expiration, and seamless reaper recovery.

### 3. Automated Multi-Worker Scaling Sweep
Automates worker pool scaling across 1, 5, 10, and 20 worker containers:
```bash
# Run full matrix sweep and regenerate charts
python benchmarks/run_benchmarks.py --jobs 300

# Regenerate report and charts from existing run data
python benchmarks/run_benchmarks.py --report-only
```
Results and charts are saved to:
- `benchmarks/results/benchmark_data.json`
- `benchmarks/results/scaling_charts.png`
