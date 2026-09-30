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
```

---

## Status Roadmap
- [x] **Phase 1: Core Queue, Single Worker** *(Completed)*
- [x] **Phase 2: Multiple Workers, Concurrency Safety** *(Completed)*
- [x] **Phase 3: Retries, Backoff, Dead-Letter Queue** *(Completed)*
- [x] **Phase 4: Scheduling - Delayed Jobs, Recurring Jobs, Priority Queues** *(Completed)*
- [ ] **Phase 5: Real-Time Dashboard**
- [ ] **Phase 6: Performance Testing & Scaling Analysis**

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
