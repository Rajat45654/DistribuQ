# PROJECT_SPEC.md — DistribuQ: Distributed Task Queue & Worker Orchestration System

> This document is the single source of truth for an AI coding agent (or any collaborator) helping build this project. Read this fully before writing any code. It defines what the system is, why every major decision was made, the exact build order, data contracts, and the acceptance criteria for each phase. Do not skip phases or introduce technology not listed here without flagging it to the user first.

---

## 0. Project Identity

- **Name:** DistribuQ
- **One-line description:** A distributed task queue system (like a simplified Celery/Sidekiq/BullMQ) that lets a client submit background jobs via a REST API, executes them across a horizontally-scalable pool of worker processes, guarantees at-least-once delivery even when workers crash, supports retries/backoff/dead-lettering/scheduling, and exposes live system state via a WebSocket dashboard.
- **Purpose:** This is a learning + portfolio project built over ~3 months by a developer who already knows HTML/CSS/JS, Django, FastAPI, REST APIs, SQL/SQLite, Python, and C++. The goal is genuine systems-engineering depth (concurrency, fault tolerance, distributed coordination), not surface-level CRUD.
- **Non-goals:** This is not trying to replace Celery in production. It does not need enterprise auth, multi-region deployment, or billing. Scope discipline matters — resist feature creep that doesn't serve the learning goals below.

### What "done" means for the whole project
A user can:
1. `POST` a job to an API and get a `job_id` back immediately.
2. Watch that job be picked up and executed by one of several independently-running worker processes.
3. Kill a worker process mid-job and see the job automatically recovered and completed by a different worker, with zero data loss.
4. See a failed job retry with exponential backoff, and eventually land in a dead-letter queue if it never succeeds.
5. Schedule a job to run later, or on a recurring interval.
6. Watch all of this happen live on a dashboard (job counts, worker health, throughput).
7. Read a benchmark report showing throughput/latency/reliability numbers, and understand where the system's bottlenecks are and why.

---

## 1. Architecture

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

### Component responsibilities

| Component | Responsibility | Must NOT do |
|---|---|---|
| **API service** (FastAPI) | Accept job submissions, validate payloads, write initial job record to Postgres, push job onto Redis queue, expose query endpoints for job/worker status | Must not execute jobs itself |
| **Redis** | Transport layer only — holds queue of pending job IDs (or lightweight job refs), sorted set for scheduled jobs, "processing" list for in-flight jobs | Must not be the source of truth for job history — that's Postgres |
| **Postgres** | Source of truth for job state, retry counts, timestamps, results, dead-letter records, worker registry | — |
| **Worker process** | Pull job refs from Redis, fetch full job from Postgres, execute, update status, send heartbeats, ack/requeue on completion/failure | Must not talk directly to the API service |
| **Scheduler process** | Separate process (not a worker) that moves due delayed/recurring jobs from the Postgres/Redis scheduled set into the main queue | Must not execute jobs |
| **Dashboard** | Read-only WebSocket client showing live state, plus manual actions (replay dead-lettered job, kill worker for demo) | Must not bypass the API for writes |

### Why these specific technology choices (do not silently swap these without asking the user)
- **Redis over building a custom broker initially:** lets the system design work (retry logic, fault tolerance, race conditions) be validated before adding broker-building complexity. A custom C++ broker is an explicit Phase 6+ stretch goal, not a Phase 1 requirement.
- **Postgres over SQLite:** the system requires many concurrent writers (multiple workers updating job status simultaneously). SQLite's single-writer model is a poor fit; Postgres row-level locking and transactions are required for correctness.
- **FastAPI:** async-native, fits an I/O-bound API layer, developer already knows it.
- **WebSockets over polling:** the dashboard needs to reflect state changes (worker deaths, job completions) within seconds, not on a fixed poll interval, and polling many rows repeatedly is wasteful.

---

## 2. Data Model (Postgres)

### `jobs` table
| Column | Type | Notes |
|---|---|---|
| `id` | UUID, PK | |
| `type` | TEXT | job "kind" — used to dispatch to the correct handler function |
| `payload` | JSONB | arbitrary job arguments |
| `status` | TEXT | `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRYING`, `DEAD_LETTER` |
| `priority` | SMALLINT | default 0; higher = more urgent |
| `attempts` | INT | default 0 |
| `max_attempts` | INT | default 3 |
| `next_retry_at` | TIMESTAMPTZ | nullable |
| `scheduled_for` | TIMESTAMPTZ | nullable — for delayed jobs |
| `recurrence_rule` | TEXT | nullable — cron-like string for recurring jobs |
| `result` | JSONB | nullable |
| `error` | TEXT | nullable — last error message |
| `locked_by` | TEXT | nullable — worker ID currently processing this job |
| `locked_at` | TIMESTAMPTZ | nullable — for visibility timeout calculation |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |

### `workers` table
| Column | Type | Notes |
|---|---|---|
| `id` | TEXT, PK | worker-generated unique ID (hostname+pid or UUID) |
| `status` | TEXT | `ALIVE`, `DEAD` |
| `last_heartbeat` | TIMESTAMPTZ | |
| `jobs_processed` | INT | running counter |
| `started_at` | TIMESTAMPTZ | |

### `dead_letters` table
| Column | Type | Notes |
|---|---|---|
| `id` | UUID, PK | |
| `job_id` | UUID, FK → jobs.id | |
| `final_error` | TEXT | |
| `attempts_made` | INT | |
| `moved_at` | TIMESTAMPTZ | |

### Database Indexing Strategy
To ensure query performance remains O(log N) under heavy concurrency and scaling:
- `idx_jobs_status`: fast filtering for queries like `GET /jobs?status=...`
- `idx_jobs_reaper`: composite index on `(status, locked_at)` for visibility timeout cleanup (`status = 'RUNNING' AND locked_at < threshold`)
- `idx_jobs_scheduler_due`: composite index on `(status, scheduled_for)` for scheduled job promotion
- `idx_jobs_retry_due`: composite index on `(status, next_retry_at)` for retry promotion
- `idx_workers_last_heartbeat`: on `(status, last_heartbeat)` for worker liveness checks

### Worker Execution & Shutdown Lifecycle
- **Graceful Shutdown (`SIGINT`/`SIGTERM`):** Worker stops consuming new items from Redis, completes the in-flight job within a grace period, marks worker `DEAD` or deregisters, and closes connections cleanly.
- **Abrupt Termination (`SIGKILL` / `docker kill`):** Worker vanishes immediately without cleanup. Relies completely on visibility timeout and heartbeat reaper to reclaim the in-flight job with zero data loss.
- **Structured Logging:** All worker, API, and scheduler logs must include contextual tags: `worker_id`, `job_id`, and `job_type`.

**Job status state machine (must be enforced in code, not just convention):**
```
PENDING → RUNNING → SUCCESS
                  → FAILED → RETRYING → RUNNING (loop)
                           → DEAD_LETTER (after max_attempts exceeded)
```

---

## 3. API Contract (Phase 1+)

All endpoints under FastAPI, prefix `/api/v1`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a new job. Body: `{type, payload, priority?, max_attempts?, scheduled_for?, recurrence_rule?}`. Returns `{job_id, status}` |
| `GET` | `/jobs/{job_id}` | Full job record including retry history |
| `GET` | `/jobs?status=&type=&limit=&offset=` | List/filter jobs |
| `POST` | `/jobs/{job_id}/cancel` | Cancel a pending job (no-op if already running/done) |
| `GET` | `/dead-letters` | List dead-lettered jobs |
| `POST` | `/dead-letters/{id}/replay` | Re-submit a dead-lettered job as a new PENDING job |
| `GET` | `/workers` | List all known workers and health status |
| `WS` | `/ws/dashboard` | WebSocket stream of job/worker state changes |

Response bodies should be defined as Pydantic models — the agent should generate these explicitly, not return raw dicts.

---

## 4. Build Order (STRICT — do not reorder or skip)

The agent must build in this order. Each phase has explicit acceptance criteria; do not begin the next phase until the current one's criteria are demonstrably met (with a test or a script, not just "it should work").

### Phase 1 — Core Queue, Single Worker
**Build:**
- Postgres schema for `jobs` table
- FastAPI `POST /jobs` and `GET /jobs/{id}`
- Redis as a simple FIFO (`LPUSH` on submit, `BRPOP` in worker loop)
- One worker script: pop job ref → fetch from Postgres → execute a dummy handler (e.g., `sleep(n)` or basic math) → update status to `SUCCESS`/`FAILED`

**Acceptance criteria:**
- Submitting a job via `POST /jobs` returns a `job_id`
- Within a few seconds, `GET /jobs/{id}` shows `status: SUCCESS`
- Restarting the worker does not lose in-flight jobs from before restart (they remain `PENDING` in Postgres if not yet popped)

### Phase 2 — Multiple Workers, Concurrency Safety
**Build:**
- Docker Compose setup running 3–5 worker containers
- Replace naive `BRPOP` with an atomic reserve pattern (`BRPOPLPUSH` into a per-worker "processing" list, or Redis `RPOPLPUSH`) so a job is never handed to two workers at once
- Worker heartbeat: each worker upserts its row in `workers` table every N seconds
- Visibility timeout: if `locked_at` is older than timeout X and job is still `RUNNING`, a reaper process/routine requeues it

**Acceptance criteria:**
- Load test: submit 1,000 jobs with 5 workers running concurrently → every job reaches `SUCCESS` exactly once, verified by checking no job was processed by more than one worker (log worker ID per completion, assert uniqueness)
- Kill one worker container mid-run (`docker kill`) → its in-flight job is requeued and completed by a different worker within (heartbeat_timeout + a few seconds)

### Phase 3 — Retries, Backoff, Dead-Letter Queue
**Build:**
- Job handler can raise an exception → status goes to `FAILED` → if `attempts < max_attempts`, compute `next_retry_at` using exponential backoff (e.g., `2^attempts * base_delay`), set status `RETRYING`
- A scheduler/reaper picks up jobs where `next_retry_at <= now()` and re-enqueues them
- After `max_attempts` exceeded, move job to `dead_letters` table, status → `DEAD_LETTER`
- `GET /dead-letters` and `POST /dead-letters/{id}/replay` endpoints

**Acceptance criteria:**
- A job handler that always fails ends up in `dead_letters` after exactly `max_attempts` tries, with correct exponential delays between attempts (assert timestamps)
- Chaos test script: run N jobs, randomly `docker kill -9` workers throughout, assert that 100% of jobs eventually reach `SUCCESS` or `DEAD_LETTER` (never stuck in `RUNNING` forever, never silently lost) — **this test result must be saved as a written report**, it's the project's core claim

### Phase 4 — Scheduling: Delayed & Recurring Jobs
**Build:**
- `scheduled_for` support: job stays out of the main queue until `scheduled_for <= now()`; separate scheduler process polls for due jobs and enqueues them
- `recurrence_rule` support (start simple: fixed interval in seconds/minutes; stretch: cron syntax via `croniter`) — on completion of a recurring job, automatically create the next occurrence
- Priority queues: `priority` field routes jobs into separate Redis lists (e.g., `queue:high`, `queue:default`, `queue:low`); workers check high before default before low

**Acceptance criteria:**
- A job scheduled 30 seconds in the future does not run before then (assert actual execution timestamp)
- A recurring job (e.g., every 60s) fires at least 3 times in a row automatically with no manual re-submission
- High-priority jobs submitted after low-priority jobs still get picked up first when workers are busy

### Phase 5 — Real-Time Dashboard
**Build:**
- `/ws/dashboard` WebSocket endpoint broadcasting: job status changes, worker heartbeat/health changes, running counters
- Frontend (plain HTML/CSS/JS, no framework required) showing:
  - Live counts per job status
  - Worker list with alive/dead indicator and last heartbeat
  - Throughput chart (jobs completed per time window)
  - Job detail drill-down with retry history
  - A "chaos" button that calls a dev-only endpoint to kill a random worker (for demo purposes)

**Acceptance criteria:**
- Opening the dashboard in a browser and submitting jobs via the API shows live updates without refreshing the page
- Killing a worker via the dashboard button visibly updates worker status to dead within one heartbeat interval, and the affected job visibly requeues

### Phase 6 — Performance Testing & Scaling Analysis
**Build:**
- Load generation script (Locust or custom async script) that submits jobs at configurable rates
- Chaos script that kills random workers at configurable intervals during load tests
- Benchmark harness that runs the same load test at 1, 5, 10, 20 workers and records throughput + latency percentiles (p50/p95/p99) + reliability (% jobs eventually successful)
- `BENCHMARKS.md` report: methodology, raw numbers, graphs (matplotlib is fine), and a written analysis of the bottleneck (expect Postgres write contention or Redis single-thread limits) — must be honest, not just "it works great"

**Acceptance criteria:**
- Benchmark report exists with real numbers from real runs (not estimated), covering throughput vs. worker count and latency percentiles
- The report explicitly identifies where the system stops scaling linearly and gives a reasoned explanation

---

## 5. Testing & Verification Philosophy

The agent must treat **fault-injection and correctness testing as first-class deliverables**, not an afterthought:
- Every phase's acceptance criteria above must be checked with an actual script/test, and results saved (logs, screenshots, or a short markdown report) — not just "I ran it once and it looked fine."
- Chaos testing (`docker kill` on running workers) is central to this project's value — do not skip it or fake it with graceful shutdowns only.
- Race conditions (duplicate job execution) must be actively tested for, not assumed away.

---

## 6. Repository Structure

```
distribuq/
├── api/                  # FastAPI job submission + query service
│   ├── main.py
│   ├── models.py         # Pydantic request/response models
│   ├── db.py              # Postgres access layer
│   └── redis_client.py
├── worker/
│   ├── worker.py          # main worker loop
│   ├── handlers/          # job type → handler function registry
│   └── heartbeat.py
├── scheduler/
│   └── scheduler.py       # delayed + recurring job promotion, retry requeueing
├── dashboard/
│   ├── index.html
│   ├── app.js
│   └── style.css
├── broker/                 # (Phase 6+ stretch) custom C++ broker
├── chaos/
│   └── kill_worker.py      # chaos testing scripts
├── benchmarks/
│   ├── load_test.py
│   └── results/            # raw output + graphs
├── db/
│   └── schema.sql
├── docker-compose.yml
├── README.md               # architecture, setup instructions, design decisions
├── BENCHMARKS.md            # Phase 6 report
└── PROJECT_SPEC.md          # this file
```

---

## 7. Constraints & Guardrails for the Agent

- **Do not introduce a new major dependency or swap a core technology** (e.g., switching Postgres for MongoDB, or Redis for RabbitMQ) without explicitly asking the user first — these choices were made deliberately for the learning goals of the project.
- **Do not skip ahead to later phases** (e.g., building the dashboard before retries/dead-letter logic exists) even if it seems more visually rewarding — the fault-tolerance core (Phase 2–3) is the point of the project.
- **Do not fake or mock the chaos tests** — they must run against real separate processes/containers being actually killed, not simulated in-process.
- **Prefer explicit, readable code over cleverness** — this project's value is that the developer can explain every design decision in an interview. Avoid unexplained abstractions.
- **Every phase should end with something demoable**, even if rough — a CLI output, a curl command sequence, or a screenshot, so progress is checkable incrementally.
- When in doubt about scope, favor **doing less but understanding it fully** over adding more surface features.

---

## 8. Current Status

*(The agent/human should update this section as work progresses — mark phases complete, note deviations from this plan and why.)*

- [x] Phase 1 — Core Queue, Single Worker
- [ ] Phase 2 — Multiple Workers, Concurrency Safety
- [ ] Phase 3 — Retries, Backoff, Dead-Letter Queue
- [ ] Phase 4 — Scheduling
- [ ] Phase 5 — Real-Time Dashboard
- [ ] Phase 6 — Performance Testing & Scaling Analysis
