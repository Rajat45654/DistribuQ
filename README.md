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
- **Transport Queue**: Redis (FIFO via `LPUSH` / `BRPOP`, atomic reserve via `BRPOPLPUSH`)
- **Job Store**: PostgreSQL 16 (single source of truth with UUIDs, JSONB payloads, and indexing)
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

### 5. Run the Worker
```bash
python -m worker.worker
```

### 6. Run Live Demo
```bash
python scripts/demo_phase1.py
```

---

## Status Roadmap
- [x] **Phase 1: Core Queue, Single Worker** *(Completed)*
- [x] **Phase 2: Multiple Workers, Concurrency Safety** *(Completed)*
- [ ] **Phase 3: Retries, Backoff, Dead-Letter Queue**
- [ ] **Phase 4: Scheduling: Delayed & Recurring Jobs**
- [ ] **Phase 5: Real-Time Dashboard**
- [ ] **Phase 6: Performance Testing & Scaling Analysis**
