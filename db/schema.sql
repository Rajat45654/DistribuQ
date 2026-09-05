-- DistribuQ Database Schema
-- Single source of truth for job states, worker registry, and dead letters.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Function to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1. jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'PENDING' 
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING', 'DEAD_LETTER')),
    priority SMALLINT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ NULL,
    scheduled_for TIMESTAMPTZ NULL,
    recurrence_rule TEXT NULL,
    result JSONB NULL,
    error TEXT NULL,
    locked_by TEXT NULL,
    locked_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER set_jobs_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Indexes for high-throughput queries and scheduler/reaper operations
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_reaper ON jobs (status, locked_at);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduler_due ON jobs (status, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_jobs_retry_due ON jobs (status, next_retry_at);

-- 2. workers table (for Phase 2+ worker heartbeats)
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'ALIVE' CHECK (status IN ('ALIVE', 'DEAD')),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    jobs_processed INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat ON workers (status, last_heartbeat);

-- 3. dead_letters table (for Phase 3+ unrecoverable jobs)
CREATE TABLE IF NOT EXISTS dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    final_error TEXT NOT NULL,
    attempts_made INT NOT NULL,
    moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_job_id ON dead_letters (job_id);
