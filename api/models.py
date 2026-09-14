from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class JobCreateRequest(BaseModel):
    type: str = Field(..., description="Job type identifier used for handler dispatch", min_length=1)
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary JSON payload")
    priority: int = Field(default=0, description="Priority level (higher is more urgent)")
    max_attempts: int = Field(default=3, ge=1, le=50, description="Maximum retry attempts allowed")
    scheduled_for: Optional[datetime] = Field(default=None, description="Future execution timestamp")
    recurrence_rule: Optional[str] = Field(default=None, description="Recurrence interval or cron expression")


class JobCreateResponse(BaseModel):
    job_id: UUID
    status: JobStatus


class JobDetailResponse(BaseModel):
    id: UUID
    type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    next_retry_at: Optional[datetime] = None
    scheduled_for: Optional[datetime] = None
    recurrence_rule: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    locked_by: Optional[str] = None
    locked_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class WorkerStatus(str, Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"


class WorkerDetailResponse(BaseModel):
    id: str
    status: WorkerStatus
    last_heartbeat: datetime
    jobs_processed: int
    started_at: datetime


class DeadLetterResponse(BaseModel):
    id: UUID
    job_id: UUID
    final_error: str
    attempts_made: int
    moved_at: datetime
    job_type: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class ReplayResponse(BaseModel):
    message: str
    job_id: UUID
    dead_letter_id: UUID
    status: JobStatus


