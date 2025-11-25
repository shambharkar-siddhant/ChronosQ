"""
Core data models for ChronosQ (file-based job broker).

These models are intentionally lightweight and stdlib-only so they can be
used across the broker, recovery engine, CLI, and example workers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt
import uuid


class JobStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILURE = "failure"
    HOLD = "hold"


@dataclass
class Job:
    """
    A unit of work in ChronosQ.

    This is the logical representation of a job, independent of how it is
    stored on disk. File paths are tracked separately in JobState.
    """

    job_id: str
    job_type: str
    payload: Dict[str, Any]
    created_at: dt.datetime
    priority: str = "normal"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """
        Create a Job from a dictionary (e.g. parsed JSON event/job file).
        """
        job_id = data.get("job_id") or str(uuid.uuid4())
        job_type = data.get("type") or data.get("job_type")
        if not job_type:
            raise ValueError("Missing 'type'/'job_type' in job data")

        # created_at may be string or omitted
        created_raw = data.get("created_at")
        if isinstance(created_raw, str):
            try:
                created_at = dt.datetime.fromisoformat(created_raw)
            except ValueError:
                # fallback
                created_at = dt.datetime.utcnow()
        else:
            created_at = created_raw or dt.datetime.utcnow()

        priority = data.get("priority", "normal")

        payload = data.get("payload") or {}

        return cls(
            job_id=job_id,
            job_type=str(job_type),
            payload=payload,
            created_at=created_at,
            priority=priority,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert Job back to a dict suitable for JSON serialization.
        """
        return {
            "job_id": self.job_id,
            "type": self.job_type,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
            "priority": self.priority,
        }


@dataclass
class Worker:
    """
    A logical worker that can process jobs of certain types.

    In ChronosQ, each worker is usually mapped to a directory representing
    its queue of assigned jobs.
    """

    worker_id: str
    supported_types: List[str] = field(default_factory=list)
    max_concurrent: int = 1  # for future extension

    def can_handle(self, job_type: str) -> bool:
        return not self.supported_types or job_type in self.supported_types


@dataclass
class JobState:
    """
    Tracks the on-disk and lifecycle state of a job.

    This metadata is stored separately (e.g., as job_id.state.json) so that
    job files remain purely about payload.
    """

    job_id: str
    status: JobStatus
    assigned_to: Optional[str] = None
    attempts: int = 0
    created_at: dt.datetime = field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = field(default_factory=dt.datetime.utcnow)
    assigned_at: Optional[dt.datetime] = None
    last_error: Optional[str] = None

    # File-system related fields
    job_path: Optional[Path] = None
    state_path: Optional[Path] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "assigned_to": self.assigned_to,
            "attempts": self.attempts,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "assigned_at": self.assigned_at.isoformat() if self.assigned_at else None,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobState":
        def parse_dt(value: Optional[str]) -> Optional[dt.datetime]:
            if not value:
                return None
            try:
                return dt.datetime.fromisoformat(value)
            except ValueError:
                return None

        return cls(
            job_id=data["job_id"],
            status=JobStatus(data["status"]),
            assigned_to=data.get("assigned_to"),
            attempts=int(data.get("attempts", 0)),
            created_at=parse_dt(data.get("created_at")) or dt.datetime.utcnow(),
            updated_at=parse_dt(data.get("updated_at")) or dt.datetime.utcnow(),
            assigned_at=parse_dt(data.get("assigned_at")),
            last_error=data.get("last_error"),
        )

    def touch(self) -> None:
        """Update updated_at to now."""
        self.updated_at = dt.datetime.utcnow()

    def mark_assigned(self, worker_id: str) -> None:
        self.status = JobStatus.ASSIGNED
        self.assigned_to = worker_id
        self.assigned_at = dt.datetime.utcnow()
        self.touch()

    def mark_processing(self) -> None:
        self.status = JobStatus.PROCESSING
        self.touch()

    def mark_success(self) -> None:
        self.status = JobStatus.SUCCESS
        self.touch()

    def mark_failure(self, error: str) -> None:
        self.status = JobStatus.FAILURE
        self.last_error = error
        self.touch()

    def mark_hold(self, reason: str) -> None:
        self.status = JobStatus.HOLD
        self.last_error = reason
        self.touch()
