"""
Core broker loop for ChronosQ.

This module implements the main scheduling logic:

- Scan pending jobs
- Decide which worker should handle each job
- Move job files into per-worker queues
- Maintain JobState metadata
- Emit structured logs

The broker is intentionally stateless between iterations except for
a simple in-memory round-robin index per job type.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time

from .config import ChronosConfig
from .fs_utils import (
    ensure_dir,
    ensure_runtime_dirs,
    jobs_in_dir,
    move_file,
    job_state_path,
    worker_job_dir,
    read_json,
    atomic_write_json,
)
from .logging_utils import log_info, log_warning, log_error
from .models import Job, JobState, JobStatus, Worker


# ---------------------------
# Worker selection / round-robin
# ---------------------------


def _eligible_workers_for_job(job_type: str, workers: List[Worker]) -> List[Worker]:
    """
    Return workers that can handle a given job_type.
    """
    return [w for w in workers if w.can_handle(job_type)]


def _select_worker_round_robin(
    job_type: str,
    workers: List[Worker],
    rr_state: Dict[str, int],
) -> Optional[Worker]:
    """
    Select a worker for the given job_type using a simple round-robin policy.

    rr_state maintains an index per job_type so distribution is fair across
    multiple calls.
    """
    eligible = _eligible_workers_for_job(job_type, workers)
    if not eligible:
        return None

    idx = rr_state.get(job_type, 0)
    worker = eligible[idx % len(eligible)]
    rr_state[job_type] = (idx + 1) % len(eligible)
    return worker


# ---------------------------
# Job loading / state handling
# ---------------------------


def _load_job(job_path: Path) -> Job:
    """
    Load a Job object from a JSON job file.
    """
    data = read_json(job_path)
    return Job.from_dict(data)


def _load_or_init_state(
    config: ChronosConfig,
    job: Job,
    job_path: Path,
) -> JobState:
    """
    Load an existing JobState from disk if present, otherwise create a new one.

    This allows the broker to tolerate re-runs or reassignments gracefully.
    """
    state_path = job_state_path(config.broker_root, job.job_id)
    if state_path.exists():
        data = read_json(state_path)
        state = JobState.from_dict(data)
        state.job_path = job_path
        state.state_path = state_path
        return state

    # New job
    state = JobState(
        job_id=job.job_id,
        status=JobStatus.PENDING,
        attempts=0,
        job_path=job_path,
        state_path=state_path,
    )
    return state


def _save_state(state: JobState) -> None:
    """
    Persist JobState to its associated state_path as JSON.
    """
    if state.state_path is None:
        raise ValueError("JobState.state_path is not set")
    atomic_write_json(state.state_path, state.to_dict())


# ---------------------------
# Core assignment logic
# ---------------------------


def assign_pending_jobs(
    config: ChronosConfig,
    rr_state: Optional[Dict[str, int]] = None,
) -> int:
    """
    Scan the pending jobs directory and assign jobs to workers.

    Returns:
        Number of jobs assigned in this run.
    """
    if rr_state is None:
        rr_state = {}

    ensure_dir(config.jobs_dir)
    ensure_runtime_dirs(config.broker_root)

    pending_jobs = jobs_in_dir(config.jobs_dir)
    if not pending_jobs:
        return 0

    assigned_count = 0

    for job_path in pending_jobs:
        try:
            job = _load_job(job_path)
        except Exception as e:  # noqa: BLE001
            log_error(
                "job_load_failed",
                job_file=str(job_path),
                error=str(e),
            )
            # Move malformed jobs to error directory in future extension
            continue

        worker = _select_worker_round_robin(job.job_type, config.workers, rr_state)
        if worker is None:
            log_warning(
                "no_eligible_worker",
                job_id=job.job_id,
                job_type=job.job_type,
                job_file=str(job_path),
            )
            # Leave job in pending dir; may be picked up later if config changes
            continue

        # Prepare worker directory and destination path
        worker_dir = worker_job_dir(config.broker_root, job.job_type, worker.worker_id)
        ensure_dir(worker_dir)
        dest_path = worker_dir / job_path.name

        # Move the job file into the worker queue
        try:
            move_file(job_path, dest_path)
        except Exception as e:
            log_error(
                "job_move_failed",
                job_id=job.job_id,
                job_type=job.job_type,
                worker_id=worker.worker_id,
                src=str(job_path),
                dst=str(dest_path),
                error=str(e),
            )
            continue

        # Load or create state, mark as assigned, persist
        state = _load_or_init_state(config, job, dest_path)
        state.attempts = max(state.attempts, 0)  # defensive
        state.mark_assigned(worker.worker_id)
        _save_state(state)

        assigned_count += 1

        log_info(
            "job_assigned",
            job_id=job.job_id,
            job_type=job.job_type,
            worker_id=worker.worker_id,
            job_file=str(dest_path),
            attempts=state.attempts,
        )

    return assigned_count


# ---------------------------
# Broker run loop
# ---------------------------


def run_broker(
    config: ChronosConfig,
    poll_interval_seconds: float = 1.0,
) -> None:
    """
    Run the ChronosQ broker loop.

    This function runs indefinitely until interrupted (e.g., Ctrl+C),
    periodically scanning for pending jobs and assigning them.

    Args:
        config: Loaded ChronosConfig instance.
        poll_interval_seconds: Sleep duration when no jobs are found.
    """
    rr_state: Dict[str, int] = {}

    ensure_dir(config.input_dir)
    ensure_dir(config.jobs_dir)
    ensure_runtime_dirs(config.broker_root)

    log_info(
        "broker_started",
        input_dir=str(config.input_dir),
        jobs_dir=str(config.jobs_dir),
        broker_root=str(config.broker_root),
        worker_count=len(config.workers),
        poll_interval_seconds=poll_interval_seconds,
    )

    try:
        while True:
            assigned = assign_pending_jobs(config, rr_state=rr_state)
            if assigned == 0:
                # No work right now; avoid busy-looping
                time.sleep(poll_interval_seconds)
    except KeyboardInterrupt:
        log_info("broker_stopped", reason="keyboard_interrupt")
    except Exception as e:  # noqa: BLE001
        log_error("broker_crashed", error=str(e))
