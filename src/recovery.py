"""
Recovery logic for ChronosQ.

This module scans for "stuck" processing jobs (job files with a `.processing`
suffix that have not been updated in a while) and either:

- requeues them back into the pending jobs directory, or
- marks them as permanently failed after exceeding max attempts.

This gives ChronosQ at-least-once delivery semantics and resilience
against worker crashes or machine restarts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from .config import ChronosConfig
from .fs_utils import (
    list_all_processing_files,
    strip_suffix,
    file_age_seconds,
    move_file,
    job_state_path,
    job_error_path,
    ensure_runtime_dirs,
    read_json,
    atomic_write_json,
)
from .logging_utils import log_info, log_warning, log_error
from .models import Job, JobState, JobStatus


PROCESSING_SUFFIX = ".processing"


def _load_state_for_job(
    config: ChronosConfig,
    job: Job,
    job_path: Path,
) -> JobState:
    """
    Load JobState for a given job_id, creating a default one if missing.
    """
    state_path = job_state_path(config.broker_root, job.job_id)
    if state_path.exists():
        data = read_json(state_path)
        state = JobState.from_dict(data)
        state.job_path = job_path
        state.state_path = state_path
        return state

    # brand new metadata for an already existing job
    state = JobState(
        job_id=job.job_id,
        status=JobStatus.PROCESSING,  # was processing when we found it
        attempts=0,
        job_path=job_path,
        state_path=state_path,
    )
    return state


def _save_state(state: JobState) -> None:
    if state.state_path is None:
        raise ValueError("JobState.state_path is not set")
    atomic_write_json(state.state_path, state.to_dict())


def recover_stuck_jobs(config: ChronosConfig) -> Dict[str, int]:
    """
    Scan for stuck processing jobs and try to recover them.

    Returns:
        A dict with basic counters, e.g.:
        {
            "requeued": 3,
            "failed": 1,
            "skipped_recent": 5
        }
    """
    ensure_runtime_dirs(config.broker_root)

    types_root = config.broker_root / "types"
    processing_files = list_all_processing_files(types_root, processing_suffix=PROCESSING_SUFFIX)
    if not processing_files:
        log_info("recovery_no_processing_files", root=str(types_root))
        return {"requeued": 0, "failed": 0, "skipped_recent": 0}

    timeout = config.settings.recovery_timeout_seconds
    max_attempts = config.settings.max_attempts

    counters = {"requeued": 0, "failed": 0, "skipped_recent": 0}

    for proc_path in processing_files:
        age = file_age_seconds(proc_path)
        if age < timeout:
            counters["skipped_recent"] += 1
            continue

        # Original job path is the same name without the .processing suffix
        job_path = strip_suffix(proc_path, PROCESSING_SUFFIX)

        try:
            # Even if the worker renamed the file, the JSON payload is still inside
            data = read_json(proc_path)
            job = Job.from_dict(data)
        except Exception as e:
            log_error(
                "recovery_job_load_failed",
                processing_file=str(proc_path),
                error=str(e),
            )
            # As a fallback, move the broken file to error area with a synthetic id
            synthetic_id = job_path.stem or "unknown"
            err_path = job_error_path(config.broker_root, synthetic_id)
            try:
                move_file(proc_path, err_path)
            except Exception as e2:  # noqa: BLE001
                log_error(
                    "recovery_error_move_failed",
                    src=str(proc_path),
                    dst=str(err_path),
                    error=str(e2),
                )
            counters["failed"] += 1
            continue

        # Load or create JobState
        state = _load_state_for_job(config, job, proc_path)
        state.attempts += 1

        if state.attempts >= max_attempts:
            # Mark as permanently failed
            state.mark_failure("max_attempts_exceeded_recovery")
            _save_state(state)

            err_path = job_error_path(config.broker_root, job.job_id)
            try:
                move_file(proc_path, err_path)
            except Exception as e:
                log_error(
                    "recovery_error_move_failed",
                    job_id=job.job_id,
                    src=str(proc_path),
                    dst=str(err_path),
                    error=str(e),
                )

            log_warning(
                "job_marked_failed_by_recovery",
                job_id=job.job_id,
                job_type=job.job_type,
                attempts=state.attempts,
                processing_age_seconds=age,
                error=state.last_error,
            )
            counters["failed"] += 1
            continue

        # Otherwise, requeue the job: rename back and move to pending jobs dir
        pending_path = config.jobs_dir / job_path.name

        # Update state to pending
        state.status = JobStatus.PENDING
        state.assigned_to = None
        state.assigned_at = None
        state.last_error = None
        _save_state(state)

        try:
            move_file(proc_path, pending_path)
        except Exception as e:
            log_error(
                "recovery_requeue_move_failed",
                job_id=job.job_id,
                src=str(proc_path),
                dst=str(pending_path),
                error=str(e),
            )
            counters["failed"] += 1
            continue

        log_warning(
            "job_requeued_from_processing",
            job_id=job.job_id,
            job_type=job.job_type,
            attempts=state.attempts,
            processing_age_seconds=age,
            pending_path=str(pending_path),
        )
        counters["requeued"] += 1

    log_info(
        "recovery_completed",
        requeued=counters["requeued"],
        failed=counters["failed"],
        skipped_recent=counters["skipped_recent"],
    )
    return counters
