#!/usr/bin/env python3
"""
Dummy worker for ChronosQ.

This script simulates a worker process that:

- Watches assigned directories under broker_root/types/<job_type>/<worker_id>/
- Picks up pending job JSON files
- Marks them as `.processing`
- Simulates work by sleeping
- Randomly marks jobs as success or failure
- Updates JobState and moves job files to output/ or error/

Usage:

    python examples/dummy_worker.py --config sample_config.yaml --worker-id user_1

You can also tweak success rate and processing time:

    python examples/dummy_worker.py \
        --config sample_config.yaml \
        --worker-id user_1 \
        --success-rate 0.9 \
        --min-sleep 0.5 \
        --max-sleep 2.0
"""

from __future__ import annotations

import argparse
import random
import time
from pathlib import Path
from typing import Optional, Tuple

from broker.config import load_config, ChronosConfig
from broker.fs_utils import (
    jobs_in_dir,
    mark_suffix,
    move_file,
    job_state_path,
    job_output_path,
    job_error_path,
    ensure_runtime_dirs,
    read_json,
    atomic_write_json,
)
from broker.logging_utils import configure_logging, log_info, log_warning, log_error
from broker.models import Job, JobState, JobStatus


PROCESSING_SUFFIX = ".processing"


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="chronosq-dummy-worker",
        description="Dummy worker for ChronosQ (simulates job processing).",
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        type=str,
        help="Path to ChronosQ YAML/JSON config file.",
    )
    parser.add_argument(
        "--worker-id",
        required=True,
        type=str,
        help="Worker ID to simulate (must match an entry in config.workers).",
    )
    parser.add_argument(
        "--success-rate",
        type=float,
        default=0.8,
        help="Probability (0.0–1.0) that a job is processed successfully. Default: 0.8",
    )
    parser.add_argument(
        "--min-sleep",
        type=float,
        default=0.5,
        help="Minimum simulated processing time in seconds. Default: 0.5",
    )
    parser.add_argument(
        "--max-sleep",
        type=float,
        default=2.0,
        help="Maximum simulated processing time in seconds. Default: 2.0",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Sleep interval when no jobs are found. Default: 1.0 seconds.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional path to a log file (JSON lines).",
    )
    return parser.parse_args(argv)


def _load_or_init_state(
    cfg: ChronosConfig,
    job: Job,
    job_path: Path,
) -> JobState:
    """
    Load an existing JobState for a job, or create one if missing.
    """
    state_path = job_state_path(cfg.broker_root, job.job_id)
    if state_path.exists():
        data = read_json(state_path)
        state = JobState.from_dict(data)
        state.job_path = job_path
        state.state_path = state_path
        return state

    state = JobState(
        job_id=job.job_id,
        status=JobStatus.PROCESSING,
        attempts=0,
        job_path=job_path,
        state_path=state_path,
    )
    return state


def _save_state(state: JobState) -> None:
    if state.state_path is None:
        raise ValueError("JobState.state_path is not set")
    atomic_write_json(state.state_path, state.to_dict())


def _find_next_job_for_worker(cfg: ChronosConfig, worker_id: str) -> Optional[Tuple[Path, str]]:
    """
    Scan all job types for this worker and return the first pending job found.

    Returns:
        (job_path, job_type) or None if no jobs are available.
    """
    types_root = cfg.broker_root / "types"
    if not types_root.exists():
        return None

    for job_type_dir in sorted(types_root.iterdir()):
        if not job_type_dir.is_dir():
            continue

        job_type = job_type_dir.name
        worker_dir = job_type_dir / worker_id
        if not worker_dir.exists():
            continue

        pending_jobs = jobs_in_dir(worker_dir)
        if not pending_jobs:
            continue

        # Pick the first job file
        return pending_jobs[0], job_type

    return None


def _process_single_job(
    cfg: ChronosConfig,
    job_path: Path,
    job_type: str,
    worker_id: str,
    success_rate: float,
    min_sleep: float,
    max_sleep: float,
) -> None:
    """
    Process a single job file:

    - Rename to .processing
    - Simulate some work via sleep
    - Randomly decide success/failure
    - Update JobState and move to output/ or error/
    """
    processing_path = mark_suffix(job_path, PROCESSING_SUFFIX)

    try:
        move_file(job_path, processing_path)
    except Exception as e:  # noqa: BLE001
        log_error(
            "worker_job_mark_processing_failed",
            job_file=str(job_path),
            dst=str(processing_path),
            error=str(e),
        )
        return

    try:
        data = read_json(processing_path)
        job = Job.from_dict(data)
    except Exception as e:  # noqa: BLE001
        log_error(
            "worker_job_load_failed",
            processing_file=str(processing_path),
            error=str(e),
        )
        # Move to error bucket with synthetic ID if needed
        err_path = job_error_path(cfg.broker_root, job_path.stem)
        try:
            move_file(processing_path, err_path)
        except Exception as e2:  # noqa: BLE001
            log_error(
                "worker_error_move_failed",
                src=str(processing_path),
                dst=str(err_path),
                error=str(e2),
            )
        return

    # Load or init state and mark as processing
    state = _load_or_init_state(cfg, job, processing_path)
    state.mark_processing()
    _save_state(state)

    log_info(
        "worker_job_started",
        worker_id=worker_id,
        job_id=job.job_id,
        job_type=job_type,
        processing_file=str(processing_path),
    )

    # Simulate processing time
    if max_sleep < min_sleep:
        max_sleep = min_sleep
    sleep_time = random.uniform(min_sleep, max_sleep)
    time.sleep(sleep_time)

    # Decide success or failure
    is_success = random.random() <= success_rate

    if is_success:
        state.mark_success()
        _save_state(state)

        out_path = job_output_path(cfg.broker_root, job.job_id)
        try:
            move_file(processing_path, out_path)
        except Exception as e:  # noqa: BLE001
            log_error(
                "worker_output_move_failed",
                worker_id=worker_id,
                job_id=job.job_id,
                src=str(processing_path),
                dst=str(out_path),
                error=str(e),
            )
            return

        log_info(
            "worker_job_success",
            worker_id=worker_id,
            job_id=job.job_id,
            job_type=job_type,
            processing_time_seconds=round(sleep_time, 3),
            output_file=str(out_path),
        )
    else:
        state.mark_failure("dummy_worker_random_failure")
        _save_state(state)

        err_path = job_error_path(cfg.broker_root, job.job_id)
        try:
            move_file(processing_path, err_path)
        except Exception as e:  # noqa: BLE001
            log_error(
                "worker_error_move_failed",
                worker_id=worker_id,
                job_id=job.job_id,
                src=str(processing_path),
                dst=str(err_path),
                error=str(e),
            )
            return

        log_warning(
            "worker_job_failed",
            worker_id=worker_id,
            job_id=job.job_id,
            job_type=job_type,
            processing_time_seconds=round(sleep_time, 3),
            error=state.last_error,
            error_file=str(err_path),
        )


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    configure_logging(log_file=args.log_file, name="chronosq-dummy-worker")

    cfg = load_config(args.config)
    ensure_runtime_dirs(cfg.broker_root)

    log_info(
        "worker_started",
        worker_id=args.worker_id,
        broker_root=str(cfg.broker_root),
        success_rate=args.success_rate,
        min_sleep=args.min_sleep,
        max_sleep=args.max_sleep,
        poll_interval_seconds=args.poll_interval,
    )

    try:
        while True:
            job_info = _find_next_job_for_worker(cfg, args.worker_id)
            if job_info is None:
                time.sleep(args.poll_interval)
                continue

            job_path, job_type = job_info
            _process_single_job(
                cfg=cfg,
                job_path=job_path,
                job_type=job_type,
                worker_id=args.worker_id,
                success_rate=args.success_rate,
                min_sleep=args.min_sleep,
                max_sleep=args.max_sleep,
            )
    except KeyboardInterrupt:
        log_info("worker_stopped", worker_id=args.worker_id, reason="keyboard_interrupt")
    except Exception as e:  # noqa: BLE001
        log_error("worker_crashed", worker_id=args.worker_id, error=str(e))


if __name__ == "__main__":  # pragma: no cover
    main()
