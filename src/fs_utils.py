"""
Filesystem utilities for ChronosQ.

These helpers encapsulate:
- directory creation
- atomic writes
- JSON loading/saving
- job/state file path helpers

They are intentionally small and composable so the core broker
and recovery logic can focus on orchestration instead of path
and IO edge cases.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import os
import tempfile
import time


# ---------------------------
# Generic FS helpers
# ---------------------------


def ensure_dir(path: Path) -> None:
    """
    Ensure that a directory exists (mkdir -p behavior).
    """
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    """
    Atomically write text to a file.

    Writes to a temporary file in the same directory and then renames it
    into place to reduce chances of partial writes.
    """
    ensure_dir(path.parent)
    tmp_fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".swap")
    try:
        with os.fdopen(tmp_fd, "w", encoding=encoding) as tmp_fp:
            tmp_fp.write(content)
        os.replace(tmp_name, path)
    finally:
        # clean up temp file
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass


def atomic_write_json(path: Path, data: Dict[str, Any], indent: Optional[int] = None) -> None:
    """
    Atomically write a JSON object to a file.
    """
    text = json.dumps(data, indent=indent, sort_keys=True, default=str)
    atomic_write_text(path, text)


def read_json(path: Path, encoding: str = "utf-8") -> Dict[str, Any]:
    """
    Read a JSON object from a file.

    Raises FileNotFoundError or json.JSONDecodeError on error.
    """
    with path.open("r", encoding=encoding) as fp:
        return json.load(fp)


def list_json_files(directory: Path) -> List[Path]:
    """
    List JSON files directly under a directory (non-recursive).

    Returns a sorted list of Paths.
    """
    if not directory.exists():
        return []
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() == ".json"
    )


def move_file(src: Path, dst: Path) -> None:
    """
    Move (rename) a file, creating destination directories if required.

    Overwrites any existing file at the destination path.
    """
    ensure_dir(dst.parent)
    os.replace(src, dst)


def file_age_seconds(path: Path) -> float:
    """
    Return the age of a file in seconds based on its last modification time.
    """
    try:
        stat = path.stat()
    except FileNotFoundError:
        return float("inf")
    return time.time() - stat.st_mtime


# ---------------------------
# specific helpers
# ---------------------------


def jobs_in_dir(jobs_dir: Path) -> List[Path]:
    """
    Convenience wrapper for listing pending job files.

    This can be extended later to filter by suffix/state if needed.
    """
    return list_json_files(jobs_dir)


def state_dir(broker_root: Path) -> Path:
    """
    Return the directory used to store job state metadata files.
    """
    return broker_root / "state"


def output_dir(broker_root: Path) -> Path:
    """
    Directory for successfully completed jobs (archival).
    """
    return broker_root / "output"


def error_dir(broker_root: Path) -> Path:
    """
    Directory for failed jobs (archival / inspection).
    """
    return broker_root / "error"


def worker_job_dir(broker_root: Path, job_type: str, worker_id: str) -> Path:
    """
    Return the directory where jobs assigned to a specific worker + type live.

    Example:
        broker_root/types/quality_check/user_1/
    """
    return broker_root / "types" / job_type / worker_id


def job_state_path(broker_root: Path, job_id: str) -> Path:
    """
    Return the path to a job's state metadata file.

    Example:
        broker_root/state/<job_id>.state.json
    """
    return state_dir(broker_root) / f"{job_id}.state.json"


def job_output_path(broker_root: Path, job_id: str) -> Path:
    """
    Path for archiving successfully completed jobs.
    """
    return output_dir(broker_root) / f"{job_id}.json"


def job_error_path(broker_root: Path, job_id: str) -> Path:
    """
    Path for archiving failed jobs.
    """
    return error_dir(broker_root) / f"{job_id}.json"


def mark_suffix(path: Path, suffix: str) -> Path:
    """
    Return a new Path with an additional suffix appended.

    Example:
        job.json + '.processing' -> job.json.processing
    """
    return path.with_name(path.name + suffix)


def strip_suffix(path: Path, suffix: str) -> Path:
    """
    Strip a known suffix from the end of the filename, if present.

    Example:
        job.json.processing - '.processing' -> job.json
    """
    name = path.name
    if name.endswith(suffix):
        return path.with_name(name[: -len(suffix)])
    return path


def is_processing_file(path: Path, processing_suffix: str = ".processing") -> bool:
    """
    Check if a path refers to a 'processing' marker file.
    """
    return path.name.endswith(processing_suffix)


def list_processing_files(directory: Path, processing_suffix: str = ".processing") -> List[Path]:
    """
    List files in a directory that represent 'processing' jobs.

    Useful for recovery logic.
    """
    if not directory.exists():
        return []
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.name.endswith(processing_suffix)
    )


def list_all_processing_files(root: Path, processing_suffix: str = ".processing") -> List[Path]:
    """
    Recursively find all '.processing' job files under a root directory.

    For example, to scan all worker queues:
        list_all_processing_files(broker_root / "types")
    """
    if not root.exists():
        return []
    matches: List[Path] = []
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.endswith(processing_suffix):
                matches.append(Path(dirpath) / name)
    return sorted(matches)


def ensure_runtime_dirs(broker_root: Path) -> None:
    """
    Ensure that the basic runtime directories for ChronosQ exist.

    This does NOT create per-worker directories (those are created lazily
    when assignments are made).
    """
    ensure_dir(state_dir(broker_root))
    ensure_dir(output_dir(broker_root))
    ensure_dir(error_dir(broker_root))
    # types/ dir will be created lazily when we know job_type + worker_id
    ensure_dir(broker_root / "types")
