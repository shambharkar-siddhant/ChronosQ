"""
Structured JSON logging utilities for ChronosQ.

We keep this intentionally simple:
- JSON lines (one event per line)
- UTC timestamps in ISO 8601
- Writes to stdout, and optionally to a log file
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt
import json
import sys
import threading


# Single global logger instance (simple enough for this project)
_lock = threading.Lock()
_log_file_path: Optional[Path] = None
_logger_name: str = "chronosq"


def configure_logging(log_file: Optional[str | Path] = None, name: str = "chronosq") -> None:
    """
    Configure logging for ChronosQ.

    Args:
        log_file: Optional path to a log file. If provided, all events
                  will be appended there in addition to stdout.
        name: Logical logger name included in each event.
    """
    global _log_file_path, _logger_name
    _logger_name = name
    if log_file is None:
        _log_file_path = None
    else:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        _log_file_path = p


def _emit_json(payload: Dict[str, Any]) -> None:
    """
    Emit a JSON log line to stdout and optional log file.

    Uses a lock to avoid interleaving log lines when used from
    multiple threads.
    """
    line = json.dumps(payload, default=str, sort_keys=True)
    with _lock:
        # stdout
        sys.stdout.write(line + "\n")
        sys.stdout.flush()

        # optional log file
        if _log_file_path is not None:
            try:
                with _log_file_path.open("a", encoding="utf-8") as fp:
                    fp.write(line + "\n")
            except OSError:
                # Logging should never crash the broker; swallow errors.
                pass


def log_event(
    event: str,
    level: str = "info",
    **fields: Any,
) -> None:
    """
    Log a structured event.

    Args:
        event: Short event name, e.g. "job_assigned", "job_requeued".
        level: Log level as string: "debug", "info", "warning", "error".
        **fields: Arbitrary extra fields to include in the log JSON.
    """
    payload: Dict[str, Any] = {
        "ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "logger": _logger_name,
        "level": level.lower(),
        "event": event,
    }
    # Merge extra fields, but don't overwrite core keys
    for k, v in fields.items():
        if k in payload:
            k = f"extra_{k}"
        payload[k] = v

    _emit_json(payload)


# Convenience wrappers


def log_debug(event: str, **fields: Any) -> None:
    log_event(event, level="debug", **fields)


def log_info(event: str, **fields: Any) -> None:
    log_event(event, level="info", **fields)


def log_warning(event: str, **fields: Any) -> None:
    log_event(event, level="warning", **fields)


def log_error(event: str, **fields: Any) -> None:
    log_event(event, level="error", **fields)
