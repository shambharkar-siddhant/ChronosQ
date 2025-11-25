"""
Configuration loading for ChronosQ.

Supports YAML or JSON config files describing:
- input directory
- jobs directory
- worker definitions
- broker settings (timeouts, attempts, etc.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import json

from .models import Worker


try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None


@dataclass
class BrokerSettings:
    """
    Tunable knobs for scheduling, retries, and recovery.
    """
    recovery_timeout_seconds: int = 300  # 5 minutes
    max_attempts: int = 3


@dataclass
class ChronosConfig:
    """
    Top-level ChronosQ configuration object.

    Parses and normalizes all paths and worker definitions.
    """

    input_dir: Path
    jobs_dir: Path
    broker_root: Path
    workers: List[Worker] = field(default_factory=list)
    settings: BrokerSettings = field(default_factory=BrokerSettings)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], base_path: Optional[Path] = None) -> "ChronosConfig":
        base = base_path or Path.cwd()

        def norm(path_value: Union[str, Path]) -> Path:
            p = Path(path_value)
            if not p.is_absolute():
                p = base / p
            return p

        input_dir = norm(data.get("input_dir", "./input"))
        jobs_dir = norm(data.get("jobs_dir", "./jobs"))
        broker_root = norm(data.get("broker_root", "./broker_runtime"))

        settings_raw = data.get("settings", {})
        settings = BrokerSettings(
            recovery_timeout_seconds=int(settings_raw.get("recovery_timeout_seconds", 300)),
            max_attempts=int(settings_raw.get("max_attempts", 3)),
        )

        workers_raw = data.get("workers", [])
        workers: List[Worker] = []
        for w in workers_raw:
            worker_id = w.get("id") or w.get("worker_id")
            if not worker_id:
                raise ValueError("Worker entry missing 'id'/'worker_id'")
            supported_types = w.get("types") or w.get("supported_types") or []
            max_concurrent = int(w.get("max_concurrent", 1))
            workers.append(
                Worker(
                    worker_id=str(worker_id),
                    supported_types=[str(t) for t in supported_types],
                    max_concurrent=max_concurrent,
                )
            )

        return cls(
            input_dir=input_dir,
            jobs_dir=jobs_dir,
            broker_root=broker_root,
            workers=workers,
            settings=settings,
        )


def load_config(path: Union[str, Path]) -> ChronosConfig:
    """
    Load a ChronosQ config file (YAML or JSON) and return a ChronosConfig object.

    Raises:
        FileNotFoundError if the file does not exist
        ValueError if parsing fails or config is invalid
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")

    # Try YAML first if available.
    data: Dict[str, Any]
    if yaml is not None and config_path.suffix.lower() in {".yml", ".yaml"}:
        try:
            parsed = yaml.safe_load(raw_text)
            if not isinstance(parsed, dict):
                raise ValueError("Config root must be a mapping")
            data = parsed
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Failed to parse YAML config: {e}") from e
    else:
        # Fallback to JSON
        try:
            parsed = json.loads(raw_text)
            if not isinstance(parsed, dict):
                raise ValueError("Config root must be a JSON object")
            data = parsed
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Could not parse config as JSON and YAML not available "
                f"or not selected by extension: {e}"
            ) from e

    return ChronosConfig.from_dict(data, base_path=config_path.parent)
