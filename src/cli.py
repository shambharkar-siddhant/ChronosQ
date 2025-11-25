"""
Command-line interface for ChronosQ.

Exposes a small CLI with subcommands:

- run      : run the broker loop (assign pending jobs to workers)
- recover  : run a one-off recovery pass for stuck processing jobs

Usage examples:

    python -m broker.cli run --config sample_config.yaml
    python -m broker.cli recover --config sample_config.yaml
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from .config import load_config
from .core import run_broker
from .recovery import recover_stuck_jobs
from .logging_utils import configure_logging, log_info


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="chronosq",
        description="ChronosQ – a filesystem-based job broker.",
    )

    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional path to a log file (JSON lines).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # run
    run_parser = subparsers.add_parser(
        "run",
        help="Run the ChronosQ broker loop (assign pending jobs to workers).",
    )
    run_parser.add_argument(
        "--config",
        "-c",
        required=True,
        type=str,
        help="Path to ChronosQ YAML/JSON config file.",
    )
    run_parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds when no jobs are available (default: 1.0).",
    )

    # recover
    rec_parser = subparsers.add_parser(
        "recover",
        help="Run a one-off recovery pass for stuck processing jobs.",
    )
    rec_parser.add_argument(
        "--config",
        "-c",
        required=True,
        type=str,
        help="Path to ChronosQ YAML/JSON config file.",
    )

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    # configure logging first
    configure_logging(log_file=args.log_file, name="chronosq")

    if args.command == "run":
        config_path = Path(args.config)
        cfg = load_config(config_path)
        log_info(
            "cli_run_broker",
            config=str(config_path),
            poll_interval_seconds=args.poll_interval,
        )
        run_broker(cfg, poll_interval_seconds=args.poll_interval)

    elif args.command == "recover":
        config_path = Path(args.config)
        cfg = load_config(config_path)
        log_info(
            "cli_run_recovery",
            config=str(config_path),
        )
        recover_stuck_jobs(cfg)

    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
