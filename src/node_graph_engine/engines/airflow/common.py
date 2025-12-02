"""Shared helpers for the Airflow engine."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

IncomingSpec = Dict[str, str]

_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_dag_id(value: str) -> str:
    sanitized = _SANITIZE_PATTERN.sub("_", value)
    if not sanitized:
        sanitized = "node_graph"
    if sanitized[0].isdigit():
        sanitized = f"ng_{sanitized}"
    return sanitized


@dataclass
class SchedulerPaths:
    """Resolved file-system locations used for scheduler-backed runs."""

    airflow_home: Path
    dags_dir: Path
    run_root: Path


@dataclass
class SchedulerRunArtifacts:
    """Metadata for a single scheduler-triggered run."""

    run_id: str
    run_conf: Dict[str, Any]
    result_path: Path
