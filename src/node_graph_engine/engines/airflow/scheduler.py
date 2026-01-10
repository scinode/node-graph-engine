"""Scheduler-facing helpers for building and triggering DAGs."""

from __future__ import annotations

import inspect
import json
import logging
import os
import textwrap
import time
from typing import Any, Callable, Dict, Optional, Tuple

from airflow import DAG
from airflow.utils import timezone
from node_graph import Graph

from .common import SchedulerPaths, SchedulerRunArtifacts


def _resolve_scheduler_paths(dag_id: str) -> SchedulerPaths:
    """Determine key Airflow directories for a generated sub-graph DAG."""

    from pathlib import Path

    airflow_home = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow"))

    dags_dir_setting = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if not dags_dir_setting:
        try:
            from airflow.configuration import conf as airflow_conf

            dags_dir_setting = airflow_conf.get("core", "dags_folder")
        except Exception:
            dags_dir_setting = None

    if dags_dir_setting:
        dags_dir = Path(dags_dir_setting).expanduser()
    else:
        dags_dir = airflow_home / "dags"

    dags_dir.mkdir(parents=True, exist_ok=True)

    run_root = airflow_home / "ng_subgraph_runs"
    run_root.mkdir(parents=True, exist_ok=True)

    return SchedulerPaths(
        airflow_home=airflow_home, dags_dir=dags_dir, run_root=run_root
    )


def _build_scheduler_payload(
    *,
    dag_id: str,
    ng: Graph,
    engine_config: Dict[str, Any],
    default_user_email: Optional[str],
    schedule_subgraphs: bool,
    serialize_fn: Callable[[Any], Any],
) -> bytes:
    """Serialize the information required to rebuild a sub-graph DAG."""

    payload = {
        "dag_id": dag_id,
        "engine_config": engine_config,
        "graph": ng.to_dict(include_sockets=True, should_serialize=True),
        "default_user_email": default_user_email,
        "schedule_subgraphs": bool(schedule_subgraphs),
    }

    serialized = serialize_fn(payload)
    if not isinstance(serialized, (str, bytes)):
        serialized = json.dumps(serialized)

    if isinstance(serialized, str):
        return serialized.encode("utf-8")
    return serialized


_DAG_TEMPLATE = textwrap.dedent(
    '''
    """Auto-generated Graph Engine DAG for sub-graph execution."""

    from __future__ import annotations

    import base64

    from aiida import load_profile
    from aiida.orm.utils.serialize import deserialize_unsafe
    from node_graph import Graph

    load_profile()

    from node_graph_engine.engines.airflow import AirflowEngine

    _PAYLOAD_DATA = """
    __PAYLOAD_BLOB__
    """.encode("utf-8")

    payload = deserialize_unsafe(base64.b64decode(_PAYLOAD_DATA))
    ng = Graph.from_dict(payload["graph"])

    engine = AirflowEngine(
        dag_id=payload["dag_id"],
        default_args=payload["engine_config"].get("default_args"),
        schedule=payload["engine_config"].get("schedule"),
        start_date=payload["engine_config"].get("start_date"),
        catchup=payload["engine_config"].get("catchup", False),
        max_active_runs=payload["engine_config"].get("max_active_runs", 1),
        _default_user_email=payload["default_user_email"],
        schedule_subgraphs=payload.get("schedule_subgraphs", False),
    )

    dag = engine.build_dag(ng)
    '''
)


def _render_generated_dag(payload_blob: bytes) -> str:
    """Embed the serialized payload inside the generated DAG template."""

    import base64
    import textwrap

    encoded_payload = base64.b64encode(payload_blob).decode("ascii")
    wrapped_payload = textwrap.fill(encoded_payload, width=76)
    return _DAG_TEMPLATE.replace("__PAYLOAD_BLOB__", wrapped_payload)


def _write_dag_file(dag_path, dag_source: str) -> None:
    """Persist the generated DAG to disk using an atomic replace."""

    from pathlib import Path

    dag_path = Path(dag_path)
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dag_path.with_suffix(f"{dag_path.suffix}.tmp")
    tmp_path.write_text(dag_source)
    os.replace(tmp_path, dag_path)


def _register_generated_dag(dag_id: str, dag_path, dags_dir) -> Tuple[DAG, bool]:
    """Load the generated DAG into a DagBag and persist it to the metadata DB."""

    from pathlib import Path

    dag_path = Path(dag_path)
    dags_dir = Path(dags_dir)

    from airflow.models import DagBag

    persisted = False
    dag_bag = DagBag(dag_folder=str(dags_dir), include_examples=False, safe_mode=False)
    dag_bag.process_file(str(dag_path))
    dag_obj = dag_bag.dags.get(dag_id)

    if dag_obj is None:
        raise RuntimeError(
            f"Failed to register Airflow DAG '{dag_id}' for sub-graph execution"
        )

    bundle_name = os.environ.get("NG_AIRFLOW_BUNDLE_NAME") or "dags-folder"
    bundle_version = os.environ.get("NG_AIRFLOW_BUNDLE_VERSION")

    try:
        from airflow.models.dag import DagModel
        from airflow.serialization.serialized_objects import LazyDeserializedDAG
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        # Ensure DagModel row exists (required FK for dag_version writes).
        with create_session() as session:
            dag_model = DagModel(dag_id=dag_id, fileloc=dag_obj.fileloc)
            if hasattr(dag_obj, "relative_fileloc"):
                dag_model.relative_fileloc = dag_obj.relative_fileloc
            dag_model.is_paused = False
            dag_model.is_stale = False
            dag_model.last_parsed_time = timezone.utcnow()
            dag_model.last_parse_duration = 0.0
            if hasattr(dag_model, "bundle_name"):
                dag_model.bundle_name = bundle_name
            if bundle_version is not None and hasattr(dag_model, "bundle_version"):
                dag_model.bundle_version = bundle_version
            session.merge(dag_model)

        lazy_dag = LazyDeserializedDAG.from_dag(dag_obj)
        SerializedDagModel.write_dag(
            lazy_dag,
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            min_update_interval=0,
        )
        persisted = True
    except Exception as exc:
        # Airflow 3 disallows ORM access from execution-time contexts; skip persistence
        # so we can still trigger runs as long as the scheduler discovers the DAG file.
        message = str(exc)
        if "Direct database access via the ORM is not allowed" in message:
            logging.getLogger(__name__).warning(
                "Skipping DAG persistence for '%s' due to Airflow execution-time DB "
                "restrictions; relying on scheduler DAG discovery. Error: %s",
                dag_id,
                exc,
            )
        else:
            logging.getLogger(__name__).info(
                "Failed to persist serialized DAG '%s': %s", dag_id, exc
            )
            raise

    return dag_obj, persisted


def _prepare_run_artifacts(
    *, paths: SchedulerPaths, dag_id: str, run_id: str, parent_pid: Optional[str]
) -> SchedulerRunArtifacts:
    """Set up filesystem targets and configuration for a scheduler run."""

    run_dir = paths.run_root / dag_id / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    result_path = run_dir / "result.json"

    run_conf: Dict[str, Any] = {}
    if parent_pid is not None:
        run_conf["ng_parent_pid"] = parent_pid

    run_conf["ng_result_path"] = str(result_path)
    run_conf.setdefault("ng_subgraph_run_id", run_id)

    return SchedulerRunArtifacts(
        run_id=run_id, run_conf=run_conf, result_path=result_path
    )


def _trigger_registered_dag(
    *,
    dag_id: str,
    dag_path,
    run_id: str,
    run_conf: Dict[str, Any],
    dag: Optional[DAG] = None,
    dag_persisted: bool = True,
) -> Tuple[bool, Optional[BaseException]]:
    """Trigger a DAG run after the DAG has been registered/serialized."""

    logger = logging.getLogger(__name__)

    try:
        from airflow.api.common.trigger_dag import trigger_dag
        from airflow.models import DagBag
    except Exception as exc:
        logger.debug(
            "Airflow force-trigger prerequisites unavailable for '%s': %s",
            dag_id,
            exc,
            exc_info=True,
        )
        return False, exc

    if dag is None:
        try:
            dagbag = DagBag(
                dag_folder=str(getattr(dag_path, "parent", dag_path)),
                include_examples=False,
                safe_mode=False,
            )
            dagbag.process_file(str(dag_path))
            dag = dagbag.dags.get(dag_id)
        except Exception as exc:
            logger.info(
                "DagBag load failed while force-registering '%s': %s", dag_id, exc
            )
            return False, exc

    if dag is None:
        error = RuntimeError(
            f"Unable to force-register Airflow DAG '{dag_id}' at '{dag_path}'"
        )
        logger.info(error)
        return False, error

    trigger_sig = inspect.signature(trigger_dag)
    trigger_kwargs: Dict[str, Any] = {
        "dag_id": dag_id,
        "run_id": run_id,
        "conf": run_conf,
        "logical_date": None,
        "replace_microseconds": False,
    }

    if "triggered_by" in trigger_sig.parameters:
        try:
            from airflow.utils.types import DagRunTriggeredByType

            trigger_kwargs["triggered_by"] = DagRunTriggeredByType.OPERATOR
        except Exception:
            trigger_kwargs.pop("triggered_by", None)
    else:
        trigger_kwargs.pop("logical_date", None)

    supported_kwargs = {
        name: value
        for name, value in trigger_kwargs.items()
        if name in trigger_sig.parameters
    }

    try:
        trigger_dag(**supported_kwargs)
    except Exception as exc:
        message = str(exc)
        if (
            exc.__class__.__name__ == "DagRunAlreadyExists"
            or "already exists" in message.lower()
        ):
            return (
                False,
                RuntimeError(
                    f"Airflow DAG '{dag_id}' already has a run with id '{run_id}'"
                ),
            )

        logger.info(
            "Force trigger attempt failed for DAG '%s': %s",
            dag_id,
            exc,
        )
        return False, exc

    if dag_persisted:
        logger.info(
            "Force-registered and triggered DAG '%s' run '%s' without waiting for scheduler discovery",
            dag_id,
            run_id,
        )
    else:
        logger.info(
            "Triggered DAG '%s' run '%s' but relying on scheduler discovery because DAG persistence was skipped",
            dag_id,
            run_id,
        )
    return True, None


def _poll_for_result(
    *,
    run_id: str,
    result_path,
    poll_interval: float,
    deserialize_fn: Callable[[bytes], Any],
) -> Dict[str, Any]:
    """Poll the scheduler result file until outputs are available or a failure occurs."""

    from pathlib import Path

    result_path = Path(result_path)
    deadline = time.monotonic() + max(poll_interval * 10, 3600)

    while time.monotonic() < deadline:
        if result_path.exists():
            serialized_result = result_path.read_text()
            try:
                payload = deserialize_fn(serialized_result.encode("utf-8"))
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to load scheduler result payload for run '{run_id}'"
                ) from exc

            return payload if isinstance(payload, dict) else {}

        time.sleep(min(1.0, poll_interval))

    raise RuntimeError(
        "Airflow scheduler run completed but no result payload was produced"
    )


def _poll_for_scheduled_result(
    *,
    dag_id: str,
    run_root,
    poll_interval: float,
    deserialize_fn: Callable[[bytes], Any],
    started_at_epoch: float,
) -> Dict[str, Any]:
    """Poll for any new result file written under the DAG's run root."""

    from pathlib import Path

    run_root = Path(run_root) / dag_id
    deadline = time.monotonic() + max(poll_interval * 10, 3600)

    while time.monotonic() < deadline:
        if run_root.exists():
            run_dirs = sorted(
                (p for p in run_root.iterdir() if p.is_dir()),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for run_dir in run_dirs:
                result_path = run_dir / "result.json"
                if not result_path.exists():
                    continue
                # Only consider results produced after we submitted the DAG.
                if result_path.stat().st_mtime < started_at_epoch:
                    continue
                serialized_result = result_path.read_text()
                try:
                    payload = deserialize_fn(serialized_result.encode("utf-8"))
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed to load scheduler result payload for DAG '{dag_id}'"
                    ) from exc
                return payload if isinstance(payload, dict) else {}

        time.sleep(min(1.0, poll_interval))

    raise RuntimeError(f"Airflow scheduler did not produce a result for DAG '{dag_id}'")
