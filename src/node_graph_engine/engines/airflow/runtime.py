"""Runtime helpers used by Airflow tasks."""

from __future__ import annotations

import json
import os
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

from aiida import orm
from node_graph import Graph
from node_graph.graph import BUILTIN_TASKS

from node_graph_engine.core.execution import (
    compute_graph_outputs,
    execute_task_job,
    mark_process_failure,
    mark_process_success,
    prepare_graph_run,
)

from node_graph_engine.core.utils import (
    close_threadlocal_aiida_session,
    ensure_aiida_profile,
    get_active_profile_name,
    get_default_user_email,
    get_nested_dict,
    load_default_user,
    load_nodegraph_data,
    update_nested_dict_with_special_keys,
)
from node_graph_engine.orm.data.knowledge_graph import persist_workflow_knowledge_graph
from node_graph_engine.core.task import TaskMeta

from .async_request import AsyncNodeExecutionRequest
from .common import IncomingSpec


def _build_runtime_kwargs(
    *,
    incoming: Iterable[IncomingSpec],
    source_map: Dict[str, Any],
    target_name: str,
) -> Dict[str, Any]:
    grouped: Dict[str, List[IncomingSpec]] = {}
    for spec in incoming:
        if spec["target"] != target_name:
            continue
        grouped.setdefault(spec["target_socket"], []).append(spec)

    kwargs: Dict[str, Any] = {}
    for to_socket, specs in grouped.items():
        active_links = [s for s in specs if s["from_socket"] != "_wait"]
        if not active_links:
            continue
        if len(active_links) == 1:
            spec = active_links[0]
            from_payload = source_map.get(spec["from"], {})
            from_socket = spec["from_socket"]
            if from_socket == "_outputs":
                kwargs[to_socket] = from_payload
            else:
                kwargs[to_socket] = get_nested_dict(
                    from_payload, from_socket, default=None
                )
            continue
        bundle: Dict[str, Any] = {}
        for spec in active_links:
            from_socket = spec["from_socket"]
            if from_socket in ("_wait", "_outputs"):
                continue
            from_payload = source_map.get(spec["from"], {})
            bundle_key = f"{spec['from']}_{from_socket}"
            bundle[bundle_key] = get_nested_dict(
                from_payload, from_socket, default=None
            )
        if bundle:
            kwargs[to_socket] = bundle

    return kwargs


def _ensure_meta(meta: Any) -> TaskMeta:
    if isinstance(meta, TaskMeta):
        return meta
    if isinstance(meta, dict):
        return TaskMeta(**meta)
    if hasattr(meta, "as_dict"):
        return TaskMeta(**meta.as_dict())
    raise TypeError(f"Unsupported metadata payload: {meta!r}")


def _execute_node(
    *,
    parent_pid: Optional[str],
    meta: Any,
    callable_payload: Optional[Dict[str, Any]],
    literals: Dict[str, Any],
    incoming: Iterable[IncomingSpec],
    source_map: Dict[str, Any],
    engine_name: str,
    node_inputs: Optional[Dict[str, Any]],
    node_outputs: Optional[Dict[str, Any]],
    default_user_email: str,
    sub_engine_config: Dict[str, Any],
    schedule_subgraphs: bool,
    task_context: Optional[Dict[str, Any]] = None,
    is_async_callable: bool = False,
    defer_callback: Optional[Callable[[AsyncNodeExecutionRequest], None]] = None,
    task_type: Optional[str] = None,
    node_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta_obj = _ensure_meta(meta)
    runtime_inputs = dict(literals)
    runtime_inputs.update(
        _build_runtime_kwargs(
            incoming=incoming,
            source_map=source_map,
            target_name=meta_obj.node_name,
        )
    )
    runtime_inputs = update_nested_dict_with_special_keys(runtime_inputs)

    user = load_default_user(default_user_email)
    task_type_value = (task_type or "").lower()
    metadata = dict(node_metadata or {})

    def _build_sub_engine(name: str) -> "AirflowEngine":
        from .engine import AirflowEngine
        from .common import _sanitize_dag_id

        sanitized = _sanitize_dag_id(name)
        return AirflowEngine(
            dag_id=sanitized,
            default_args=sub_engine_config.get("default_args"),
            schedule=sub_engine_config.get("schedule"),
            start_date=sub_engine_config.get("start_date"),
            catchup=sub_engine_config.get("catchup", False),
            max_active_runs=sub_engine_config.get("max_active_runs", 1),
            _default_user_email=default_user_email,
            schedule_subgraphs=sub_engine_config.get(
                "schedule_subgraphs", schedule_subgraphs
            ),
        )

    request = AsyncNodeExecutionRequest(
        parent_pid=parent_pid,
        meta=meta_obj.as_dict(),
        callable_payload=callable_payload,
        runtime_inputs=runtime_inputs,
        engine_name=engine_name,
        node_inputs=node_inputs,
        node_outputs=node_outputs,
        default_user_email=default_user_email,
        profile_name=get_active_profile_name(),
        sub_engine_config=sub_engine_config,
        schedule_subgraphs=schedule_subgraphs,
        task_type=task_type,
        node_metadata=metadata,
    )

    try:
        if is_async_callable and defer_callback is not None:
            defer_callback(request)
            # ``defer_callback`` is expected to raise ``TaskDeferred``
            return {}

        if is_async_callable or task_type_value == "remotefunction":
            return request.run()

        return execute_task_job(
            parent_pid=parent_pid,
            meta=meta_obj,
            callable_payload=callable_payload,
            runtime_inputs=runtime_inputs,
            engine_name=engine_name,
            node_inputs=node_inputs,
            node_outputs=node_outputs,
            build_sub_engine=_build_sub_engine,
            user=user,
            schedule_subgraphs=schedule_subgraphs,
            task_context=task_context,
        )
    finally:
        close_threadlocal_aiida_session()


def airflow_node_task(
    _ng_is_async: bool = False,
    _ng_defer: Optional[Callable[[AsyncNodeExecutionRequest], None]] = None,
    **context: Any,
) -> Dict[str, Any]:
    runtime_context_task_id = context.get("_ng_runtime_context_task_id")

    ti = context.get("ti")
    runtime_context: Optional[Dict[str, Any]] = None
    if runtime_context_task_id and ti is not None:
        runtime_context = ti.xcom_pull(task_ids=runtime_context_task_id)

    parent_pid = context.get("_ng_parent_pid")
    if parent_pid is None and runtime_context:
        parent_pid = runtime_context.get("graph_pid")
    if parent_pid is None:
        dag_run = context.get("dag_run")
        if dag_run is not None:
            parent_pid = dag_run.conf.get("ng_parent_pid")

    profile_name = context.get("_ng_profile_name")
    ensure_aiida_profile(profile_name)

    default_user_email = context.get("_ng_default_user_email")
    if default_user_email is None:
        default_user_email = get_default_user_email()

    base_values: Dict[str, Any]
    if runtime_context and "values" in runtime_context:
        base_values = dict(runtime_context.get("values", {}))
    else:
        base_values = dict(context.get("_ng_base_values", {}))

    source_map = dict(base_values)
    incoming_specs: Iterable[IncomingSpec] = context.get("_ng_incoming", [])
    upstream_ids = {spec["from"] for spec in incoming_specs}
    if ti is not None:
        for task_id in upstream_ids:
            if task_id in BUILTIN_TASKS:
                continue
            pulled = ti.xcom_pull(task_ids=task_id)
            if pulled is not None:
                source_map[task_id] = pulled

    graph_pid = runtime_context.get("graph_pid") if runtime_context else None

    try:
        result = _execute_node(
            parent_pid=parent_pid,
            meta=context["_ng_meta"],
            callable_payload=context.get("_ng_callable"),
            literals=context.get("_ng_literals", {}),
            incoming=incoming_specs,
            source_map=source_map,
            engine_name=context.get("_ng_engine_name", "airflow"),
            node_inputs=context.get("_ng_task_inputs"),
            node_outputs=context.get("_ng_task_outputs"),
            default_user_email=default_user_email,
            sub_engine_config=context.get("_ng_engine_config", {}),
            schedule_subgraphs=context.get("_ng_schedule_subgraphs", False),
            task_context=context,
            is_async_callable=_ng_is_async,
            defer_callback=_ng_defer,
            task_type=context.get("_ng_task_type"),
            node_metadata=context.get("_ng_task_metadata"),
        )
    except Exception as exc:
        if graph_pid:
            process_node = orm.load_node(graph_pid)
            mark_process_failure(process_node, exc)
            process_node.seal()
        raise

    if ti is not None:
        ti.xcom_push(key="return_value", value=result)
    return result


def airflow_init_task(**context: Any) -> Dict[str, Any]:
    ng: Graph = context["_ng_graph"]
    profile_name = context.get("_ng_profile_name")
    ensure_aiida_profile(profile_name)
    default_user_email = context.get("_ng_default_user_email")
    if default_user_email is None:
        default_user_email = get_default_user_email()
    user = load_default_user(default_user_email)

    parent_pid = context.get("_ng_parent_pid")
    dag_run = context.get("dag_run")
    if parent_pid is None and dag_run is not None:
        parent_pid = dag_run.conf.get("ng_parent_pid")

    graph_context = prepare_graph_run(
        ng,
        parent_pid=parent_pid,
        user=user,
        encode_graph_inputs=True,
    )

    builtins = dict(context.get("_ng_builtins", {}))
    for key, value in builtins.items():
        graph_context.values.setdefault(key, value)

    semantics_data = (
        graph_context.semantics.to_dict()
        if graph_context.semantics is not None
        else None
    )
    return {
        "graph_pid": graph_context.process_node.uuid,
        "values": dict(graph_context.values),
        "semantics": semantics_data,
        "graph_uuid": ng.uuid,
    }


def airflow_finalize_task(**context: Any) -> Dict[str, Any]:
    runtime_context_task_id = context["_ng_context_task_id"]
    profile_name = context.get("_ng_profile_name")
    ensure_aiida_profile(profile_name)
    node_task_ids: Iterable[str] = context.get("_ng_task_task_ids", [])
    incoming_specs: Dict[str, List[IncomingSpec]] = context.get("_ng_incoming", {})

    ti = context.get("ti")
    if ti is None:
        raise RuntimeError("Task instance context is required for finalize task")

    dag_run = context.get("dag_run")
    result_path_str: Optional[str] = None
    if dag_run is not None and getattr(dag_run, "conf", None):
        result_path_str = dag_run.conf.get("ng_result_path")
    if result_path_str is None and dag_run is not None:
        try:
            from pathlib import Path

            airflow_home = Path(
                os.environ.get("AIRFLOW_HOME", Path.home() / "airflow")
            )
            run_root = airflow_home / "ng_subgraph_runs"
            fallback_path = run_root / dag_run.dag_id / dag_run.run_id / "result.json"
            result_path_str = str(fallback_path)
            fallback_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            result_path_str = None

    runtime_context = ti.xcom_pull(task_ids=runtime_context_task_id)
    if not runtime_context:
        raise RuntimeError("Missing Graph runtime context; ensure init task succeeded")

    graph_pid = runtime_context.get("graph_pid")
    if not graph_pid:
        raise RuntimeError("Graph runtime context did not provide a graph PID")

    process_node = orm.load_node(graph_pid)

    graph_proxy = None
    try:
        ngdata = load_nodegraph_data(process_node)
        if ngdata:
            graph_proxy = Graph.from_dict(ngdata)
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to rebuild graph %s for knowledge graph persistence", graph_pid
        )

    success = False
    if process_node.is_excepted:
        if not process_node.is_sealed:
            process_node.seal()
        raise RuntimeError("Graph execution failed; see upstream task logs for details")

    values: Dict[str, Any] = dict(runtime_context.get("values", {}))
    for task_id in node_task_ids:
        if task_id in BUILTIN_TASKS:
            continue
        pulled = ti.xcom_pull(task_ids=task_id)
        if pulled is not None:
            values[task_id] = pulled

    try:
        graph_outputs = compute_graph_outputs(
            incoming=incoming_specs,
            values=values,
            link_builder=lambda target, links, source_map: _build_runtime_kwargs(
                incoming=links,
                source_map=source_map,
                target_name=target,
            ),
        )
        mark_process_success(process_node, graph_outputs)
        success = True
        if result_path_str:
            try:
                from aiida.orm.utils.serialize import serialize

                result_payload = serialize(
                    {"graph_pid": graph_pid, "outputs": graph_outputs}
                )
                if not isinstance(result_payload, (str, bytes)):
                    result_payload = json.dumps(result_payload)
                if isinstance(result_payload, bytes):
                    result_payload = result_payload.decode("utf-8")

                from pathlib import Path

                result_path = Path(result_path_str)
                result_path.parent.mkdir(parents=True, exist_ok=True)
                result_path.write_text(result_payload)
            except Exception:
                logging.getLogger(__name__).exception(
                    "Failed to persist Graph scheduler results to %s",
                    result_path_str,
                )
        return graph_outputs
    except Exception as exc:
        mark_process_failure(process_node, exc)
        raise
    finally:
        if success and graph_proxy is not None:
            try:
                persist_workflow_knowledge_graph(
                    process_node=process_node,
                    graph=graph_proxy,
                    engine_kind="airflow",
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Failed to persist workflow knowledge graph for %s", process_node
                )
        if not process_node.is_sealed:
            process_node.seal()
