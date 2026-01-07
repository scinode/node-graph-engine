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
    _decode_runtime_inputs,
    _build_node_link_kwargs,
    update_nested_dict,
    update_nested_dict_with_special_keys,
)
from node_graph_engine.orm.data.knowledge_graph import persist_workflow_knowledge_graph
from node_graph_engine.core.task import TaskMeta

from .async_request import AsyncNodeExecutionRequest
from .common import IncomingSpec

_NG_RUNTIME_CONTEXT_KEY = "ng_runtime_context"
_NG_WHILE_STATE_KEY = "ng_while_state"


def _reset_task_instances_via_api(
    *,
    dag_id: str,
    run_id: str,
    task_ids: List[str],
) -> None:
    if not task_ids:
        return
    try:
        from airflow.configuration import conf

        base_url = conf.get("api", "base_url", fallback=None)
        if not base_url:
            base_url = conf.get("webserver", "base_url", fallback=None)
    except Exception:
        base_url = None
    if not base_url:
        base_url = "http://localhost:8080"
    base_url = base_url.rstrip("/")
    url = f"{base_url}/api/v2/dags/{dag_id}/clearTaskInstances"
    payload = {
        "dry_run": False,
        "only_failed": False,
        "only_running": False,
        "reset_dag_runs": False,
        "dag_run_id": run_id,
        "task_ids": task_ids,
    }
    headers: Dict[str, str] = {}
    token = os.environ.get("NG_AIRFLOW_API_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    user = os.environ.get("NG_AIRFLOW_API_USER")
    password = os.environ.get("NG_AIRFLOW_API_PASSWORD")
    basic_auth = os.environ.get("NG_AIRFLOW_API_BASIC_AUTH")
    if "Authorization" not in headers and user and password:
        try:
            import httpx
            from urllib.parse import urlsplit, urlunsplit

            parsed = urlsplit(base_url)
            path = parsed.path.rstrip("/")
            if path.endswith("/api/v2"):
                path = path[: -len("/api/v2")]
            auth_base = urlunsplit(
                (parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment)
            ).rstrip("/")
            auth_url = f"{auth_base}/auth/token"
            resp = httpx.post(
                auth_url,
                json={"username": user, "password": password},
                timeout=10.0,
            )
            resp.raise_for_status()
            token_payload = resp.json()
            access_token = token_payload.get("access_token")
            if access_token:
                headers["Authorization"] = f"Bearer {access_token}"
        except Exception:
            pass
    if "Authorization" not in headers:
        if basic_auth:
            raw = basic_auth
        elif user and password:
            raw = f"{user}:{password}"
        else:
            raw = ""
        if raw:
            import base64

            creds = base64.b64encode(raw.encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {creds}"
    try:
        import httpx

        resp = httpx.post(url, json=payload, headers=headers, timeout=10.0)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to clear task instances via Airflow API: {exc}"
        ) from exc


def _get_runtime_context(
    *,
    ti: Any,
    runtime_context_task_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    if ti is None or runtime_context_task_id is None:
        return None
    try:
        from airflow.models.xcom import XCom

        payload = XCom.get_one(
            key=_NG_RUNTIME_CONTEXT_KEY,
            dag_id=ti.dag_id,
            task_id=runtime_context_task_id,
            run_id=ti.run_id,
        )
        if payload is not None:
            return payload
    except Exception:
        pass
    return ti.xcom_pull(task_ids=runtime_context_task_id)


def _set_runtime_context(
    *,
    ti: Any,
    runtime_context_task_id: Optional[str],
    payload: Dict[str, Any],
) -> None:
    if ti is None or runtime_context_task_id is None:
        return
    try:
        from airflow.models.xcom import XCom

        XCom.set(
            key=_NG_RUNTIME_CONTEXT_KEY,
            value=payload,
            dag_id=ti.dag_id,
            task_id=runtime_context_task_id,
            run_id=ti.run_id,
        )
    except Exception:
        ti.xcom_push(key=_NG_RUNTIME_CONTEXT_KEY, value=payload)


def _apply_ctx_updates(
    *,
    runtime_context: Dict[str, Any],
    ctx_updates: Iterable[Dict[str, str]],
    source_map: Dict[str, Any],
) -> None:
    values = runtime_context.setdefault("values", {})
    ctx_values = values.setdefault("graph_ctx", {})
    for update in ctx_updates:
        from_task = update.get("from")
        if not from_task:
            continue
        payload = source_map.get(from_task)
        if payload is None:
            continue
        from_socket = update.get("from_socket", "")
        if from_socket == "_outputs":
            value = payload
        else:
            value = get_nested_dict(payload, from_socket, default=None)
        to_socket = update.get("to_socket")
        if to_socket:
            update_nested_dict(ctx_values, to_socket, value)


def _evaluate_while_condition(
    *,
    ti: Any,
    condition_specs: List[IncomingSpec],
) -> Tuple[bool, List[Any]]:
    """Return the boolean condition result and raw evaluated values."""
    source_map: Dict[str, Any] = {}
    for spec in condition_specs:
        from_task = spec.get("from")
        task_id = spec.get("from_task_id") or from_task
        if not from_task or not task_id:
            continue
        payload = ti.xcom_pull(task_ids=task_id)
        if payload is not None:
            try:
                source_map[from_task] = _decode_runtime_inputs(payload)
            except Exception:
                source_map[from_task] = payload

    condition_values: List[Any] = []
    for spec in condition_specs:
        from_task = spec.get("from")
        if not from_task:
            continue
        payload = source_map.get(from_task)
        if payload is None:
            continue
        from_socket = spec.get("from_socket") or ""
        if from_socket == "_outputs":
            condition_values.append(payload)
        else:
            condition_values.append(get_nested_dict(payload, from_socket, default=None))

    condition_value = bool(condition_values) and all(
        bool(value) for value in condition_values
    )
    return condition_value, condition_values


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
        runtime_context = _get_runtime_context(
            ti=ti,
            runtime_context_task_id=runtime_context_task_id,
        )

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
    if ti is not None:
        for spec in incoming_specs:
            task_name = spec.get("from")
            task_id = spec.get("from_task_id") or task_name
            if not task_name or task_name in BUILTIN_TASKS:
                continue
            pulled = ti.xcom_pull(task_ids=task_id)
            if pulled is not None:
                source_map[task_name] = pulled

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

    ctx_updates: List[Dict[str, str]] = list(context.get("_ng_ctx_updates") or [])
    if ctx_updates and ti is not None and runtime_context_task_id:
        runtime_context = _get_runtime_context(
            ti=ti,
            runtime_context_task_id=runtime_context_task_id,
        )
        if runtime_context is None:
            runtime_context = {"values": dict(base_values)}
        meta = _ensure_meta(context["_ng_meta"])
        _apply_ctx_updates(
            runtime_context=runtime_context,
            ctx_updates=ctx_updates,
            source_map={meta.node_name: result},
        )
        _set_runtime_context(
            ti=ti,
            runtime_context_task_id=runtime_context_task_id,
            payload=runtime_context,
        )

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

    # Only seed ctx from builtins; task-driven ctx updates happen during execution.
    graph_ctx_links = [
        lk
        for lk in ng.links
        if lk.to_task.name == "graph_ctx" and lk.from_task.name in BUILTIN_TASKS
    ]
    if graph_ctx_links:
        source_map = dict(graph_context.values)
        graph_ctx_values = _build_node_link_kwargs(
            "graph_ctx",
            graph_ctx_links,
            source_map,
            resolve_socket=lambda from_name, from_socket, source: get_nested_dict(
                source.get(from_name, {}), from_socket, default=None
            ),
            resolve_whole=lambda from_name, source: source.get(from_name),
            bundle_factory=lambda payload: payload,
        )
        graph_ctx_values = update_nested_dict_with_special_keys(graph_ctx_values)
        graph_context.values.setdefault("graph_ctx", {})
        if isinstance(graph_context.values["graph_ctx"], dict):
            graph_context.values["graph_ctx"].update(graph_ctx_values)
        else:
            graph_context.values["graph_ctx"] = graph_ctx_values

    semantics_data = (
        graph_context.semantics.to_dict()
        if graph_context.semantics is not None
        else None
    )
    payload = {
        "graph_pid": graph_context.process_node.uuid,
        "values": dict(graph_context.values),
        "semantics": semantics_data,
        "graph_uuid": ng.uuid,
    }
    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key=_NG_RUNTIME_CONTEXT_KEY, value=payload)
    return payload


def airflow_finalize_task(**context: Any) -> Dict[str, Any]:
    runtime_context_task_id = context["_ng_context_task_id"]
    profile_name = context.get("_ng_profile_name")
    ensure_aiida_profile(profile_name)
    node_task_ids: Iterable[str] = context.get("_ng_task_task_ids", [])
    task_id_map: Dict[str, str] = context.get("_ng_task_id_map", {})
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

    runtime_context = _get_runtime_context(
        ti=ti,
        runtime_context_task_id=runtime_context_task_id,
    )
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
    for task_name in node_task_ids:
        if task_name in BUILTIN_TASKS:
            continue
        task_id = task_id_map.get(task_name, task_name)
        pulled = ti.xcom_pull(task_ids=task_id)
        if pulled is not None:
            values[task_name] = pulled

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


def airflow_while_check_task(
    *,
    _ng_while_zone: str,
    _ng_while_condition_specs: List[IncomingSpec],
    _ng_while_condition_task_ids: List[str],
    _ng_while_child_task_ids: List[str],
    _ng_while_precheck_task_ids: Optional[List[str]] = None,
    _ng_while_max_iterations: int,
    _ng_ctx_updates: Optional[List[Dict[str, str]]] = None,
    _ng_runtime_context_task_id: Optional[str] = None,
    **context: Any,
) -> Dict[str, Any]:
    """Tail check for while: update ctx, reschedule if condition stays true."""
    ti = context.get("ti")
    dag_run = context.get("dag_run")
    runtime_context = _get_runtime_context(
        ti=ti,
        runtime_context_task_id=_ng_runtime_context_task_id,
    )
    if runtime_context is None:
        runtime_context = {"values": {}}

    if ti is None or dag_run is None:
        raise RuntimeError("Task instance context is required for while check task")

    ctx_updates = list(_ng_ctx_updates or [])
    if ctx_updates:
        source_map: Dict[str, Any] = {}
        for update in ctx_updates:
            from_task = update.get("from")
            task_id = update.get("from_task_id") or from_task
            if not task_id or not from_task:
                continue
            payload = ti.xcom_pull(task_ids=task_id)
            if payload is not None:
                source_map[from_task] = payload
        if source_map:
            _apply_ctx_updates(
                runtime_context=runtime_context,
                ctx_updates=ctx_updates,
                source_map=source_map,
            )

    _set_runtime_context(
        ti=ti,
        runtime_context_task_id=_ng_runtime_context_task_id,
        payload=runtime_context,
    )

    condition_value, _condition_values = _evaluate_while_condition(
        ti=ti,
        condition_specs=_ng_while_condition_specs,
    )

    while_state = runtime_context.setdefault(_NG_WHILE_STATE_KEY, {})
    current_iterations = int(while_state.get(_ng_while_zone, 0))
    if condition_value and current_iterations < int(_ng_while_max_iterations):
        while_state[_ng_while_zone] = current_iterations + 1
        _set_runtime_context(
            ti=ti,
            runtime_context_task_id=_ng_runtime_context_task_id,
            payload=runtime_context,
        )
        try:
            reset_ids = list(_ng_while_child_task_ids) + list(
                _ng_while_condition_task_ids
            )
            if _ng_while_precheck_task_ids:
                reset_ids.extend(list(_ng_while_precheck_task_ids))
            _reset_task_instances_via_api(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                task_ids=reset_ids,
            )
        except Exception as exc:
            logging.getLogger(__name__).exception(
                "Failed to reset tasks for while zone %s", _ng_while_zone
            )
            raise
        from airflow.exceptions import AirflowRescheduleException
        from airflow.sdk import timezone

        raise AirflowRescheduleException(timezone.utcnow())

    return {
        "condition": bool(condition_value),
        "iterations": current_iterations,
    }


def airflow_while_precheck_task(
    *,
    _ng_while_condition_specs: List[IncomingSpec],
    **context: Any,
) -> Dict[str, Any]:
    """Head check for while: skip body when the condition is false."""
    ti = context.get("ti")
    if ti is None:
        raise RuntimeError("Task instance context is required for while pre-check task")

    condition_value, _condition_values = _evaluate_while_condition(
        ti=ti,
        condition_specs=_ng_while_condition_specs,
    )
    if not condition_value:
        from airflow.exceptions import AirflowSkipException

        raise AirflowSkipException("While condition evaluated to False")
    return {"condition": True}
