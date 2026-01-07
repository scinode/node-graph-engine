from __future__ import annotations

"""Shared helpers for executing graph tasks across engine implementations."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import asyncio
import inspect
import logging
import uuid

from aiida import orm
from aiida.common.links import LinkType
from aiida_pythonjob.calculations.pyfunction import PyFunction
from aiida_pythonjob.data.deserializer import deserialize_to_raw_python_data
from aiida_pythonjob.data.serializer import all_serializers
from aiida_pythonjob.parsers.utils import parse_outputs
from node_graph import Graph, task
from node_graph.executor import RuntimeExecutor
from node_graph.graph import BUILTIN_TASKS
from node_graph.task_spec import TaskHandle
from node_graph.socket_spec import SocketSpec
from node_graph.utils.graph import materialize_graph
from plumpy import ProcessState

from node_graph.semantics import TaskSemantics

from .task import TaskMeta
from .utils import (
    _decode_runtime_inputs,
    _encode_runtime_inputs,
    _scan_links_topology,
    setup_inputs,
    update_nested_dict_with_special_keys,
    update_outputs,
)

logger = logging.getLogger(__name__)


@dataclass
class GraphRunContext:
    """Container describing a Graph workflow run."""

    ng: Graph
    order: Tuple[str, ...]
    incoming: Dict[str, Any]
    required: Dict[str, Any]
    process_node: orm.WorkflowNode
    parent_node: Optional[orm.Node]
    values: Dict[str, Any]
    semantics: Optional[TaskSemantics]


def _ensure_meta(meta: Any) -> TaskMeta:
    if isinstance(meta, TaskMeta):
        return meta
    if isinstance(meta, dict):
        return TaskMeta(**meta)
    if hasattr(meta, "as_dict"):
        return TaskMeta(**meta.as_dict())
    raise TypeError(f"Unsupported metadata payload for task execution: {meta!r}")


def _resolve_callable(callable_payload: Optional[Dict[str, Any]], node_name: str):
    if callable_payload is None:
        raise ValueError(f"Cannot execute task {node_name} without a callable payload")
    callable_obj = RuntimeExecutor(**callable_payload).callable
    if isinstance(callable_obj, TaskHandle):
        if hasattr(callable_obj, "_callable"):
            callable_obj = callable_obj._callable
        else:
            raise TypeError(f"Cannot execute TaskHandle for task {node_name}")
    return callable_obj


def prepare_graph_run(
    ng: Graph,
    *,
    parent_pid: Optional[str],
    user: Optional[orm.User] = None,
    encode_graph_inputs: bool = False,
) -> GraphRunContext:
    from node_graph_engine.orm.node_graph import NodeGraphNode
    from .utils import save_nodegraph_data

    order, incoming, required = _scan_links_topology(ng)
    parent_node = orm.load_node(parent_pid) if parent_pid else None
    workflow_kwargs = {"user": user} if user is not None else {}
    process_node = NodeGraphNode(**workflow_kwargs)
    if parent_node is not None:
        process_node.base.links.add_incoming(
            parent_node, LinkType.CALL_WORK, link_label=ng.name
        )
    process_node.set_process_label(f"Graph<{ng.name}>")
    process_node.set_process_state(ProcessState.RUNNING)
    inputs = save_nodegraph_data(process_node, ng, user)
    process_node.store()
    values_payload: Dict[str, Any]
    if encode_graph_inputs:
        values_payload = _encode_runtime_inputs(inputs)
    else:
        values_payload = inputs
    values = {"graph_inputs": values_payload}
    semantics = TaskSemantics.from_specs(ng.spec.inputs, ng.spec.outputs)
    return GraphRunContext(
        ng=ng,
        order=tuple(order),
        incoming=incoming,
        required=required,
        process_node=process_node,
        parent_node=parent_node,
        values=values,
        semantics=semantics,
    )


def compute_graph_outputs(
    *,
    incoming: Dict[str, Any],
    values: Dict[str, Any],
    link_builder: Callable[[str, Iterable[Any], Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    graph_outputs = link_builder(
        "graph_outputs",
        incoming.get("graph_outputs", []),
        values,
    )
    graph_outputs = _decode_runtime_inputs(graph_outputs)
    return update_nested_dict_with_special_keys(graph_outputs)


def mark_process_success(
    process_node: orm.WorkflowNode, outputs: Dict[str, Any]
) -> None:
    update_outputs(process_node, outputs)
    process_node.set_process_state(ProcessState.FINISHED)
    process_node.set_exit_status(0)
    process_node.set_checkpoint(None)


def mark_process_failure(process_node: orm.WorkflowNode, exc: BaseException) -> None:
    process_node.set_process_state(ProcessState.EXCEPTED)
    process_node.set_exit_message(str(exc))
    process_node.set_checkpoint(None)


def execute_task_job(
    *,
    parent_pid: Optional[str],
    meta: Any,
    callable_payload: Optional[Dict[str, Any]],
    runtime_inputs: Dict[str, Any],
    engine_name: str,
    node_inputs: Optional[Dict[str, Any]],
    node_outputs: Optional[Dict[str, Any]],
    build_sub_engine: Optional[Callable[[str], Any]] = None,
    user: Optional[orm.User] = None,
    schedule_subgraphs: bool = False,
    task_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta_obj = _ensure_meta(meta)
    parent_calc_node = orm.load_node(parent_pid) if parent_pid else None
    inputs = _decode_runtime_inputs(runtime_inputs)
    inputs = update_nested_dict_with_special_keys(inputs)
    callable_obj = _resolve_callable(callable_payload, meta_obj.node_name)

    if not meta_obj.is_graph:
        process_kwargs = {"user": user} if user is not None else {}
        process_node = orm.CalcFunctionNode(**process_kwargs)
        process_node.set_process_label(meta_obj.node_name)
        if parent_calc_node is not None:
            process_node.base.links.add_incoming(
                parent_calc_node, LinkType.CALL_CALC, link_label=meta_obj.node_name
            )
        process_node.set_process_state(ProcessState.RUNNING)
        setup_inputs(process_node, inputs)
        process_node.store()
        try:
            call_kwargs = deserialize_to_raw_python_data(inputs)
            print("Call kwargs:", call_kwargs)
            results = callable_obj(**call_kwargs)
            if inspect.isawaitable(results):
                results = asyncio.run(results)
            outputs_spec_dict = meta_obj.outputs_spec or {}
            outputs_spec = (
                SocketSpec.from_dict(outputs_spec_dict) if outputs_spec_dict else None
            )
            parse_kwargs = {
                "output_spec": outputs_spec,
                "exit_codes": PyFunction.exit_codes,
                "logger": logger,
                "serializers": all_serializers,
            }
            if user is not None:
                parse_kwargs["user"] = user
            outputs, exit_code = parse_outputs(results, **parse_kwargs)
            if exit_code is not None and exit_code.status != 0:
                process_node.set_exit_status(exit_code.status)
                process_node.set_exit_message(exit_code.message)
                process_node.set_process_state(ProcessState.EXCEPTED)
            else:
                update_outputs(process_node, outputs)
                process_node.set_process_state(ProcessState.FINISHED)
                process_node.set_exit_status(0)
            process_node.seal()
            return _encode_runtime_inputs(outputs)
        except Exception as exc:
            process_node.set_exit_status(1)
            process_node.set_exit_message(str(exc))
            process_node.set_process_state(ProcessState.EXCEPTED)
            process_node.seal()
            raise

    if build_sub_engine is None:
        raise RuntimeError(
            "Sub-graph execution requested but no engine builder supplied"
        )

    inputs_spec = SocketSpec.from_dict(node_inputs or {})
    outputs_spec = SocketSpec.from_dict(node_outputs or {})
    callable_obj.__globals__[callable_obj.__name__] = task.graph()(callable_obj)
    sub_ng = materialize_graph(
        callable_obj,
        inputs_spec,
        outputs_spec,
        meta_obj.node_name,
        Graph,
        args=(),
        kwargs=inputs,
        var_kwargs={},
    )
    sub_ng.name = f"{meta_obj.node_name}"
    sub_engine_name = meta_obj.node_name
    if schedule_subgraphs:
        sub_engine_name = f"{sub_engine_name}__{uuid.uuid4().hex}"
    sub_engine = build_sub_engine(sub_engine_name)
    if schedule_subgraphs:
        if hasattr(sub_engine, "submit"):
            results = sub_engine.submit(
                sub_ng,
                parent_pid=parent_pid,
                task_context=task_context,
                wait=True,
                force_trigger=False,
            )
            if results is None:
                raise RuntimeError(
                    "Engine.submit returned no results despite wait=True"
                )
        elif hasattr(sub_engine, "run_via_scheduler"):
            results = sub_engine.run_via_scheduler(
                sub_ng, parent_pid=parent_pid, task_context=task_context, wait=True
            )
        else:
            results = sub_engine.run(sub_ng, parent_pid=parent_pid)
    else:
        results = sub_engine.run(sub_ng, parent_pid=parent_pid)
    return _encode_runtime_inputs(results)


def iterate_task_order(order: Iterable[str]) -> Iterable[str]:
    for name in order:
        if name in BUILTIN_TASKS:
            continue
        yield name
