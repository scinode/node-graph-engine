from __future__ import annotations

"""Shared helpers for executing graph tasks across engine implementations."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple


from aiida import orm
from aiida.common.links import LinkType
from node_graph import Graph
from node_graph.graph import BUILTIN_TASKS
from plumpy import ProcessState

from node_graph.semantics import TaskSemantics

from .task import TaskMeta
from .task_execution import (
    TaskExecutionCoordinator,
    TaskExecutionRequest,
    ensure_meta,
    resolve_callable,
)
from .utils import (
    _decode_runtime_inputs,
    _encode_runtime_inputs,
    _scan_links_topology,
    update_nested_dict_with_special_keys,
    update_outputs,
)


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
    """Backward-compatible wrapper around ensure_meta."""
    return ensure_meta(meta)


def _resolve_callable(callable_payload: Optional[Dict[str, Any]], node_name: str):
    """Backward-compatible wrapper around resolve_callable."""
    return resolve_callable(callable_payload, node_name)


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
    request = TaskExecutionRequest(
        parent_pid=parent_pid,
        meta=meta,
        callable_payload=callable_payload,
        runtime_inputs=runtime_inputs,
        engine_name=engine_name,
        node_inputs=node_inputs,
        node_outputs=node_outputs,
        build_sub_engine=build_sub_engine,
        user=user,
        schedule_subgraphs=schedule_subgraphs,
        task_context=task_context,
    )
    coordinator = TaskExecutionCoordinator()
    return coordinator.run(request)


def iterate_task_order(order: Iterable[str]) -> Iterable[str]:
    for name in order:
        if name in BUILTIN_TASKS:
            continue
        yield name
