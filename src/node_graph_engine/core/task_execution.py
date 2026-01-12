from __future__ import annotations

"""Modular task execution helpers shared across engine adapters."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

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
from aiida_pythonjob.utils import serialize_ports
from node_graph import Graph
from node_graph.executor import RuntimeExecutor
from node_graph.socket_spec import SocketSpec
from node_graph.task_spec import TaskHandle
from node_graph.utils.graph import materialize_graph
from plumpy import ProcessState

from .task import TaskMeta
from .utils import (
    _decode_runtime_inputs,
    _encode_runtime_inputs,
    setup_inputs,
    update_nested_dict_with_special_keys,
    update_outputs,
)

logger = logging.getLogger(__name__)


def ensure_meta(meta: Any) -> TaskMeta:
    """Normalize task metadata to a TaskMeta instance."""
    if isinstance(meta, TaskMeta):
        return meta
    if isinstance(meta, dict):
        return TaskMeta(**meta)
    if hasattr(meta, "as_dict"):
        return TaskMeta(**meta.as_dict())
    raise TypeError(f"Unsupported metadata payload for task execution: {meta!r}")


def resolve_callable(callable_payload: Optional[Dict[str, Any]], node_name: str):
    """Resolve the runtime callable from an engine payload."""
    if callable_payload is None:
        raise ValueError(f"Cannot execute task {node_name} without a callable payload")
    callable_obj = RuntimeExecutor(**callable_payload).callable
    if isinstance(callable_obj, TaskHandle):
        if hasattr(callable_obj, "_callable"):
            callable_obj = callable_obj._callable
        else:
            raise TypeError(f"Cannot execute TaskHandle for task {node_name}")
    return callable_obj


@dataclass(frozen=True)
class TaskExecutionRequest:
    """Container describing a single node execution request."""

    parent_pid: Optional[str]
    meta: Any
    callable_payload: Optional[Dict[str, Any]]
    runtime_inputs: Dict[str, Any]
    engine_name: str
    node_inputs: Optional[Dict[str, Any]]
    node_outputs: Optional[Dict[str, Any]]
    build_sub_engine: Optional[Callable[[str], Any]]
    user: Optional[orm.User]
    schedule_subgraphs: bool
    task_context: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class TaskExecutionResult:
    """Result payload for a completed task."""

    outputs: Dict[str, Any]
    process_node: Optional[orm.Node]


class LocalFunctionExecutor:
    """Execute a Python callable as an AiiDA CalcFunctionNode."""

    def __init__(self, *, user: Optional[orm.User]) -> None:
        self._user = user

    def run(
        self,
        *,
        parent_pid: Optional[str],
        meta: TaskMeta,
        callable_obj: Callable[..., Any],
        inputs: Dict[str, Any],
    ) -> TaskExecutionResult:
        process_kwargs = {"user": self._user} if self._user is not None else {}
        process_node = orm.CalcFunctionNode(**process_kwargs)
        process_node.set_process_label(meta.node_name)
        parent_calc_node = orm.load_node(parent_pid) if parent_pid else None
        if parent_calc_node is not None:
            process_node.base.links.add_incoming(
                parent_calc_node, LinkType.CALL_CALC, link_label=meta.node_name
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
            outputs_spec_dict = meta.outputs_spec or {}
            outputs_spec = (
                SocketSpec.from_dict(outputs_spec_dict) if outputs_spec_dict else None
            )
            parse_kwargs = {
                "output_spec": outputs_spec,
                "exit_codes": PyFunction.exit_codes,
                "logger": logger,
                "serializers": all_serializers,
            }
            if self._user is not None:
                parse_kwargs["user"] = self._user
            try:
                outputs, exit_code = parse_outputs(results, **parse_kwargs)
            except Exception as exc:
                raise TypeError(
                    f"Task {meta.node_name!r} failed while parsing outputs: {exc}"
                ) from exc
            if exit_code is not None and exit_code.status != 0:
                process_node.set_exit_status(exit_code.status)
                process_node.set_exit_message(exit_code.message)
                process_node.set_process_state(ProcessState.EXCEPTED)
            else:
                update_outputs(process_node, outputs)
                process_node.set_process_state(ProcessState.FINISHED)
                process_node.set_exit_status(0)
            process_node.seal()
            return TaskExecutionResult(
                outputs=_encode_runtime_inputs(outputs),
                process_node=process_node,
            )
        except Exception as exc:
            process_node.set_exit_status(1)
            process_node.set_exit_message(str(exc))
            process_node.set_process_state(ProcessState.EXCEPTED)
            process_node.seal()
            raise


class SubgraphExecutor:
    """Execute a subgraph by delegating to a nested engine."""

    def __init__(
        self,
        *,
        build_sub_engine: Callable[[str], Any],
        schedule_subgraphs: bool,
    ) -> None:
        self._build_sub_engine = build_sub_engine
        self._schedule_subgraphs = schedule_subgraphs

    def run(
        self,
        *,
        parent_pid: Optional[str],
        meta: TaskMeta,
        callable_obj: Callable[..., Any],
        inputs: Dict[str, Any],
        node_inputs: Dict[str, Any],
        node_outputs: Dict[str, Any],
        task_context: Optional[Dict[str, Any]],
    ) -> TaskExecutionResult:
        inputs_spec = SocketSpec.from_dict(node_inputs)
        outputs_spec = SocketSpec.from_dict(node_outputs)
        sub_ng = materialize_graph(
            callable_obj,
            inputs_spec,
            outputs_spec,
            meta.node_name,
            Graph,
            args=(),
            kwargs=inputs,
            var_kwargs={},
        )
        sub_ng.name = f"{meta.node_name}"
        sub_engine_name = meta.node_name
        if self._schedule_subgraphs:
            sub_engine_name = f"{sub_engine_name}__{uuid.uuid4().hex}"
        sub_engine = self._build_sub_engine(sub_engine_name)
        if self._schedule_subgraphs:
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
        return TaskExecutionResult(
            outputs=_encode_runtime_inputs(results),
            process_node=None,
        )


class TaskExecutionCoordinator:
    """Coordinate task execution using the appropriate backend."""

    def run(self, request: TaskExecutionRequest) -> Dict[str, Any]:
        meta_obj = ensure_meta(request.meta)
        try:
            inputs = _decode_runtime_inputs(request.runtime_inputs)
        except Exception as exc:
            raise TypeError(
                f"Task {meta_obj.node_name!r} failed while decoding runtime inputs: {exc}"
            ) from exc
        inputs = update_nested_dict_with_special_keys(inputs)
        if not meta_obj.is_graph and request.node_inputs is not None:
            try:
                inputs_spec = SocketSpec.from_dict(request.node_inputs)
                inputs = serialize_ports(
                    python_data=inputs,
                    port_schema=inputs_spec,
                    serializers=all_serializers,
                    user=request.user,
                )
            except Exception as exc:
                raise TypeError(
                    f"Task {meta_obj.node_name!r} failed while serializing runtime inputs: {exc}"
                ) from exc
        callable_obj = resolve_callable(request.callable_payload, meta_obj.node_name)

        if not meta_obj.is_graph:
            executor = LocalFunctionExecutor(user=request.user)
            result = executor.run(
                parent_pid=request.parent_pid,
                meta=meta_obj,
                callable_obj=callable_obj,
                inputs=inputs,
            )
            return result.outputs

        if request.build_sub_engine is None:
            raise RuntimeError(
                "Sub-graph execution requested but no engine builder supplied"
            )
        executor = SubgraphExecutor(
            build_sub_engine=request.build_sub_engine,
            schedule_subgraphs=request.schedule_subgraphs,
        )
        result = executor.run(
            parent_pid=request.parent_pid,
            meta=meta_obj,
            callable_obj=callable_obj,
            inputs=inputs,
            node_inputs=request.node_inputs or {},
            node_outputs=request.node_outputs or {},
            task_context=request.task_context,
        )
        return result.outputs
