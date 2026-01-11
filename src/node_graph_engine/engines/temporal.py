from __future__ import annotations

"""Temporal engine adapter for node-graph workflows."""

from datetime import timedelta
from typing import Any, Dict, Optional, TYPE_CHECKING
import asyncio
from uuid import uuid4

from temporalio import activity, workflow
from temporalio.client import Client

with workflow.unsafe.imports_passed_through():
    # Pass-through avoids Temporal sandbox restrictions for non-workflow modules.
    from ..core.base import BaseEngine
    from ..core.execution import (
        compute_graph_outputs,
        execute_task_job,
        mark_process_failure,
        mark_process_success,
        prepare_graph_run,
    )
    from ..core.remote_execution import _remote_task_job
    from ..core.task import TaskMeta
    from ..core.utils import (
        _build_node_link_kwargs,
        _collect_literals,
        _scan_links_topology,
        close_threadlocal_aiida_session,
        get_active_profile_name,
        get_default_user_email,
        get_nested_dict,
        load_default_user,
        update_nested_dict_with_special_keys,
    )
    from ..neo4j.knowledge_graph import persist_workflow_knowledge_graph

if TYPE_CHECKING:
    from node_graph import Graph


def _temporal_link_socket_value(
    from_name: str, from_socket: str, source_map: Dict[str, Any]
) -> Any:
    # Resolve a single socket value from an upstream task payload.
    return get_nested_dict(source_map[from_name], from_socket, default=None)


def _temporal_link_whole_output(from_name: str, source_map: Dict[str, Any]) -> Any:
    """Return the entire upstream task payload."""
    return source_map[from_name]


def _temporal_link_bundle(payload: Dict[str, Any]) -> Any:
    """Bundle multiple upstream values into a single payload."""
    return payload


def _build_temporal_link_kwargs(
    target_name: str,
    links,
    source_map: Dict[str, Any],
) -> Dict[str, Any]:
    # Share node-graph's link semantics for _wait, _outputs, and multi-fan-in.
    return _build_node_link_kwargs(
        target_name,
        links,
        source_map,
        resolve_socket=_temporal_link_socket_value,
        resolve_whole=_temporal_link_whole_output,
        bundle_factory=_temporal_link_bundle,
    )


@activity.defn(name="ng:temporal_task")
async def temporal_node_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a graph task as a Temporal activity."""

    runtime_inputs = dict(payload.get("runtime_inputs") or {})
    meta = payload.get("_ng_meta")
    callable_payload = payload.get("_ng_callable")
    engine_name = payload.get("_ng_engine_name") or "temporal-flow"
    task_inputs = payload.get("_ng_task_inputs")
    task_outputs = payload.get("_ng_task_outputs")
    task_type = (payload.get("_ng_task_type") or "").lower()
    task_metadata = payload.get("_ng_task_metadata") or {}
    parent_pid = payload.get("parent_pid")
    default_user_email = payload.get("_ng_default_user_email")
    profile_name = payload.get("_ng_profile_name")
    schedule_subgraphs = bool(payload.get("_ng_schedule_subgraphs", False))

    def _run_sync() -> Dict[str, Any]:
        try:
            if task_type == "remotefunction":
                return _remote_task_job(
                    parent_pid=parent_pid,
                    _ng_meta=meta,
                    _ng_callable=callable_payload,
                    _ng_engine_name=engine_name,
                    _ng_task_inputs=task_inputs,
                    _ng_task_outputs=task_outputs,
                    _ng_task_metadata=task_metadata,
                    _ng_default_user_email=default_user_email,
                    _ng_profile_name=profile_name,
                    **runtime_inputs,
                )

            user = load_default_user(default_user_email) if default_user_email else None

            def _build_sub_engine(name: str):
                from .local import LocalEngine

                return LocalEngine(name=name)

            return execute_task_job(
                parent_pid=parent_pid,
                meta=meta,
                callable_payload=callable_payload,
                runtime_inputs=runtime_inputs,
                engine_name=engine_name,
                node_inputs=task_inputs,
                node_outputs=task_outputs,
                build_sub_engine=_build_sub_engine,
                user=user,
                schedule_subgraphs=schedule_subgraphs,
            )
        finally:
            close_threadlocal_aiida_session()

    return await asyncio.to_thread(_run_sync)


@workflow.defn(name="NodeGraphWorkflow", sandboxed=False)
class NodeGraphWorkflow:
    """Temporal workflow that schedules node-graph tasks as activities."""

    @workflow.run
    async def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with workflow.unsafe.imports_passed_through():
            from node_graph import Graph
            from node_graph.graph import BUILTIN_TASKS

        # Reconstruct the graph inside the workflow worker.
        ng = Graph.from_dict(payload["graph"])
        order, incoming, _required = _scan_links_topology(ng)
        base_values: Dict[str, Any] = dict(payload.get("values") or {})

        parent_pid = payload.get("parent_pid")
        engine_name = payload.get("engine_name") or "temporal-flow"
        default_user_email = payload.get("default_user_email")
        profile_name = payload.get("profile_name")
        activity_timeout_seconds = payload.get("activity_timeout_seconds", 600.0)
        activity_task_queue = payload.get("activity_task_queue")
        schedule_subgraphs = bool(payload.get("schedule_subgraphs", False))

        # Build a dependency map so independent nodes can run in parallel.
        dependencies: Dict[str, set[str]] = {}
        for name in order:
            if name in BUILTIN_TASKS:
                continue
            deps: set[str] = set()
            for link in incoming.get(name, []):
                src = link.from_task.name
                if src in BUILTIN_TASKS:
                    continue
                deps.add(src)
            dependencies[name] = deps

        # Activity execution configuration shared across tasks.
        activity_kwargs = {
            "start_to_close_timeout": timedelta(seconds=float(activity_timeout_seconds))
        }
        if activity_task_queue:
            activity_kwargs["task_queue"] = activity_task_queue

        task_futures: Dict[str, asyncio.Task] = {}

        async def _run_node(node_name: str) -> Dict[str, Any]:
            # Resolve upstream results before scheduling this node's activity.
            deps = dependencies.get(node_name, set())
            resolved: Dict[str, Any] = {}
            for dep in deps:
                resolved[dep] = await task_futures[dep]

            task = ng.tasks[node_name]
            source_map = dict(base_values)
            source_map.update(resolved)
            kw = dict(_collect_literals(task))
            kw.update(
                _build_temporal_link_kwargs(
                    target_name=node_name,
                    links=incoming.get(node_name, []),
                    source_map=source_map,
                )
            )
            kw = update_nested_dict_with_special_keys(kw)

            task_type = getattr(task.spec, "task_type", "") or ""
            label_kind = "return" if task_type.lower() == "graph" else "create"
            meta = TaskMeta.from_task(task, label_kind=label_kind).as_dict()
            metadata = getattr(task.spec, "metadata", {}) or {}

            activity_payload = {
                "parent_pid": parent_pid,
                "_ng_meta": meta,
                "_ng_callable": task.spec.executor.to_dict(),
                "_ng_engine_name": engine_name,
                "_ng_task_inputs": (
                    task.spec.inputs.to_dict() if task.spec.inputs else None
                ),
                "_ng_task_outputs": (
                    task.spec.outputs.to_dict() if task.spec.outputs else None
                ),
                "_ng_task_type": task_type,
                "_ng_task_metadata": dict(metadata),
                "_ng_default_user_email": default_user_email,
                "_ng_profile_name": profile_name,
                "_ng_schedule_subgraphs": schedule_subgraphs,
                "runtime_inputs": kw,
            }

            return await workflow.execute_activity(
                temporal_node_task,
                activity_payload,
                **activity_kwargs,
            )

        # Schedule all nodes; dependency awaits enforce ordering where needed.
        for name in order:
            if name in BUILTIN_TASKS:
                continue
            task_futures[name] = asyncio.create_task(_run_node(name))

        values: Dict[str, Any] = dict(base_values)
        for name, fut in task_futures.items():
            values[name] = await fut

        return values


class TemporalEngine(BaseEngine):
    """Temporal engine adapter for Graph workflows."""

    engine_kind = "temporal"

    def __init__(
        self,
        workflow_id: Optional[str] = None,
        *,
        task_queue: str = "node-graph",
        client: Optional[Client] = None,
        activity_timeout: Optional[timedelta] = None,
        workflow_run_timeout: Optional[timedelta] = None,
        schedule_subgraphs: bool = False,
        _default_user_email: Optional[str] = None,
        _profile_name: Optional[str] = None,
    ) -> None:
        name = workflow_id or "node-graph-temporal"
        super().__init__(name)
        self.workflow_id = workflow_id
        self.task_queue = task_queue
        self.client = client
        self.activity_timeout = activity_timeout or timedelta(minutes=10)
        self.workflow_run_timeout = workflow_run_timeout
        self.schedule_subgraphs = schedule_subgraphs
        self._default_user_email = _default_user_email or get_default_user_email()
        self._profile_name = _profile_name or get_active_profile_name()

    @staticmethod
    def workflow_definitions():
        return [NodeGraphWorkflow]

    @staticmethod
    def activity_definitions():
        return [temporal_node_task]

    async def run_async(
        self,
        ng: Graph,
        *,
        parent_pid: Optional[str] = None,
        client: Optional[Client] = None,
        workflow_id: Optional[str] = None,
        task_queue: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute ``ng`` via Temporal and return the graph outputs as plain values."""

        temporal_client = client or self.client
        if temporal_client is None:
            raise RuntimeError("Temporal client is required to run workflows.")

        context = prepare_graph_run(
            ng,
            parent_pid=parent_pid,
            user=load_default_user(self._default_user_email),
            encode_graph_inputs=True,
        )
        self._graph_pid = context.process_node.uuid
        values = context.values
        success = False

        # Serialize the graph into a pure-data payload for Temporal.
        ng_payload = ng.to_dict(include_sockets=True, should_serialize=True)
        payload = {
            "graph": ng_payload,
            "values": values,
            "parent_pid": self._graph_pid,
            "engine_name": self.name,
            "default_user_email": self._default_user_email,
            "profile_name": self._profile_name,
            "activity_timeout_seconds": self.activity_timeout.total_seconds(),
            "activity_task_queue": task_queue or self.task_queue,
            "schedule_subgraphs": self.schedule_subgraphs,
        }

        try:
            wf_id = workflow_id or self.workflow_id or f"{self.name}-{uuid4().hex}"
            values = await temporal_client.execute_workflow(
                NodeGraphWorkflow.run,
                payload,
                id=wf_id,
                task_queue=task_queue or self.task_queue,
                run_timeout=self.workflow_run_timeout,
            )
            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=values,
                link_builder=_build_temporal_link_kwargs,
            )
            mark_process_success(context.process_node, graph_outputs)
            success = True
            return graph_outputs
        except Exception as exc:
            mark_process_failure(context.process_node, exc)
            raise
        finally:
            if success:
                persist_workflow_knowledge_graph(
                    process_node=context.process_node,
                    graph=context.ng,
                    engine_kind=self.engine_kind,
                )
            context.process_node.seal()

    def run(self, ng: Graph, parent_pid: Optional[str] = None) -> Dict[str, Any]:
        """Sync wrapper around :meth:`run_async`."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run_async(ng, parent_pid=parent_pid))
        raise RuntimeError("Use run_async when running inside an event loop.")
