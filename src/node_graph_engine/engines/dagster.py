from __future__ import annotations

import os
import re
import tempfile
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aiida import orm
from dagster import (
    DagsterInstance,
    DependencyDefinition,
    GraphDefinition,
    JobDefinition,
    In,
    Out,
    op,
)
from node_graph import Graph
from node_graph.graph import BUILTIN_TASKS

from ..core.base import BaseEngine
from ..core.execution import (
    compute_graph_outputs,
    execute_task_job,
    iterate_task_order,
    mark_process_failure,
    mark_process_success,
    prepare_graph_run,
    _scan_links_topology,
)
from ..core.semantics import TaskSemantics
from ..core.task import EngineTaskExecutor, TaskMeta
from ..core.utils import (
    _collect_literals,
    update_nested_dict_with_special_keys,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..orm.data.knowledge_graph import persist_workflow_knowledge_graph


def _node_job(
    parent_pid: Optional[str],
    _ng_meta: TaskMeta,
    _ng_callable: Optional[Dict[str, Any]] = None,
    _ng_engine_name: str = "dagster_flow",
    _ng_task_inputs: Optional[Dict[str, Any]] = None,
    _ng_task_outputs: Optional[Dict[str, Any]] = None,
    _ng_default_user_email: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Execute a single task within a Dagster op."""

    user = load_default_user(_ng_default_user_email) if _ng_default_user_email else None

    try:
        return execute_task_job(
            parent_pid=parent_pid,
            meta=_ng_meta,
            callable_payload=_ng_callable,
            runtime_inputs=kwargs,
            engine_name=_ng_engine_name,
            node_inputs=_ng_task_inputs,
            node_outputs=_ng_task_outputs,
            build_sub_engine=lambda name: DagsterEngine(
                job_name=name,
                _default_user_email=_ng_default_user_email,
            ),
            user=user,
        )
    finally:
        close_threadlocal_aiida_session()


class DagsterEngine(BaseEngine):
    """Execute ``Graph`` instances using Dagster jobs."""

    engine_kind = "dagster"

    def __init__(
        self,
        job_name: str = "dagster_flow",
        *,
        instance: Optional[DagsterInstance] = None,
        _default_user_email: Optional[str] = None,
    ) -> None:
        super().__init__(job_name)
        self.job_name = job_name
        self._temp_dagster_home: Optional[tempfile.TemporaryDirectory] = None
        if instance is None:
            dagster_home = os.environ.get("DAGSTER_HOME")
            if dagster_home:
                self._instance = DagsterInstance.get()
            else:
                self._temp_dagster_home = tempfile.TemporaryDirectory()
                dagster_home = self._temp_dagster_home.name
                os.environ.setdefault("DAGSTER_HOME", dagster_home)
                self._instance = DagsterInstance.ephemeral(dagster_home)
        else:
            self._instance = instance
        self._instance = instance or DagsterInstance.get()
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email
        self._dagster_run_id: Optional[str] = None
        self._last_graph_outputs: Optional[Dict[str, Any]] = None

    def _metadata_extra_key(self) -> str:
        sanitized = re.sub(r"[^0-9A-Za-z_]+", "_", self.job_name) or "job"
        return f"node_graph_engine_dagster__{sanitized}"

    def _set_run_metadata(
        self,
        process_node: orm.WorkflowNode,
        *,
        run_id: Optional[str] = None,
        finalized: Optional[bool] = None,
    ) -> None:
        extra_key = self._metadata_extra_key()
        metadata = process_node.base.extras.get(extra_key, {})
        if run_id is not None:
            metadata["run_id"] = run_id
        if finalized is not None:
            metadata["finalized"] = finalized
        process_node.base.extras.set(extra_key, metadata)

    def _load_process_node(self, graph_pid: str) -> orm.WorkflowNode:
        return orm.load_node(graph_pid)

    def _finalize_run(self, process_node: orm.WorkflowNode) -> None:
        self._set_run_metadata(process_node, finalized=True)
        if not process_node.is_sealed:
            process_node.seal()

    def _mark_failure_once(self, graph_pid: str, exc: BaseException) -> None:
        process_node = self._load_process_node(graph_pid)
        metadata = process_node.base.extras.get(self._metadata_extra_key(), {})
        if metadata.get("finalized"):
            return
        mark_process_failure(process_node, exc)
        self._finalize_run(process_node)

    def _encode_output_value(self, value: Any) -> Any:
        """Convert output values into Dagster-persistable representations."""

        if isinstance(value, orm.Node):
            return {"__node_graph_engine__aiida_node_uuid__": value.uuid}
        if isinstance(value, tuple):
            return {
                "__node_graph_engine__tuple__": [
                    self._encode_output_value(item) for item in value
                ]
            }
        if isinstance(value, list):
            return [self._encode_output_value(item) for item in value]
        if isinstance(value, dict):
            return {key: self._encode_output_value(item) for key, item in value.items()}
        return value

    def _decode_output_value(self, value: Any) -> Any:
        """Restore Dagster-stored results back into runtime objects."""

        if isinstance(value, dict):
            if "__node_graph_engine__aiida_node_uuid__" in value:
                node_uuid = value["__node_graph_engine__aiida_node_uuid__"]
                return orm.load_node(node_uuid)
            if "__node_graph_engine__tuple__" in value:
                encoded = value["__node_graph_engine__tuple__"]
                return tuple(self._decode_output_value(item) for item in encoded)
            return {key: self._decode_output_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._decode_output_value(item) for item in value]
        return value

    # ------------------------------------------------------------------
    # Dagster op/graph construction
    # ------------------------------------------------------------------
    def _build_task_executor(self, task, label_kind: str) -> EngineTaskExecutor:
        executor_payload = task.spec.executor.to_dict()
        if executor_payload is None:
            raise ValueError(
                f"Cannot build executor for task {task.name} without a callable."
            )

        meta = self._build_node_task_meta(task, label_kind=label_kind)

        static_kwargs: Dict[str, Any] = {
            "_ng_engine_name": self.job_name,
            "_ng_task_inputs": task.spec.inputs.to_dict() if task.spec.inputs else {},
            "_ng_task_outputs": task.spec.outputs.to_dict()
            if task.spec.outputs
            else {},
            "_ng_default_user_email": self._default_user_email,
        }

        return EngineTaskExecutor(
            runner=_node_job,
            meta=meta,
            callable=executor_payload,
            static_kwargs=static_kwargs,
        )

    def _make_node_op(
        self,
        *,
        node_name: str,
        task,
        executor: EngineTaskExecutor,
        incoming_links: Iterable[Any],
        context_param: str,
    ) -> Tuple[Any, Dict[str, DependencyDefinition]]:
        """Create a Dagster op for ``task`` and its dependency map."""

        incoming_links = tuple(incoming_links)
        ins: Dict[str, In] = {context_param: In()}
        dependency_map: Dict[str, DependencyDefinition] = {
            context_param: DependencyDefinition("engine__init")
        }
        dep_index = 0
        upstream_param_map: Dict[str, str] = {}

        for lk in incoming_links:
            from_name = lk.from_task.name
            if from_name in BUILTIN_TASKS:
                continue
            if from_name in upstream_param_map:
                continue
            key = f"dep_{dep_index}"
            dep_index += 1
            upstream_param_map[from_name] = key
            ins[key] = In()
            dependency_map[key] = DependencyDefinition(from_name)

        op_name = node_name

        @op(
            name=op_name,
            ins=ins,
            out=Out(),
            description=f"Graph task '{node_name}' executed via Dagster.",
        )
        def _dagster_node_op(context, engine_context, **dependencies):
            source_map: Dict[str, Any] = dict(engine_context["values"])
            for from_name, param_key in upstream_param_map.items():
                source_map[from_name] = dependencies.get(param_key)
            try:
                kw = dict(_collect_literals(task))
                kw.update(
                    self._build_link_kwargs(
                        target_name=node_name,
                        links=incoming_links,
                        source_map=source_map,
                    )
                )
                kw = update_nested_dict_with_special_keys(kw)
                results = executor.invoke(
                    parent_pid=engine_context["graph_pid"],
                    **kw,
                )
            except Exception as exc:
                self._mark_failure_once(engine_context["graph_pid"], exc)
                raise
            return results

        return _dagster_node_op, dependency_map

    def _build_job_components(
        self,
        ng: Graph,
        *,
        parent_pid: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> Tuple[GraphDefinition, Dict[str, Any]]:
        order, incoming, _ = _scan_links_topology(ng)
        incoming_map = incoming

        context_param = "engine_context"

        @op(
            name="engine__init",
            out=Out(),
            description="Prepare Graph execution context for Dagster run.",
        )
        def _init_graph_op(context):
            graph_context = prepare_graph_run(
                ng,
                parent_pid=parent_pid,
                user=load_default_user(self._default_user_email),
                encode_graph_inputs=True,
            )
            builtins = self._snapshot_builtins(ng)
            for key, value in builtins.items():
                graph_context.values.setdefault(key, value)
            process_node = graph_context.process_node
            self._graph_pid = process_node.uuid
            self._last_graph_outputs = None
            self._set_run_metadata(process_node, run_id=context.run_id, finalized=False)
            values = dict(graph_context.values)
            semantics_data = (
                graph_context.semantics.to_dict()
                if graph_context.semantics is not None
                else None
            )
            return {
                "graph_pid": process_node.uuid,
                "values": values,
                "semantics": semantics_data,
                "graph_uuid": ng.uuid,
            }

        node_defs = [_init_graph_op]
        dependencies: Dict[str, Dict[str, DependencyDefinition]] = {}
        scheduled_nodes: List[str] = []

        for name in iterate_task_order(tuple(order)):
            task = ng.tasks[name]
            label_kind = "return" if self._is_graph_task(task) else "create"
            executor = self._build_task_executor(task, label_kind=label_kind)
            op_def, dep_map = self._make_node_op(
                node_name=name,
                task=task,
                executor=executor,
                incoming_links=incoming.get(name, ()),
                context_param=context_param,
            )
            node_defs.append(op_def)
            scheduled_nodes.append(name)
            if dep_map:
                dependencies[op_def.name] = dep_map

        finalize_input_map: Dict[str, str] = {}
        finalize_ins: Dict[str, In] = {context_param: In()}
        finalize_deps: Dict[str, DependencyDefinition] = {
            context_param: DependencyDefinition("engine__init")
        }

        for index, name in enumerate(scheduled_nodes):
            param = f"node_{index}"
            finalize_input_map[param] = name
            finalize_ins[param] = In()
            finalize_deps[param] = DependencyDefinition(f"{name}")

        @op(
            name="engine__finalize",
            ins=finalize_ins,
            out=Out(),
            description="Finalize Graph execution and compute graph outputs.",
        )
        def _finalize_graph_op(context, engine_context, **node_results):
            graph_pid = engine_context["graph_pid"]
            process_node = self._load_process_node(graph_pid)
            semantics_spec = TaskSemantics.from_dict(engine_context.get("semantics"))

            success = False
            try:
                values: Dict[str, Any] = dict(engine_context["values"])
                for param, result in node_results.items():
                    node_name = finalize_input_map[param]
                    values[node_name] = result
                graph_outputs = compute_graph_outputs(
                    incoming=incoming_map,
                    values=values,
                    link_builder=self._build_link_kwargs,
                )
                mark_process_success(process_node, graph_outputs)
                self._finalize_run(process_node)
                success = True
            except Exception as exc:
                self._mark_failure_once(graph_pid, exc)
                raise
            finally:
                if success:
                    persist_workflow_knowledge_graph(
                        process_node=process_node,
                        graph=ng,
                        engine_kind=self.engine_kind,
                    )
            self._last_graph_outputs = graph_outputs
            return self._encode_output_value(graph_outputs)

        node_defs.append(_finalize_graph_op)
        if finalize_deps:
            dependencies[_finalize_graph_op.name] = finalize_deps

        dagster_graph = GraphDefinition(
            name=job_name or self.job_name,
            node_defs=node_defs,
            dependencies=dependencies,
        )

        return dagster_graph, {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(
        self,
        ng: Graph,
        parent_pid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a Dagster job for ``ng`` and execute it synchronously."""

        dagster_graph, resource_defs = self._build_job_components(
            ng,
            parent_pid=parent_pid,
        )

        dagster_job = dagster_graph.to_job(resource_defs=resource_defs)

        run_result = dagster_job.execute_in_process(
            instance=self._instance, raise_on_error=True
        )
        self._dagster_run_id = run_result.run_id

        graph_outputs = self._last_graph_outputs
        if graph_outputs is None:
            try:
                encoded_outputs = run_result.output_for_node("engine__finalize")
                graph_outputs = self._decode_output_value(encoded_outputs)
            except KeyError:
                graph_outputs = None

        if graph_outputs is None:
            raise RuntimeError("Dagster job completed without recording outputs.")

        return graph_outputs

    def build_job(
        self,
        ng: Graph,
        *,
        job_name: Optional[str] = None,
        parent_pid: Optional[str] = None,
    ) -> JobDefinition:
        """Return a Dagster job that executes ``ng`` using this engine."""

        dagster_graph, resource_defs = self._build_job_components(
            ng,
            parent_pid=parent_pid,
            job_name=job_name,
        )

        return dagster_graph.to_job(resource_defs=resource_defs)

    @property
    def dagster_run_id(self) -> Optional[str]:
        """Return the Dagster run identifier from the last execution, if any."""

        return self._dagster_run_id
