from __future__ import annotations
from typing import Any, Dict, Optional
from node_graph import Graph

from ..core.base import BaseEngine
from ..core.execution import (
    compute_graph_outputs,
    execute_task_job,
    iterate_task_order,
    mark_process_failure,
    mark_process_success,
    prepare_graph_run,
)
from ..core.remote_execution import _remote_task_job
from ..core.semantics import finalize_pending_semantics, record_graph_semantics
from ..core.task import EngineTaskExecutor
from ..core.utils import (
    _collect_literals,
    close_threadlocal_aiida_session,
    update_nested_dict_with_special_keys,
)
from aiida import orm


def _node_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    **kwargs: Any,
) -> Dict[str, Any]:
    try:
        return execute_task_job(
            parent_pid=parent_pid,
            meta=_ng_meta,
            callable_payload=_ng_callable,
            runtime_inputs=kwargs,
            engine_name=_ng_engine_name,
            node_inputs=_ng_task_inputs,
            node_outputs=_ng_task_outputs,
            build_sub_engine=lambda name: LocalEngine(name=name),
        )
    finally:
        close_threadlocal_aiida_session()


class LocalEngine(BaseEngine):
    """
    Sync, dependency-free runner with provenance:

    - @task: calls the underlying python function, normalizes to {result: ...}
    - @task.graph: builds & runs a sub-Graph, resolves returned socket-handles to values
    - Provenance: records runtime *flattened* inputs & outputs around each task run
    - Link semantics from utils: _wait, _outputs, and multi-fan-in bundling
    """

    engine_kind = "local"

    def __init__(
        self,
        name: str = "local-flow",
    ):
        super().__init__(name)

    def run(
        self,
        ng: Graph,
        parent_pid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute ``ng`` and return the graph outputs as plain values."""
        context = prepare_graph_run(
            ng,
            parent_pid=parent_pid,
            encode_graph_inputs=True,
        )
        self._graph_pid = context.process_node.uuid
        values = context.values
        success = False

        try:
            for name in iterate_task_order(context.order):
                print(f"Running task: {name}")

                task = ng.tasks[name]

                kw = dict(_collect_literals(task))
                link_kwargs = self._build_link_kwargs(
                    target_name=name,
                    links=context.incoming.get(name, []),
                    source_map=values,
                )
                kw.update(link_kwargs)
                kw = update_nested_dict_with_special_keys(kw)

                label_kind = "return" if self._is_graph_task(task) else "create"
                executor = self._build_task_executor(
                    task,
                    label_kind=label_kind,
                )
                results = executor.invoke(
                    parent_pid=self._graph_pid,
                    **kw,
                )
                values[name] = results

            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=values,
                link_builder=self._build_link_kwargs,
            )
            process_node = orm.load_node(self._graph_pid)
            context.process_node = process_node
            mark_process_success(context.process_node, graph_outputs)
            record_graph_semantics(context.process_node, context.semantics)
            success = True
            return graph_outputs
        except Exception as e:
            mark_process_failure(context.process_node, e)
            raise
        finally:
            finalize_pending_semantics(
                context.process_node, context.ng, success=success
            )
            context.process_node.seal()

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)
        task_type = (getattr(task.spec, "task_type", "") or "").lower()

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": task.spec.inputs.to_dict(),
            "_ng_task_outputs": task.spec.outputs.to_dict(),
        }

        if task_type == "remotefunction":
            metadata = getattr(task.spec, "metadata", {}) or {}
            static_kwargs["_ng_task_metadata"] = dict(metadata)
            runner = _remote_task_job
        else:
            runner = _node_job

        return EngineTaskExecutor(
            runner=runner,
            meta=meta,
            callable=executor,
            static_kwargs=static_kwargs,
        )
