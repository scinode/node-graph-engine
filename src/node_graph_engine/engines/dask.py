from __future__ import annotations

"""Engine implementation backed by the standard Dask scheduler."""

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
from ..core.task import EngineTaskExecutor
from ..core.utils import (
    _collect_literals,
    get_nested_dict,
    update_nested_dict_with_special_keys,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..neo4j.knowledge_graph import persist_workflow_knowledge_graph

from dask import compute, delayed
from dask.delayed import Delayed


def _ensure_dask_available() -> None:
    if compute is None or delayed is None:
        raise RuntimeError("dask is not installed. Install `dask` to use DaskEngine.")


def _node_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_engine_options: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    engine_options = dict(_ng_engine_options or {})
    default_user_email = engine_options.pop("_default_user_email", None)
    user = load_default_user(default_user_email) if default_user_email else None

    def _build_sub_engine(name: str) -> "DaskEngine":
        return DaskEngine(
            name=name,
            _default_user_email=default_user_email,
            **engine_options,
        )

    try:
        return execute_task_job(
            parent_pid=parent_pid,
            meta=_ng_meta,
            callable_payload=_ng_callable,
            runtime_inputs=kwargs,
            engine_name=_ng_engine_name,
            node_inputs=_ng_task_inputs,
            node_outputs=_ng_task_outputs,
            build_sub_engine=_build_sub_engine,
            user=user,
        )
    finally:
        close_threadlocal_aiida_session()


def _dask_node_submit(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    **kwargs: Any,
):
    submit_kwargs = dict(kwargs)
    engine_name = submit_kwargs.pop("_ng_engine_name", None) or "dask-flow"
    submit_kwargs.update(
        {
            "parent_pid": parent_pid,
            "_ng_meta": _ng_meta,
            "_ng_callable": _ng_callable,
            "_ng_engine_name": engine_name,
        }
    )
    return delayed(_node_job, pure=False)(**submit_kwargs)


class DaskEngine(BaseEngine):
    """Run Graphs using Dask's threaded scheduler with provenance tracking."""

    engine_kind = "dask"

    def __init__(
        self,
        name: str = "dask-flow",
        *,
        scheduler: Optional[Any] = "threads",
        compute_kwargs: Optional[Dict[str, Any]] = None,
        _default_user_email: Optional[str] = None,
    ) -> None:
        _ensure_dask_available()
        super().__init__(name)
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email
        self._scheduler = scheduler
        self._compute_kwargs: Dict[str, Any] = dict(compute_kwargs or {})
        if self._scheduler is not None and "scheduler" not in self._compute_kwargs:
            self._compute_kwargs["scheduler"] = self._scheduler

    def _engine_options_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "compute_kwargs": dict(self._compute_kwargs),
        }
        if self._scheduler is not None:
            payload["scheduler"] = self._scheduler
        payload["_default_user_email"] = self._default_user_email
        return payload

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, Delayed):
            if not from_socket:
                return upstream
            return delayed(get_nested_dict)(upstream, from_socket, default=None)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_whole_output(self, from_name: str, source_map: Dict[str, Any]) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, Delayed):
            return upstream
        return super()._link_whole_output(from_name, source_map)

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": task.spec.inputs.to_dict(),
            "_ng_task_outputs": task.spec.outputs.to_dict(),
            "_ng_engine_options": self._engine_options_payload(),
        }

        return EngineTaskExecutor(
            runner=_dask_node_submit,
            meta=meta,
            callable=executor,
            static_kwargs=static_kwargs,
        )

    def _resolve_values(self, values: Dict[str, Any]) -> Dict[str, Any]:
        resolved = dict(values)
        delayed_items = [
            (key, val) for key, val in values.items() if isinstance(val, Delayed)
        ]
        if delayed_items:
            keys, delayed_objs = zip(*delayed_items)
            results = compute(*delayed_objs, **self._compute_kwargs)
            for key, result in zip(keys, results):
                resolved[key] = result
        return resolved

    def run(
        self,
        ng: Graph,
        parent_pid: Optional[str] = None,
    ) -> Dict[str, Any]:
        context = prepare_graph_run(
            ng,
            parent_pid=parent_pid,
            user=load_default_user(self._default_user_email),
            encode_graph_inputs=True,
        )
        self._graph_pid = context.process_node.uuid
        values = context.values

        success = False
        try:
            for name in iterate_task_order(context.order):
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
                executor_obj = self._build_task_executor(
                    task,
                    label_kind=label_kind,
                )
                task = executor_obj.invoke(
                    parent_pid=context.process_node.uuid,
                    **kw,
                )
                values[name] = task

            resolved_values = self._resolve_values(values)
            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=resolved_values,
                link_builder=self._build_link_kwargs,
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
