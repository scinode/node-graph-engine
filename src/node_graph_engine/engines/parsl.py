from __future__ import annotations

from typing import Any, Dict, Optional

import parsl
from aiida import orm
from parsl import load
from parsl.app.app import AppFuture, python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

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
from ..core.semantics import finalize_pending_semantics, record_graph_semantics
from ..core.task import EngineTaskExecutor
from ..core.utils import (
    _collect_literals,
    update_nested_dict_with_special_keys,
    _is_encoded_tagged,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..orm.data.knowledge_graph import persist_workflow_knowledge_graph


@python_app
def _parsl_get_nested(d: Dict[str, Any], dotted: str, default=None) -> Any:
    from node_graph_engine.core.utils import get_nested_dict

    return get_nested_dict(d, dotted, default=default)


@python_app
def _parsl_bundle(**kwargs: Any) -> Dict[str, Any]:
    return kwargs


def _resolve_app_future_value(value: Any) -> Any:
    if AppFuture is not None and isinstance(value, AppFuture):
        return value.result()
    if isinstance(value, orm.Data):
        return value
    if _is_encoded_tagged(value):
        return value
    if isinstance(value, dict):
        return {k: _resolve_app_future_value(v) for k, v in value.items()}
    raise ValueError(f"Unsupported value type for resolving AppFuture: {type(value)}")


def _default_config() -> "Config":
    if Config is None:
        raise RuntimeError("Parsl is not installed.")
    return Config(
        executors=[ThreadPoolExecutor(max_threads=4)],
        strategy=None,
    )


def _ensure_dfk(config: "Config") -> "parsl.dataflow.dflow.DataFlowKernel":
    assert parsl is not None
    try:
        return parsl.dfk()
    except Exception:
        return load(config)


@python_app
def _node_app(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_config=None,
    _ng_dfk=None,
    _ng_default_user_email: Optional[str] = None,
    **kwargs: Any,
):

    user = load_default_user(_ng_default_user_email) if _ng_default_user_email else None

    def _build_sub_engine(name: str) -> "ParslEngine":
        return ParslEngine(
            name=name,
            config=_ng_config,
            dfk=_ng_dfk,
            _default_user_email=_ng_default_user_email,
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


class ParslEngine(BaseEngine):
    """
    Run Graphs using Parsl python apps while recording provenance.
    """

    engine_kind = "parsl"

    def __init__(
        self,
        name: str = "parsl-flow",
        *,
        config: Optional["Config"] = None,
        dfk: Optional["parsl.dataflow.dflow.DataFlowKernel"] = None,
        _default_user_email: Optional[str] = None,
    ):
        if parsl is None:
            raise RuntimeError(
                "Parsl is not installed. Install `parsl` to use ParslEngine."
            )
        super().__init__(name)
        self.config = config or _default_config()
        self._dfk = dfk
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, AppFuture):
            return _parsl_get_nested(upstream, from_socket, default=None)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_bundle(self, payload: Dict[str, Any]) -> Any:
        return _parsl_bundle(**payload)

    def _build_task_executor(self, task, label_kind: str) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": task.spec.inputs.to_dict(),
            "_ng_task_outputs": task.spec.outputs.to_dict(),
            "_ng_config": self.config,
            "_ng_dfk": self._dfk,
            "_ng_default_user_email": self._default_user_email,
        }

        return EngineTaskExecutor(
            runner=_node_app,
            meta=meta,
            callable=executor,
            static_kwargs=static_kwargs,
        )

    def _ensure_runtime(self):
        if self._dfk is None:
            self._dfk = _ensure_dfk(self.config)

    def run(
        self,
        ng: Graph,
        parent_pid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute ``ng`` and return resolved graph outputs."""
        self._ensure_runtime()

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

                executor = self._build_task_executor(task, label_kind=label_kind)
                future = executor.invoke(
                    parent_pid=context.process_node.uuid,
                    **kw,
                )
                values[name] = future

            resolved_values = _resolve_app_future_value(values)
            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=resolved_values,
                link_builder=self._build_link_kwargs,
            )
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
            if success:
                persist_workflow_knowledge_graph(
                    process_node=context.process_node,
                    graph=context.ng,
                    engine_kind=self.engine_kind,
                )
            context.process_node.seal()
