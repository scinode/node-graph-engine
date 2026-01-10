from __future__ import annotations

import re
from typing import Any, Dict, Optional

from aiida import orm
from node_graph import Graph
from redun import task as redun_task
from redun.config import Config as RedunConfig
from redun.scheduler import Scheduler, TaskExpression

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
    update_nested_dict_with_special_keys,
    get_nested_dict,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..neo4j.knowledge_graph import persist_workflow_knowledge_graph


_TASK_NAME_SANITIZE_RE = re.compile(r"[^0-9A-Za-z_]")
_GRAPH_OUTPUTS_KEY = "graph_outputs"


def _sanitize_task_name(*parts: str) -> str:
    name = "_".join(parts)
    return _TASK_NAME_SANITIZE_RE.sub("_", name)


def _node_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_config=None,
    _ng_default_user_email: Optional[str] = None,
    _ng_preserve_session: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    user = load_default_user(_ng_default_user_email) if _ng_default_user_email else None

    def _build_sub_engine(name: str) -> "RedunEngine":
        return RedunEngine(
            name=name,
            config=_ng_config,
            _default_user_email=_ng_default_user_email,
            _preserve_session=_ng_preserve_session,
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
        if not _ng_preserve_session:
            close_threadlocal_aiida_session()


@redun_task(name=_sanitize_task_name("ng", "get_nested"), cache=False)
def _redun_get_nested(d: Dict[str, Any], dotted: str, default=None):
    return get_nested_dict(d, dotted, default=default)


@redun_task(name=_sanitize_task_name("ng", "bundle"), cache=False)
def _redun_bundle(**kwargs: Any) -> Dict[str, Any]:
    return dict(kwargs)


def _default_config() -> "RedunConfig":
    if RedunConfig is None:
        raise RuntimeError("redun is required to use RedunEngine.")
    return RedunConfig(
        {
            "scheduler": {
                "backend": "sqlite",
                "db_uri": "sqlite:///:memory:",
            }
        }
    )


class RedunEngine(BaseEngine):
    """Execute Graphs using redun's scheduler while recording provenance."""

    engine_kind = "redun"

    def __init__(
        self,
        name: str = "redun-flow",
        *,
        config: Optional["RedunConfig"] = None,
        scheduler: Optional["Scheduler"] = None,
        _default_user_email: Optional[str] = None,
        _preserve_session: bool = True,
    ):
        if Scheduler is None or redun_task is None:
            raise RuntimeError(
                "redun is not installed. Install `redun` to use RedunEngine."
            )
        super().__init__(name)
        self.config = config or _default_config()
        self.scheduler = scheduler or Scheduler(config=self.config)
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email
        self._preserve_session = _preserve_session

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, dict):
            return get_nested_dict(upstream, from_socket, default=None)
        if isinstance(upstream, TaskExpression):
            return _redun_get_nested(upstream, from_socket, default=None)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_bundle(self, payload: Dict[str, Any]) -> Any:
        return _redun_bundle(**payload)

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)

        task_name = _sanitize_task_name("task", self.name, task.name)

        runner = redun_task(name=task_name, cache=False)(_node_job)

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": task.spec.inputs.to_dict(),
            "_ng_task_outputs": task.spec.outputs.to_dict(),
            "_ng_config": self.config,
            "_ng_default_user_email": self._default_user_email,
            "_ng_preserve_session": self._preserve_session,
        }

        return EngineTaskExecutor(
            runner=runner,
            meta=meta,
            callable=executor,
            static_kwargs=static_kwargs,
        )

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
        process_uuid = context.process_node.uuid
        self._graph_pid = process_uuid
        process_node: Optional[orm.WorkflowNode] = context.process_node
        values = context.values

        success = False
        try:
            task_exprs: Dict[str, Any] = {}
            for name in iterate_task_order(context.order):
                task = ng.tasks[name]
                label_kind = "return" if self._is_graph_task(task) else "create"
                executor = self._build_task_executor(task, label_kind=label_kind)

                kw = dict(_collect_literals(task))
                link_kwargs = self._build_link_kwargs(
                    target_name=name,
                    links=context.incoming.get(name, []),
                    source_map=values,
                )
                kw.update(link_kwargs)
                kw = update_nested_dict_with_special_keys(kw)

                task_expression = executor.invoke(
                    parent_pid=context.process_node.uuid,
                    **kw,
                )
                values[name] = task_expression
                if isinstance(task_expression, TaskExpression):
                    task_exprs[name] = task_expression

            bundle_items: Dict[str, Any] = dict(task_exprs)
            graph_kwargs = self._build_link_kwargs(
                target_name="graph_outputs",
                links=context.incoming.get("graph_outputs", []),
                source_map=values,
            )
            graph_kwargs = update_nested_dict_with_special_keys(graph_kwargs)
            if graph_kwargs:
                bundle_items[_GRAPH_OUTPUTS_KEY] = _redun_bundle(**graph_kwargs)

            resolved: Dict[str, Any] = {}
            if bundle_items:
                final_expr = _redun_bundle(**bundle_items)
                resolved = self._execute_expression(final_expr, cache=False) or {}

            graph_outputs_payload = (
                resolved.pop(_GRAPH_OUTPUTS_KEY, {}) if resolved else {}
            )
            for name, result in resolved.items():
                values[name] = result
            if graph_outputs_payload:
                values["graph_outputs"] = graph_outputs_payload

            process_node = orm.load_node(process_uuid)

            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=values,
                link_builder=self._build_link_kwargs,
            )
            mark_process_success(process_node, graph_outputs)
            success = True
            return graph_outputs
        except Exception as e:
            process_node = orm.load_node(process_uuid)
            mark_process_failure(process_node, e)
            raise
        finally:
            try:
                if success:
                    persist_workflow_knowledge_graph(
                        process_node=process_node,
                        graph=ng,
                        engine_kind=self.engine_kind,
                    )
                process_node.seal()
            except Exception:
                pass
            if not self._preserve_session:
                close_threadlocal_aiida_session()

    def _execute_expression(self, expr, cache: bool):
        if Scheduler is None:
            raise RuntimeError(
                "redun is not installed. Install `redun` to use RedunEngine."
            )
        backend = getattr(self.scheduler, "backend", None)
        import types

        patched_methods = []
        if not cache and backend is not None:
            if hasattr(backend, "set_eval_cache"):
                original_set_eval_cache = backend.set_eval_cache

                def _noop_set_eval_cache(_self, *args, **kwargs):
                    return None

                backend.set_eval_cache = types.MethodType(_noop_set_eval_cache, backend)
                patched_methods.append(("set_eval_cache", original_set_eval_cache))

            if hasattr(backend, "record_value"):
                original_record_value = backend.record_value

                def _skip_record_value(_self, *args, **kwargs):
                    return None

                backend.record_value = types.MethodType(_skip_record_value, backend)
                patched_methods.append(("record_value", original_record_value))

            if hasattr(backend, "record_call_node"):
                original_record_call_node = backend.record_call_node

                def _skip_record_call_node(_self, *args, **kwargs):
                    return None

                backend.record_call_node = types.MethodType(
                    _skip_record_call_node, backend
                )
                patched_methods.append(("record_call_node", original_record_call_node))

        try:
            try:
                return self.scheduler.run(expr, cache=cache)
            except TypeError:
                try:
                    return self.scheduler.run(expr, cache=cache)
                except TypeError:
                    return self.scheduler.run(expr)
        finally:
            for name, original in patched_methods:
                setattr(backend, name, original)
