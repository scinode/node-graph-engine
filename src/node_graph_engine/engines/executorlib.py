from __future__ import annotations

from concurrent.futures import Future
from contextlib import nullcontext
from typing import Any, Dict, Optional

from aiida import orm, load_profile
from aiida.common import exceptions as aiida_exceptions
from aiida.manage.manager import get_manager
from executorlib import SingleNodeExecutor, get_item_from_future
from executorlib.standalone.select import FutureSelector
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
    update_nested_dict_with_special_keys,
    _is_encoded_tagged,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..neo4j.knowledge_graph import persist_workflow_knowledge_graph


def _is_future_like(value: Any) -> bool:
    if isinstance(value, Future):
        return True
    if FutureSelector is not None and isinstance(value, FutureSelector):
        return True
    return hasattr(value, "result") and callable(getattr(value, "result"))


def _future_path_selector(future: Any, socket: str) -> Any:
    if not socket:
        return future
    current = future
    for part in socket.split("."):
        current = get_item_from_future(current, part)
    return current


def _resolve_future_payload(value: Any) -> Any:
    if isinstance(value, orm.Data):
        return value
    if _is_encoded_tagged(value):
        return value
    if _is_future_like(value):
        return value.result()
    if isinstance(value, dict):
        return {k: _resolve_future_payload(v) for k, v in value.items()}
    raise ValueError(f"Unsupported value type for resolving AppFuture: {type(value)}")


def _ensure_aiida_profile_loaded() -> None:
    """Ensure an AiiDA profile is loaded before accessing ORM resources."""

    manager = get_manager()
    if manager.get_profile() is not None:
        return

    try:
        load_profile()
    except Exception as exc:
        raise aiida_exceptions.ConfigurationError(
            "ExecutorlibEngine requires an AiiDA profile. "
            "Call `aiida.load_profile()` before using the engine or configure a default profile."
        ) from exc


def _node_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_default_user_email: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:

    _ensure_aiida_profile_loaded()

    user = load_default_user(_ng_default_user_email) if _ng_default_user_email else None

    def _build_sub_engine(name: str) -> "ExecutorlibEngine":
        return ExecutorlibEngine(
            name=name,
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


def _executorlib_node_submit(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_default_user_email: Optional[str] = None,
    **kwargs: Any,
):
    submit_kwargs = dict(kwargs)
    engine_name = submit_kwargs.pop("_ng_engine_name", None) or "executorlib"
    submit_kwargs.update(
        {
            "parent_pid": parent_pid,
            "_ng_meta": _ng_meta,
            "_ng_callable": _ng_callable,
            "_ng_engine_name": engine_name,
            "_ng_default_user_email": _ng_default_user_email,
        }
    )
    executor = submit_kwargs.pop("_ng_executor_ref")
    return executor.submit(_node_job, **submit_kwargs)


class ExecutorlibEngine(BaseEngine):
    """Run Graphs using executorlib executors with provenance tracking."""

    engine_kind = "executorlib"

    def __init__(
        self,
        name: str = "executorlib-flow",
        *,
        executor: Optional[Any] = None,
        manage_executor: Optional[bool] = None,
        max_workers: Optional[int] = None,
        _default_user_email: Optional[str] = None,
    ) -> None:
        if get_item_from_future is None or SingleNodeExecutor is None:
            raise RuntimeError(
                "executorlib is not installed. Install `executorlib` to use ExecutorlibEngine."
            )
        super().__init__(name)
        _ensure_aiida_profile_loaded()
        self._provided_executor = executor
        if manage_executor is None:
            manage_executor = executor is None
        self._manage_executor = manage_executor
        self._max_workers = max_workers
        self._runtime_executor: Optional[Any] = None
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email

    def _create_executor(self) -> Any:
        if SingleNodeExecutor is None:
            raise RuntimeError(
                "executorlib is not installed. Install `executorlib` to use ExecutorlibEngine."
            )
        return SingleNodeExecutor(max_workers=self._max_workers)

    def _executor_context(self):
        if not self._manage_executor:
            if self._provided_executor is None:
                raise RuntimeError(
                    "No executor instance available for ExecutorlibEngine"
                )
            return nullcontext(self._provided_executor)
        executor = self._create_executor()
        if hasattr(executor, "__enter__"):
            return executor
        return nullcontext(executor)

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if _is_future_like(upstream):
            return _future_path_selector(upstream, from_socket)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_whole_output(self, from_name: str, source_map: Dict[str, Any]) -> Any:
        upstream = source_map[from_name]
        if _is_future_like(upstream):
            return upstream
        return super()._link_whole_output(from_name, source_map)

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        if self._runtime_executor is None:
            raise RuntimeError("Executorlib runtime executor has not been initialised")
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": task.spec.inputs.to_dict(),
            "_ng_task_outputs": task.spec.outputs.to_dict(),
            "_ng_executor_ref": self._runtime_executor,
            "_ng_default_user_email": self._default_user_email,
        }

        return EngineTaskExecutor(
            runner=_executorlib_node_submit,
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
        self._graph_pid = context.process_node.uuid
        values = context.values

        success = False
        try:
            with self._executor_context() as executor:
                self._runtime_executor = executor

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
                    future = executor_obj.invoke(
                        parent_pid=context.process_node.uuid,
                        **kw,
                    )
                    values[name] = future
                resolved_values = _resolve_future_payload(values)
                graph_outputs = compute_graph_outputs(
                    incoming=context.incoming,
                    values=resolved_values,
                    link_builder=self._build_link_kwargs,
                )
                mark_process_success(context.process_node, graph_outputs)
                success = True
                return graph_outputs
        except Exception as e:
            mark_process_failure(context.process_node, e)
            raise
        finally:
            self._runtime_executor = None
            if success:
                persist_workflow_knowledge_graph(
                    process_node=context.process_node,
                    graph=context.ng,
                    engine_kind=self.engine_kind,
                )
            context.process_node.seal()
