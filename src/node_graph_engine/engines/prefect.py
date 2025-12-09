from __future__ import annotations

from typing import Any, Callable, Dict, Optional
import inspect

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from node_graph import Graph

from ..core.base import BaseEngine
from ..core.execution import (
    compute_graph_outputs,
    execute_task_job,
    mark_process_failure,
    mark_process_success,
    prepare_graph_run,
)
from ..core.semantics import record_graph_semantics
from ..core.remote_execution import _remote_task_job
from ..core.task import EngineTaskExecutor, TaskMeta
from ..core.utils import (
    update_nested_dict_with_special_keys,
    get_nested_dict,
    _collect_literals,
    _is_encoded_tagged,
    _scan_links_topology,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..orm.data.knowledge_graph import persist_workflow_knowledge_graph
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.futures import PrefectFuture
from prefect.states import State
from prefect.utilities.asyncutils import sync as _prefect_sync
from aiida import orm
from node_graph.graph import BUILTIN_TASKS


@task(name="ng:get_nested", cache_policy=NO_CACHE)
def _prefect_get_nested(d: Dict[str, Any], dotted: str, default=None):
    return get_nested_dict(d, dotted, default=default)


@task(name="task:generic", cache_policy=NO_CACHE)
def _node_task(
    _ng_meta: TaskMeta,
    _ng_callable: Optional[Callable] = None,
    _ng_engine_name: Optional[str] = None,
    _ng_task_inputs: Optional[Dict[str, Any]] = None,
    _ng_task_outputs: Optional[Dict[str, Any]] = None,
    _ng_task_runner=None,
    _ng_flow_name: Optional[str] = None,
    _ng_use_analysis: bool = False,
    _ng_parent_pid: Optional[str] = None,
    _ng_default_user_email: Optional[str] = None,
    _ng_task_type: Optional[str] = None,
    _ng_task_metadata: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
):
    flow_base = _ng_flow_name or _ng_engine_name or "prefect-flow"

    def _build_sub_engine(name: str) -> "PrefectEngine":
        return PrefectEngine(
            flow_name=name,
            task_runner=_ng_task_runner,
            use_analysis=_ng_use_analysis,
            _default_user_email=_ng_default_user_email,
        )

    try:
        if (_ng_task_type or "").lower() == "remotefunction":
            return _remote_task_job(
                parent_pid=_ng_parent_pid,
                _ng_meta=_ng_meta,
                _ng_callable=_ng_callable,
                _ng_engine_name=flow_base,
                _ng_task_inputs=_ng_task_inputs,
                _ng_task_outputs=_ng_task_outputs,
                _ng_task_metadata=dict(_ng_task_metadata or {}),
                _ng_default_user_email=_ng_default_user_email,
                **kwargs,
            )
        user = load_default_user(_ng_default_user_email)
        return execute_task_job(
            parent_pid=_ng_parent_pid,
            meta=_ng_meta,
            callable_payload=_ng_callable,
            runtime_inputs=kwargs,
            engine_name=flow_base,
            node_inputs=_ng_task_inputs,
            node_outputs=_ng_task_outputs,
            build_sub_engine=_build_sub_engine,
            user=user,
        )
    finally:
        close_threadlocal_aiida_session()


def _prefect_node_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_task: Optional[Any] = None,
    _ng_engine_name: Optional[str] = None,
    _ng_default_user_email: Optional[str] = None,
    **kwargs: Any,
):
    if _ng_task is None:
        raise RuntimeError("Prefect task handle is not available for execution")

    submit_kwargs = dict(kwargs)
    submit_kwargs.update(
        {
            "_ng_meta": _ng_meta,
            "_ng_callable": _ng_callable,
            "_ng_parent_pid": parent_pid,
            "_ng_engine_name": _ng_engine_name,
            "_ng_default_user_email": _ng_default_user_email,
        }
    )
    return _ng_task.submit(**submit_kwargs)


class PrefectEngine(BaseEngine):
    """
    Prefect engine for Graph.
      - Builds a Flow from nodes/links with Kahn topological order.
      - Supports multi-fan-in by bundling into a dict with "{fromNode}_{fromSocket}" keys.
    """

    engine_kind = "prefect"

    def __init__(
        self,
        flow_name: str = "node-graph-flow",
        use_analysis: bool = False,
        task_runner=None,
        *,
        _default_user_email: Optional[str] = None,
    ):
        super().__init__(flow_name)
        self.flow_name = flow_name
        self.use_analysis = use_analysis
        self.task_runner = task_runner or ThreadPoolTaskRunner()
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, PrefectFuture):
            return _prefect_get_nested.submit(upstream, from_socket, default=None)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)
        task_type = getattr(task.spec, "task_type", "") or ""
        metadata = getattr(task.spec, "metadata", {}) or {}
        task_obj = _node_task.with_options(name=f"task:{task.name}")
        static_kwargs = {
            "_ng_engine_name": self.flow_name,
            "_ng_task_inputs": (task.spec.inputs.to_dict() if task.spec.inputs else {}),
            "_ng_task_outputs": (
                task.spec.outputs.to_dict() if task.spec.outputs else {}
            ),
            "_ng_task_runner": self.task_runner,
            "_ng_flow_name": self.flow_name,
            "_ng_use_analysis": self.use_analysis,
            "_ng_task": task_obj,
            "_ng_default_user_email": self._default_user_email,
            "_ng_task_type": task_type,
        }
        if metadata:
            static_kwargs["_ng_task_metadata"] = dict(metadata)
        return EngineTaskExecutor(
            runner=_prefect_node_job,
            meta=meta,
            callable=executor,
            static_kwargs=static_kwargs,
        )

    def to_flow(self, ng: Graph, values: Dict[str, Any]):
        order, incoming, required_out_sockets = _scan_links_topology(ng)

        executors: Dict[str, EngineTaskExecutor] = {}
        for name in ng.get_task_names():
            if name in BUILTIN_TASKS:
                continue
            task = ng.tasks[name]
            task_type = getattr(task.spec, "task_type", "") or ""
            label_kind = "return" if task_type.upper() == "GRAPH" else "create"
            executors[name] = self._build_task_executor(task, label_kind)

        @flow(name=self.flow_name, task_runner=self.task_runner)  # <-- concurrency ON
        def adapted_flow():
            literals = {n: _collect_literals(ng.tasks[n]) for n in ng.get_task_names()}

            all_task_future: Dict[str, Any] = values

            for n in order:
                if n in BUILTIN_TASKS:
                    continue

                kw = dict(literals[n])

                kw.update(
                    self._build_link_kwargs(
                        target_name=n,
                        links=incoming.get(n, []),
                        source_map=all_task_future,
                    )
                )
                kw = update_nested_dict_with_special_keys(kw)

                # collect explicit wait deps from _wait edges
                wait_deps = []
                for lk in incoming.get(n, []):
                    if lk.from_socket._scoped_name == "_wait":
                        # depend on the WHOLE upstream task dict future
                        up = all_task_future.get(lk.from_task.name)
                        if up is not None:
                            wait_deps.append(up)

                # schedule with explicit dependencies (does not block others)
                executor = executors[n]
                node_future = executor.invoke(
                    parent_pid=self._graph_pid,
                    **kw,
                )
                all_task_future[n] = node_future

            return all_task_future

        return adapted_flow

    def run(self, ng: Graph, parent_pid: Optional[str] = None) -> Dict[str, Any]:
        """Build the flow and execute it synchronously; returns mapping of Prefect futures."""
        context = prepare_graph_run(
            ng,
            parent_pid=parent_pid,
            user=load_default_user(self._default_user_email),
            encode_graph_inputs=True,
        )
        self._graph_pid = context.process_node.uuid
        values = context.values
        flow_fn = self.to_flow(ng, values)
        success = False
        try:
            state_map = flow_fn()
            print("state_map:", state_map)
            values.update(
                {name: self._resolve_state(value) for name, value in state_map.items()}
            )
            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values=values,
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
            if success:
                persist_workflow_knowledge_graph(
                    process_node=context.process_node,
                    graph=context.ng,
                    engine_kind=self.engine_kind,
                )
            context.process_node.seal()

    def _resolve_state(self, value: Any) -> Any:
        """Resolve Prefect futures/states to their Python values.

        Handles:
        - PrefectFuture: unwrap via .result()
        - State (incl. Completed/Failed/etc.): unwrap via .result()
        - Containers (dict/list/tuple): recurse
        - Encoded/tagged and AiiDA Data: return as-is

        Raises TypeError only for clearly unsupported, unresolved types.
        """
        # Unwrap Prefect artifacts until we get to plain Python data
        if isinstance(value, PrefectFuture):
            res = value.result()
            if inspect.isawaitable(res):
                if hasattr(value, "aresult"):
                    res = _prefect_sync(value.aresult)
            return self._resolve_state(res)

        if isinstance(value, State):  # covers Completed/Failed/etc.
            res = value.result()
            if inspect.isawaitable(res):
                if hasattr(value, "aresult"):
                    res = _prefect_sync(value.aresult)
            return self._resolve_state(res)

        if isinstance(value, orm.Data):
            return value
        if _is_encoded_tagged(value):
            return value
        if isinstance(value, dict):
            return {k: self._resolve_state(v) for k, v in value.items()}
        raise TypeError(f"Cannot resolve Prefect state for value: {value}")
