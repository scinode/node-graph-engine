from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Optional, Tuple

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
    update_nested_dict_with_special_keys,
    get_nested_dict,
    _collect_literals,
    close_threadlocal_aiida_session,
    get_default_user_email,
    load_default_user,
)
from ..orm.data.knowledge_graph import persist_workflow_knowledge_graph

from jobflow import Flow, job, run_locally
from jobflow.core.job import Job
from jobflow.core.reference import OutputReference
from aiida import orm


@job(name="jobflow_bundle")
def _jobflow_bundle(**kwargs: Any) -> Dict[str, Any]:
    return kwargs


@job(name="jobflow_get_nested")
def _jobflow_get_nested(d: Dict[str, Any], dotted: str, default=None):
    return get_nested_dict(d, dotted, default=default)


def _node_job(
    parent_pid: Optional[str],
    _ng_meta: dict,
    _ng_callable,
    _ng_engine_name: str,
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_default_user_email: Optional[str] = None,
    _ng_preserve_session: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    user = load_default_user(_ng_default_user_email)
    try:
        return execute_task_job(
            parent_pid=parent_pid,
            meta=_ng_meta,
            callable_payload=_ng_callable,
            runtime_inputs=kwargs,
            engine_name=_ng_engine_name,
            node_inputs=_ng_task_inputs,
            node_outputs=_ng_task_outputs,
            build_sub_engine=lambda name: JobflowEngine(
                name=name,
                _default_user_email=_ng_default_user_email,
                _preserve_session=_ng_preserve_session,
            ),
            user=user,
        )
    finally:
        if not _ng_preserve_session:
            close_threadlocal_aiida_session()


class JobflowEngine(BaseEngine):
    """Execute Graphs using jobflow while recording provenance."""

    engine_kind = "jobflow"

    def __init__(
        self,
        name: str = "jobflow-flow",
        *,
        _default_user_email: Optional[str] = None,
        _preserve_session: bool = True,
    ):
        super().__init__(name)
        self._link_jobs: Dict[Tuple[str, str], Job] = {}
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email
        self._preserve_session = _preserve_session

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        upstream = source_map[from_name]
        if isinstance(upstream, Job):
            upstream = upstream.output
        if isinstance(upstream, OutputReference):
            key = (from_name, from_socket)
            nested_job = self._link_jobs.get(key)
            if nested_job is None:
                nested_job = _jobflow_get_nested(upstream, from_socket, default=None)
                self._link_jobs[key] = nested_job
            return nested_job.output
        if isinstance(upstream, dict):
            return get_nested_dict(upstream, from_socket, default=None)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_bundle(self, payload: Dict[str, Any]) -> Any:
        return _jobflow_bundle(**payload)

    def _build_task_executor(self, task, label_kind: str) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)

        job_name = f"{task.name}"

        if executor is None:
            raise ValueError(
                f"Cannot build executor for task {task.name} without a callable."
            )
        else:
            runner = job(_node_job, name=job_name)
            static_kwargs = {
                "_ng_engine_name": self.name,
                "_ng_task_inputs": task.spec.inputs.to_dict(),
                "_ng_task_outputs": task.spec.outputs.to_dict(),
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
        job_map: Dict[str, Job] = {}

        success = False
        try:
            self._link_jobs = {}
            for name in iterate_task_order(context.order):
                task = ng.tasks[name]
                label_kind = "return" if self._is_graph_task(task) else "create"
                executor = self._build_task_executor(task, label_kind=label_kind)

                kw = dict(_collect_literals(task))
                link_kwargs = self._compile_link_payloads(
                    target_name=name,
                    links=context.incoming.get(name, []),
                    source_map=values,
                )
                kw.update(link_kwargs)
                kw = update_nested_dict_with_special_keys(kw)

                job_obj = executor.invoke(
                    parent_pid=context.process_node.uuid,
                    **kw,
                )
                if isinstance(job_obj, Job):
                    job_map[name] = job_obj
                    values[name] = job_obj
                else:
                    values[name] = job_obj
                    continue

            flow_jobs = list(job_map.values())
            if self._link_jobs:
                flow_jobs.extend(self._link_jobs.values())

            flow = Flow(flow_jobs, name=self.engine_kind)
            run_locally(flow)
            resolved_outputs: Dict[str, Any] = {}
            process_node = orm.load_node(process_uuid)
            for called in process_node.called:
                if called.process_label in job_map:
                    values[called.process_label] = called.outputs
            graph_outputs = compute_graph_outputs(
                incoming=context.incoming,
                values={**values, **resolved_outputs},
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

    def _compile_link_payloads(
        self,
        target_name: str,
        links,
        source_map: Dict[str, Any],
    ) -> Dict[str, Any]:
        grouped = defaultdict(list)
        for lk in links:
            if lk.to_task.name == target_name:
                grouped[lk.to_socket._scoped_name].append(lk)

        payloads: Dict[str, Any] = {}
        for to_sock, lks in grouped.items():
            active_links = [lk for lk in lks if lk.from_socket._scoped_name != "_wait"]
            if not active_links:
                continue

            if len(active_links) == 1:
                lk = active_links[0]
                from_name = lk.from_task.name
                from_sock = lk.from_socket._scoped_name
                if from_sock == "_outputs":
                    payloads[to_sock] = self._link_whole_output(from_name, source_map)
                else:
                    payloads[to_sock] = self._link_socket_value(
                        from_name, from_sock, source_map
                    )
                continue

            bundle_payload: Dict[str, Any] = {}
            for lk in active_links:
                from_name = lk.from_task.name
                from_sock = lk.from_socket._scoped_name
                if from_sock in ("_wait", "_outputs"):
                    continue
                key = f"{from_name}_{from_sock}"
                bundle_payload[key] = self._link_socket_value(
                    from_name, from_sock, source_map
                )
            if bundle_payload:
                payloads[to_sock] = self._link_bundle(bundle_payload)

        return payloads
