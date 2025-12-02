"""Async request/trigger helpers for Airflow-based execution."""

from __future__ import annotations

import asyncio
import base64
import traceback
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional, Tuple

import cloudpickle
from airflow.triggers.base import BaseTrigger, TriggerEvent

from node_graph.executor import RuntimeExecutor

from node_graph_engine.core.remote_execution import _remote_task_job
from node_graph_engine.core.utils import (
    close_threadlocal_aiida_session,
    ensure_aiida_profile,
    get_default_user_email,
    load_default_user,
)

from .common import _sanitize_dag_id


@dataclass
class AsyncNodeExecutionRequest:
    """Serializable payload describing an async task execution."""

    parent_pid: Optional[str]
    meta: Dict[str, Any]
    callable_payload: Optional[Dict[str, Any]]
    runtime_inputs: Dict[str, Any]
    engine_name: str
    node_inputs: Optional[Dict[str, Any]]
    node_outputs: Optional[Dict[str, Any]]
    default_user_email: Optional[str]
    profile_name: Optional[str]
    sub_engine_config: Dict[str, Any]
    schedule_subgraphs: bool
    task_type: Optional[str] = None
    node_metadata: Optional[Dict[str, Any]] = None

    def serialize(self) -> str:
        blob = cloudpickle.dumps(self)
        return base64.b64encode(blob).decode("ascii")

    @classmethod
    def deserialize(cls, payload: str) -> "AsyncNodeExecutionRequest":
        data = base64.b64decode(payload.encode("ascii"))
        return cloudpickle.loads(data)

    def run(self) -> Dict[str, Any]:
        ensure_aiida_profile(self.profile_name)
        task_type_value = (self.task_type or "").lower()
        metadata = dict(self.node_metadata or {})
        runtime_inputs = dict(self.runtime_inputs)

        def _build_sub_engine(name: str) -> "AirflowEngine":
            from .engine import AirflowEngine

            sanitized = _sanitize_dag_id(name)
            return AirflowEngine(
                dag_id=sanitized,
                default_args=self.sub_engine_config.get("default_args"),
                schedule=self.sub_engine_config.get("schedule"),
                start_date=self.sub_engine_config.get("start_date"),
                catchup=self.sub_engine_config.get("catchup", False),
                max_active_runs=self.sub_engine_config.get("max_active_runs", 1),
                _default_user_email=self.default_user_email,
                schedule_subgraphs=self.sub_engine_config.get(
                    "schedule_subgraphs", self.schedule_subgraphs
                ),
            )

        try:
            if task_type_value == "remotefunction":
                return _remote_task_job(
                    parent_pid=self.parent_pid,
                    _ng_meta=self.meta,
                    _ng_callable=self.callable_payload,
                    _ng_engine_name=self.engine_name,
                    _ng_task_inputs=self.node_inputs,
                    _ng_task_outputs=self.node_outputs,
                    _ng_task_metadata=metadata,
                    _ng_default_user_email=self.default_user_email
                    or get_default_user_email(),
                    **runtime_inputs,
                )
            # normal job will run in a separate process, so we need to load the user again
            user = load_default_user(self.default_user_email)
            from node_graph_engine.core.execution import execute_task_job

            return execute_task_job(
                parent_pid=self.parent_pid,
                meta=self.meta,
                callable_payload=self.callable_payload,
                runtime_inputs=runtime_inputs,
                engine_name=self.engine_name,
                node_inputs=self.node_inputs,
                node_outputs=self.node_outputs,
                build_sub_engine=_build_sub_engine,
                user=user,
                schedule_subgraphs=self.schedule_subgraphs,
            )
        finally:
            close_threadlocal_aiida_session()


class GraphAsyncTrigger(BaseTrigger):
    """Trigger used to resume a deferred async task execution."""

    def __init__(self, *, payload: str):
        super().__init__()
        self.payload = payload

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "node_graph_engine.engines.airflow.GraphAsyncTrigger",
            {"payload": self.payload},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        try:
            request = AsyncNodeExecutionRequest.deserialize(self.payload)
            result = await asyncio.to_thread(request.run)
        except Exception:
            message = traceback.format_exc()
            yield TriggerEvent({"status": "error", "message": message})
            return

        yield TriggerEvent({"status": "success", "result": result})


def _callable_is_async(callable_payload: Optional[Dict[str, Any]]) -> bool:
    """Return ``True`` when the runtime callable is asynchronous."""

    if not callable_payload:
        return False

    try:
        runtime_callable = RuntimeExecutor(**callable_payload).callable
    except Exception:
        return False

    return asyncio.iscoroutinefunction(runtime_callable)
