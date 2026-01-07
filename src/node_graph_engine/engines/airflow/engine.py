"""Airflow engine entry point and DAG compilation."""

from __future__ import annotations

import inspect
import logging
import os
import uuid
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from node_graph import Graph
from node_graph.graph import BUILTIN_TASKS

from node_graph_engine.core.base import BaseEngine
from node_graph_engine.core.utils import (
    _collect_literals,
    _scan_links_topology,
    get_active_profile_name,
    get_default_user_email,
)

from .async_request import _callable_is_async
from .common import IncomingSpec, _sanitize_dag_id
from .operators import GraphPythonOperator
from .runtime import (
    airflow_finalize_task,
    airflow_init_task,
    airflow_node_task,
    airflow_while_check_task,
    airflow_while_precheck_task,
    airflow_if_precheck_task,
)
from .scheduler import (
    _build_scheduler_payload,
    _trigger_registered_dag,
    _poll_for_result,
    _poll_for_scheduled_result,
    _prepare_run_artifacts,
    _register_generated_dag,
    _render_generated_dag,
    _resolve_scheduler_paths,
    _write_dag_file,
)


@dataclass
class _CompiledDag:
    dag: DAG
    order: Tuple[str, ...]
    incoming: Dict[str, Any]
    incoming_specs: Dict[str, List[IncomingSpec]]
    task_configs: Dict[str, Dict[str, Any]]
    builtins: Dict[str, Any]


class AirflowEngine(BaseEngine):
    """Build Airflow DAGs from Graph workflows."""

    engine_kind = "airflow"

    def __init__(
        self,
        dag_id: str = "node-graph-dag",
        *,
        default_args: Optional[Dict[str, Any]] = None,
        schedule: Optional[Any] = None,
        start_date: Optional[Any] = None,
        catchup: bool = False,
        max_active_runs: int = 1,
        _default_user_email: Optional[str] = None,
        schedule_subgraphs: bool = False,
    ) -> None:
        super().__init__(dag_id)
        self.dag_id = dag_id
        self.default_args = dict(default_args) if default_args else {}
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        default_email = _default_user_email or get_default_user_email()
        self._default_user_email = default_email
        self.schedule_subgraphs = schedule_subgraphs
        self._profile_name = get_active_profile_name()

    def _compile(
        self,
        ng: Graph,
        *,
        base_values: Optional[Dict[str, Any]] = None,
        runtime_context_task_id: Optional[str] = None,
        schedule_subgraphs: Optional[bool] = None,
    ) -> _CompiledDag:
        if schedule_subgraphs is None:
            schedule_subgraphs = self.schedule_subgraphs

        order, incoming, _required = _scan_links_topology(ng)

        incoming_specs: Dict[str, List[IncomingSpec]] = {}
        for target, links in incoming.items():
            incoming_specs[target] = [
                {
                    "from": lk.from_task.name,
                    "from_socket": lk.from_socket._scoped_name,
                    "target": lk.to_task.name,
                    "target_socket": lk.to_socket._scoped_name,
                }
                for lk in links
            ]

        init_params = inspect.signature(DAG.__init__).parameters
        accepts_kwargs = any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in init_params.values()
        )

        dag_kwargs: Dict[str, Any] = {"dag_id": self.dag_id}

        def _add_param(name: str, value: Any) -> None:
            if accepts_kwargs or name in init_params:
                dag_kwargs[name] = value

        _add_param("default_args", dict(self.default_args))
        effective_start_date = self.start_date
        if effective_start_date is None and schedule_subgraphs:
            effective_start_date = datetime.now(timezone.utc) - timedelta(minutes=1)
        _add_param("catchup", self.catchup)
        if accepts_kwargs or "max_active_runs" in init_params:
            dag_kwargs["max_active_runs"] = self.max_active_runs
        elif "max_active_runs_per_dag" in init_params:
            dag_kwargs["max_active_runs_per_dag"] = self.max_active_runs
        if effective_start_date is not None:
            _add_param("start_date", effective_start_date)
        _add_param("is_paused_upon_creation", False)

        schedule_value = self.schedule
        if schedule_value is not None:
            if accepts_kwargs or "schedule" in init_params:
                dag_kwargs["schedule"] = schedule_value
            elif "schedule_interval" in init_params:
                dag_kwargs["schedule_interval"] = schedule_value
            elif "timetable" in init_params:
                dag_kwargs["timetable"] = schedule_value
            else:
                raise RuntimeError(
                    "Unsupported DAG signature: unable to determine schedule parameter"
                )

        dag = DAG(**dag_kwargs)

        builtin_snapshot = self._snapshot_builtins(ng)
        if base_values is None:
            base_values = builtin_snapshot
        else:
            merged_values = dict(builtin_snapshot)
            merged_values.update(base_values)
            base_values = merged_values
        task_configs: Dict[str, Dict[str, Any]] = {}
        tasks: Dict[str, "PythonOperator"] = {}
        while_zones: Dict[str, Any] = {}
        if_zones: Dict[str, Any] = {}
        for name in ng.get_task_names():
            task = ng.tasks[name]
            task_type = getattr(task.spec, "task_type", "") or ""
            if task_type.lower() == "while":
                while_zones[name] = task
            elif task_type.lower() == "if":
                if_zones[name] = task

        ctx_updates_by_task: Dict[str, List[Dict[str, str]]] = {}
        ctx_writers_by_key: Dict[str, List[str]] = {}
        ctx_readers_by_key: Dict[str, List[str]] = {}
        for link in incoming.get("graph_ctx", []):
            from_task = link.from_task.name
            if from_task in BUILTIN_TASKS:
                continue
            from_socket = link.from_socket._scoped_name
            if from_socket == "_wait":
                continue
            to_socket = link.to_socket._scoped_name
            ctx_updates_by_task.setdefault(from_task, []).append(
                {
                    "from": from_task,
                    "from_socket": from_socket,
                    "to_socket": to_socket,
                }
            )
            ctx_writers_by_key.setdefault(to_socket, []).append(from_task)

        for link in ng.links:
            if link.from_task.name != "graph_ctx":
                continue
            from_socket = link.from_socket._scoped_name
            to_task = link.to_task.name
            if to_task in BUILTIN_TASKS:
                continue
            ctx_readers_by_key.setdefault(from_socket, []).append(to_task)

        task_groups: Dict[str, TaskGroup] = {}
        zone_groups = dict(while_zones)
        zone_groups.update(if_zones)

        def _ensure_task_group(zone_name: str) -> TaskGroup:
            # Create TaskGroups for zones so nested tasks share a namespace in Airflow.
            if zone_name in task_groups:
                return task_groups[zone_name]
            zone_task = ng.tasks[zone_name]
            parent_group = None
            parent_task = getattr(zone_task, "parent", None)
            while parent_task is not None:
                parent_name = parent_task.name
                if parent_name in zone_groups:
                    parent_group = _ensure_task_group(parent_name)
                    break
                parent_task = getattr(parent_task, "parent", None)
            group = TaskGroup(group_id=zone_name, dag=dag, parent_group=parent_group)
            task_groups[zone_name] = group
            return group

        for zone_name in zone_groups:
            _ensure_task_group(zone_name)

        def _find_parent_zone(task_obj) -> Optional[str]:
            # Walk parent links to determine if a task is inside a zone.
            parent_task = getattr(task_obj, "parent", None)
            while parent_task is not None:
                if parent_task.name in zone_groups:
                    return parent_task.name
                parent_task = getattr(parent_task, "parent", None)
            return None

        def _collect_zone_descendants(zone_task) -> List[str]:
            # Gather non-zone tasks that live under a zone (used for wiring).
            collected: List[str] = []
            seen: set[str] = set()
            stack = list(getattr(zone_task, "children", []))
            while stack:
                child = stack.pop()
                child_task = child if hasattr(child, "name") else ng.tasks.get(child)
                if child_task is None:
                    continue
                child_name = child_task.name
                if child_name in seen:
                    continue
                seen.add(child_name)
                if child_name not in zone_groups:
                    collected.append(child_name)
                stack.extend(list(getattr(child_task, "children", [])))
            return collected

        for name in ng.get_task_names():
            if name in BUILTIN_TASKS:
                continue
            task = ng.tasks[name]
            task_type = getattr(task.spec, "task_type", "") or ""
            if task_type.lower() in ("while", "if"):
                # Zone tasks are handled via TaskGroups and explicit pre/tail checks.
                continue
            metadata = getattr(task.spec, "metadata", {}) or {}
            label_kind = "return" if task_type.upper() == "GRAPH" else "create"
            executor = getattr(task.spec, "executor", None)
            callable_payload = executor.to_dict() if executor is not None else None
            incoming_specs_for_node = incoming_specs.get(name, [])
            is_async_callable = _callable_is_async(callable_payload)
            if task_type.lower() == "remotefunction":
                is_async_callable = True
            if task_type.lower() == "graph" and schedule_subgraphs:
                # Treat sub-graph tasks as async so we can defer to a triggerer
                # instead of blocking a worker while waiting for the scheduler.
                is_async_callable = True
            op_kwargs = {
                "_ng_meta": self._build_node_task_meta(task, label_kind).as_dict(),
                "_ng_callable": callable_payload,
                "_ng_engine_name": self.dag_id,
                "_ng_task_inputs": (
                    task.spec.inputs.to_dict() if task.spec.inputs else None
                ),
                "_ng_task_outputs": (
                    task.spec.outputs.to_dict() if task.spec.outputs else None
                ),
                "_ng_incoming": incoming_specs_for_node,
                "_ng_literals": _collect_literals(task),
                "_ng_default_user_email": self._default_user_email,
                "_ng_engine_config": {
                    "default_args": self.default_args,
                    "schedule": self.schedule,
                    "start_date": self.start_date,
                    "catchup": self.catchup,
                    "max_active_runs": self.max_active_runs,
                    "schedule_subgraphs": schedule_subgraphs,
                },
                "_ng_schedule_subgraphs": schedule_subgraphs,
                "_ng_profile_name": self._profile_name,
                "_ng_is_async": is_async_callable,
                "_ng_task_type": task_type,
            }
            ctx_updates = ctx_updates_by_task.get(name)
            if ctx_updates:
                op_kwargs["_ng_ctx_updates"] = list(ctx_updates)
            if metadata:
                op_kwargs["_ng_task_metadata"] = dict(metadata)
            if runtime_context_task_id is not None:
                op_kwargs["_ng_runtime_context_task_id"] = runtime_context_task_id
            else:
                op_kwargs["_ng_base_values"] = base_values

            task_group = None
            parent_zone = _find_parent_zone(task)
            if parent_zone:
                task_group = task_groups.get(parent_zone)

            task = GraphPythonOperator(
                task_id=name,
                dag=dag,
                task_group=task_group,
                python_callable=airflow_node_task,
                op_kwargs=op_kwargs,
                is_async=is_async_callable,
            )
            tasks[name] = task
            upstream_ids = {
                lk.from_task.name
                for lk in incoming.get(name, [])
                if lk.from_task.name not in BUILTIN_TASKS
            }
            task_configs[name] = {
                "meta": op_kwargs["_ng_meta"],
                "callable": callable_payload,
                "literals": op_kwargs["_ng_literals"],
                "incoming": incoming_specs_for_node,
                "node_inputs": op_kwargs["_ng_task_inputs"],
                "node_outputs": op_kwargs["_ng_task_outputs"],
                "engine_config": op_kwargs["_ng_engine_config"],
                "base_values": base_values,
                "upstream": upstream_ids,
                "schedule_subgraphs": schedule_subgraphs,
                "is_async": is_async_callable,
                "task_id": task.task_id,
            }

        task_id_map = {name: task.task_id for name, task in tasks.items()}
        for updates in ctx_updates_by_task.values():
            for update in updates:
                from_task = update.get("from")
                if from_task in task_id_map:
                    update["from_task_id"] = task_id_map[from_task]
        for specs in incoming_specs.values():
            for spec in specs:
                from_task = spec.get("from")
                if from_task in task_id_map:
                    spec["from_task_id"] = task_id_map[from_task]

        while_check_tasks: Dict[str, PythonOperator] = {}
        condition_task_zone: Dict[str, str] = {}
        for zone_name, zone_task in while_zones.items():
            # While semantics in Airflow:
            # 1) `check_condition` runs first and skips the body if False.
            # 2) Body tasks run when True.
            # 3) `check_condition_tail` runs after the body, updates ctx, and
            #    reschedules the loop (resetting tasks) while the condition stays True.
            zone_children = _collect_zone_descendants(zone_task)
            condition_specs = [
                spec
                for spec in incoming_specs.get(zone_name, [])
                if spec.get("target_socket") == "conditions"
            ]
            condition_task_names = [
                spec["from"]
                for spec in condition_specs
                if spec.get("from") in task_id_map
            ]
            for condition_name in condition_task_names:
                condition_task_zone.setdefault(condition_name, zone_name)
            condition_task_ids = [
                task_id_map[name] for name in condition_task_names if name in task_id_map
            ]
            child_task_ids = [
                task_id_map[name] for name in zone_children if name in task_id_map
            ]
            zone_ctx_updates: List[Dict[str, str]] = []
            for child_name in zone_children:
                zone_ctx_updates.extend(ctx_updates_by_task.get(child_name, []))
            zone_literals = _collect_literals(zone_task)
            max_iterations = zone_literals.get("max_iterations", 10000)
            if max_iterations is None:
                max_iterations = 10000

            # Run a pre-check before the body to enforce while semantics.
            pre_check_task = PythonOperator(
                task_id="check_condition",
                dag=dag,
                task_group=task_groups.get(zone_name),
                python_callable=airflow_while_precheck_task,
                op_kwargs={
                    "_ng_while_condition_specs": condition_specs,
                },
            )
            check_task = PythonOperator(
                task_id="check_condition_tail",
                dag=dag,
                task_group=task_groups.get(zone_name),
                python_callable=airflow_while_check_task,
                # Run tail check even if pre-check skipped to release downstream tasks.
                trigger_rule="all_done",
                op_kwargs={
                    "_ng_while_zone": zone_name,
                    "_ng_while_condition_specs": condition_specs,
                    "_ng_while_condition_task_ids": condition_task_ids,
                    "_ng_while_child_task_ids": child_task_ids,
                    "_ng_while_precheck_task_ids": [pre_check_task.task_id],
                    "_ng_while_max_iterations": max_iterations,
                    "_ng_ctx_updates": zone_ctx_updates,
                    "_ng_runtime_context_task_id": runtime_context_task_id,
                },
            )
            while_check_tasks[zone_name] = check_task
            pre_check_task >> check_task
            for child_name in zone_children:
                if child_name in tasks:
                    pre_check_task >> tasks[child_name]
                    tasks[child_name] >> check_task
            for condition_name in condition_task_names:
                if condition_name in tasks:
                    tasks[condition_name] >> pre_check_task
            zone_children_set = set(zone_children)
            for target_name, links in incoming.items():
                if target_name in zone_children_set or target_name in BUILTIN_TASKS:
                    continue
                if target_name in condition_task_names:
                    continue
                if any(lk.from_task.name in zone_children_set for lk in links):
                    if target_name in tasks:
                        # Allow downstream to proceed when loop body is skipped.
                        tasks[target_name].trigger_rule = "none_failed"
                        # Gate on the tail check so downstream waits for loop to finish.
                        check_task >> tasks[target_name]

        for name, task in tasks.items():
            for lk in incoming.get(name, []):
                upstream = lk.from_task.name
                if upstream in tasks:
                    tasks[upstream] >> task

        if_check_tasks: Dict[str, PythonOperator] = {}
        for zone_name, zone_task in if_zones.items():
            zone_children = _collect_zone_descendants(zone_task)
            condition_specs = [
                spec
                for spec in incoming_specs.get(zone_name, [])
                if spec.get("target_socket") == "conditions"
            ]
            condition_task_names = [
                spec["from"]
                for spec in condition_specs
                if spec.get("from") in task_id_map
            ]
            for condition_name in condition_task_names:
                condition_task_zone.setdefault(condition_name, zone_name)
            pre_check_task = PythonOperator(
                task_id="check_condition",
                dag=dag,
                task_group=task_groups.get(zone_name),
                python_callable=airflow_if_precheck_task,
                op_kwargs={
                    "_ng_if_condition_specs": condition_specs,
                },
            )
            if_check_tasks[zone_name] = pre_check_task
            for child_name in zone_children:
                if child_name in tasks:
                    pre_check_task >> tasks[child_name]
            for condition_name in condition_task_names:
                if condition_name in tasks:
                    tasks[condition_name] >> pre_check_task
            zone_children_set = set(zone_children)
            for target_name, links in incoming.items():
                if target_name in zone_children_set or target_name in BUILTIN_TASKS:
                    continue
                if target_name in condition_task_names:
                    continue
                if any(lk.from_task.name in zone_children_set for lk in links):
                    if target_name in tasks:
                        # Allow downstream to proceed when if-body is skipped.
                        tasks[target_name].trigger_rule = "none_failed"

        for ctx_key, readers in ctx_readers_by_key.items():
            writers = ctx_writers_by_key.get(ctx_key, [])
            for reader_name in readers:
                if reader_name not in tasks:
                    continue
                reader_zone = condition_task_zone.get(reader_name) or _find_parent_zone(
                    ng.tasks[reader_name]
                )
                for writer_name in writers:
                    if writer_name == reader_name:
                        continue
                    writer_zone = _find_parent_zone(ng.tasks[writer_name])
                    if writer_zone is not None and writer_zone == reader_zone:
                        continue
                    if reader_zone is None and writer_zone in while_check_tasks:
                        # ctx read outside the loop waits for tail check to publish updates.
                        while_check_tasks[writer_zone] >> tasks[reader_name]
                        continue
                    if writer_zone is not None and writer_zone != reader_zone:
                        check_task = while_check_tasks.get(writer_zone)
                        if check_task is not None:
                            # Cross-zone ctx reads also wait for the writer's tail check.
                            check_task >> tasks[reader_name]
                            continue
                    if writer_name in tasks:
                        tasks[writer_name] >> tasks[reader_name]

        return _CompiledDag(
            dag=dag,
            order=tuple(order),
            incoming=incoming,
            incoming_specs=incoming_specs,
            task_configs=task_configs,
            builtins=builtin_snapshot,
        )

    def build_dag(self, ng: Graph) -> DAG:
        """Return an Airflow DAG representing ``ng`` without executing it."""
        context_task_id = "engine__init"
        finalize_task_id = "engine__finalize"

        compiled = self._compile(
            ng, runtime_context_task_id=context_task_id, schedule_subgraphs=True
        )
        dag = compiled.dag

        init_task = PythonOperator(
            task_id=context_task_id,
            dag=dag,
            python_callable=airflow_init_task,
            op_kwargs={
                "_ng_graph": ng,
                "_ng_default_user_email": self._default_user_email,
                "_ng_profile_name": self._profile_name,
                "_ng_builtins": compiled.builtins,
            },
        )

        task_id_map = {
            name: config.get("task_id", name)
            for name, config in compiled.task_configs.items()
        }
        node_task_ids = [
            name
            for name in compiled.order
            if name not in BUILTIN_TASKS and name in task_id_map
        ]

        finalize_task = PythonOperator(
            task_id=finalize_task_id,
            dag=dag,
            python_callable=airflow_finalize_task,
            op_kwargs={
                "_ng_context_task_id": context_task_id,
                "_ng_task_task_ids": node_task_ids,
                "_ng_task_id_map": task_id_map,
                "_ng_profile_name": self._profile_name,
                "_ng_incoming": compiled.incoming_specs,
            },
            trigger_rule="all_done",
        )

        for task_name in node_task_ids:
            task_id = task_id_map.get(task_name, task_name)
            task = dag.task_dict[task_id]
            if not task.upstream_task_ids:
                init_task >> task
            if not task.downstream_task_ids:
                task >> finalize_task
        for task in dag.task_dict.values():
            if task.task_id in (context_task_id, finalize_task_id):
                continue
            if not task.downstream_task_ids:
                task >> finalize_task
        if not finalize_task.upstream_task_ids:
            init_task >> finalize_task

        return dag

    def run(self, ng: Graph, parent_pid: Optional[str] = None) -> Dict[str, Any]:
        result = self.submit(ng, parent_pid=parent_pid, force_trigger=True, wait=True)
        if result is None:
            raise RuntimeError(
                "AirflowEngine.submit did not return results despite wait=True"
            )
        return result

    def submit(
        self,
        ng: Graph,
        parent_pid: Optional[str] = None,
        force_trigger: Optional[bool] = None,
        wait: bool = False,
        task_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        # ``task_context`` is accepted for interface compatibility with sub-graph execution.
        _ = task_context
        try:
            from aiida.orm.utils.serialize import deserialize_unsafe, serialize
        except Exception as exc:
            raise RuntimeError(
                "Airflow scheduler components are required to schedule sub-graphs"
            ) from exc

        if wait and force_trigger is None:
            # Waiting implies we need to create and trigger a run immediately unless
            # explicitly disabled by the caller.
            force_trigger = True

        self._profile_name = get_active_profile_name()
        self._graph_pid = None

        base_dag_id = _sanitize_dag_id(self.dag_id)
        if base_dag_id != self.dag_id:
            self.dag_id = base_dag_id

        # Use a unique DAG id/file name per submission to avoid collisions.
        dag_id = _sanitize_dag_id(f"{base_dag_id}__{uuid.uuid4().hex}")

        paths = _resolve_scheduler_paths(dag_id)
        dag_path = paths.dags_dir / f"{dag_id}.py"
        os.environ.setdefault("AIRFLOW_HOME", str(paths.airflow_home))
        os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(paths.dags_dir))

        effective_start_date = self.start_date or (
            datetime.now(timezone.utc) - timedelta(minutes=1)
        )
        # Default to a one-off schedule only when we rely on the scheduler to trigger.
        schedule_for_submit = self.schedule
        if schedule_for_submit is None and not force_trigger:
            schedule_for_submit = "@once"

        payload_blob = _build_scheduler_payload(
            dag_id=dag_id,
            ng=ng,
            engine_config={
                "default_args": self.default_args,
                "schedule": schedule_for_submit,
                "start_date": effective_start_date,
                "catchup": self.catchup,
                "max_active_runs": self.max_active_runs,
            },
            default_user_email=self._default_user_email,
            schedule_subgraphs=self.schedule_subgraphs,
            serialize_fn=serialize,
        )

        dag_source = _render_generated_dag(payload_blob)
        _write_dag_file(dag_path, dag_source)

        if not wait and not force_trigger:
            logging.getLogger(__name__).info(
                (
                    "Wrote Airflow DAG '%s' to '%s'; scheduler will pick it up on the "
                    "next refresh (run `airflow dags reserialize` to refresh immediately)"
                ),
                dag_id,
                dag_path,
            )
            return None

        poll_interval = float(os.environ.get("NG_AIRFLOW_POLL_INTERVAL", 2.0))

        if force_trigger:
            run_id = f"{uuid.uuid4().hex}"
            artifacts = _prepare_run_artifacts(
                paths=paths, dag_id=dag_id, run_id=run_id, parent_pid=parent_pid
            )

            dag_obj, dag_persisted = _register_generated_dag(
                dag_id, dag_path, paths.dags_dir
            )

            triggered, trigger_error = _trigger_registered_dag(
                dag_id=dag_id,
                dag_path=dag_path,
                run_id=artifacts.run_id,
                run_conf=artifacts.run_conf,
                dag=dag_obj,
                dag_persisted=dag_persisted,
            )
            if not triggered:
                raise RuntimeError(
                    f"Failed to trigger Airflow DAG '{dag_id}' run '{artifacts.run_id}'"
                ) from trigger_error
        if wait:
            # If we did not force-trigger, rely on the scheduler to create the DAG run
            # and poll for the result written by finalize_task.
            if not force_trigger:
                payload = _poll_for_scheduled_result(
                    dag_id=dag_id,
                    run_root=paths.run_root,
                    poll_interval=poll_interval,
                    deserialize_fn=deserialize_unsafe,
                    started_at_epoch=time.time(),
                )
                graph_pid = payload.get("graph_pid")
                if graph_pid:
                    self._graph_pid = graph_pid
                outputs = payload.get("outputs")
                if not isinstance(outputs, dict):
                    outputs = {}
                return outputs

            payload = _poll_for_result(
                run_id=run_id,
                result_path=artifacts.result_path,
                poll_interval=poll_interval,
                deserialize_fn=deserialize_unsafe,
            )

            graph_pid = payload.get("graph_pid")
            if graph_pid:
                self._graph_pid = graph_pid

            outputs = payload.get("outputs")
            if not isinstance(outputs, dict):
                outputs = {}
            return outputs
