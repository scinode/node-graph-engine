from __future__ import annotations

"""Celery-backed engine implementation."""

from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from aiida import orm
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
from ..core.task import EngineTaskExecutor, TaskMeta
from ..core.utils import (
    _collect_literals,
    _is_encoded_tagged,
    update_nested_dict_with_special_keys,
)
from ..neo4j.knowledge_graph import persist_workflow_knowledge_graph

from celery import Celery
from celery.result import AsyncResult


_ASYNC_RESULT_TYPES: Tuple[type, ...]
_ASYNC_RESULT_TYPES = (AsyncResult,)


_CELERY_APP_REGISTRY: Dict[str, "Celery"] = {}


def _build_celery_app_from_init(name: str, init: Dict[str, Any]) -> "Celery":
    """Instantiate a Celery application from a serialized payload."""

    kwargs = dict(init.get("app_kwargs") or {})
    broker_url = init.get("broker_url")
    backend_url = init.get("backend_url")

    if broker_url is not None and "broker" not in kwargs and "broker_url" not in kwargs:
        kwargs.setdefault("broker", broker_url)

    if (
        backend_url is not None
        and "backend" not in kwargs
        and "result_backend" not in kwargs
    ):
        kwargs.setdefault("backend", backend_url)

    app = Celery(name, **kwargs)

    config = init.get("app_config")
    if config:
        app.conf.update(config)

    eager_default = init.get("always_eager")
    if eager_default is not None:
        app.conf.task_always_eager = eager_default
        if eager_default:
            app.conf.task_eager_propagates = True

    return app


def _ensure_celery_available() -> None:
    if Celery is None:
        raise RuntimeError(
            "celery is not installed. Install `celery` to use CeleryEngine."
        )


def _register_celery_app(app: "Celery") -> str:
    app_id = getattr(app, "_node_graph_engine_id", None)
    if not app_id:
        app_id = str(uuid4())
        setattr(app, "_node_graph_engine_id", app_id)
    _CELERY_APP_REGISTRY[app_id] = app
    return app_id


def _get_registered_celery_app(app_id: str) -> "Celery":
    try:
        return _CELERY_APP_REGISTRY[app_id]
    except KeyError as exc:
        raise RuntimeError(
            f"Celery application with id {app_id!r} is not registered."
        ) from exc


def _celery_node_task(**kwargs: Any) -> Dict[str, Any]:
    return _node_job(**kwargs)


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
    default_user_email = engine_options.get("_default_user_email")
    user = None
    if default_user_email is not None:
        user = orm.User.collection.get(email=default_user_email)

    celery_app_id = engine_options.get("celery_app_id")
    task_queue = engine_options.get("task_queue")
    apply_async_kwargs = engine_options.get("apply_async_kwargs")
    celery_init = engine_options.get("celery_init")

    def _build_sub_engine(name: str) -> "CeleryEngine":
        return CeleryEngine(
            name=name,
            _celery_app_id=celery_app_id,
            task_queue=task_queue,
            task_options=dict(apply_async_kwargs or {}),
            _default_user_email=default_user_email,
            _celery_init=celery_init,
        )

    return execute_task_job(
        parent_pid=parent_pid,
        meta=_ng_meta,
        callable_payload=_ng_callable,
        runtime_inputs=kwargs,
        engine_name=_ng_engine_name or "celery-flow",
        node_inputs=_ng_task_inputs,
        node_outputs=_ng_task_outputs,
        build_sub_engine=_build_sub_engine,
        user=user,
    )


def _celery_node_submit(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: Optional[str] = None,
    _ng_task=None,
    _ng_engine_options: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
):
    if _ng_task is None:
        raise RuntimeError("Celery task handle is not available for execution")

    submit_kwargs = dict(kwargs)
    if isinstance(_ng_meta, TaskMeta):
        meta_payload: Any = _ng_meta.as_dict()
    elif hasattr(_ng_meta, "as_dict") and callable(getattr(_ng_meta, "as_dict")):
        meta_payload = _ng_meta.as_dict()
    else:
        meta_payload = _ng_meta

    submit_kwargs.update(
        {
            "parent_pid": parent_pid,
            "_ng_meta": meta_payload,
            "_ng_callable": _ng_callable,
            "_ng_engine_name": _ng_engine_name or "celery-flow",
            "_ng_engine_options": _ng_engine_options,
        }
    )

    engine_options = dict(_ng_engine_options or {})

    apply_async_kwargs = dict(engine_options.get("apply_async_kwargs", {}))
    queue = engine_options.get("task_queue")
    if queue is not None and "queue" not in apply_async_kwargs:
        apply_async_kwargs["queue"] = queue

    result = _ng_task.apply_async(kwargs=submit_kwargs, **apply_async_kwargs)
    return result


def _resolve_async_payload(value: Any) -> Any:
    """Resolve Celery ``AsyncResult`` instances to concrete values."""

    if isinstance(value, _ASYNC_RESULT_TYPES):
        resolved = value.get(disable_sync_subtasks=False)
        return _resolve_async_payload(resolved)
    if isinstance(value, orm.Data):
        return value
    if _is_encoded_tagged(value):
        return value
    if isinstance(value, dict):
        return {k: _resolve_async_payload(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        container = type(value)
        return container(_resolve_async_payload(v) for v in value)
    return value


class CeleryEngine(BaseEngine):
    """Run Graphs using Celery tasks with provenance tracking."""

    engine_kind = "celery"

    def __init__(
        self,
        name: str = "celery-flow",
        *,
        celery_app: Optional["Celery"] = None,
        broker_url: Optional[str] = "memory://",
        backend_url: Optional[str] = "rpc://",
        app_kwargs: Optional[Dict[str, Any]] = None,
        app_config: Optional[Dict[str, Any]] = None,
        task_queue: Optional[str] = None,
        task_options: Optional[Dict[str, Any]] = None,
        always_eager: Optional[bool] = None,
        _default_user_email: Optional[str] = None,
        _celery_app_id: Optional[str] = None,
        _celery_init: Optional[Dict[str, Any]] = None,
    ) -> None:
        _ensure_celery_available()
        super().__init__(name)

        init_payload: Dict[str, Any] = dict(_celery_init or {})
        if _celery_app_id is not None:
            if celery_app is not None:
                raise ValueError(
                    "Cannot provide both `celery_app` and `_celery_app_id`."
                )
            try:
                app = _get_registered_celery_app(_celery_app_id)
            except RuntimeError:
                if not init_payload:
                    raise
                app = _build_celery_app_from_init(name, init_payload)
                setattr(app, "_node_graph_engine_id", _celery_app_id)
            else:
                if not init_payload:
                    init_payload = {
                        "broker_url": app.conf.get("broker_url"),
                        "backend_url": app.conf.get("result_backend"),
                        "always_eager": app.conf.task_always_eager,
                    }
        else:
            if celery_app is not None:
                app = celery_app
                init_payload.setdefault("app_kwargs", {})
                broker_from_app = getattr(app.conf, "broker_url", None)
                if broker_from_app is not None:
                    init_payload.setdefault("broker_url", broker_from_app)
                backend_from_app = getattr(app.conf, "result_backend", None)
                if backend_from_app is not None:
                    init_payload.setdefault("backend_url", backend_from_app)
            else:
                kwargs = dict(app_kwargs or {})
                if (
                    broker_url is not None
                    and "broker" not in kwargs
                    and "broker_url" not in kwargs
                ):
                    kwargs.setdefault("broker", broker_url)
                if (
                    backend_url is not None
                    and "backend" not in kwargs
                    and "result_backend" not in kwargs
                ):
                    kwargs.setdefault("backend", backend_url)
                app = Celery(name, **kwargs)
                init_payload.setdefault("app_kwargs", dict(kwargs))
                if broker_url is not None:
                    init_payload.setdefault("broker_url", broker_url)
                if backend_url is not None:
                    init_payload.setdefault("backend_url", backend_url)

            if app_config:
                app.conf.update(app_config)
                config_copy = dict(app_config)
                existing_config = init_payload.get("app_config", {})
                merged_config = dict(existing_config)
                merged_config.update(config_copy)
                init_payload["app_config"] = merged_config

            eager_default: Optional[bool] = always_eager
            if eager_default is None:
                broker_config = app.conf.get("broker_url") or broker_url
                if isinstance(broker_config, str) and broker_config.startswith(
                    "memory://"
                ):
                    eager_default = True

            if eager_default is not None:
                app.conf.task_always_eager = eager_default
                if eager_default:
                    app.conf.task_eager_propagates = True
            init_payload.setdefault("always_eager", app.conf.task_always_eager)

        if "always_eager" not in init_payload:
            init_payload["always_eager"] = app.conf.task_always_eager

        self._app = app
        self._celery_app_id = _register_celery_app(app)
        self._task_queue = task_queue
        self._apply_async_kwargs: Dict[str, Any] = dict(task_options or {})
        self._default_user_email = (
            _default_user_email or orm.User.collection.get_default().email
        )
        self._default_user = orm.User.collection.get(email=self._default_user_email)
        self._node_task = self._ensure_node_task()
        self._init_payload = {k: v for k, v in init_payload.items() if v is not None}

    def _ensure_node_task(self):
        task_name = "node_graph_engine.celery.node_task"
        task = self._app.tasks.get(task_name)
        if task is None:
            task = self._app.task(name=task_name)(_celery_node_task)
        return task

    @property
    def celery_app(self) -> "Celery":
        """Return the underlying :class:`~celery.Celery` application instance."""

        return self._app

    def _ensure_node_value(self, source_map: Dict[str, Any], name: str) -> Any:
        value = source_map[name]
        if isinstance(value, _ASYNC_RESULT_TYPES):
            resolved = value.get(disable_sync_subtasks=False)
            source_map[name] = resolved
            return resolved
        return value

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        self._ensure_node_value(source_map, from_name)
        return super()._link_socket_value(from_name, from_socket, source_map)

    def _link_whole_output(self, from_name: str, source_map: Dict[str, Any]) -> Any:
        self._ensure_node_value(source_map, from_name)
        return super()._link_whole_output(from_name, source_map)

    def _resolve_values(self, values: Dict[str, Any]) -> Dict[str, Any]:
        resolved: Dict[str, Any] = dict(values)
        for key, value in list(resolved.items()):
            resolved[key] = _resolve_async_payload(value)
        return resolved

    def _engine_options_payload(self) -> Dict[str, Any]:
        payload = {
            "celery_app_id": self._celery_app_id,
            "task_queue": self._task_queue,
            "apply_async_kwargs": dict(self._apply_async_kwargs),
            "_default_user_email": self._default_user_email,
            "celery_init": dict(self._init_payload),
        }
        return payload

    def _build_task_executor(
        self,
        task,
        label_kind: str,
    ) -> EngineTaskExecutor:
        executor = task.spec.executor.to_dict()
        meta = self._build_node_task_meta(task, label_kind)
        inputs_spec = task.spec.inputs.to_dict() if task.spec.inputs else {}
        outputs_spec = task.spec.outputs.to_dict() if task.spec.outputs else {}

        static_kwargs = {
            "_ng_engine_name": self.name,
            "_ng_task_inputs": inputs_spec,
            "_ng_task_outputs": outputs_spec,
            "_ng_engine_options": self._engine_options_payload(),
            "_ng_task": self._node_task,
        }

        return EngineTaskExecutor(
            runner=_celery_node_submit,
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
            user=self._default_user,
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
                task_result = executor_obj.invoke(
                    parent_pid=context.process_node.uuid,
                    **kw,
                )
                values[name] = task_result

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
