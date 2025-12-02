from __future__ import annotations

import asyncio
import shutil
import tempfile
import threading
from typing import Any, Callable, Coroutine, Dict, Optional, TypeVar

from aiida import load_profile, orm
from aiida.common import exceptions as aiida_exceptions
from aiida.engine.processes.calcjobs.manager import JobManager
from aiida.engine.processes.calcjobs.tasks import (
    task_retrieve_job,
    task_submit_job,
    task_update_job,
    task_upload_job,
)
from aiida.engine.processes.ports import PORT_NAMESPACE_SEPARATOR
from aiida.engine.transports import TransportQueue
from aiida.engine.utils import InterruptableFuture
from aiida.plugins.utils import PluginVersionProvider
from aiida_pythonjob.calculations.pythonjob import PythonJob
from aiida_pythonjob.launch import prepare_pythonjob_inputs
from node_graph.socket_spec import SocketSpec
from plumpy import ProcessState
from aiida.orm.utils.serialize import serialize, deserialize_unsafe

from ..core.execution import (
    _ensure_meta,
    _resolve_callable,
)
from ..core.semantics import TaskSemantics, store_socket_semantics_from_links
from ..core.utils import (
    _decode_runtime_inputs,
    _encode_runtime_inputs,
    close_threadlocal_aiida_session,
    load_default_user,
    reset_default_user_cache,
    update_nested_dict_with_special_keys,
)
import logging


logger = logging.getLogger(__name__)


def _ensure_aiida_profile_loaded(profile_name: Optional[str] = None) -> None:
    """Ensure an AiiDA profile is loaded before accessing ORM resources."""

    from aiida.manage.manager import get_manager

    manager = get_manager()
    current = manager.get_profile()
    if current is not None:
        if profile_name and current.name != profile_name:
            load_profile(profile_name)
        return

    try:
        if profile_name:
            load_profile(profile_name)
        else:
            load_profile()
    except Exception as exc:
        raise aiida_exceptions.ConfigurationError(
            "AiidaRemoteEngine requires an AiiDA profile. "
            "Call `aiida.load_profile()` before using the engine or configure a default profile."
        ) from exc


T = TypeVar("T")


class ProcessRunner:
    """Single process-wide runner that keeps transport resources alive."""

    _instance: Optional["ProcessRunner"] = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        logger.info("Creating process-wide remote execution runner")
        self.loop = asyncio.new_event_loop()
        self.communicator = None
        self.persister = None
        self.controller = None
        self.transport = TransportQueue(self.loop)
        self.job_manager = JobManager(self.transport)
        self.plugin_version_provider = PluginVersionProvider()
        self._started = threading.Event()
        self._thread = threading.Thread(
            name="GraphRemoteRunner",
            target=self._run_event_loop,
            daemon=True,
        )
        self._thread.start()
        self._started.wait()

    @classmethod
    def get_instance(cls) -> "ProcessRunner":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _run_event_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._started.set()
        self.loop.run_forever()

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        if self.loop.is_closed():
            raise RuntimeError("Remote runner loop is closed")

        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()


async def _execute_pythonjob_remote(
    *,
    pythonjob_inputs: Dict[str, Any],
    parent_pk: Optional[int],
    meta_dict: Dict[str, Any],
    engine_name: str,
    profile_name: Optional[str],
    default_user_email: Optional[str],
    runner: ProcessRunner,
) -> Dict[str, Any]:
    """Launch a PythonJob CalcJob asynchronously using AiiDA transport helpers."""

    reset_default_user_cache()
    load_default_user(default_user_email)

    pythonjob_inputs = deserialize_unsafe(pythonjob_inputs)

    process = PythonJob(
        inputs=pythonjob_inputs,
        runner=runner,
        parent_pid=parent_pk,
        enable_persistence=False,
    )

    node = process.node
    node.store()

    node.set_process_label(f"Remote<{meta_dict['node_name']}>")
    node.set_process_state(ProcessState.RUNNING)

    transport_queue = runner.transport
    authinfo = node.get_authinfo()
    if authinfo is None:
        raise aiida_exceptions.ConfigurationError(
            "No AuthInfo available for remote execution; configure computer for the current user."
        )
    job_manager = runner.job_manager

    try:
        cancellable = InterruptableFuture()
        skip_submit = await task_upload_job(process, transport_queue, cancellable)

        if not skip_submit:
            cancellable = InterruptableFuture()
            await task_submit_job(node, transport_queue, cancellable)

            job_done = False
            while not job_done:
                cancellable = InterruptableFuture()
                job_done = await task_update_job(node, job_manager, cancellable)

        retrieved_tmp = tempfile.mkdtemp()
        try:
            cancellable = InterruptableFuture()
            await task_retrieve_job(
                process,
                transport_queue,
                retrieved_tmp,
                cancellable,
            )
            process.update_outputs()
            exit_code = process.parse(retrieved_tmp)
        finally:
            shutil.rmtree(retrieved_tmp, ignore_errors=True)

        if exit_code is not None and exit_code.status != 0:
            message = exit_code.message or "Remote PythonJob failed"
            node.set_exit_status(exit_code.status)
            node.set_exit_message(message)
            node.set_process_state(ProcessState.EXCEPTED)
            raise RuntimeError(message)

        node.set_exit_status(0)
        node.set_process_state(ProcessState.FINISHED)

        process.update_outputs()
        semantics_spec = TaskSemantics.from_dict(meta_dict.get("semantics"))
        store_socket_semantics_from_links(node, semantics_spec)
        outgoing = node.base.links.get_outgoing()
        flattened: Dict[str, Any] = {
            entry.link_label.replace(PORT_NAMESPACE_SEPARATOR, "."): entry.node
            for entry in outgoing
        }
        outputs = update_nested_dict_with_special_keys(dict(flattened))
        node.base.extras.set(
            "node_graph",
            {
                "engine": engine_name,
                "meta": meta_dict,
            },
        )
        return outputs
    except Exception:
        if node.process_state not in (ProcessState.FINISHED, ProcessState.EXCEPTED):
            node.set_process_state(ProcessState.EXCEPTED)
        raise
    finally:
        node.seal()
        reset_default_user_cache()


def _run_async(coro_factory: Callable[[ProcessRunner], Coroutine[Any, Any, T]]) -> T:
    """Execute the coroutine factory on the process-wide runner loop."""

    runner = ProcessRunner.get_instance()
    return runner.run(coro_factory(runner))


def _remote_task_job(
    parent_pid: Optional[str],
    _ng_meta,
    _ng_callable=None,
    _ng_engine_name: str = "",
    _ng_task_inputs=None,
    _ng_task_outputs=None,
    _ng_task_metadata=None,
    _ng_default_user_email: Optional[str] = None,
    _ng_profile_name: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Execute a task by launching a remote PythonJob without the AiiDA engine runner."""

    _ensure_aiida_profile_loaded(_ng_profile_name)
    meta = _ensure_meta(_ng_meta)

    try:
        if meta.is_graph:
            raise RuntimeError(
                "Subgraph execution on remote computers is not supported."
            )

        callable_obj = _resolve_callable(_ng_callable, meta.node_name)
        runtime_inputs = _decode_runtime_inputs(kwargs)
        runtime_inputs = update_nested_dict_with_special_keys(runtime_inputs)

        inputs_spec = (
            SocketSpec.from_dict(_ng_task_inputs or {}) if _ng_task_inputs else None
        )
        if _ng_task_outputs:
            for key in _ng_task_metadata.get("non_function_outputs", []):
                _ng_task_outputs["fields"].pop(key, None)
        outputs_spec = (
            SocketSpec.from_dict(_ng_task_outputs) if _ng_task_outputs else None
        )
        # Pull out code, computer, etc
        computer = runtime_inputs.pop("computer", "localhost")
        code = runtime_inputs.pop("code", None)
        if isinstance(computer, orm.Str):
            computer = computer.value
        command_info = runtime_inputs.pop("command_info", {})
        register_pickle_by_value = runtime_inputs.pop("register_pickle_by_value", False)
        upload_files = runtime_inputs.pop("upload_files", {})

        metadata = runtime_inputs.pop("metadata", {})
        metadata.update({"call_link_label": meta.node_name})

        pythonjob_inputs = prepare_pythonjob_inputs(
            function=callable_obj,
            function_inputs=runtime_inputs,
            inputs_spec=inputs_spec,
            outputs_spec=outputs_spec,
            code=code,
            computer=computer,
            command_info=command_info or None,
            metadata=metadata,
            upload_files=upload_files or None,
            process_label=meta.node_name,
            register_pickle_by_value=register_pickle_by_value,
        )

        parent_pk: Optional[int] = None
        if parent_pid:
            parent_node = orm.load_node(parent_pid)
            parent_pk = parent_node.pk

        pythonjob_inputs = serialize(pythonjob_inputs)
        outputs = _run_async(
            lambda runner: _execute_pythonjob_remote(
                pythonjob_inputs=pythonjob_inputs,
                parent_pk=parent_pk,
                meta_dict=meta.as_dict(),
                engine_name=_ng_engine_name,
                profile_name=_ng_profile_name,
                default_user_email=_ng_default_user_email,
                runner=runner,
            )
        )
        encoded = _encode_runtime_inputs(outputs or {})
        return encoded
    finally:
        close_threadlocal_aiida_session()
