from __future__ import annotations
from typing import Callable, List, Optional, Dict, Annotated
from node_graph.socket_spec import (
    infer_specs_from_callable,
    SocketSpec,
    namespace,
    SocketMeta,
    merge_specs,
)
from node_graph.task_spec import (
    TaskSpec,
    TaskHandle,
    SchemaSource,
)
from node_graph.executor import RuntimeExecutor
from node_graph.error_handler import ErrorHandlerSpec, normalize_error_handlers
from node_graph.task import Task
from .utils import from_aiida_process
from aiida_pythonjob import PythonJob


class RemoteFunctionTask(Task):
    """A task that wraps a Python callable (function or method)."""

    identifier: str = "node_graph_engine.remote_function_node"
    catalog: str = "Builtins"
    is_dynamic: bool = True

    @classmethod
    def build(
        cls,
        *,
        obj: Callable,
        identifier: Optional[str] = None,
        task_type: str = "RemoteFunction",
        catalog: str = None,
        input_spec: Optional[SocketSpec | List[str]] = None,
        output_spec: Optional[SocketSpec | List[str]] = None,
        error_handlers: Optional[Dict[str, ErrorHandlerSpec]] = None,
        metadata: Optional[dict] = None,
        register_pickle_by_value: bool | None = None,
    ) -> TaskSpec:
        """
        - infers function I/O
        - optionally merges process-contributed I/O
        - optionally merges additional I/O
        - records *each* contribution in metadata
        """
        from node_graph.socket_spec import validate_socket_data

        input_spec = validate_socket_data(input_spec)
        output_spec = validate_socket_data(output_spec)
        func_in, func_out = infer_specs_from_callable(obj, input_spec, output_spec)
        # additions specific to PythonJob
        add_inputs = namespace(
            computer=Annotated[str, SocketMeta(required=False)],
            command_info=Annotated[dict, SocketMeta(required=False)],
            register_pickle_by_value=Annotated[bool, SocketMeta(required=False)],
        )
        add_outputs = namespace()
        proc_in, proc_out = from_aiida_process(PythonJob)
        func_in = merge_specs(func_in, proc_in)
        func_in = merge_specs(func_in, add_inputs)
        func_out = merge_specs(func_out, proc_out)
        error_handlers = normalize_error_handlers(error_handlers)
        metadata = metadata or {}
        metadata.update(
            {
                "non_function_inputs": list(
                    set((proc_in and proc_in.fields.keys()) or [])
                    | set((add_inputs and add_inputs.fields.keys()) or [])
                ),
                "non_function_outputs": list(
                    set((proc_out and proc_out.fields.keys()) or [])
                    | set((add_outputs and add_outputs.fields.keys()) or [])
                ),
            }
        )
        executor = RuntimeExecutor.from_callable(
            obj, register_pickle_by_value=register_pickle_by_value
        )
        # We always use the EMBEDDED schema for the function task, but when storing the spec in the DB,
        # we will check if the callable is a BaseHandler, and switch the schema_source to HANDLER accordingly.
        # This avoids cyclic import.
        schema_source = SchemaSource.EMBEDDED
        spec = TaskSpec(
            identifier=identifier or obj.__name__,
            schema_source=schema_source,
            task_type=task_type,
            catalog=catalog,
            inputs=func_in,
            outputs=func_out,
            executor=executor,
            error_handlers=error_handlers,
            base_class=cls,
            metadata=metadata,
        )
        handle = TaskHandle(spec)
        handle._callable = obj
        return handle
