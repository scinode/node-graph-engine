from __future__ import annotations
from typing import Any, Optional, Callable, List, Dict
from node_graph.error_handler import ErrorHandlerSpec
from node_graph.task_spec import TaskHandle
from node_graph.socket_spec import SocketSpec
from node_graph.decorator import task


def decorator_remote_task(
    identifier: Optional[str] = None,
    inputs: Optional[SocketSpec | List[str]] = None,
    outputs: Optional[SocketSpec | List[str]] = None,
    error_handlers: Optional[Dict[str, ErrorHandlerSpec]] = None,
    catalog: str = "Others",
    register_pickle_by_value: bool | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Generate a decorator that register a function as a Graph task.
    After decoration, calling that function `func(x, y, ...)`
    dynamically creates a task in the current Graph context
    instead of executing Python code directly.

    Attributes:
        indentifier (str): task identifier
        catalog (str): task catalog
        inputs (dict): task inputs
        outputs (dict): task outputs
    """

    def wrap(func) -> TaskHandle:
        from node_graph_engine.tasks.remote_fnction_task import RemoteFunctionTask

        return RemoteFunctionTask.build(
            obj=func,
            identifier=identifier or func.__name__,
            catalog=catalog,
            input_spec=inputs,
            output_spec=outputs,
            error_handlers=error_handlers,
            register_pickle_by_value=register_pickle_by_value,
        )

    return wrap


task.remote = decorator_remote_task
