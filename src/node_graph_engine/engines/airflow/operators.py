"""Custom Airflow operators for Graph tasks."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from .async_request import AsyncNodeExecutionRequest, GraphAsyncTrigger


class GraphPythonOperator(BaseOperator):
    """Custom operator that supports deferrable async Graph tasks."""

    ui_color = "#ffefeb"

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_kwargs: Optional[Dict[str, Any]] = None,
        is_async: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}
        self.is_async = is_async

    def execute(self, context: Dict[str, Any]) -> Any:
        runtime_kwargs = dict(context)
        runtime_kwargs.update(self.op_kwargs)
        runtime_kwargs.setdefault("_ng_is_async", self.is_async)

        if self.is_async:

            def _defer(request: AsyncNodeExecutionRequest) -> None:
                trigger = GraphAsyncTrigger(payload=request.serialize())
                self.defer(trigger=trigger, method_name="execute_complete")

            runtime_kwargs["_ng_defer"] = _defer

        return self.python_callable(**runtime_kwargs)

    def execute_complete(
        self,
        context: Dict[str, Any],
        event: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if not event:
            return None

        status = event.get("status")
        if status == "success":
            result = event.get("result")
            ti = context.get("ti")
            if ti is not None:
                ti.xcom_push(key="return_value", value=result)
            return result

        message = event.get("message", "unknown async failure")
        raise AirflowException(f"Async task execution failed: {message}")
