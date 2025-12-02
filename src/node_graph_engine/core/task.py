from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Union

from node_graph.socket import TaggedValue
from node_graph.socket_spec import SocketSpec

from .semantics import TaskSemantics


def _coerce_outputs_spec(spec: Any) -> Optional[SocketSpec]:
    if isinstance(spec, SocketSpec):
        return spec
    if isinstance(spec, dict):
        if not spec:
            return None
        return SocketSpec.from_dict(spec)
    to_dict = getattr(spec, "to_dict", None)
    if callable(to_dict):
        return SocketSpec.from_dict(to_dict())
    raise TypeError(f"Cannot coerce outputs spec from type: {type(spec)}")


@dataclass(frozen=True)
class TaskMeta:
    node_name: str
    outputs_spec: Optional[Dict[str, Any]]
    label_kind: str
    is_graph: bool
    semantics: Optional[TaskSemantics] = None

    def __post_init__(self) -> None:
        semantics = self.semantics
        if semantics is None or isinstance(semantics, TaskSemantics):
            return
        converted: Optional[TaskSemantics]
        if isinstance(semantics, Mapping):
            converted = TaskSemantics.from_dict(semantics)
        else:
            to_dict = getattr(semantics, "to_dict", None)
            converted = (
                TaskSemantics.from_dict(to_dict()) if callable(to_dict) else None
            )
        object.__setattr__(self, "semantics", converted)

    def as_dict(self) -> Dict[str, Any]:
        data = {
            "node_name": self.node_name,
            "outputs_spec": self.outputs_spec,
            "label_kind": self.label_kind,
            "is_graph": self.is_graph,
        }
        if self.semantics is not None:
            data["semantics"] = self.semantics.to_dict()
        return data

    @classmethod
    def from_task(
        cls,
        task: Any,
        *,
        label_kind: str = "return",
    ) -> "TaskMeta":
        outputs_spec_obj = getattr(task.spec, "outputs", None)
        outputs_spec: Optional[Dict[str, Any]]
        outputs_semantics_spec: Optional[SocketSpec]
        if isinstance(outputs_spec_obj, SocketSpec):
            outputs_semantics_spec = outputs_spec_obj
            outputs_spec = outputs_spec_obj.to_dict() or None
        elif isinstance(outputs_spec_obj, dict):
            outputs_spec = outputs_spec_obj or None
            outputs_semantics_spec = (
                SocketSpec.from_dict(outputs_spec_obj) if outputs_spec_obj else None
            )
        elif outputs_spec_obj is None:
            outputs_spec = None
            outputs_semantics_spec = None
        else:
            to_dict = getattr(outputs_spec_obj, "to_dict", None)
            if callable(to_dict):
                raw_dict = to_dict() or None
                outputs_spec = raw_dict
                outputs_semantics_spec = (
                    SocketSpec.from_dict(raw_dict) if raw_dict else None
                )
            else:
                outputs_spec = None
                outputs_semantics_spec = None

        inputs_spec_obj = getattr(task.spec, "inputs", None)
        if isinstance(inputs_spec_obj, SocketSpec):
            inputs_semantics_spec = inputs_spec_obj
        elif isinstance(inputs_spec_obj, dict):
            inputs_semantics_spec = (
                SocketSpec.from_dict(inputs_spec_obj) if inputs_spec_obj else None
            )
        else:
            to_dict = getattr(inputs_spec_obj, "to_dict", None)
            if callable(to_dict):
                raw_dict = to_dict() or None
                inputs_semantics_spec = (
                    SocketSpec.from_dict(raw_dict) if raw_dict else None
                )
            else:
                inputs_semantics_spec = None

        semantics = TaskSemantics.from_specs(
            inputs_semantics_spec, outputs_semantics_spec
        )
        task_type = getattr(task.spec, "task_type", "") or ""
        is_graph = task_type.lower() == "graph"
        return cls(
            node_name=getattr(task, "name", "<task>"),
            outputs_spec=outputs_spec,
            label_kind=label_kind,
            is_graph=is_graph,
            semantics=semantics,
        )


def normalize_outputs_for_spec(
    result: Any, outputs_spec: Union[SocketSpec, Dict[str, Any], None]
) -> Dict[str, TaggedValue]:
    from aiida_pythonjob.parsers.utils import parse_outputs

    spec = _coerce_outputs_spec(outputs_spec)
    parsed = parse_outputs(result, spec)
    if isinstance(parsed, tuple):
        parsed = parsed[0]
    if parsed is None:
        raise ValueError("Failed to normalise task outputs against socket spec")
    return parsed


@dataclass(frozen=True)
class EngineTaskExecutor:
    """Encapsulates the runtime callable and metadata for a task execution."""

    runner: Callable[..., Any]
    meta: TaskMeta
    callable: Optional[Callable] = None
    static_kwargs: Dict[str, Any] = field(default_factory=dict)

    def invoke(
        self,
        *,
        parent_pid: Optional[str],
        **inputs: Any,
    ) -> Any:
        payload = dict(self.static_kwargs)
        payload.update(inputs)
        runtime_kwargs = {
            "parent_pid": parent_pid,
            "_ng_meta": self.meta,
            "_ng_callable": self.callable,
        }
        runtime_kwargs.update(payload)
        return self.runner(**runtime_kwargs)
