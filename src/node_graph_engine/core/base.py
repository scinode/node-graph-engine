from __future__ import annotations

from abc import ABC
from typing import Any, Callable, Dict, Optional
from .task import TaskMeta
from node_graph import Graph
from .utils import (
    _build_node_link_kwargs,
    get_nested_dict,
)
from node_graph_engine.utils.graphviz_html import GraphvizHTMLProxy


class BaseEngine(ABC):
    """Common helpers shared by engine implementations."""

    engine_kind = "engine"

    def __init__(
        self,
        name: str,
    ) -> None:
        self.name = name
        self._graph_pid: Optional[str] = None

    @staticmethod
    def _is_graph_task(task) -> bool:
        return getattr(task.spec, "task_type", "").lower() == "graph"

    @staticmethod
    def _extract_executor_callable(task) -> Optional[Callable]:
        exec_obj = getattr(task.spec, "executor", None)
        if not exec_obj:
            return None
        fn = getattr(exec_obj, "callable", None)
        if hasattr(fn, "_callable"):
            fn = getattr(fn, "_callable")
        return fn

    @staticmethod
    def _snapshot_builtins(ng: Graph) -> Dict[str, Dict[str, Any]]:
        return {
            "graph_ctx": ng.ctx._collect_values(raw=False),
            "graph_inputs": ng.inputs._collect_values(raw=False),
            "graph_outputs": ng.outputs._collect_values(raw=False),
        }

    def _graph_flow_run_id(self, ng: Graph) -> str:
        return f"{self.engine_kind}:{self.name}"

    def _graph_task_run_id(self, ng: Graph) -> str:
        return f"{self.engine_kind}:{ng.name}"

    def _build_node_task_meta(self, task, label_kind: str) -> TaskMeta:
        return TaskMeta.from_task(task, label_kind=label_kind)

    def _link_socket_value(
        self, from_name: str, from_socket: str, source_map: Dict[str, Any]
    ) -> Any:
        return get_nested_dict(source_map[from_name], from_socket, default=None)

    def _link_whole_output(self, from_name: str, source_map: Dict[str, Any]) -> Any:
        return source_map[from_name]

    def _link_bundle(self, payload: Dict[str, Any]) -> Any:
        return payload

    def _build_link_kwargs(
        self,
        target_name: str,
        links,
        source_map: Dict[str, Any],
    ) -> Dict[str, Any]:
        return _build_node_link_kwargs(
            target_name,
            links,
            source_map,
            resolve_socket=self._link_socket_value,
            resolve_whole=self._link_whole_output,
            bundle_factory=self._link_bundle,
        )

    @property
    def provenance_graph(self):
        from aiida.tools.visualization import Graph

        graph = Graph(engine="dot", node_id_type="uuid")
        graph.recurse_ancestors(
            self._graph_pid,
            annotate_links="both",
        )
        graph.recurse_descendants(
            self._graph_pid,
            annotate_links="both",
        )
        return GraphvizHTMLProxy(graph.graphviz)
