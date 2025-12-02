"""Core utilities for building node-graph engines.

This module exposes a convenience import surface while deferring the actual
module imports until attributes are requested. Importing eagerly at module load
time created a circular dependency between :mod:`node_graph_engine.core` and
``node_graph_engine.persistence`` which prevented callers from importing helper
functions via :mod:`node_graph_engine.persistence`. The lazy access pattern keeps
the public API intact without the circular import.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "BaseEngine",
    "EngineTaskExecutor",
    "TaskMeta",
    "callable_path_for",
    "get_nested_dict",
    "normalize_outputs_for_spec",
    "parse_outputs",
    "_build_node_link_kwargs",
    "_collect_literals",
    "_resolve_tagged_value",
    "_scan_links_topology",
    "update_nested_dict_with_special_keys",
]


_LAZY_ATTRS = {
    "BaseEngine": (".base", "BaseEngine"),
    "EngineTaskExecutor": (".task", "EngineTaskExecutor"),
    "TaskMeta": (".task", "TaskMeta"),
    "callable_path_for": (".task", "callable_path_for"),
    "normalize_outputs_for_spec": (".task", "normalize_outputs_for_spec"),
    "_build_node_link_kwargs": (".utils", "_build_node_link_kwargs"),
    "_collect_literals": (".utils", "_collect_literals"),
    "_resolve_tagged_value": (".utils", "_resolve_tagged_value"),
    "_scan_links_topology": (".utils", "_scan_links_topology"),
    "get_nested_dict": (".utils", "get_nested_dict"),
    "parse_outputs": (".utils", "parse_outputs"),
    "update_nested_dict_with_special_keys": (
        ".utils",
        "update_nested_dict_with_special_keys",
    ),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_ATTRS:
        raise AttributeError(
            f"module 'node_graph_engine.core' has no attribute {name!r}"
        )
    module_name, attr_name = _LAZY_ATTRS[name]
    module = __import__(f"{__name__}{module_name}", fromlist=[attr_name])
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
